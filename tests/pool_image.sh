#!/bin/sh
# A deploy that changes the worker image replaces the pool's machines
# (morca.pool_scale, #356), against tests/fake_hcloud.py as
# tests/pool_scale.sh is, with plain (tests/grants/grantee.mojo, a run of
# 6 s) on a pool of max_machines = 1 and idle_seconds = 600:
#
# - a machine records the image it was made for, and boots with it;
# - after the scheduler restarts with another MORCA_WORKER_IMAGE, the
#   machine running a run drains: the run finishes on it, and it's given
#   no other (the run queued behind it waits);
# - once idle it's removed, without waiting for idle_seconds, saying why,
#   and a machine of the new image is made (never two at once) and runs
#   the waiting run. The UI, not restarted, still has the old image: a
#   machine boots with the image the autoscaler made it for.
#
# Needs MORCA_DATABASE_URL (pixi run test-pool-image starts a throwaway
# server), curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
one_cluster_test
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC -D GRANTEE=plain tests/grants/grantee.mojo -o build/plain
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/pool-image.log
: > "$log"
dir=$PWD/build/pool-image
rm -rf "$dir"
mkdir -p "$dir"
export MORCA_WAREHOUSE="$dir/wh" MORCA_LOGS="$dir/logs" GRANTEE_OUT="$dir/out" GRANTEE_SLEEP=6
mkdir -p "$GRANTEE_OUT"
export MORCA_STALE_SECONDS=5 MORCA_START_SECONDS=20 MORCA_SAMPLE_SECONDS=0 MORCA_WATCHDOG_SECONDS=0
unset MORCA_RUNNER MORCA_CONFIG
free_port hport
free_port uport
base="http://127.0.0.1:$uport"
export MORCA_HCLOUD_ENDPOINT="http://127.0.0.1:$hport/v1" MORCA_HCLOUD_TOKEN=test-token
export MORCA_REGISTRY_ENDPOINT="http://127.0.0.1:$hport"
old=ghcr.io/morca-dev/census:t1
new=ghcr.io/morca-dev/census:t2
pids=
sched=
cleanup() {
    for p in $pids $sched; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    tail -20 build/pool-image-sched.out 2> /dev/null || true
    for f in "$dir"/server-*.log; do [ -f "$f" ] && { echo "== $f"; tail -10 "$f"; }; done
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
is() { [ "$(q "$1")" = "$2" ]; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
machines() { q "SELECT coalesce(string_agg(name || ' ' || coalesce(image, '-') || ' ' || CASE WHEN ended IS NULL THEN 'live' ELSE 'ended: ' || why END, '; ' ORDER BY id), '') FROM orca.pool_machines"; }
# boot_image N: the image the Nth server made booted with.
boot_image() { cat "$(ls -d "$dir"/boot-* | sort -V | sed -n "${1}p")/image"; }
worker_of() { q "SELECT m.name FROM orca.runs r JOIN orca.pool_machines m ON m.worker = r.worker WHERE r.partitions = '$1'"; }

env -u MORCA_DATABASE_URL python3 tests/fake_hcloud.py "$hport" "$dir" &
pids="$pids $!"
until_ 10 "the fake didn't start" curl -s -o /dev/null "$MORCA_HCLOUD_ENDPOINT/server_types"
echo "amd64 arm64" > "$dir/arches"
echo "cpx32 hel1" > "$dir/stock"
# The UI keeps the old image throughout.
MORCA_WORKER_IMAGE=$old MORCA_UI_ADDR="127.0.0.1:$uport" ./build/morca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "morca ui didn't start" curl -fs "$base/" -o /dev/null

cat > build/pool-image.toml << 'EOF'
[pools.t]
provider = "hetzner"
machines = [{ type = "cpx32" }]
locations = ["hel1"]
max_machines = 1
idle_seconds = 600

[pipelines.plain]
placement = "worker:t"
EOF
./build/morca register --config build/pool-image.toml ./build/plain >> "$log" 2>&1 || fail "register"

# start_sched IMAGE: the scheduler, as a deploy of IMAGE starts it.
start_sched() {
    MORCA_WORKER_IMAGE=$1 MORCA_POOL_REMOTE="$base" MORCA_POOL_WORKER_COMMAND="$PWD/build/morca" \
    MORCA_POOL_SCALE_SECONDS=0.5 MORCA_POOL_GROW_AFTER=0 MORCA_POOL_TRY_SECONDS=0.5 MORCA_POOL_IMAGE_SECONDS=0 \
        ./build/morca scheduler >> build/pool-image-sched.out 2>&1 &
    sched=$!
}
: > build/pool-image-sched.out
start_sched "$old"

# Never two machines live at once, from here on.
peak=build/pool-image-peak.out
: > "$peak"
(
    while :; do
        q "SELECT count(*) FROM orca.pool_machines WHERE ended IS NULL" >> "$peak" 2> /dev/null || true
        sleep 0.2
    done
) &
pids="$pids $!"

./build/morca backfill plain 2024-01 >> "$log" 2>&1 || fail "backfill 2024-01"
until_ 90 "2024-01 didn't succeed: $(q "SELECT id, status, held FROM orca.runs") / $(machines)" \
    is "SELECT status FROM orca.runs WHERE partitions = '2024-01'" succeeded
m1=$(q "SELECT name FROM orca.pool_machines")
is "SELECT image FROM orca.pool_machines WHERE name = '$m1'" "$old" || fail "the machine's image: $(machines)"
[ "$(boot_image 1)" = "$old" ] || fail "it booted with $(boot_image 1)"
echo "ok   a machine records the image it was made for, and boots with it"

# A run going on it, another queued behind it (max_runs = 1); then the
# deploy: the scheduler restarts with the new image.
./build/morca backfill plain 2024-02 >> "$log" 2>&1 || fail "backfill 2024-02"
until_ 20 "2024-02 didn't start on $m1: $(q "SELECT id, status FROM orca.runs")" \
    is "SELECT status FROM orca.runs WHERE partitions = '2024-02'" running
./build/morca backfill plain 2024-03 >> "$log" 2>&1 || fail "backfill 2024-03"
kill "$sched"
wait "$sched" 2> /dev/null || true
start_sched "$new"
until_ 20 "$m1 isn't draining: $(machines)" \
    is "SELECT draining IS NOT NULL FROM orca.pool_machines WHERE name = '$m1'" t
grep -q "draining machine $m1 of pool t (it runs $old, the pool's image is now $new)" build/pool-image-sched.out \
    || fail "the scheduler didn't say it drains $m1"
is "SELECT status FROM orca.runs WHERE partitions = '2024-02'" running || fail "2024-02 isn't still running on $m1"
is "SELECT ended IS NULL FROM orca.pool_machines WHERE name = '$m1'" t || fail "$m1 was removed while busy: $(machines)"
echo "ok   after a deploy of another image, the busy machine drains and keeps its run"

until_ 30 "2024-02 didn't succeed: $(q "SELECT id, status FROM orca.runs")" \
    is "SELECT status FROM orca.runs WHERE partitions = '2024-02'" succeeded
[ "$(worker_of 2024-02)" = "$m1" ] || fail "2024-02 ran on $(worker_of 2024-02)"
until_ 30 "$m1 wasn't replaced: $(machines)" \
    is "SELECT why FROM orca.pool_machines WHERE name = '$m1'" "replaced: it ran $old, the pool runs $new"
until_ 90 "2024-03 didn't succeed: $(q "SELECT id, status, held FROM orca.runs") / $(machines)" \
    is "SELECT status FROM orca.runs WHERE partitions = '2024-03'" succeeded
m2=$(worker_of 2024-03)
[ -n "$m2" ] && [ "$m2" != "$m1" ] || fail "2024-03 ran on '$m2', not a new machine: $(machines)"
is "SELECT image || ' ' || (draining IS NULL) FROM orca.pool_machines WHERE name = '$m2'" "$new true" \
    || fail "the new machine: $(machines)"
[ "$(boot_image 2)" = "$new" ] || fail "$m2 booted with $(boot_image 2), the UI's"
is "SELECT count(*) FROM orca.runs WHERE starts <> 1" 0 || fail "a run started twice: $(q "SELECT id, partitions, starts FROM orca.runs")"
most=$(sort -n "$peak" | tail -1)
[ "$most" -le 1 ] || fail "$most machines live at once: $(machines)"
echo "ok   once idle it's replaced by a machine of the new image, which runs the run that waited ($m1 -> $m2)"
echo "pool image: ok"
