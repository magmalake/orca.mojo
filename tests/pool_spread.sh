#!/bin/sh
# One run over two machines the pool autoscaler makes (#268, milestone B),
# against tests/fake_hcloud.py: fanout, with workers = 1 on [pools.t], from
# an empty pool:
#
# - the first machine is made for the queued run and starts it as a cluster;
# - the run's want of a member is demand: a second machine is made, joins
#   the private network the first did, and joins the run;
# - both run its tasks, the total is one machine's;
# - idle once the run ends, both are removed.
#
# Each fake server runs its boot script, which runs `orca worker` with the
# address ORCA_BOOT_ADVERTISE gives it (127.0.0.1 here; the private one
# from the provider's metadata on a real machine).
#
# Needs ORCA_DATABASE_URL (pixi run test-pool-spread), curl, jq, python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/pool-spread.log
: > "$log"
dir=$PWD/build/pool-spread
rm -rf "$dir"
mkdir -p "$dir"
export ORCA_WAREHOUSE="$dir/wh" ORCA_LOGS="$dir/logs"
export ORCA_STALE_SECONDS=5 ORCA_START_SECONDS=20 ORCA_SAMPLE_SECONDS=0 ORCA_WATCHDOG_SECONDS=0
export ORCA_FANOUT_SLEEP=4 ORCA_FANOUT_SHOW_HOST=1 DISTA_SHM=0
export ORCA_BOOT_ADVERTISE=127.0.0.1
unset ORCA_RUNNER ORCA_CONFIG ORCA_WORKER_ADVERTISE
free_port hport
free_port uport
base="http://127.0.0.1:$uport"
export ORCA_HCLOUD_ENDPOINT="http://127.0.0.1:$hport/v1" ORCA_HCLOUD_TOKEN=test-token
export ORCA_REGISTRY_ENDPOINT="http://127.0.0.1:$hport" ORCA_WORKER_IMAGE="ghcr.io/magmalake/orca:t1"
pids=
cleanup() { for p in $pids; do kill "$p" 2> /dev/null || true; done; }
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -20 "$log"
    tail -20 build/pool-spread-sched.out 2> /dev/null || true
    for f in "$dir"/server-*.log; do [ -f "$f" ] && { echo "== $f"; tail -8 "$f"; }; done
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
is() { [ "$(q "$1")" = "$2" ]; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
env -u ORCA_DATABASE_URL python3 tests/fake_hcloud.py "$hport" "$dir" &
pids="$pids $!"
until_ 10 "the fake didn't start" curl -s -o /dev/null "$ORCA_HCLOUD_ENDPOINT/server_types"
echo "cax31 hel1" > "$dir/stock"
ORCA_UI_ADDR="127.0.0.1:$uport" ./build/orca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "orca ui didn't start" curl -fs "$base/" -o /dev/null
cat > build/pool-spread.toml << 'EOF'
[pools.t]
provider = "hetzner"
machines = [{ type = "cax31" }]
locations = ["hel1"]
max_machines = 2
idle_seconds = 10

[pipelines.fanout]
placement = "worker:t"
workers = 1
EOF
./build/orca register --config build/pool-spread.toml ./build/fanout >> "$log" 2>&1 || fail "register"
ORCA_POOL_REMOTE="$base" ORCA_POOL_WORKER_COMMAND="$PWD/build/orca" \
ORCA_POOL_SCALE_SECONDS=0.5 ORCA_POOL_GROW_AFTER=0 ORCA_POOL_TRY_SECONDS=0.5 ORCA_POOL_IMAGE_SECONDS=0 \
    ./build/orca scheduler > build/pool-spread-sched.out 2>&1 &
pids="$pids $!"

./build/orca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill"
run=$(q "SELECT max(id) FROM orca.runs")
head_known() { [ -n "$(q "SELECT coalesce(cluster_head, '') FROM orca.runs WHERE id = $run")" ]; }
until_ 60 "no head: $(q "SELECT status, held FROM orca.runs WHERE id = $run") / $(q "SELECT status FROM orca.pools")" head_known
echo "ok   the first machine is made for the run and starts it as a cluster"

two() { is "SELECT count(*) FROM orca.pool_machines" 2; }
until_ 60 "no second machine: $(q "SELECT status FROM orca.pools")" two
member() { is "SELECT count(*) FROM orca.run_members WHERE run = $run" 1; }
until_ 60 "the second machine didn't join the run" member
nets=$(jq -r 'select(.method == "POST" and .path == "/v1/networks") | .path' "$dir/calls.jsonl" | wc -l | tr -d ' ')
[ "$nets" = 1 ] || fail "networks made: $nets"
attached=$(jq -r 'select(.method == "POST" and .path == "/v1/servers") | .body | fromjson | .networks | length' "$dir/calls.jsonl" | sort -u | tr '\n' ' ')
[ "$attached" = "1 " ] || fail "servers not all on the network: $attached"
echo "ok   the run's want of a member grows a second machine, on the same private network, which joins"

until_ 120 "the run didn't succeed: $(q "SELECT status, error FROM orca.runs WHERE id = $run")" \
    is "SELECT status FROM orca.runs WHERE id = $run" succeeded
ran_on=$(cat "$dir"/boot-*/stage/fanout-"$run"-*.log 2> /dev/null | grep -o "trips on orca-pool-t-[0-9]*" | sort -u | wc -l | tr -d ' ')
[ "$ran_on" = 2 ] || fail "tasks ran on $ran_on machines"
# The driver's log is kept once its process has ended, a moment after.
logged() { grep -q "trips in 1 months" "$ORCA_LOGS/fanout/$run/driver.log" 2> /dev/null; }
until_ 30 "no total in run $run's driver log" logged
spread=$(grep -ho "[0-9]* trips in 1 months" "$ORCA_LOGS/fanout/$run/driver.log" | head -1)
want=$(awk 'BEGIN { t = 0; for (d = 1; d <= 31; d++) t += (2024 * 13 + 31 + d * 7) % 1000; print t " trips in 1 months" }')
[ "$spread" = "$want" ] || fail "spread: '$spread', one machine: '$want'"
echo "ok   both machines run its tasks, and the total is one machine's ($spread)"

gone() { is "SELECT count(*) FROM orca.pool_machines WHERE ended IS NULL" 0; }
until_ 60 "the machines weren't removed: $(q "SELECT name, ended, why FROM orca.pool_machines")" gone
echo "ok   idle once the run ends, both machines are removed ($(q "SELECT string_agg(DISTINCT why, ', ') FROM orca.pool_machines"))"
echo "pool spread: ok"
