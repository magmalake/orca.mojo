#!/bin/sh
# The pool autoscaler (orca.pool_scale, #267) end to end: a [pools.t] of
# hetzner machines against tests/fake_hcloud.py (each "server" a local
# process running its boot script, which asks orca ui for what it needs and
# runs `orca worker`), with plain (tests/grants/grantee.mojo) placed on it:
#
# - a type the worker image has no variant for is skipped, saying so;
# - with no stock of the first type, it keeps trying it for its wait, then
#   makes the next, in the first location that has it;
# - the machine boots, joins as a worker and runs the runs; with plain's
#   max_runs = 1 one machine is all the demand there is;
# - idle for idle_seconds, it's removed: token revoked, server deleted;
# - a server with the pool's and the stack's labels that orca doesn't know
#   is deleted;
# - /api/worker/boot answers only a token the autoscaler made.
#
# Needs ORCA_DATABASE_URL (pixi run test-pool-scale starts a throwaway
# server), curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC -D GRANTEE=plain tests/grants/grantee.mojo -o build/plain
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/pool-scale.log
: > "$log"
dir=$PWD/build/pool-scale
rm -rf "$dir"
mkdir -p "$dir"
export ORCA_WAREHOUSE="$dir/wh" ORCA_LOGS="$dir/logs" GRANTEE_OUT="$dir/out" GRANTEE_SLEEP=2
mkdir -p "$GRANTEE_OUT"
export ORCA_STALE_SECONDS=5 ORCA_START_SECONDS=20 ORCA_SAMPLE_SECONDS=0 ORCA_WATCHDOG_SECONDS=0
unset ORCA_RUNNER ORCA_CONFIG
free_port hport
free_port uport
base="http://127.0.0.1:$uport"
export ORCA_HCLOUD_ENDPOINT="http://127.0.0.1:$hport/v1" ORCA_HCLOUD_TOKEN=test-token
export ORCA_REGISTRY_ENDPOINT="http://127.0.0.1:$hport"
export ORCA_WORKER_IMAGE="ghcr.io/morca-dev/census:t1"
pids=
sched=
cleanup() {
    for p in $pids $sched; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    tail -20 build/pool-scale-sched.out 2> /dev/null || true
    for f in "$dir"/server-*.log; do [ -f "$f" ] && { echo "== $f"; tail -10 "$f"; }; done
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
status() { q "SELECT coalesce(status, '') FROM orca.pools WHERE name = 't'"; }
has_status() { status | grep -qF "$1"; }
servers() { curl -fsS -H 'Authorization: Bearer test-token' "$ORCA_HCLOUD_ENDPOINT/servers?label_selector=orca-pool%3Dt" | jq -r '[.servers[].name] | join(" ")'; }

# The fake's servers run workers, which refuse a database URL.
env -u ORCA_DATABASE_URL python3 tests/fake_hcloud.py "$hport" "$dir" &
pids="$pids $!"
until_ 10 "the fake didn't start" curl -s -o /dev/null "$ORCA_HCLOUD_ENDPOINT/server_types"
ORCA_UI_ADDR="127.0.0.1:$uport" ./build/orca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "orca ui didn't start" curl -fs "$base/" -o /dev/null

cat > build/pool-scale.toml << 'EOF'
[pools.t]
provider = "hetzner"
machines = [{ type = "cax31", wait = 3 }, { type = "cpx32" }]
locations = ["hel1", "nbg1"]
max_machines = 2
idle_seconds = 10

[pipelines.plain]
placement = "worker:t"
EOF
./build/orca register --config build/pool-scale.toml ./build/plain >> "$log" 2>&1 || fail "register"
grep -q '^pool t (hetzner)$' "$log" || fail "register didn't say it recorded pool t"
is "SELECT provider || ' ' || max_machines FROM orca.pools WHERE name = 't'" "hetzner 2" || fail "orca.pools"

start_sched() {
    ORCA_POOL_REMOTE="$base" ORCA_POOL_WORKER_COMMAND="$PWD/build/orca" \
    ORCA_POOL_SCALE_SECONDS=0.5 ORCA_POOL_GROW_AFTER=0 ORCA_POOL_TRY_SECONDS=0.5 ORCA_POOL_IMAGE_SECONDS=0 \
        ./build/orca scheduler >> build/pool-scale-sched.out 2>&1 &
    sched=$!
}
: > build/pool-scale-sched.out
start_sched

# The image has arm64 only: cpx32 (x86) is skipped, and cax31 is out of stock.
echo arm64 > "$dir/arches"
echo "cpx32 hel1" > "$dir/stock"
./build/orca backfill plain 2024-01..2024-03 >> "$log" 2>&1 || fail "backfill"
until_ 20 "no waiting status: $(status)" has_status "waiting: no cax31 in hel1, nbg1 yet"
has_status "cpx32 skipped: ghcr.io/morca-dev/census:t1 has no linux/amd64" || fail "the skip isn't said: $(status)"
[ -z "$(servers)" ] || fail "made a server: $(servers)"
echo "ok   a type the image has no variant for is skipped, saying so"

# Both architectures: cax31 for its 3 s, then cpx32, in hel1.
echo "amd64 arm64" > "$dir/arches"
until_ 30 "no cpx32 made: $(status)" has_status "made orca-pool-t-"
# Ops > Pools shows the pool: its types, its machine, the last try.
curl -fsS "$base/ops/pools" | grep -q 'id="autoscaled"' || fail "no Autoscaled pools card on Ops > Pools"
curl -fsS -H 'Accept: application/json' "$base/api/pools" > "$dir/pools.json" || fail "GET /api/pools"
jq -e '.autoscaled | length == 1 and .[0].name == "t" and .[0].provider == "hetzner" and .[0].live == 1 and .[0].max_machines == 2 and (.[0].machines | map(.type)) == ["cax31", "cpx32"] and .[0].machines[0].wait == 3 and (.[0].status | test("made orca-pool-t-"))' "$dir/pools.json" > /dev/null \
    || fail "the autoscaled pools JSON: $(jq -c .autoscaled "$dir/pools.json")"
has_status ": cpx32 in hel1" || fail "made the wrong one: $(status)"
is "SELECT server_type || ' ' || arch || ' ' || location FROM orca.pool_machines" "cpx32 amd64 hel1" \
    || fail "the machine: $(q "SELECT server_type, arch, location FROM orca.pool_machines")"
tried() { jq -r 'select(.method == "POST" and .path == "/v1/servers") | .body | fromjson | "\(.server_type) \(.location)"' "$dir/calls.jsonl" | sort -u | tr '\n' ' '; }
[ "$(tried)" = "cax31 hel1 cax31 nbg1 cpx32 hel1 " ] || fail "tried: $(tried)"
echo "ok   after cax31's wait with none in stock, cpx32 is made in hel1"

# It boots, joins and runs plain's three runs; max_runs = 1 needs one machine.
until_ 120 "the runs didn't succeed: $(q "SELECT id, status, held FROM orca.runs")" \
    is "SELECT count(*) FILTER (WHERE status = 'succeeded') FROM orca.runs" 3
is "SELECT count(*) FROM orca.pool_machines" 1 || fail "more machines than the demand: $(q "SELECT name FROM orca.pool_machines")"
m=$(q "SELECT name FROM orca.pool_machines")
is "SELECT count(DISTINCT r.worker) FROM orca.runs r JOIN orca.pool_machines m ON m.worker = r.worker" 1 \
    || fail "the runs didn't run on the machine"
echo "ok   the machine boots as a worker and runs the runs; one is all max_runs = 1 needs"

# Idle 10 s: removed.
until_ 40 "the idle machine wasn't removed: $(q "SELECT name, ended, why FROM orca.pool_machines")" \
    is "SELECT why FROM orca.pool_machines WHERE ended IS NOT NULL" "idle 10 s"
is "SELECT count(*) FROM orca.workers w JOIN orca.pool_machines m ON m.worker = w.id WHERE w.revoked IS NOT NULL" 1 \
    || fail "its token isn't revoked"
[ -z "$(servers)" ] || fail "its server is still there: $(servers)"
echo "ok   an idle machine is removed: token revoked, server deleted ($m)"

# A server with the pool's and the stack's labels that orca doesn't know.
owner=$(q "SELECT id FROM orca.stack_identity")
touch "$dir/no-run"
echo "cax31 hel1" > "$dir/stock"
curl -fsS -X POST -H 'Authorization: Bearer test-token' -H 'Content-Type: application/json' \
    -d "{\"name\": \"stray-1\", \"server_type\": \"cax31\", \"location\": \"hel1\", \"labels\": {\"orca-pool\": \"t\", \"orca-owner\": \"$owner\"}}" \
    "$ORCA_HCLOUD_ENDPOINT/servers" > /dev/null || fail "make a stray"
curl -fsS -X POST -H 'Authorization: Bearer test-token' -H 'Content-Type: application/json' \
    -d '{"name": "other-stack", "server_type": "cax31", "location": "hel1", "labels": {"orca-pool": "t", "orca-owner": "someone-else"}}' \
    "$ORCA_HCLOUD_ENDPOINT/servers" > /dev/null || fail "make another stack's"
only_other() { [ "$(servers)" = "other-stack" ]; }
until_ 20 "the stray wasn't deleted: $(servers)" only_other
rm -f "$dir/no-run"
echo "ok   a stray of this stack's is deleted, another stack's left alone"

# Over the project's quota for cax31: the next type, at once, saying why.
rm -f "$dir/no-run"
printf 'cax31 hel1\ncpx32 hel1\n' > "$dir/stock"
echo cax31 > "$dir/quota"
before=$(q "SELECT count(*) FROM orca.pool_machines")
./build/orca backfill plain 2024-04 >> "$log" 2>&1 || fail "backfill 2024-04"
# The status still says the last step's "made ...: cpx32 in hel1", so wait
# for this step's machine, then for the pass that made it to say why.
made_one() { is "SELECT count(*) FROM orca.pool_machines" $((before + 1)); }
until_ 30 "no machine after the quota refusal: $(status) / $(q "SELECT name, server_type FROM orca.pool_machines")" made_one
until_ 10 "the quota refusal isn't said: $(status)" has_status "cax31 refused for quota"
has_status ": cpx32 in hel1" || fail "not cpx32: $(status)"
rm -f "$dir/quota"
echo "ok   a type refused for the project's quota: the next is made at once, saying why"

# /api/worker/boot only for a token the autoscaler made.
tok=$(./build/orca worker-token create --pool t --label hand 2>> "$log") || fail "worker-token create"
code=$(curl -s -o "$dir/boot.out" -w '%{http_code}' -X POST -H "Authorization: Bearer $tok" \
    -H 'Content-Type: application/json' -d '{"pool": "t"}' "$base/api/worker/boot")
[ "$code" = 403 ] || fail "a hand-made token booted: $code $(cat "$dir/boot.out")"
echo "ok   /api/worker/boot refuses a token the autoscaler didn't make"
echo "pool scale: ok"
