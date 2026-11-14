#!/bin/sh
# A run's CPU priority without cgroups (morca.run_cpu, #229): with
# MORCA_CGROUPS=0 a local run's process starts at nice MORCA_RUN_NICE (7
# here), which `ps -o ni` shows, and Ops > Control's JSON and `morca status`
# say so. Runs on macOS and Linux.
#
# Needs MORCA_DATABASE_URL (pixi run test-cgroups starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"

mkdir -p build
scripts/pipelines.sh grower &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
b2=$!
wait "$b1"
wait "$b2"
log=build/run-nice.log
: > "$log"
rm -f build/run-nice-*.out
export MORCA_CGROUPS=0 MORCA_RUN_NICE=7 MORCA_SAMPLE_SECONDS=0 MORCA_STALE_SECONDS=5
# Held long enough to look at its process; a little memory only.
export GROW_MB=8 GROW_SECONDS=30
pids=
pid=
cleanup() {
    for p in $pids $pid; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/run-nice-*.out; do [ -f "$f" ] && { echo "== $f"; tail -25 "$f"; }; done
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
is() { [ "$(q "$1")" = "$2" ]; }

free_port port
base="http://127.0.0.1:$port"
./build/morca register ./build/grower >> "$log" 2>&1 || fail "register"
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "morca ui didn't start" curl -fs "$base/" -o /dev/null
./build/morca scheduler >> build/run-nice-sched.out 2>&1 &
pids="$pids $!"
until_ 20 "the scheduler didn't say its runs' CPU priority" \
    grep -q "morca: runs' CPU priority: nice 7 (MORCA_RUN_NICE)" build/run-nice-sched.out

./build/morca backfill grower 2024-01 >> "$log" 2>&1 || fail "backfill"
r=$(q "SELECT max(id) FROM orca.runs")
until_ 30 "run $r didn't start" is "SELECT status FROM orca.runs WHERE id = $r" running
found() {
    pid=$(pgrep -f 'build/grower resume' | head -1)
    [ -n "$pid" ]
}
until_ 10 "no process of run $r" found
ni=$(ps -o ni= -p "$pid" | tr -d ' ')
[ "$ni" = 7 ] || fail "run $r's process (pid $pid) is at nice $ni, not 7"
# Whatever it starts inherits it: the run's forked workers too.
for c in $(pgrep -P "$pid" 2> /dev/null); do
    cni=$(ps -o ni= -p "$c" | tr -d ' ')
    [ -z "$cni" ] || [ "$cni" = 7 ] || fail "run $r's child $c is at nice $cni, not 7"
done

curl -fs -H 'Accept: application/json' "$base/ops/control" > build/run-nice-control.json \
    || fail "GET /ops/control"
jq -e ".runs[] | select(.run == $r and .nice == 7 and .cpu_weight == null)" \
    build/run-nice-control.json > /dev/null || fail "Ops > Control's runs: $(jq -c .runs build/run-nice-control.json)"
curl -fs "$base/ops/control" | grep -q '<td>nice 7</td>' || fail "Ops > Control's Runs card has no nice 7"
./build/morca status > build/run-nice-status.out 2>&1 || fail "morca status: $(cat build/run-nice-status.out)"
grep -q '^run CPU priority: cpu.weight 20 in a cgroup, else nice 7$' build/run-nice-status.out \
    || fail "morca status: $(cat build/run-nice-status.out)"
echo "run_nice: ok"
