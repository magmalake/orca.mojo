#!/bin/sh
# Admission by memory pressure (orca.pressure, orca.admission), over
# tests/pressure/presser.mojo on the local runner, with the PSI file a
# fixture the test writes (ORCA_MEMORY_PRESSURE_FILE) and the limit 10%:
#
# - while `some avg10` reads 50%, a queued run waits, held "waiting: memory
#   pressure 50% (limit 10%)", which its page, the pipeline's, their JSON,
#   the home and runs pages, Ops > Control, /metrics and `orca status` say;
# - at 8% (under the limit, over half of it) it still waits, easing;
# - at 1% it starts, and its process is given the file
#   (ORCA_MEMORY_PRESSURE_FILE) and reads it (orca.memory_pressure);
# - a hold with nothing of orca's going lets runs start one at a time once
#   its grace (ORCA_ADMIT_PRESSURE_GRACE_SECONDS) has passed, said once;
# - a file that can't be read holds nothing;
# - a remote worker claims nothing while its own host's pressure is high,
#   its pool's run saying so, and runs it once it eases.
#
# Needs ORCA_DATABASE_URL (pixi run test-pressure starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC tests/pressure/presser.mojo -o build/presser
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/pressure.log
: > "$log"
out=$PWD/build/pressure-out
psi=$PWD/build/pressure-psi
rm -rf "$out" build/pressure-stage build/pressure-*.out
mkdir -p "$out"
export PRESSER_OUT="$out" PRESSER_SLEEP=2
export ORCA_STALE_SECONDS=5 ORCA_START_SECONDS=20 ORCA_SAMPLE_SECONDS=0 ORCA_WATCHDOG_SECONDS=0
export ORCA_WORKERS=1 ORCA_MEMORY_PRESSURE_FILE="$psi" ORCA_ADMIT_MEMORY_PRESSURE=10
unset ORCA_RUNNER ORCA_CONFIG ORCA_ADMIT_PRESSURE_GRACE_SECONDS
pids=
sched=
cleanup() {
    for p in $pids $sched; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/pressure-*.out; do [ -f "$f" ] && { echo "== $f"; tail -15 "$f"; }; done
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
is() { [ "$(q "$1")" = "$2" ]; }
# set_psi FILE AVG10: a PSI file reading AVG10 (some), written whole.
set_psi() {
    printf 'some avg10=%s avg60=%s avg300=1.00 total=123456\nfull avg10=0.50 avg60=0.25 avg300=0.10 total=4567\n' \
        "$2" "$2" > "$1.tmp"
    mv "$1.tmp" "$1"
}
start_sched() {
    ./build/orca scheduler >> build/pressure-sched.out 2>&1 &
    sched=$!
}
stop_sched() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$sched" ] || wait "$sched" 2> /dev/null || true
    sched=
}

./build/orca register --no-config ./build/presser >> "$log" 2>&1 || fail "register presser"
free_port port
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "orca ui didn't start" curl -fs "$base/" -o /dev/null
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }

# High: the run waits, and everything says why.
set_psi "$psi" 50.00
start_sched
./build/orca backfill presser 2024-01 >> "$log" 2>&1 || fail "backfill presser"
r1=$(q "SELECT max(id) FROM orca.runs")
want="waiting: memory pressure 50% (limit 10%)"
until_ 20 "the run wasn't held: $(q "SELECT status, held FROM orca.runs WHERE id = $r1")" \
    is "SELECT status || ' ' || starts || ' ' || held FROM orca.runs WHERE id = $r1" "queued 0 $want"
grep -q "memory pressure from $psi" build/pressure-sched.out || fail "the scheduler didn't say what it reads"
grep -q "$want - no new runs start on this host" build/pressure-sched.out || fail "the scheduler didn't say it holds"
json "/runs/$r1" | jq -e --arg w "$want" '.run.held == $w' > /dev/null || fail "the run's JSON: $(json "/runs/$r1" | jq -c .run)"
json /pipelines/presser | jq -e --arg w "$want" '.pipeline.waiting == $w' > /dev/null \
    || fail "the pipeline's JSON: $(json /pipelines/presser | jq -c .pipeline)"
curl -fsS "$base/runs/$r1" | grep -q 'memory pressure 50% (limit 10%)' || fail "the run's page doesn't say why it waits"
curl -fsS "$base/pipelines/presser" | grep -q 'memory pressure 50% (limit 10%)' || fail "the pipeline's page doesn't say why it waits"
until_ 10 "Control's JSON: $(json /api/system | jq -c .memory_pressure)" \
    sh -c "curl -fsS '$base/api/system' | jq -e --arg w '$want' '.memory_pressure.held == \$w and .memory_pressure.some.avg10 == 50 and .memory_pressure.kind == \"file\" and .memory_pressure.limit == 10' > /dev/null"
curl -fsS "$base/ops/control" | grep -q 'Memory pressure' || fail "Control has no Memory pressure"
curl -fsS "$base/ops/control" | grep -q 'holding' || fail "Control doesn't say it holds"
json / | jq -e '.memory_pressure.held != null' > /dev/null || fail "the home JSON: $(json / | jq -c .memory_pressure)"
curl -fsS "$base/" | grep -q 'No new runs start on the scheduler&#39;s host\|No new runs start on the scheduler.s host' \
    || fail "the home page doesn't warn"
curl -fsS "$base/runs" | grep -q 'No new runs start on the scheduler' || fail "the runs page doesn't warn"
curl -fsS "$base/metrics" > build/pressure-metrics.out || fail "GET /metrics"
grep -q '^orca_memory_pressure_some_avg10{host="scheduler"} 50' build/pressure-metrics.out \
    || fail "/metrics: $(grep pressure build/pressure-metrics.out)"
grep -q '^orca_admission_memory_pressure_held{host="scheduler"} 1' build/pressure-metrics.out \
    || fail "/metrics: $(grep pressure build/pressure-metrics.out)"
./build/orca status > build/pressure-status.out 2>&1 || fail "orca status"
grep -q "^memory pressure: 50% (avg10; 50% avg60): $want$" build/pressure-status.out \
    || fail "orca status: $(cat build/pressure-status.out)"
echo "ok   while pressure is over the limit the run waits, and every page says why"

# Under the limit but over half of it: still waiting, easing.
set_psi "$psi" 8.00
until_ 10 "the run isn't easing: $(q "SELECT status, held FROM orca.runs WHERE id = $r1")" \
    is "SELECT status || ' ' || held FROM orca.runs WHERE id = $r1" "queued waiting: memory pressure 8%, easing (limit 10%)"
sleep 2
is "SELECT status FROM orca.runs WHERE id = $r1" queued || fail "the run started at 8%"
echo "ok   under the limit but over half of it, the run still waits"

# Under half: it starts, given the file.
set_psi "$psi" 1.00
until_ 30 "the run didn't succeed: $(q "SELECT status, held, error FROM orca.runs WHERE id = $r1")" \
    is "SELECT status FROM orca.runs WHERE id = $r1" succeeded
is "SELECT coalesce(held, 'none') FROM orca.runs WHERE id = $r1" none || fail "the run kept its reason"
printf 'ORCA_MEMORY_PRESSURE_FILE=%s\nsome_avg10=1.0\n' "$psi" > build/pressure-want.out
cmp -s build/pressure-want.out "$out/2024-01.env" || fail "the run's process got: $(cat "$out/2024-01.env")"
grep -q "runs start on this host again" build/pressure-sched.out || fail "the scheduler didn't say it resumed"
until_ 10 "/metrics still holds" sh -c "curl -fsS '$base/metrics' | grep -q '^orca_admission_memory_pressure_held{host=\"scheduler\"} 0'"
json /api/system | jq -e '.memory_pressure.held == null' > /dev/null || fail "Control still holds"
echo "ok   once pressure eases the run starts, and its process can read it"

# A hold with nothing of orca's going: one at a time after the grace.
stop_sched
./build/orca register --no-config --max-runs 2 ./build/presser >> "$log" 2>&1 || fail "register presser --max-runs 2"
set_psi "$psi" 50.00
export ORCA_ADMIT_PRESSURE_GRACE_SECONDS=3
start_sched
./build/orca backfill presser 2024-02..2024-03 >> "$log" 2>&1 || fail "backfill presser"
most=0
i=0
while ! is "SELECT count(*) FROM orca.runs WHERE status <> 'succeeded'" 0; do
    i=$((i + 1)); [ "$i" -lt 600 ] || fail "the runs didn't end: $(q "SELECT id, status, held FROM orca.runs")"
    n=$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")
    [ "$n" -le "$most" ] || most=$n
    sleep 0.1
done
[ "$most" -eq 1 ] || fail "$most runs ran at once under a hold past its grace"
[ "$(grep -c 'with none of orca.s going' build/pressure-sched.out)" -eq 1 ] \
    || fail "the scheduler didn't say once that it starts one at a time: $(grep 'one at a time' build/pressure-sched.out)"
stop_sched
unset ORCA_ADMIT_PRESSURE_GRACE_SECONDS
echo "ok   a hold with nothing of orca's to wait for starts runs one at a time"

# A file that can't be read holds nothing.
export ORCA_MEMORY_PRESSURE_FILE="$PWD/build/pressure-none"
rm -f "$PWD/build/pressure-none"
start_sched
./build/orca backfill presser 2024-04 >> "$log" 2>&1 || fail "backfill presser"
r4=$(q "SELECT max(id) FROM orca.runs")
until_ 30 "the run didn't succeed without a PSI file: $(q "SELECT status, held FROM orca.runs WHERE id = $r4")" \
    is "SELECT status FROM orca.runs WHERE id = $r4" succeeded
grep -q "can't read memory pressure from $PWD/build/pressure-none" build/pressure-sched.out \
    || fail "the scheduler didn't say it can't read the file"
stop_sched
export ORCA_MEMORY_PRESSURE_FILE="$psi"
echo "ok   a pressure file that can't be read holds nothing"

# A remote worker, against its own host's pressure.
./build/orca register --no-config --placement worker:p ./build/presser >> "$log" 2>&1 || fail "register presser on a worker"
tok=$(./build/orca worker-token create --pool p --label pressure 2>> "$log") || fail "worker-token create"
start_sched
wpsi=$PWD/build/pressure-worker-psi
set_psi "$wpsi" 40.00
./build/orca backfill presser 2024-05 >> "$log" 2>&1 || fail "backfill presser"
r5=$(q "SELECT max(id) FROM orca.runs")
env -u ORCA_DATABASE_URL ORCA_MEMORY_PRESSURE_FILE="$wpsi" ORCA_WORKER_TOKEN="$tok" \
    ./build/orca worker --pool p --remote "$base" --poll-seconds 0.3 --name w1 \
    --dir "$PWD/build/pressure-stage" > build/pressure-worker.out 2>&1 &
pids="$pids $!"
wwant="waiting: memory pressure 40% (limit 10%) on worker w1"
until_ 20 "the worker's run wasn't held: $(q "SELECT status, held FROM orca.runs WHERE id = $r5")" \
    is "SELECT status || ' ' || held FROM orca.runs WHERE id = $r5" "queued $wwant"
json /api/workers | jq -e '.workers[0].memory_pressure.some.avg10 == 40 and .workers[0].memory_pressure.held != null' > /dev/null \
    || fail "the workers' JSON: $(json /api/workers | jq -c '.workers[0]')"
curl -fsS "$base/metrics" | grep -q '^orca_admission_memory_pressure_held{host="worker:[0-9]*"} 1' \
    || fail "/metrics has no worker hold"
sleep 1
is "SELECT status FROM orca.runs WHERE id = $r5" queued || fail "the worker claimed the run under pressure"
set_psi "$wpsi" 1.00
until_ 60 "the run didn't succeed on the worker: $(q "SELECT id, status, error, held FROM orca.runs WHERE id = $r5")" \
    is "SELECT status FROM orca.runs WHERE id = $r5" succeeded
printf 'ORCA_MEMORY_PRESSURE_FILE=%s\nsome_avg10=1.0\n' "$wpsi" > build/pressure-want.out
cmp -s build/pressure-want.out "$out/2024-05.env" || fail "the worker's run got: $(cat "$out/2024-05.env")"
echo "ok   a worker claims nothing while its host's pressure is high, then runs it"
echo "pressure: ok"
