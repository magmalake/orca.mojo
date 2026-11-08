#!/bin/sh
# `orca runner` (docs/runner.md): orca ui, a scheduler with
# ORCA_LOCAL_RUNNER=runner and a runner, over examples/toy.mojo with every
# step taking ORCA_TOY_SLEEP seconds:
#
# - with no runner, the scheduler starts nothing: a queued run is held
#   "waiting for the runner";
# - the runner claims the run the scheduler assigned (audited) and starts
#   it as its own child;
# - killing the scheduler and the UI (their exact pids) mid-run and starting
#   new ones leaves the run alone: it succeeds with one start and one
#   attempt per step, recorded once;
# - killing the runner alone (a run that outlives its runner): the run goes
#   on, is left alone by the scheduler, and succeeds with one start;
# - killing the runner and its run's process group (a runner container that
#   stops): the run is queued again by the dead-worker rule, audited, waits
#   for the runner, and succeeds on the next one with a second start;
# - a control-placed run goes through the runner too, at the control
#   placement's nice.
#
# Needs ORCA_DATABASE_URL (pixi run test-runner starts a throwaway server),
# curl.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
b2=$!
wait "$b1"
wait "$b2"
log=build/runner.log
: > "$log"
export ORCA_LOGS="$PWD/build/runner-logs"
rm -rf "$ORCA_LOGS" build/runner-stage build/runner-*.out
export ORCA_STALE_SECONDS=3 ORCA_START_SECONDS=10 ORCA_SAMPLE_SECONDS=0 ORCA_WATCHDOG_SECONDS=0
export ORCA_NOW=2024-01-15T00:00Z ORCA_LOCAL_RUNNER=runner ORCA_TOY_SLEEP=2
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/runner-*.out; do [ -f "$f" ] && { echo "== $f"; tail -15 "$f"; }; done
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
# until_ SECONDS WHAT CONDITION...: waits for the condition, or fails.
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
is() { [ "$(q "$1")" = "$2" ]; }
runs() { q "SELECT id, status, starts, owner, worker, held, error FROM orca.runs ORDER BY id"; }
n=0
scheduler() {
    n=$((n + 1))
    ./build/orca scheduler > "build/runner-sched$n.out" 2>&1 &
    sched=$!
    pids="$pids $sched"
}
ui() {
    ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
    uipid=$!
    pids="$pids $uipid"
    until_ 10 "orca ui didn't start" curl -fs "http://127.0.0.1:$port/" -o /dev/null
}
m=0
runner() {
    m=$((m + 1))
    ./build/orca runner --poll-seconds 0.3 --dir "$PWD/build/runner-stage" > "build/runner-runner$m.out" 2>&1 &
    rpid=$!
    pids="$pids $rpid"
}
# done_once RUN STARTS: the run succeeded with STARTS starts and one
# succeeded attempt for each of its three steps.
done_once() {
    is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $1" "succeeded $2" \
        && is "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $1 AND a.status = 'succeeded'" 3
}
# pid_of RUN: the pid the runner started RUN's latest start with.
pid_of() {
    cat build/runner-runner*.out | sed -n "s/^started toy run $1 start [0-9]* pid \([0-9]*\)$/\1/p" | tail -1
}

./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
free_port port
ui
scheduler

# --- No runner: nothing starts, and the run says why ---
./build/orca backfill toy 2024-03 >> "$log" 2>&1 || fail "backfill"
r1=$(q "SELECT max(id) FROM orca.runs")
until_ 10 "the run isn't held for the runner: $(runs)" \
    is "SELECT held FROM orca.runs WHERE id = $r1" "held: waiting for the runner (ORCA_LOCAL_RUNNER=runner)"
is "SELECT starts FROM orca.runs WHERE id = $r1" 0 || fail "the scheduler started a run: $(runs)"
echo "ok   with no runner, runs wait for it"

# --- The runner claims what the scheduler assigns ---
runner
until_ 20 "the runner didn't start the run: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE id = $r1 AND status = 'running' AND heartbeat IS NOT NULL AND owner LIKE '%:%'" 1
rid=$(q "SELECT id FROM orca.workers WHERE pool = 'local'")
[ -n "$rid" ] || fail "the runner has no orca.workers row"
is "SELECT worker || ' ' || placement || ' ' || starts FROM orca.runs WHERE id = $r1" "$rid local 1" \
    || fail "the run's runner: $(runs)"
is "SELECT count(*) FROM orca.audit WHERE run = $r1 AND action = '/api/worker/claim' AND source = 'runner'" 1 \
    || fail "the claim's audit: $(q "SELECT * FROM orca.audit")"
p1=$(pid_of "$r1")
[ -n "$p1" ] || fail "the runner didn't say what it started: $(cat build/runner-runner1.out)"
q "SELECT owner FROM orca.runs WHERE id = $r1" | grep -q ":$p1\$" || fail "the run's owner isn't pid $p1"
[ "$(ps -o ppid= -p "$p1" | tr -d ' ')" = "$rpid" ] || fail "the run's process isn't the runner's child"
grep -q "assigned toy run $r1 to the runner" build/runner-sched1.out || fail "the scheduler didn't say it assigned the run"
echo "ok   the runner claims and starts the run the scheduler assigned, audited"

# --- The scheduler and the UI restart: the run is left alone ---
kill -9 "$sched" "$uipid"
wait "$sched" "$uipid" 2> /dev/null || true
sleep 4  # past the stale limit
scheduler
ui
until_ 40 "the run didn't finish once: $(runs)" done_once "$r1" 1
kill -0 "$rpid" || fail "the runner exited"
grep -q "toy run $r1" "build/runner-sched2.out" && fail "the new scheduler touched the run: $(cat build/runner-sched2.out)"
is "SELECT count(*) FROM orca.audit WHERE run = $r1 AND action = '/api/worker/requeue'" 0 || fail "the run was requeued"
until_ 10 "the run's driver log isn't kept" test -s "$ORCA_LOGS/toy/$r1/driver.log"
echo "ok   a scheduler and UI restarted mid-run leave it alone: it succeeds once"

# --- The runner stops alone: its run outlives it, and is left alone ---
./build/orca backfill toy 2024-04 >> "$log" 2>&1 || fail "backfill"
r2=$(q "SELECT max(id) FROM orca.runs")
until_ 20 "the runner didn't start run $r2: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE id = $r2 AND heartbeat IS NOT NULL AND owner LIKE '%:%'" 1
kill -9 "$rpid"
wait "$rpid" 2> /dev/null || true
until_ 40 "the run that outlived its runner didn't finish once: $(runs)" done_once "$r2" 1
echo "ok   a run that outlives its runner is left alone and succeeds once"

# --- The runner and its run stop: queued again, and run by the next ---
runner
./build/orca backfill toy 2024-05 >> "$log" 2>&1 || fail "backfill"
r3=$(q "SELECT max(id) FROM orca.runs")
until_ 20 "the runner didn't start run $r3: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE id = $r3 AND heartbeat IS NOT NULL AND owner LIKE '%:%'" 1
p3=$(pid_of "$r3")
[ -n "$p3" ] || fail "no pid for run $r3"
kill -9 "$rpid"
kill -9 -- "-$p3"
wait "$rpid" 2> /dev/null || true
until_ 20 "the run wasn't queued again: $(runs)" \
    is "SELECT count(*) FROM orca.audit WHERE run = $r3 AND action = '/api/worker/requeue'" 1
d=$(q "SELECT detail FROM orca.audit WHERE run = $r3 AND action = '/api/worker/requeue'")
case "$d" in "queued again: worker runner (token $rid) stopped: not seen for "*) ;; *) fail "the requeue's audit: $d" ;; esac
until_ 10 "the requeued run doesn't wait for the runner: $(runs)" \
    is "SELECT status || ' ' || held FROM orca.runs WHERE id = $r3" "queued held: waiting for the runner (ORCA_LOCAL_RUNNER=runner)"
runner
until_ 40 "the requeued run didn't succeed: $(runs)" done_once "$r3" 2
is "SELECT count(*) FROM orca.workers WHERE pool = 'local'" 1 || fail "a restarted runner made a second row"
echo "ok   a runner's run that stopped with it is queued again and succeeds on the next runner"

# --- A control-placed run goes through the runner ---
./build/orca register --placement control ./build/toy >> "$log" 2>&1 || fail "register --placement control"
./build/orca backfill toy 2024-06 >> "$log" 2>&1 || fail "backfill"
r4=$(q "SELECT max(id) FROM orca.runs")
until_ 20 "the runner didn't start run $r4: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE id = $r4 AND heartbeat IS NOT NULL AND owner LIKE '%:%'" 1
p4=$(pid_of "$r4")
[ "$(ps -o ni= -p "$p4" | tr -d ' ')" = 10 ] || fail "the control run's nice: $(ps -o ni= -p "$p4")"
until_ 40 "the control run didn't succeed: $(runs)" done_once "$r4" 1
is "SELECT placement FROM orca.runs WHERE id = $r4" control || fail "its placement: $(runs)"
echo "ok   a control-placed run is started by the runner, at the control nice"

# The reserved pool.
code=0
./build/orca worker-token create --pool local >> "$log" 2>&1 || code=$?
[ "$code" != 0 ] || fail "a worker token for pool local"
code=0
./build/orca register --placement worker:local ./build/toy >> "$log" 2>&1 || code=$?
[ "$code" != 0 ] || fail "placement worker:local"
echo "ok   the pool local is the runner's alone"
