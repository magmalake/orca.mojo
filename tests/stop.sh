#!/bin/sh
# Stopping a step that won't end (#68), over tests/stop_pipeline.mojo:
#
# - an attempt past its step's timeout_seconds is failed ("timed out"), its
#   run failed with that as its error, and its process exits within a few
#   beats of the timeout, not when the step would have ended;
# - a cancel of a running run whose step is still going after
#   MORCA_CANCEL_KILL_SECONDS ends the run `cancelled` and its process;
# - both leave an orca.audit row saying so;
# - a local process the scheduler started whose heartbeat went stale (here
#   stopped with SIGSTOP) is killed by the scheduler, not left beside its
#   run's next start.
#
# Needs MORCA_DATABASE_URL (pixi run test-stop starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC tests/stop_pipeline.mojo -o build/stop
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/stop.log
: > "$log"
export MORCA_STALE_SECONDS=2
sched=
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$pid" ] || kill -9 "$pid" 2> /dev/null || true
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
# Waits up to $2 tenths of a second for the SQL $1 to say t.
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt "$2" ] || fail "waited for: $1"; sleep 0.1
    done
}
step_running() {
    until_true "SELECT count(*) = 1 FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.step = '$1' AND a.status = 'running'" 300
}

# A timeout: slow would sleep a minute, and is stopped after 2 s.
start=$(date +%s)
code=0
STOP_SLOW=60 ./build/stop run --partitions 2024-01 >> "$log" 2>&1 || code=$?
took=$(($(date +%s) - start))
[ "$code" -eq 1 ] || fail "the timed-out run exited $code, not 1"
[ "$took" -lt 20 ] || fail "the timed-out run took $took s"
run=$(q "SELECT max(id) FROM orca.runs")
[ "$(q "SELECT status || ' ' || error FROM orca.runs WHERE id = $run")" \
    = "failed step slow 2024-01 timed out after 2 s (timeout_seconds), so its process was stopped" ] \
    || fail "run $run: $(q "SELECT status, error FROM orca.runs WHERE id = $run")"
[ "$(q "SELECT a.status || ' ' || s.status FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")" = "failed failed" ] \
    || fail "slow's attempt: $(q "SELECT a.status, a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")"
[ "$(q "SELECT count(*) FROM orca.audit WHERE run = $run AND username = 'morca' AND action = 'stop' AND detail LIKE '%timed out after 2 s%'")" -eq 1 ] \
    || fail "no audit row for the timeout"

# A cancel: wait has no timeout, and is stopped a second after the cancel.
./build/morca register ./build/stop >> "$log" 2>&1 || fail "register"
STOP_WAIT=60 ./build/stop run --partitions 2024-02 >> "$log" 2>&1 &
pid=$!
step_running wait
run=$(q "SELECT max(id) FROM orca.runs")
MORCA_CANCEL_KILL_SECONDS=1 ./build/morca cancel stopping @all >> "$log" 2>&1 || fail "morca cancel"
[ "$(q "SELECT cancel_at > now() FROM orca.runs WHERE id = $run")" = t ] || fail "the cancel has no time to stop it"
code=0
wait "$pid" || code=$?
pid=
[ "$code" -eq 1 ] || fail "the cancelled run exited $code"
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = cancelled ] \
    || fail "run $run is $(q "SELECT status FROM orca.runs WHERE id = $run")"
[ "$(q "SELECT a.status FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND s.step = 'wait'")" = failed ] \
    || fail "wait's attempt wasn't ended"
[ "$(q "SELECT count(*) FROM orca.audit WHERE run = $run AND action = 'stop' AND detail LIKE '%cancelled:%'")" -eq 1 ] \
    || fail "no audit row for the stop"

# A wedged process of the scheduler's: killed once its heartbeat is stale.
STOP_WAIT=60 ./build/morca scheduler >> "$log" 2>&1 &
sched=$!
./build/morca backfill stopping 2024-03 >> "$log" 2>&1 || fail "backfill"
step_running wait
run=$(q "SELECT max(id) FROM orca.runs")
pid=$(q "SELECT split_part(owner, ':', 2) FROM orca.runs WHERE id = $run")
kill -STOP "$pid"
until_true "SELECT status = 'failed' FROM orca.runs WHERE id = $run" 300
i=0
while kill -0 "$pid" 2> /dev/null; do
    i=$((i + 1)); [ "$i" -lt 50 ] || fail "pid $pid of run $run is still there"; sleep 0.1
done
pid=
grep -q "killed pid [0-9]* of stopping run $run (its heartbeat went stale)" "$log" || fail "the kill wasn't said"
kill "$sched"
sched=

# A timeout on SQLite (morca.sqlite_store): the same stop, against the file.
db=build/stop.db
rm -f "$db" "$db-wal" "$db-shm"
code=0
MORCA_DATABASE_URL="sqlite:$db" STOP_SLOW=60 ./build/stop run --partitions 2024-01 >> "$log" 2>&1 || code=$?
[ "$code" -eq 1 ] || fail "the SQLite run exited $code"
[ "$(sqlite3 "$db" "SELECT status || '|' || error FROM runs")" \
    = "failed|step slow 2024-01 timed out after 2 s (timeout_seconds), so its process was stopped" ] \
    || fail "the SQLite run: $(sqlite3 "$db" "SELECT status, error FROM runs")"
[ "$(sqlite3 "$db" "SELECT status FROM attempts")" = failed ] || fail "the SQLite attempt"
echo "ok   stop"
