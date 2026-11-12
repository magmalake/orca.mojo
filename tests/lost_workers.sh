#!/bin/sh
# What dista recovers from dead workers, and what's left to morca (DESIGN.md,
# "Failure and resume"), over examples/fanout.mojo, whose step waits on 31
# dista tasks of MORCA_FANOUT_SLEEP seconds each over 4 local workers:
#
# - One worker killed mid-step: dista sends its tasks to the others, and the
#   run succeeds with the total of an undisturbed run.
# - Every worker killed mid-step: the step fails with dista's error, its
#   retry in the same run fails at once with "no workers left" rather than
#   waiting for ever, and the run fails quickly. Resumed as a new process,
#   with a fresh runtime, it succeeds with the same total.
#
# Needs MORCA_DATABASE_URL (pixi run test-lost-workers starts a throwaway
# server). A local runtime forks its head first and then its workers, so a
# run's workers are its child processes after the lowest-numbered one.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout
log=build/lost-workers.log
: > "$log"
export MORCA_WORKERS=4
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
total() { sed -n 's/.*\[month_trips 2024-01\] 31 days, \([0-9]*\) trips.*/\1/p' "$1" | tail -1; }

# The total of an undisturbed run, in memory.
MORCA_DATABASE_URL= ./build/fanout run --partitions 2024-01 > "$log.base" 2>&1 || fail "the undisturbed run"
want=$(total "$log.base")
[ -n "$want" ] || fail "no total in the undisturbed run"

# start_slow: a run in the background whose step is waiting on its tasks;
# sets $pid and $run.
start_slow() {
    MORCA_FANOUT_SLEEP=0.4 ./build/fanout run --partitions 2024-01 > "$log.run" 2>&1 &
    pid=$!
    i=0
    # Quietly: the run creates the schema as it starts.
    until [ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id = s.run WHERE r.status = 'running' AND a.status = 'running'" 2> /dev/null)" = 1 ]; do
        i=$((i + 1)); [ "$i" -lt 200 ] || fail "the step didn't start"; sleep 0.05
    done
    run=$(q "SELECT max(id) FROM orca.runs")
    sleep 0.6  # some tasks done, more running and queued
}
workers() { pgrep -P "$pid" | sort -n | tail -n +2; }

# One worker killed: dista reruns its tasks elsewhere.
start_slow
[ "$(workers | wc -l)" -eq 4 ] || fail "not 4 workers: $(pgrep -P "$pid" | tr '\n' ' ')"
kill -9 "$(workers | head -1)"
code=0; wait "$pid" || code=$?
cat "$log.run" >> "$log"
[ "$code" -eq 0 ] || fail "the run that lost one worker exited $code"
[ "$(total "$log.run")" = "$want" ] || fail "the run that lost one worker totalled $(total "$log.run"), not $want"

# Every worker killed: the step fails, its retry fails fast, the run fails.
start_slow
kill -9 $(workers)
t0=$(date +%s)
code=0; wait "$pid" || code=$?
took=$(($(date +%s) - t0))
cat "$log.run" >> "$log"
[ "$code" -eq 1 ] || fail "the run that lost every worker exited $code, not 1"
[ "$took" -lt 20 ] || fail "the run that lost every worker took $took s to fail"
errors=$(q "SELECT a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND s.step = 'month_trips' ORDER BY a.attempt")
[ "$(echo "$errors" | wc -l)" -eq 2 ] || fail "not 2 attempts: $errors"
echo "$errors" | head -1 | grep -qE 'lost|no workers left' || fail "attempt 1's error: $errors"
echo "$errors" | tail -1 | grep -qF 'no workers left' || fail "attempt 2's error: $errors"
[ "$(q "SELECT status FROM orca.steps WHERE run = $run AND step = 'all_trips'")" = skipped ] \
    || fail "all_trips wasn't skipped"

# Resumed as a new process: a fresh runtime with every worker.
./build/fanout resume "$run" > "$log.run" 2>&1 || { cat "$log.run" >> "$log"; fail "the resume"; }
cat "$log.run" >> "$log"
[ "$(total "$log.run")" = "$want" ] || fail "the resumed run totalled $(total "$log.run"), not $want"
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "the resumed run didn't succeed"
rm -f "$log.base" "$log.run"
echo "ok   lost workers"
