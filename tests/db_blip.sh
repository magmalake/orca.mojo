#!/bin/sh
# A run outlives a restart of the Postgres server under it (#67). The toy
# pipeline's steps each sleep; the server is restarted (pg_ctl -m
# immediate) while a step works, and again while the next one does, so the
# writes that record their ends hit a dead connection. The run must go on,
# on new connections, and succeed in the same process with one attempt per
# step: nothing redone, nothing left running.
#
# Needs MORCA_DATABASE_URL and MORCA_PGDATA (pixi run test-db-blip starts a
# throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
scripts/pipelines.sh toy
log=build/db-blip.log
: > "$log"
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
restart() { pg_ctl -D "$MORCA_PGDATA" -m immediate -w restart > /dev/null 2>&1 || fail "pg_ctl restart"; }

MORCA_TOY_SLEEP=3 ./build/toy run --partitions 2024-01 >> "$log" 2>&1 &
toy=$!
sleep 1.5
restart
sleep 3
restart
code=0
wait "$toy" || code=$?
[ "$code" -eq 0 ] || fail "the run exited $code"
grep -qx "run succeeded" "$log" || fail "the run didn't succeed"
grep -q "Postgres failed" "$log" || fail "no write was retried, so the test tested nothing"
run=$(q "SELECT max(id) FROM orca.runs")
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "run $run is $(q "SELECT status FROM orca.runs WHERE id = $run")"
[ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")" -eq 3 ] \
    || fail "attempts: $(q "SELECT s.step, a.attempt, a.status, a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")"
[ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND a.status <> 'succeeded'")" -eq 0 ] \
    || fail "an attempt didn't succeed"

# A server that stays away longer than MORCA_DB_RETRY_SECONDS still stops
# the run, as before: exit 3, the run left for a resume.
MORCA_TOY_SLEEP=2 MORCA_DB_RETRY_SECONDS=2 ./build/toy run --partitions 2024-02 >> "$log" 2>&1 &
toy=$!
sleep 1
pg_ctl -D "$MORCA_PGDATA" -m immediate -w stop > /dev/null 2>&1 || fail "pg_ctl stop"
code=0
wait "$toy" || code=$?
pg_ctl -D "$MORCA_PGDATA" -w start > /dev/null 2>&1 || fail "pg_ctl start"
[ "$code" -eq 3 ] || fail "with the server gone, the run exited $code, not 3"
echo "ok   db blip"
