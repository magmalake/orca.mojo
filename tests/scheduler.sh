#!/bin/sh
# The morca scheduler over examples/toy.mojo (03:00 UTC on the 2nd, for the
# month before, from 2024-01, max_runs=2), with MORCA_NOW stepping time:
# fires once each, catch-up, ticks racing, max_runs, retries with backoff, a
# killed run restarted, a backfill, then one with --rerun, and the machine's
# metrics beside a run. Needs MORCA_DATABASE_URL (pixi run
# test-scheduler starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
scripts/pipelines.sh toy
scripts/pipelines.sh daily
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/scheduler.log
: > "$log"
export MORCA_LOGS=build/scheduler-logs MORCA_STALE_SECONDS=2 MORCA_RETRY_SECONDS=60
rm -rf "$MORCA_LOGS"

fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
tick() { MORCA_NOW="$1" ./build/morca tick >> "$log" 2>&1 || fail "tick at $1 exited $?"; }
# Waits until no run is running, checking max_runs all along.
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")" -gt 0 ]; do
        n=$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND status = 'running'")
        [ "$n" -le 2 ] || fail "$n toy runs running, max_runs is 2"
        i=$((i + 1))
        [ "$i" -lt 600 ] || fail "runs didn't finish"
        sleep 0.1
    done
}
succeeded() { q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND status = 'succeeded' AND partitions = '$1'"; }

MORCA_NOW=2024-01-15T00:00Z ./build/morca register ./build/toy >> "$log" 2>&1 \
    || fail "register"
[ "$(q "SELECT binary_path FROM orca.pipelines WHERE name = 'toy'")" = "$PWD/build/toy" ] \
    || fail "the binary's path wasn't recorded"

# Not due yet; then due once, for January.
tick 2024-02-01T00:00Z
[ "$(q "SELECT count(*) FROM orca.fires")" -eq 0 ] || fail "fired early"
tick 2024-02-02T03:00Z
settle
[ "$(q "SELECT partition FROM orca.fires WHERE pipeline = 'toy'")" = "2024-01" ] || fail "no fire for 2024-01"
[ "$(succeeded 2024-01)" -eq 1 ] || fail "2024-01 didn't succeed"
[ -s "$MORCA_LOGS/toy/1/driver.log" ] || fail "no driver log"
# The sampler beside it (morca.sample) left the machine's metrics, though the
# run was shorter than an interval.
i=0
until [ -s "$MORCA_LOGS/toy/1/metrics/local.jsonl" ]; do
    i=$((i + 1)); [ "$i" -lt 50 ] || fail "no metrics for run 1"; sleep 0.1
done
python3 tests/metrics_ok.py "$MORCA_LOGS/toy/1/metrics/local.jsonl" > /dev/null || fail "run 1's metrics"
tick 2024-02-02T03:00Z
[ "$(q "SELECT count(*) FROM orca.fires")" -eq 1 ] || fail "fired twice"

# Three missed fires, made at once by ticks racing each other; two runs
# start, the third waits for a slot.
for _ in 1 2 3 4; do
    MORCA_TOY_SLEEP=0.2 MORCA_NOW=2024-05-02T03:30Z ./build/morca tick >> "$log" 2>&1 &
done
wait
[ "$(q "SELECT count(*) FROM orca.fires WHERE pipeline = 'toy'")" -eq 4 ] || fail "catch-up fired the wrong number"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND trigger = 'schedule'")" -eq 4 ] \
    || fail "racing ticks made extra runs"
[ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")" -le 2 ] || fail "more than max_runs started"
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
    MORCA_TOY_SLEEP=0.2 MORCA_NOW=2024-05-02T03:30Z ./build/morca tick >> "$log" 2>&1
    n=$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")
    [ "$n" -le 2 ] || fail "$n running, max_runs is 2"
    sleep 0.2
done
for m in 2024-02 2024-03 2024-04; do
    [ "$(succeeded $m)" -eq 1 ] || fail "$m didn't succeed once"
done

# A failed run is retried after its backoff, not before.
MORCA_TOY_FAIL=2024-06 MORCA_NOW=2024-07-02T03:00Z ./build/morca tick >> "$log" 2>&1
settle
run=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-06'")
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = "failed" ] || fail "2024-06 didn't fail"
tick 2024-07-02T03:00:30Z
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = "failed" ] || fail "retried before the backoff"
due=$(q "SELECT retry_at - extract(epoch from '2024-07-02T03:00:30Z'::timestamptz)::bigint FROM orca.runs WHERE id = $run")
[ "$due" -ge 60 ] && [ "$due" -le 66 ] || fail "backoff was $due s, not 60 to 66"
tick 2024-07-02T03:02Z
settle
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $run")" = "succeeded 2" ] \
    || fail "2024-06 wasn't retried once"

# A run whose process is killed is started again once its heartbeat is stale.
MORCA_TOY_SLEEP=1 MORCA_NOW=2024-08-02T03:00Z ./build/morca tick >> "$log" 2>&1
run=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-07'")
i=0
until owner=$(q "SELECT owner FROM orca.runs WHERE id = $run") && [ "${owner#*:}" != "$owner" ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "run $run never claimed"; sleep 0.1
done
kill -9 "${owner##*:}"
sleep 2.5
MORCA_TOY_SLEEP=0 MORCA_NOW=2024-08-02T03:01Z ./build/morca tick >> "$log" 2>&1
settle
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $run")" = "succeeded 2" ] \
    || fail "the killed run wasn't restarted"

# A backfill over the year queues only what hasn't succeeded, and runs it two
# at a time.
out=$(MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill toy 2024-01..2024-12) || fail "backfill exited $?"
[ "$out" = "queued 5 runs" ] || fail "backfill: $out"
set +e
MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill toy 2023-12 >> "$log" 2>&1
code=$?
set -e
[ "$code" -eq 3 ] || fail "a backfill before the start exited $code"
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
    MORCA_TOY_SLEEP=0.1 MORCA_NOW=2024-08-02T04:00Z ./build/morca tick >> "$log" 2>&1
    n=$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")
    [ "$n" -le 2 ] || fail "$n running during the backfill"
    sleep 0.2
done
for m in 2024-01 2024-02 2024-03 2024-04 2024-05 2024-06 2024-07 2024-08 2024-09 2024-10 2024-11 2024-12; do
    [ "$(succeeded $m)" -eq 1 ] || fail "$m didn't succeed exactly once"
done

# Done months aren't backfilled again, unless with --rerun, which still
# leaves a month that's queued alone.
out=$(MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill toy 2024-03..2024-04) || fail "backfill exited $?"
[ "$out" = "queued 0 runs" ] || fail "a backfill of done months: $out"
out=$(MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill --rerun toy 2024-03..2024-04) || fail "backfill --rerun exited $?"
[ "$out" = "queued 2 runs" ] || fail "backfill --rerun: $out"
out=$(MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill --rerun toy 2024-03..2024-04) || fail "backfill --rerun again exited $?"
[ "$out" = "queued 0 runs" ] || fail "backfill --rerun of queued months: $out"
[ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND trigger = 'backfill'")" -eq 2 ] || fail "the reruns' runs"
code=0
MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill --again toy 2024-03 >> "$log" 2>&1 || code=$?
[ "$code" -eq 2 ] || fail "an unknown backfill option exited $code"
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
    MORCA_TOY_SLEEP=0 MORCA_NOW=2024-08-02T04:00Z ./build/morca tick >> "$log" 2>&1
    sleep 0.2
done
for m in 2024-03 2024-04; do
    [ "$(succeeded $m)" -eq 2 ] || fail "$m didn't succeed again"
done

# The daily example has catchup=False: registered on the 10th and first
# ticked on the 15th, only the latest fire makes a run.
MORCA_NOW=2024-08-10T00:00Z ./build/morca register ./build/daily >> "$log" 2>&1 || fail "register daily"
tick 2024-08-15T12:00Z
[ "$(q "SELECT count(*) || ' ' || max(partition) FROM orca.fires WHERE pipeline = 'daily'")" = "1 2024-08-14" ] \
    || fail "catchup=False made $(q "SELECT count(*) FROM orca.fires WHERE pipeline = 'daily'") fires"
settle
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'daily' AND status = 'succeeded'")" -eq 1 ] \
    || fail "the daily run didn't succeed"

# A run's start is when it started, not when the tick's transaction began:
# here that waits 2 s for the pipeline's row, as it would for another
# scheduler's tick, and the start comes after. (Recorded as the transaction's
# start, a run that waited for another to end could look as if it ran beside
# it: tests/kind.sh's budget check.)
out=$(MORCA_NOW=2024-08-02T04:00Z ./build/morca backfill --rerun toy 2024-05) || fail "backfill exited $?"
[ "$out" = "queued 1 runs" ] || fail "backfill of 2024-05: $out"
psql "$MORCA_DATABASE_URL" -Atq -c "BEGIN; SELECT name FROM orca.pipelines WHERE name = 'toy' FOR UPDATE; SELECT pg_sleep(2); COMMIT" > /dev/null &
locker=$!
i=0
until [ "$(q "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE c.relname = 'pipelines' AND l.mode = 'RowShareLock' AND l.pid <> pg_backend_pid()")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the pipeline's row wasn't locked"; sleep 0.05
done
before=$(q "SELECT extract(epoch FROM clock_timestamp())")
MORCA_TOY_SLEEP=0 MORCA_NOW=2024-08-02T04:00Z ./build/morca tick >> "$log" 2>&1 || fail "tick behind the lock"
wait "$locker"
waited=$(q "SELECT round(extract(epoch FROM started) - $before, 1) FROM orca.runs WHERE pipeline = 'toy' AND partitions = '2024-05' ORDER BY id DESC LIMIT 1")
awk -v w="$waited" 'BEGIN { exit !(w >= 1) }' || fail "the run's start is ${waited}s after the tick began, before the lock was let go"
settle
echo "ok   scheduler"
