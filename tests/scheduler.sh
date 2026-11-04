#!/bin/sh
# The orca scheduler over examples/toy.mojo (03:00 UTC on the 2nd, for the
# month before, from 2024-01, max_runs=2), with ORCA_NOW stepping time:
# fires once each, catch-up, ticks racing, max_runs, retries with backoff, a
# killed run restarted, and a backfill, then one with --rerun. Needs ORCA_DATABASE_URL (pixi run
# test-scheduler starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/daily
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/scheduler.log
: > "$log"
export ORCA_LOGS=build/scheduler-logs ORCA_STALE_SECONDS=2 ORCA_RETRY_SECONDS=60
rm -rf "$ORCA_LOGS"

fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
tick() { ORCA_NOW="$1" ./build/orca tick >> "$log" 2>&1 || fail "tick at $1 exited $?"; }
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

ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 \
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
[ -s "$ORCA_LOGS/toy/1/driver.log" ] || fail "no driver log"
tick 2024-02-02T03:00Z
[ "$(q "SELECT count(*) FROM orca.fires")" -eq 1 ] || fail "fired twice"

# Three missed fires, made at once by ticks racing each other; two runs
# start, the third waits for a slot.
for _ in 1 2 3 4; do
    ORCA_TOY_SLEEP=0.2 ORCA_NOW=2024-05-02T03:30Z ./build/orca tick >> "$log" 2>&1 &
done
wait
[ "$(q "SELECT count(*) FROM orca.fires WHERE pipeline = 'toy'")" -eq 4 ] || fail "catch-up fired the wrong number"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND trigger = 'schedule'")" -eq 4 ] \
    || fail "racing ticks made extra runs"
[ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")" -le 2 ] || fail "more than max_runs started"
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
    ORCA_TOY_SLEEP=0.2 ORCA_NOW=2024-05-02T03:30Z ./build/orca tick >> "$log" 2>&1
    n=$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")
    [ "$n" -le 2 ] || fail "$n running, max_runs is 2"
    sleep 0.2
done
for m in 2024-02 2024-03 2024-04; do
    [ "$(succeeded $m)" -eq 1 ] || fail "$m didn't succeed once"
done

# A failed run is retried after its backoff, not before.
ORCA_TOY_FAIL=2024-06 ORCA_NOW=2024-07-02T03:00Z ./build/orca tick >> "$log" 2>&1
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
ORCA_TOY_SLEEP=1 ORCA_NOW=2024-08-02T03:00Z ./build/orca tick >> "$log" 2>&1
run=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-07'")
i=0
until owner=$(q "SELECT owner FROM orca.runs WHERE id = $run") && [ "${owner#*:}" != "$owner" ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "run $run never claimed"; sleep 0.1
done
kill -9 "${owner##*:}"
sleep 2.5
ORCA_TOY_SLEEP=0 ORCA_NOW=2024-08-02T03:01Z ./build/orca tick >> "$log" 2>&1
settle
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $run")" = "succeeded 2" ] \
    || fail "the killed run wasn't restarted"

# A backfill over the year queues only what hasn't succeeded, and runs it two
# at a time.
out=$(ORCA_NOW=2024-08-02T04:00Z ./build/orca backfill toy 2024-01..2024-12) || fail "backfill exited $?"
[ "$out" = "queued 5 runs" ] || fail "backfill: $out"
set +e
ORCA_NOW=2024-08-02T04:00Z ./build/orca backfill toy 2023-12 >> "$log" 2>&1
code=$?
set -e
[ "$code" -eq 3 ] || fail "a backfill before the start exited $code"
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
    ORCA_TOY_SLEEP=0.1 ORCA_NOW=2024-08-02T04:00Z ./build/orca tick >> "$log" 2>&1
    n=$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")
    [ "$n" -le 2 ] || fail "$n running during the backfill"
    sleep 0.2
done
for m in 2024-01 2024-02 2024-03 2024-04 2024-05 2024-06 2024-07 2024-08 2024-09 2024-10 2024-11 2024-12; do
    [ "$(succeeded $m)" -eq 1 ] || fail "$m didn't succeed exactly once"
done

# Done months aren't backfilled again, unless with --rerun, which still
# leaves a month that's queued alone.
out=$(ORCA_NOW=2024-08-02T04:00Z ./build/orca backfill toy 2024-03..2024-04) || fail "backfill exited $?"
[ "$out" = "queued 0 runs" ] || fail "a backfill of done months: $out"
out=$(ORCA_NOW=2024-08-02T04:00Z ./build/orca backfill --rerun toy 2024-03..2024-04) || fail "backfill --rerun exited $?"
[ "$out" = "queued 2 runs" ] || fail "backfill --rerun: $out"
out=$(ORCA_NOW=2024-08-02T04:00Z ./build/orca backfill --rerun toy 2024-03..2024-04) || fail "backfill --rerun again exited $?"
[ "$out" = "queued 0 runs" ] || fail "backfill --rerun of queued months: $out"
[ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND trigger = 'backfill'")" -eq 2 ] || fail "the reruns' runs"
code=0
ORCA_NOW=2024-08-02T04:00Z ./build/orca backfill --again toy 2024-03 >> "$log" 2>&1 || code=$?
[ "$code" -eq 2 ] || fail "an unknown backfill option exited $code"
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
    ORCA_TOY_SLEEP=0 ORCA_NOW=2024-08-02T04:00Z ./build/orca tick >> "$log" 2>&1
    sleep 0.2
done
for m in 2024-03 2024-04; do
    [ "$(succeeded $m)" -eq 2 ] || fail "$m didn't succeed again"
done

# The daily example has catchup=False: registered on the 10th and first
# ticked on the 15th, only the latest fire makes a run.
ORCA_NOW=2024-08-10T00:00Z ./build/orca register ./build/daily >> "$log" 2>&1 || fail "register daily"
tick 2024-08-15T12:00Z
[ "$(q "SELECT count(*) || ' ' || max(partition) FROM orca.fires WHERE pipeline = 'daily'")" = "1 2024-08-14" ] \
    || fail "catchup=False made $(q "SELECT count(*) FROM orca.fires WHERE pipeline = 'daily'") fires"
settle
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'daily' AND status = 'succeeded'")" -eq 1 ] \
    || fail "the daily run didn't succeed"
echo "ok   scheduler"
