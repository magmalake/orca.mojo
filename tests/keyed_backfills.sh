#!/bin/sh
# The keyed partitions gate: backfills and a sensor over keys.bulk, 20,000
# generated names, with the scheduler running (tests/keyed/bulk.mojo: 100
# keys to a run, at most 4 runs at once, never retried).
#
# - `orca backfill bulk @all` queues 200 runs and never more than 4 run; with
#   3 keys told to fail, every other key is done exactly once;
# - the UI lists the 3 as failed, and a key's page agrees with the steps;
# - a second backfill queues one run, of those 3, which then succeeds;
# - a third queues nothing;
# - 500 names appended to keys.bulk start 5 sensor runs, of the new names;
# - a keyed pipeline writing what a monthly sensor reads isn't registered.
#
# Needs ORCA_DATABASE_URL (pixi run -e data test-keyed-backfills starts a
# throwaway server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# The pipelines and orca compile at once (scripts/mojo-build.sh).
pids=
for p in names bulk events; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src -I tests/lineage $DEP_INC "tests/keyed/$p.mojo" -o "build/kd-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src -I tests/lineage $DEP_INC tests/sensors/clean.mojo -o build/sn-clean &
pids="$pids $!"
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
for pid in $pids $!; do wait "$pid"; done
log=build/keyed-backfills.log
: > "$log"
export ORCA_WAREHOUSE=build/keyed-backfills-wh ORCA_SENSE_SECONDS=1 ORCA_STALE_SECONDS=10
rm -rf "$ORCA_WAREHOUSE"
sched=
ui=
stop() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    sched=
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    stop
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
# scheduler [<names to fail>]: the variable is set on the command, as one
# set in front of a function call would outlive it.
scheduler() {
    ORCA_KEYED_FAIL=${1:-} ./build/orca scheduler >> "$log" 2>&1 &
    sched=$!
}
# Until no bulk run is queued or running, checking max_runs all along.
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'bulk' AND status IN ('queued', 'running')")" -gt 0 ]; do
        n=$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'bulk' AND status = 'running'")
        [ "$n" -le 4 ] || fail "$n bulk runs running, max_runs is 4"
        i=$((i + 1)); [ "$i" -lt 6000 ] || fail "the runs didn't finish"
        sleep 0.1
    done
}
backfill() { ./build/orca backfill bulk @all 2>> "$log" || fail "backfill exited $?"; }
done_once() { q "SELECT count(DISTINCT partition) FROM orca.steps WHERE step = 'work' AND status = 'succeeded'"; }

./build/kd-names "$ORCA_WAREHOUSE" 20000 bulk 0 5 > /dev/null || fail "names"
[ "$(./build/kd-bulk keys | tail -n +2 | wc -l | tr -d ' ')" -eq 20000 ] || fail "20,000 keys"
./build/orca register ./build/kd-bulk >> "$log" 2>&1 || fail "register"

# Every key, with three told to fail.
scheduler log-00007,log-01234,log-19999
[ "$(backfill)" = "queued 200 runs" ] || fail "the first backfill"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'bulk' AND array_length(string_to_array(partitions, ','), 1) = 100")" -eq 200 ] \
    || fail "runs of 100 keys"
settle
stop
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'bulk' AND status = 'failed'")" -eq 3 ] || fail "3 failed runs"
[ "$(done_once)" -eq 19997 ] || fail "$(done_once) keys done, not 19,997"
[ "$(q "SELECT count(*) FROM (SELECT partition FROM orca.steps WHERE step = 'work' AND status = 'succeeded' GROUP BY partition HAVING count(*) > 1) x")" -eq 0 ] \
    || fail "a key done twice"

# What the UI says of the keys, and of one.
port=$((28000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }
[ "$(json /pipelines/bulk | jq -c '.keys | [.total, .done, .live, .failed]')" \
    = '[20000,19997,0,["log-00007","log-01234","log-19999"]]' ] || fail "the keys: $(json /pipelines/bulk | jq -c .keys)"
[ "$(json /pipelines/bulk/keys/log-01234 | jq -c '[.done, [.steps[] | "\(.step):\(.status)"]]')" \
    = '[false,["work:failed"]]' ] || fail "log-01234: $(json /pipelines/bulk/keys/log-01234)"
[ "$(json /pipelines/bulk/keys/log-01235 | jq -r '[.steps[] | select(.step == "work") | .status] | join(",")')" \
    = "$(q "SELECT string_agg(status, ',' ORDER BY run) FROM orca.steps WHERE step = 'work' AND partition = 'log-01235'")" ] \
    || fail "log-01235's page and its steps"

# Again: one run, of the three; then nothing.
scheduler
[ "$(backfill)" = "queued 1 runs" ] || fail "the second backfill"
[ "$(q "SELECT partitions FROM orca.runs WHERE pipeline = 'bulk' ORDER BY id DESC LIMIT 1")" = "log-00007,log-01234,log-19999" ] \
    || fail "the second backfill's run"
settle
[ "$(done_once)" -eq 20000 ] || fail "$(done_once) keys done, not 20,000"
[ "$(backfill)" = "queued 0 runs" ] || fail "the third backfill"
[ "$(json /pipelines/bulk | jq -c '.keys | [.done, .failed]')" = '[20000,[]]' ] || fail "the keys after"

# 500 new names: the sensor runs them, and only them.
before=$(q "SELECT coalesce(max(id), 0) FROM orca.runs")
./build/kd-names "$ORCA_WAREHOUSE" 500 bulk 20000 5 > /dev/null || fail "500 more names"
i=0
until [ "$(q "SELECT count(*) FROM orca.runs WHERE id > $before AND status = 'succeeded'")" -eq 5 ]; do
    i=$((i + 1)); [ "$i" -lt 1200 ] || fail "the sensor's runs: $(q "SELECT id, trigger, status FROM orca.runs WHERE id > $before")"; sleep 0.1
done
sleep 3
[ "$(q "SELECT count(*) FROM orca.runs WHERE id > $before")" -eq 5 ] || fail "more than 5 runs for 500 names"
[ "$(q "SELECT count(*) FROM orca.runs WHERE id > $before AND trigger = 'sensor'")" -eq 5 ] || fail "the runs' trigger"
[ "$(q "SELECT count(*) FROM (SELECT unnest(string_to_array(partitions, ',')) AS k FROM orca.runs WHERE id > $before) x WHERE k < 'log-20000'")" -eq 0 ] \
    || fail "an old name in a sensor run"
[ "$(done_once)" -eq 20500 ] || fail "$(done_once) keys done, not 20,500"
stop

# A keyed writer of what a monthly sensor reads.
./build/orca register ./build/sn-clean >> "$log" 2>&1 || fail "register clean"
code=0
out=$(./build/orca register ./build/kd-events 2>&1) || code=$?
[ "$code" -eq 3 ] && echo "$out" | grep -q "no rule turns a name into a date" || fail "events registered: $out"
echo "ok   keyed backfills"
