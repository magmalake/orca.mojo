#!/bin/sh
# A keyed sensor whose keys a dated pipeline writes (tests/keyed/): `repos`
# is daily and adds two names a day to keys.repos; `history` is keyed on
# keys.repos, with sensor=True, and reads it too.
#
# - both register, in either order: a keyed sensor's own keys dataset needs
#   no rule from dates to names;
# - a keyed sensor reading another dataset `repos` writes isn't registered;
# - with the scheduler running, a backfill of a day of `repos` starts a
#   `history` run of that day's two names, and of no earlier ones;
# - `morca lineage` goes from lake.history through keys.repos to `repos`.
#
# Needs MORCA_DATABASE_URL (pixi run -e data test-keyed-sensor starts a
# throwaway server) and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# The pipelines and morca compile at once (scripts/mojo-build.sh).
pids=
for p in repos history days_reader; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src -I tests/keyed $DEP_INC "tests/keyed/$p.mojo" -o "build/ks-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
for pid in $pids $!; do wait "$pid"; done
log=build/keyed-sensor.log
: > "$log"
export MORCA_WAREHOUSE=build/keyed-sensor-wh MORCA_SENSE_SECONDS=1 MORCA_STALE_SECONDS=10
rm -rf "$MORCA_WAREHOUSE"
sched=
stop() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    sched=
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    stop
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
# wait_runs PIPELINE AFTER N: until N runs of PIPELINE after run AFTER succeeded.
wait_runs() {
    i=0
    until [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = '$1' AND id > $2 AND status = 'succeeded'")" -ge "$3" ]; do
        i=$((i + 1))
        [ "$i" -lt 1200 ] || fail "$1's runs: $(q "SELECT id, trigger, status, partitions FROM orca.runs WHERE id > $2")"
        sleep 0.1
    done
}
# history_keys AFTER: the names of history's runs after run AFTER, sorted.
history_keys() {
    q "SELECT string_agg(k, ',' ORDER BY k) FROM (SELECT unnest(string_to_array(partitions, ',')) AS k FROM orca.runs WHERE pipeline = 'history' AND id > $1) x"
}

# Registered in either order; the writer again once the sensor is there.
for b in repos history repos; do
    ./build/morca register "./build/ks-$b" >> "$log" 2>&1 || fail "register $b"
done
code=0
out=$(./build/morca register ./build/ks-days_reader 2>&1) || code=$?
[ "$code" -eq 3 ] && echo "$out" | grep -q "reads lake.days, which repos (daily) writes, and no rule turns a name into a date" \
    || fail "days_reader registered ($code): $out"

./build/morca scheduler >> "$log" 2>&1 &
sched=$!

# A day of repos: its commit to keys.repos starts history for its names.
before=$(q "SELECT coalesce(max(id), 0) FROM orca.runs")
[ "$(./build/morca backfill repos 2024-06-01 2>> "$log")" = "queued 1 runs" ] || fail "the first backfill"
wait_runs repos "$before" 1
wait_runs history "$before" 1
[ "$(history_keys "$before")" = "day-01-a,day-01-b" ] || fail "history ran $(history_keys "$before")"
[ "$(q "SELECT trigger FROM orca.runs WHERE pipeline = 'history' AND id > $before")" = "sensor" ] \
    || fail "history's run wasn't the sensor's"

# The next day: only the new names.
before=$(q "SELECT coalesce(max(id), 0) FROM orca.runs")
[ "$(./build/morca backfill repos 2024-06-02 2>> "$log")" = "queued 1 runs" ] || fail "the second backfill"
wait_runs repos "$before" 1
wait_runs history "$before" 1
sleep 3
[ "$(history_keys "$before")" = "day-02-a,day-02-b" ] || fail "history ran $(history_keys "$before") for the second day"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'history' AND status <> 'succeeded'")" -eq 0 ] \
    || fail "a history run didn't succeed"
stop

# Lineage, back from lake.history's latest snapshot through keys.repos.
out=$(./build/morca lineage lake.history) || fail "morca lineage exited $?"
echo "$out" >> "$log"
echo "$out" | grep -qE "^ *0 lake\.history@[0-9-]+ by history run [0-9]+ hist day-02-[ab] code 0x[0-9a-f]+ from keys\.repos@[0-9-]+\$" \
    || fail "lake.history's snapshot and its input: $out"
echo "$out" | grep -qE "^ *1 keys\.repos@[0-9-]+ by repos run [0-9]+ find 2024-06-0[12] code 0x[0-9a-f]+\$" \
    || fail "the keys.repos snapshot history read: $out"
echo "ok   keyed sensor"
