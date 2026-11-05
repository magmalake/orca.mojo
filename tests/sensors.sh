#!/bin/sh
# Phase 4's gate: sensors and a rebuild over tests/sensors/. `source`
# commits to raw.events as a program that isn't an orca pipeline would, and
# three sensors read: clean (raw.events to lake.clean), report (lake.clean
# to lake.report) and other (raw.other, which nothing writes, to
# lake.other). With the scheduler running:
#
# - what raw.events held when clean was registered starts nothing;
# - a source snapshot holding 2024-03 starts clean for 2024-03, whose commit
#   starts report for 2024-03, and nothing else runs;
# - each snapshot starts one run, however many ticks see it;
# - a source snapshot that says nothing of partitions runs clean for the
#   month it is (ORCA_NOW), and report after it;
# - `orca rebuild raw.events 2024-03` reruns clean, then report, and its
#   commits start no sensor;
# - the UI shows the rebuild and what started each run;
# - while a run of a partition is live, a snapshot of it is left to that
#   run, and a rebuild of it waits for it.
#
# With ORCA_TEST_S3 set, the warehouse and logs are in the bucket
# ORCA_S3_BUCKET names (pixi run -e data test-sensors-s3 starts a throwaway
# MinIO), and the scheduler keeps each run's driver log there.
#
# Needs ORCA_DATABASE_URL (pixi run -e data test-sensors starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# The pipelines and orca compile at once (scripts/mojo-build.sh).
pids=
for p in source clean report other; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src -I tests/sensors -I tests/lineage $DEP_INC "tests/sensors/$p.mojo" -o "build/sn-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
for pid in $pids $!; do wait "$pid"; done
log=build/sensors.log
: > "$log"
export ORCA_SENSE_SECONDS=1 ORCA_NOW=2024-05-15T00:00Z
if [ -n "${ORCA_TEST_S3:-}" ]; then
    export ORCA_WAREHOUSE="$ORCA_S3_BUCKET/sensors-$$" ORCA_LOGS="$ORCA_S3_BUCKET/sensors-logs-$$"
else
    export ORCA_WAREHOUSE=build/sensors-wh ORCA_LOGS=build/sensors-logs
    rm -rf "$ORCA_WAREHOUSE" "$ORCA_LOGS"
fi
sched=
ui=
stop() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
}
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    stop
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
runs() { q "SELECT count(*) FROM orca.runs WHERE pipeline = '$1' AND trigger = '${2:-sensor}'"; }
# Waits until the SQL says t, for up to 60 s.
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "waited for: $2"; sleep 0.1
    done
}
source_commit() { ./build/sn-source "$ORCA_WAREHOUSE" "$@" 2>> "$log" || fail "source $*"; }

# History: raw.events had a snapshot before clean was a sensor.
s0=$(source_commit 2024-01)
./build/orca register ./build/sn-clean ./build/sn-report ./build/sn-other >> "$log" 2>&1 || fail "register"
[ "$(q "SELECT count(*) FROM orca.sensed WHERE pipeline = 'clean' AND snapshot_id = $s0 AND run IS NULL")" -eq 1 ] \
    || fail "the snapshot raw.events had at registration isn't recorded as seen"
[ "$(./build/sn-clean sense)" = "raw.events $s0 other 2024-01" ] || fail "sense: $(./build/sn-clean sense)"
[ "$(./build/sn-other sense)" = "raw.other -1 other -" ] || fail "sense of a missing table: $(./build/sn-other sense)"

./build/orca scheduler >> "$log" 2>&1 &
sched=$!
sleep 2
[ "$(q "SELECT count(*) FROM orca.runs")" -eq 0 ] || fail "a run started before any new snapshot"

# A snapshot that says it holds 2024-03: clean, then report, for 2024-03.
s1=$(source_commit 2024-03)
until_true "SELECT count(*) = 1 FROM orca.runs WHERE pipeline = 'report' AND status = 'succeeded'" "report's run"
[ "$(q "SELECT partitions || ' ' || status FROM orca.runs WHERE pipeline = 'clean'")" = "2024-03 succeeded" ] \
    || fail "clean's run: $(q "SELECT partitions, status, trigger FROM orca.runs WHERE pipeline = 'clean'")"
[ "$(q "SELECT partitions FROM orca.runs WHERE pipeline = 'report'")" = 2024-03 ] || fail "report's partitions"
clean1=$(q "SELECT id FROM orca.runs WHERE pipeline = 'clean'")
report1=$(q "SELECT id FROM orca.runs WHERE pipeline = 'report'")
[ "$(q "SELECT run FROM orca.sensed WHERE pipeline = 'clean' AND snapshot_id = $s1")" = "$clean1" ] \
    || fail "raw.events@$s1 doesn't name the run it started"
c1=$(q "SELECT c.snapshot_id FROM orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN orca.steps s ON s.id = a.step WHERE s.run = $clean1 AND a.status = 'succeeded'")
[ "$(q "SELECT dataset || '@' || snapshot_id || ' ' || run FROM orca.sensed WHERE pipeline = 'report'")" = "lake.clean@$c1 $report1" ] \
    || fail "report wasn't started by clean's snapshot: $(q "SELECT * FROM orca.sensed WHERE pipeline = 'report'")"

# More ticks, racing the scheduler, and more polls: nothing new.
ticks=
for _ in 1 2 3; do
    ./build/orca tick >> "$log" 2>&1 &
    ticks="$ticks $!"
done
for t in $ticks; do wait "$t" || fail "a tick failed"; done
sleep 3
[ "$(q "SELECT count(*) FROM orca.runs")" -eq 2 ] || fail "$(q "SELECT count(*) FROM orca.runs") runs; one snapshot each should start one"

# A snapshot that says nothing of partitions: the month ORCA_NOW is in.
s2=$(source_commit)
until_true "SELECT count(*) = 2 FROM orca.runs WHERE pipeline = 'report' AND status = 'succeeded'" "report's second run"
[ "$(q "SELECT partitions FROM orca.runs WHERE pipeline = 'clean' ORDER BY id DESC LIMIT 1")" = 2024-05 ] \
    || fail "clean's run for a snapshot without partitions"
[ "$(q "SELECT partitions FROM orca.runs WHERE pipeline = 'report' ORDER BY id DESC LIMIT 1")" = 2024-05 ] \
    || fail "report's second run"
[ "$(q "SELECT run FROM orca.sensed WHERE pipeline = 'clean' AND snapshot_id = $s2")" -gt "$clean1" ] \
    || fail "raw.events@$s2 started no run"

# Rebuild downstream of raw.events: clean, then report.
out=$(./build/orca rebuild raw.events 2024-03) || fail "orca rebuild: $out"
echo "$out" >> "$log"
rb=$(echo "$out" | sed -n 's/^rebuild \([0-9]*\)$/\1/p')
[ -n "$rb" ] || fail "orca rebuild printed no ID: $out"
echo "$out" | grep -qx "  level 0 clean 2024-03" || fail "the plan: $out"
echo "$out" | grep -qx "  level 1 report 2024-03" || fail "the plan: $out"
until_true "SELECT status = 'succeeded' FROM orca.rebuilds WHERE id = $rb" "the rebuild"
[ "$(q "SELECT count(*) FROM orca.runs WHERE rebuild = $rb AND trigger = 'rebuild' AND status = 'succeeded'")" -eq 2 ] \
    || fail "the rebuild's runs"
[ "$(q "SELECT (SELECT started FROM orca.runs WHERE rebuild = $rb AND pipeline = 'report') >= (SELECT ended FROM orca.runs WHERE rebuild = $rb AND pipeline = 'clean')")" = t ] \
    || fail "report's rebuild run started before clean's ended"
sleep 3
[ "$(runs clean)" -eq 2 ] && [ "$(runs report)" -eq 2 ] || fail "the rebuild's commits started a sensor"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'other'")" -eq 0 ] || fail "other ran"
[ "$(q "SELECT count(*) FROM orca.runs")" -eq 6 ] || fail "$(q "SELECT count(*) FROM orca.runs") runs, not 6"

# The UI: the rebuild, and what started each run.
port=$((24000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }
[ "$(json "/rebuilds/$rb" | jq -r '.rebuild | "\(.status) \([.plan[] | "\(.level):\(.pipeline):\(.status)"] | join(" "))"')" \
    = "succeeded 0:clean:succeeded 1:report:succeeded" ] || fail "/rebuilds/$rb: $(json "/rebuilds/$rb")"
[ "$(json "/runs/$clean1" | jq -r '.sensed | join(",")')" = "raw.events@$s1" ] || fail "run $clean1's sensed"
[ "$(json "/runs/$(q "SELECT id FROM orca.runs WHERE rebuild = $rb AND pipeline = 'report'")" | jq .rebuild)" = "$rb" ] \
    || fail "the rebuild's run doesn't name it"
[ "$(json /datasets/raw.events | jq '.rebuilds[0].id')" = "$rb" ] || fail "raw.events lists no rebuild"
curl -fsS "$base/datasets/raw.events" | grep -q 'action="/datasets/raw.events/rebuild"' || fail "no rebuild form"
# The form, posted as a browser would: a second rebuild, which runs too.
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Origin: $base" -d partitions=2024-03 "$base/datasets/raw.events/rebuild")
[ "$code" = 303 ] || fail "posting the rebuild form answered $code"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Origin: $base" -d partitions=2024-03 "$base/datasets/lake.report/rebuild")
[ "$code" = 409 ] || fail "a rebuild of what nothing reads answered $code"
until_true "SELECT count(*) = 2 FROM orca.rebuilds WHERE status = 'succeeded'" "the UI's rebuild"
if [ -n "${ORCA_TEST_S3:-}" ]; then
    curl -fsS "$base/runs/$clean1/driver.log" | grep -q "run succeeded" \
        || fail "run $clean1's driver log from the bucket"
fi

# One live run per partition: while a run of clean over 2024-03 is live (a
# process elsewhere holds it), a new snapshot of 2024-03 is left to it, and
# a rebuild of 2024-03 waits for it instead of running beside it.
live=$(psql "$ORCA_DATABASE_URL" -qAtc "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, owner, heartbeat, started) SELECT name, registry_hash, '2024-03', 'manual', 'running', 'elsewhere:1', now(), now() FROM orca.pipelines WHERE name = 'clean' RETURNING id")
sensed=$(runs clean)
s3=$(source_commit 2024-03)
until_true "SELECT count(*) = 1 FROM orca.sensed WHERE pipeline = 'clean' AND snapshot_id = $s3" "raw.events@$s3 sensed"
[ "$(q "SELECT run FROM orca.sensed WHERE pipeline = 'clean' AND snapshot_id = $s3")" = "$live" ] \
    || fail "raw.events@$s3 wasn't left to live run $live: $(q "SELECT run FROM orca.sensed WHERE snapshot_id = $s3")"
[ "$(runs clean)" -eq "$sensed" ] || fail "a second run of clean over 2024-03 beside a live one"
out=$(./build/orca rebuild raw.events 2024-03) || fail "orca rebuild: $out"
echo "$out" >> "$log"
rb3=$(echo "$out" | sed -n 's/^rebuild \([0-9]*\)$/\1/p')
q "UPDATE orca.runs SET heartbeat = now() WHERE id = $live" > /dev/null
sleep 3
[ "$(q "SELECT run IS NULL FROM orca.rebuild_pipelines WHERE rebuild = $rb3 AND pipeline = 'clean'")" = t ] \
    || fail "the rebuild queued clean beside live run $live"
q "UPDATE orca.runs SET status = 'succeeded', ended = now(), heartbeat = NULL WHERE id = $live" > /dev/null
until_true "SELECT status = 'succeeded' FROM orca.rebuilds WHERE id = $rb3" "the rebuild once the live run ended"
stop
echo "ok   sensors${ORCA_TEST_S3:+ on S3}"
