#!/bin/sh
# Autoscaling on the ec2 runner (orca.ec2), against the stand-in EC2 API
# (tests/fake_ec2.py). fanout, registered with --workers 0..3, starts its
# run's head alone; the scheduler reads the head's demand each tick and
# starts workers for the month's tasks, never more than 3 at once; then,
# while all_trips pauses with nothing to run (ORCA_FANOUT_PAUSE), it drains
# the idle workers, which shut themselves down, down to none. The run
# succeeds, and orca.instances says which workers were drained.
#
# Needs ORCA_DATABASE_URL (pixi run test-autoscale starts a throwaway
# server), python3 and curl.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/autoscale.log
: > "$log"
dir=$PWD/build/fake-ec2-autoscale
rm -rf "$dir" build/autoscale-logs
mkdir -p "$dir"
port=$((34000 + $$ % 2000))
fake=
sched=
stop() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$fake" ] || kill "$fake" 2> /dev/null || true
    sched=
    fake=
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    stop
    exit 1
}
. "$PWD/tests/wait_log.sh"
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
export ORCA_RUNNER=ec2 ORCA_EC2_MODE=direct ORCA_EC2_ENDPOINT="http://127.0.0.1:$port"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test ORCA_EC2_AMI=ami-test
export ORCA_STALE_SECONDS=10 ORCA_START_SECONDS=60
export ORCA_LOGS=$PWD/build/autoscale-logs ORCA_CLUSTER_LOG_SECONDS=1
export ORCA_FANOUT_SLEEP=0.5 ORCA_FANOUT_PAUSE=8 ORCA_RUN_ENV=ORCA_FANOUT_SLEEP,ORCA_FANOUT_PAUSE
export ORCA_SCALE_UP_AFTER=1 ORCA_SCALE_IDLE=2
python3 tests/fake_ec2.py "$port" "$dir" >> "$log" 2>&1 &
fake=$!
i=0
until curl -s -o /dev/null "$ORCA_EC2_ENDPOINT/"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the stand-in EC2 didn't start"; sleep 0.1
done
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt "$2" ] || fail "waited for: $1"; sleep 0.5
    done
}

./build/orca register --image orca:test --worker-type c7g.xlarge --workers 0..3 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
[ "$(q "SELECT ec2_min_workers || '..' || ec2_workers FROM orca.pipelines WHERE name = 'fanout'")" = 0..3 ] \
    || fail "fanout's range"
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
./build/orca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill fanout"
run=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout'")

# The head starts alone: its RunInstances is the run's only one at first.
until_true "SELECT count(*) > 0 FROM orca.instances WHERE run = $run" 120
[ "$(q "SELECT string_agg(role, ',') FROM orca.instances WHERE run = $run")" = head ] \
    || fail "run $run started with workers"
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $run" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "run $run"
# The head uploads its log in its grace after the run: wait for it.
wait_log "$ORCA_LOGS/fanout/$run/driver.log" "trips in 1 months" || fail "run $run's driver log"

# At most 3 workers at once, each started by a decision; all drained.
most=$(q "SELECT max(n) FROM (SELECT (SELECT count(*) FROM orca.instances j WHERE j.run = $run AND j.role = 'worker' AND j.launched <= i.launched AND (j.ended IS NULL OR j.ended > i.launched)) AS n FROM orca.instances i WHERE i.run = $run AND i.role = 'worker') x")
workers=$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND role = 'worker'")
drained=$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND role = 'worker' AND drained IS NOT NULL")
why=$(q "SELECT string_agg(DISTINCT coalesce(why, 'still up'), ',') FROM orca.instances WHERE run = $run AND role = 'worker'")
echo "run $run: $workers workers, at most $most at once, $drained drained ($why)"
[ "$most" -le 3 ] && [ "$most" -ge 2 ] || fail "run $run had $most workers at once"
[ "$drained" -ge 1 ] || fail "run $run drained no worker"
i=0
until [ "$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND drained IS NOT NULL AND ended IS NULL")" -eq 0 ]; do
    i=$((i + 1)); [ "$i" -lt 60 ] || fail "drained workers are still up"; sleep 0.5
done
q "SELECT why FROM orca.instances WHERE run = $run AND drained IS NOT NULL" | grep -q "^drained: idle$" \
    || fail "a drained worker's reason: $(q "SELECT string_agg(why, ',') FROM orca.instances WHERE run = $run AND drained IS NOT NULL")"
grep -q "scaled run workers" "$log" || fail "the scheduler never scaled"
stop
echo "ok   autoscale: a run's workers grew from 0 to $most and were drained when idle"
