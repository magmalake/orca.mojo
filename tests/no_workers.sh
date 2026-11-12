#!/bin/sh
# A run whose tasks wait for workers that never come (morca.ec2,
# MORCA_NO_WORKERS_SECONDS), against the stand-in EC2 API
# (tests/fake_ec2.py), where workers can be made to sleep instead of joining
# their head. With the scheduler running:
#
# - fanout, registered with --workers 0..3, starts its worker instances, but
#   they never join: after MORCA_NO_WORKERS_SECONDS (6) with tasks waiting
#   the attempt fails with "no workers joined in 6 s (tasks waiting: N)",
#   the start's instances are terminated, and the run is started again, as
#   one whose process died is; its new workers join and it succeeds;
# - a pipeline registered again while its run is live (another worker type
#   and at most 1 worker) doesn't change how that run's workers start: they
#   are of the type and number it was registered with when the run started;
# - with MORCA_NO_WORKERS_SECONDS=0 a run whose workers never come goes on
#   waiting, as before.
#
# Needs MORCA_DATABASE_URL (pixi run test-no-workers starts a throwaway
# server), python3 and curl.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
one_cluster_test
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/no-workers.log
: > "$log"
dir=$PWD/build/fake-ec2-no-workers
rm -rf "$dir" build/no-workers-logs
mkdir -p "$dir"
free_port port
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
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
export MORCA_RUNNER=ec2 MORCA_EC2_MODE=direct MORCA_EC2_ENDPOINT="http://127.0.0.1:$port"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test MORCA_EC2_AMI=ami-test
export MORCA_STALE_SECONDS=10 MORCA_START_SECONDS=60 MORCA_EC2_POOL_SECONDS=0
export MORCA_LOGS=$PWD/build/no-workers-logs MORCA_CLUSTER_LOG_SECONDS=1
export MORCA_FANOUT_SLEEP=0.5 MORCA_RUN_ENV=MORCA_FANOUT_SLEEP
export MORCA_SCALE_UP_AFTER=1 MORCA_SCALE_IDLE=2
python3 tests/fake_ec2.py "$port" "$dir" >> "$log" 2>&1 &
fake=$!
i=0
until curl -s -o /dev/null "$MORCA_EC2_ENDPOINT/"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the stand-in EC2 didn't start"; sleep 0.1
done
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt "$2" ] || fail "waited for: $1"; sleep 0.5
    done
}
scheduler() {  # starts the scheduler with MORCA_NO_WORKERS_SECONDS=$1
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    MORCA_NO_WORKERS_SECONDS=$1 ./build/morca scheduler >> "$log" 2>&1 &
    sched=$!
}

./build/morca register --image morca:test --worker-type c7g.xlarge --workers 0..3 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
scheduler 6

# Workers that never join: the attempt fails, the instances go, the run
# starts again and succeeds.
touch "$dir/no-workers"
./build/morca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill fanout"
r1=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout' AND partitions = '2024-01'")
until_true "SELECT waiting_since IS NOT NULL FROM orca.runs WHERE id = $r1" 240
# The scheduler notes the run waiting, then starts the workers it can't see
# yet, in the same pass: their rows come a moment after waiting_since.
until_true "SELECT count(*) > 0 FROM orca.instances WHERE run = $r1 AND role = 'worker'" 20
until_true "SELECT count(*) > 0 FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $r1 AND a.error LIKE 'no workers joined in 6 s (tasks waiting: %)'" 240
rm "$dir/no-workers"
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $r1" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $r1")" = succeeded ] \
    || fail "run $r1: $(q "SELECT status || ' ' || coalesce(error, '') FROM orca.runs WHERE id = $r1")"
[ "$(q "SELECT starts FROM orca.runs WHERE id = $r1")" -eq 2 ] || fail "run $r1 started $(q "SELECT starts FROM orca.runs WHERE id = $r1") times"
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $r1 AND start = 1 AND why LIKE 'terminated by morca: no workers joined in 6 s (tasks waiting: %)'")" -ge 2 ] \
    || fail "run $r1's first start's instances: $(q "SELECT string_agg(role || ': ' || coalesce(why, 'up'), ', ') FROM orca.instances WHERE run = $r1 AND start = 1")"
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $r1 AND start = 1 AND ended IS NULL")" -eq 0 ] \
    || fail "run $r1's first start's instances are still up"
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $r1 AND start = 2 AND role = 'worker'")" -ge 1 ] \
    || fail "run $r1's second start had no workers"
[ -z "$(q "SELECT waiting_since FROM orca.runs WHERE id = $r1")" ] || fail "run $r1 is still waiting"
grep -q "no workers joined in 6 s" "$log" || fail "the scheduler said nothing"
echo "ok   run $r1: workers that never joined failed the attempt, and the run started again and succeeded"

# A live run scales from its own registration, not the pipeline's now.
./build/morca backfill fanout 2024-02 >> "$log" 2>&1 || fail "backfill fanout 2024-02"
r2=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout' AND partitions = '2024-02'")
until_true "SELECT count(*) > 0 FROM orca.instances WHERE run = $r2" 120
./build/morca register --image morca:test --worker-type c7g.2xlarge --workers 0..1 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout again"
[ "$(q "SELECT worker_type || ' ' || ec2_workers FROM orca.pipelines WHERE name = 'fanout'")" = "c7g.2xlarge 1" ] \
    || fail "the second registration"
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $r2" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $r2")" = succeeded ] \
    || fail "run $r2: $(q "SELECT status || ' ' || coalesce(error, '') FROM orca.runs WHERE id = $r2")"
types=$(q "SELECT string_agg(DISTINCT instance_type, ',') FROM orca.instances WHERE run = $r2 AND role = 'worker'")
[ "$types" = c7g.xlarge ] || fail "run $r2's workers were $types, not of the registration it started with"
most=$(q "SELECT max(n) FROM (SELECT (SELECT count(*) FROM orca.instances j WHERE j.run = $r2 AND j.role = 'worker' AND j.launched <= i.launched AND (j.ended IS NULL OR j.ended > i.launched)) AS n FROM orca.instances i WHERE i.run = $r2 AND i.role = 'worker') x")
[ "$most" -ge 2 ] || fail "run $r2 had at most $most workers at once, not up to the 3 it was registered with"
echo "ok   run $r2: registered again while it ran, it still got $most c7g.xlarge workers at once"

# MORCA_NO_WORKERS_SECONDS=0: waiting goes on.
./build/morca register --image morca:test --worker-type c7g.xlarge --workers 0..3 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout a third time"
scheduler 0
touch "$dir/no-workers"
./build/morca backfill fanout 2024-03 >> "$log" 2>&1 || fail "backfill fanout 2024-03"
r3=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout' AND partitions = '2024-03'")
until_true "SELECT count(*) > 0 FROM orca.instances WHERE run = $r3 AND role = 'worker'" 240
sleep 25
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $r3")" = "running 1" ] \
    || fail "run $r3 with the limit off: $(q "SELECT status || ' ' || starts || ' ' || coalesce(error, '') FROM orca.runs WHERE id = $r3")"
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $r3 AND ended IS NOT NULL")" -eq 0 ] \
    || fail "run $r3's instances ended with the limit off"
[ -z "$(q "SELECT waiting_since FROM orca.runs WHERE id = $r3")" ] || fail "run $r3 is tracked with the limit off"
stop
echo "ok   no workers: MORCA_NO_WORKERS_SECONDS=0 leaves a run waiting"
