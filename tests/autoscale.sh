#!/bin/sh
# Autoscaling on the ec2 runner (morca.ec2), against the stand-in EC2 API
# (tests/fake_ec2.py). fanout, registered with --workers 0..3, starts its
# run's head alone; the scheduler reads the head's demand each tick and
# starts workers for the month's tasks, never more than 3 at once; then,
# while all_trips pauses with nothing to run (MORCA_FANOUT_PAUSE), it drains
# the idle workers, which shut themselves down, down to none. The run
# succeeds, and orca.instances says which workers were drained.
#
# Then several dista worker processes per worker instance (morca register
# --worker-procs): the flag is parsed and stored (a count, or auto for one
# per vCPU). fanout, registered with --workers 0..3 --worker-procs 2, starts
# its head alone, then worker instances (never more than 3) whose user data
# starts 2 supervised dista worker processes each; the head sees 2 per
# instance, the tasks run on both of an instance's processes, and while
# all_trips pauses the idle processes are drained one by one until each
# instance, once its last has gone, is recorded as drained and shuts itself
# down. (This was tests/worker_procs.sh, #447.)
#
# Needs MORCA_DATABASE_URL (pixi run test-autoscale starts a throwaway
# server), python3 and curl.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
one_cluster_test
mkdir -p build
scripts/pipelines.sh fanout
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/autoscale.log
: > "$log"
dir=$PWD/build/fake-ec2-autoscale
rm -rf "$dir" build/autoscale-logs
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
. "$PWD/tests/wait_log.sh"
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
export MORCA_RUNNER=ec2 MORCA_EC2_MODE=direct MORCA_EC2_ENDPOINT="http://127.0.0.1:$port"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test MORCA_EC2_AMI=ami-test
export MORCA_STALE_SECONDS=10 MORCA_START_SECONDS=60
export MORCA_LOGS=$PWD/build/autoscale-logs MORCA_CLUSTER_LOG_SECONDS=1
export MORCA_FANOUT_SLEEP=0.5 MORCA_FANOUT_PAUSE=8 MORCA_FANOUT_SHOW_HOST=1 MORCA_RUN_ENV=MORCA_FANOUT_SLEEP,MORCA_FANOUT_PAUSE,MORCA_FANOUT_SHOW_HOST
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

./build/morca register --image morca:test --worker-type c7g.xlarge --workers 0..3 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
[ "$(q "SELECT ec2_min_workers || '..' || ec2_workers FROM orca.pipelines WHERE name = 'fanout'")" = 0..3 ] \
    || fail "fanout's range"
./build/morca scheduler >> "$log" 2>&1 &
sched=$!
./build/morca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill fanout"
run=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout'")

# The head starts alone: its RunInstances is the run's only one at first.
until_true "SELECT count(*) > 0 FROM orca.instances WHERE run = $run" 120
[ "$(q "SELECT string_agg(role, ',') FROM orca.instances WHERE run = $run")" = head ] \
    || fail "run $run started with workers"
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $run" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "run $run"
# The head uploads its log in its grace after the run: wait for it.
wait_log "$MORCA_LOGS/fanout/$run/driver.log" "trips in 1 months" || fail "run $run's driver log"

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
# The scheduler again, after the registrations below, on its own log.
if [ -n "$sched" ]; then
    kill "$sched" 2> /dev/null || true
    wait "$sched" 2> /dev/null || true
fi
sched=
echo "ok   autoscale: a run's workers grew from 0 to $most and were drained when idle"

log=build/autoscale-procs.log
: > "$log"
# The flag: a count or auto; anything else is refused; kept when not given.
for bad in 0 -2 x 1.5 ""; do
    if ./build/morca register --worker-procs "$bad" ./build/fanout > /dev/null 2>&1; then
        fail "register took --worker-procs '$bad'"
    fi
done
stored() { q "SELECT coalesce(worker_procs::text, 'null') FROM orca.pipelines WHERE name = 'fanout'"; }
./build/morca register --image morca:test --worker-procs 4 ./build/fanout >> "$log" 2>&1 || fail "register 4"
[ "$(stored)" = 4 ] || fail "--worker-procs 4 stored as $(stored)"
./build/morca register --worker-procs auto ./build/fanout >> "$log" 2>&1 || fail "register auto"
[ "$(stored)" = 0 ] || fail "--worker-procs auto stored as $(stored)"
./build/morca register --image morca:test ./build/fanout >> "$log" 2>&1 || fail "register again"
[ "$(stored)" = 0 ] || fail "a register without the flag changed it to $(stored)"
./build/morca register --image morca:test --worker-type c7g.xlarge --workers 0..3 --worker-procs 2 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
[ "$(stored)" = 2 ] || fail "--worker-procs 2 stored as $(stored)"
./build/morca scheduler >> "$log" 2>&1 &
sched=$!
./build/morca backfill fanout 2024-03 >> "$log" 2>&1 || fail "backfill fanout 2024-03"
run=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'fanout'")
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $run" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "run $run"
wait_log "$MORCA_LOGS/fanout/$run/driver.log" "trips in 1 months" || fail "run $run's driver log"

# Instances, not processes, are what's started and counted: at most 3, each
# with 2 processes in its script and its record.
workers=$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND role = 'worker'")
[ "$workers" -ge 1 ] && [ "$workers" -le 3 ] || fail "run $run started $workers worker instances"
[ "$(q "SELECT string_agg(DISTINCT procs::text, ',') FROM orca.instances WHERE run = $run AND role = 'worker'")" = 2 ] \
    || fail "the workers' procs"
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND role = 'head' AND procs = 1")" = 1 ] || fail "the head's procs"
ids=$(q "SELECT id FROM orca.instances WHERE run = $run AND role = 'worker'")
for w in $ids; do
    grep -q 'while \[ \$k -le 2 \]' "$dir/$w.sh" || fail "worker $w's script doesn't start 2 processes"
    grep -q 'kill -TERM \$p' "$dir/$w.sh" || fail "worker $w's script doesn't forward SIGTERM"
    l=$MORCA_LOGS/fanout/$run/cluster/worker-$w.log
    # A worker uploads its log as it exits, after the run.
    wait_log "$l" 'worker process .* exited with status 0' 300 || fail "worker $w's log wasn't kept"
    grep -q '^== morca-.*\.p1 ==$' "$l" && grep -q '^== morca-.*\.p2 ==$' "$l" || fail "worker $w's log lacks a process: $(head -5 "$l")"
    # Both processes of an instance were exited and noticed.
    [ "$(grep -c 'worker process .* exited with status 0' "$l")" -eq 2 ] || fail "worker $w's processes' exits: $(grep 'exited' "$l")"
done
# The tasks ran on more processes than instances: both processes of an
# instance took tasks, on the same host.
ran=$(for w in $ids; do cat "$MORCA_LOGS/fanout/$run/cluster/worker-$w.log"; done | grep -c '^trips on ')
[ "$ran" -eq 31 ] || fail "run $run's tasks on workers: $ran"
busy=$(for w in $ids; do awk '/^== /{sec=$0} /^trips on /{seen[sec]=1} END{for (k in seen) print k}' "$MORCA_LOGS/fanout/$run/cluster/worker-$w.log"; done | wc -l | tr -d ' ')
[ "$busy" -gt "$workers" ] || fail "run $run: $busy processes took tasks on $workers instances"

# All processes drained, so each instance is: no process drains left over.
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND role = 'worker' AND (drained IS NULL OR procs_drained <> 2)")" = 0 ] \
    || fail "drained: $(q "SELECT string_agg(id || ':' || procs_drained || '/' || coalesce(drained::text, 'no'), ' ') FROM orca.instances WHERE run = $run AND role = 'worker'")"
q "SELECT why FROM orca.instances WHERE run = $run AND role = 'worker'" | grep -vq "^drained: idle$" \
    && fail "a worker's reason: $(q "SELECT string_agg(why, ',') FROM orca.instances WHERE run = $run AND role = 'worker'")"
# The run page says how many each ran.
grep -q "scaled run workers" "$log" || fail "the scheduler never scaled"
stop
echo "ok   worker-procs: $workers instances of 2 worker processes each, drained when idle"
