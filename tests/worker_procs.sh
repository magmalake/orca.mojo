#!/bin/sh
# Several dista worker processes per worker instance on the ec2 runner
# (orca register --worker-procs), against the stand-in EC2 API
# (tests/fake_ec2.py). The flag is parsed and stored (a count, or auto for
# one per vCPU). fanout, registered with --workers 0..3 --worker-procs 2,
# starts its head alone, then worker instances (never more than 3) whose user
# data starts 2 supervised dista worker processes each; the head sees 2 per
# instance, the tasks run on both of an instance's processes, and while
# all_trips pauses the idle processes are drained one by one until each
# instance, once its last has gone, is recorded as drained and shuts itself
# down. A process killed on its own is noticed in the instance's log.
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
log=build/procs.log
: > "$log"
dir=$PWD/build/fake-ec2-procs
rm -rf "$dir" build/procs-logs
mkdir -p "$dir"
port=$((36000 + $$ % 2000))
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
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
export ORCA_RUNNER=ec2 ORCA_EC2_MODE=direct ORCA_EC2_ENDPOINT="http://127.0.0.1:$port"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test ORCA_EC2_AMI=ami-test
export ORCA_STALE_SECONDS=10 ORCA_START_SECONDS=60
export ORCA_LOGS=$PWD/build/procs-logs ORCA_CLUSTER_LOG_SECONDS=1
export ORCA_FANOUT_SLEEP=0.5 ORCA_FANOUT_PAUSE=8 ORCA_FANOUT_SHOW_HOST=1 ORCA_RUN_ENV=ORCA_FANOUT_SLEEP,ORCA_FANOUT_PAUSE,ORCA_FANOUT_SHOW_HOST
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


# The flag: a count or auto; anything else is refused; kept when not given.
for bad in 0 -2 x 1.5 ""; do
    if ./build/orca register --worker-procs "$bad" ./build/fanout > /dev/null 2>&1; then
        fail "register took --worker-procs '$bad'"
    fi
done
stored() { q "SELECT coalesce(worker_procs::text, 'null') FROM orca.pipelines WHERE name = 'fanout'"; }
./build/orca register --image orca:test --worker-procs 4 ./build/fanout >> "$log" 2>&1 || fail "register 4"
[ "$(stored)" = 4 ] || fail "--worker-procs 4 stored as $(stored)"
./build/orca register --worker-procs auto ./build/fanout >> "$log" 2>&1 || fail "register auto"
[ "$(stored)" = 0 ] || fail "--worker-procs auto stored as $(stored)"
./build/orca register --image orca:test ./build/fanout >> "$log" 2>&1 || fail "register again"
[ "$(stored)" = 0 ] || fail "a register without the flag changed it to $(stored)"
./build/orca register --image orca:test --worker-type c7g.xlarge --workers 0..3 --worker-procs 2 ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
[ "$(stored)" = 2 ] || fail "--worker-procs 2 stored as $(stored)"
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
./build/orca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill fanout"
run=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout'")
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $run" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "run $run"
grep -q "trips in 1 months" "$ORCA_LOGS/fanout/$run/driver.log" || fail "run $run's driver log"

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
    l=$ORCA_LOGS/fanout/$run/cluster/worker-$w.log
    [ -f "$l" ] || fail "worker $w's log wasn't kept"
    grep -q '^== orca-.*\.p1 ==$' "$l" && grep -q '^== orca-.*\.p2 ==$' "$l" || fail "worker $w's log lacks a process: $(head -5 "$l")"
    # Both processes of an instance were exited and noticed.
    [ "$(grep -c 'worker process .* exited with status 0' "$l")" -eq 2 ] || fail "worker $w's processes' exits: $(grep 'exited' "$l")"
done
# The tasks ran on more processes than instances: both processes of an
# instance took tasks, on the same host.
ran=$(for w in $ids; do cat "$ORCA_LOGS/fanout/$run/cluster/worker-$w.log"; done | grep -c '^trips on ')
[ "$ran" -eq 31 ] || fail "run $run's tasks on workers: $ran"
busy=$(for w in $ids; do awk '/^== /{sec=$0} /^trips on /{seen[sec]=1} END{for (k in seen) print k}' "$ORCA_LOGS/fanout/$run/cluster/worker-$w.log"; done | wc -l | tr -d ' ')
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
