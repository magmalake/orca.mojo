#!/bin/sh
# The ec2 runner (orca.ec2) against a stand-in EC2 API (tests/fake_ec2.py),
# where each instance is a local process running its user data. With the
# scheduler running:
#
# - a fanout run starts one on-demand head instance (c7g.large) and, with
#   the head's address in their user data, 3 Spot worker instances
#   (c7g.xlarge), all tagged with the run; its tasks reach all 3 workers;
#   a worker terminated mid-run (a Spot interruption) is survived;
# - nothing of a run is left running after it, and its driver log is kept;
# - a head terminated mid-run is restarted on new instances (start 2), which
#   succeed;
# - with ORCA_VCPU_BUDGET room for one fanout run's 14 vCPUs and not one
#   more instance, a toy run never runs beside a fanout run;
# - an instance tagged with a run Postgres doesn't have is terminated.
#
# Needs ORCA_DATABASE_URL (pixi run test-ec2 starts a throwaway server),
# python3 and curl.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/fanout.mojo -o build/fanout
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/ec2.log
: > "$log"
dir=$PWD/build/fake-ec2
rm -rf "$dir" build/ec2-logs
mkdir -p "$dir"
port=$((32000 + $$ % 2000))
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
export ORCA_STALE_SECONDS=10 ORCA_START_SECONDS=60 ORCA_VCPU_BUDGET=15
export ORCA_LOGS=$PWD/build/ec2-logs ORCA_FANOUT_SHOW_HOST=1 ORCA_FANOUT_SLEEP=0.5
export ORCA_RUN_ENV=ORCA_FANOUT_SHOW_HOST,ORCA_FANOUT_SLEEP
python3 tests/fake_ec2.py "$port" "$dir" >> "$log" 2>&1 &
fake=$!
i=0
until curl -s -o /dev/null "$ORCA_EC2_ENDPOINT/"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the stand-in EC2 didn't start"; sleep 0.1
done
# ec2 ACTION ARGS...: the stand-in's answer.
ec2() { a=$1; shift; curl -s -d "Action=$a$(printf '&%s' "$@")" "$ORCA_EC2_ENDPOINT/"; }
running_of() {  # how many running instances are tagged orca.run=$1
    ec2 DescribeInstances Filter.1.Name=tag-key Filter.1.Value.1=orca.run \
        Filter.2.Name=instance-state-name Filter.2.Value.1=running | python3 -c "
import re, sys
xml = sys.stdin.read()
items = re.findall(r'<instanceId>.*?</tagSet>', xml)
print(sum(1 for i in items if '<key>orca.run</key><value>$1</value>' in i))
"
}
ids_of() {  # the instance IDs of run $1 with role $2, from the calls' answers
    for f in "$dir"/*.sh; do
        grep -q "^# orca run $1 start [0-9]*: $2" "$f" && basename "$f" .sh
    done
}
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt "$2" ] || fail "waited for: $1"; sleep 0.5
    done
}

./build/orca register --image orca:test ./build/toy >> "$log" 2>&1 || fail "register the toy"
./build/orca register --image orca:test --head-type c7g.large --worker-type c7g.xlarge \
    --workers 3 --spot ./build/fanout >> "$log" 2>&1 || fail "register fanout"
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
./build/orca backfill fanout 2024-01..2024-02 >> "$log" 2>&1 || fail "backfill fanout"
./build/orca backfill toy 2024-03 >> "$log" 2>&1 || fail "backfill the toy"
f1=$(q "SELECT min(id) FROM orca.runs WHERE pipeline = 'fanout'")
f2=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'fanout'")

# The first fanout run: its instances, and its tasks on all 3 workers.
until_true "SELECT status = 'running' AND heartbeat IS NOT NULL FROM orca.runs WHERE id = $f1" 120
calls=$(grep '"RunInstances"' "$dir/calls.jsonl")
made=$(echo "$calls" | python3 -c "
import json, sys, base64
for line in sys.stdin:
    p = json.loads(line)['params']
    tags = {p[k]: p[k.replace('Key', 'Value')] for k in p if k.startswith('TagSpecification.1.Tag.') and k.endswith('.Key')}
    if tags.get('orca.run') == '$f1':
        ud = base64.b64decode(p['UserData']).decode()
        print(tags['orca.role'], p['InstanceType'], p['MaxCount'], p.get('InstanceMarketOptions.MarketType', 'on-demand'),
              p['InstanceInitiatedShutdownBehavior'], 'DISTA_HEAD=127.0.0.1:6390' in ud)
")
[ "$made" = "head c7g.large 1 on-demand terminate True
worker c7g.xlarge 3 spot terminate True" ] || fail "run $f1's RunInstances: $made"
workers=$(ids_of "$f1" worker)
[ "$(echo "$workers" | wc -l | tr -d ' ')" -eq 3 ] || fail "3 worker instances: $workers"
grep -q "DISTA_HEAD=127.0.0.1:6390" "$dir/$(echo "$workers" | head -1).sh" || fail "a worker's DISTA_HEAD"
i=0
while :; do
    n=0
    for w in $workers; do grep -q "trips on $w" "$dir/$w.log" && n=$((n + 1)); done
    [ "$n" -eq 3 ] && break
    i=$((i + 1)); [ "$i" -lt 120 ] || fail "run $f1's tasks reached $n of 3 workers"; sleep 0.5
done
# A Spot interruption: one worker gone, mid-run.
ec2 TerminateInstances "InstanceId.1=$(echo "$workers" | head -1)" > /dev/null
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $f1" 240
[ "$(q "SELECT status FROM orca.runs WHERE id = $f1")" = succeeded ] || fail "run $f1 after losing a worker"
i=0
while [ "$(running_of "$f1")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 180 ] || fail "run $f1's instances are still running"; sleep 0.5
done
grep -q "^run succeeded" "$ORCA_LOGS/fanout/$f1/driver.log" || fail "run $f1's driver log"
echo "ok   a fanout run on a head and 3 Spot workers, its tasks on all 3, one lost, nothing left"

# The second: its head instance gone mid-run.
until_true "SELECT status = 'running' AND heartbeat IS NOT NULL FROM orca.runs WHERE id = $f2" 120
sleep 2
ec2 TerminateInstances "InstanceId.1=$(ids_of "$f2" head | head -1)" > /dev/null
until_true "SELECT status = 'succeeded' FROM orca.runs WHERE id = $f2" 300
[ "$(q "SELECT starts FROM orca.runs WHERE id = $f2")" -eq 2 ] || fail "run $f2 started $(q "SELECT starts FROM orca.runs WHERE id = $f2") times"
grep -l "^# orca run $f2 start 2: head" "$dir"/*.sh > /dev/null || fail "no new head for run $f2"
echo "ok   a run whose head instance went was restarted on new instances"

# The budget, and an instance of no run.
until_true "SELECT status = 'succeeded' FROM orca.runs WHERE pipeline = 'toy'" 120
[ "$(q "SELECT count(*) FROM orca.runs t JOIN orca.runs f ON f.pipeline = 'fanout' WHERE t.pipeline = 'toy' AND t.started < f.ended AND f.started < t.ended")" -eq 0 ] \
    || fail "the toy ran beside a fanout run over the budget"
ud=$(printf '#!/bin/sh\nsleep 600\n' | base64 | tr -d '\n')
ec2 RunInstances MaxCount=1 InstanceType=t4g.nano "UserData=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$ud")" \
    TagSpecification.1.Tag.1.Key=orca.run TagSpecification.1.Tag.1.Value=999 \
    TagSpecification.1.Tag.2.Key=orca.start TagSpecification.1.Tag.2.Value=1 > /dev/null
i=0
while [ "$(running_of 999)" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 60 ] || fail "the instance of no run is still running"; sleep 0.5
done
echo "ok   the budget kept the toy waiting, and an instance of no run was terminated"
stop
echo "ok   ec2"
