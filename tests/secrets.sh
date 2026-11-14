#!/bin/sh
# Secrets for runs (morca.secrets), on the ec2 runner against the stand-in EC2
# (tests/fake_ec2.py), with tests/fake_secret.sh standing in for Secrets
# Manager. fanout, registered with --secret GITHUB_TOKEN=orca/github-token,
# prints the token on its workers and in its driver, and logs it in a step
# (MORCA_FANOUT_SECRET): the run succeeds and its tasks saw the value, yet no
# log under MORCA_LOGS (driver, attempts, head and workers) holds it, and
# Postgres holds only its ID. A pipeline whose secret can't be read fails.
# Then the same on the local runner: fanout's next run gets the value and no
# log holds it, and the toy's fails with "can't start: can't read secret".
#
# Needs MORCA_DATABASE_URL (pixi run test-secrets starts a throwaway server),
# python3 and curl.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
one_cluster_test
mkdir -p build
scripts/pipelines.sh fanout
scripts/pipelines.sh toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/secrets.log
: > "$log"
dir=$PWD/build/fake-ec2-secrets
rm -rf "$dir" build/secrets-logs
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
value=s3cr3t-orca-github-token-value
export MORCA_RUNNER=ec2 MORCA_EC2_MODE=direct MORCA_EC2_ENDPOINT="http://127.0.0.1:$port"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test MORCA_EC2_AMI=ami-test
export MORCA_STALE_SECONDS=10 MORCA_START_SECONDS=60
export MORCA_LOGS=$PWD/build/secrets-logs MORCA_CLUSTER_LOG_SECONDS=1
export MORCA_SECRET_FETCH=$PWD/tests/fake_secret.sh
export MORCA_FANOUT_SECRET=GITHUB_TOKEN MORCA_FANOUT_SLEEP=0.3 MORCA_RUN_ENV=MORCA_FANOUT_SECRET,MORCA_FANOUT_SLEEP
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

if ./build/morca register --secret github_token=orca/x ./build/fanout >> "$log" 2>&1; then
    fail "registered a lower-case secret name"
fi
if ./build/morca register --secret GITHUB_TOKEN=prod/x ./build/fanout >> "$log" 2>&1; then
    fail "registered a secret outside orca/"
fi
./build/morca register --image morca:test --workers 2 --secret GITHUB_TOKEN=orca/github-token ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
./build/morca register --image morca:test --secret TOY_TOKEN=orca/missing ./build/toy >> "$log" 2>&1 \
    || fail "register the toy"
[ "$(q "SELECT secrets->>'GITHUB_TOKEN' FROM orca.pipelines WHERE name = 'fanout'")" = orca/github-token ] \
    || fail "fanout's secret"
./build/morca scheduler >> "$log" 2>&1 &
sched=$!
./build/morca backfill fanout 2024-02 >> "$log" 2>&1 || fail "backfill fanout"
./build/morca backfill toy 2024-02 >> "$log" 2>&1 || fail "backfill the toy"
run=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout'")
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $run" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] || fail "run $run"
# Its last logs are uploaded as its instances end, or go idle in the warm
# pool (morca.ec2).
i=0
until [ "$(q "SELECT count(*) FROM orca.instances WHERE run = $run AND ended IS NULL AND idle IS NULL")" -eq 0 ]; do
    i=$((i + 1)); [ "$i" -lt 120 ] || fail "run $run's instances are still up"; sleep 0.5
done

# The value reached the step and the tasks, masked wherever it was logged.
grep -rq "secret \*\*\* of length ${#value}" "$MORCA_LOGS/fanout/$run" || fail "the step's log didn't see the secret"
n=$(cat "$MORCA_LOGS/fanout/$run/cluster/"worker-*.log | grep -c "^secret \*\*\*$" || true)
# The last second of a worker's output can go with its instance.
[ "$n" -ge 10 ] || fail "the workers' logs show only $n masked secrets"
grep -q "secret \*\*\*" "$MORCA_LOGS/fanout/$run/driver.log" || fail "the driver's log"
if grep -rq "$value" "$MORCA_LOGS"; then
    fail "the secret's value is in $(grep -rl "$value" "$MORCA_LOGS")"
fi
if q "SELECT row_to_json(p)::text FROM orca.pipelines p" | grep -q "$value"; then
    fail "Postgres holds the secret's value"
fi
echo "ok   a run's secret reached its step and tasks, and no log holds it"

toy=$(q "SELECT id FROM orca.runs WHERE pipeline = 'toy'")
until_true "SELECT count(*) > 0 FROM orca.instances WHERE run = $toy AND ended IS NOT NULL" 120
grep -rq "can't read secret orca/missing" "$dir" || fail "the toy's instance didn't say why"
echo "ok   an instance that can't read its secret stops"
stop

# The local runner: the scheduler reads the secrets as it starts each run.
unset MORCA_RUNNER MORCA_EC2_MODE MORCA_EC2_ENDPOINT
./build/morca scheduler >> "$log" 2>&1 &
sched=$!
./build/morca backfill fanout 2024-03 >> "$log" 2>&1 || fail "backfill fanout locally"
./build/morca backfill toy 2024-03 >> "$log" 2>&1 || fail "backfill the toy locally"
local=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout' AND id <> $run")
until_true "SELECT status IN ('succeeded', 'failed') FROM orca.runs WHERE id = $local" 300
[ "$(q "SELECT status FROM orca.runs WHERE id = $local")" = succeeded ] || fail "local run $local"
# keep-log appends the driver's output to driver.log once it has ended.
i=0
until grep -q "secret \*\*\*$" "$MORCA_LOGS/fanout/$local/driver.log" 2> /dev/null; do
    i=$((i + 1)); [ "$i" -lt 60 ] || fail "local run $local's driver.log"; sleep 0.5
done
grep -rq "secret \*\*\* of length ${#value}" "$MORCA_LOGS/fanout/$local" \
    || fail "the local step's log didn't see the secret"
if grep -rq "$value" "$MORCA_LOGS"; then
    fail "the secret's value is in $(grep -rl "$value" "$MORCA_LOGS")"
fi
echo "ok   a local run's secret reached its step, and no log holds it"
toy=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'toy'")
until_true "SELECT status = 'failed' AND error LIKE 'can''t start: can''t read secret orca/missing%' FROM orca.runs WHERE id = $toy" 60
echo "ok   a local run whose secret can't be read fails"
stop
echo "ok   secrets"
