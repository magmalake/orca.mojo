#!/bin/sh
# Placement (orca register --placement, orca.control): the scheduler, on the
# ec2 runner against the stand-in EC2 (tests/fake_ec2.py), with this machine
# as its control host. toy and fanout are placed on the control host, daily
# keeps the scheduler's runner:
#
# - the flag is stored, kept by a registration without it, and reset by
#   `default`; a bad value is refused;
# - a control-placed run waits, held with the reason, while the control
#   instance's newest minute (made-up orca.system_minutes rows) has memory
#   or a disk over 85%, and starts once it hasn't;
# - control-placed runs start no instance, while daily's run starts one; at
#   most ORCA_CONTROL_MAX_RUNS (2) run at once, a third held until one ends;
#   /ops/fleet lists them on the control host, and the pages and JSON say where
#   each run was placed;
# - fanout's secret, read from the stand-in's Secrets Manager, reaches its
#   step and its tasks (forked locally), and no log holds its value;
# - a run whose process is killed is noticed and run again, and succeeds;
# - a run over ORCA_CONTROL_MEMORY_MB is killed, saying so, and a run whose
#   secret can't be read fails to start, saying so.
#
# Needs ORCA_DATABASE_URL (pixi run test-placement starts a throwaway
# server), python3, curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
for b in toy fanout daily; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src $DEP_INC "examples/$b.mojo" -o "build/$b"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/placement.log
: > "$log"
dir=$PWD/build/fake-ec2-placement
rm -rf "$dir" build/placement-logs build/placement-stage
mkdir -p "$dir"
port=$((38000 + $$ % 1000))
uiport=$((port + 1000))
base="http://127.0.0.1:$uiport"
fake=
sched=
ui=
stop_sched() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$sched" ] || wait "$sched" 2> /dev/null || true
    sched=
}
stop() {
    stop_sched
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    [ -z "$fake" ] || kill "$fake" 2> /dev/null || true
    ui=
    fake=
}
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    stop
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -qAtc "$1"; }
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt "$2" ] || fail "waited for: $1"; sleep 0.25
    done
}
# A file the scheduler writes once a run's process has been reaped.
wait_file() {
    i=0
    until [ -s "$1" ]; do
        i=$((i + 1)); [ "$i" -lt 80 ] || fail "no $1"; sleep 0.25
    done
}
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }
value=s3cr3t-orca-github-token-value
export ORCA_RUNNER=ec2 ORCA_EC2_MODE=direct ORCA_EC2_ENDPOINT="http://127.0.0.1:$port"
export ORCA_SECRETS_ENDPOINT="$ORCA_EC2_ENDPOINT"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test ORCA_EC2_AMI=ami-test
export ORCA_STALE_SECONDS=4 ORCA_START_SECONDS=60 ORCA_EC2_POOL_SECONDS=0
export ORCA_LOGS=$PWD/build/placement-logs ORCA_LOG_STAGE=$PWD/build/placement-stage
export ORCA_CLUSTER_LOG_SECONDS=1 ORCA_FLEET_SECONDS=0
# The memory-pressure hold below seeds a 95 % /ops/control minute, which would
# also trip the watchdog's control rule (orca.watchdog) and pause every
# pipeline before placement could hold the run: this test is placement's.
export ORCA_WATCHDOG_SECONDS=0
# The scheduler's clock stays where the pipelines were registered, so none
# of their schedules fires.
export ORCA_NOW=2024-01-15T00:00Z
export ORCA_TOY_SLEEP=1 ORCA_FANOUT_SECRET=GITHUB_TOKEN ORCA_FANOUT_SLEEP=0.05
unset ORCA_SECRET_FETCH ORCA_CONTROL_MAX_RUNS ORCA_CONTROL_MEMORY_MB ORCA_CONTROL_WORKERS
python3 tests/fake_ec2.py "$port" "$dir" >> "$log" 2>&1 &
fake=$!
i=0
until curl -s -o /dev/null "$ORCA_EC2_ENDPOINT/"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the stand-in EC2 didn't start"; sleep 0.1
done

# The flag, stored with the registration.
placement() { q "SELECT coalesce(placement, 'null') FROM orca.pipelines WHERE name = '$1'"; }
if ./build/orca register --placement local ./build/toy >> "$log" 2>&1; then
    fail "registered --placement local"
fi
./build/orca register --placement control ./build/toy >> "$log" 2>&1 || fail "register the toy"
[ "$(placement toy)" = control ] || fail "toy's placement: $(placement toy)"
./build/orca register ./build/toy >> "$log" 2>&1 || fail "register the toy again"
[ "$(placement toy)" = control ] || fail "a registration without --placement changed it: $(placement toy)"
./build/orca register --placement default ./build/toy >> "$log" 2>&1 || fail "register the toy by default"
[ "$(placement toy)" = null ] || fail "--placement default kept $(placement toy)"
./build/orca register --placement ec2 ./build/toy >> "$log" 2>&1 || fail "register the toy on ec2"
[ "$(placement toy)" = ec2 ] || fail "toy's placement: $(placement toy)"
./build/orca register --placement control ./build/toy >> "$log" 2>&1 || fail "register the toy"
./build/orca register --image orca:test --placement control --secret GITHUB_TOKEN=orca/github-token ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
./build/orca register --image orca:test ./build/daily >> "$log" 2>&1 || fail "register daily"
echo "ok   --placement is stored, kept, and reset by default"

ORCA_UI_ADDR="127.0.0.1:$uiport" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json /pipelines/toy | jq -e '.pipeline.placement == "control"' > /dev/null || fail "toy's JSON placement"
json /pipelines/daily | jq -e '.pipeline.placement == "default"' > /dev/null || fail "daily's JSON placement"
curl -fsS "$base/pipelines/toy" | grep -q "on the scheduler's host" || fail "toy's page doesn't show its placement"

# The control instance's newest minute: memory at $1 %, the fullest disk at $2 %.
minute() {
    q "DELETE FROM orca.system_minutes"
    q "INSERT INTO orca.system_minutes VALUES (date_trunc('minute', now()), 1, now(), 5, 5, $1, $1, $1::int8 * 40000000, 4000000000, $2, $2::int8 * 200000000, 20000000000, '/var/lib/orca/stage', 0, 0)"
}
minute 95 10
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
./build/orca backfill toy 2024-01 >> "$log" 2>&1 || fail "backfill the toy"
first=$(q "SELECT max(id) FROM orca.runs")
until_true "SELECT held LIKE '%memory is 95% used, over 85%' FROM orca.runs WHERE id = $first" 40
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $first")" = "queued 0" ] || fail "the held run started"
json "/runs/$first" | jq -e '.run.held | test("memory is 95% used")' > /dev/null || fail "the run's JSON doesn't say why it's held"
curl -fsS "$base/runs/$first" | grep -q "memory is 95% used" || fail "the run's page doesn't say why it's held"
minute 40 90
until_true "SELECT held LIKE '%disk /var/lib/orca/stage is 90% full%' FROM orca.runs WHERE id = $first" 40
minute 40 10
until_true "SELECT status = 'succeeded' FROM orca.runs WHERE id = $first" 120
[ "$(q "SELECT coalesce(held, 'none') || ' ' || placement FROM orca.runs WHERE id = $first")" = "none control" ] \
    || fail "run $first: $(q "SELECT held, placement FROM orca.runs WHERE id = $first")"
wait_file "$ORCA_LOGS/toy/$first/driver.log"
grep -q "^--- start 1$" "$ORCA_LOGS/toy/$first/driver.log" || fail "run $first's driver.log"
[ -s "$ORCA_LOGS/toy/$first/metrics/control.jsonl" ] || fail "run $first has no samples"
echo "ok   a control-placed run is held while memory or a disk is over 85%, then runs"

# Three control-placed runs and one on EC2.
./build/orca backfill toy 2024-02..2024-03 >> "$log" 2>&1 || fail "backfill the toy"
./build/orca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill fanout"
./build/orca backfill daily 2024-01-01 >> "$log" 2>&1 || fail "backfill daily"
most=0
held=0
fleet=0
i=0
while [ "$(q "SELECT count(*) FROM orca.runs WHERE status NOT IN ('succeeded', 'failed', 'cancelled')")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 600 ] || fail "the runs didn't end"
    n=$(q "SELECT count(*) FROM orca.runs WHERE status = 'running' AND placement = 'control'")
    [ "$n" -le "$most" ] || most=$n
    if [ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND held LIKE '%2 of 2 control-placed runs%'")" -gt 0 ]; then
        held=1
    fi
    if [ "$n" -eq 2 ] && [ "$fleet" -eq 0 ]; then
        live=$(q "SELECT string_agg(id::text, ',' ORDER BY id) FROM orca.runs WHERE status = 'running' AND placement = 'control'")
        got=$(json /api/fleet | jq -r '[.hosts[] | select(.role == "control") | .runs[]] | map(tostring) | join(",")')
        [ "$got" = "$live" ] && fleet=1
    fi
    sleep 0.2
done
[ "$most" -eq 2 ] || fail "$most control-placed runs ran at once"
[ "$held" -eq 1 ] || fail "no run was held for a control slot"
[ "$fleet" -eq 1 ] || fail "/ops/fleet didn't list the control host's runs"
[ "$(q "SELECT count(*) FROM orca.runs WHERE status <> 'succeeded'")" -eq 0 ] \
    || fail "runs that didn't succeed: $(q "SELECT id, pipeline, status, error FROM orca.runs WHERE status <> 'succeeded'")"
[ "$(q "SELECT count(*) FROM orca.instances i JOIN orca.runs r ON r.id = i.run WHERE r.pipeline <> 'daily'")" -eq 0 ] \
    || fail "a control-placed run started an instance"
daily=$(q "SELECT id FROM orca.runs WHERE pipeline = 'daily'")
[ "$(q "SELECT count(*) FROM orca.instances WHERE run = $daily")" -ge 1 ] || fail "daily's run started no instance"
[ "$(q "SELECT placement FROM orca.runs WHERE id = $daily")" = ec2 ] || fail "daily's run's placement"
[ "$(grep -c '"action": "RunInstances"' "$dir/calls.jsonl")" -eq "$(q "SELECT count(*) FROM orca.instances")" ] \
    || fail "instances were started for other runs"
json "/runs/$daily" | jq -e '.run.placement == "ec2" and .run.held == null' > /dev/null || fail "daily's run's JSON"
json "/runs/$first" | jq -e '.run.placement == "control"' > /dev/null || fail "run $first's JSON"
curl -fsS "$base/runs/$first" | grep -q "on the control host" || fail "run $first's page doesn't say where it ran"
echo "ok   control-placed runs start no instance, two at a time, listed on /ops/fleet's control host"

fanout=$(q "SELECT id FROM orca.runs WHERE pipeline = 'fanout'")
wait_file "$ORCA_LOGS/fanout/$fanout/driver.log"
i=0
while [ -n "$(ls "$ORCA_LOG_STAGE/control" 2> /dev/null)" ]; do
    i=$((i + 1)); [ "$i" -lt 80 ] || break; sleep 0.25
done
grep -q '"action": "GetSecretValue", "params": {"SecretId": "orca/github-token"}' "$dir/calls.jsonl" \
    || fail "the secret wasn't read from Secrets Manager"
grep -rq "secret \*\*\* of length ${#value}" "$ORCA_LOGS/fanout/$fanout" || fail "the step didn't see the secret"
grep -q "^secret \*\*\*$" "$ORCA_LOGS/fanout/$fanout/driver.log" || fail "the tasks didn't see the secret"
if grep -rq "$value" "$ORCA_LOGS" "$ORCA_LOG_STAGE"; then
    fail "the secret's value is in $(grep -rl "$value" "$ORCA_LOGS" "$ORCA_LOG_STAGE")"
fi
[ -z "$(ls "$ORCA_LOG_STAGE/control" 2> /dev/null)" ] || fail "staging files are left: $(ls "$ORCA_LOG_STAGE/control")"
echo "ok   a control-placed run's secret reaches its step and tasks, and no log holds it"

# A run whose process is killed mid-step is run again.
./build/orca backfill toy 2024-04 >> "$log" 2>&1 || fail "backfill the toy"
killed=$(q "SELECT max(id) FROM orca.runs")
until_true "SELECT count(*) > 0 FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $killed AND a.status = 'running'" 120
pid=$(q "SELECT split_part(owner, ':', 2) FROM orca.runs WHERE id = $killed")
kill -9 "$pid" || fail "kill run $killed's process $pid"
until_true "SELECT status = 'succeeded' FROM orca.runs WHERE id = $killed" 160
[ "$(q "SELECT starts FROM orca.runs WHERE id = $killed")" -eq 2 ] || fail "run $killed started $(q "SELECT starts FROM orca.runs WHERE id = $killed") times"
[ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $killed AND a.error = 'interrupted: the process running it stopped'")" -ge 1 ] \
    || fail "the killed attempt doesn't say so"
echo "ok   a control-placed run whose process was killed is run again"
stop_sched

# Over its memory cap, and a secret that can't be read.
ORCA_CONTROL_MEMORY_MB=1 ./build/orca scheduler >> "$log" 2>&1 &
sched=$!
./build/orca backfill toy 2024-05 >> "$log" 2>&1 || fail "backfill the toy"
big=$(q "SELECT max(id) FROM orca.runs")
until_true "SELECT EXISTS (SELECT 1 FROM orca.runs WHERE id = $big AND error LIKE '%over the control instance''s cap of 1 MB a run (ORCA_CONTROL_MEMORY_MB)') OR EXISTS (SELECT 1 FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $big AND a.error LIKE 'killed: %over the control instance''s cap of 1 MB%')" 120
echo "ok   a control-placed run over its memory cap is killed, saying why"
./build/orca register --image orca:test --placement control --secret GITHUB_TOKEN=orca/missing ./build/fanout >> "$log" 2>&1 \
    || fail "register fanout"
./build/orca backfill fanout 2024-02 >> "$log" 2>&1 || fail "backfill fanout"
nosecret=$(q "SELECT max(id) FROM orca.runs")
until_true "SELECT status = 'failed' AND error LIKE 'can''t start: can''t read secret orca/missing: 400%' FROM orca.runs WHERE id = $nosecret" 80
echo "ok   a control-placed run whose secret can't be read doesn't start"
stop
echo "ok   placement"
