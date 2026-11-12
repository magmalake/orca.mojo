#!/bin/sh
# morca ui on this machine, over runs made for it, to look around before
# anything runs on AWS:
#
#     pixi run playground          # then http://127.0.0.1:8270
#
# A throwaway Postgres (scripts/with-postgres.sh) gets the example
# pipelines, built with this checkout's commit so their runs link it; runs
# that succeed and fail, each step and task linked to its line on GitHub;
# and two fanout runs on the ec2 runner against the
# tests' stand-in EC2 (tests/fake_ec2.py), each "instance" a local process,
# whose workers scale from 0 to 3 and are drained when idle, so the run page
# shows its instances, their logs, its timeline and the workers over time. Then the scheduler keeps running the schedules, and
# the UI's actions need the login demo / demo-password. Ctrl-C ends it and
# removes everything. MORCA_UI_ADDR changes the address.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
if [ -z "${MORCA_DATABASE_URL:-}" ]; then
    exec scripts/with-postgres.sh "$0" "$@"
fi
mkdir -p build
repo=$(git remote get-url origin 2> /dev/null || echo "")
commit=$(git rev-parse HEAD 2> /dev/null || echo "")
dirty=0
git diff --quiet HEAD 2> /dev/null || dirty=1
echo "building the examples and morca ..."
for p in toy fanout daily; do
    # shellcheck disable=SC2086
    mojo build -I src $DEP_INC -D MORCA_SOURCE="$repo" -D MORCA_COMMIT="$commit" -D MORCA_DIRTY=$dirty \
        examples/$p.mojo -o build/$p
done
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/morca
# Where each step and task is, for the UI's links to the code.
./build/morca locate build/toy build/fanout build/daily --root . > /dev/null

dir=$PWD/build/playground
rm -rf "$dir"
mkdir -p "$dir/ec2"
log=$dir/playground.log
export MORCA_LOGS=$dir/logs MORCA_STALE_SECONDS=10
pids=
stop() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
}
trap stop EXIT INT TERM
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }

echo "making runs ..."
for p in toy fanout daily; do ./build/morca register ./build/$p >> "$log" 2>&1; done
./build/toy run --partitions 2024-01..2024-03 >> "$log" 2>&1
MORCA_TOY_FAIL=2024-05 ./build/toy run --partitions 2024-04..2024-06 >> "$log" 2>&1 || true
./build/daily run --partitions 2024-02-27..2024-03-02 >> "$log" 2>&1
./build/fanout run --partitions 2024-01 >> "$log" 2>&1

echo "two fanout runs on the stand-in EC2, their workers scaling from 0 to 3 ..."
free_port port
python3 tests/fake_ec2.py "$port" "$dir/ec2" >> "$log" 2>&1 &
fake=$!
pids="$fake"
until curl -s -o /dev/null "http://127.0.0.1:$port/"; do sleep 0.1; done
./build/morca register --image morca:playground --head-type c7g.large --worker-type c7g.2xlarge \
    --workers 0..3 ./build/fanout >> "$log" 2>&1
MORCA_RUNNER=ec2 MORCA_EC2_MODE=direct MORCA_EC2_ENDPOINT="http://127.0.0.1:$port" \
    AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=playground AWS_SECRET_ACCESS_KEY=playground \
    MORCA_EC2_AMI=ami-playground MORCA_CLUSTER_LOG_SECONDS=1 MORCA_FANOUT_SLEEP=0.3 \
    MORCA_FANOUT_SHOW_HOST=1 MORCA_FANOUT_PAUSE=6 MORCA_SCALE_UP_AFTER=1 MORCA_SCALE_IDLE=2 \
    MORCA_RUN_ENV=MORCA_FANOUT_SLEEP,MORCA_FANOUT_SHOW_HOST,MORCA_FANOUT_PAUSE \
    ./build/morca scheduler >> "$log" 2>&1 &
sched=$!
pids="$pids $sched"
./build/morca backfill fanout 2024-02..2024-03 >> "$log" 2>&1
i=0
until [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'fanout' AND trigger = 'backfill' AND status IN ('succeeded', 'failed')")" -eq 2 ]; do
    i=$((i + 1))
    if [ "$i" -gt 600 ]; then echo "the ec2 runs didn't finish; see $log"; break; fi
    sleep 0.5
done
# Their instances' last sweep, then back to running on this machine.
sleep 3
kill "$sched" 2> /dev/null || true
wait "$sched" 2> /dev/null || true
kill "$fake" 2> /dev/null || true
pids=

./build/morca scheduler >> "$log" 2>&1 &
pids="$!"
echo demo-password | ./build/morca user add demo >> "$log" 2>&1
addr=${MORCA_UI_ADDR:-127.0.0.1:8270}
echo
echo "morca ui on http://$addr  (actions: demo / demo-password; Ctrl-C to stop)"
MORCA_UI_ADDR=$addr MORCA_UI_URL="http://$addr" ./build/morca ui >> "$log" 2>&1
