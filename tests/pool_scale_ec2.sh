#!/bin/sh
# The pool autoscaler on EC2 (morca.machines_ec2, #267), end to end against
# tests/fake_ec2.py (each instance a local process running its boot script),
# with tests/fake_hcloud.py as the image registry only, and plain
# (tests/grants/grantee.mojo) placed on [pools.t]:
#
# - an x86 type the arm64-only image can't run is skipped, saying so;
# - with no capacity for the first type in either subnet, it keeps trying it
#   for its wait, then makes the next in the first subnet that has it, with
#   Ubuntu for its architecture, the pool's tags, the security group and
#   IMDSv2;
# - the instance boots, joins and runs the runs;
# - idle for idle_seconds, it's terminated, its token revoked;
# - a stray instance with the pool's and the stack's tags is terminated.
#
# Needs MORCA_DATABASE_URL (pixi run test-pool-scale-ec2), curl, jq, python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC -D GRANTEE=plain tests/grants/grantee.mojo -o build/plain
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/pool-ec2.log
: > "$log"
dir=$PWD/build/pool-ec2
rm -rf "$dir"
mkdir -p "$dir/ec2" "$dir/reg"
export MORCA_WAREHOUSE="$dir/wh" MORCA_LOGS="$dir/logs" GRANTEE_OUT="$dir/out" GRANTEE_SLEEP=2
mkdir -p "$GRANTEE_OUT"
export MORCA_STALE_SECONDS=5 MORCA_START_SECONDS=20 MORCA_SAMPLE_SECONDS=0 MORCA_WATCHDOG_SECONDS=0
unset MORCA_RUNNER MORCA_CONFIG MORCA_EC2_SUBNET MORCA_EC2_SECURITY_GROUP MORCA_EC2_PROFILE
free_port eport
free_port rport
free_port uport
base="http://127.0.0.1:$uport"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=AKIDTEST AWS_SECRET_ACCESS_KEY=test-secret
export MORCA_EC2_ENDPOINT="http://127.0.0.1:$eport" MORCA_SSM_ENDPOINT="http://127.0.0.1:$eport"
export MORCA_REGISTRY_ENDPOINT="http://127.0.0.1:$rport"
export MORCA_WORKER_IMAGE="ghcr.io/morca-dev/census:t1"
export MORCA_POOL_EC2_SECURITY_GROUP=sg-pool MORCA_POOL_EC2_PROFILE=orca-pool-worker
pids=
cleanup() { for p in $pids; do kill "$p" 2> /dev/null || true; done; }
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    tail -20 build/pool-ec2-sched.out 2> /dev/null || true
    for f in "$dir"/ec2/i-*.log; do [ -f "$f" ] && { echo "== $f"; tail -10 "$f"; }; done
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
is() { [ "$(q "$1")" = "$2" ]; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
status() { q "SELECT coalesce(status, '') FROM orca.pools WHERE name = 't'"; }
has_status() { status | grep -qF "$1"; }
runs_of() { jq -c "select(.action == \"RunInstances\") | .params" "$dir/ec2/calls.jsonl"; }
live() { # the fake's instances with tag orca-pool=t that run
    curl -fsS -d 'Action=DescribeInstances&Filter.1.Name=tag:orca-pool&Filter.1.Value.1=t&Filter.2.Name=instance-state-name&Filter.2.Value.1=running' \
        "$MORCA_EC2_ENDPOINT/" | grep -o '<instanceId>[^<]*' | sed 's/<instanceId>//' | tr '\n' ' '
}

env -u MORCA_DATABASE_URL python3 tests/fake_ec2.py "$eport" "$dir/ec2" &
pids="$pids $!"
python3 tests/fake_hcloud.py "$rport" "$dir/reg" &
pids="$pids $!"
until_ 10 "the fake EC2 didn't start" curl -s -o /dev/null "$MORCA_EC2_ENDPOINT/"
until_ 10 "the fake registry didn't start" curl -s -o /dev/null "$MORCA_REGISTRY_ENDPOINT/token"
MORCA_UI_ADDR="127.0.0.1:$uport" ./build/morca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "morca ui didn't start" curl -fs "$base/" -o /dev/null

cat > build/pool-ec2.toml << 'EOT'
[pools.t]
provider = "ec2"
machines = [{ type = "c7g.large", wait = 3 }, { type = "m7i.large" }]
locations = ["subnet-a", "subnet-b"]
max_machines = 2
idle_seconds = 10

[pipelines.plain]
placement = "worker:t"
EOT
./build/morca register --config build/pool-ec2.toml ./build/plain >> "$log" 2>&1 || fail "register"
MORCA_POOL_REMOTE="$base" MORCA_POOL_WORKER_COMMAND="$PWD/build/morca" \
MORCA_POOL_SCALE_SECONDS=0.5 MORCA_POOL_GROW_AFTER=0 MORCA_POOL_TRY_SECONDS=0.5 MORCA_POOL_IMAGE_SECONDS=0 \
    ./build/morca scheduler > build/pool-ec2-sched.out 2>&1 &
pids="$pids $!"

# The image is arm64 only: m7i (x86) is skipped; no c7g capacity anywhere.
echo arm64 > "$dir/reg/arches"
echo "m7i.large subnet-a" > "$dir/ec2/pool-stock"
./build/morca backfill plain 2024-01..2024-03 >> "$log" 2>&1 || fail "backfill"
until_ 20 "no waiting status: $(status)" has_status "waiting: no c7g.large in subnet-a, subnet-b yet"
has_status "m7i.large skipped: ghcr.io/morca-dev/census:t1 has no linux/amd64" || fail "the skip: $(status)"
[ -z "$(live)" ] || fail "an instance runs: $(live)"
echo "ok   an x86 type the image can't run is skipped; no capacity is waited on"

# Both architectures: c7g for its 3 s, then m7i in subnet-a.
echo "amd64 arm64" > "$dir/reg/arches"
until_ 30 "no m7i made: $(status)" has_status ": m7i.large in subnet-a"
is "SELECT server_type || ' ' || arch || ' ' || location FROM orca.pool_machines" "m7i.large amd64 subnet-a" \
    || fail "the machine: $(q "SELECT server_type, arch, location FROM orca.pool_machines")"
made=$(runs_of | tail -1)
for kv in '"ImageId":"ami-amd64"' '"InstanceType":"m7i.large"' '"SubnetId":"subnet-a"' \
    '"SecurityGroupId.1":"sg-pool"' '"IamInstanceProfile.Name":"orca-pool-worker"' \
    '"MetadataOptions.HttpTokens":"required"' '"InstanceInitiatedShutdownBehavior":"terminate"'; do
    echo "$made" | grep -qF "$kv" || fail "RunInstances without $kv: $made"
done
echo "$made" | jq -e 'to_entries | map(select(.key | test("Tag\\.[0-9]+\\.(Key|Value)"))) | map(.value) | index("orca-pool") != null and index("t") != null' > /dev/null \
    || fail "RunInstances without the pool's tag: $made"
tried=$(runs_of | jq -r '"\(.InstanceType) \(.SubnetId)"' | sort -u | tr '\n' ' ')
[ "$tried" = "c7g.large subnet-a c7g.large subnet-b m7i.large subnet-a " ] || fail "tried: $tried"
echo "ok   after c7g's wait with no capacity, m7i is made in subnet-a: Ubuntu amd64, tags, group, IMDSv2"

until_ 120 "the runs didn't succeed: $(q "SELECT id, status, held FROM orca.runs")" \
    is "SELECT count(*) FILTER (WHERE status = 'succeeded') FROM orca.runs" 3
is "SELECT count(DISTINCT r.worker) FROM orca.runs r JOIN orca.pool_machines m ON m.worker = r.worker" 1 \
    || fail "the runs didn't run on the instance"
echo "ok   the instance boots as a worker and runs the runs"

until_ 40 "the idle instance wasn't removed: $(q "SELECT name, ended, why FROM orca.pool_machines")" \
    is "SELECT why FROM orca.pool_machines WHERE ended IS NOT NULL" "idle 10 s"
[ -z "$(live)" ] || fail "it still runs: $(live)"
is "SELECT count(*) FROM orca.workers WHERE revoked IS NOT NULL" 1 || fail "its token isn't revoked"
echo "ok   an idle instance is terminated, its token revoked"

owner=$(q "SELECT id FROM orca.stack_identity")
rm -f "$dir/ec2/pool-stock"
data=$(printf '#!/bin/sh\nsleep 600\n' | base64 | tr -d '\n')
curl -fsS --data-urlencode "Action=RunInstances" --data-urlencode "MinCount=1" --data-urlencode "MaxCount=1" \
    --data-urlencode "InstanceType=c7g.large" --data-urlencode "UserData=$data" \
    --data-urlencode "TagSpecification.1.Tag.1.Key=orca-pool" --data-urlencode "TagSpecification.1.Tag.1.Value=t" \
    --data-urlencode "TagSpecification.1.Tag.2.Key=orca-owner" --data-urlencode "TagSpecification.1.Tag.2.Value=$owner" \
    "$MORCA_EC2_ENDPOINT/" > /dev/null || fail "make a stray"
no_live() { [ -z "$(live)" ]; }
until_ 20 "the stray wasn't terminated: $(live)" no_live
echo "ok   a stray instance with the pool's and the stack's tags is terminated"
echo "pool scale ec2: ok"
