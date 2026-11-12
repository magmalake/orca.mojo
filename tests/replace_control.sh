#!/bin/sh
# morca aws replace-control (morca.aws_replace) against the stand-in aws
# command (tests/fake_bin/aws), which records each call: the template's two
# control slots and their boot script; up pinning the slots' AMIs and
# refusing a stack from before the slots; --plan changing nothing; a refusal
# while a run is live; --wait; the backup, launch, check and switch issued
# in order, with the CloudFormation parameters that replace the instance and
# move the Elastic IP; the rollback window; a rollback after the switch and
# one before it; and retiring the old slot.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
morca=$PWD/build/morca
work=$PWD/build/replace-control-test
rm -rf "$work"
mkdir -p "$work/project" "$work/state"
export FAKE_AWS_LOG=$work/calls.log FAKE_AWS_STATE=$work/state PATH="$PWD/tests/fake_bin:$PATH"
export AWS_REGION=us-east-2 MORCA_REPLACE_POLL=0 MORCA_REPLACE_TRIES=3
: > "$FAKE_AWS_LOG"
fail() { echo "FAIL: $*"; exit 1; }
calls() { cat "$FAKE_AWS_LOG"; : > "$FAKE_AWS_LOG"; }
# The line number of the first call matching $2 in $1, or 0.
at() { echo "$1" | grep -nF -- "$2" | head -1 | cut -d: -f1 | grep . || echo 0; }
in_order() {
    got=$1
    shift
    prev=0
    for want in "$@"; do
        n=$(at "$got" "$want")
        [ "$n" -gt "$prev" ] || fail "not after the step before: $want (line $n, before $prev); calls:
$got"
        prev=$n
    done
}
cd "$work/project"
y=build/morca-aws/morca.yaml

# The template: two slots booting one script, the address on the active
# one, each slot's AMI pinned, and backups that runs can't read.
"$morca" aws up --email ops@example.com > ../up.out 2>&1 || fail "up: $(cat ../up.out)"
calls > /dev/null
grep -q '^  ControlBoot:' $y && grep -q 'Type: AWS::EC2::LaunchTemplate' $y || fail "no ControlBoot launch template"
grep -q '^  ControlGreen:' $y && grep -q 'Condition: GreenOn' $y || fail "no green slot"
grep -qF 'exec /usr/local/bin/orca-boot blue' $y && grep -qF 'exec /usr/local/bin/orca-boot green' $y || fail "the slots don't run the boot script"
grep -qF 'InstanceId: !If [GreenActive, !Ref ControlGreen, !Ref Control]' $y || fail "the address isn't on the active slot"
grep -qF 'ImageId: !If [BlueAmiPinned, !Ref BlueAmi, !Ref Ami]' $y || fail "blue's AMI isn't pinned"
grep -B2 -A2 'Effect: Deny' $y | grep -qF 'backups/*' || fail "runs may read the backups"
restore=$(grep -n 'restore-$slot' $y | head -1 | cut -d: -f1)
refresh=$(grep -n '^              orca-refresh$' $y | cut -d: -f1)
[ "$restore" -gt 0 ] && [ "$restore" -lt "$refresh" ] || fail "the boot doesn't restore before the scheduler starts"
grep -qF "printf '%s;' '\${ControlEnv}'" $y || fail "the boot doesn't write ControlEnv"
echo "ok   the template"

# up on a stack with slots pins their AMIs; --env is ControlEnv, and
# secrets or quotes are refused.
"$morca" aws up --env 'MORCA_UI_LINKS=Census ↗=https://census.mlake.dev' --env DISTA_WORKER_SLOTS=4 > ../up.out 2>&1 || fail "up --env: $(cat ../up.out)"
got=$(calls)
echo "$got" | grep -qF "BlueAmi=ami-0old" || fail "up didn't pin blue's AMI: $got"
echo "$got" | grep -qF "ControlEnv=MORCA_UI_LINKS=Census ↗=https://census.mlake.dev;DISTA_WORKER_SLOTS=4" || fail "up's ControlEnv: $got"
if "$morca" aws up --env MORCA_DB_PASSWORD=x > ../up.out 2>&1; then fail "up took a password as a setting"; fi
if "$morca" aws up --env "A=it's" > ../up.out 2>&1; then fail "up took a quote"; fi
if "$morca" aws up --env 'a=b' > ../up.out 2>&1; then fail "up took a lower-case key"; fi
calls | grep -q "cloudformation deploy" && fail "a refused --env deployed"
touch "$FAKE_AWS_STATE/preslot"
if "$morca" aws up > ../up.out 2>&1; then fail "up updated a stack from before the slots without --yes"; fi
grep -q "predates the control instance's slots" ../up.out || fail "up's message: $(cat ../up.out)"
calls | grep -q "cloudformation deploy" && fail "up deployed without --yes"
"$morca" aws up --yes > /dev/null 2>&1 || fail "up --yes"
calls | grep -qF "BlueAmi=ami-0old" || fail "up --yes didn't pin the old instance's AMI"
echo "ok   up"

# --plan: the steps, nothing done.
touch "$FAKE_AWS_STATE/preslot"
"$morca" aws replace-control --plan --smoke daily=2024-01-01 > ../plan.out 2>&1 || fail "plan: $(cat ../plan.out)"
got=$(calls)
for s in "1. quiet" "2. freeze" "3. backup" "4. launch" "5. boot" "6. check" "7. switch" "8. retire" "rollback"; do
    grep -qF "$s" ../plan.out || fail "the plan has no $s: $(cat ../plan.out)"
done
grep -qF "replace i-0control (slot blue) with a new instance in slot green" ../plan.out || fail "the plan's slots: $(cat ../plan.out)"
grep -qF "GreenSlot=true" ../plan.out && grep -qF "ActiveControl=green" ../plan.out && grep -qF "BlueSlot=false" ../plan.out || fail "the plan's deploys"
grep -qF "a smoke run, daily=2024-01-01" ../plan.out || fail "the plan's smoke run"
echo "$got" | grep -qE "send-command|cloudformation deploy|stop-instances" && fail "--plan did something: $got"
[ ! -f build/morca-aws/replace-morca.state ] || fail "--plan wrote a state file"
echo "ok   plan"

# Refused while a run is live, and while a pipeline isn't paused; nothing
# stopped or deployed.
touch "$FAKE_AWS_STATE/live"
if "$morca" aws replace-control > ../rc.out 2>&1; then fail "replaced with a live run"; fi
got=$(calls)
grep -qF "running: daily run 7" ../rc.out || fail "the refusal doesn't say what's live: $(cat ../rc.out)"
echo "$got" | grep -qF "# morca-replace quiet" || fail "no quiet check: $got"
echo "$got" | grep -qE "morca-replace freeze|cloudformation deploy" && fail "a refusal went on: $got"
if "$morca" aws replace-control > ../rc.out 2>&1; then fail "a second start while one is in progress"; fi
grep -q "in progress" ../rc.out || fail "the in-progress message: $(cat ../rc.out)"
rm "$FAKE_AWS_STATE/live"
touch "$FAKE_AWS_STATE/unpaused"
if "$morca" aws replace-control --resume --wait > ../rc.out 2>&1; then fail "replaced with a pipeline not paused"; fi
grep -qF "not paused: hourly" ../rc.out || fail "the refusal doesn't name the pipeline: $(cat ../rc.out)"
rm "$FAKE_AWS_STATE/unpaused"
calls > /dev/null
echo "ok   refused unless quiet"

# --wait for the run, then every step to the switch, in order.
echo drain > "$FAKE_AWS_STATE/live"
"$morca" aws replace-control --resume --wait --smoke daily=2024-01-01 --env DISTA_WORKER_SLOTS=4 \
    --hostname morca.example.dev --console-hostname census.example.dev > ../rc.out 2>&1 || fail "replace: $(cat ../rc.out)"
got=$(calls)
[ "$(echo "$got" | grep -c '# morca-replace quiet')" = 2 ] || fail "--wait didn't check again: $got"
backup=$(echo "$got" | grep -F '# morca-replace backup')
echo "$backup" | grep -qF 'pg_dump -U orca -d orca -n orca -Fc' || fail "no pg_dump: $backup"
echo "$backup" | grep -qF 'pg_restore -U orca -d morca_verify' || fail "the dump isn't verified: $backup"
echo "$backup" | grep -qE "s3://morca-123456789012-us-east-2/backups/morca/[0-9]{8}T[0-9]{6}Z" || fail "the backup's place: $backup"
echo "$backup" | grep -qF 's3://morca-123456789012-us-east-2/backups/morca/restore-green' || fail "no pointer for green: $backup"
echo "$backup" | grep -qF "console census.example.dev 8080" || fail "the console isn't carried over: $backup"
launch=$(echo "$got" | grep -F 'cloudformation deploy' | head -1)
for p in GreenSlot=true BlueSlot=true ActiveControl=blue GreenAmi=ami-0new BlueAmi=ami-0old ControlEnv=DISTA_WORKER_SLOTS=4 \
    Hostname=morca.example.dev ConsoleHostname=census.example.dev; do
    echo "$launch" | grep -qF -- " $p" || fail "the launch has no $p: $launch"
done
check=$(echo "$got" | grep -F '# morca-replace check')
echo "$check" | grep -qF 'i-0green' || fail "the check isn't on the new instance"
echo "$backup" | grep -qF "b.sites" || fail "the backup has no Caddy sites"
for c in "replaced.sites" "/var/lib/orca/restored" "orca.schema_version" "http://ui:8270/api/system" "orca.system_minutes" "morca status" "comm -23" "morca start 'daily' '2024-01-01'"; do
    echo "$check" | grep -qF -- "$c" || fail "the check has no $c"
done
in_order "$got" "# morca-replace quiet" "# morca-replace freeze" "# morca-replace backup" " GreenSlot=true" \
    "# morca-replace boot" "# morca-replace check" " ActiveControl=green" "ec2 describe-addresses" \
    "ec2 stop-instances --instance-ids i-0control" "s3 rm --only-show-errors s3://morca-123456789012-us-east-2/backups/morca/restore-green" \
    "curl -fsS -o /dev/null --max-time 10 https://3-14-15-92.sslip.io"
echo "$got" | grep -qF "BlueSlot=false" && fail "retired the old instance at once"
grep -q "kept until" ../rc.out || fail "no rollback window: $(cat ../rc.out)"
grep -q "^20[0-9-]*T.* switch: done" build/morca-aws/replace-morca.log || fail "the log has no switch"
echo "ok   replace"

# Within the window --resume retires nothing.
"$morca" aws replace-control --resume > ../rc.out 2>&1 || fail "resume: $(cat ../rc.out)"
calls | grep -qF "cloudformation deploy" && fail "retired inside the window"
echo "ok   the window"

# A rollback after the switch: the new instance's database back on the old.
"$morca" aws replace-control --rollback > ../rb.out 2>&1 || fail "rollback: $(cat ../rb.out)"
got=$(calls)
in_order "$got" "# morca-replace rollback-quiet" "# morca-replace rollback-freeze" "# morca-replace rollback-backup" \
    "ec2 start-instances --instance-ids i-0control" "# morca-replace rollback-restore" " ActiveControl=blue" " GreenSlot=false"
echo "$got" | grep -F '# morca-replace rollback-freeze' | grep -qF "i-0green" || fail "froze the wrong instance"
restore=$(echo "$got" | grep -F '# morca-replace rollback-restore')
echo "$restore" | grep -qF "i-0control" || fail "restored on the wrong instance"
echo "$restore" | grep -qE "backups/morca/[0-9]{8}T[0-9]{6}Z-back'?\.dump" || fail "the rollback's restore: $restore"
echo "$restore" | grep -qF "orca-refresh" || fail "the rollback doesn't start the scheduler"
[ "$(cat "$FAKE_AWS_STATE/active")" = blue ] && [ ! -f "$FAKE_AWS_STATE/green" ] || fail "the stack after the rollback"
[ ! -f build/morca-aws/replace-morca.state ] || fail "the state file is left after the rollback"
echo "ok   rollback after the switch"

# Again, then retire once the window has passed (MORCA_NOW, far ahead).
"$morca" aws replace-control > ../rc.out 2>&1 || fail "replace again: $(cat ../rc.out)"
calls > /dev/null
MORCA_NOW=4102444800 "$morca" aws replace-control --resume > ../rc.out 2>&1 || fail "retire: $(cat ../rc.out)"
got=$(calls)
echo "$got" | grep -F "cloudformation deploy" | grep -qF " BlueSlot=false" || fail "didn't retire blue: $got"
[ -f "$FAKE_AWS_STATE/blue-off" ] && [ "$(cat "$FAKE_AWS_STATE/active")" = green ] || fail "the stack after retiring"
ls build/morca-aws/replace-morca.state.done-* > /dev/null || fail "the state file isn't kept as done"
echo "ok   retire"

# From green back to blue, a check that fails, and a rollback before the
# switch: the new slot off, the old scheduler and UI started again.
"$morca" aws replace-control --plan > ../plan.out 2>&1
grep -qF "replace i-0green (slot green) with a new instance in slot blue" ../plan.out || fail "the next plan: $(cat ../plan.out)"
touch "$FAKE_AWS_STATE/unhealthy"
calls > /dev/null
if "$morca" aws replace-control --keep-old 0 > ../rc.out 2>&1; then fail "switched to an unhealthy instance"; fi
got=$(calls)
grep -qF "FAIL: the UI does not answer" ../rc.out || fail "the check's failure isn't shown: $(cat ../rc.out)"
echo "$got" | grep -qF " ActiveControl=blue" && fail "switched after a failed check"
echo "$got" | grep -F "cloudformation deploy" | grep -qF " BlueSlot=true" || fail "blue wasn't launched: $got"
rm "$FAKE_AWS_STATE/unhealthy"
"$morca" aws replace-control --rollback > ../rb.out 2>&1 || fail "rollback: $(cat ../rb.out)"
got=$(calls)
in_order "$got" " BlueSlot=false" "# morca-replace rollback" "s3 rm"
echo "$got" | grep -F "# morca-replace rollback" | grep -F "i-0green" | grep -qF "docker start scheduler ui" || fail "the old scheduler and UI weren't started again: $got"
echo "$got" | grep -qF "rollback-restore" && fail "restored before the switch"
[ "$(cat "$FAKE_AWS_STATE/active")" = green ] && [ -f "$FAKE_AWS_STATE/blue-off" ] || fail "the stack after the early rollback"
echo "ok   rollback before the switch"
echo "ok   replace-control"
