#!/bin/sh
# orca aws (orca.aws) against stand-ins for the aws and docker commands
# (tests/fake_bin), from a project directory: up refuses a new stack without
# --email, then deploys the template orca carries with a generated password;
# push refuses a project without linux-aarch64, then builds the staged
# project for arm64 and pushes both tags; register and app send the control
# instance the commands they should; down needs --yes.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
orca=$PWD/build/orca
root=$PWD
python3 scripts/embed-aws.py --check
work=$PWD/build/aws-test
rm -rf "$work"
mkdir -p "$work/project/bin" "$work/project/pipelines" "$work/state"
export FAKE_AWS_LOG=$work/calls.log FAKE_AWS_STATE=$work/state PATH="$PWD/tests/fake_bin:$PATH"
export AWS_REGION=us-east-2
: > "$FAKE_AWS_LOG"
fail() { echo "FAIL: $*"; cat "$FAKE_AWS_LOG"; exit 1; }
calls() { cat "$FAKE_AWS_LOG"; : > "$FAKE_AWS_LOG"; }
cd "$work/project"
printf '[workspace]\nplatforms = ["osx-arm64"]\n' > pixi.toml
: > pixi.lock
printf 'from orca.command import orca_command\n' > bin/scheduler.mojo
: > pipelines/daily.mojo

if "$orca" aws up > ../up.out 2>&1; then fail "up made a stack without --email"; fi
grep -q "needs --email" ../up.out || fail "up's message: $(cat ../up.out)"
calls > /dev/null
"$orca" aws up --email ops@example.com --console-hostname libs.example.dev --zone us-east-2b > ../up.out 2>&1 || fail "up: $(cat ../up.out)"
got=$(calls)
echo "$got" | grep -q "aws --region us-east-2 cloudformation deploy --stack-name orca --template-file build/orca-aws/orca.yaml --capabilities CAPABILITY_IAM" \
    || fail "up's deploy: $got"
echo "$got" | grep -q "DbPassword=[0-9a-f]\{36\} BudgetEmail=ops@example.com ConsoleHostname=libs.example.dev AvailabilityZone=us-east-2b" || fail "up's parameters: $got"
grep -q "PublicBucket" ../up.out || fail "up didn't print the outputs"
if grep -q "DbPassword" ../up.out; then fail "up printed the password"; fi
cmp -s build/orca-aws/orca.yaml "$root/deploy/aws/orca.yaml" || fail "the template written isn't deploy/aws/orca.yaml"
# The control instance's sampler reads the cgroup files and Docker's configs
# (read-only), not the Docker socket, and its day files expire in 14 days.
grep -qF -- '-v /sys/fs/cgroup:/host/cgroup:ro' build/orca-aws/orca.yaml || fail "the sampler doesn't mount the cgroup files read-only"
grep -qF -- '--cgroup /host/cgroup --names /host/containers' build/orca-aws/orca.yaml || fail "the sampler isn't told where they are"
grep -q 'docker.sock' build/orca-aws/orca.yaml && fail "a container gets the Docker socket"
# Control-placed runs (orca.control) read their secrets with the control
# instance's role.
grep -A16 'ControlRole:' build/orca-aws/orca.yaml | grep -q 'secretsmanager:GetSecretValue' || fail "the control role can't read runs' secrets"
grep -qF 'Prefix: logs/control/, ExpirationInDays: 14' build/orca-aws/orca.yaml || fail "the control samples don't expire in 14 days"
# An update keeps the password: no new one.
"$orca" aws up > /dev/null 2>&1 || fail "up again"
if calls | grep -q DbPassword; then fail "an update made a new password"; fi
# The lake's bucket is private unless asked; an update without the flag
# keeps the stack's setting.
if calls | grep -q PublicData; then fail "an update without --public-data set PublicData"; fi
"$orca" aws up --public-data true > /dev/null 2>&1 || fail "up --public-data true"
calls | grep -q "PublicData=true" || fail "up --public-data true didn't pass PublicData=true"
if "$orca" aws up --public-data yes > /dev/null 2>&1; then fail "up took --public-data yes"; fi
grep -q 'Default: "false"' build/orca-aws/orca.yaml || fail "PublicData isn't false by default"
echo "ok   up"

if "$orca" aws push > ../push.out 2>&1; then fail "push without linux-aarch64"; fi
grep -q "pixi workspace platform add linux-aarch64" ../push.out || fail "push's message: $(cat ../push.out)"
printf '[workspace]\nplatforms = ["osx-arm64", "linux-aarch64"]\n' > pixi.toml
calls > /dev/null
"$orca" aws push --tag v1 > ../push.out 2>&1 || fail "push: $(cat ../push.out)"
got=$(calls)
repo=123456789012.dkr.ecr.us-east-2.amazonaws.com/orca
echo "$got" | grep -q "docker build --platform linux/arm64 -f build/orca-aws/Dockerfile .* -t $repo:v1 -t $repo:latest build/orca-aws/context" \
    || fail "push's build: $got"
echo "$got" | grep -q "docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-2.amazonaws.com" || fail "push's login: $got"
echo "$got" | grep -q "docker push $repo:v1" && echo "$got" | grep -q "docker push $repo:latest" || fail "push's pushes: $got"
[ -f build/orca-aws/context/bin/scheduler.mojo ] && [ -f build/orca-aws/context/pipelines/daily.mojo ] || fail "the staged context"
grep -q "pixi shelf add orca-mojo" build/orca-aws/Dockerfile || fail "the Dockerfile written isn't the template"
echo "ok   push"

if "$orca" aws register --tag v1 --image x daily > /dev/null 2>&1; then fail "register took --image"; fi
"$orca" aws register --tag v1 --workers 0..4 --worker-procs auto --secret GITHUB_TOKEN=orca/github-token --placement control daily > ../reg.out 2>&1 || fail "register: $(cat ../reg.out)"
got=$(calls)
echo "$got" | grep -q "ssm send-command --instance-ids i-0control --document-name AWS-RunShellScript" || fail "register's send-command: $got"
echo "$got" | grep -qF "orca-refresh 'v1'" || fail "register's refresh: $got"
echo "$got" | grep -qF "docker exec scheduler orca register --image '$repo:v1' '--workers' '0..4' '--worker-procs' 'auto' '--secret' 'GITHUB_TOKEN=orca/github-token' '--placement' 'control' '/opt/orca/pipelines/daily'" \
    || fail "register's command: $got"
grep -q "registered on the control instance" ../reg.out || fail "register didn't print the output"
echo "ok   register"

"$orca" aws app console > ../app.out 2>&1 || fail "app: $(cat ../app.out)"
got=$(calls)
echo "$got" | grep -qF "orca-app 'console' 'libs.mlake.dev' '8080'" || fail "app's command: $got"
grep -q "console at https://libs.mlake.dev" ../app.out || fail "app's message"
if "$orca" aws app reports > /dev/null 2>&1; then fail "an app other than console without --host"; fi
echo "ok   app"

if "$orca" aws down > ../down.out 2>&1; then fail "down without --yes"; fi
if calls | grep -q delete-stack; then fail "down deleted without --yes"; fi
"$orca" aws down --yes > /dev/null 2>&1 || fail "down --yes"
calls | grep -q "cloudformation delete-stack --stack-name orca" || fail "down's delete"
echo "ok   down"
echo "ok   aws"
