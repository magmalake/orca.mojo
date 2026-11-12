#!/bin/sh
# morca aws (morca.aws) against stand-ins for the aws and docker commands
# (tests/fake_bin), from a project directory: up refuses a new stack without
# --email, then deploys the template morca carries with a generated password;
# push refuses a project without linux-aarch64, then builds the staged
# project for arm64 and pushes both tags; register and app send the control
# instance the commands they should; down needs --yes.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
morca=$PWD/build/morca
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
printf 'from morca.command import morca_command\n' > bin/scheduler.mojo
: > pipelines/daily.mojo

if "$morca" aws up > ../up.out 2>&1; then fail "up made a stack without --email"; fi
grep -q "needs --email" ../up.out || fail "up's message: $(cat ../up.out)"
calls > /dev/null
"$morca" aws up --email ops@example.com --console-hostname libs.example.dev --zone us-east-2b > ../up.out 2>&1 || fail "up: $(cat ../up.out)"
got=$(calls)
echo "$got" | grep -q "aws --region us-east-2 cloudformation deploy --stack-name orca --template-file build/morca-aws/morca.yaml --capabilities CAPABILITY_IAM" \
    || fail "up's deploy: $got"
echo "$got" | grep -q "DbPassword=[0-9a-f]\{36\} BudgetEmail=ops@example.com ConsoleHostname=libs.example.dev AvailabilityZone=us-east-2b" || fail "up's parameters: $got"
grep -q "PublicBucket" ../up.out || fail "up didn't print the outputs"
if grep -q "DbPassword" ../up.out; then fail "up printed the password"; fi
cmp -s build/morca-aws/morca.yaml "$root/deploy/aws/morca.yaml" || fail "the template written isn't deploy/aws/morca.yaml"
# The control instance's sampler reads the cgroup files and Docker's configs
# (read-only), not the Docker socket, and its day files expire in 14 days.
grep -qF -- '-v /sys/fs/cgroup:/host/cgroup:ro' build/morca-aws/morca.yaml || fail "the sampler doesn't mount the cgroup files read-only"
grep -qF -- '--cgroup /host/cgroup --names /host/containers' build/morca-aws/morca.yaml || fail "the sampler isn't told where they are"
grep -q 'docker.sock' build/morca-aws/morca.yaml && fail "a container gets the Docker socket"
# Control-placed runs (morca.control) read their secrets with the control
# instance's role.
grep -A16 'ControlRole:' build/morca-aws/morca.yaml | grep -q 'secretsmanager:GetSecretValue' || fail "the control role can't read runs' secrets"
grep -qF 'Prefix: logs/control/, ExpirationInDays: 14' build/morca-aws/morca.yaml || fail "the control samples don't expire in 14 days"
# An update keeps the password: no new one.
"$morca" aws up > /dev/null 2>&1 || fail "up again"
if calls | grep -q DbPassword; then fail "an update made a new password"; fi
# The lake's bucket is private unless asked; an update without the flag
# keeps the stack's setting.
if calls | grep -q PublicData; then fail "an update without --public-data set PublicData"; fi
"$morca" aws up --public-data true > /dev/null 2>&1 || fail "up --public-data true"
calls | grep -q "PublicData=true" || fail "up --public-data true didn't pass PublicData=true"
if "$morca" aws up --public-data yes > /dev/null 2>&1; then fail "up took --public-data yes"; fi
grep -q 'Default: "false"' build/morca-aws/morca.yaml || fail "PublicData isn't false by default"
echo "ok   up"

if "$morca" aws push > ../push.out 2>&1; then fail "push without linux-aarch64"; fi
grep -q "pixi workspace platform add linux-aarch64" ../push.out || fail "push's message: $(cat ../push.out)"
printf '[workspace]\nplatforms = ["osx-arm64", "linux-aarch64"]\n' > pixi.toml
# The template builds FROM morca-runtime:<the morca-mojo in pixi.lock>: none
# there, no build.
calls > /dev/null
if "$morca" aws push --tag v1 > ../push.out 2>&1; then fail "push without morca-mojo in pixi.lock"; fi
grep -q "pixi.lock has no morca-mojo" ../push.out || fail "push's lock message: $(cat ../push.out)"
calls | grep -q "docker build" && fail "push built without a version"
# A source package's record, as `pixi shelf add` pins one by revision.
cat > pixi.lock << 'EOF'
packages:
- conda: https://conda.anaconda.org/conda-forge/linux-aarch64/libpq-18.1-h1.conda
  sha256: 00
- conda_source: morca-mojo[1ff61e40] @ git+https://github.com/morca-dev/morca?rev=abc#abc
  version: 0.24.0
  build: he8cfe8b_0
- conda_source: other-mojo[11] @ git+https://example.com/other?rev=def#def
  version: 9.9.9
EOF
"$morca" aws push --tag v1 > ../push.out 2>&1 || fail "push: $(cat ../push.out)"
got=$(calls)
repo=123456789012.dkr.ecr.us-east-2.amazonaws.com/orca
echo "$got" | grep -q "docker build --platform linux/arm64 -f build/morca-aws/Dockerfile --build-arg MORCA_VERSION=0.24.0 .* -t $repo:v1 -t $repo:latest build/morca-aws/context" \
    || fail "push's build: $got"
grep -q '^FROM ${MORCA_RUNTIME}:${MORCA_VERSION}' build/morca-aws/Dockerfile || fail "the template isn't FROM the runtime"
# A channel package's file name.
printf 'packages:\n- conda: https://repo.example/linux-aarch64/morca-mojo-0.25.1-h0_0.conda\n  sha256: 00\n' > pixi.lock
calls > /dev/null
"$morca" aws push --tag v1 > ../push.out 2>&1 || fail "push with a channel package: $(cat ../push.out)"
calls | grep -q "MORCA_VERSION=0.25.1 " || fail "the channel package's version"
echo "$got" | grep -q "docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-2.amazonaws.com" || fail "push's login: $got"
echo "$got" | grep -q "docker push $repo:v1" && echo "$got" | grep -q "docker push $repo:latest" || fail "push's pushes: $got"
[ -f build/morca-aws/context/bin/scheduler.mojo ] && [ -f build/morca-aws/context/pipelines/daily.mojo ] || fail "the staged context"
grep -q "pixi shelf add morca-mojo" build/morca-aws/Dockerfile || fail "the Dockerfile written isn't the template"
echo "ok   push"

if "$morca" aws register --tag v1 --image x daily > /dev/null 2>&1; then fail "register took --image"; fi
"$morca" aws register --tag v1 --workers 0..4 --worker-procs auto --secret GITHUB_TOKEN=orca/github-token --placement control daily > ../reg.out 2>&1 || fail "register: $(cat ../reg.out)"
got=$(calls)
echo "$got" | grep -q "ssm send-command --instance-ids i-0control --document-name AWS-RunShellScript" || fail "register's send-command: $got"
echo "$got" | grep -qF "orca-refresh 'v1'" || fail "register's refresh: $got"
echo "$got" | grep -qF "docker exec scheduler morca register --image '$repo:v1' '--workers' '0..4' '--worker-procs' 'auto' '--secret' 'GITHUB_TOKEN=orca/github-token' '--placement' 'control' --no-config '/opt/morca/pipelines/daily'" \
    || fail "register's command: $got"
grep -q "registered on the control instance" ../reg.out || fail "register didn't print the output"
# The project's morca.toml (morca.config) goes whole, base64, with its name;
# a bad one is refused before anything is sent.
printf '[pipelines.daily]\nworkers = "0..2"\nenv = { A = "b" }\n' > morca.toml
"$morca" aws register --tag v1 daily > ../reg.out 2>&1 || fail "register with morca.toml: $(cat ../reg.out)"
b64=$(base64 < morca.toml | tr -d '\n')
got=$(calls)
# (with --config-commit too: the project here is inside morca's checkout)
echo "$got" | grep -qF "docker exec scheduler morca register --image '$repo:v1' --config-b64 '$b64' --config-name 'morca.toml'" \
    && echo "$got" | grep -qF "'/opt/morca/pipelines/daily'" || fail "register didn't send morca.toml: $got"
printf '[pipelines.daily]\nworkrs = 2\n' > morca.toml
if "$morca" aws register --tag v1 daily > ../reg.out 2>&1; then fail "register sent a bad morca.toml"; fi
grep -q "morca.toml:2: \[pipelines.daily\] has no key workrs" ../reg.out || fail "the bad file's message: $(cat ../reg.out)"
if calls | grep -q "send-command"; then fail "a bad morca.toml was sent"; fi
rm morca.toml
echo "ok   register"

"$morca" aws app console > ../app.out 2>&1 || fail "app: $(cat ../app.out)"
got=$(calls)
echo "$got" | grep -qF "orca-app 'console' 'libs.mlake.dev' '8080'" || fail "app's command: $got"
grep -q "console at https://libs.mlake.dev" ../app.out || fail "app's message"
if "$morca" aws app reports > /dev/null 2>&1; then fail "an app other than console without --host"; fi
echo "ok   app"

if "$morca" aws down > ../down.out 2>&1; then fail "down without --yes"; fi
if calls | grep -q delete-stack; then fail "down deleted without --yes"; fi
"$morca" aws down --yes > /dev/null 2>&1 || fail "down --yes"
calls | grep -q "cloudformation delete-stack --stack-name orca" || fail "down's delete"
echo "ok   down"
echo "ok   aws"
