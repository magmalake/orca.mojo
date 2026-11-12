#!/bin/sh
# The runtime image (deploy/runtime.Dockerfile) and a project's image built
# FROM it (deploy/project.Dockerfile), as CI builds them
# (.github/workflows/runtime-image.yml):
#
#     sh tests/runtime-image.sh morca-runtime:ci                 # the runtime
#     sh tests/runtime-image.sh morca-runtime:ci --project [CPU] # and a project on it
#
# The runtime: morca, morca_maintain, git over HTTPS (libcurl and the CA
# bundle), llvm-symbolizer, and every binary's libraries found. The
# project: a scratch project that adds this commit of morca-mojo, with
# examples/toy.mojo as its one pipeline and no bin/ of its own, built on the
# runtime; its toy has its source map and runs a partition, and its `morca`
# is the runtime's. Needs docker, and pixi for the project's lock.
set -eu
cd "$(dirname "$0")/.."
img=$1
fail() { echo "FAIL: $*"; exit 1; }
run() { docker run --rm "$@"; }

# Without arguments, each prints its usage (and exits non-zero).
run "$img" 2>&1 | grep -q "morca locate" || fail "morca's usage"
run --entrypoint morca_maintain "$img" 2>&1 | grep -q "expected .expire <dataset>" || fail "morca_maintain's usage"
run --entrypoint /opt/morca/libexec/llvm-symbolizer "$img" --version | grep -q LLVM || fail "llvm-symbolizer"
run --entrypoint git "$img" ls-remote https://github.com/morca-dev/morca HEAD | grep -q HEAD \
    || fail "git over HTTPS"
# Every binary and library finds what it links.
missing=$(run --entrypoint sh "$img" -c \
    'for f in /opt/morca/bin/* /opt/morca/libexec/llvm-symbolizer /opt/morca/libexec/git/* /opt/morca/lib/*.so*; do
        [ -f "$f" ] && ldd "$f" 2>/dev/null | grep "not found" | sed "s|^|$f: |"; done; true')
[ -z "$missing" ] || fail "libraries not found: $missing"
run --entrypoint sh "$img" -c '[ -s /opt/morca/ssl/cacert.pem ] && [ -s /src/project/.pixi/envs/default/ssl/cacert.pem ] \
    && [ -s /opt/morca/share/pixi.lock ] && [ -d /opt/morca/pipelines ]' || fail "the CA bundles, pixi.lock or the layout"
echo "ok   runtime image"

[ "${2:-}" = --project ] || exit 0
cpu=${3:-neoverse-n1}
# morca-mojo as `pixi shelf add` pins it: by revision, which must be on
# GitHub (CI's commit; MORCA_REV and MORCA_REPO say another).
rev=${MORCA_REV:-$(git rev-parse HEAD)}
repo=${MORCA_REPO:-https://github.com/morca-dev/morca}
ctx=$PWD/build/runtime-project
rm -rf "$ctx"
mkdir -p "$ctx/pipelines"
cp examples/toy.mojo "$ctx/pipelines/"
cat > "$ctx/pixi.toml" << EOF
[workspace]
name = "runtime-project"
channels = ["https://conda.modular.com/max-nightly", "conda-forge"]
platforms = ["linux-64", "linux-aarch64"]
preview = ["pixi-build"]

[dependencies]
mojo-compiler = "==1.2.0"
morca-mojo = { git = "$repo", rev = "$rev" }
EOF
pixi lock --manifest-path "$ctx/pixi.toml"
docker build -f deploy/project.Dockerfile --build-arg MORCA_RUNTIME="${img%:*}" --build-arg MORCA_VERSION="${img##*:}" \
    --build-arg MOJO_TARGET_CPU="$cpu" --build-arg MORCA_SOURCE=https://github.com/morca-dev/morca \
    --build-arg MORCA_COMMIT=0000000 -t runtime-project:ci "$ctx"
proj=runtime-project:ci
run --entrypoint sh "$proj" -c '[ -x /opt/morca/bin/morca_maintain ] && [ -x /opt/morca/bin/git ]' || fail "the project's layout"
run --entrypoint cat "$proj" /opt/morca/pipelines/toy.source.json | grep -q '"pipeline"' || fail "toy's source map"
run --entrypoint toy "$proj" describe | grep -q '"pipeline": "toy"' || fail "toy describe"
out=$(run --entrypoint toy "$proj" run --partitions 2024-01 2>&1) || fail "toy run: $out"
[ "$(docker image inspect -f '{{.Config.Entrypoint}}' "$proj")" = "[/opt/morca/bin/morca]" ] || fail "the entrypoint"
# The runtime's morca, unchanged: the project has no bin/scheduler.mojo.
a=$(run --entrypoint sha256sum "$img" /opt/morca/bin/morca | cut -d' ' -f1)
b=$(run --entrypoint sha256sum "$proj" /opt/morca/bin/morca | cut -d' ' -f1)
[ "$a" = "$b" ] || fail "the project's morca isn't the runtime's"
echo "ok   project image on the runtime"
