#!/bin/sh
# The runtime image (deploy/runtime.Dockerfile) and a project's image built
# FROM it (deploy/project.Dockerfile), as CI builds them
# (.github/workflows/runtime-image.yml):
#
#     sh tests/runtime-image.sh orca-runtime:ci                 # the runtime
#     sh tests/runtime-image.sh orca-runtime:ci --project [CPU] # and a project on it
#
# The runtime: orca, orca_maintain, git over HTTPS (libcurl and the CA
# bundle), llvm-symbolizer, and every binary's libraries found. The
# project: a scratch project that adds this checkout of orca-mojo, with
# examples/toy.mojo as its one pipeline and no bin/ of its own, built on the
# runtime; its toy has its source map and runs a partition, and its `orca`
# is the runtime's. Needs docker, and pixi for the project's lock.
set -eu
cd "$(dirname "$0")/.."
img=$1
fail() { echo "FAIL: $*"; exit 1; }
run() { docker run --rm "$@"; }

out=$(run "$img" 2>&1) || fail "orca: $out"
echo "$out" | grep -q "orca locate" || fail "orca's usage: $out"
run --entrypoint orca_maintain "$img" > /dev/null 2>&1 || [ $? -eq 2 ] || fail "orca_maintain didn't start"
run --entrypoint llvm-symbolizer "$img" --version | grep -q LLVM || fail "llvm-symbolizer"
run --entrypoint git "$img" ls-remote https://github.com/magmalake/orca.mojo HEAD | grep -q HEAD \
    || fail "git over HTTPS"
# Every binary and library finds what it links.
missing=$(run --entrypoint sh "$img" -c \
    'for f in /opt/orca/bin/* /opt/orca/libexec/llvm-symbolizer /opt/orca/libexec/git/* /opt/orca/lib/*.so*; do
        [ -f "$f" ] && ldd "$f" 2>/dev/null | grep "not found" | sed "s|^|$f: |"; done; true')
[ -z "$missing" ] || fail "libraries not found: $missing"
run --entrypoint sh "$img" -c '[ -s /opt/orca/ssl/cacert.pem ] && [ -s /src/project/.pixi/envs/default/ssl/cacert.pem ] \
    && [ -s /opt/orca/share/pixi.lock ] && [ -d /opt/orca/pipelines ]' || fail "the CA bundles, pixi.lock or the layout"
echo "ok   runtime image"

[ "${2:-}" = --project ] || exit 0
cpu=${3:-neoverse-n1}
ctx=$PWD/build/runtime-project
rm -rf "$ctx"
mkdir -p "$ctx/pipelines" "$ctx/orca.mojo"
cp -R pixi.toml src "$ctx/orca.mojo/"
cp examples/toy.mojo "$ctx/pipelines/"
cat > "$ctx/pixi.toml" << 'EOF'
[workspace]
name = "runtime-project"
channels = ["https://conda.modular.com/max-nightly", "conda-forge"]
platforms = ["linux-64", "linux-aarch64"]
preview = ["pixi-build"]

[dependencies]
mojo-compiler = "==1.2.0"
orca-mojo = { path = "orca.mojo" }
EOF
pixi lock --manifest-path "$ctx/pixi.toml"
docker build -f deploy/project.Dockerfile --build-arg ORCA_RUNTIME="${img%:*}" --build-arg ORCA_VERSION="${img##*:}" \
    --build-arg MOJO_TARGET_CPU="$cpu" --build-arg ORCA_SOURCE=https://github.com/magmalake/orca.mojo \
    --build-arg ORCA_COMMIT=0000000 -t runtime-project:ci "$ctx"
proj=runtime-project:ci
run --entrypoint sh "$proj" -c '[ -x /opt/orca/bin/orca_maintain ] && [ -x /opt/orca/bin/git ]' || fail "the project's layout"
run --entrypoint cat "$proj" /opt/orca/pipelines/toy.source.json | grep -q '"pipeline"' || fail "toy's source map"
run --entrypoint toy "$proj" describe | grep -q '"pipeline": "toy"' || fail "toy describe"
out=$(run --entrypoint toy "$proj" run --partitions 2024-01 2>&1) || fail "toy run: $out"
[ "$(docker image inspect -f '{{.Config.Entrypoint}}' "$proj")" = "[/opt/orca/bin/orca]" ] || fail "the entrypoint"
# The runtime's orca, unchanged: the project has no bin/scheduler.mojo.
a=$(run --entrypoint sha256sum "$img" /opt/orca/bin/orca | cut -d' ' -f1)
b=$(run --entrypoint sha256sum "$proj" /opt/orca/bin/orca | cut -d' ' -f1)
[ "$a" = "$b" ] || fail "the project's orca isn't the runtime's"
echo "ok   project image on the runtime"
