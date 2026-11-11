#!/bin/sh
# A scratch orca project, for CI to call orca's reusable project workflows
# on (.github/workflows/project-image.yml and project-test.yml, from ci.yml's
# project-* jobs) as a project's own CI would:
#
#     sh tests/example-project.sh build/example-project
#
# It adds this commit of orca-mojo as `pixi shelf add` pins it, by revision,
# which must be on GitHub (CI's commit; ORCA_REV and ORCA_REPO say another).
# Its one pipeline is examples/toy.mojo, its orca.toml configures it, and its
# `test` task builds the toy and runs a partition. Needs pixi, for the lock.
set -eu
cd "$(dirname "$0")/.."
dir=${1:?usage: sh tests/example-project.sh <dir>}
rev=${ORCA_REV:-$(git rev-parse HEAD)}
repo=${ORCA_REPO:-https://github.com/morca-dev/morca}
rm -rf "$dir"
mkdir -p "$dir/pipelines"
cp examples/toy.mojo "$dir/pipelines/"
cat > "$dir/pixi.toml" << EOF
[workspace]
name = "example-project"
channels = ["https://conda.modular.com/max-nightly", "conda-forge"]
platforms = ["linux-64", "linux-aarch64"]
preview = ["pixi-build"]

[dependencies]
mojo-compiler = "==1.2.0"
orca-mojo = { git = "$repo", rev = "$rev" }

[tasks]
test = "mkdir -p build && mojo build pipelines/toy.mojo -o build/toy && ./build/toy describe && ./build/toy run --partitions 2024-01"
EOF
cat > "$dir/orca.toml" << 'EOF'
[defaults]
placement = "control"

[pipelines.toy]
max_runs = 2
cpus = "1..2"
memory_gb = "0.5..2"
EOF
pixi lock --manifest-path "$dir/pixi.toml"
