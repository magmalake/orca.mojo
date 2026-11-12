#!/bin/sh
# Locks deploy/runtime/pixi.toml, the workspace deploy/runtime.Dockerfile
# installs `--locked`, into deploy/runtime/pixi.lock:
#
#     sh deploy/runtime/lock.sh           # write deploy/runtime/pixi.lock
#     sh deploy/runtime/lock.sh --check   # fail if it's out of date (CI)
#
# The workspace adds morca-mojo by path, `../morca`, as the Dockerfile
# lays it out (/src/project beside /src/morca), so this locks it in that
# layout: a scratch directory with this checkout linked in as morca.
#
# The lock records morca-mojo's dependencies (the revisions of the tins it
# pins, its conda packages), not its version, so a release that only bumps
# the version leaves it valid. A change to what morca-mojo depends on (a
# tin's revision, libpq's range) makes it stale: run this and commit the
# lock with the change. CI's `runtime lock` job says so when it's stale.
set -eu
cd "$(dirname "$0")/../.."
root=$PWD
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/project"
ln -s "$root" "$tmp/morca"
cp deploy/runtime/pixi.toml "$tmp/project/pixi.toml"
[ ! -f deploy/runtime/pixi.lock ] || cp deploy/runtime/pixi.lock "$tmp/project/pixi.lock"
if [ "${1:-}" = --check ]; then
    pixi lock --manifest-path "$tmp/project/pixi.toml" --check --dry-run || {
        echo "::error::deploy/runtime/pixi.lock is out of date: run sh deploy/runtime/lock.sh and commit it"
        exit 1
    }
else
    pixi lock --manifest-path "$tmp/project/pixi.toml"
    cp "$tmp/project/pixi.lock" deploy/runtime/pixi.lock
fi
