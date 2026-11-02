#!/bin/sh
# Datasets on steps, without tables: describe lists each step's reads and
# writes, and `orca register` refuses a pipeline that would close a cycle
# across pipelines (tests/datasets/: first reads what third writes, second
# what first writes, third what second writes), but not one that reads
# and writes one dataset. Needs ORCA_DATABASE_URL (pixi run test-datasets
# starts a throwaway server) and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
for p in first second third compacts; do
    # shellcheck disable=SC2086
    mojo build -I src $DEP_INC "tests/datasets/$p.mojo" -o "build/ds-$p"
done
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/datasets.log
: > "$log"
fail() {
    echo "FAIL: $*"
    tail -20 "$log"
    exit 1
}

[ "$(./build/ds-second describe | jq -c '.steps[0] | [.reads, .writes]')" = '[["lake.a"],["lake.b"]]' ] \
    || fail "describe doesn't list the step's datasets"
./build/orca register ./build/ds-first ./build/ds-second ./build/ds-compacts >> "$log" 2>&1 \
    || fail "registering first, second and compacts"
if out=$(./build/orca register ./build/ds-third 2>&1); then
    fail "third, which closes a cycle, was registered"
fi
echo "$out" | grep -qF "third writes lake.c, which first reads; first writes lake.a, which second reads; second writes lake.b, which third reads" \
    || { echo "$out"; fail "the refusal doesn't name the cycle"; }
[ "$(psql "$ORCA_DATABASE_URL" -Atc "SELECT count(*) FROM orca.pipelines WHERE name = 'third'")" -eq 0 ] \
    || fail "third is in orca.pipelines"
# Registering a pipeline again, unchanged, isn't a cycle with itself.
./build/orca register ./build/ds-second >> "$log" 2>&1 || fail "registering second again"
echo "ok   datasets"
