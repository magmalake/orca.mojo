#!/bin/sh
# The command line every pipeline binary gets, on examples/toy.mojo.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy

fail() { echo "FAIL: $*"; exit 1; }

./build/toy describe | grep -q '"pipeline": "toy", "registry_hash": "0x' \
    || fail "describe"

out=$(./build/toy run --partitions 2024-01..2024-03) || fail "run exited $?"
echo "$out" | grep -q '^total succeeded$' || fail "total didn't succeed"
echo "$out" | grep -q '3 months, 2184 hours' || fail "wrong total"
echo "$out" | grep -q '^run succeeded$' || fail "run didn't succeed"

set +e
out=$(ORCA_TOY_FAIL=2024-02 ./build/toy run --partitions 2024-01..2024-03)
code=$?
set -e
[ "$code" -eq 1 ] || fail "a failed run exited $code, not 1"
echo "$out" | grep -q '^measure 2024-02 failed: failing 2024-02 as asked$' \
    || fail "measure 2024-02 didn't fail"
echo "$out" | grep -q '^measure 2024-03 succeeded$' \
    || fail "other partitions didn't carry on"
echo "$out" | grep -q '^total skipped' || fail "total wasn't skipped"

for args in "" "run" "run --partitions 2024-03..2024-01" "nope"; do
    set +e
    # shellcheck disable=SC2086
    ./build/toy $args > /dev/null 2>&1
    code=$?
    set -e
    [ "$code" -eq 2 ] || fail "'toy $args' exited $code, not 2"
done
echo "ok   cli"
