#!/bin/sh
# The command line every pipeline binary gets, on examples/toy.mojo.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy

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

# A daily pipeline: days are keys, and days before its start are refused.
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/daily
./build/daily describe | grep -q '"partitions": {"kind": "daily", "start": "2024-01-01"}' \
    || fail "daily describe"
out=$(./build/daily run --partitions 2024-02-27..2024-03-02) || fail "daily run exited $?"
echo "$out" | grep -q '^trips 2024-02-29 succeeded$' || fail "no 2024-02-29"
echo "$out" | grep -q '5 days, 1287 trips' || fail "wrong daily total"
set +e
./build/daily run --partitions 2023-12-31..2024-01-01 > /dev/null 2>&1
code=$?
set -e
[ "$code" -eq 2 ] || fail "a day before the start exited $code, not 2"
# Its schedule: 02:30 in New York, for the day before. 2024-03-10 skips 02:30.
out=$(./build/daily fires 2 --after 2024-03-09T12:00Z) || fail "fires exited $?"
[ "$out" = "2024-03-10T07:00:00Z 2024-03-10T03:00:00 2024-03-09
2024-03-11T06:30:00Z 2024-03-11T02:30:00 2024-03-10" ] || fail "wrong fires: $out"
out=$(ORCA_NOW=2024-11-02T12:00Z ./build/daily fires 1) || fail "fires exited $?"
[ "$out" = "2024-11-03T07:30:00Z 2024-11-03T02:30:00 2024-11-02" ] \
    || fail "ORCA_NOW wasn't used: $out"
./build/toy fires 1 --after 2024-01-15T00:00Z | grep -q '^2024-02-02T03:00:00Z 2024-02-02T03:00:00 2024-01$' \
    || fail "toy fires"

for args in "" "run" "run --partitions 2024-03..2024-01" "nope"; do
    set +e
    # shellcheck disable=SC2086
    ./build/toy $args > /dev/null 2>&1
    code=$?
    set -e
    [ "$code" -eq 2 ] || fail "'toy $args' exited $code, not 2"
done
echo "ok   cli"
