#!/bin/sh
# examples/taxi over the fixtures in tests/fixtures/taxi (two real months,
# sliced): a run loads and compacts them, and a run whose download fails
# resumes at that month once the file is there. Needs ORCA_DATABASE_URL (pixi
# run -e data test-taxi starts a throwaway server). The full 24-month load is
# run by hand: it downloads 1.2 GB.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src -I examples/taxi $DEP_INC examples/taxi/taxi.mojo -o build/taxi

fail() {
    echo "FAIL: $*"
    echo "$out"
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }

root=build/taxi-test
rm -rf "$root"
export TAXI_TLC="$root/tlc" TAXI_WAREHOUSE="$root/wh"

# The fixtures: 499 rows of 2023-01 and 498 of 2024-01 picked up in their
# month, 48 and 18 not, in 128-row row groups.
out=$(TAXI_SOURCE=tests/fixtures/taxi ./build/taxi run \
    --partitions 2023-01,2024-01 2>&1) || fail "run exited $?"
echo "$out" | grep -q '^\[load 2023-01\] 499 rows in 4 batches, 48 dropped' \
    || fail "2023-01 didn't load 499 rows"
echo "$out" | grep -q '^\[load 2024-01\] 498 rows in 4 batches, 18 dropped' \
    || fail "2024-01 didn't load 498 rows"
echo "$out" | grep -q '^\[compact\] 997 rows in 2 files' \
    || fail "compaction didn't leave 997 rows in 2 files"
echo "$out" | grep -q '^run succeeded$' || fail "run didn't succeed"

# A source without 2024-01: its download fails, its load and the compaction
# are skipped, and 2023-01 is loaded.
rm -rf "$root"
mkdir -p "$root/source"
cp tests/fixtures/taxi/yellow_tripdata_2023-01.parquet "$root/source/"
set +e
out=$(TAXI_SOURCE="$root/source" ./build/taxi run \
    --partitions 2023-01,2024-01 2>&1)
code=$?
set -e
[ "$code" -eq 1 ] || fail "a failed run exited $code, not 1"
run=$(echo "$out" | sed -n 's/^run \([0-9][0-9]*\)$/\1/p' | head -1)
[ -n "$run" ] || fail "no run ID"
echo "$out" | grep -q '^download 2024-01 failed after 3 attempts' \
    || fail "download 2024-01 didn't fail after its retries"
echo "$out" | grep -q '^load 2023-01 succeeded$' || fail "2023-01 didn't load"
echo "$out" | grep -q '^compact skipped' || fail "compact wasn't skipped"

# Once the file is there, resuming runs only what didn't succeed.
cp tests/fixtures/taxi/yellow_tripdata_2024-01.parquet "$root/source/"
out=$(TAXI_SOURCE="$root/source" ./build/taxi resume "$run" 2>&1) \
    || fail "resume exited $?"
echo "$out" | grep -q "^resuming run $run with 2 steps done" \
    || fail "resume didn't keep download and load 2023-01"
echo "$out" | grep -q '^\[load 2023-01\]' && fail "2023-01 loaded again"
echo "$out" | grep -q '^\[compact\] 997 rows in 2 files' \
    || fail "compaction didn't leave 997 rows in 2 files"
echo "$out" | grep -q '^run succeeded$' || fail "resumed run didn't succeed"
[ "$(q "SELECT count(*) FROM orca.commits c JOIN orca.attempts a ON a.id = c.attempt
        JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")" -eq 3 ] \
    || fail "the run didn't commit exactly 3 snapshots"

# Two runs at once into one table, as a backfill starts them: each process
# seeds its own snapshot IDs and file names, so neither overwrites the other's
# commits, and each compacts only its own month.
rm -rf "$root"
TAXI_SOURCE=tests/fixtures/taxi ./build/taxi run --partitions 2023-01 > "$root.a" 2>&1 &
a=$!
TAXI_SOURCE=tests/fixtures/taxi ./build/taxi run --partitions 2024-01 > "$root.b" 2>&1 &
b=$!
set +e
wait $a
code_a=$?
wait $b
code_b=$?
set -e
out=$(cat "$root.a")
[ "$code_a" -eq 0 ] || fail "the 2023-01 run exited $code_a"
out=$(cat "$root.b")
[ "$code_b" -eq 0 ] || fail "the 2024-01 run exited $code_b"
out=$(cat "$root.a" "$root.b")
meta=$(ls "$root"/wh/taxi/trips/metadata/*.metadata.json | tail -1)
ids=$(jq -r '.snapshots[]["snapshot-id"]' "$meta")
[ "$(echo "$ids" | wc -l)" -eq 4 ] || fail "not 4 snapshots: $ids"
[ "$(echo "$ids" | sort -u | wc -l)" -eq 4 ] || fail "two runs drew the same snapshot ID: $ids"
[ "$(jq -r '.snapshots[]["manifest-list"]' "$meta" | sort -u | wc -l)" -eq 4 ] \
    || fail "two snapshots share a manifest list"
grep -q 'pickup_month=2023-01: 4 files to 1$' "$root.a" && ! grep -q 2024-01 "$root.a" \
    || fail "the 2023-01 run didn't compact just its month"
grep -q 'pickup_month=2024-01: 4 files to 1$' "$root.b" && ! grep -q 2023-01 "$root.b" \
    || fail "the 2024-01 run didn't compact just its month"
rm -f "$root.a" "$root.b"
echo "ok   taxi"
