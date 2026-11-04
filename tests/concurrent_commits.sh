#!/bin/sh
# Six processes appending to one new table at once, five times: every one
# commits. Writers that clash on a metadata version all back out, so
# orca.tables retries after a random, growing wait; retrying at once, a
# round lost up to half its writers. In memory, with the lineage test's
# ingest pipeline (tests/lineage/).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src -I tests/lineage $DEP_INC tests/lineage/ingest.mojo -o build/ln-ingest
export ORCA_WAREHOUSE=build/commits-wh
for round in 1 2 3 4 5; do
    rm -rf "$ORCA_WAREHOUSE"
    pids=
    for m in 01 02 03 04 05 06; do
        ORCA_DATABASE_URL= ./build/ln-ingest run --partitions "2024-$m" > "build/commits-$m.log" 2>&1 &
        pids="$pids $!"
    done
    failed=0
    for p in $pids; do wait "$p" || failed=$((failed + 1)); done
    if [ "$failed" -ne 0 ]; then
        grep -h failed build/commits-*.log | head -5
        echo "FAIL: round $round: $failed of 6 writers failed"
        exit 1
    fi
    meta=$(ls "$ORCA_WAREHOUSE"/lake/raw/metadata/*.metadata.json | sort | tail -1)
    n=$(jq '.snapshots | length' "$meta")
    [ "$n" -eq 6 ] || { echo "FAIL: round $round: $n snapshots, not 6"; exit 1; }
done
rm -f build/commits-*.log
echo "ok   concurrent commits"
