#!/bin/sh
# Six processes appending to one new table at once, five times: every one
# commits, once. Writers that clash on a metadata version all back out, so
# morca.tables retries after a random, growing wait; retrying at once, a
# round lost up to half its writers. In memory, with the lineage test's
# ingest pipeline (tests/lineage/), which appends three rows a month.
#
# Each round checks the table, not just the snapshot count: the current
# snapshot's lineage holds one append per writer (none lost, none twice,
# no snapshot off it), 3 rows and 1 data file each, and the totals add up.
# A failure prints every snapshot's summary and the metadata files.
#
# COMMIT_WRITERS (1-12, default 6) and COMMIT_ROUNDS (default 5) make the
# race likelier when looping it by hand.
set -eu
cd "$(dirname "$0")/.."
writers=${COMMIT_WRITERS:-6}
rounds=${COMMIT_ROUNDS:-5}
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src -I tests/lineage $DEP_INC tests/lineage/ingest.mojo -o build/ln-ingest
export MORCA_WAREHOUSE=build/commits-wh
months=$(seq -f %02g 1 "$writers")

# The newest metadata file: the highest version (one file per version once
# every writer is done).
newest() {
    # shellcheck disable=SC2012
    ls "$MORCA_WAREHOUSE"/lake/raw/metadata/*.metadata.json | sort | tail -1
}

# Prints what's wrong with the table after a round, or nothing.
problems() {
    jq -r --argjson w "$writers" '
        (.snapshots | map({key: (.["snapshot-id"] | tostring), value: .}) | from_entries) as $by
        | [ .["current-snapshot-id"]
            | recurse(($by[tostring] // {})["parent-snapshot-id"] // empty)
            | $by[tostring] // empty ] as $chain
        | ($chain | map(.summary["orca.partition"])) as $parts
        | ($chain[0].summary // {}) as $top
        | [ (if (.snapshots | length) != $w then "\(.snapshots | length) snapshots, not \($w)" else empty end),
            (if ($chain | length) != (.snapshots | length) then "\(.snapshots | length - ($chain | length)) snapshots not in the current lineage" else empty end),
            ($parts | group_by(.) | map(select(length > 1)) | .[] | "partition \(.[0]) appended \(length) times"),
            (if ($parts | unique | length) != $w then "\($parts | unique | length) distinct partitions in the lineage, not \($w)" else empty end),
            ($chain[] | select(.summary.operation != "append" or .summary["added-records"] != "3" or .summary["added-data-files"] != "1")
                | "snapshot \(.["snapshot-id"]) (\(.summary["orca.partition"])): \(.summary.operation), \(.summary["added-records"]) records in \(.summary["added-data-files"]) files"),
            (if $top["total-records"] != "\(3 * $w)" then "total-records \($top["total-records"]), not \(3 * $w)" else empty end),
            (if $top["total-data-files"] != "\($w)" then "total-data-files \($top["total-data-files"]), not \($w)" else empty end)
          ] | .[]' "$1"
}

# Every snapshot, oldest first, and the metadata files left.
dump() {
    echo "metadata: $1"
    jq -r '"current snapshot: \(.["current-snapshot-id"])",
        (.snapshots | sort_by(.["sequence-number"]) | .[]
            | "  snapshot \(.["snapshot-id"]) parent \(.["parent-snapshot-id"] // "-") seq \(.["sequence-number"]): \(.summary.operation) partition \(.summary["orca.partition"]) added-records \(.summary["added-records"]) added-data-files \(.summary["added-data-files"]) total-records \(.summary["total-records"]) total-data-files \(.summary["total-data-files"])")' "$1"
    echo "metadata files:"
    ls "$MORCA_WAREHOUSE"/lake/raw/metadata/ | grep metadata.json | sed 's/^/  /'
    echo "data files on disk: $(find "$MORCA_WAREHOUSE"/lake/raw/data -name '*.parquet' | wc -l | tr -d ' ')"
    grep -h -i "clash\|another writer\|wrote version\|retry" build/commits-*.log | head -20 || true
}

round=1
while [ "$round" -le "$rounds" ]; do
    rm -rf "$MORCA_WAREHOUSE"
    pids=
    for m in $months; do
        MORCA_DATABASE_URL= ./build/ln-ingest run --partitions "2024-$m" > "build/commits-$m.log" 2>&1 &
        pids="$pids $!"
    done
    failed=0
    for p in $pids; do wait "$p" || failed=$((failed + 1)); done
    if [ "$failed" -ne 0 ]; then
        grep -h failed build/commits-*.log | head -5
        echo "FAIL: round $round: $failed of $writers writers failed"
        exit 1
    fi
    meta=$(newest)
    bad=$(problems "$meta")
    if [ -n "$bad" ]; then
        echo "$bad" | sed "s/^/FAIL: round $round: /"
        dump "$meta"
        exit 1
    fi
    round=$((round + 1))
done
rm -f build/commits-*.log
echo "ok   concurrent commits"
