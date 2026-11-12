#!/bin/sh
# Five runs replacing one month of one table at once, three commits each,
# five times over (tests/replace/writer.mojo, in memory): the table always
# ends with one run's three commits, and that run succeeded. Writers that
# clash plan their removals again on the snapshot they retry on, and one
# that finds another's files in the month after its own first commit fails
# rather than commit half a month (morca.tables, "Replacing partitions").
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC tests/replace/writer.mojo -o build/replace-writer
export MORCA_WAREHOUSE=build/replace-wh
export REPLACE_NAP=0.05
for round in 1 2 3 4 5; do
    rm -rf "$MORCA_WAREHOUSE"
    ./build/replace-writer init
    pids=
    for w in 1 2 3 4 5; do
        REPLACE_BASE=${w}000 MORCA_DATABASE_URL= ./build/replace-writer run --partitions 2024-01 > "build/replace-$w.log" 2>&1 &
        pids="$pids $!"
    done
    w=0
    ok=
    for p in $pids; do
        w=$((w + 1))
        if wait "$p"; then ok="$ok ${w}000"; fi
    done
    got=$(./build/replace-writer check)
    base=$(echo "$got" | sed -n 's/^bases \([0-9]*\) ids 12$/\1/p')
    if [ -z "$base" ]; then
        echo "FAIL: round $round: lake.w holds $got, not one run's 12 ids"
        tail -3 build/replace-*.log
        exit 1
    fi
    case " $ok " in
    *" $base "*) ;;
    *) echo "FAIL: round $round: lake.w holds run $base's ids, which failed (succeeded:$ok)"; exit 1 ;;
    esac
    # A loser says why.
    for w in 1 2 3 4 5; do
        case " $ok " in *" ${w}000 "*) continue ;; esac
        grep -q "another writer is writing a partition of lake.w" "build/replace-$w.log" \
            || { echo "FAIL: round $round: writer $w failed otherwise"; tail -5 "build/replace-$w.log"; exit 1; }
    done
    echo "round $round: run ${base} kept, succeeded:$ok"
done
rm -f build/replace-*.log
echo "ok   concurrent replaces"
