#!/bin/sh
# Datasets on steps, without tables: describe lists each step's reads and
# writes, and `orca register` refuses a pipeline that would close a cycle
# across pipelines (tests/datasets/: first reads what third writes, second
# what first writes, third what second writes), but not one that reads
# and writes one dataset. A rerun of a done key (`orca backfill --rerun`,
# `orca rerun`) of a pipeline whose steps append is refused without
# --allow-append; one whose steps replace (replacer's `replaces=`) isn't.
# Needs ORCA_DATABASE_URL (pixi run test-datasets starts a throwaway
# server) and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# The pipelines and orca compile at once (scripts/mojo-build.sh).
pids=
for p in first second third compacts replacer; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src $DEP_INC "tests/datasets/$p.mojo" -o "build/ds-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
for pid in $pids $!; do wait "$pid"; done
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

# Reruns of a done month: first appends to lake.a, replacer replaces lake.r.
[ "$(./build/ds-replacer describe | jq -c '.steps[0] | [.writes, .replaces]')" = '[["lake.r"],["lake.r"]]' ] \
    || fail "describe doesn't list the step's replaces"
[ "$(./build/ds-first describe | jq -c '.steps[0] | has("replaces")')" = false ] \
    || fail "describe lists replaces for a step without them"
./build/orca register ./build/ds-replacer >> "$log" 2>&1 || fail "registering replacer"
./build/ds-first run --partitions 2024-01 >> "$log" 2>&1 || fail "running first"
./build/ds-replacer run --partitions 2024-01 >> "$log" 2>&1 || fail "running replacer"
if out=$(./build/orca backfill --rerun first 2024-01 2>&1); then
    fail "backfill --rerun of first, which appends, wasn't refused: $out"
fi
echo "$out" | grep -qF "rerunning 2024-01 (done) appends again to what first appended to before, duplicating those rows: step step writes lake.a. Declare a dataset in its step's replaces=" \
    || { echo "$out"; fail "the refusal doesn't say why"; }
echo "$out" | grep -qF -- "--allow-append" || fail "the refusal doesn't name --allow-append"
# A backfill without --rerun leaves the done month, so there's nothing to refuse.
out=$(./build/orca backfill first 2024-01 2>/dev/null) || fail "backfill of first exited $?"
[ "$out" = "queued 0 runs" ] || fail "backfill of first: $out"
out=$(./build/orca backfill --rerun --allow-append first 2024-01) || fail "backfill --rerun --allow-append exited $?"
[ "$out" = "queued 1 runs" ] || fail "backfill --rerun --allow-append: $out"
out=$(./build/orca backfill --rerun replacer 2024-01) || fail "backfill --rerun of replacer exited $?"
[ "$out" = "queued 1 runs" ] || fail "backfill --rerun of replacer: $out"
# `orca rerun` of a run whose steps append, the same.
run=$(psql "$ORCA_DATABASE_URL" -Atc "SELECT min(id) FROM orca.runs WHERE pipeline = 'first'")
if out=$(./build/orca rerun "$run" step 2>&1); then
    fail "rerun of first's run $run wasn't refused: $out"
fi
echo "$out" | grep -qF "rerunning run $run from step step appends again" || { echo "$out"; fail "the rerun refusal"; }
./build/orca rerun --allow-append "$run" step >> "$log" 2>&1 || fail "rerun --allow-append"
echo "ok   datasets"
