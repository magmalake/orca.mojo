#!/bin/sh
# Each attempt's log is where `status` says, with the step's lines and its
# error; a run of a pipeline with dista tasks leaves its timeline beside them.
# Needs ORCA_DATABASE_URL; pixi run test-logs gives it a fresh SQLite file,
# since nothing here needs Postgres.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/fanout.mojo -o build/fanout

ORCA_LOGS=$(pwd)/build/logs-$$
export ORCA_LOGS
trap 'rm -rf "$ORCA_LOGS"' EXIT
find_logs() { find "$1" -name "*.log" | sort; }
log=build/logs.log

fail() {
    echo "FAIL: $*"
    cat "$log"
    exit 1
}

# A failed attempt, resumed: two logs for measure 2024-02, each where it says.
ORCA_TOY_FAIL=2024-02 ./build/toy run --partitions 2024-01..2024-02 > "$log" 2>&1 \
    && fail "the failing run succeeded"
run=$(sed -n 's/^run \([0-9][0-9]*\)$/\1/p' "$log" | head -1)
./build/toy resume "$run" > "$log" 2>&1 || fail "resume failed"
./build/toy status "$run" > "$log" 2>&1
dir="$ORCA_LOGS/toy/$run"
grep -qxF "measure 2024-02 succeeded after 2 attempts [$dir/measure/2024-02/2.log]" "$log" \
    || fail "status doesn't show measure 2024-02's second log"
grep -qxF "total succeeded [$dir/total/_run/1.log]" "$log" \
    || fail "status doesn't show total's log"
grep -q 'measure 2024-02\] attempt 1 failed: failing 2024-02 as asked$' "$dir/measure/2024-02/1.log" \
    || fail "the first attempt's log lacks its error"
grep -q '^20[0-9-]*T[0-9:]*Z \[measure 2024-02\] 2024-02 has 29 days$' "$dir/measure/2024-02/2.log" \
    || fail "the second attempt's log lacks its line"
n=$(find "$dir" -name '*.log' | wc -l)
[ "$n" -eq 6 ] || { find "$dir" -name '*.log' | sort; fail "$n logs under $dir, not 6"; }

# dista inside steps, through the CLI: the timeline lands beside the logs.
./build/fanout run --partitions 2024-01..2024-03 > "$log" 2>&1 || fail "fanout failed"
run=$(sed -n 's/^run \([0-9][0-9]*\)$/\1/p' "$log" | head -1)
grep -q '91 days\|31 days, ' "$log" || fail "fanout didn't fan out"
grep -q '^\[all_trips\] .* trips in 3 months$' "$log" || fail "no total"
timeline="$ORCA_LOGS/fanout/$run/timeline.json"
[ -s "$timeline" ] || fail "no timeline at $timeline"
grep -q '"ph":"X"' "$timeline" || fail "the timeline isn't a Chrome trace"
grep -q 'month_trips 2024-02\] 29 days' "$ORCA_LOGS/fanout/$run/month_trips/2024-02/1.log" \
    || fail "fanout's step log lacks its line"
echo "ok   logs"
