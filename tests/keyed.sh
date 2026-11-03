#!/bin/sh
# Keyed partitions over tests/keyed/: `names` writes the dataset keys.logs,
# 50 log names each twice, and `labels` is partitioned by its log_id column.
#
# - `labels keys` prints the snapshot, then the 50 names once each, sorted;
# - `run --partitions @all` runs the mapped step for each and the gather
#   over all 50; a list and an @file run just theirs;
# - a range, an unknown name and a malformed one exit 2;
# - with Postgres, a run is recorded and resumed by its names;
# - describe says how the pipeline is keyed;
# - keyed pipelines with a schedule, with fills, or whose first step takes a
#   Month don't compile.
#
# Needs ORCA_DATABASE_URL (pixi run -e data test-keyed starts a throwaway
# server) and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
for p in names labels; do
    # shellcheck disable=SC2086
    mojo build -I src $DEP_INC "tests/keyed/$p.mojo" -o "build/kd-$p"
done
log=build/keyed.log
: > "$log"
export ORCA_WAREHOUSE=build/keyed-wh
rm -rf "$ORCA_WAREHOUSE"
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
labels() { ./build/kd-labels "$@" 2>> "$log"; }

snap=$(./build/kd-names "$ORCA_WAREHOUSE") || fail "names"
out=$(labels keys) || fail "keys exited $?"
[ "$(echo "$out" | head -1)" = "$snap" ] || fail "keys' snapshot: $(echo "$out" | head -1), not $snap"
want=$(i=0; while [ $i -lt 50 ]; do printf 'log-%02d\n' $i; i=$((i + 1)); done)
[ "$(echo "$out" | tail -n +2)" = "$want" ] || fail "keys: $(echo "$out" | tail -n +2 | tr '\n' ' ')"

# In memory: every key, a list, a file.
out=$(env -u ORCA_DATABASE_URL ./build/kd-labels run --partitions @all 2>> "$log") || fail "run @all"
echo "$out" >> "$log"
[ "$(echo "$out" | grep -c '^label log-[0-9][0-9] succeeded$')" -eq 50 ] || fail "50 labels"
echo "$out" | grep -q '^\[score\] 50 names, 300 bytes$' || fail "the score over 50"
echo "$out" | grep -qx "run succeeded" || fail "run @all didn't succeed"
out=$(env -u ORCA_DATABASE_URL ./build/kd-labels run --partitions log-07,log-03 2>> "$log") || fail "run a list"
[ "$(echo "$out" | grep '^label ' | tr '\n' ' ')" = "label log-07 succeeded label log-03 succeeded " ] || fail "the list: $out"
printf 'log-01\n\nlog-02\nlog-49\n' > build/keyed-list.txt
out=$(env -u ORCA_DATABASE_URL ./build/kd-labels run --partitions @file:build/keyed-list.txt 2>> "$log") || fail "run @file"
echo "$out" | grep -q '^\[score\] 3 names, 18 bytes$' || fail "the file's 3: $out"

# Refused with exit code 2: a range, a name it hasn't, a malformed one.
for bad in log-01..log-03 log-50 'log 1' @nope; do
    code=0
    env -u ORCA_DATABASE_URL ./build/kd-labels run --partitions "$bad" > build/keyed-bad.txt 2>&1 || code=$?
    [ "$code" -eq 2 ] || fail "--partitions '$bad' exited $code: $(cat build/keyed-bad.txt)"
done
env -u ORCA_DATABASE_URL ./build/kd-labels run --partitions log-50 2>&1 | grep -q "isn't a key of keys.logs" \
    || fail "an unknown name's message"

# Recorded, and resumed by its names.
out=$(labels run --partitions log-05,log-06) || fail "a recorded run"
run=$(echo "$out" | sed -n 's/^run \([0-9]*\)$/\1/p' | head -1)
[ "$(psql "$ORCA_DATABASE_URL" -Atc "SELECT partitions FROM orca.runs WHERE id = $run")" = "log-05,log-06" ] \
    || fail "run $run's partitions"
psql "$ORCA_DATABASE_URL" -qc "UPDATE orca.runs SET status = 'failed', heartbeat = NULL WHERE id = $run; DELETE FROM orca.attempts WHERE step IN (SELECT id FROM orca.steps WHERE run = $run AND step = 'score'); DELETE FROM orca.steps WHERE run = $run AND step = 'score'"
out=$(labels resume "$run") || fail "resume: $out"
echo "$out" | grep -q "resuming run $run with 2 steps done" || fail "resume: $out"
echo "$out" | grep -qx "run succeeded" || fail "the resumed run"

[ "$(labels describe | jq -c '[.partitions, .keys_per_run]')" \
    = '[{"kind":"keyed","start":null,"dataset":"keys.logs","column":"log_id"},10]' ] \
    || fail "describe: $(labels describe | jq -c .partitions)"

tests/compile_fail.sh tests/compile_fail_keyed || fail "a keyed pipeline that shouldn't compile"
echo "ok   keyed"
