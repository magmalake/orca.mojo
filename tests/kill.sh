#!/bin/sh
# A run killed at a random point, then resumed, finishes with every step
# succeeded exactly once. Needs MORCA_DATABASE_URL (pixi run test-kill starts
# a throwaway server). KILL_RUNS sets how many runs (default 8), KILL_SEED the
# first seed.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy

runs=${KILL_RUNS:-8}
seed=${KILL_SEED:-$$}
export MORCA_STALE_SECONDS=1
export MORCA_TOY_SLEEP=0.05
months=2024-01..2024-06 # 6 days + 6 measure + 1 total = 13 steps, ~0.7 s
log=build/kill.log

fail() {
    echo "FAIL (seed $seed): $*"
    cat "$log"
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }

i=0
killed=0
while [ "$i" -lt "$runs" ]; do
    i=$((i + 1))
    seed=$((seed + 1))
    delay=$(awk -v s="$seed" 'BEGIN { srand(s); printf "%.2f", 0.15 + rand() * 0.7 }')
    ./build/toy run --partitions "$months" > "$log" 2>&1 &
    pid=$!
    sleep "$delay"
    kill -9 "$pid" 2> /dev/null || true
    wait "$pid" 2> /dev/null || true
    run=$(sed -n 's/^run \([0-9][0-9]*\)$/\1/p' "$log" | head -1)
    if [ -z "$run" ]; then
        echo "kill $i: before the run started (${delay}s)"
        continue
    fi
    # The database, not the log: a kill can land after the run is recorded
    # as succeeded and before it says so.
    if [ "$(q "SELECT status FROM orca.runs WHERE id = $run")" != succeeded ]; then
        killed=$((killed + 1))
        # Wait out the dead process's heartbeat, then resume.
        tries=0
        until ./build/toy resume "$run" >> "$log" 2>&1; do
            tries=$((tries + 1))
            [ "$tries" -lt 10 ] || fail "run $run never resumed"
            sleep 0.5
        done
    fi
    [ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = succeeded ] \
        || fail "run $run didn't succeed"
    [ "$(q "SELECT count(*) FROM orca.steps WHERE run = $run AND status = 'succeeded'")" = 13 ] \
        || fail "run $run doesn't have 13 succeeded steps"
    [ "$(q "SELECT count(*) FROM orca.steps s WHERE s.run = $run AND (SELECT count(*) FROM orca.attempts a WHERE a.step = s.id AND a.status = 'succeeded') <> 1")" = 0 ] \
        || fail "run $run has a step that succeeded more or less than once"
    [ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND a.status = 'running'")" = 0 ] \
        || fail "run $run has an attempt left running"
    grep -q '6 months, 4368 hours' "$log" || fail "run $run's total is wrong"
    echo "kill $i: run $run killed after ${delay}s, $(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run") attempts"
done
[ "$killed" -gt 0 ] || fail "no run was killed mid-way"

# Resuming with different code is refused unless asked for.
MORCA_TOY_FAIL=2024-02 ./build/toy run --partitions "$months" > "$log" 2>&1 && fail "the failing run succeeded"
run=$(sed -n 's/^run \([0-9][0-9]*\)$/\1/p' "$log" | head -1)
q "UPDATE orca.runs SET registry_hash = '0xdead' WHERE id = $run" > /dev/null
set +e
./build/toy resume "$run" > "$log" 2>&1
code=$?
set -e
[ "$code" -eq 3 ] || fail "resume with new code exited $code, not 3"
grep -q 'ran code 0xdead' "$log" || fail "resume with new code didn't say why"
./build/toy resume "$run" --allow-new-code > "$log" 2>&1 || fail "--allow-new-code didn't resume"
grep -q '^measure 2024-02 succeeded after 2 attempts$' "$log" || fail "measure 2024-02 wasn't retried"
echo "ok   kill ($killed of $runs killed mid-run)"
