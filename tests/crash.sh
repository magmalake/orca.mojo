#!/bin/sh
# A run whose process crashes mid-step, under the scheduler's local runner,
# over examples/toy.mojo built with line tables (-g1, as the project image
# builds): the crash's stack dump is in the run's driver.log, symbolized,
# and the attempt it ended failed with the abort's message and the step's
# frame (orca.crash) rather than staying `running`. Needs ORCA_DATABASE_URL
# (pixi run test-crash starts a throwaway server) and llvm-symbolizer on
# PATH, which the pixi environment has.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -g1 -I src $DEP_INC examples/toy.mojo -o build/toy-g1
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/crash.log
: > "$log"
export ORCA_LOGS=build/crash-logs ORCA_STALE_SECONDS=2 ORCA_SAMPLE_SECONDS=0
rm -rf "$ORCA_LOGS"
sched=
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -qAtc "$1"; }

ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy-g1 >> "$log" 2>&1 || fail "register"
ORCA_NOW=2024-01-15T00:00Z ./build/orca backfill toy 2024-02 >> "$log" 2>&1 || fail "backfill"
ORCA_NOW=2024-01-15T00:00Z ORCA_TOY_CRASH=2024-02 ./build/orca scheduler >> "$log" 2>&1 &
sched=$!

i=0
until [ "$(q "SELECT count(*) FROM orca.attempts WHERE error LIKE 'crashed%'")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 300 ] || fail "no attempt failed with the crash"; sleep 0.1
done
kill "$sched"
wait "$sched" 2> /dev/null || true
sched=

run=$(q "SELECT max(id) FROM orca.runs")
err=$(q "SELECT a.error FROM orca.attempts a JOIN orca.steps s ON a.step = s.id WHERE a.error LIKE 'crashed%' ORDER BY a.id LIMIT 1")
echo "$err" | grep -q "^crashed: .*crashing 2024-02 as asked; at measure .*toy.mojo:[0-9]" \
    || fail "the attempt's error: $err"
[ "$(q "SELECT error FROM orca.steps WHERE run = $run AND step = 'measure' AND partition = '2024-02'")" = "$err" ] \
    || fail "the step's error isn't the attempt's"
grep -q "^--- start 1" "$ORCA_LOGS/toy/$run/driver.log" || fail "no start line in the driver log"
grep -q "^#[0-9]* 0x[0-9a-f]* measure .*toy.mojo:" "$ORCA_LOGS/toy/$run/driver.log" \
    || fail "no symbolized frame in the driver log"
echo "ok   a crashed attempt failed with its stack dump's first frames: $err"
