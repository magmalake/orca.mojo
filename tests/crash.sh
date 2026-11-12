#!/bin/sh
# A run whose process crashes mid-step, under the scheduler's local runner,
# over examples/toy.mojo built with line tables (-g1, as the project image
# builds): the crash's stack dump is in the run's driver.log, symbolized,
# and the attempt it ended failed with the abort's message and the step's
# frame (morca.crash) rather than staying `running`. Needs MORCA_DATABASE_URL
# (pixi run test-crash starts a throwaway server) and llvm-symbolizer on
# PATH, which the pixi environment has.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -g1 -I src $DEP_INC examples/toy.mojo -o build/toy-g1
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/crash.log
: > "$log"
export MORCA_LOGS=build/crash-logs MORCA_STALE_SECONDS=2 MORCA_SAMPLE_SECONDS=0
rm -rf "$MORCA_LOGS"
sched=
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -qAtc "$1"; }

MORCA_NOW=2024-01-15T00:00Z ./build/morca register ./build/toy-g1 >> "$log" 2>&1 || fail "register"
MORCA_NOW=2024-01-15T00:00Z ./build/morca backfill toy 2024-02 >> "$log" 2>&1 || fail "backfill"
MORCA_NOW=2024-01-15T00:00Z MORCA_TOY_CRASH=2024-02 ./build/morca scheduler >> "$log" 2>&1 &
sched=$!

i=0
# Wait for the step's row too: the scheduler records the crashed attempt
# before it copies the error onto the step, and killing it in between left
# the step without its error (a race a fast, warm machine hits).
until [ "$(q "SELECT count(*) FROM orca.attempts WHERE error LIKE 'crashed%'")" -gt 0 ] \
    && [ "$(q "SELECT count(*) FROM orca.steps WHERE step = 'measure' AND partition = '2024-02' AND error LIKE 'crashed%'")" -gt 0 ]; do
    i=$((i + 1)); [ "$i" -lt 300 ] || fail "no attempt failed with the crash"; sleep 0.1
done
kill "$sched"
wait "$sched" 2> /dev/null || true
sched=

run=$(q "SELECT max(id) FROM orca.runs")
err=$(q "SELECT a.error FROM orca.attempts a JOIN orca.steps s ON a.step = s.id WHERE a.error LIKE 'crashed%' ORDER BY a.id LIMIT 1")
echo "$err" | grep -q "^crashed: .*crashing 2024-02 as asked; at measure .*toy.mojo:[0-9]" \
    || fail "the attempt's error: $err"
step_err=$(q "SELECT error FROM orca.steps WHERE run = $run AND step = 'measure' AND partition = '2024-02'")
[ "$step_err" = "$err" ] \
    || fail "the step's error isn't the attempt's: step has [$step_err], attempt has [$err], attempts: $(q "SELECT string_agg(a.id || ':' || coalesce(a.error, '-'), ' | ' ORDER BY a.id) FROM orca.attempts a")"
grep -q "^--- start 1" "$MORCA_LOGS/toy/$run/driver.log" || fail "no start line in the driver log"
grep -q "^ *#[0-9]* 0x[0-9a-f]* measure .*toy.mojo:" "$MORCA_LOGS/toy/$run/driver.log" \
    || fail "no symbolized frame in the driver log"
echo "ok   a crashed attempt failed with its stack dump's first frames: $err"
