#!/bin/sh
# One live run per partition (examples/toy.mojo, max_runs=2): a fire for a
# month a live run has is left to that run, and the UI won't start a second
# run over it; a schedule that fires more than once per partition is warned
# of at registration (tests/six_hourly.mojo). Sensors and rebuilds are
# checked in tests/sensors.sh. Needs MORCA_DATABASE_URL (pixi run
# test-one-run-per-key starts a throwaway server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
scripts/pipelines.sh toy
scripts/pipelines.sh six-hourly
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/one-run-per-key.log
: > "$log"
ui=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -qAtc "$1"; }
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('queued', 'running')")" -gt 0 ]; do
        MORCA_NOW=2024-03-02T03:00Z ./build/morca tick >> "$log" 2>&1
        i=$((i + 1)); [ "$i" -lt 300 ] || fail "runs didn't finish"; sleep 0.2
    done
}

MORCA_NOW=2024-01-15T00:00Z ./build/morca register ./build/toy >> "$log" 2>&1 || fail "register"
! grep -q "warning" "$log" || fail "the toy's monthly schedule was warned of"

free_port port
base="http://127.0.0.1:$port"
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
done
start() {
    curl -s -o build/one-run-per-key.body -w '%{http_code}' -H "Origin: $base" \
        --data-urlencode "partitions=$1" "$base/pipelines/toy/runs"
}

# The UI starts February, then won't start it again while that run is live.
[ "$(start 2024-02)" = 303 ] || fail "the UI's start of 2024-02"
feb=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-02'")
[ "$(start 2024-01..2024-02)" = 409 ] || fail "a second start over 2024-02 wasn't refused"
grep -q "2024-02 is in run $feb" build/one-run-per-key.body || fail "the refusal doesn't name the run"

# February's fire is left to that run instead of queuing a second one.
MORCA_NOW=2024-03-02T03:00Z ./build/morca tick >> "$log" 2>&1 || fail "tick"
[ "$(q "SELECT run FROM orca.fires WHERE pipeline = 'toy' AND partition = '2024-02'")" = "$feb" ] \
    || fail "the fire's run: $(q "SELECT partition, run FROM orca.fires")"
grep -q "for 2024-02 left to live run $feb" "$log" || fail "the tick didn't say the fire was left to run $feb"
settle
[ "$(q "SELECT count(*) FROM orca.runs WHERE partitions LIKE '%2024-02%'")" -eq 1 ] \
    || fail "two runs over 2024-02: $(q "SELECT id, partitions, trigger FROM orca.runs")"

# Once it has ended, the UI may run the month again.
[ "$(start 2024-02)" = 303 ] || fail "the UI's start of a done month"
kill "$ui"
wait "$ui" 2> /dev/null || true
ui=

# Four fires a day for daily partitions: registered, with a warning.
out=$(MORCA_NOW=2024-01-15T00:00Z ./build/morca register ./build/six-hourly 2>&1) || fail "register six_hourly: $out"
echo "$out" >> "$log"
echo "$out" | grep -q "^morca: warning: the schedule of six_hourly (0 \*/6 \* \* \*) fires more than once for 2024-01-14" \
    || fail "no warning: $out"
echo "ok   one run per key: fires and UI starts leave a live run's keys to it"
