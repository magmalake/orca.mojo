#!/bin/sh
# A pipeline binary that hangs holds up nothing else (#64). With
# tests/fake_sensor's `sense` hanging and ORCA_BINARY_SECONDS=3:
#
# - the scheduler's ticks go on: the toy pipeline's due fire runs while the
#   sense is still going;
# - the sense is killed after 3 s, and kept as the sensor's sense_error;
#   once sense answers again, the error is cleared;
# - orca ui's keyed pipeline page (tests/fake_keyed) runs `keys` once for
#   many views, keeps what it printed in orca.keys, and when `keys` hangs,
#   answers within its own 10 s with the keys kept and a note.
#
# Needs ORCA_DATABASE_URL (pixi run test-binary-timeouts starts a
# throwaway server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/binary-timeouts.log
: > "$log"
export ORCA_SENSE_SECONDS=0 ORCA_BINARY_SECONDS=3 ORCA_STALE_SECONDS=5
export FAKE_SENSE="$PWD/build/binary-timeouts-sense.txt" FAKE_KEYS="$PWD/build/binary-timeouts-keys"
rm -f "$FAKE_SENSE".* "$FAKE_KEYS".*
sched=
ui=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    [ ! -s "$FAKE_SENSE.pid" ] || kill "$(cat "$FAKE_SENSE.pid")" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
until_true() {
    i=0
    until [ "$(q "$1")" = t ]; do
        i=$((i + 1)); [ "$i" -lt "$2" ] || fail "waited for: $1"; sleep 0.1
    done
}

printf 'raw.a -1 other -\nraw.b -1 other -\n' > "$FAKE_SENSE"
for b in tests/fake_sensor build/toy tests/fake_keyed; do
    ORCA_NOW=2024-01-15T00:00Z ./build/orca register "$b" >> "$log" 2>&1 || fail "register $b"
done
# The toy pipeline's fire for 2024-01 is due.
export ORCA_NOW=2024-02-02T03:00Z

# fake_sensor comes first in each tick, and its sense hangs.
touch "$FAKE_SENSE.slow"
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
until_true "SELECT count(*) = 1 FROM orca.runs WHERE pipeline = 'toy' AND status = 'succeeded'" 100
[ -s "$FAKE_SENSE.pid" ] || fail "sense wasn't asked"
pid=$(cat "$FAKE_SENSE.pid")
until_true "SELECT sense_error LIKE '%sense% took longer than 3 s, and was killed' FROM orca.pipelines WHERE name = 'fake_sensor'" 100
sleep 4
[ "$(grep -c "fake_sensor can't sense" "$log")" -eq 1 ] || fail "the hung sense was said $(grep -c "can't sense" "$log") times"
i=0
while kill -0 "$pid" 2> /dev/null; do
    i=$((i + 1)); [ "$i" -lt 50 ] || fail "the hung sense (pid $pid) is still there"; sleep 0.1
done
rm "$FAKE_SENSE.slow"
until_true "SELECT sense_error IS NULL FROM orca.pipelines WHERE name = 'fake_sensor'" 100
kill "$sched"
sched=

# The UI's keyed page: keys read once, then kept.
free_port port
base="http://127.0.0.1:$port"
ORCA_SENSE_SECONDS=2 ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json() { curl -fsS -m 30 -H 'Accept: application/json' "$base/pipelines/fake_keyed" || fail "GET /pipelines/fake_keyed"; }
for _ in 1 2 3; do
    [ "$(json | jq -c '[.keys.snapshot, .keys.total, .keys.error]')" = '[7,3,""]' ] || fail "the keys: $(json)"
done
[ "$(wc -l < "$FAKE_KEYS.calls")" -eq 1 ] || fail "keys ran $(wc -l < "$FAKE_KEYS.calls") times for three views"
[ "$(q "SELECT snapshot_id || ' ' || replace(keys, chr(10), ',') FROM orca.keys WHERE pipeline = 'fake_keyed'")" = "7 ann,bob,cy" ] \
    || fail "orca.keys: $(q "SELECT * FROM orca.keys")"
# Kept keys go stale after ORCA_SENSE_SECONDS; a keys that hangs then
# costs the page its 10 s, not a hang, and the kept ones are shown.
touch "$FAKE_KEYS.slow"
sleep 2.5
start=$(date +%s)
page=$(json)
took=$(($(date +%s) - start))
[ "$took" -lt 20 ] || fail "the page took $took s"
[ "$(printf '%s' "$page" | jq -c '[.keys.snapshot, .keys.total]')" = '[7,3]' ] || fail "the kept keys: $page"
printf '%s' "$page" | jq -e '.keys.note | contains("took longer than 10 s")' > /dev/null || fail "no note: $page"
kill "$ui"
ui=
echo "ok   binary timeouts"
