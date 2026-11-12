#!/bin/sh
# Rolling back: a morca that knows fewer migrations than the database has
# had (MORCA_TEST_MIGRATIONS=N acts as one) runs on it while the migrations
# it doesn't know are additive, and refuses once one raised the floor
# (src/morca/state.mojo's `_floor_raisers`; 24 is one):
#
# - a binary that knows 39 of 41 registers, ticks and runs a run, says once
#   that the database is ahead, and leaves the schema version alone;
# - one that knows 23 exits with why, and starts nothing.
#
# Needs MORCA_DATABASE_URL (pixi run test-older-binary starts a throwaway
# server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/older-binary.log
: > "$log"
export MORCA_LOGS=build/older-binary-logs
rm -rf "$MORCA_LOGS"
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }

# This binary migrates the database to the latest.
./build/morca register ./build/toy >> "$log" 2>&1 || fail "register"
latest=$(q "SELECT max(version) FROM orca.schema_version")
[ "$latest" -ge 41 ] || fail "schema version $latest"

# An older one, two migrations behind (45 and 46 are additive).
older=$((latest - 2))
MORCA_TEST_MIGRATIONS=$older ./build/morca register ./build/toy > /dev/null 2> "$log.err" \
    || { cat "$log.err" >> "$log"; fail "an older binary's register"; }
grep -q "the database is at schema version $latest, ahead of this binary's $older; its migrations since are additive, so running on it" "$log.err" \
    || fail "no ahead line: $(cat "$log.err")"
[ "$(grep -c 'ahead of this binary' "$log.err")" = 1 ] || fail "the ahead line more than once: $(cat "$log.err")"
[ "$(MORCA_TEST_MIGRATIONS=$older ./build/morca backfill toy 2024-03 2>> "$log")" = "queued 1 runs" ] \
    || fail "an older binary's backfill"
MORCA_NOW=2024-04-01T00:00Z MORCA_TEST_MIGRATIONS=$older ./build/morca tick >> "$log" 2>&1 \
    || fail "an older binary's tick"
i=0
until [ "$(q "SELECT status FROM orca.runs WHERE partitions = '2024-03'")" = succeeded ]; do
    i=$((i + 1))
    [ "$i" -lt 300 ] || fail "the run is $(q "SELECT status FROM orca.runs WHERE partitions = '2024-03'")"
    MORCA_NOW=2024-04-01T00:00Z MORCA_TEST_MIGRATIONS=$older ./build/morca tick >> "$log" 2>&1 || true
    sleep 0.2
done
[ "$(q "SELECT max(version) FROM orca.schema_version")" = "$latest" ] || fail "the older binary changed the schema version"

# One from before migration 24, which raised the floor, refuses.
if MORCA_TEST_MIGRATIONS=23 ./build/morca tick > "$log.err" 2>&1; then
    fail "a binary that knows 23 migrations ran"
fi
grep -q "newer than this binary's 23, and a migration since needs a binary that knows at least 24" "$log.err" \
    || fail "no refusal: $(cat "$log.err")"

echo "ok   older binary"
