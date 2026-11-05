#!/bin/sh
# Runs the scheduler doesn't retry (manual, ui, rerun) whose process died,
# over examples/toy.mojo: they don't hold their keys from a backfill, the
# scheduler fails them with an error instead of leaving them `running`, and
# a backfill says which keys it left and why. Needs ORCA_DATABASE_URL (pixi
# run test-dead-runs starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/dead-runs.log
: > "$log"
export ORCA_STALE_SECONDS=2
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -qAtc "$1"; }
backfill() { ORCA_NOW=2024-01-15T00:00Z ./build/orca backfill toy "$1" 2> build/dead-runs.err; }

# The scheduler's clock stays where the toy was registered, so it fires
# nothing of its own.
ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/toy run --partitions 2024-01 >> "$log" 2>&1 || fail "the run of 2024-01"

# A run by hand over March and April, killed mid-step.
ORCA_TOY_SLEEP=5 ./build/toy run --partitions 2024-03..2024-04 >> "$log" 2>&1 &
toy=$!
i=0
until [ "$(q "SELECT count(*) FROM orca.attempts")" -gt 3 ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the run never started a step"; sleep 0.1
done
kill -9 "$toy"
wait "$toy" 2> /dev/null || true
run=$(q "SELECT max(id) FROM orca.runs")
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = running ] || fail "the killed run isn't running"
sleep 2.5

# Its heartbeat is stale, so it doesn't hold March from a backfill, even
# before the scheduler has looked at it.
out=$(backfill 2024-03) || fail "backfill exited $?"
[ "$out" = "queued 1 runs" ] || fail "a backfill of a dead run's key: $out"
march=$(q "SELECT max(id) FROM orca.runs")

# A UI run whose process went too.
ui=$(q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, owner, heartbeat, started, starts) SELECT 'toy', registry_hash, '2024-06', 'ui', 'running', 'gone:1', now() - interval '1 hour', now() - interval '1 hour', 1 FROM orca.pipelines WHERE name = 'toy' RETURNING id")

# A tick fails both, saying why, and starts neither again.
q "UPDATE orca.runs SET status = 'cancelled' WHERE id = $march" > /dev/null
ORCA_NOW=2024-01-15T00:00Z ./build/orca tick >> "$log" 2>&1 || fail "tick"
for r in "$run" "$ui"; do
    [ "$(q "SELECT status || ': ' || coalesce(error, '') FROM orca.runs WHERE id = $r")" = "failed: interrupted: its process stopped" ] \
        || fail "dead run $r: $(q "SELECT status, starts, error FROM orca.runs WHERE id = $r")"
done
[ "$(q "SELECT starts FROM orca.runs WHERE id = $run")" -eq 0 ] || fail "the manual run was started"
! grep -q "started toy run" "$log" || fail "the scheduler started a run it doesn't retry"

# A backfill now queues their keys, and says which it left: January is
# done, and the run queued for April is live.
out=$(backfill 2024-04) || fail "backfill exited $?"
[ "$out" = "queued 1 runs" ] || fail "a backfill of a failed run's key: $out"
april=$(q "SELECT max(id) FROM orca.runs")
out=$(backfill 2024-01..2024-06) || fail "backfill exited $?"
[ "$out" = "queued 4 runs" ] || fail "the backfill of the half year: $out"
grep -qx "skipped 1 done: 2024-01" build/dead-runs.err || fail "the backfill didn't say 2024-01 was done: $(cat build/dead-runs.err)"
grep -qx "skipped 1 in live runs: 2024-04 (run $april)" build/dead-runs.err \
    || fail "the backfill didn't say 2024-04 was live: $(cat build/dead-runs.err)"
echo "ok   dead runs: a manual or UI run whose process died fails, and holds no keys"
