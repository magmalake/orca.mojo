#!/bin/sh
# A snapshot sensed while a run of its partition is live (#139), over
# tests/sensors' clean (monthly, max_runs 2 here, so only the rule below
# keeps a second run of a month from starting), driven by `orca tick`:
#
# - a live run that read an older raw.events has its month queued again,
#   in a run of its own that waits for it, then runs on the new snapshot;
# - a live run that read the new snapshot already is left the month, and
#   nothing is queued.
#
# The live runs are rows a process elsewhere would hold. Needs
# ORCA_DATABASE_URL (pixi run -e data test-sensed-behind-live starts a
# throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
pids=
for p in source clean; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src -I tests/sensors -I tests/lineage $DEP_INC "tests/sensors/$p.mojo" -o "build/sbl-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
for pid in $pids $!; do wait "$pid"; done
log=build/sensed-behind-live.log
: > "$log"
proj=$PWD/build/sbl-project
rm -rf "$proj" build/sbl-wh build/sbl-logs
mkdir -p "$proj"
export ORCA_WAREHOUSE=$PWD/build/sbl-wh ORCA_LOGS=$PWD/build/sbl-logs
export ORCA_SENSE_SECONDS=0 ORCA_WATCHDOG_SECONDS=0 ORCA_SAMPLE_SECONDS=0
export ORCA_NOW=2024-05-15T00:00Z
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -qAtc "$1"; }
source_commit() { ./build/sbl-source "$ORCA_WAREHOUSE" "$@" 2>> "$log" || fail "source $*"; }
tick() { ./build/orca tick >> "$log" 2>&1 || fail "tick exited $?"; }
sensed_run() { q "SELECT run FROM orca.sensed WHERE pipeline = 'clean' AND snapshot_id = $1"; }
# A run of clean over $1, running elsewhere, whose step read raw.events@$2.
live_run() {
    q "WITH r AS (INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, owner, heartbeat, started, starts) SELECT name, registry_hash, '$1', 'manual', 'running', 'elsewhere:1', now(), now(), 1 FROM orca.pipelines WHERE name = 'clean' RETURNING id, registry_hash), s AS (INSERT INTO orca.steps (run, step, partition, ord, status) SELECT id, 'clean', '$1', 0, 'running' FROM r RETURNING id), a AS (INSERT INTO orca.attempts (step, attempt, status, host, registry_hash) SELECT s.id, 1, 'running', 'elsewhere', r.registry_hash FROM s, r RETURNING id) INSERT INTO orca.reads (attempt, table_name, snapshot_id, dataset) SELECT id, '$ORCA_WAREHOUSE/raw/events', $2, 'raw.events' FROM a RETURNING (SELECT id FROM r)"
}
end_run() { q "UPDATE orca.runs SET status = 'succeeded', ended = now(), heartbeat = NULL WHERE id = $1" > /dev/null; }

s0=$(source_commit 2024-01)
cat > "$proj/orca.toml" << 'EOF'
[pipelines.clean]
max_runs = 2
EOF
./build/orca register --config "$proj/orca.toml" ./build/sbl-clean >> "$log" 2>&1 || fail "register"

# A live run read raw.events@s0; s1 holds its month: queued behind it.
live=$(live_run 2024-03 "$s0")
s1=$(source_commit 2024-03)
tick
behind=$(sensed_run "$s1")
[ -n "$behind" ] && [ "$behind" != "$live" ] || fail "raw.events@$s1 was left to live run $live, which read @$s0"
[ "$(q "SELECT partitions || ' ' || trigger || ' ' || status FROM orca.runs WHERE id = $behind")" = "2024-03 sensor queued" ] \
    || fail "the run behind it: $(q "SELECT partitions, trigger, status FROM orca.runs WHERE id = $behind")"
grep -q "behind live run $live" "$log" || fail "the tick didn't say it queued behind run $live"
# It waits while the live run goes on, though a slot is free.
for _ in 1 2; do
    q "UPDATE orca.runs SET heartbeat = now() WHERE id = $live" > /dev/null
    tick
done
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $behind")" = "queued 0" ] \
    || fail "run $behind started beside live run $live: $(q "SELECT status, starts FROM orca.runs WHERE id = $behind")"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'clean' AND partitions LIKE '%2024-03%'")" -eq 2 ] \
    || fail "runs over 2024-03: $(q "SELECT id, partitions, status FROM orca.runs WHERE pipeline = 'clean'")"
# Once it ends, the run behind it runs, on the new snapshot.
end_run "$live"
i=0
until [ "$(q "SELECT status FROM orca.runs WHERE id = $behind")" = succeeded ]; do
    i=$((i + 1)); [ "$i" -lt 300 ] || fail "run $behind: $(q "SELECT status, error FROM orca.runs WHERE id = $behind")"
    tick
    sleep 0.2
done
[ "$(q "SELECT string_agg(DISTINCT rd.snapshot_id::text, ',') FROM orca.reads rd JOIN orca.attempts a ON a.id = rd.attempt JOIN orca.steps s ON s.id = a.step WHERE s.run = $behind AND rd.dataset = 'raw.events'")" = "$s1" ] \
    || fail "run $behind didn't read raw.events@$s1"

# A live run that read s2 already: s2 is left to it.
s2=$(source_commit 2024-04)
live2=$(live_run 2024-04 "$s2")
tick
[ "$(sensed_run "$s2")" = "$live2" ] || fail "raw.events@$s2 wasn't left to live run $live2, which read it: run $(sensed_run "$s2")"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'clean' AND partitions LIKE '%2024-04%'")" -eq 1 ] \
    || fail "a run was queued beside live run $live2"
grep -q "left to live run $live2" "$log" || fail "the tick didn't say it left raw.events@$s2 to run $live2"
end_run "$live2"
echo "ok   sensed behind live: an older pin queues the keys behind the run, the same one leaves them to it"
