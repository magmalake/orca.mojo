#!/bin/sh
# The adoption rule (docs/runner.md, orca.live.held_sql): a local run that
# `orca runner` (a worker of pool local) claimed is left alone by the
# scheduler while its runner is seen and lists it, whatever its heartbeat,
# and counts as live; once its runner is gone, or seen without it, and its
# heartbeat is stale, it's queued again whatever its trigger, audited, a
# start it never made given back. A local run of no runner whose process
# died fails as before. The runner is made up here as its rows: a
# `orca.workers` row of pool local, and runs placed local with its id.
# Over examples/toy.mojo, paused so `orca tick` starts nothing. Needs
# ORCA_DATABASE_URL (pixi run test-adopt starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
b2=$!
wait "$b1"
wait "$b2"

log=build/adopt.log
: > "$log"
export ORCA_STALE_SECONDS=2 ORCA_WATCHDOG_SECONDS=0 ORCA_NOW=2024-01-15T00:00Z
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -qAtc "$1"; }
tick() { ./build/orca tick >> "$log" 2>&1 || fail "tick"; }
status() { q "SELECT status || ' ' || starts || ' ' || coalesce(held, '-') || ' ' || coalesce(error, '-') FROM orca.runs WHERE id = $1"; }

./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/orca pause toy >> "$log" 2>&1 || fail "pause"
runner=$(q "INSERT INTO orca.workers (token_hash, pool, label, created_by, expires, last_seen) VALUES ('test', 'local', 'runner', 'test', now() + interval '1 day', now()) RETURNING id")
# run KEY OWNER HEARTBEAT WORKER: a UI run going on this host since an hour.
run() {
    q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, owner, heartbeat, started, starts, placement, worker) SELECT 'toy', registry_hash, '$1', 'ui', 'running', '$2', $3, now() - interval '1 hour', 1, 'local', $4 FROM orca.pipelines WHERE name = 'toy' RETURNING id"
}
stale="now() - interval '1 hour'"
held=$(run 2024-03 host:11 "$stale" "$runner")       # its runner lists it
orphan=$(run 2024-04 host:12 "now()" "$runner")      # fresh, not listed
mine=$(run 2024-05 host:13 "$stale" NULL)            # the scheduler's, died
claimed=$(run 2024-06 worker NULL "$runner")         # claimed, never started
q "UPDATE orca.workers SET running = '$held,$claimed' WHERE id = $runner" > /dev/null

# --- A live runner holds its runs, whatever their heartbeats ---
tick
case "$(status "$held")" in "running 1 "*) ;; *) fail "a held run: $(status "$held")" ;; esac
case "$(status "$orphan")" in "running 1 "*) ;; *) fail "a fresh run: $(status "$orphan")" ;; esac
case "$(status "$claimed")" in "running 1 "*) ;; *) fail "a claimed run: $(status "$claimed")" ;; esac
[ "$(status "$mine")" = "failed 1 - interrupted: its process stopped" ] || fail "the scheduler's dead run: $(status "$mine")"
# Live, so a backfill leaves its key to it.
out=$(./build/orca backfill toy 2024-03..2024-04 2> build/adopt.err) || fail "backfill exited $?"
[ "$out" = "queued 0 runs" ] || fail "a backfill over the runner's runs: $out ($(cat build/adopt.err))"
grep -qx "skipped 2 in live runs: 2024-03 (run $held), 2024-04 (run $orphan)" build/adopt.err \
    || fail "the backfill didn't leave the keys to the runner's runs: $(cat build/adopt.err)"
echo "ok   a live runner's runs are left alone and live, a stale heartbeat or none"

# --- The runner gone: its stale and unstarted runs are queued again ---
q "UPDATE orca.workers SET last_seen = now() - interval '1 minute' WHERE id = $runner" > /dev/null
tick
[ "$(status "$held")" = "queued 1 - -" ] || fail "the gone runner's run: $(status "$held")"
[ "$(status "$claimed")" = "queued 0 - -" ] || fail "the gone runner's unstarted run: $(status "$claimed")"
case "$(status "$orphan")" in "running 1 "*) ;; *) fail "a fresh run of a gone runner: $(status "$orphan")" ;; esac
d=$(q "SELECT detail FROM orca.audit WHERE run = $held AND action = '/api/worker/requeue'")
case "$d" in "queued again: worker runner (token $runner) stopped: not seen for "*) ;; *) fail "the requeue's audit: $d" ;; esac
d=$(q "SELECT detail FROM orca.audit WHERE run = $claimed AND action = '/api/worker/requeue'")
case "$d" in *", before starting it") ;; *) fail "the unstarted run's audit: $d" ;; esac
grep -q "requeued run $held of toy: worker runner" "$log" || fail "no log line for the requeue"
echo "ok   a gone runner's runs are queued again whatever their trigger, audited"

# --- A runner seen without a run whose heartbeat went stale ---
q "UPDATE orca.workers SET last_seen = now(), running = NULL WHERE id = $runner" > /dev/null
tick
case "$(status "$orphan")" in "running 1 "*) ;; *) fail "a fresh run its runner doesn't list: $(status "$orphan")" ;; esac
q "UPDATE orca.runs SET heartbeat = now() - interval '1 hour' WHERE id = $orphan" > /dev/null
tick
[ "$(status "$orphan")" = "queued 1 - -" ] || fail "a stale run its runner doesn't list: $(status "$orphan")"
d=$(q "SELECT detail FROM orca.audit WHERE run = $orphan AND action = '/api/worker/requeue'")
[ "$d" = "queued again: worker runner (token $runner) no longer runs it" ] || fail "its audit: $d"
echo "ok   a run its live runner no longer lists is queued again once its heartbeat is stale"
