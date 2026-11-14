#!/bin/sh
# The adoption rule (docs/overlap.md, morca.db.live.held_sql): a local run that
# another scheduler instance started (`runs.instance`) is left alone while
# that instance is seen and lists it, whatever its heartbeat, and counts as
# live; once the instance is gone (not seen for the stale limit, or ended)
# and the run's heartbeat is stale, it's queued again whatever its trigger,
# audited. A run of a live instance that no longer lists it is dead as
# before (a UI run fails), and so is a local run of no instance. The other
# instance is made up here as its rows: an `orca.schedulers` row, and runs
# placed local with its id. Over examples/toy.mojo, paused so `morca tick`
# starts nothing. Needs MORCA_DATABASE_URL (pixi run test-adopt starts a
# throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
scripts/pipelines.sh toy &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
b2=$!
wait "$b1"
wait "$b2"

log=build/adopt.log
: > "$log"
export MORCA_STALE_SECONDS=2 MORCA_WATCHDOG_SECONDS=0 MORCA_NOW=2024-01-15T00:00Z
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -qAtc "$1"; }
tick() { ./build/morca tick >> "$log" 2>&1 || fail "tick"; }
status() { q "SELECT status || ' ' || starts || ' ' || coalesce(held, '-') || ' ' || coalesce(error, '-') FROM orca.runs WHERE id = $1"; }

./build/morca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/morca pause toy >> "$log" 2>&1 || fail "pause"
inst=$(q "INSERT INTO orca.schedulers (host, pid, known, role, last_seen) VALUES ('old', 1, 42, 'draining', now()) RETURNING id")
# run KEY OWNER HEARTBEAT INSTANCE: a UI run going on this host since an hour.
run() {
    q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, owner, heartbeat, started, starts, placement, instance) SELECT 'toy', registry_hash, '$1', 'ui', 'running', '$2', $3, now() - interval '1 hour', 1, 'local', $4 FROM orca.pipelines WHERE name = 'toy' RETURNING id"
}
stale="now() - interval '1 hour'"
held=$(run 2024-03 host:11 "$stale" "$inst")       # its instance lists it
fresh=$(run 2024-04 host:12 "now()" "$inst")       # fresh, not listed
mine=$(run 2024-05 host:13 "$stale" NULL)          # no instance's, died
unclaimed=$(run 2024-06 scheduler NULL "$inst")    # started, never claimed
reaped=$(run 2024-07 host:14 "$stale" "$inst")     # its live instance reaped it
q "UPDATE orca.schedulers SET running = '$held,$unclaimed' WHERE id = $inst" > /dev/null

# --- A live instance holds its runs, whatever their heartbeats ---
tick
case "$(status "$held")" in "running 1 "*) ;; *) fail "a held run: $(status "$held")" ;; esac
case "$(status "$fresh")" in "running 1 "*) ;; *) fail "a fresh run: $(status "$fresh")" ;; esac
case "$(status "$unclaimed")" in "running 1 "*) ;; *) fail "an unclaimed run: $(status "$unclaimed")" ;; esac
[ "$(status "$mine")" = "failed 1 - interrupted: its process stopped" ] || fail "a dead run of no instance: $(status "$mine")"
[ "$(status "$reaped")" = "failed 1 - interrupted: its process stopped" ] || fail "a dead run its live instance doesn't list: $(status "$reaped")"
# Live, so a backfill leaves its key to it.
out=$(./build/morca backfill toy 2024-03..2024-04 2> build/adopt.err) || fail "backfill exited $?"
[ "$out" = "queued 0 runs" ] || fail "a backfill over the instance's runs: $out ($(cat build/adopt.err))"
grep -qx "skipped 2 in live runs: 2024-03 (run $held), 2024-04 (run $fresh)" build/adopt.err \
    || fail "the backfill didn't leave the keys to the instance's runs: $(cat build/adopt.err)"
echo "ok   a live instance's runs are left alone and live, a stale heartbeat or none"

# --- The instance gone: its stale runs are queued again ---
q "UPDATE orca.schedulers SET last_seen = now() - interval '1 minute' WHERE id = $inst" > /dev/null
tick
[ "$(status "$held")" = "queued 1 - -" ] || fail "the gone instance's run: $(status "$held")"
[ "$(status "$unclaimed")" = "queued 1 - -" ] || fail "the gone instance's unclaimed run: $(status "$unclaimed")"
case "$(status "$fresh")" in "running 1 "*) ;; *) fail "a fresh run of a gone instance: $(status "$fresh")" ;; esac
d=$(q "SELECT detail FROM orca.audit WHERE run = $held AND action = '/api/scheduler/requeue'")
case "$d" in "queued again: scheduler instance $inst on old stopped: not seen for "*) ;; *) fail "the requeue's audit: $d" ;; esac
[ "$(q "SELECT count(*) FROM orca.runs WHERE id IN ($held, $unclaimed) AND instance IS NULL")" = 2 ] \
    || fail "the requeued runs still name the instance"
grep -q "requeued run $held of toy: scheduler instance $inst" "$log" || fail "no log line for the requeue"
echo "ok   a gone instance's runs are queued again whatever their trigger, audited"

# --- An ended instance is gone at once; a fresh heartbeat is still left ---
q "UPDATE orca.schedulers SET last_seen = now(), ended = now() WHERE id = $inst" > /dev/null
tick
case "$(status "$fresh")" in "running 1 "*) ;; *) fail "a fresh run of an ended instance: $(status "$fresh")" ;; esac
q "UPDATE orca.runs SET heartbeat = $stale WHERE id = $fresh" > /dev/null
tick
[ "$(status "$fresh")" = "queued 1 - -" ] || fail "a stale run of an ended instance: $(status "$fresh")"
d=$(q "SELECT detail FROM orca.audit WHERE run = $fresh AND action = '/api/scheduler/requeue'")
[ "$d" = "queued again: scheduler instance $inst on old ended" ] || fail "its audit: $d"
echo "ok   an ended instance's run is queued again once its heartbeat is stale"
