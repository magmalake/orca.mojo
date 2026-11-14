#!/bin/sh
# No Postgres deadlock on orca.runs (#440). A scheduler pass over a pool
# pipeline updates its queued runs over several statements (which run
# each waits behind, then the rest waiting for a worker), while other
# statements update many of the same runs at once: an operator's bulk
# update (tests/pool_fills.sh cancels 60 queued runs), a worker's claim
# that is refused and holds them. Locking the rows in crossing orders,
# they deadlocked (CI on sai, 2026-11-13); now each locks them in `id`
# order, and the pass takes all of its runs at once first.
#
# For STRESS_SECONDS (default 20), two loops of `morca tick` race a loop
# of bulk updates of 300 queued runs over one key and a loop of refused
# claims; then Postgres must have detected no deadlock
# (pg_stat_database.deadlocks), and no tick, update or claim failed.
#
# Needs MORCA_DATABASE_URL (pixi run test-run-locks starts a throwaway
# server), curl and psql.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
b1=$!
scripts/pipelines.sh toy &
b2=$!
wait "$b1"
wait "$b2"
log=build/run-locks.log
errs=build/run-locks-errors.out
: > "$log"
: > "$errs"
export MORCA_LOGS="$PWD/build/run-locks-logs"
rm -rf "$MORCA_LOGS" build/run-locks-tick*.out
unset MORCA_IMAGE
seconds=${STRESS_SECONDS:-20}
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
deadlocks() { q "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()"; }
. "$PWD/tests/port.sh"
free_port port
base="http://127.0.0.1:$port"

./build/morca register --placement worker:t ./build/toy >> "$log" 2>&1 || fail "register toy"
# One key, so each run but the first waits behind an older one: a pass
# says so on 299 runs, then that the first waits for a worker.
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status) SELECT name, registry_hash, '2024-01', 'ui', 'queued' FROM orca.pipelines, generate_series(1, 300) WHERE name = 'toy'" > /dev/null
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
pids="$pids $!"
i=0
until curl -fs "$base/" -o /dev/null; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
done
token=$(./build/morca worker-token create --pool t --label stress 2>> "$log") || fail "worker-token create"
before=$(deadlocks)

end=$(($(date +%s) + seconds))
loops=
for n in 1 2; do
    (
        while [ "$(date +%s)" -lt "$end" ]; do
            ./build/morca tick >> build/run-locks-tick$n.out 2>&1 || echo "tick $n failed" >> "$errs"
        done
    ) &
    loops="$loops $!"
done
# An operator's bulk update, in id order as `morca cancel` now is: every
# queued run says nothing, so the next pass says why again on all of them.
(
    while [ "$(date +%s)" -lt "$end" ]; do
        q "UPDATE orca.runs SET held = NULL WHERE id IN (SELECT id FROM orca.runs WHERE pipeline = 'toy' AND status = 'queued' ORDER BY id FOR UPDATE)" > /dev/null 2>> "$errs" \
            || echo "the bulk update failed" >> "$errs"
    done
) &
loops="$loops $!"
# A worker of another image: each claim is refused, and holds the queued
# runs saying why.
(
    while [ "$(date +%s)" -lt "$end" ]; do
        curl -fsS -o /dev/null -H "Authorization: Bearer $token" \
            -d '{"pool": "t", "slots": 1, "name": "stress", "image": "other", "running": ""}' \
            "$base/api/worker/claim" 2>> "$errs" || echo "a claim failed" >> "$errs"
    done
) &
loops="$loops $!"
pids="$pids $loops"
for p in $loops; do wait "$p"; done
sleep 1 # pg_stat_database is a moment behind
after=$(deadlocks)
if [ "$after" != "$before" ] || [ -s "$errs" ]; then
    grep -h -B1 -A3 deadlock build/run-locks-tick*.out | head -20
    head -20 "$errs"
    fail "$((after - before)) deadlocks in ${seconds}s"
fi
queued=$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND status = 'queued'")
[ "$queued" = 300 ] || fail "the runs: $(q "SELECT status, count(*) FROM orca.runs GROUP BY 1")"
echo "ok   ${seconds}s of ticks beside bulk updates and refused claims of 300 queued runs: no deadlock"
echo "run locks: ok"
