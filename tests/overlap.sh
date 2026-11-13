#!/bin/sh
# Two scheduler instances on one Postgres (docs/overlap.md), with
# MORCA_OVERLAP=on, over examples/toy.mojo (max_runs 2, every step taking
# MORCA_TOY_SLEEP seconds):
#
# - an instance refuses to start without its drain limit
#   (MORCA_DRAIN_SECONDS unset, 0, negative or not a number, #390), before
#   it touches the database;
# - instance A leads and starts two runs, its children;
# - instance B joins: A drains (starts nothing more), B leads; a third run
#   waits while A's two are going, counted against max_runs across both,
#   and A's runs are left alone: each succeeds with one start, no double
#   start;
# - A exits once its runs have ended, its row ended; then B starts the
#   third run;
# - instance C joins while B's run is going, and B's drain runs out
#   (MORCA_DRAIN_SECONDS): B kills it and exits, and C queues it again once
#   its heartbeat is stale (audited) and runs it to success, a second start;
# - `morca scheduler status` lists the instances, and `morca register`
#   while the newest instance doesn't lead yet waits, then refuses;
# - SIGTERM to the leading instance C (docker stop) kills its run and ends
#   its row before it exits, so nothing counts it leading (#426).
#
# Then the floors (morca.state._floor_raisers): `morca scheduler can-overlap`
# says yes beside a live leading instance that knows this binary's floor
# and no beside one that doesn't, and a migration that would raise the
# floor past a live instance is refused.
#
# Needs MORCA_DATABASE_URL (pixi run test-overlap starts a throwaway server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
b2=$!
wait "$b1"
wait "$b2"
log=build/overlap.log
: > "$log"
rm -f build/overlap-*.out
export MORCA_STALE_SECONDS=4 MORCA_SAMPLE_SECONDS=0 MORCA_WATCHDOG_SECONDS=0
export MORCA_NOW=2024-01-15T00:00Z MORCA_OVERLAP=on MORCA_TOY_SLEEP=2
export MORCA_DRAIN_SECONDS=3600
unset MORCA_LOGS
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/overlap-*.out; do [ -f "$f" ] && { echo "== $f"; tail -20 "$f"; }; done
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -qAtc "$1"; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
is() { [ "$(q "$1")" = "$2" ]; }
said() { grep -q "$2" "build/overlap-$1.out"; }
runs() { q "SELECT id, status, starts, owner, instance, held, error FROM orca.runs ORDER BY id"; }
# scheduler NAME [ENV...]: an instance, its output in build/overlap-NAME.out.
scheduler() {
    name=$1; shift
    env "$@" ./build/morca scheduler > "build/overlap-$name.out" 2>&1 &
    eval "pid_$name=$!"
    pids="$pids $!"
}
# done_once RUN STARTS: succeeded with STARTS starts, one attempt a step.
done_once() {
    is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $1" "succeeded $2" \
        && is "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $1" 3
}

# --- No drain limit: refused, before anything (#390) ---
for d in unset "" 0 -5 soon; do
    if [ "$d" = unset ]; then
        env -u MORCA_DRAIN_SECONDS ./build/morca scheduler > build/overlap-nolimit.out 2>&1 && fail "a scheduler started without MORCA_DRAIN_SECONDS"
    else
        env MORCA_DRAIN_SECONDS="$d" ./build/morca scheduler > build/overlap-nolimit.out 2>&1 && fail "a scheduler started with MORCA_DRAIN_SECONDS='$d'"
    fi
    case $d in
        unset | "") said nolimit "MORCA_DRAIN_SECONDS isn't set: with MORCA_OVERLAP=on" || fail "no message for $d: $(cat build/overlap-nolimit.out)" ;;
        soon) said nolimit "MORCA_DRAIN_SECONDS isn't a number of seconds (soon)" || fail "no message for $d: $(cat build/overlap-nolimit.out)" ;;
        *) said nolimit "MORCA_DRAIN_SECONDS isn't above 0 ($d)" || fail "no message for $d: $(cat build/overlap-nolimit.out)" ;;
    esac
    said nolimit "DRAIN_SECONDS in hetzner.env" || fail "the message doesn't name hetzner.env's setting: $(cat build/overlap-nolimit.out)"
done
[ "$(q "SELECT count(*) FROM pg_tables WHERE schemaname = 'orca'")" = 0 ] \
    || fail "a scheduler refused for its drain limit migrated the database"
echo "ok   an instance refuses to start without its drain limit"

./build/morca register ./build/toy >> "$log" 2>&1 || fail "register"

# --- A leads, and starts two runs ---
scheduler a
until_ 20 "instance a doesn't lead" said a "^morca: scheduler instance 1 leads"
./build/morca backfill toy 2024-03..2024-04 >> "$log" 2>&1 || fail "backfill"
until_ 20 "a didn't start both runs: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE status = 'running' AND instance = 1 AND heartbeat IS NOT NULL" 2
r1=$(q "SELECT min(id) FROM orca.runs")
r2=$(q "SELECT max(id) FROM orca.runs")
echo "ok   instance a leads and starts two runs"

# --- B joins: A drains, B leads and counts A's runs ---
# shellcheck disable=SC2154
scheduler b MORCA_DRAIN_SECONDS=3
until_ 20 "a didn't drain" said a "^morca: scheduler instance 1 drains (instance 2 is newer): 2 runs going"
until_ 20 "b doesn't lead" said b "^morca: scheduler instance 2 leads"
./build/morca backfill toy 2024-05 >> "$log" 2>&1 || fail "backfill"
r3=$(q "SELECT max(id) FROM orca.runs")
sleep 2
is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $r3" "queued 0" \
    || fail "b started a third run beside a's two (max_runs 2): $(runs)"
said b "toy run $r1\|toy run $r2" && fail "b touched a's runs: $(cat build/overlap-b.out)"
said a "started toy run $r3" && fail "a draining started a run"
echo "ok   b leads, a drains, and a's runs count against max_runs"

until_ 40 "a's first run didn't succeed once: $(runs)" done_once "$r1" 1
until_ 40 "a's second run didn't succeed once: $(runs)" done_once "$r2" 1
# shellcheck disable=SC2154
until_ 20 "a didn't exit once drained" sh -c "! kill -0 $pid_a 2> /dev/null"
wait "$pid_a" || fail "a exited $?"
said a "^morca: scheduler instance 1 drained" || fail "a didn't say it drained"
is "SELECT role || ' ' || (ended IS NOT NULL) FROM orca.schedulers WHERE id = 1" "ended true" \
    || fail "a's row: $(q "SELECT * FROM orca.schedulers")"
is "SELECT count(*) FROM orca.audit WHERE action = '/api/scheduler/requeue'" 0 || fail "a run was requeued"
echo "ok   a's runs succeed once each, and a exits once they have"

until_ 20 "b didn't start the third run: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE id = $r3 AND status = 'running' AND instance = 2 AND heartbeat IS NOT NULL" 1
echo "ok   b starts the third run once a's have ended"

# --- C joins, B's drain runs out: its run is killed and queued again ---
scheduler c
until_ 20 "b didn't drain" said b "^morca: scheduler instance 2 drains (instance 3 is newer): 1 runs going"
until_ 20 "c doesn't lead" said c "^morca: scheduler instance 3 leads"
# shellcheck disable=SC2154
until_ 20 "b's drain didn't run out" sh -c "! kill -0 $pid_b 2> /dev/null"
said b "^killed pid [0-9]* of run $r3 (the drain ran out after 3 s)" || fail "b didn't say it killed the run"
until_ 30 "c didn't queue b's run again: $(runs)" \
    is "SELECT count(*) FROM orca.audit WHERE run = $r3 AND action = '/api/scheduler/requeue'" 1
d=$(q "SELECT detail FROM orca.audit WHERE run = $r3 AND action = '/api/scheduler/requeue'")
case "$d" in "queued again: scheduler instance 2 on "*" ended") ;; *) fail "the requeue's audit: $d" ;; esac
until_ 40 "the requeued run didn't succeed on c: $(runs)" \
    is "SELECT status || ' ' || starts || ' ' || instance FROM orca.runs WHERE id = $r3" "succeeded 2 3"
echo "ok   a drain that runs out kills its run, and the leader queues it again"

# --- Registering waits for a handover; status lists the instances ---
./build/morca scheduler status > build/overlap-status.txt || fail "status"
grep -q "^3 .* leading " build/overlap-status.txt || fail "status: $(cat build/overlap-status.txt)"
grep -q "^1 .* ended " build/overlap-status.txt || fail "status: $(cat build/overlap-status.txt)"
echo "ok   status lists the instances"

# --- SIGTERM (docker stop): C kills its run and ends its row at once ---
./build/morca backfill toy 2024-06 >> "$log" 2>&1 || fail "backfill"
r4=$(q "SELECT max(id) FROM orca.runs")
until_ 20 "c didn't start run $r4: $(runs)" \
    is "SELECT count(*) FROM orca.runs WHERE id = $r4 AND status = 'running' AND instance = 3 AND heartbeat IS NOT NULL" 1
kill -TERM "$pid_c"
until_ 10 "c didn't exit on SIGTERM" sh -c "! kill -0 $pid_c 2> /dev/null"
wait "$pid_c" || fail "c exited $? on SIGTERM"
said c "^killed pid [0-9]* of run $r4 (stopped)" || fail "c didn't say it killed its run"
said c "^morca: scheduler instance 3 stopped" || fail "c didn't say it stopped"
is "SELECT role || ' ' || (ended IS NOT NULL) FROM orca.schedulers WHERE id = 3" "ended true" \
    || fail "c's row after SIGTERM: $(q "SELECT * FROM orca.schedulers")"
is "SELECT count(*) FROM orca.schedulers WHERE role = 'leading'" 0 \
    || fail "a stopped instance still counts leading: $(q "SELECT * FROM orca.schedulers")"
echo "ok   SIGTERM ends a leading instance's row and its run (#426)"
q "UPDATE orca.runs SET status = 'failed', ended = now() WHERE id = $r4" > /dev/null
q "UPDATE orca.schedulers SET ended = now() WHERE ended IS NULL" > /dev/null
# A newer instance that doesn't lead yet (made up): register waits, then
# refuses.
q "INSERT INTO orca.schedulers (host, pid, known, last_seen) VALUES ('new', 1, 42, now() + interval '1 hour')" > /dev/null
code=0
MORCA_REGISTER_WAIT_SECONDS=1 ./build/morca register ./build/toy > build/overlap-register.out 2>&1 || code=$?
[ "$code" != 0 ] || fail "register under a handover didn't refuse: $(cat build/overlap-register.out)"
grep -q "a scheduler handover is going on" build/overlap-register.out || fail "register: $(cat build/overlap-register.out)"
code=0
./build/morca scheduler wait-lead --timeout 1 >> "$log" 2>&1 || code=$?
[ "$code" != 0 ] || fail "wait-lead under a handover"
q "DELETE FROM orca.schedulers WHERE host = 'new'" > /dev/null
./build/morca scheduler wait-lead --timeout 5 >> "$log" 2>&1 || fail "wait-lead"
./build/morca register ./build/toy >> "$log" 2>&1 || fail "register with no handover"
echo "ok   register waits for the newest instance to lead"

# --- Floors: can-overlap, and a migration refused beside an older binary ---
out=$(./build/morca scheduler can-overlap) && fail "can-overlap with no instance: $out"
[ "$out" = "no: no live scheduler instance leads (none is going, or it has MORCA_OVERLAP off)" ] || fail "can-overlap: $out"
old=$(q "INSERT INTO orca.schedulers (host, pid, known, role, last_seen) VALUES ('old', 1, 41, 'leading', now() + interval '1 hour') RETURNING id")
out=$(./build/morca scheduler can-overlap) || fail "can-overlap beside an additive one: $out"
[ "$out" = yes ] || fail "can-overlap: $out"
# One that knows fewer than 24, the floor (made up: an older binary at 23
# keeps no row; the check is the same).
q "UPDATE orca.schedulers SET known = 23 WHERE id = $old" > /dev/null
out=$(./build/morca scheduler can-overlap) && fail "can-overlap past the floor said yes"
[ "$out" = "no: this binary's migrations need a binary that knows at least 24, and scheduler instance $old on old knows 23" ] \
    || fail "can-overlap: $out"
echo "ok   can-overlap says yes beside a binary that knows the floor, and no beside one that doesn't"

# The database at 23 migrations, as before a floor-raiser, with that
# instance live: this binary refuses to migrate it.
q "SET client_min_messages = warning; DROP SCHEMA orca CASCADE" > /dev/null
MORCA_TEST_MIGRATIONS=23 ./build/morca user list >> "$log" 2>&1 || true
is "SELECT max(version) FROM orca.schema_version" 23 || fail "the database isn't at 23"
q "CREATE TABLE orca.schedulers (id bigserial PRIMARY KEY, host text NOT NULL, pid int NOT NULL, known int NOT NULL, last_seen timestamptz NOT NULL DEFAULT now(), ended timestamptz)" > /dev/null
q "INSERT INTO orca.schedulers (host, pid, known) VALUES ('old', 1, 23)" > /dev/null
code=0
./build/morca user list > build/overlap-migrate.out 2>&1 || code=$?
[ "$code" != 0 ] || fail "migrated past a live older instance"
grep -q "need a binary that knows at least 24, and scheduler instance 1 on old knows 23 and is still going" build/overlap-migrate.out \
    || fail "the refusal: $(cat build/overlap-migrate.out)"
is "SELECT max(version) FROM orca.schema_version" 23 || fail "it migrated anyway"
q "DROP TABLE orca.schedulers" > /dev/null
./build/morca user list >> "$log" 2>&1 || fail "migrating once the old instance is gone"
echo "ok   a migration that raises the floor is refused while an older instance is live"
