#!/bin/sh
# A long run learns that it's outdated, and morca leaves it alone (#397),
# over tests/outdated_pipeline.mojo (a step that polls morca.outdated()),
# with three scheduler instances on one Postgres (MORCA_OVERLAP=on):
#
# - instance A starts two runs of the binary built at commit aaaa1111...,
#   which aren't outdated;
# - B joins and leads, and the binary built at bbbb2222... is registered:
#   each run's heartbeat sees it. The run that hands over (2024-04) returns
#   and succeeds; the one that ignores it (2024-03) keeps running, beat
#   after beat, is shown outdated ("newer version bbbb2222bbbb is leading")
#   in `morca status`, its JSON and its page, and succeeds once it returns,
#   with one start and one attempt; nothing was killed, cancelled or queued
#   again. Only then does A exit, without MORCA_DRAIN_SECONDS (no limit);
# - C joins with the same code (a deploy of new settings, say): B's run is
#   outdated because its scheduler drains, the same version on both sides.
#
# Needs MORCA_DATABASE_URL (pixi run test-outdated starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
c1=aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111
c2=bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC -D MORCA_COMMIT=$c1 tests/outdated_pipeline.mojo -o build/longpoll-1 &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC -D MORCA_COMMIT=$c2 tests/outdated_pipeline.mojo -o build/longpoll-2 &
b3=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
b2=$!
wait "$b1"
wait "$b2"
wait "$b3"
log=build/outdated.log
: > "$log"
rm -rf build/outdated-*.out build/outdated-stop
mkdir build/outdated-stop
out=$PWD/build/outdated-runs.out
: > "$out"
export MORCA_STALE_SECONDS=2 MORCA_SAMPLE_SECONDS=0 MORCA_WATCHDOG_SECONDS=0
export MORCA_NOW=2024-01-15T00:00Z MORCA_OVERLAP=on
export OUTDATED_OUT=$out OUTDATED_STOP=$PWD/build/outdated-stop OUTDATED_HANDOVER=2024-04
unset MORCA_LOGS MORCA_DRAIN_SECONDS
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    echo "== $out"
    cat "$out"
    for f in build/outdated-*.out; do [ -f "$f" ] && { echo "== $f"; tail -20 "$f"; }; done
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
said() { grep -q "$2" "build/outdated-$1.out"; }
wrote() { grep -q "$1" "$out"; }
runs() { q "SELECT id, status, starts, instance, error FROM orca.runs ORDER BY id"; }
scheduler() {
    name=$1; shift
    env "$@" ./build/morca scheduler > "build/outdated-$name.out" 2>&1 &
    eval "pid_$name=$!"
    pids="$pids $!"
}
# json RUN FIELD: the run's FIELD in the pipeline's JSON (`morca status`).
json() {
    ./build/morca status longpoll --json | jq -r ".runs[] | select(.id == $1) | .$2"
}

./build/morca register ./build/longpoll-1 >> "$log" 2>&1 || fail "register"

# --- A starts two runs at c1: not outdated ---
scheduler a
until_ 20 "instance a doesn't lead" said a "^morca: scheduler instance 1 leads"
./build/morca backfill longpoll 2024-03..2024-04 >> "$log" 2>&1 || fail "backfill"
r1=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-03'")
r2=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-04'")
until_ 20 "the runs didn't start: $(runs)" wrote "^run $r1 started on aaaa1111aaaa$"
until_ 20 "the runs didn't start: $(runs)" wrote "^run $r2 started on aaaa1111aaaa$"
sleep 1
wrote "outdated=True" && fail "a run was outdated before a newer version"
[ "$(json "$r1" code_version)" = aaaa1111aaaa ] || fail "code_version: $(json "$r1" code_version)"
[ "$(json "$r1" outdated)" = null ] || fail "outdated before a newer version: $(json "$r1" outdated)"
echo "ok   a's runs run aaaa1111aaaa, not outdated"

# --- B leads and c2 is registered: both runs see it ---
scheduler b
until_ 20 "a didn't drain" said a "^morca: scheduler instance 1 drains (instance 2 is newer): 2 runs going"
until_ 20 "b doesn't lead" said b "^morca: scheduler instance 2 leads"
./build/morca register ./build/longpoll-2 >> "$log" 2>&1 || fail "register c2"
until_ 20 "run $r2 didn't see it's outdated" wrote "^run $r2 outdated=True code=aaaa1111aaaa "
until_ 20 "run $r2 didn't hand over" wrote "^run $r2 hands over$"
until_ 20 "run $r2 didn't succeed: $(runs)" is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $r2" "succeeded 1"
until_ 20 "run $r1 didn't see it's outdated" wrote "^run $r1 outdated=True code=aaaa1111aaaa "
echo "ok   the heartbeat says a newer version leads; the run that hands over ends"

# The run that ignores it runs on, beat after beat, shown outdated.
until_ 20 "run $r1's JSON: $(./build/morca status longpoll --json)" \
    sh -c "[ \"\$(./build/morca status longpoll --json | jq -r '.runs[] | select(.id == $r1) | .outdated')\" = 'newer version bbbb2222bbbb is leading' ]"
[ "$(json "$r1" code_version)" = aaaa1111aaaa ] || fail "code_version: $(json "$r1" code_version)"
[ "$(json "$r1" desired_code_version)" = bbbb2222bbbb ] || fail "desired_code_version: $(json "$r1" desired_code_version)"
./build/morca status longpoll > build/outdated-status.out 2>&1 || fail "morca status"
grep -q "^  $r1 running .* (outdated: newer version bbbb2222bbbb is leading)" build/outdated-status.out \
    || fail "morca status: $(cat build/outdated-status.out)"
free_port port
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
pids="$pids $!"
base="http://127.0.0.1:$port"
until_ 30 "the UI didn't answer" curl -fs "$base/" -o /dev/null
[ "$(curl -fsS -H 'Accept: application/json' "$base/runs/$r1" | jq -r .run.outdated)" = "newer version bbbb2222bbbb is leading" ] \
    || fail "the run's JSON: $(curl -fsS -H 'Accept: application/json' "$base/runs/$r1")"
curl -fsS "$base/runs/$r1" | grep -q "<strong>outdated</strong> (newer version bbbb2222bbbb is leading)" \
    || fail "the run's page doesn't say it's outdated"
h1=$(q "SELECT heartbeat FROM orca.runs WHERE id = $r1")
sleep 3
is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $r1" "running 1" || fail "run $r1 isn't running on: $(runs)"
is "SELECT heartbeat > '$h1' FROM orca.runs WHERE id = $r1" t || fail "run $r1's heartbeat stopped"
kill -0 "$pid_a" 2> /dev/null || fail "a exited with its run going"
echo "ok   the run that ignores it runs on, shown outdated in status, JSON and its page"

touch "build/outdated-stop/2024-03"
until_ 20 "run $r1 didn't succeed: $(runs)" is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $r1" "succeeded 1"
is "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run IN ($r1, $r2)" 2 \
    || fail "more than one attempt each"
is "SELECT count(*) FROM orca.audit WHERE run IN ($r1, $r2)" 0 || fail "audited: $(q "SELECT * FROM orca.audit")"
# shellcheck disable=SC2154
until_ 20 "a didn't exit once drained" sh -c "! kill -0 $pid_a 2> /dev/null"
said a "^morca: scheduler instance 1 drained" || fail "a didn't say it drained"
said a "^killed pid" && fail "a killed a run"
echo "ok   it succeeds once it returns, one start, nothing killed or requeued; then a exits"

# --- C, the same code: B's run is outdated because B drains ---
./build/morca backfill longpoll 2024-05 >> "$log" 2>&1 || fail "backfill"
r3=$(q "SELECT id FROM orca.runs WHERE partitions = '2024-05'")
until_ 20 "run $r3 didn't start: $(runs)" wrote "^run $r3 started on bbbb2222bbbb$"
scheduler c
until_ 20 "b didn't drain" said b "^morca: scheduler instance 2 drains (instance 3 is newer): 1 runs going"
until_ 20 "run $r3 didn't see its scheduler drain" \
    wrote "^run $r3 outdated=True code=bbbb2222bbbb desired=bbbb2222bbbb why=its scheduler drains: scheduler instance 3 takes over$"
touch "build/outdated-stop/2024-05"
until_ 20 "run $r3 didn't succeed: $(runs)" is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $r3" "succeeded 1"
# shellcheck disable=SC2154
until_ 20 "b didn't exit once drained" sh -c "! kill -0 $pid_b 2> /dev/null"
echo "ok   a run on the same code is outdated while its scheduler drains"
