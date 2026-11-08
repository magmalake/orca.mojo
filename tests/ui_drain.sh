#!/bin/sh
# Stopping orca ui (orca.web.drain), as `docker stop` does when a deploy
# retires the old UI:
#
# - SIGTERM with a slow request in flight (its query waits on a lock this
#   test holds): new connections are refused at once, the slow request
#   still gets its 200, and orca ui exits 0 soon after, well within
#   ORCA_UI_DRAIN_SECONDS;
# - SIGTERM with nothing in flight: it exits 0 at once;
# - SIGINT with a request that outlasts ORCA_UI_DRAIN_SECONDS: it exits
#   (1, the request cut) within the bound plus half a second.
#
# Needs ORCA_DATABASE_URL (pixi run test-ui-drain starts a throwaway server)
# and curl.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/ui-drain.log
: > "$log"
ui=
locker=
fail() {
    echo "FAIL: $*"
    tail -20 "$log"
    [ -z "$ui" ] || kill -9 "$ui" 2> /dev/null || true
    [ -z "$locker" ] || kill "$locker" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"

# start DRAIN_SECONDS: orca ui on a free port, once it answers.
start() {
    free_port port
    base="http://127.0.0.1:$port"
    ORCA_UI_DRAIN_SECONDS=$1 ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
    ui=$!
    i=0
    until curl -fs "$base/" > /dev/null 2>&1; do
        i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
    done
}
# lock SECONDS: holds orca.runs from another session for that long, once
# the lock is taken, so a page that reads runs waits on it.
lock() {
    psql "$ORCA_DATABASE_URL" -q -c "BEGIN; LOCK TABLE orca.runs IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep($1); COMMIT;" > /dev/null 2>&1 &
    locker=$!
    i=0
    until [ "$(q "SELECT count(*) FROM pg_locks WHERE relation = 'orca.runs'::regclass AND mode = 'AccessExclusiveLock' AND granted")" = 1 ]; do
        i=$((i + 1)); [ "$i" -lt 100 ] || fail "the lock wasn't taken"; sleep 0.05
    done
}
# slow: GET /runs in the background, its status in build/ui-drain-slow.txt,
# once orca ui's query waits on the lock.
slow() {
    rm -f build/ui-drain-slow.txt
    curl -s -o /dev/null -w '%{http_code}' --max-time 30 "$base/runs" > build/ui-drain-slow.txt 2>&1 &
    slow_curl=$!
    i=0
    until [ "$(q "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'")" -ge 1 ]; do
        i=$((i + 1)); [ "$i" -lt 100 ] || fail "the slow request never waited"; sleep 0.05
    done
}
# stopped TENTHS: waits up to that many tenths of a second for orca ui to
# exit; sets $took (tenths) and $code.
stopped() {
    took=0
    while kill -0 "$ui" 2> /dev/null; do
        took=$((took + 1))
        [ "$took" -le "$1" ] || fail "orca ui still running after $took tenths of a second"
        sleep 0.1
    done
    code=0
    wait "$ui" || code=$?
    ui=
}

# A slow request in flight.
start 5
lock 2
slow
kill -TERM "$ui"
sleep 0.3
new=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$base/" 2> /dev/null || true)
[ "$new" = 000 ] || fail "a new connection after SIGTERM got $new"
stopped 45
[ "$code" = 0 ] || fail "exited $code after draining"
wait "$slow_curl" || true
[ "$(cat build/ui-drain-slow.txt)" = 200 ] || fail "the request in flight got $(cat build/ui-drain-slow.txt)"
wait "$locker" || true
locker=
grep -q "orca ui: stopping: no new connections; waiting up to 5 s for 1 request(s) in flight" "$log" \
    || fail "no drain message"
grep -q "orca ui: stopped" "$log" || fail "no stopped message"

# Nothing in flight: at once.
start 5
curl -fs "$base/runs" > /dev/null || fail "GET /runs"
kill -TERM "$ui"
stopped 10
[ "$code" = 0 ] || fail "exited $code with nothing in flight"

# A request that outlasts the drain: the bound holds.
start 1
lock 6
slow
kill -INT "$ui"
stopped 25
[ "$code" = 1 ] || fail "exited $code with a request cut"
grep -q "a request is still running: exiting without it" "$log" || fail "no message for the cut request"
kill "$locker" 2> /dev/null || true
wait "$locker" 2> /dev/null || true
locker=
wait "$slow_curl" 2> /dev/null || true
echo "ui drain: ok"
