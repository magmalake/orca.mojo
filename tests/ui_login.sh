#!/bin/sh
# Login to orca ui's actions (orca.auth), over HTTP only, as tests/actions.sh
# is. With no user and no ORCA_UI_LOGIN, actions are open, as before. Once a
# user exists:
#
# - an action without a session gets 401 (JSON) or the login page (HTML),
#   and changes nothing; pages still read without one;
# - a wrong password gets 401 and no cookie; the right one a session cookie
#   that's HttpOnly and SameSite=Strict, and a `next` off the site is
#   ignored;
# - with the cookie the action is done and recorded as the user's, which the
#   run's page shows;
# - logging out, an expired session, a removed user and a changed password
#   all end the session; a login posted from another site is refused.
#
# Needs ORCA_DATABASE_URL (pixi run test-ui-login starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/ui-login.log
: > "$log"
ui=
fail() {
    echo "FAIL: $*"
    tail -20 "$log"
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
port=$((24000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
jar=build/ui-login-cookies.txt
rm -f "$jar"
# post PATH BODY [curl args]: the status, as the UI's own form would send it.
post() {
    p=$1; b=$2; shift 2
    curl -s -o build/ui-login-body.txt -w '%{http_code}' -H "Origin: $base" -d "$b" "$@" "$base$p"
}
runs() { q "SELECT count(*) FROM orca.runs"; }

# No user: open, as before.
[ "$(post /pipelines/toy/runs partitions=2024-01)" = 303 ] || fail "an action with no users"
[ "$(runs)" -eq 1 ] || fail "the open action made no run"

# A user: actions need a session.
printf 'correct horse battery\n' | ./build/orca user add alice >> "$log" 2>&1 || fail "user add"
[ "$(./build/orca user list)" = alice ] || fail "user list"
[ "$(q "SELECT password_hash LIKE '\$2a\$12\$%' FROM orca.users")" = t ] || fail "not a bcrypt hash"
[ "$(post /pipelines/toy/runs partitions=2024-02 -H 'Accept: application/json')" = 401 ] || fail "JSON without a session"
[ "$(post /pipelines/toy/runs partitions=2024-02 -H "Referer: $base/pipelines/toy")" = 303 ] || fail "HTML without a session"
loc=$(curl -s -o /dev/null -w '%{redirect_url}' -H "Origin: $base" -H "Referer: $base/pipelines/toy" -d partitions=2024-02 "$base/pipelines/toy/runs")
[ "$loc" = "$base/login?next=%2Fpipelines%2Ftoy" ] || fail "sent to $loc"
[ "$(runs)" -eq 1 ] || fail "an action without a session made a run"
curl -fs "$base/runs" > /dev/null || fail "reading needs no session"
curl -fs "$base/" | grep -q 'href="/login">Log in' || fail "no way to log in"

# Logging in.
[ "$(post /login 'name=alice&password=wrong+password&next=/runs' -c "$jar")" = 401 ] || fail "a wrong password"
! grep -q orca_session "$jar" 2> /dev/null || fail "a cookie for a wrong password"
[ "$(post /login 'name=nobody&password=correct+horse+battery' -c "$jar")" = 401 ] || fail "no such user"
hdrs=$(curl -s -D - -o /dev/null -H "Origin: $base" -c "$jar" \
    -d 'name=alice&password=correct+horse+battery&next=//evil.example/x' "$base/login")
# Header lines end in CRLF, which grep's `\r` doesn't match the same way on
# every platform, so the CRs go first.
hdrs=$(echo "$hdrs" | tr -d '\r')
echo "$hdrs" | grep -qix 'location: /' || fail "followed an off-site next: $hdrs"
echo "$hdrs" | grep -i '^set-cookie: orca_session=' | grep -i 'httponly' | grep -qi 'samesite=strict' \
    || fail "the cookie: $hdrs"
[ "$(post /pipelines/toy/runs partitions=2024-02 -b "$jar")" = 303 ] || fail "an action with a session"
run=$(q "SELECT max(id) FROM orca.runs")
[ "$(q "SELECT username || ' ' || action || ' ' || run FROM orca.audit")" = "alice /pipelines/toy/runs $run" ] \
    || fail "the audit: $(q "SELECT * FROM orca.audit")"
curl -fs -b "$jar" "$base/runs/$run" | grep -q '<td>alice</td>' || fail "the run's page doesn't say who"
curl -fs -b "$jar" "$base/" | grep -q 'class="top__user">alice' || fail "the header doesn't say who"
[ "$(post "/runs/$run/cancel" '' -b "$jar")" = 303 ] || fail "cancel with a session"
[ "$(q "SELECT count(*) FROM orca.audit WHERE run = $run")" -eq 2 ] || fail "the cancel's audit"

# Ending sessions.
[ "$(post /login 'name=alice&password=correct+horse+battery' -c "$jar")" = 303 ] || fail "log in again"
[ "$(post /logout '' -b "$jar" -c "$jar")" = 303 ] || fail "log out"
[ "$(post /pipelines/toy/runs partitions=2024-03 -b "$jar" -H 'Accept: application/json')" = 401 ] || fail "after logout"
[ "$(post /login 'name=alice&password=correct+horse+battery' -c "$jar")" = 303 ] || fail "log in"
q "UPDATE orca.sessions SET expires = now() - interval '1 second'" > /dev/null
[ "$(post /pipelines/toy/runs partitions=2024-03 -b "$jar" -H 'Accept: application/json')" = 401 ] || fail "an expired session"
[ "$(post /login 'name=alice&password=correct+horse+battery' -c "$jar")" = 303 ] || fail "log in"
printf 'a new long password\n' | ./build/orca user add alice >> "$log" 2>&1
[ "$(post /pipelines/toy/runs partitions=2024-03 -b "$jar" -H 'Accept: application/json')" = 401 ] || fail "after a new password"
[ "$(post /login 'name=alice&password=a+new+long+password' -c "$jar")" = 303 ] || fail "the new password"
./build/orca user remove alice >> "$log" 2>&1
[ "$(q "SELECT count(*) FROM orca.sessions")" -eq 0 ] || fail "sessions of a removed user"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Origin: http://evil.example" \
    -d 'name=alice&password=x' "$base/login")
[ "$code" = 403 ] || fail "a login from another site answered $code"
printf 'short\n' | ./build/orca user add bob > /dev/null 2>&1 && fail "a short password was taken"

# ORCA_UI_LOGIN=required: no user yet, and still no action.
kill "$ui"
ORCA_UI_LOGIN=required ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start again"; sleep 0.1
done
[ "$(post /pipelines/toy/runs partitions=2024-03 -H 'Accept: application/json')" = 401 ] \
    || fail "ORCA_UI_LOGIN=required with no users"
kill "$ui"
echo "ok   ui login"
