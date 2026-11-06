#!/bin/sh
# The calendar on a dated pipeline's page: /pipelines/<name>/calendar says
# each day's (month's) state from the database, the page draws the same as
# coverage and offers the picker only on top of the text field, and a range
# picked on it is a range Start runs queues the right partitions for.
# Needs ORCA_DATABASE_URL (pixi run test-calendar starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/daily
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/calendar.log
: > "$log"
export ORCA_LOGS=build/calendar-logs
rm -rf "$ORCA_LOGS"
# Today is 2024-03-20, so the days after it are outside the pipeline's range.
export ORCA_NOW=2024-03-20T00:00Z
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
ui=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    exit 1
}

./build/orca register ./build/daily >> "$log" 2>&1 || fail "register daily"
./build/orca register ./build/toy >> "$log" 2>&1 || fail "register toy"
hash=$(./build/daily describe | jq -r .registry_hash)
# Done: a real run over three days. Failed, queued, running, and one that
# succeeded as other code (so not done for this one).
./build/daily run --partitions 2024-03-02..2024-03-04 >> "$log" 2>&1 || fail "the daily run"
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status) VALUES
    ('daily', '$hash', '2024-03-06', 'manual', 'failed'),
    ('daily', '$hash', '2024-03-10', 'manual', 'queued'),
    ('daily', 'other', '2024-03-08', 'manual', 'succeeded')" > /dev/null
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, heartbeat, started)
    VALUES ('daily', '$hash', '2024-03-11', 'manual', 'running', now(), now())" > /dev/null
# A monthly pipeline: January done, February failed.
./build/toy run --partitions 2024-01 >> "$log" 2>&1 || fail "the toy run"
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status)
    SELECT 'toy', registry_hash, '2024-02', 'manual', 'failed' FROM orca.pipelines WHERE name = 'toy'" > /dev/null

port=$((22000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
get() { curl -fsS "$base$1" || fail "GET $1"; }
code() { curl -s -o /dev/null -w '%{http_code}' "$base$1"; }
post() {
    path=$1; shift
    set -- "$@" ""
    form=
    for f in "$@"; do [ -z "$f" ] || form="$form --data-urlencode $f"; done
    # shellcheck disable=SC2086
    curl -s -o "$log.body" -w '%{http_code}' -H "Origin: $base" -X POST $form "$base$path"
}
state() { echo "$2" | jq -r --arg k "$1" '.cells[] | select(.key == $k) | .state'; }

# A daily pipeline's month.
c=$(get '/pipelines/daily/calendar?month=2024-03')
echo "$c" | jq -e '.unit == "day" and .period == "2024-03" and (.cells | length) == 31
    and .weekday == 4 and .prev == "2024-02" and .next == null and .start == "2024-01-01" and .today == "2024-03-20"' > /dev/null \
    || fail "the month: $(echo "$c" | jq -c 'del(.cells)')"
want="2024-03-01=open 2024-03-02=done 2024-03-03=done 2024-03-04=done 2024-03-05=open 2024-03-06=failed 2024-03-08=open 2024-03-10=live 2024-03-11=live 2024-03-20=open 2024-03-21=outside 2024-03-31=outside"
for w in $want; do
    [ "$(state "${w%=*}" "$c")" = "${w#*=}" ] || fail "${w%=*} is $(state "${w%=*}" "$c"), not ${w#*=}"
done
echo "$c" | jq -e '.counts == {done: 3, failed: 1, live: 2, open: 14, outside: 11}' > /dev/null \
    || fail "the counts: $(echo "$c" | jq -c .counts)"
curl -fsS -D - -o /dev/null "$base/pipelines/daily/calendar?month=2024-03" | grep -qi '^content-type: application/json' \
    || fail "the calendar isn't JSON"
echo "ok   calendar: a daily month's states"

# Before the first partition: nothing to go back to, and every day outside.
c=$(get '/pipelines/daily/calendar?month=2024-01')
echo "$c" | jq -e '.prev == null and .next == "2024-02"' > /dev/null || fail "January's neighbours"
c=$(get '/pipelines/daily/calendar?month=2023-12')
echo "$c" | jq -e '[.cells[].state] | unique == ["outside"]' > /dev/null || fail "December before the start"
# No month: today's.
get '/pipelines/daily/calendar' | jq -e '.period == "2024-03"' > /dev/null || fail "no month isn't this month"
[ "$(code '/pipelines/daily/calendar?month=2024-13')" = 400 ] || fail "a bad month"
[ "$(code '/pipelines/nope/calendar')" = 404 ] || fail "an unknown pipeline"
echo "ok   calendar: bounds and bad requests"

# A monthly pipeline's year.
c=$(get '/pipelines/toy/calendar?month=2024')
echo "$c" | jq -e '.unit == "month" and .period == "2024" and (.cells | length) == 12 and .prev == null and .next == null' > /dev/null \
    || fail "the year: $(echo "$c" | jq -c 'del(.cells)')"
[ "$(state 2024-01 "$c")" = done ] && [ "$(state 2024-02 "$c")" = failed ] && [ "$(state 2024-03 "$c")" = open ] \
    && [ "$(state 2024-04 "$c")" = outside ] || fail "the months: $(echo "$c" | jq -c '[.cells[].state]')"
echo "ok   calendar: a monthly year's states"

# The page: the text field stays the form; the picker is hidden until
# scripts show it; coverage is plain HTML.
page=$(get /pipelines/daily)
echo "$page" | grep -qF '<input name="partitions" required' || fail "no text field"
echo "$page" | grep -qF '<div class="cal" hidden data-calendar="/pipelines/daily/calendar"></div>' || fail "no hidden picker"
echo "$page" | grep -qF '<li class="cal__day cal--done" data-key="2024-03-02" title="2024-03-02: done">' || fail "coverage lacks 2024-03-02 done"
echo "$page" | grep -qF 'cal--failed" data-key="2024-03-06"' || fail "coverage lacks 2024-03-06 failed"
echo "$page" | grep -qF 'href="/pipelines/daily?cal=2024-02#coverage"' || fail "coverage lacks its previous-month link"
get '/pipelines/daily?cal=2024-02' | grep -qF 'data-key="2024-02-29"' || fail "coverage of February"
get '/pipelines/toy' | grep -qF 'data-key="2024-02" title="2024-02: failed"' || fail "monthly coverage"
get '/pipelines/daily?cal=bad' | grep -qF 'class="refused"' || fail "coverage of a bad month"
echo "ok   calendar: text field fallback and coverage"

# A range as the picker writes it: the preview, then the backfill, queue
# the days that aren't done or live, and no others.
r='2024-03-02..2024-03-07'
get "/pipelines/daily?preview=1&partitions=$r" | grep -qF 'data-cmd="backfill">orca backfill daily 2024-03-02..2024-03-07<' \
    || fail "the command for the range"
p=$(curl -fsS -H 'Accept: application/json' "$base/pipelines/daily?preview=1&partitions=$r")
echo "$p" | jq -e '.preview.keys == ["2024-03-05", "2024-03-06", "2024-03-07"] and .preview.done == ["2024-03-02", "2024-03-03", "2024-03-04"]' > /dev/null \
    || fail "the preview: $(echo "$p" | jq -c .preview)"
[ "$(post /pipelines/daily/backfill "partitions=$r")" = 303 ] || fail "backfill: $(cat "$log.body")"
got=$(q "SELECT string_agg(partitions, ',' ORDER BY partitions) FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'backfill'")
[ "$got" = 2024-03-05,2024-03-06,2024-03-07 ] || fail "the backfill queued $got"
c=$(get '/pipelines/daily/calendar?month=2024-03')
[ "$(state 2024-03-05 "$c")" = live ] && [ "$(state 2024-03-06 "$c")" = live ] && [ "$(state 2024-03-07 "$c")" = live ] \
    || fail "the queued days aren't live"
# A multi-range, as ctrl-click writes it.
[ "$(post /pipelines/daily/backfill 'partitions=2024-03-12,2024-03-14..2024-03-15')" = 303 ] || fail "multi-range backfill"
got=$(q "SELECT string_agg(partitions, ',' ORDER BY partitions) FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'backfill'")
[ "$got" = 2024-03-05,2024-03-06,2024-03-07,2024-03-12,2024-03-14,2024-03-15 ] || fail "the multi-range queued $got"
echo "ok   calendar: a picked range queues the right partitions"

kill "$ui" 2> /dev/null || true
ui=
