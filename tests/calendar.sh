#!/bin/sh
# The calendar on a dated pipeline's page: /pipelines/<name>/calendar says
# each day's (month's) state from the database, the page draws the same as
# coverage and offers the picker only on top of the text field, and a range
# picked on it is a range Start runs queues the right partitions for.
# Needs ORCA_DATABASE_URL (pixi run test-calendar starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
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
# A keyed pipeline (its binary a script), for its coverage; its `keys`
# notes each call in $FAKE_KEYS.calls.
export FAKE_KEYS="$PWD/build/calendar-keys"
rm -f "$FAKE_KEYS".*
./build/orca register tests/fake_keyed >> "$log" 2>&1 || fail "register fake_keyed"
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

free_port port
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
page=$(get '/pipelines/daily?tab=operate')
echo "$page" | grep -qF '<input name="partitions" required' || fail "no text field"
echo "$page" | grep -qF '<div class="cal" hidden data-calendar="/pipelines/daily/calendar"></div>' || fail "no hidden picker"
echo "$page" | grep -qF '<li class="cal__day cal--done" data-key="2024-03-02" title="2024-03-02: done">' || fail "coverage lacks 2024-03-02 done"
echo "$page" | grep -qF 'cal--failed" data-key="2024-03-06"' || fail "coverage lacks 2024-03-06 failed"
echo "$page" | grep -qF 'href="/pipelines/daily?cal=2024-02&amp;cov=month#coverage"' || fail "coverage lacks its previous-month link"
get '/pipelines/daily?cal=2024-02' | grep -qF 'data-key="2024-02-29"' || fail "coverage of February"
get '/pipelines/toy?tab=operate' | grep -qF 'data-key="2024-02" title="2024-02: failed"' || fail "monthly coverage"
get '/pipelines/daily?cal=bad' | grep -qF 'class="refused"' || fail "coverage of a bad month"
echo "ok   calendar: text field fallback and coverage"

# Coverage's Month and Year tabs: a daily pipeline opens on Year, both are
# in the page, and ?cov= picks one without scripts.
echo "$page" | grep -qF 'data-view="year" href="/pipelines/daily?cal=2024&amp;cov=year#coverage" aria-selected="true"' \
    || fail "Year isn't the default tab"
echo "$page" | grep -qF 'data-view="month" href="/pipelines/daily?cal=2024-03&amp;cov=month#coverage" aria-selected="false"' \
    || fail "no Month tab for this month"
echo "$page" | grep -qF '<div id="coverage-month" role="tabpanel" class="cov__body" hidden>' || fail "the month isn't hidden"
echo "$page" | grep -qF '<div id="coverage-year" role="tabpanel" class="cov__body">' || fail "the year is hidden"
m=$(get '/pipelines/daily?cov=month')
echo "$m" | grep -qF '<div id="coverage-month" role="tabpanel" class="cov__body">' || fail "?cov=month hides the month"
echo "$m" | grep -qF '<div id="coverage-year" role="tabpanel" class="cov__body" hidden>' || fail "?cov=month shows the year"
echo "$m" | grep -qF 'id="start"' || fail "?cov= doesn't open Operate"
# The year: twelve months of cells, each with its share done (of its days
# from the start to today) and its failures, its name linking its month.
[ "$(echo "$page" | grep -o 'class="cov__month"' | wc -l | tr -d ' ')" = 12 ] || fail "not twelve months"
echo "$page" | grep -qF 'href="/pipelines/daily?cal=2024-03&amp;cov=month#coverage">March</a><span class="cov__sum">3/20 done, 1 failed</span>' \
    || fail "March's summary: $(echo "$page" | grep -o 'March</a><span class="cov__sum">[^<]*')"
echo "$page" | grep -qF 'January</a><span class="cov__sum">0/31 done</span>' || fail "January's summary"
echo "$page" | grep -qF 'April</a><span class="cov__sum">not in range</span>' || fail "April's summary"
echo "$page" | grep -qF 'aria-label="Days of February 2024"' || fail "no February grid"
echo "$page" | grep -qF '<li class="cal__day cal--failed" data-key="2024-03-06" title="2024-03-06: failed"><span class="sr">' \
    || fail "the year's cells lack their tooltip"
[ "$(echo "$page" | grep -o 'data-key="2024-' | wc -l | tr -d ' ')" = $((366 + 31)) ] || fail "not the year's days and the month's"
echo "$page" | grep -qF '<span class="cal__key cal--done"></span> 3 done' || fail "the year's legend"
# The strip: each month of the range, linking its year.
echo "$page" | grep -qF '<a class="cov__bar cov--0 cov--here" href="/pipelines/daily?cal=2024&amp;cov=year#coverage" title="2024-03: 3/20 done (15%)">' \
    || fail "the strip lacks March"
[ "$(echo "$page" | grep -o 'class="cov__bar' | wc -l | tr -d ' ')" = 3 ] || fail "the strip isn't three months"
# A year before the start: all outside, its next year linked, none before.
y=$(get '/pipelines/daily?cal=2023&cov=year')
echo "$y" | grep -qF 'href="/pipelines/daily?cal=2024&amp;cov=year#coverage" aria-label="Next year"' || fail "2023's next year"
echo "$y" | grep -qF 'aria-label="Previous year"' && fail "a year before 2023"
echo "$y" | grep -qF 'December</a><span class="cov__sum">not in range</span>' || fail "2023 isn't outside"
echo "$y" | grep -qF 'data-view="month" href="/pipelines/daily?cal=2023-12&amp;cov=month' || fail "a past year's month isn't December"
# A monthly pipeline keeps its year of months, with no tabs.
t=$(get '/pipelines/toy?tab=operate')
echo "$t" | grep -qF 'id="coverage"' && echo "$t" | grep -qF 'data-view="year"' && fail "a monthly pipeline has tabs"
echo "ok   calendar: coverage's month and year"

# Coverage in one number (orca.coverage). daily's range so far is
# 2024-01-01..2024-03-18 (today is 03-19 in New York, still going): 78
# days, 3 done; failed, queued, running and other code's aren't. toy's is
# 2024-01..2024-02: January done, February failed.
j=$(curl -fsS -H 'Accept: application/json' "$base/api/pipelines/daily")
echo "$j" | jq -e '.pipeline.coverage == {done: 3, total: 78, pct: 3.8, unit: "day"}' > /dev/null \
    || fail "daily's coverage: $(echo "$j" | jq -c .pipeline.coverage)"
p=$(get '/pipelines/daily/progress')
echo "$p" | grep -qF '<a href="/pipelines/daily?tab=operate&amp;cov=year#coverage">3 / 78 days done (3%)</a>' \
    || fail "the Progress card's Coverage: $(echo "$p" | grep -o 'Coverage<.*' | head -c 600)"
echo "$p" | grep -qF 'aria-label="About Coverage"' || fail "Coverage on the Progress card has no help"
get '/pipelines/toy/progress' | grep -qF '<a href="/pipelines/toy?tab=operate#coverage">1 / 2 months done (50%)</a>' \
    || fail "toy's Coverage"
# A keyed pipeline whose keys haven't been read has none; once they are,
# its keys done of them: ann done, bob failed, cy never run.
h=$(get '/home/pipelines')
echo "$h" | grep -qF '<td class="num muted">—</td>' || fail "fake_keyed's coverage isn't a dash"
# (The list never runs `keys`; the pipeline's own page would.)
[ "$(curl -fsS -H 'Accept: application/json' "$base/" | jq -c '.pipelines[] | select(.name == "fake_keyed") | .coverage')" = null ] \
    || fail "fake_keyed has coverage before its keys"
[ "$(q "SELECT count(*) FROM orca.keys")" = 0 ] || fail "the list read fake_keyed's keys"
q "INSERT INTO orca.keys (pipeline, snapshot_id, keys) VALUES ('fake_keyed', 7, E'ann\\nbob\\ncy')" > /dev/null
k=$(q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status)
    VALUES ('fake_keyed', '0x4e75', 'ann,bob', 'manual', 'failed') RETURNING id" | head -1)
q "INSERT INTO orca.steps (run, step, partition, ord, status) VALUES
    ($k, 'label', 'ann', 0, 'succeeded'), ($k, 'label', 'bob', 0, 'failed')" > /dev/null
j=$(curl -fsS -H 'Accept: application/json' "$base/api/pipelines/fake_keyed")
echo "$j" | jq -e '.pipeline.coverage == {done: 1, total: 3, pct: 33.3, unit: "key"} and .keys.total == 3' > /dev/null \
    || fail "fake_keyed's coverage: $(echo "$j" | jq -c '[.pipeline.coverage, .keys]')"
get '/pipelines/fake_keyed/progress' | grep -qF '<a href="/pipelines/fake_keyed#keys">1 / 3 keys done (33%)</a>' \
    || fail "fake_keyed's Progress card"
get '/pipelines/fake_keyed' | grep -qF '<h2 id="keys">Keys' || fail "no #keys to link to"
# The home page: a Coverage column with its help, each pipeline's percent
# linking where its Progress card does, the counts on hover; the same in
# its JSON.
h=$(get '/')
echo "$h" | grep -qF 'aria-label="About Coverage"' || fail "the Coverage column has no help"
echo "$h" | grep -qF '<td class="num"><a class="covpct" href="/pipelines/daily?tab=operate&amp;cov=year#coverage" title="3 / 78 days done (3%)">3%' \
    || fail "daily's row: $(echo "$h" | grep -o '<a class="covpct"[^>]*>[^<]*' | tr '\n' ' ')"
echo "$h" | grep -qF 'href="/pipelines/toy?tab=operate#coverage" title="1 / 2 months done (50%)">50%' || fail "toy's row"
# Coverage is the first cell after the name, then Running and Queued.
get '/home/pipelines' | grep -qE '</td><td class="num"><a class="covpct" href="/pipelines/daily\?[^"]*"[^>]*>3%.*</a></td><td><span class="badge[^"]*">[0-9]+ / [0-9]+</span></td><td><span class="badge[^"]*">[0-9]+</span></td><td>[a-z]+ from' \
    || fail "daily's row isn't name, Coverage, Running, Queued, Partitions"
echo "$h" | grep -qF 'href="/pipelines/fake_keyed#keys" title="1 / 3 keys done (33%)">33%' || fail "fake_keyed's row"
j=$(curl -fsS -H 'Accept: application/json' "$base/")
echo "$j" | jq -e '[.pipelines[] | {(.name): .coverage.pct}] | add == {daily: 3.8, fake_keyed: 33.3, toy: 50}' > /dev/null \
    || fail "the list's coverage: $(echo "$j" | jq -c '[.pipelines[] | [.name, .coverage]]')"
echo "ok   calendar: coverage in one number"

# A range as the picker writes it: the preview, then the backfill, queue
# the days that aren't done or live, and no others.
r='2024-03-02..2024-03-07'
get "/pipelines/daily?preview=1&partitions=$r" | grep -qF 'data-cmd="backfill">orca login &lt;url&gt; &amp;&amp; orca --remote &lt;url&gt; backfill daily 2024-03-02..2024-03-07<' \
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

# A sensor that follows another pipeline's dataset: how far behind it is.
# daily becomes a sensor of up.days, which toy committed for 2024-03-01..08
# (a step a day), and a snapshot it sensed itself started a run of
# 2024-03-18 that failed. The backfill above queued 03-05..07.
j=$(curl -fsS -H 'Accept: application/json' "$base/api/pipelines/daily")
echo "$j" | jq -e '.follow == null' > /dev/null || fail "a pipeline that isn't a sensor follows: $(echo "$j" | jq -c .follow)"
q "UPDATE orca.pipelines SET describe = jsonb_set(jsonb_set(describe, '{sensor}', 'true'),
    '{steps,0,reads}', '[\"up.days\"]') WHERE name = 'daily'" > /dev/null
up=$(q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status)
    SELECT 'toy', registry_hash, '2024-03-01..2024-03-08', 'manual', 'succeeded' FROM orca.pipelines WHERE name = 'toy' RETURNING id" | head -1)
q "WITH s AS (INSERT INTO orca.steps (run, step, partition, ord, status)
      SELECT $up, 'write', to_char(d, 'YYYY-MM-DD'), 0, 'succeeded'
      FROM generate_series('2024-03-01'::date, '2024-03-08'::date, '1 day') d RETURNING id, partition),
    a AS (INSERT INTO orca.attempts (step, attempt, status, host, registry_hash)
      SELECT id, 1, 'succeeded', 'h', 'x' FROM s RETURNING id)
    INSERT INTO orca.commits (attempt, table_name, snapshot_id, dataset)
      SELECT id, 'up.days', id, 'up.days' FROM a" > /dev/null
r18=$(q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status)
    VALUES ('daily', '$hash', '2024-03-18', 'sensor', 'failed') RETURNING id" | head -1)
q "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id, run) VALUES ('daily', 'up.days', 999, $r18)" > /dev/null
j=$(curl -fsS -H 'Accept: application/json' "$base/api/pipelines/daily")
echo "$j" | jq -e '.follow | .datasets == ["up.days"] and .upstream == ["toy"] and .unit == "day"
    and .available == 9 and .done == 3 and .behind == 6
    and .live == {count: 3, first: ["2024-03-05", "2024-03-06", "2024-03-07"]}
    and .failed == {count: 1, first: ["2024-03-18"]}
    and .waiting == {count: 2, first: ["2024-03-01", "2024-03-08"]}' > /dev/null \
    || fail "what daily follows: $(echo "$j" | jq -c .follow)"
p=$(get '/pipelines/daily/progress')
echo "$p" | grep -qF 'toy (up.days): 3 of 9 days done; <span class="warn">6 behind</span>: 3 running or queued, 1 failed and 2 not queued yet' \
    || fail "the Progress card's Following: $(echo "$p" | grep -o 'toy (up.days)[^<]*')"
echo "$p" | grep -qF 'Not queued: <a href="/pipelines/daily?cal=2024-03&amp;cov=month#coverage">2024-03-01</a>, <a' \
    || fail "the days not queued aren't linked"
page=$(get '/pipelines/daily?tab=operate')
echo "$page" | grep -qF '<li class="cal__day cal--behind" data-key="2024-03-01" title="2024-03-01: behind what it follows">' \
    || fail "2024-03-01 isn't behind"
echo "$page" | grep -qF 'cal--behind" data-key="2024-03-08"' || fail "2024-03-08 isn't behind"
echo "$page" | grep -qF 'cal--open" data-key="2024-03-09"' || fail "2024-03-09 isn't open"
echo "$page" | grep -qF '<span class="cal__key cal--behind"></span> 2 behind what it follows' || fail "no behind in the legend"
echo "$page" | grep -qF 'Behind: up.days has it' || fail "no help for behind"
get '/pipelines/toy?tab=operate' | grep -qF 'cal--behind' && fail "toy follows nothing"
# Its Queue them button backfills just those two, as ranges of consecutive days.
echo "$p" | grep -qF 'action="/pipelines/daily/backfill" data-confirm="Queue the days not queued yet?" data-confirm-body="This queues 2 runs, one per day: 2024-03-01 … 2024-03-08. ' \
    || fail "no Queue them: $(echo "$p" | grep -o '<span class="follow__queue">.*')"
echo "$p" | grep -qF '<input type="hidden" name="partitions" value="2024-03-01,2024-03-08">' || fail "Queue them's partitions"
[ "$(post /pipelines/daily/backfill partitions=2024-03-01,2024-03-08)" = 303 ] || fail "Queue them: $(cat "$log.body")"
j=$(curl -fsS -H 'Accept: application/json' "$base/api/pipelines/daily")
echo "$j" | jq -e '.follow | .waiting.count == 0 and .live.count == 5' > /dev/null \
    || fail "Queue them didn't queue them: $(echo "$j" | jq -c .follow)"
get '/pipelines/daily/progress' | grep -qF 'follow__queue' && fail "Queue them with nothing left to queue"
echo "ok   calendar: how far a sensor is behind what it follows"

kill "$ui" 2> /dev/null || true
ui=
