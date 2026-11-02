#!/bin/sh
# orca ui over the toy pipeline's runs: every page answers HTML and JSON, a
# run's JSON says what `toy status <run>` says, values from the database are
# escaped, and unknown paths and methods are refused; attempts' logs, the
# DAG, a running run's page following it to the end, and a Dista timeline.
# Needs ORCA_DATABASE_URL (pixi run test-ui starts a throwaway server), curl
# and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/daily.mojo -o build/daily
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/fanout.mojo -o build/fanout

log=build/ui.log
: > "$log"
export ORCA_LOGS=build/ui-logs
rm -rf "$ORCA_LOGS"
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    exit 1
}

# Runs to show: one that succeeds over two months, one whose 2024-02 fails.
ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/toy run --partitions 2024-01..2024-02 >> "$log" 2>&1 || fail "the first run"
ORCA_TOY_FAIL=2024-02 ./build/toy run --partitions 2024-01..2024-02 >> "$log" 2>&1 && fail "the second run didn't fail"
ok_run=$(q "SELECT min(id) FROM orca.runs")
bad_run=$(q "SELECT max(id) FROM orca.runs")
# A value with markup in it, as an error message could have.
q "UPDATE orca.steps SET error = '<script>alert(1)</script> & \"so\"' WHERE run = $bad_run AND status = 'failed'" > /dev/null

port=$((18000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pid=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done

html() { curl -fsS "$base$1" || fail "GET $1"; }
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1 as JSON"; }

# Every page as HTML and as JSON.
for path in / /pipelines/toy /runs "/runs?pipeline=toy&status=failed" "/runs/$ok_run" "/runs/$bad_run"; do
    html "$path" | grep -q '<!doctype html>' || fail "$path isn't HTML"
    json "$path" | jq -e . > /dev/null || fail "$path isn't JSON"
done
[ "$(curl -fsS "$base/runs/$ok_run?format=json" | jq -r .run.status)" = succeeded ] \
    || fail "?format=json"

# What the pages say.
[ "$(json / | jq -r '.pipelines[0].name')" = toy ] || fail "/ doesn't list toy"
[ "$(json / | jq -r '.pipelines[0].next_fire.partition')" != null ] || fail "/ has no next fire"
[ "$(json /pipelines/toy | jq -r '[.steps[].name] | join(",")')" = days,measure,total ] \
    || fail "the pipeline's steps"
[ "$(json /pipelines/toy | jq '.fires | length')" -eq 5 ] || fail "not 5 next fires"
[ "$(json /pipelines/toy | jq -r '.steps[1].after')" = days ] || fail "measure isn't after days"
[ "$(json '/runs?status=failed' | jq -r '[.runs[].id] | join(",")')" = "$bad_run" ] \
    || fail "/runs?status=failed"
[ "$(json /runs | jq '.runs | length')" -eq 2 ] || fail "/runs doesn't have 2 runs"

# A run's JSON says what `toy status <run>` says, line for line.
for run in "$ok_run" "$bad_run"; do
    want=$(./build/toy status "$run")
    got=$(json "/runs/$run" | jq -r '
        "run \(.run.id) \(.run.pipeline) \(.run.status) \(.run.partitions)",
        (.steps[] | "\(.step)\(if .partition != "" then " " + .partition else "" end) \(.status)\(if .attempts > 1 then " after \(.attempts) attempts" else "" end)\(if .error != "" then ": " + .error else "" end)\(if .log != "" then " [\(.log)]" else "" end)")')
    [ "$got" = "$want" ] || { printf 'status:\n%s\nui:\n%s\n' "$want" "$got"; fail "run $run's JSON differs from status"; }
done

# Escaped in HTML, as it is in JSON.
page=$(html "/runs/$bad_run")
echo "$page" | grep -q '<script>alert' && fail "a value's markup reached the page"
echo "$page" | grep -q '&lt;script&gt;alert(1)&lt;/script&gt; &amp; &quot;so&quot;' \
    || fail "the error isn't on the page, escaped"
[ "$(json "/runs/$bad_run" | jq -r '[.steps[] | select(.status == "failed")][0].error')" \
    = '<script>alert(1)</script> & "so"' ] || fail "the JSON error"

# Unknown paths, runs and pipelines; a method the UI doesn't take.
for path in /nope /runs/999999 /runs/x /pipelines/nope; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$base$path")
    [ "$code" = 404 ] || fail "$path answered $code, not 404"
done
[ "$(curl -s -H 'Accept: application/json' "$base/runs/999999" | jq -r .error)" = "no run 999999" ] \
    || fail "a 404's JSON"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/runs")
[ "$code" = 405 ] || fail "POST answered $code, not 405"
curl -fsS "$base/static/orca.css" | grep -q -- '--ember' || fail "the stylesheet"

# An attempt's page: its error, its log, the step's other attempts.
failed=$(json "/runs/$bad_run" | jq -r '[.steps[] | select(.status == "failed")][0].attempt_id')
[ "$failed" -gt 0 ] || fail "no attempt id for the failed step"
page=$(html "/runs/$bad_run/attempts/$failed")
echo "$page" | grep -q '&lt;script&gt;' && fail "the attempt page has the step's error, not the attempt's"
echo "$page" | grep -q 'failing 2024-02 as asked' || fail "the attempt's error isn't on its page"
[ "$(json "/runs/$bad_run/attempts/$failed" | jq -r .attempt.status)" = failed ] || fail "the attempt's JSON"
json "/runs/$bad_run/attempts/$failed" | jq -r .text | grep -q 'failed: failing 2024-02 as asked' \
    || fail "the attempt's log text"
echo "$page" | grep -q 'data-poll="' && fail "an ended attempt's page still polls"
html "/runs/$bad_run" | grep -q "/runs/$bad_run/attempts/$failed" || fail "the run doesn't link the attempt"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$ok_run/attempts/$failed")
[ "$code" = 404 ] || fail "another run's attempt answered $code"

# The DAG: a box per step, an edge per after, coloured by the run.
[ "$(html /pipelines/toy | grep -o 'class="dag__box' | wc -l)" -eq 3 ] || fail "toy's DAG hasn't 3 boxes"
[ "$(html /pipelines/toy | grep -o 'class="dag__edge' | wc -l)" -eq 2 ] || fail "toy's DAG hasn't 2 edges"
ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/daily >> "$log" 2>&1 || fail "register daily"
[ "$(html /pipelines/daily | grep -o 'class="dag__box' | wc -l)" -eq 2 ] || fail "daily's DAG hasn't 2 boxes"
[ "$(html /pipelines/daily | grep -o 'class="dag__edge' | wc -l)" -eq 1 ] || fail "daily's DAG hasn't 1 edge"
page=$(html "/runs/$bad_run")
echo "$page" | grep -q 'dag__box dag__box--failed"><title>measure' || fail "measure isn't failed in the DAG"
echo "$page" | grep -q 'dag__box dag__box--skipped"><title>total' || fail "total isn't skipped in the DAG"
echo "$page" | grep -q 'data-poll="' && fail "an ended run's page still polls"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$bad_run/steps")
[ "$code" = 286 ] || fail "an ended run's fragment answered $code, not 286"

# A run going on: its page polls, its fragment and its running attempt's log
# answer 200 until they end, then 286 with the end in them.
ORCA_TOY_SLEEP=1 ./build/toy run --partitions 2024-03 >> "$log" 2>&1 &
toy=$!
i=0
until [ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")" -eq 1 ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the slow run didn't start"; sleep 0.05
done
live=$(q "SELECT id FROM orca.runs WHERE status = 'running'")
html "/runs/$live" | grep -q "data-poll=\"/runs/$live/steps\"" || fail "a running run's page doesn't poll"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$live/steps")
[ "$code" = 200 ] || fail "a running run's fragment answered $code"
i=0
until a=$(q "SELECT a.id FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $live AND a.status = 'running' LIMIT 1") && [ -n "$a" ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "no running attempt"; sleep 0.05
done
html "/runs/$live/attempts/$a" | grep -q "data-poll=\"/runs/$live/attempts/$a/log\"" \
    || fail "a running attempt's page doesn't poll"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$live/attempts/$a/log")
[ "$code" = 200 ] || fail "a running attempt's log answered $code"
wait "$toy" || fail "the slow run failed"
code=$(curl -s -o "$log.frag" -w '%{http_code}' "$base/runs/$live/steps")
[ "$code" = 286 ] || fail "the ended run's fragment answered $code"
grep -q 'status--succeeded">succeeded</span></h2>' "$log.frag" || fail "the fragment doesn't show the run succeeded"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$live/attempts/$a/log")" = 286 ] \
    || fail "the ended attempt's log didn't answer 286"

# A run with Dista tasks: its timeline, as Chrome's trace format.
./build/fanout run --partitions 2024-01 >> "$log" 2>&1 || fail "fanout failed"
fan=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'fanout'")
html "/runs/$fan" | grep -q "/runs/$fan/timeline.json" || fail "the run doesn't link its timeline"
curl -fsS "$base/runs/$fan/timeline.json" | grep -q '"ph":"X"' || fail "the timeline isn't a Chrome trace"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$ok_run/timeline.json")
[ "$code" = 404 ] || fail "a run without tasks answered $code for its timeline"

kill "$pid"
echo "ok   ui"
