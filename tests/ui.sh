#!/bin/sh
# orca ui over the toy pipeline's runs: every page answers HTML and JSON, a
# run's JSON says what `toy status <run>` says, values from the database are
# escaped, and unknown paths and methods are refused; attempts' logs, the
# DAG, a running run's page following it to the end, a Dista timeline, and
# a run's machine metrics (orca.metrics): charts, peaks and warnings from
# recorded samples, live while the run goes on, and nothing without them.
# Needs ORCA_DATABASE_URL (pixi run test-ui starts a throwaway server), curl
# and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/daily
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout

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
# The deployment's own nav links: one good, one with markup, one not http(s).
ORCA_UI_LINKS='Census ↗=https://census.example/?a=b, <b>x</b>=http://x.example, Bad=javascript:alert(1)' \
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
nav=$(html /)
echo "$nav" | grep -qF '<a href="https://census.example/?a=b" target="_blank" rel="noopener">Census ↗</a>' \
    || fail "ORCA_UI_LINKS' link isn't in the nav"
echo "$nav" | grep -qF '>&lt;b&gt;x&lt;/b&gt;</a>' || fail "an ORCA_UI_LINKS label isn't escaped"
echo "$nav" | grep -q 'javascript:' && fail "a javascript: link from ORCA_UI_LINKS is in the nav"
# Runs per day on the home page: 30 days, today's with both runs.
json / | jq -e '(.runs_per_day | length) == 30 and .runs_per_day[-1].succeeded == 1 and .runs_per_day[-1].failed == 1' > /dev/null \
    || fail "runs per day: $(json / | jq -c '.runs_per_day[-1]')"
html / | grep -q 'aria-label="Runs per day"' || fail "the home page's chart"

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

# Machine metrics: none recorded, the page is as it was; then a worker's
# (tests/fixtures/metrics-worker.jsonl: memory over 90% for 30 s, steal
# over 20% for 10 s) and this machine's, from the sampler itself.
html "/runs/$ok_run" | grep -q 'Machines' && fail "a run without samples shows machines"
[ "$(json "/runs/$ok_run" | jq '.instances | length')" -eq 0 ] || fail "a run without samples has instances"
m=$ORCA_LOGS/toy/$ok_run/metrics
mkdir -p "$m"
cp tests/fixtures/metrics-worker.jsonl "$m/worker-i-0001.jsonl"
./build/orca sample --out "$m/local.jsonl" --count 2 --seconds 0.2 || fail "the sampler"
page=$(html "/runs/$ok_run")
echo "$page" | grep -q 'aria-label="The run&#39;s machines over time"\|aria-label="The run.s machines over time"' \
    || fail "the run page doesn't chart its machines"
[ "$(echo "$page" | grep -o 'class="mx__name"' | wc -l | tr -d ' ')" -eq 2 ] || fail "not a row per instance"
echo "$page" | grep -q 'class="mx__cpu" d="M' || fail "no CPU in the charts"
echo "$page" | grep -q 'class="mx__mem" d="M' || fail "no memory in the charts"
echo "$page" | grep -q '>days</text>' || fail "the charts have no steps lane"
[ "$(echo "$page" | grep -o 'class="mx__warn"' | wc -l | tr -d ' ')" -eq 2 ] || fail "not 2 warnings on the charts"
echo "$page" | grep -q 'memory over 90%, up to 95%' || fail "the peaks table's warning"
echo "$page" | grep -q '<td><code>worker-i-0001</code></td><td class="num">40</td>' || fail "the peaks table"
json "/runs/$ok_run" | jq -e '[.instances[].id] == ["local", "worker-i-0001"]' > /dev/null \
    || fail "the instances: $(json "/runs/$ok_run" | jq -c '[.instances[].id]')"
json "/runs/$ok_run" | jq -e '.instances[1] | .role == "worker" and .peaks.samples == 40
    and .peaks.mem_pct == 95 and .peaks.cpu == 101 and .peaks.disk_used == 29500000000
    and ([.peaks.warnings[].kind] == ["memory", "steal"])' > /dev/null \
    || fail "the worker's peaks: $(json "/runs/$ok_run" | jq -c '.instances[1].peaks')"
json "/runs/$ok_run" | jq -e '.instances[0].peaks | .samples == 2 and .mem_total > 0' > /dev/null \
    || fail "this machine's peaks: $(json "/runs/$ok_run" | jq -c '.instances[0].peaks')"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$ok_run/metrics")" = 286 ] \
    || fail "an ended run's metrics fragment didn't answer 286"
echo "ok   a run's machines: charts, peaks and warnings, and none without samples"

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
# Its machines too: the fragment draws samples as they come.
html "/runs/$live" | grep -q "data-poll=\"/runs/$live/metrics\"" || fail "a running run's machines don't poll"
mkdir -p "$ORCA_LOGS/toy/$live/metrics"
./build/orca sample --out "$ORCA_LOGS/toy/$live/metrics/local.jsonl" --count 1 --seconds 0.1 || fail "the live sampler"
code=$(curl -s -o "$log.mx" -w '%{http_code}' "$base/runs/$live/metrics")
[ "$code" = 200 ] || fail "a running run's metrics fragment answered $code"
grep -q 'class="mx__now"' "$log.mx" || fail "the live fragment doesn't chart the samples"
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
html "/runs/$fan" | grep -q '<svg class="tl"' || fail "the run page doesn't draw its timeline"
curl -fsS -D - -o /dev/null "$base/runs/$fan/timeline.json" | grep -qi '^access-control-allow-origin: https://ui.perfetto.dev' \
    || fail "Perfetto can't fetch the timeline"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$ok_run/timeline.json")
[ "$code" = 404 ] || fail "a run without tasks answered $code for its timeline"

kill "$pid"
echo "ok   ui"
