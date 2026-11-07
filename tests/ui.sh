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
# Ops: one top-level link with Control and Fleet under it, as plain links;
# the old System and Fleet items are gone.
echo "$nav" | grep -qF '<div class="menu"><a href="/ops" aria-haspopup="true">Ops</a><div class="menu__sub"><a href="/ops/control">Control</a><a href="/ops/fleet">Fleet</a></div></div>' \
    || fail "the Ops menu isn't in the nav"
echo "$nav" | grep -qE '<a href="/(system|fleet)"' && fail "the old System or Fleet nav item is still there"
curl -fsS "$base/static/orca.css" | grep -qF '.menu:focus-within .menu__sub' || fail "the Ops menu doesn't open on keyboard focus"
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
# Its page draws it in the browser (orca.ui_viz): a section that fetches
# timeline.json once it's in view, so the page holds none of the trace.
page=$(html "/runs/$fan")
echo "$page" | grep -qF "<section class=\"card tlv\" id=\"timeline\" data-src=\"/runs/$fan/timeline.json\">" \
    || fail "the run page has no timeline viewer"
echo "$page" | grep -qF '>Reset zoom</button>' || fail "the viewer has no Reset zoom"
echo "$page" | grep -qF 'Open in Perfetto ↗</a>' || fail "the viewer doesn't link Perfetto"
echo "$page" | grep -q 'new IntersectionObserver' || fail "the viewer doesn't wait to be in view"
echo "$page" | grep -qF 'fetch(box.dataset.src)' || fail "the viewer doesn't fetch its data-src"
echo "$page" | grep -q '"ph":' && fail "the run page holds the trace itself"
html "/runs/$ok_run" | grep -q 'class="card tlv"' && fail "a run without tasks has a timeline viewer"
curl -fsS "$base/static/orca.css" | grep -qF '.tlv__canvas' || fail "the stylesheet has no viewer rules"
curl -fsS -D - -o /dev/null "$base/runs/$fan/timeline.json" | grep -qi '^access-control-allow-origin: https://ui.perfetto.dev' \
    || fail "Perfetto can't fetch the timeline"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$ok_run/timeline.json")
[ "$code" = 404 ] || fail "a run without tasks answered $code for its timeline"

# /runs: the filters are dropdowns with the current choice selected, and an
# unknown value is kept, escaped.
page=$(html "/runs?pipeline=toy&status=failed")
echo "$page" | grep -qF '<select name="pipeline"><option value="">any</option>' || fail "no pipeline dropdown"
echo "$page" | grep -qF '<option value="toy" selected>toy</option>' || fail "toy isn't selected"
echo "$page" | grep -qF '<option value="daily">daily</option>' || fail "daily isn't a pipeline option"
echo "$page" | grep -qF '<option value="failed" selected>failed</option>' || fail "failed isn't selected"
echo "$page" | grep -qF '<option value="cancelled">cancelled</option>' || fail "cancelled isn't a status option"
echo "$page" | grep -qF '<select name="trigger"><option value="" selected>any</option><option value="schedule">schedule</option>' \
    || fail "the trigger dropdown"
echo "$page" | grep -qF '<option value="rebuild">rebuild</option>' || fail "rebuild isn't a trigger option"
echo "$page" | grep -qF '<button type="submit">Filter</button>' || fail "the Filter button"
html '/runs?trigger=%3Cb%3E' | grep -qF '<option value="&lt;b&gt;" selected>&lt;b&gt;</option>' \
    || fail "an unknown trigger isn't kept, escaped"

# Paging by id: 60 more runs, 25 to a page, across the boundaries with the
# filters kept.
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, ended)
   SELECT 'toy', registry_hash, '2023-01', 'schedule', 'succeeded', now()
   FROM orca.runs, generate_series(1, 60) WHERE id = $ok_run" > /dev/null
f='pipeline=toy&status=succeeded&trigger=schedule&n=25'
p1=$(json "/runs?$f")
echo "$p1" | jq -e '(.runs | length) == 25 and .total == 60 and .newer == 0 and .before == null and .n == 25
    and .filters == {"pipeline": "toy", "status": "succeeded", "trigger": "schedule"}
    and .next_before == .runs[-1].id and ([.runs[].trigger] | unique) == ["schedule"]' > /dev/null \
    || fail "the first page's JSON: $(echo "$p1" | jq -c 'del(.runs)')"
b2=$(echo "$p1" | jq .next_before)
page=$(html "/runs?$f")
echo "$page" | grep -qF '<span class="pager__at">1–25 of 60</span>' || fail "the first page's range"
echo "$page" | grep -q 'Newer' && fail "the first page links newer runs"
echo "$page" | grep -qF "<a href=\"/runs?pipeline=toy&amp;status=succeeded&amp;trigger=schedule&amp;n=25&amp;before=$b2\">Older →</a>" \
    || fail "the first page's Older link"
echo "$page" | grep -qF '<input type="hidden" name="n" value="25">' || fail "the form doesn't keep n"
p2=$(json "/runs?$f&before=$b2")
echo "$p2" | jq -e --argjson b "$b2" '(.runs | length) == 25 and .newer == 25 and .runs[0].id < $b
    and .runs[0].id == ([.runs[].id] | max)' > /dev/null || fail "the second page: $(echo "$p2" | jq -c "del(.runs)")"
[ "$(echo "$p1" "$p2" | jq -s '[.[].runs[].id] | unique | length')" -eq 50 ] || fail "pages overlap"
page=$(html "/runs?$f&before=$b2")
echo "$page" | grep -qF '<span class="pager__at">26–50 of 60</span>' || fail "the second page's range"
echo "$page" | grep -qF '<a href="/runs?pipeline=toy&amp;status=succeeded&amp;trigger=schedule&amp;n=25">← Newer</a>' \
    || fail "the second page's Newer link isn't the first page"
b3=$(echo "$p2" | jq .next_before)
p3=$(json "/runs?$f&before=$b3")
echo "$p3" | jq -e '(.runs | length) == 10 and .next_before == null and .newer == 50' > /dev/null \
    || fail "the last page: $(echo "$p3" | jq -c 'del(.runs)')"
page=$(html "/runs?$f&before=$b3")
echo "$page" | grep -qF '<span class="pager__at">51–60 of 60</span>' || fail "the last page's range"
echo "$page" | grep -q 'Older' && fail "the last page links older runs"
echo "$page" | grep -qF "before=$b2\">← Newer</a>" || fail "the last page's Newer link isn't the second page"
[ "$(json '/runs?n=100000' | jq .n)" -eq 500 ] || fail "n isn't capped at 500"
[ "$(json /runs | jq .n)" -eq 50 ] || fail "the default page isn't 50"
# Tries: the header says what it counts, and a cell has a number only when
# the run was started more than once; the JSON keeps `starts`.
q "UPDATE orca.runs SET starts = 3 WHERE id = $bad_run" > /dev/null
page=$(html "/runs?pipeline=toy&status=failed")
echo "$page" | grep -qF "<th class=\"num\"><abbr title=\"Times the run's process was started: 1 is normal; more means retries, a lost instance or a resume.\"" \
    || fail "the Tries header"
echo "$page" | grep -q '>Starts<' && fail "a runs table still says Starts"
echo "$page" | grep -qF '<td class="num">3</td>' || fail "a run started 3 times doesn't say 3"
echo "$page" | grep -qF '<th>Created</th><th class="num">Took</th><th>Partitions</th>' \
    || fail "Created and Took don't lead the runs table"
echo "$page" | grep -qE '<td class="mono"><time class="rel" datetime="[0-9-]+T[0-9:]+Z">' \
    || fail "a run's created time isn't a <time> the script shows as relative"
echo "$page" | grep -qE '<td class="num">[0-9]+s</td>' || fail "an ended run's Took"
was=$(q "SELECT partitions FROM orca.runs WHERE id = $bad_run")
q "UPDATE orca.runs SET partitions = 'a,b,c,d,e' WHERE id = $bad_run" > /dev/null
html "/runs?pipeline=toy&status=failed" \
    | grep -qF '<td class="mono parts" title="a,b,c,d,e">a, b <span class="muted">+3 more</span></td>' \
    || fail "a long list of partitions isn't cut short"
q "UPDATE orca.runs SET partitions = '$was' WHERE id = $bad_run" > /dev/null
page=$(html "/runs?pipeline=toy&status=succeeded&trigger=manual")
echo "$page" | grep -qF '<td class="num"></td>' || fail "a run started once isn't empty under Tries"
echo "$page" | grep -qF '<td class="num">1</td>' && fail "a run started once shows 1"
[ "$(json "/runs?pipeline=toy&status=failed" | jq '.runs[0].starts')" -eq 3 ] || fail "the JSON's starts"
# Actions beside their statuses: Pause per pipeline, Retry for a failed
# run, Rerun for a succeeded one, Rerun from here on an ended run's attempt,
# Cancel for a queued one; each titled with its command line.
html / | grep -qF 'action="/pipelines/toy/pause"><button type="submit" class="act__btn" title="orca pause toy">Pause</button></form>' \
    || fail "no Pause on the home page's row"
html /pipelines/toy | grep -qF '<h1>toy<form class="act" method="post" action="/pipelines/toy/pause">' \
    || fail "no Pause in the pipeline's header"
html "/runs?pipeline=toy&status=failed" | grep -qF "action=\"/runs/$bad_run/resume\"><button type=\"submit\" class=\"act__btn\" title=\"&lt;toy binary&gt; resume $bad_run" \
    || fail "no Retry for the failed run"
html "/runs/$bad_run" | grep -qF '>Retry</button>' || fail "no Retry in the failed run's header"
page=$(html "/runs/$ok_run")
echo "$page" | grep -qF "<form class=\"act\" method=\"post\" action=\"/runs/$ok_run/rerun\" data-confirm=\"Rerun?\"><input type=\"hidden\" name=\"step\" value=\"days\">" \
    || fail "no Rerun in the succeeded run's header"
html "/runs/$bad_run/attempts/$failed" | grep -qF '<input type="hidden" name="step" value="measure"><button type="submit" class="act__btn danger" title="no orca command for this yet: a new run from measure on, the steps before it copied">Rerun from here</button>' \
    || fail "no Rerun from here on the attempt"
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status)
   SELECT 'toy', registry_hash, '2023-02,2023-03', 'manual', 'queued' FROM orca.runs WHERE id = $ok_run" > /dev/null
qd=$(q "SELECT max(id) FROM orca.runs")
html "/runs?status=queued" | grep -qF "action=\"/runs/$qd/cancel\" data-confirm=\"Cancel?\"><button type=\"submit\" class=\"act__btn danger\" title=\"orca cancel toy 2023-02,2023-03" \
    || fail "no Cancel for the queued run"
html "/runs/$qd" | grep -qF '>Cancel</button>' || fail "no Cancel in the queued run's header"
html /runs | grep -q 'class="anon"' && fail "a page with login off is anon"
q "UPDATE orca.pipelines SET paused = true WHERE name = 'toy'" > /dev/null
html /pipelines/toy | grep -qF 'action="/pipelines/toy/unpause"><button type="submit" class="act__btn" title="orca unpause toy">Resume</button>' \
    || fail "no Resume for the paused pipeline"
q "UPDATE orca.pipelines SET paused = false WHERE name = 'toy'" > /dev/null
q "DELETE FROM orca.runs WHERE id = $qd" > /dev/null
echo "ok   /runs: dropdown filters, pages and tries"

# The home page's Timeline tab (orca.ui_viz): List stays the default and
# works without scripts; /api/gantt has the pipelines in dataset order
# (gantt_raw writes raw.a, which fake_sensor reads to write lake.ab, which
# gantt_report reads), their runs in the window, the edges between them,
# and the run gantt_raw's commit started in fake_sensor.
page=$(html /)
echo "$page" | grep -qF '<button type="button" role="tab" class="vtabs__tab" data-view="list" aria-selected="true" aria-controls="pipelines-list">List</button>' \
    || fail "the home page has no List tab, selected"
echo "$page" | grep -qF 'data-view="timeline" aria-selected="false" aria-controls="pipelines-gantt">Timeline</button>' \
    || fail "the home page has no Timeline tab"
echo "$page" | grep -qF '<div class="vtabs" hidden>' || fail "the tabs show without scripts"
echo "$page" | grep -qF '<div id="pipelines-list" role="tabpanel"><section class="card"><div class="scroll"><table>' \
    || fail "the list isn't the tabs' first panel"
echo "$page" | grep -qF 'id="pipelines-gantt" role="tabpanel" data-src="/api/gantt" hidden>' \
    || fail "the timeline panel isn't hidden until asked for"
echo "$page" | grep -qF 'localStorage.setItem' || fail "the tab isn't remembered"
fake() { # name, reads, writes: a pipeline's binary that only describes it
    cat > "build/$1" << FAKE
#!/bin/sh
case "\$1" in
describe) echo '{"pipeline": "$1", "registry_hash": "0x$1", "partitions": {"kind": "monthly", "start": null}, "schedule": null, "max_runs": 1, "run_retries": 0, "steps": [{"name": "s", "kind": "mapped", "after": null, "function": "s", "input": "orca.month.Month", "output": "Int", "reads": $2, "writes": $3}], "tasks": [], "source": {"repo": "", "commit": "", "dirty": false}}' ;;
*) exit 0 ;;
esac
FAKE
    chmod +x "build/$1"
}
fake gantt_report '["lake.ab"]' '[]'
fake gantt_raw '[]' '["raw.a"]'
export FAKE_SENSE="$PWD/build/ui-sense.txt"
printf 'raw.a -1 other -\nraw.b -1 other -\n' > "$FAKE_SENSE"
for b in ./build/gantt_report ./tests/fake_sensor ./build/gantt_raw; do
    ./build/orca register "$b" >> "$log" 2>&1 || fail "register $b"
done
# gantt_raw's run commits raw.a@7, which starts fake_sensor's run (as the
# scheduler records it in orca.sensed); gantt_report has a fill running.
mk() { # pipeline, status, trigger, minutes ago started, ended (or null)
    q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, created, started, ended)
       SELECT '$1', registry_hash, '2024-01', '$3', '$2', now() - interval '$4 minutes',
       now() - interval '$4 minutes', $5 FROM orca.pipelines WHERE name = '$1' RETURNING id" | head -1
}
up=$(mk gantt_raw succeeded schedule 120 "now() - interval '110 minutes'")
down=$(mk fake_sensor succeeded sensor 105 "now() - interval '100 minutes'")
filling=$(mk gantt_report running fill 30 null)
old=$(mk gantt_raw succeeded schedule 3000 "now() - interval '2990 minutes'")
st=$(q "INSERT INTO orca.steps (run, step, partition, ord, status) VALUES ($up, 's', '2024-01', 0, 'succeeded') RETURNING id" | head -1)
at=$(q "INSERT INTO orca.attempts (step, attempt, status, host, registry_hash) VALUES ($st, 1, 'succeeded', 'h', '0xgantt_raw') RETURNING id" | head -1)
q "INSERT INTO orca.commits (attempt, table_name, snapshot_id, dataset) VALUES ($at, 'raw/a', 7, 'raw.a')" > /dev/null
q "INSERT INTO orca.sensed (pipeline, dataset, snapshot_id, run) VALUES ('fake_sensor', 'raw.a', 7, $down)" > /dev/null
g=$(json '/api/gantt?window=24h')
echo "$g" | jq -e '[.rows[].pipeline | select(startswith("gantt_") or . == "fake_sensor")] == ["gantt_raw", "fake_sensor", "gantt_report"]' > /dev/null \
    || fail "/api/gantt's rows aren't in dataset order: $(echo "$g" | jq -c '[.rows[] | [.pipeline, .depth]]')"
echo "$g" | jq -e '.window == "24h" and .seconds == 86400 and .now > 0 and .truncated == false
    and ([.rows[] | select(.pipeline == "fake_sensor")][0] | .depth == 1 and .how == "sensor" and .reads == ["raw.a", "raw.b"] and .writes == ["lake.ab"])
    and ([.rows[] | select(.pipeline == "gantt_report")][0].depth == 2)' > /dev/null \
    || fail "/api/gantt's rows: $(echo "$g" | jq -c '.rows')"
echo "$g" | jq -e '[.edges[] | select(.from == "gantt_raw" or .to == "gantt_report") | [.from, .to, .dataset]]
    == [["gantt_raw", "fake_sensor", "raw.a"], ["fake_sensor", "gantt_report", "lake.ab"]]' > /dev/null \
    || fail "/api/gantt's edges: $(echo "$g" | jq -c '.edges')"
echo "$g" | jq -e --argjson up "$up" --argjson down "$down" --argjson f "$filling" '
    ([.runs[] | select(.pipeline | startswith("gantt_") or . == "fake_sensor") | .id] | sort) == ([$up, $down, $f] | sort)
    and ([.runs[] | select(.id == $f)][0] | .end == null and .trigger == "fill" and .status == "running")
    and ([.runs[] | select(.id == $up)][0] | .end > .start)
    and .triggers == [{"from": $up, "to": $down, "dataset": "raw.a", "snapshot": 7}]' > /dev/null \
    || fail "/api/gantt's runs and triggers: $(echo "$g" | jq -c '{runs: [.runs[] | [.id, .pipeline, .status]], triggers}')"
[ "$(json '/api/gantt?window=7d' | jq --argjson old "$old" '[.runs[] | select(.id == $old)] | length')" -eq 1 ] \
    || fail "the 7 d window hasn't the run of two days ago"
[ "$(json '/api/gantt?window=1h' | jq -r '[.window, (.triggers | length)] | join(" ")')" = "1h 0" ] \
    || fail "the 1 h window"
[ "$(json '/api/gantt?window=x' | jq -r .window)" = 24h ] || fail "an unknown window isn't 24h"
echo "ok   the home page's timeline: tabs, rows in dataset order, runs and what started them"

kill "$pid"
echo "ok   ui"
