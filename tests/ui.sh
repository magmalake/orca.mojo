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
. "$PWD/tests/port.sh"
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

free_port port
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
# Ops: one top-level link with Control, Fleet and Workers under it, as plain links;
# the old System and Fleet items are gone.
echo "$nav" | grep -qF '<div class="menu"><a href="/ops" aria-haspopup="true">Ops</a><div class="menu__sub"><a href="/ops/control">Control</a><a href="/ops/fleet">Fleet</a><a href="/ops/workers">Workers</a></div></div>' \
    || fail "the Ops menu isn't in the nav"
echo "$nav" | grep -qE '<a href="/(system|fleet)"' && fail "the old System or Fleet nav item is still there"
curl -fsS "$base/static/orca.css" | grep -qF '.menu:focus-within .menu__sub' || fail "the Ops menu doesn't open on keyboard focus"
# PostHog: no snippet without ORCA_POSTHOG_KEY; with a phc_ key, every page's
# head has it, with the key and site "orca"; a key that isn't one adds nothing.
echo "$nav" | grep -q 'ph/static/array.js' && fail "the PostHog snippet is there without ORCA_POSTHOG_KEY"
for key in phc_TestKey_123 'phc_x");alert(1);//' not_a_key; do
    free_port php
    ORCA_POSTHOG_KEY=$key ORCA_UI_ADDR="127.0.0.1:$php" ./build/orca ui >> "$log" 2>&1 &
    phpid=$!
    i=0
    until curl -fs "http://127.0.0.1:$php/" > /dev/null 2>&1; do
        i=$((i + 1)); [ "$i" -lt 100 ] || { kill "$phpid"; fail "orca ui with ORCA_POSTHOG_KEY didn't start"; }; sleep 0.1
    done
    for path in / /runs "/runs/$ok_run" /login; do
        page=$(curl -sS "http://127.0.0.1:$php$path")
        head=${page%%</head>*}
        if [ "$key" = phc_TestKey_123 ]; then
            echo "$head" | grep -qF '<script defer src="/ph/static/array.js"></script>' \
                && echo "$head" | grep -qF 'posthog.init("phc_TestKey_123",{api_host:location.origin+'"'/ph'"',' \
                && echo "$head" | grep -qF "persistence:'memory'" \
                && echo "$head" | grep -qF "posthog.register({site:'orca'})" \
                || { kill "$phpid"; fail "$path's head hasn't the PostHog snippet"; }
        else
            echo "$page" | grep -q 'posthog' && { kill "$phpid"; fail "ORCA_POSTHOG_KEY=$key added a snippet to $path"; }
        fi
    done
    kill "$phpid"
    wait "$phpid" 2> /dev/null || true
done
echo "ok   PostHog only with a phc_ ORCA_POSTHOG_KEY"
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
# The page's header: Progress (refreshed from its fragment), Config and
# Code cards, each fact with its help; Steps with Graph and Table tabs,
# the graph shown for more than one step, and ?steps= for the other.
page=$(html /pipelines/toy)
for id in progress config code; do
    echo "$page" | grep -q "<section class=\"card card--facts\" id=\"$id\">" || fail "toy's page has no $id card"
done
echo "$page" | grep -qF '<div data-poll="/pipelines/toy/progress" data-poll-every="5"><dl class="facts"><dt>Runs at once' \
    || fail "the Progress card isn't refreshed"
for key in Partitions Schedule "Runs at once" "Run retries" Placement Binary; do
    echo "$page" | grep -qF "<dt>$key<span class=\"help\"><button type=\"button\" class=\"help__btn\" aria-label=\"About $key\" aria-describedby=\"help-" \
        || fail "$key has no help"
done
[ "$(echo "$page" | grep -o 'role="tooltip" id="help-' | wc -l)" -eq "$(echo "$page" | grep -o 'aria-describedby="help-' | wc -l)" ] \
    || fail "a help button without its text"
echo "$page" | grep -qF '<div id="steps-graph" role="tabpanel" class="scroll steps__graph">' || fail "toy's graph isn't shown first"
echo "$page" | grep -qF '<div id="steps-table" role="tabpanel" class="scroll" hidden>' || fail "toy's table isn't the other tab"
html '/pipelines/toy?steps=table' | grep -qF '<div id="steps-graph" role="tabpanel" class="scroll steps__graph" hidden>' \
    || fail "?steps=table still shows the graph"
html /pipelines/toy/progress | grep -q '^<dl class="facts"><dt>Runs at once' || fail "the progress fragment"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/pipelines/nope/progress")" = 404 ] || fail "an unknown pipeline's progress"
# Three tabs, each its own page with Progress on top: Overview (the
# default and anything unknown), Runs and Operate, which ?cal= and
# ?preview= open; the JSON is the same on each.
tab_of() { html "$1" | grep -o 'class="ptabs__tab" href="[^"]*" aria-current="page">[A-Za-z]*' | sed 's/.*">//'; }
for t in "/pipelines/toy Overview" "/pipelines/toy?tab=bogus Overview" "/pipelines/toy?tab=runs Runs" \
    "/pipelines/toy?tab=operate Operate" "/pipelines/toy?cal=2024 Operate" "/pipelines/toy?preview=1&partitions=2024-01 Operate"; do
    set -- $t
    [ "$(tab_of "$1")" = "$2" ] || fail "$1 isn't on $2: $(tab_of "$1")"
done
echo "$page" | grep -qF '<nav class="ptabs" aria-label="Pipeline sections"><a class="ptabs__tab" href="/pipelines/toy" aria-current="page">Overview</a><a class="ptabs__tab" href="/pipelines/toy?tab=runs">Runs</a><a class="ptabs__tab" href="/pipelines/toy?tab=operate">Operate</a></nav>' \
    || fail "the tabs"
echo "$page" | grep -q 'id="start"\|<h2>Versions\|<h2>Runs</h2>\|id="coverage"' && fail "Overview has another tab's sections"
page=$(html '/pipelines/toy?tab=runs')
echo "$page" | grep -q '<h2>Runs</h2>' && echo "$page" | grep -q '<h2>Versions' || fail "the Runs tab"
echo "$page" | grep -q 'id="config"\|id="steps"\|id="start"' && fail "the Runs tab has another tab's sections"
page=$(html '/pipelines/toy?tab=operate')
echo "$page" | grep -q 'id="coverage"' && echo "$page" | grep -q 'id="start"' && echo "$page" | grep -q '<h2>Stop runs' \
    || fail "the Operate tab"
echo "$page" | grep -q 'data-poll="/pipelines/toy/progress"' || fail "Operate has no live Progress"
[ "$(json '/pipelines/toy?tab=runs' | jq -c '[.steps | length, (.runs | length)]')" = "$(json /pipelines/toy | jq -c '[.steps | length, (.runs | length)]')" ] \
    || fail "the JSON differs by tab"
ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/daily >> "$log" 2>&1 || fail "register daily"
[ "$(html /pipelines/daily | grep -o 'class="dag__box' | wc -l)" -eq 2 ] || fail "daily's DAG hasn't 2 boxes"
[ "$(html /pipelines/daily | grep -o 'class="dag__edge' | wc -l)" -eq 1 ] || fail "daily's DAG hasn't 1 edge"
page=$(html "/runs/$bad_run")
echo "$page" | grep -q 'dag__box dag__box--failed" data-step="measure"[^>]*><title>measure' || fail "measure isn't failed in the DAG"
echo "$page" | grep -q 'dag__box dag__box--skipped" data-step="total"[^>]*><title>total' || fail "total isn't skipped in the DAG"
echo "$page" | grep -q 'data-poll="' && fail "an ended run's page still polls"
# Step cards (orca.ui.step_cards): what each step's card shows is in the page,
# one JSON per Steps card, so a hover asks nothing; each step in the table,
# graph and timeline shows it, on a run's page with that attempt's.
cards() { sed -n 's/.*<script type="application\/json" class="step-cards">\([^<]*\)<\/script>.*/\1/p'; }
pc=$(html /pipelines/toy | cards)
echo "$pc" | jq -e . > /dev/null || fail "the pipeline's step cards aren't JSON: $pc"
[ "$(echo "$pc" | jq -r '.steps | keys | join(",")')" = "days,measure,total" ] || fail "the step cards' steps: $pc"
[ "$(echo "$pc" | jq -r '.steps.total | [.kind, .after, .input, .output] | join(" ")')" = "gather measure Seq[Measured] Int" ] \
    || fail "total's card: $(echo "$pc" | jq -c .steps.total)"
echo "$pc" | jq -r .steps.days.sig | grep -qF '<span class="hl hl--kw">def</span> <span class="hl hl--fn">days</span>(' \
    || fail "days' card has no coloured signature"
echo "$pc" | jq -r .steps.days.kind_help | grep -q 'once per partition' || fail "a card's kind has no meaning"
[ "$(echo "$pc" | jq -r '.steps.measure.recent | [.runs, (.failed > 0), (.took | startswith("p50 "))] | join(" ")')" = "2 true true" ] \
    || fail "measure's recent runs: $(echo "$pc" | jq -c .steps.measure)"
pt=$(html '/pipelines/toy?steps=table')
echo "$pt" | grep -qF '<section class="card steps" id="steps" data-tabs="orca.steps.view" data-tabs-param="steps" data-step-cards>' \
    || fail "the pipeline's Steps card isn't one with cards"
echo "$pt" | grep -qF '<td><span class="step-ref" data-step="days" tabindex="0">days</span></td>' || fail "the table's step shows no card"
echo "$pt" | grep -q '<g class="dag__box" data-step="total" tabindex="0">' || fail "the graph's step shows no card"
rc=$(html "/runs/$bad_run" | cards)
failed_attempt=$(echo "$rc" | jq -r '.attempts | to_entries[] | select(.value.status == "failed") | .key' | head -1)
[ -n "$failed_attempt" ] || fail "the run's step cards have no failed attempt: $rc"
[ "$(echo "$rc" | jq -r ".attempts[\"$failed_attempt\"] | [.step, .partition, .href] | join(\" \")")" = "measure 2024-02 /runs/$bad_run/attempts/$failed_attempt" ] \
    || fail "the failed attempt's card: $(echo "$rc" | jq -c ".attempts[\"$failed_attempt\"]")"
echo "$rc" | jq -r ".attempts[\"$failed_attempt\"].error" | grep -q 'failing 2024-02 as asked' || fail "the attempt's card has no error"
echo "$rc" | jq -e '.steps.measure.recent' > /dev/null && fail "a run's card has the pipeline's recent runs"
echo "$pt" | grep -q 'data-step="measure" data-attempt=' && fail "the pipeline's table names an attempt"
html "/runs/$bad_run" | grep -q "<span class=\"step-ref\" data-step=\"measure\" data-attempt=\"$failed_attempt\" tabindex=\"0\">measure</span>" \
    || fail "the run's table step shows no attempt's card"
html "/runs/$bad_run?steps=timeline" | grep -q "<rect class=\"tl__bar tl---1\"[^>]*data-step=\"measure\" data-attempt=\"$failed_attempt\" tabindex=\"0\"><title>" \
    || fail "the timeline's bar shows no attempt's card"
html "/runs/$bad_run/steps" | cards | jq -e ".attempts[\"$failed_attempt\"]" > /dev/null || fail "the live Steps card lost its cards"
html "/runs/$bad_run/live" | cards | jq -e ".attempts[\"$failed_attempt\"] and .steps.measure" > /dev/null || fail "the run's live fragment lost the step cards"
curl -fsS "$base/static/orca.css" | grep -qF '.scard {' || fail "the stylesheet has no step card"
html / | grep -qF '"&at=" + m[2]' || fail "the step card doesn't ask /code for its signature"
# /code?at= answers a def's signature and docstring: a line, or refused.
gh="https://github.com/a/b/blob/abcdef1/x.mojo"
for at in x 0 -3; do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$base/code?u=$(printf %s "$gh" | jq -sRr @uri)&at=$at")" = 400 ] || fail "/code took at=$at"
done
# The run page's cards, each fact with its help; Steps with Table,
# Timeline and Graph tabs, the table first, ?steps= for the others; the
# page's script follows a refresh (tests/lineage.sh has its Snapshots).
for id in status trigger code resources settings; do
    echo "$page" | grep -q "<section class=\"card card--facts\" id=\"$id\"><h2>" || fail "the run page has no $id card"
done
for key in Duration Created Started Ended Tries Partitions Trigger "Peak memory" "CPU used"; do
    echo "$page" | grep -qF "<dt>$key<span class=\"help\"><button type=\"button\" class=\"help__btn\" aria-label=\"About $key\" aria-describedby=\"help-" \
        || fail "the run's $key has no help"
done
[ "$(echo "$page" | grep -o 'role="tooltip" id="help-' | wc -l)" -eq "$(echo "$page" | grep -o 'aria-describedby="help-' | wc -l)" ] \
    || fail "a run page help button without its text"
[ "$(echo "$page" | grep -o ' id="help-[^"]*"' | sort | uniq -d | wc -l)" -eq 0 ] || fail "two helps with one id"
echo "$page" | grep -qF '<p class="hash">Code hash <code>' || fail "the run's code hash"
echo "$page" | grep -qF '<section class="card steps" id="steps" data-tabs="orca.run.steps" data-tabs-param="steps" data-step-cards>' \
    || fail "the run's Steps card"
echo "$page" | grep -qF '<div id="steps-table" role="tabpanel" class="scroll">' || fail "the run's table isn't shown first"
echo "$page" | grep -qF '<div id="steps-timeline" role="tabpanel" class="scroll steps__graph" hidden><svg' || fail "the run's timeline tab"
echo "$page" | grep -qF '<div id="steps-graph" role="tabpanel" class="scroll steps__graph" hidden><svg class="dag"' || fail "the run's graph tab"
echo "$page" | grep -q 'aria-label="The run&#39;s attempts over time"' || fail "the timeline doesn't draw the attempts"
[ "$(echo "$page" | grep -o '<h2>Steps' | wc -l)" -eq 1 ] || fail "the run's steps are shown twice"
html "/runs/$bad_run?steps=timeline" | grep -qF '<div id="steps-timeline" role="tabpanel" class="scroll steps__graph"><svg' \
    || fail "?steps=timeline doesn't show the timeline"
html "/runs/$bad_run?steps=graph" | grep -qF '<div id="steps-table" role="tabpanel" class="scroll" hidden>' \
    || fail "?steps=graph still shows the table"
echo "$page" | grep -qF 'el.dispatchEvent(new Event("orca:swap"' || fail "a refresh doesn't tell the tabs"
echo "$page" | grep -qF 'document.addEventListener("orca:swap"' || fail "the tabs don't follow a refresh"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$bad_run/live")
[ "$code" = 286 ] || fail "an ended run's fragment answered $code, not 286"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/999999/live")
[ "$code" = 404 ] || fail "an unknown run's fragment answered $code, not 404"

# A run's settings: the pipeline's registered environment, or what the run's
# start recorded; secrets by name only.
q "UPDATE orca.pipelines SET env = '{\"REG_FLAG\": \"reg-on\"}', max_runs = 3,
    secrets = '{\"DB_PASSWORD\": \"arn:sm:reg-secret-id\"}' WHERE name = 'toy'" > /dev/null
q "UPDATE orca.runs SET spec = '{\"env\": {\"SPEC_FLAG\": \"spec-on\"},
    \"secrets\": {\"API_TOKEN\": \"arn:sm:spec-secret-id\"}, \"workers\": 2}' WHERE id = $bad_run" > /dev/null
page=$(html "/runs/$ok_run")
echo "$page" | grep -q '<section class="card card--facts" id="settings">' || fail "no settings on the run page"
echo "$page" | grep -q 'REG_FLAG</code>=<code class="env">reg-on' || fail "the registered environment isn't shown"
echo "$page" | grep -q '<code>DB_PASSWORD</code>' || fail "the secret's name isn't shown"
echo "$page" | grep -q 'reg-secret-id' && fail "a secret's value is on the page"
page=$(html "/runs/$bad_run")
echo "$page" | grep -q 'SPEC_FLAG</code>=<code class="env">spec-on' || fail "the run's recorded environment isn't shown"
echo "$page" | grep -q 'REG_FLAG' && fail "the registered environment shows over the run's own"
echo "$page" | grep -q '<code>API_TOKEN</code>' || fail "the recorded secret's name isn't shown"
echo "$page" | grep -q 'spec-secret-id' && fail "a recorded secret's value is on the page"
q "UPDATE orca.runs SET spec = NULL WHERE id = $bad_run" > /dev/null
q "UPDATE orca.pipelines SET env = NULL, max_runs = NULL, secrets = NULL WHERE name = 'toy'" > /dev/null

# Machine metrics: none recorded, the page is as it was; then a worker's
# (tests/fixtures/metrics-worker.jsonl: memory over 90% for 30 s, steal
# over 20% for 10 s) and this machine's, from the sampler itself.
html "/runs/$ok_run" | grep -q 'id="machines"' && fail "a run without samples shows machines"
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
echo "$page" | grep -qF '<section class="card" id="machines"><h2>Machines<span class="help">' || fail "the Machines card has no help"
echo "$page" | grep -qF '<p class="card__sub">The whole machine, while this run ran: the scheduler' \
    || fail "the Machines card doesn't say it's the whole machine"
# A run that ran beside another on the scheduler's machine says it shared it.
echo "$page" | grep -q 'card__warn' && fail "a run alone on its machine says it shared it"
was=$(q "SELECT coalesce(placement, 'NULL') || '|' || started || '|' || ended FROM orca.runs WHERE id = $bad_run")
q "UPDATE orca.runs b SET placement = 'local', started = o.started, ended = o.ended + interval '1 hour'
   FROM orca.runs o WHERE o.id = $ok_run AND b.id = $bad_run" > /dev/null
html "/runs/$ok_run" | grep -qF '<p class="card__warn">A shared machine: 1 other run ran on it at the same time' \
    || fail "a run beside another doesn't say it shared the machine"
q "UPDATE orca.runs SET placement = nullif(split_part('$was', '|', 1), 'NULL'),
   started = split_part('$was', '|', 2)::timestamptz, ended = split_part('$was', '|', 3)::timestamptz WHERE id = $bad_run" > /dev/null
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
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$ok_run/live")" = 286 ] \
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
html "/runs/$live" | grep -q "data-poll=\"/runs/$live/live\" data-poll-every=\"5\"" || fail "a running run's page doesn't poll"
code=$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$live/live")
[ "$code" = 200 ] || fail "a running run's fragment answered $code"
[ "$(html "/runs/$live" | grep -c 'data-poll="')" -eq 1 ] || fail "a running run's page has more than one poller"
html "/runs/$live" | grep -q '<dt>Duration<span class="help">.*</dt><dd title="started [^"]*; still going">[0-9]* s</dd>' || fail "a running run has no elapsed duration"
html "/runs/$ok_run" | grep -q '<dt>Duration<span class="help">.*</dt><dd title="started [^"]*; ended [^"]*">[0-9 a-z]*</dd>' || fail "a finished run has no total duration"
html "/runs/$live" | grep -q "rel=\"prev\" href=\"/runs/$((live - 1))\"" || fail "no link to the previous run"
html "/runs/$live" | grep -q 'rel="next" href=' && fail "the newest run links a next one"
html "/runs/$ok_run" | grep -q 'rel="prev" href=' && fail "the first run links a previous one"
html "/runs/$ok_run" | grep -q "rel=\"next\" href=\"/runs/$((ok_run + 1))\"" || fail "no link to the next run"
html "/runs/$live" | grep -q 'id="settings"' || fail "a running run's page has no settings"
# Its machines too: the fragment draws samples as they come.
mkdir -p "$ORCA_LOGS/toy/$live/metrics"
./build/orca sample --out "$ORCA_LOGS/toy/$live/metrics/local.jsonl" --count 1 --seconds 0.1 || fail "the live sampler"
code=$(curl -s -o "$log.mx" -w '%{http_code}' "$base/runs/$live/live")
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
# Its facts (times, use, the step's attempts) refresh too, until it ends.
html "/runs/$live/attempts/$a" | grep -q "data-poll=\"/runs/$live/attempts/$a/facts\" data-poll-every=\"5\"" \
    || fail "a running attempt's facts don't refresh"
code=$(curl -s -o "$log.af" -w '%{http_code}' "$base/runs/$live/attempts/$a/facts")
[ "$code" = 200 ] || fail "a running attempt's facts answered $code"
grep -qF '<dl class="facts"><dt>Host</dt>' "$log.af" || fail "the facts fragment hasn't the facts"
wait "$toy" || fail "the slow run failed"
code=$(curl -s -o "$log.frag" -w '%{http_code}' "$base/runs/$live/live")
[ "$code" = 286 ] || fail "the ended run's fragment answered $code"
grep -q 'status--succeeded">succeeded</span></h2>' "$log.frag" || fail "the fragment doesn't show the run succeeded"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$live/attempts/$a/log")" = 286 ] \
    || fail "the ended attempt's log didn't answer 286"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/runs/$live/attempts/$a/facts")" = 286 ] \
    || fail "the ended attempt's facts didn't answer 286"

# A run with Dista tasks: its timeline, as Chrome's trace format.
./build/fanout run --partitions 2024-01 >> "$log" 2>&1 || fail "fanout failed"
fan=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'fanout'")
html "/runs/$fan" | grep -q "/runs/$fan/timeline.json" || fail "the run doesn't link its timeline"
curl -fsS "$base/runs/$fan/timeline.json" | grep -q '"ph":"X"' || fail "the timeline isn't a Chrome trace"
# Its page draws it in the browser (orca.ui.viz): a section that fetches
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

# Paging by id (keyset): 60 more runs, 25 to a page, across the boundaries
# with the filters kept, newer pages by `after`, and pages that stay put
# while new runs arrive.
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, ended)
   SELECT 'toy', registry_hash, '2023-01', 'schedule', 'succeeded', now()
   FROM orca.runs, generate_series(1, 60) WHERE id = $ok_run" > /dev/null
f='pipeline=toy&status=succeeded&trigger=schedule&limit=25'
fa='pipeline=toy&amp;status=succeeded&amp;trigger=schedule&amp;limit=25'
p1=$(json "/runs?$f")
echo "$p1" | jq -e '(.runs | length) == 25 and .total == 60 and .newer == 0 and .older == 35
    and .before == null and .after == null and .n == 25 and .limit == 25
    and .filters == {"pipeline": "toy", "status": "succeeded", "trigger": "schedule"}
    and .next_before == .runs[-1].id and .next == .runs[-1].id and .prev == null and .prev_url == null
    and .next_url == "/runs?pipeline=toy&status=succeeded&trigger=schedule&limit=25&before=\(.next)"
    and ([.runs[].trigger] | unique) == ["schedule"]' > /dev/null \
    || fail "the first page's JSON: $(echo "$p1" | jq -c 'del(.runs)')"
b2=$(echo "$p1" | jq .next_before)
page=$(html "/runs?$f")
echo "$page" | grep -qF '<span class="pager__at">1–25 of 60</span>' || fail "the first page's range"
echo "$page" | grep -qE 'Newer|Newest' && fail "the first page links newer runs"
echo "$page" | grep -qF "<a rel=\"next\" href=\"/runs?$fa&amp;before=$b2\">Older →</a>" \
    || fail "the first page's Older link"
echo "$page" | grep -qF '<input type="hidden" name="limit" value="25">' || fail "the form doesn't keep the page size"
# The newest page refreshes from its fragment, filters and size kept:
# slowly, as nothing on it is going on.
echo "$page" | grep -qF "<div data-poll=\"/runs/table?view=list&amp;$fa\" data-poll-every=\"30\">" \
    || fail "the newest page doesn't refresh"
p2=$(json "/runs?$f&before=$b2")
echo "$p2" | jq -e --argjson b "$b2" '(.runs | length) == 25 and .newer == 25 and .older == 10 and .runs[0].id < $b
    and .runs[0].id == ([.runs[].id] | max) and .prev == .runs[0].id
    and .prev_url == "/runs?pipeline=toy&status=succeeded&trigger=schedule&limit=25"' > /dev/null \
    || fail "the second page: $(echo "$p2" | jq -c "del(.runs)")"
[ "$(echo "$p1" "$p2" | jq -s '[.[].runs[].id] | unique | length')" -eq 50 ] || fail "pages overlap"
page=$(html "/runs?$f&before=$b2")
echo "$page" | grep -qF '<span class="pager__at">26–50 of 60</span>' || fail "the second page's range"
echo "$page" | grep -qF "<a rel=\"prev\" href=\"/runs?$fa\">← Newer</a>" \
    || fail "the second page's Newer link isn't the first page"
echo "$page" | grep -q 'Newest' && fail "the second page links the newest as well as Newer, the same page"
echo "$page" | grep -q 'data-poll="/runs/table' && fail "an older page with nothing going on refreshes"
b3=$(echo "$p2" | jq .next_before)
p3=$(json "/runs?$f&before=$b3")
echo "$p3" | jq -e '(.runs | length) == 10 and .next_before == null and .next == null and .newer == 50 and .older == 0' > /dev/null \
    || fail "the last page: $(echo "$p3" | jq -c 'del(.runs)')"
top3=$(echo "$p3" | jq '.runs[0].id')
page=$(html "/runs?$f&before=$b3")
echo "$page" | grep -qF '<span class="pager__at">51–60 of 60</span>' || fail "the last page's range"
echo "$page" | grep -q 'Older' && fail "the last page links older runs"
echo "$page" | grep -qF "<a rel=\"prev\" href=\"/runs?$fa&amp;after=$top3\">← Newer</a>" \
    || fail "the last page's Newer link isn't the page above it"
echo "$page" | grep -qF "<a href=\"/runs?$fa\">« Newest</a>" || fail "the last page doesn't link the newest"
# The page above the last is the second, by `after`; just under the top,
# `after` is the newest page.
pa=$(json "/runs?$f&after=$top3")
[ "$(echo "$pa" | jq -c '[.runs[].id]')" = "$(echo "$p2" | jq -c '[.runs[].id]')" ] \
    || fail "after=$top3 isn't the second page: $(echo "$pa" | jq -c '[.runs[].id]')"
echo "$pa" | jq -e --argjson a "$top3" '.after == $a and .newer == 25 and .older == 10' > /dev/null \
    || fail "the page above the last: $(echo "$pa" | jq -c 'del(.runs)')"
near=$(echo "$p1" | jq '.runs[3].id')
pn=$(json "/runs?$f&after=$near")
[ "$(echo "$pn" | jq -c '[.after, [.runs[].id]]')" = "$(echo "$p1" | jq -c '[null, [.runs[].id]]')" ] \
    || fail "after a run near the top isn't the newest page"
# New runs: the newest page shows them, an older one stays as it was.
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, ended)
   SELECT 'toy', registry_hash, '2023-01', 'schedule', 'succeeded', now()
   FROM orca.runs, generate_series(1, 3) WHERE id = $ok_run" > /dev/null
[ "$(json "/runs?$f&before=$b2" | jq -c '[.runs[].id]')" = "$(echo "$p2" | jq -c '[.runs[].id]')" ] \
    || fail "the second page moved when new runs arrived"
json "/runs?$f" | jq -e --argjson t "$(echo "$p1" | jq '.runs[0].id')" '.total == 63 and .runs[0].id > $t' > /dev/null \
    || fail "the newest page doesn't have the new runs"
[ "$(json '/runs?n=100000' | jq .n)" -eq 500 ] || fail "n isn't capped at 500"
[ "$(json '/runs?limit=100000' | jq .limit)" -eq 500 ] || fail "limit isn't capped at 500"
[ "$(json '/runs?pipeline=toy&n=7' | jq '[.limit, (.runs | length)] | join(" ")' -r)" = "7 7" ] || fail "n isn't still the page size"
[ "$(json /runs | jq .n)" -eq 50 ] || fail "the default page isn't 50"
# The fragment a list refreshes from: the table and pager, and when to ask
# again: soon while a run on it is going on, slowly on the newest page,
# never (286) for an older page with nothing going on.
frag() { curl -s -D "$log.h" -o "$log.frag" -w '%{http_code}' "$base$1"; }
[ "$(frag "/runs/table?view=list&$f")" = 200 ] || fail "the newest page's fragment"
grep -qi '^X-Poll-Every: 30' "$log.h" || fail "the idle newest page's fragment doesn't say 30 s"
grep -qF '<span class="pager__at">1–25 of 63</span>' "$log.frag" || fail "the fragment's pager"
head -c 30 "$log.frag" | grep -qF '<div class="scroll"><table>' || fail "the fragment isn't the table"
[ "$(frag "/runs/table?view=list&$f&before=$b2")" = 286 ] || fail "an idle older page's fragment didn't answer 286"
mid=$(echo "$p2" | jq '.runs[5].id')
q "UPDATE orca.runs SET status = 'running', started = now(), ended = NULL WHERE id = $mid" > /dev/null
[ "$(frag "/runs/table?view=list&pipeline=toy&limit=25&before=$b2")" = 200 ] \
    || fail "an older page with a running run stopped refreshing"
grep -qi '^X-Poll-Every: 5' "$log.h" || fail "a page with a running run doesn't refresh every 5 s"
html "/runs?pipeline=toy&limit=25&before=$b2" \
    | grep -qF "<div data-poll=\"/runs/table?view=list&amp;pipeline=toy&amp;limit=25&amp;before=$b2\" data-poll-every=\"5\">" \
    || fail "an older page with a running run doesn't refresh"
q "UPDATE orca.runs SET status = 'succeeded', ended = now() WHERE id = $mid" > /dev/null
# A pipeline's Runs tab: 20 to a page, its own pager and fragment, and its
# JSON's runs_page; the home page's recent runs, without a pager.
page=$(html "/pipelines/toy?tab=runs")
echo "$page" | grep -qF '<div data-poll="/runs/table?view=pipeline&amp;pipeline=toy&amp;limit=20" data-poll-every="30">' \
    || fail "the Runs tab doesn't refresh"
pj=$(json "/api/pipelines/toy")
echo "$pj" | jq -e '(.runs | length) == 20 and .runs_page.limit == 20 and .runs_page.next == .runs[-1].id
    and .runs_page.prev == null and .runs_page.total > 60' > /dev/null || fail "the pipeline's runs_page: $(echo "$pj" | jq -c .runs_page)"
pb=$(echo "$pj" | jq .runs_page.next)
echo "$page" | grep -qF "<a rel=\"next\" href=\"/pipelines/toy?tab=runs&amp;before=$pb\">Older →</a>" \
    || fail "the Runs tab's Older link"
json "/api/pipelines/toy?tab=runs&before=$pb" | jq -e --argjson b "$pb" '.runs[0].id < $b and .runs_page.newer == 20' > /dev/null \
    || fail "the Runs tab's second page"
html "/pipelines/toy?tab=runs&before=$pb" | grep -qF '<a rel="prev" href="/pipelines/toy?tab=runs">← Newer</a>' \
    || fail "the Runs tab's Newer link"
[ "$(frag "/runs/table?view=pipeline&pipeline=toy&limit=20")" = 200 ] || fail "the Runs tab's fragment"
grep -q '<th>Pipeline' "$log.frag" && fail "the Runs tab's fragment has a Pipeline column"
html / | grep -qF '<div data-poll="/runs/table?view=home&amp;limit=10" data-poll-every="' || fail "the home page's recent runs don't refresh"
[ "$(frag "/runs/table?view=home&limit=10")" = 200 ] || fail "the home page's recent runs' fragment"
grep -q 'pager' "$log.frag" && fail "the home page's recent runs have a pager"
[ "$(grep -o '<tr><td><a class="chip"' "$log.frag" | wc -l)" -eq 10 ] || fail "the home page's fragment hasn't 10 runs"
[ "$(frag /home/pipelines)" = 200 ] || fail "the home page's pipelines fragment"
grep -qF '<section class="card"><div class="scroll"><table><thead><tr><th>Pipeline' "$log.frag" || fail "the pipelines fragment isn't the table"
grep -qiE '^X-Poll-Every: (5|30)' "$log.h" || fail "the pipelines fragment doesn't say when to ask again"
# Tries: the header says what it counts, and a cell has a number only when
# the run was started more than once; the JSON keeps `starts`.
q "UPDATE orca.runs SET starts = 3 WHERE id = $bad_run" > /dev/null
page=$(html "/runs?pipeline=toy&status=failed")
echo "$page" | grep -qF "<th class=\"num\"><abbr title=\"Times the run's process was started: 1 is normal; more means retries, a lost instance or a resume.\"" \
    || fail "the Tries header"
echo "$page" | grep -q '>Starts<' && fail "a runs table still says Starts"
# Help on every table (orca.ui.terms): a "?" on each header it knows, and on
# a value from a fixed set (a status, a trigger, a step's kind) its meaning
# on hover or focus, without a "?" in every cell.
for p in /runs /pipelines/toy "/runs/$bad_run" "/pipelines/toy?tab=runs"; do
    pg=$(html "$p")
    [ "$(echo "$pg" | grep -o '<th[ >]' | wc -l)" -gt 0 ] || continue
    echo "$pg" | grep -q '<th>Status<span class="help"><button type="button" class="help__btn" aria-label="About Status"' \
        || echo "$pg" | grep -q '<th>Step<span class="help">' || fail "$p's headers have no help"
    [ "$(echo "$pg" | grep -o ' id="help-[^"]*"' | sort | uniq -d | wc -l)" -eq 0 ] || fail "$p has two helps with one id"
done
echo "$page" | grep -qF '<span data-tip="Something failed after its last retry. Resume runs what hadn&#39;t succeeded." tabindex="0" class="status status--failed">failed</span>' \
    || echo "$page" | grep -q '<span data-tip="Something failed after its last retry[^"]*" tabindex="0" class="status status--failed">failed</span>' \
    || fail "a status has no meaning"
echo "$page" | grep -q '<span class="term" data-tip="[^"]*" tabindex="0">manual</span>' || fail "a trigger has no meaning"
html /pipelines/toy | grep -q '<td><span class="term" data-tip="Runs once per partition, each on its own." tabindex="0">mapped</span></td>' \
    || fail "a step's kind has no meaning"
curl -fsS "$base/static/orca.css" | grep -qF '.help__tip.tip' || fail "the stylesheet doesn't place a value's meaning"
curl -fsS "$base/static/orca.css" | grep -qF '.hl--kw' || fail "the stylesheet doesn't colour code"
# /code fetches only a GitHub file at a commit, for the code shown on hover.
for u in https://evil.example/x "https://github.com/a/b/blob/main/x.mojo" "https://github.com/a/b/blob/abcdef1/../x" "https://github.com/a/b/tree/abcdef1/x"; do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$base/code?u=$(printf %s "$u" | jq -sRr @uri)")" = 400 ] || fail "/code took $u"
done
html / | grep -qF 'fetch("/code?u=" + encodeURIComponent(url))' || fail "the hover doesn't ask /code"
echo "$page" | grep -qF '<td class="num">3</td>' || fail "a run started 3 times doesn't say 3"
echo "$page" | grep -q '<th>Created<span class="help">[^h]*help__btn.*</span></span></th><th class="num">Took<span class="help">.*</th><th>Partitions<span class="help">' \
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

# The home page's Timeline tab (orca.ui.viz): List stays the default and
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
echo "$page" | grep -qE '<div id="pipelines-list" role="tabpanel"><div data-poll="/home/pipelines" data-poll-every="(5|30)"><section class="card"><div class="scroll"><table>' \
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

# A dataset's snapshots, 50 to a page by the attempt that committed each
# and then the snapshot: 59 more of raw.a, all from one attempt.
q "INSERT INTO orca.commits (attempt, table_name, snapshot_id, dataset)
   SELECT $at, 'raw/a', 100 + g, 'raw.a' FROM generate_series(1, 59) g" > /dev/null
d1=$(json /datasets/raw.a)
echo "$d1" | jq -e '(.snapshots | length) == 50 and .snapshots_page.n == 50 and .snapshots_page.prev == null
    and .snapshots_page.before == null and (.snapshots_page.next | test("^[0-9]+[.]110$"))
    and .snapshots[0].snapshot == 159' > /dev/null || fail "raw.a's first page: $(echo "$d1" | jq -c .snapshots_page)"
dc=$(echo "$d1" | jq -r .snapshots_page.next)
html /datasets/raw.a | grep -qF "<span class=\"pager__at\">50 shown</span><a rel=\"next\" href=\"/datasets/raw.a?before=$dc\">Older →</a>" \
    || fail "raw.a's Older link"
d2=$(json "/datasets/raw.a?before=$dc")
echo "$d2" | jq -e '(.snapshots | length) == 10 and .snapshots_page.next == null and .snapshots_page.prev != null
    and .snapshots[-1].snapshot == 7' > /dev/null || fail "raw.a's second page: $(echo "$d2" | jq -c .snapshots_page)"
[ "$(echo "$d1" "$d2" | jq -s '[.[].snapshots[].snapshot] | unique | length')" -eq 60 ] || fail "raw.a's pages overlap or skip"
html "/datasets/raw.a?before=$dc" | grep -qF '<a href="/datasets/raw.a">« Newest</a>' || fail "raw.a's second page doesn't link the newest"
[ "$(json "/datasets/raw.a?after=$(echo "$d2" | jq -r .snapshots_page.prev)" | jq -c '[.snapshots_page.after, [.snapshots[].snapshot]]')" \
    = "$(echo "$d1" | jq -c '[null, [.snapshots[].snapshot]]')" ] || fail "the page above raw.a's second isn't the first"
echo "ok   a dataset's snapshots, in pages"

# The Ops pages and the watchdog refresh at their own pace, from fragments.
html /ops/workers | grep -qF '<div data-poll="/ops/workers/live" data-poll-every="10"><h1>Workers</h1>' \
    || fail "Workers doesn't refresh"
curl -fsS "$base/ops/workers/live" | grep -qF '<h1>Workers</h1>' || fail "the Workers fragment"
html '/ops/control?range=7d' | grep -qF '<div data-poll="/ops/control/live?range=7d" data-poll-every="30">' \
    || fail "Control doesn't refresh"
curl -fsS "$base/ops/control/live?range=7d" > /dev/null || fail "the Control fragment"
html /watchdog | grep -qF '<div data-poll="/watchdog/live" data-poll-every="30"><h1>Watchdog</h1>' \
    || fail "the watchdog page doesn't refresh"
curl -fsS "$base/watchdog/live" | grep -qF '<h1>Watchdog</h1>' || fail "the watchdog fragment"
echo "ok   live pages: Workers, Control, the watchdog"

# The worker API on a listener of its own (ORCA_WORKER_ADDR, orca.api.worker):
# a worker's calls there, and nothing else; the UI's port keeps answering
# them too.
wtok=$(./build/orca worker-token create --pool uitest --label ui 2>> "$log") || fail "worker-token create"
wport=$((port + 3)); uport=$((port + 4))
ORCA_UI_ADDR="127.0.0.1:$uport" ORCA_WORKER_ADDR="127.0.0.1:$wport" ./build/orca ui >> "$log" 2>&1 &
wpid=$!
i=0
until curl -fs "http://127.0.0.1:$uport/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui with ORCA_WORKER_ADDR didn't start"; sleep 0.1
done
beat() { curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $2" -d '{"name": "w-ui", "slots": 0}' "$1/api/worker/claim"; }
[ "$(beat "http://127.0.0.1:$wport" "$wtok")" = 200 ] || fail "a heartbeat on the worker listener"
[ "$(beat "http://127.0.0.1:$wport" orca_nope)" = 401 ] || fail "a bad token on the worker listener"
[ "$(beat "http://127.0.0.1:$uport" "$wtok")" = 200 ] || fail "a heartbeat on the UI's port, beside a worker listener"
[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$wport/")" = 404 ] || fail "the worker listener answers a page"
[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$wport/api/pipelines/toy")" = 404 ] \
    || fail "the worker listener answers the API"
[ "$(q "SELECT name FROM orca.workers WHERE label = 'ui'")" = w-ui ] || fail "the heartbeat wasn't recorded"
kill "$wpid"
echo "ok   the worker API on ORCA_WORKER_ADDR"

kill "$pid"
echo "ok   ui"
