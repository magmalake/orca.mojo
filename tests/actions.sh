#!/bin/sh
# Phase 3's gate, through HTTP only: a toy run fails a step; it's found on
# /runs, its failed attempt's log read, and resume posted; with the scheduler
# running, the run succeeds. Then the other actions: rerun from `measure`
# reruns measure and total and not days; a run started and a range
# backfilled from the UI run, and backfilled again with rerun=1; a running
# run cancelled ends `cancelled` before its next step, and resumes onto new
# code once that's confirmed; a cross-origin POST changes nothing.
# Needs ORCA_DATABASE_URL (pixi run test-actions starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/actions.log
: > "$log"
export ORCA_LOGS=build/actions-logs
rm -rf "$ORCA_LOGS"
# The scheduler's clock stays where the toy was registered, so it makes no
# fires of its own: every run it starts is one the UI queued.
export ORCA_NOW=2024-01-15T00:00Z
ui=
sched=
stop() {
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    stop
    exit 1
}

./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
ORCA_TOY_FAIL=2024-02 ./build/toy run --partitions 2024-01..2024-02 >> "$log" 2>&1 \
    && fail "the run didn't fail"

free_port port
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done

json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }
# post PATH [FIELD=VALUE...]: the code, with the body in $log.body.
post() {
    path=$1; shift
    set -- "$@" ""
    form=
    for f in "$@"; do [ -z "$f" ] || form="$form --data-urlencode $f"; done
    # shellcheck disable=SC2086
    curl -s -o "$log.body" -w '%{http_code}' -H "Origin: $base" -X POST $form "$base$path"
}
# until_status RUN STATUS: waits up to a minute for the run to get there.
until_status() {
    i=0
    until [ "$(json "/runs/$1" | jq -r .run.status)" = "$2" ]; do
        i=$((i + 1))
        [ "$i" -lt 300 ] || fail "run $1 is $(json "/runs/$1" | jq -r .run.status), not $2"
        sleep 0.2
    done
}

# The failed run, found on /runs, and its failed attempt's log.
run=$(json '/runs?status=failed' | jq -r '.runs[0].id')
[ "$run" != null ] || fail "no failed run on /runs"
attempt=$(json "/runs/$run" | jq -r '[.steps[] | select(.status == "failed")][0].attempt_id')
json "/runs/$run/attempts/$attempt" | jq -r .text | grep -q 'failed: failing 2024-02 as asked' \
    || fail "the failed attempt's log"
curl -fsS "$base/runs/$run" | grep -q "action=\"/runs/$run/resume\"" || fail "no resume form"

# A POST from another site, or with neither Origin nor Referer, is refused.
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Origin: http://evil.example' -X POST "$base/runs/$run/resume")
[ "$code" = 403 ] || fail "a cross-origin resume answered $code"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$base/runs/$run/cancel")
[ "$code" = 403 ] || fail "a POST without Origin or Referer answered $code"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Referer: http://evil.example/runs/$run" -X POST "$base/runs/$run/cancel")
[ "$code" = 403 ] || fail "a POST with another site's Referer answered $code"
[ "$(json "/runs/$run" | jq -r .run.status)" = failed ] || fail "a refused POST changed the run"
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Referer: $base/runs/$run" -X POST "$base/runs/999999/cancel")
[ "$code" = 409 ] || fail "a same-site Referer answered $code"

# Resume: a browser is sent to the run, which is queued until the scheduler
# starts it.
[ "$(post "/runs/$run/resume")" = 303 ] || fail "resume answered $(cat "$log.body")"
[ "$(json "/runs/$run" | jq -r .run.status)" = queued ] || fail "the resumed run isn't queued"
[ "$(post "/runs/$run/resume")" = 409 ] || fail "a queued run was resumed"
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
until_status "$run" succeeded
[ "$(json "/runs/$run" | jq -r '.steps[] | select(.step == "measure" and .partition == "2024-02") | .attempts')" -eq 2 ] \
    || fail "measure 2024-02 wasn't attempted again"
[ "$(json "/runs/$run" | jq -r '.steps[] | select(.step == "days") | .attempts' | sort -u)" -eq 1 ] \
    || fail "days ran again on resume"

# Rerun from measure: a new run where days is copied and the rest runs.
code=$(curl -s -o "$log.body" -w '%{http_code}' -H 'Accept: application/json' -H "Origin: $base" \
    -X POST --data-urlencode step=measure "$base/runs/$run/rerun")
[ "$code" = 200 ] || fail "rerun answered $code: $(cat "$log.body")"
rerun=$(jq -r .run "$log.body")
until_status "$rerun" succeeded
[ "$(json "/runs/$rerun" | jq -r .run.trigger)" = rerun ] || fail "the rerun's trigger"
got=$(json "/runs/$rerun" | jq -r '[.steps[] | "\(.step) \(.partition) \(.attempts) \(.from_run)"] | join(",")')
want="days 2024-01 0 $run,days 2024-02 0 $run,measure 2024-01 1 0,measure 2024-02 1 0,total  1 0"
[ "$got" = "$want" ] || fail "the rerun's steps: $got"
[ "$(post "/runs/$run/rerun" step=nope)" = 409 ] || fail "a rerun from no step"

# Start a run and backfill a range from the pipeline's page.
curl -fsS "$base/pipelines/toy?tab=operate" | grep -q 'action="/pipelines/toy/runs"' || fail "no start form"
[ "$(post /pipelines/toy/runs partitions=2024-03..2024-04)" = 303 ] || fail "start: $(cat "$log.body")"
started=$(json '/runs?trigger=ui' | jq -r '.runs[0].id')
[ "$(json "/runs/$started" | jq -r .run.partitions)" = 2024-03,2024-04 ] || fail "the started run's partitions"
until_status "$started" succeeded
[ "$(post /pipelines/toy/runs partitions=2023-13)" = 409 ] || fail "a bad partition was started"
grep -qF '<a href="/pipelines/toy?tab=operate">Back</a>' "$log.body" || fail "a refused start doesn't go back to Operate"
grep -q 'Not done' "$log.body" || fail "the refusal page"
[ "$(post /pipelines/toy/backfill partitions=2024-05..2024-06)" = 303 ] || fail "backfill: $(cat "$log.body")"
[ "$(json '/runs?trigger=backfill' | jq '.runs | length')" -eq 2 ] || fail "backfill didn't queue 2024-05 and 2024-06"
code=$(curl -s -o "$log.body" -w '%{http_code}' -H 'Accept: application/json' -H "Origin: $base" \
    -X POST --data-urlencode partitions=2024-05..2024-06 "$base/pipelines/toy/backfill")
[ "$code" = 200 ] && [ "$(jq .queued "$log.body")" -eq 0 ] || fail "backfilling it again queued $(cat "$log.body")"
for r in $(json '/runs?trigger=backfill' | jq -r '.runs[].id'); do until_status "$r" succeeded; done
# Again with rerun=1, as the form's checkbox sends: the done ones too.
code=$(curl -s -o "$log.body" -w '%{http_code}' -H 'Accept: application/json' -H "Origin: $base" \
    -X POST --data-urlencode partitions=2024-05..2024-06 --data-urlencode rerun=1 "$base/pipelines/toy/backfill")
[ "$code" = 200 ] && [ "$(jq .queued "$log.body")" -eq 2 ] || fail "backfilling with rerun=1 queued $(cat "$log.body")"
curl -fsS "$base/pipelines/toy?tab=operate" | grep -q 'name="rerun"' || fail "no rerun checkbox on the backfill form"
for r in $(json '/runs?trigger=backfill' | jq -r '.runs[].id'); do until_status "$r" succeeded; done

# The Start runs form: one range, the backfill first, and a preview (a GET)
# that counts what each button would queue, with the same logic, queueing
# nothing; and the same from the command line.
page=$(curl -fsS "$base/pipelines/toy?tab=operate")
echo "$page" | grep -qF '<button type="submit" class="primary">Backfill: one run per month</button>' \
    || fail "the backfill button"
echo "$page" | grep -qF 'formaction="/pipelines/toy/runs">Start one run over the whole range</button>' \
    || fail "the one-run button"
echo "$page" | grep -qF ' Include months that already succeeded (rerun them)</label>' || fail "the rerun label"
echo "$page" | grep -qF '<code data-cmd="backfill">orca login &lt;url&gt; &amp;&amp; orca --remote &lt;url&gt; backfill toy &lt;range&gt;</code>' || fail "the empty form's command"
echo "$page" | grep -qF '</code></pre><button type="button" class="copy" data-copy>Copy</button></div>' || fail "the command's Copy button"
before=$(psql "$ORCA_DATABASE_URL" -Atc "SELECT count(*), max(id) FROM orca.runs")
p=$(json '/pipelines/toy?preview=1&partitions=2024-05..2024-08')
echo "$p" | jq -e '.preview | .runs == 2 and .keys == ["2024-07", "2024-08"] and .done == ["2024-05", "2024-06"]
    and .one_run == 4 and .refused == null
    and .commands.backfill == "orca backfill toy 2024-05..2024-08"
    and .commands.run == "\(.commands.run | split(" ")[0]) run --partitions 2024-05..2024-08"' > /dev/null \
    || fail "the preview: $(echo "$p" | jq -c .preview)"
json '/pipelines/toy?preview=1&partitions=2024-05..2024-08&rerun=1' | jq -e '.preview | .runs == 4 and .done == []
    and .commands.backfill == "orca backfill --rerun toy 2024-05..2024-08"' > /dev/null || fail "the rerun preview"
page=$(curl -fsS "$base/pipelines/toy?preview=1&partitions=2024-05..2024-08")
echo "$page" | grep -qF '<p data-preview="backfill">This queues 2 runs: 2024-07 … 2024-08. 2 already done are skipped.</p>' \
    || fail "the preview line"
echo "$page" | grep -qF 'One run over the whole range runs all 4 months, done or not.' || fail "the one-run line"
echo "$page" | grep -qF 'name="partitions" required placeholder="2024-01..2024-01" value="2024-05..2024-08">' \
    || fail "the preview doesn't keep the range"
echo "$page" | grep -qF '<code data-cmd="backfill">orca login &lt;url&gt; &amp;&amp; orca --remote &lt;url&gt; backfill toy 2024-05..2024-08</code>' || fail "the preview's command"
curl -fsS "$base/pipelines/toy?preview=1&partitions=2024-05&rerun=1" | grep -qF 'name="rerun" value="1" checked>' \
    || fail "the preview doesn't keep rerun"
curl -fsS "$base/pipelines/toy?preview=1&partitions=2023-13" | grep -q '<p class="refused">' || fail "a bad range's preview"
[ "$(psql "$ORCA_DATABASE_URL" -Atc "SELECT count(*), max(id) FROM orca.runs")" = "$before" ] \
    || fail "a preview queued runs"
echo "ok   the Start runs form's preview"

# Partitions from a file are the CLI's: the UI refuses @file: with a 400 and
# reads nothing, so a logged-in user can't make it echo a file's lines. The
# keyed pipeline is a stand-in whose `keys` prints two names.
printf '#!/bin/sh\n[ "$1" = keys ] && printf "1\\nalice\\nbob\\n"\n' > build/actions-keyed.sh
chmod +x build/actions-keyed.sh
psql "$ORCA_DATABASE_URL" -qc "INSERT INTO orca.pipelines (name, registry_hash, describe, binary_path)
    VALUES ('names', 'k', '{\"pipeline\": \"names\", \"partitions\": {\"kind\": \"keyed\",
    \"dataset\": \"people.names\"}, \"steps\": []}', '$PWD/build/actions-keyed.sh')" > /dev/null
[ "$(post /pipelines/names/runs partitions=carol)" = 409 ] || fail "a key the stand-in hasn't was started"
grep -q 'a key of people.names' "$log.body" || fail "the keyed refusal: $(cat "$log.body")"
# Keyed pipelines keep the text form: no calendar, no coverage.
page=$(curl -fsS "$base/pipelines/names?tab=operate")
echo "$page" | grep -qF 'name="partitions"' || fail "the keyed pipeline has no text field"
echo "$page" | grep -qF 'data-calendar=' && fail "a keyed pipeline has a calendar picker"
echo "$page" | grep -qF 'id="coverage"' && fail "a keyed pipeline has coverage"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/pipelines/names/calendar")" = 400 ] || fail "a keyed pipeline's calendar"
printf 'secret value, line one\nalice\n' > build/actions-secret.txt
for action in runs backfill; do
    code=$(post "/pipelines/names/$action" "partitions=@file:$PWD/build/actions-secret.txt")
    [ "$code" = 400 ] || fail "$action with @file: answered $code: $(cat "$log.body")"
    grep -q 'secret value' "$log.body" && fail "$action with @file: echoed the file"
    code=$(post "/pipelines/toy/$action" partitions=@file:/etc/passwd)
    [ "$code" = 400 ] || fail "$action of toy with @file: answered $code"
done
[ "$(json '/runs?pipeline=names' | jq '.runs | length')" -eq 0 ] || fail "@file: queued a run"
# A preview of @file: is refused too, and keyed commands quote what the
# shell would split.
p=$(json "/pipelines/names?preview=1&partitions=@file:$PWD/build/actions-secret.txt")
echo "$p" | grep -q 'secret value' && fail "a preview of @file: echoed the file"
echo "$p" | jq -e '.preview.refused | test("@file:")' > /dev/null || fail "a preview of @file: wasn't refused"
json '/pipelines/names?preview=1&partitions=alice,bob' | jq -e '.preview | .runs >= 1 and .keys == ["alice", "bob"]
    and .commands.backfill == "orca backfill names alice,bob"' > /dev/null \
    || fail "a keyed preview: $(json '/pipelines/names?preview=1&partitions=alice,bob' | jq -c .preview)"
want="orca backfill names 'alice bob;x'"
got=$(json "/pipelines/names?preview=1&partitions=alice%20bob%3Bx" | jq -r .preview.commands.backfill)
[ "$got" = "$want" ] || fail "a keyed command isn't quoted: $got"
curl -fsS "$base/pipelines/names?tab=operate" | grep -qF 'Include keys that already succeeded (rerun them)' || fail "the keyed rerun label"

# Cancel a queued run: it never starts.
kill "$sched"; wait "$sched" 2> /dev/null || true; sched=
[ "$(post /pipelines/toy/runs partitions=2024-07)" = 303 ] || fail "start 2024-07"
queued=$(json '/runs?trigger=ui' | jq -r '.runs[0].id')
[ "$(post "/runs/$queued/cancel")" = 303 ] || fail "cancel a queued run"
[ "$(json "/runs/$queued" | jq -r .run.status)" = cancelled ] || fail "the queued run isn't cancelled"
[ "$(post "/runs/$queued/cancel")" = 409 ] || fail "a cancelled run was cancelled again"

# Cancel a running run: it stops before its next step and ends cancelled.
ORCA_TOY_SLEEP=1 ./build/toy run --partitions 2024-08..2024-09 >> "$log" 2>&1 &
toy=$!
i=0
until slow=$(json '/runs?status=running' | jq -r '.runs[0].id // empty') && [ -n "$slow" ]; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the slow run didn't start"; sleep 0.05
done
[ "$(post "/runs/$slow/cancel")" = 303 ] || fail "cancel a running run"
[ "$(json "/runs/$slow" | jq -r .run.cancel_requested)" = true ] || fail "cancel_requested"
code=0; wait "$toy" || code=$?
[ "$code" = 1 ] || fail "the cancelled run's process exited $code"
[ "$(json "/runs/$slow" | jq -r .run.status)" = cancelled ] || fail "the running run didn't end cancelled"
[ "$(json "/runs/$slow" | jq '[.steps[] | select(.step == "total")] | length')" -eq 0 ] \
    || fail "the cancelled run went on to total"
[ "$(json "/runs/$slow" | jq '[.steps[] | select(.status == "running")] | length')" -eq 0 ] \
    || fail "the cancelled run left a step running"

# Resume it onto new code: refused until confirmed, then the scheduler
# passes --allow-new-code and it finishes.
psql "$ORCA_DATABASE_URL" -qc "UPDATE orca.runs SET registry_hash = 'old' WHERE id = $slow" > /dev/null
[ "$(post "/runs/$slow/resume")" = 409 ] || fail "a resume onto new code wasn't refused"
grep -q 'name="allow_new_code"' "$log.body" || fail "the refusal doesn't ask to confirm"
[ "$(post "/runs/$slow/resume" allow_new_code=1)" = 303 ] || fail "the confirmed resume"
./build/orca scheduler >> "$log" 2>&1 &
sched=$!
until_status "$slow" succeeded

stop
echo "ok   actions"
