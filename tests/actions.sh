#!/bin/sh
# Phase 3's gate, through HTTP only: a toy run fails a step; it's found on
# /runs, its failed attempt's log read, and resume posted; with the scheduler
# running, the run succeeds. Then the other actions: rerun from `measure`
# reruns measure and total and not days; a run started and a range
# backfilled from the UI run; a running run cancelled ends `cancelled`
# before its next step, and resumes onto new code once that's confirmed; a
# cross-origin POST changes nothing.
# Needs ORCA_DATABASE_URL (pixi run test-actions starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca

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

port=$((20000 + $$ % 2000))
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
curl -fsS "$base/pipelines/toy" | grep -q 'action="/pipelines/toy/runs"' || fail "no start form"
[ "$(post /pipelines/toy/runs partitions=2024-03..2024-04)" = 303 ] || fail "start: $(cat "$log.body")"
started=$(json '/runs?trigger=ui' | jq -r '.runs[0].id')
[ "$(json "/runs/$started" | jq -r .run.partitions)" = 2024-03,2024-04 ] || fail "the started run's partitions"
until_status "$started" succeeded
[ "$(post /pipelines/toy/runs partitions=2023-13)" = 409 ] || fail "a bad partition was started"
grep -q 'Not done' "$log.body" || fail "the refusal page"
[ "$(post /pipelines/toy/backfill partitions=2024-05..2024-06)" = 303 ] || fail "backfill: $(cat "$log.body")"
[ "$(json '/runs?trigger=backfill' | jq '.runs | length')" -eq 2 ] || fail "backfill didn't queue 2024-05 and 2024-06"
code=$(curl -s -o "$log.body" -w '%{http_code}' -H 'Accept: application/json' -H "Origin: $base" \
    -X POST --data-urlencode partitions=2024-05..2024-06 "$base/pipelines/toy/backfill")
[ "$code" = 200 ] && [ "$(jq .queued "$log.body")" -eq 0 ] || fail "backfilling it again queued $(cat "$log.body")"
for r in $(json '/runs?trigger=backfill' | jq -r '.runs[].id'); do until_status "$r" succeeded; done

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
