#!/bin/sh
# Pausing a pipeline and cancelling its runs in bulk, from the command line
# and the UI, over examples/toy.mojo:
#
# - `orca pause`: a fire, a backfill and a retry that's due all stay queued
#   (or failed and due) through ticks; the pipeline page says paused, and
#   offers to unpause; `orca unpause` (or the UI's form) lets the scheduler
#   start them;
# - `orca cancel <pipeline> KEYS`: the runs over those keys that are queued
#   or due a retry end cancelled, the others stay; `@all` takes the rest, and
#   a running run is asked to stop before its next step; a failed run that
#   won't be retried, and a succeeded one, are left alone;
# - the pipeline page's cancel form, empty for all of them.
#
# Needs ORCA_DATABASE_URL (pixi run test-pause starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/pause.log
: > "$log"
export ORCA_LOGS=build/pause-logs
rm -rf "$ORCA_LOGS"
ui=
sched=
toy=
stop() {
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$toy" ] || kill "$toy" 2> /dev/null || true
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    stop
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
orca() { ./build/orca "$@" 2>> "$log"; }
tick() { ORCA_NOW="$1" ./build/orca tick >> "$log" 2>&1 || fail "tick at $1 exited $?"; }
status_of() { q "SELECT status FROM orca.runs WHERE pipeline = 'toy' AND partitions = '$1' ORDER BY id DESC LIMIT 1"; }
count() { q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND status = '$1'"; }

ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"

# Paused: nothing starts, but runs are still queued.
[ "$(orca pause toy)" = "paused toy" ] || fail "orca pause"
orca pause toy > /dev/null && fail "pausing a paused pipeline"
orca pause nope > /dev/null && fail "pausing no pipeline"
[ "$(orca backfill toy 2024-03..2024-06)" = "queued 4 runs" ] || fail "the backfill"
# A run that failed and is due a retry, and two that are done with.
q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, starts, retry_at)
   SELECT 'toy', registry_hash, '2024-07', 'backfill', 'failed', 1, 0 FROM orca.pipelines WHERE name = 'toy';
   INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, starts)
   SELECT 'toy', registry_hash, '2024-08', 'ui', 'failed', 1 FROM orca.pipelines WHERE name = 'toy';
   INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status, starts)
   SELECT 'toy', registry_hash, '2024-09', 'ui', 'succeeded', 1 FROM orca.pipelines WHERE name = 'toy'" > /dev/null
tick 2024-02-02T03:00Z
tick 2024-02-02T03:01Z
[ "$(count running)" -eq 0 ] || fail "a paused pipeline's run was started"
[ "$(status_of 2024-01)" = queued ] || fail "the fire wasn't queued while paused: $(status_of 2024-01)"
[ "$(count queued)" -eq 5 ] || fail "$(count queued) runs queued, not 5"
[ "$(status_of 2024-07)" = failed ] || fail "the retry was started while paused"

# The UI says so, and offers to unpause.
port=$((26000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }
# post PATH [FIELD=VALUE]: the code, with the body in $log.body.
post() {
    if [ $# -gt 1 ]; then
        curl -s -o "$log.body" -w '%{http_code}' -H "Origin: $base" -H 'Accept: application/json' \
            --data-urlencode "$2" -X POST "$base$1"
    else
        curl -s -o "$log.body" -w '%{http_code}' -H "Origin: $base" -H 'Accept: application/json' \
            -d '' -X POST "$base$1"
    fi
}
[ "$(json /pipelines/toy | jq .pipeline.paused)" = true ] || fail "the page's JSON isn't paused"
curl -fsS "$base/pipelines/toy" | grep -q 'action="/pipelines/toy/unpause"' || fail "no unpause form"
curl -fsS "$base/" | grep -q 'status--paused' || fail "the list doesn't say paused"
curl -fsS "$base/pipelines/toy?tab=operate" | grep -q 'action="/pipelines/toy/cancel"' || fail "no cancel form"
[ "$(post /pipelines/toy/pause)" = 409 ] || fail "the UI paused a paused pipeline"

# Cancel by keys: just those runs. Then @all: the queued and the retrying.
[ "$(orca cancel toy 2024-03..2024-04)" = "cancelled 2 runs" ] || fail "cancel 2024-03..2024-04"
[ "$(status_of 2024-03)" = cancelled ] && [ "$(status_of 2024-04)" = cancelled ] \
    || fail "2024-03 and 2024-04 aren't cancelled"
[ "$(status_of 2024-05)" = queued ] || fail "2024-05 was cancelled too"
orca cancel toy 2024-13 > /dev/null && fail "cancelling a bad key"
[ "$(orca cancel toy @all)" = "cancelled 4 runs" ] || fail "cancel @all"
for k in 2024-01 2024-05 2024-06 2024-07; do
    [ "$(status_of $k)" = cancelled ] || fail "$k is $(status_of $k), not cancelled"
done
[ "$(status_of 2024-08)" = failed ] || fail "a failed run that won't be retried was cancelled"
[ "$(status_of 2024-09)" = succeeded ] || fail "a succeeded run was cancelled"
[ "$(orca cancel toy @all)" = "cancelled 0 runs" ] || fail "cancel @all again"

# The UI's cancel form, empty for all of the pipeline's runs.
[ "$(orca backfill toy 2024-10..2024-11)" = "queued 2 runs" ] || fail "the second backfill"
[ "$(post /pipelines/toy/cancel partitions=)" = 200 ] || fail "the UI's cancel: $(cat "$log.body")"
[ "$(jq .cancelled "$log.body")" -eq 2 ] || fail "the UI cancelled $(cat "$log.body")"
[ "$(count queued)" -eq 0 ] || fail "the UI's cancel left runs queued"

# A running run is asked to stop, and ends cancelled before its next step.
ORCA_TOY_SLEEP=1 ./build/toy run --partitions 2024-12 >> "$log" 2>&1 &
toy=$!
i=0
until [ "$(count running)" -eq 1 ]; do
    i=$((i + 1)); [ "$i" -lt 200 ] || fail "the slow run didn't start"; sleep 0.05
done
[ "$(orca cancel toy 2024-12)" = "cancelled 1 runs" ] || fail "cancel a running run"
code=0; wait "$toy" || code=$?
toy=
[ "$code" = 1 ] || fail "the cancelled run's process exited $code"
[ "$(status_of 2024-12)" = cancelled ] || fail "the running run ended $(status_of 2024-12)"

# Unpaused from the UI, the scheduler starts what's queued.
[ "$(orca backfill toy 2024-03)" = "queued 1 runs" ] || fail "the third backfill"
tick 2024-02-02T03:02Z
[ "$(status_of 2024-03)" = queued ] || fail "started while still paused"
[ "$(post /pipelines/toy/unpause)" = 200 ] || fail "the UI's unpause: $(cat "$log.body")"
[ "$(json /pipelines/toy | jq .pipeline.paused)" = false ] || fail "still paused"
[ "$(post /pipelines/toy/unpause)" = 409 ] || fail "unpaused an unpaused pipeline"
ORCA_NOW=2024-02-02T03:03Z ./build/orca scheduler >> "$log" 2>&1 &
sched=$!
i=0
until [ "$(status_of 2024-03)" = succeeded ]; do
    i=$((i + 1)); [ "$i" -lt 300 ] || fail "the queued run is $(status_of 2024-03) after unpausing"; sleep 0.2
done
kill "$sched"; wait "$sched" 2> /dev/null || true; sched=
[ "$(orca pause toy)" = "paused toy" ] && [ "$(orca unpause toy)" = "unpaused toy" ] || fail "pause, unpause"

stop
echo "ok   pause"
