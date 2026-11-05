#!/bin/sh
# Retries that wait, and steps that say "not yet" (#66), over
# tests/defer_pipeline.mojo with ORCA_NOW stepping the scheduler's clock:
#
# - a step's retry waits its backoff_seconds;
# - a run's retries wait the pipeline's retry_seconds doubling, capped at
#   max_retry_seconds;
# - a step that raises defer(3600) ends its attempt `deferred`, with no
#   retry spent, and its run `deferred` until an hour later; the scheduler
#   starts it then, not before, without counting it against run_retries,
#   and the UI shows the wait; a backfill doesn't queue its key again.
#
# Needs ORCA_DATABASE_URL (pixi run test-defer starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC tests/defer_pipeline.mojo -o build/defer
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/defer.log
: > "$log"
export ORCA_STALE_SECONDS=2 DEFER_GATE="$PWD/build/defer-gate"
rm -f "$DEFER_GATE"
ui=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
epoch() { q "SELECT extract(epoch FROM '$1'::timestamptz)::bigint"; }
tick() { ORCA_NOW="$1" ./build/orca tick >> "$log" 2>&1 || fail "tick at $1 exited $?"; }
# Waits until run $1 isn't running.
settle() {
    i=0
    while [ "$(q "SELECT status FROM orca.runs WHERE id = $1")" = running ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "run $1 didn't end"; sleep 0.1
    done
}

[ "$(./build/defer describe | jq -c '[.retry_seconds, .max_retry_seconds]')" = "[10,20]" ] \
    || fail "describe: $(./build/defer describe)"

# A step retried after its backoff.
touch "$DEFER_GATE"
out=$(DEFER_FLAKY=1 ./build/defer run --partitions 2024-01 2>&1) || fail "the flaky run: $out"
echo "$out" >> "$log"
echo "$out" | grep -q "^\[fetch 2024-01\] retrying in 1 s$" || fail "no backoff said: $out"
run=$(echo "$out" | sed -n 's/^run \([0-9]*\)$/\1/p')
gap=$(q "SELECT extract(epoch FROM b.started - a.ended) FROM orca.attempts a JOIN orca.attempts b ON b.step = a.step AND b.attempt = 2 JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND s.step = 'fetch' AND a.attempt = 1")
[ "$(q "SELECT $gap >= 1")" = t ] || fail "the retry came $gap s after the failure, not 1"

ORCA_NOW=2024-03-01T00:00Z ./build/orca register ./build/defer >> "$log" 2>&1 || fail "register"

# Run retries: 10 s, then 20, then 20 again (the cap), each plus a tenth.
DEFER_FAIL=2024-02 ORCA_NOW=2024-03-01T00:00Z ./build/orca backfill deferring 2024-02 >> "$log" 2>&1 || fail "backfill 2024-02"
failing=$(q "SELECT max(id) FROM orca.runs")
t=$(epoch 2024-03-01T00:00Z)
for want in 10 20 20; do
    now=$(q "SELECT to_char(to_timestamp($t) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
    DEFER_FAIL=2024-02 tick "$now"
    settle "$failing"
    [ "$(q "SELECT status FROM orca.runs WHERE id = $failing")" = failed ] || fail "run $failing didn't fail"
    DEFER_FAIL=2024-02 tick "$now"  # gives it its retry time
    wait_s=$(q "SELECT retry_at - $t FROM orca.runs WHERE id = $failing")
    [ "$wait_s" -ge "$want" ] && [ "$wait_s" -le $((want + want / 10)) ] \
        || fail "retry $(q "SELECT starts FROM orca.runs WHERE id = $failing") waits $wait_s s, not $want"
    t=$(q "SELECT retry_at FROM orca.runs WHERE id = $failing")
done
q "UPDATE orca.runs SET status = 'cancelled' WHERE id = $failing" > /dev/null

# Deferred: 2024-03 isn't published until the gate exists.
rm -f "$DEFER_GATE"
ORCA_NOW=2024-04-01T00:00Z ./build/orca backfill deferring 2024-03 >> "$log" 2>&1 || fail "backfill 2024-03"
run=$(q "SELECT max(id) FROM orca.runs")
tick 2024-04-01T00:00Z
settle "$run"
[ "$(q "SELECT status || ' ' || retry_at || ' ' || deferrals || ' ' || starts FROM orca.runs WHERE id = $run")" \
    = "deferred $(($(epoch 2024-04-01T00:00Z) + 3600)) 1 1" ] \
    || fail "run $run: $(q "SELECT status, retry_at, deferrals, starts FROM orca.runs WHERE id = $run")"
[ "$(q "SELECT a.status || ' ' || a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND s.step = 'fetch'")" \
    = "deferred deferred until 2024-04-01T01:00:00Z: 2024-03 isn't published yet" ] \
    || fail "fetch's attempts: $(q "SELECT a.attempt, a.status, a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")"
[ "$(q "SELECT status FROM orca.steps WHERE run = $run AND step = 'total'")" = skipped ] || fail "total ran"
[ "$(ORCA_NOW=2024-04-01T00:10Z ./build/orca backfill deferring 2024-03 2>> "$log")" = "queued 0 runs" ] \
    || fail "a backfill queued the deferred run's key again"

port=$((28000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
[ "$(curl -fsS -H 'Accept: application/json' "$base/runs/$run" | jq -r .run.waiting_until)" = 2024-04-01T01:00:00Z ] \
    || fail "the UI's run: $(curl -fsS -H 'Accept: application/json' "$base/runs/$run")"
curl -fsS "$base/runs/$run" | grep -q "until 2024-04-01T01:00:00Z" || fail "the run's page doesn't say how long it waits"
kill "$ui"
ui=

# Not before its time; then again, deferred again (no retry spent).
tick 2024-04-01T00:30Z
[ "$(q "SELECT status FROM orca.runs WHERE id = $run")" = deferred ] || fail "started before its time"
tick 2024-04-01T01:00Z
settle "$run"
[ "$(q "SELECT status || ' ' || deferrals || ' ' || starts FROM orca.runs WHERE id = $run")" = "deferred 2 2" ] \
    || fail "the second start: $(q "SELECT status, deferrals, starts FROM orca.runs WHERE id = $run")"
# Published: it succeeds, past run_retries' count of starts if deferrals counted.
touch "$DEFER_GATE"
tick 2024-04-01T02:00Z
settle "$run"
[ "$(q "SELECT status || ' ' || starts FROM orca.runs WHERE id = $run")" = "succeeded 3" ] \
    || fail "run $run once published: $(q "SELECT status, starts FROM orca.runs WHERE id = $run")"
[ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND s.step = 'fetch'")" -eq 3 ] \
    || fail "fetch's attempts"
echo "ok   defer"
