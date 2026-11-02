#!/bin/sh
# orca ui over the toy pipeline's runs: every page answers HTML and JSON, a
# run's JSON says what `toy status <run>` says, values from the database are
# escaped, and unknown paths and methods are refused. Needs
# ORCA_DATABASE_URL (pixi run test-ui starts a throwaway server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca

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
echo "$page" | grep -q '<script>' && fail "a value's markup reached the page"
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

kill "$pid"
echo "ok   ui"
