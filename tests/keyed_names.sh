#!/bin/sh
# Keyed partitions named with a `/`, as repositories are (`owner/name`):
# tests/keyed/slashes.mojo over keys.slashed, names org/repo-00 to -04.
#
# - `keys` prints them; `run` takes them, in a list, from a file and as @all;
# - a `/` at either end or two in a row is refused with exit code 2;
# - with MORCA_LOGS, an attempt's log is one directory per name, the `/`
#   written `%2F`, and nothing escapes the step's directory;
# - `morca backfill` takes them, and the UI's key page answers at the name
#   with `%2F`, and the pipeline page links a failed one there.
#
# Needs MORCA_DATABASE_URL (pixi run -e data test-keyed-names starts a
# throwaway server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# The pipelines and morca compile at once (scripts/mojo-build.sh).
pids=
for p in names slashes; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src $DEP_INC "tests/keyed/$p.mojo" -o "build/kn-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
for pid in $pids $!; do wait "$pid"; done
log=build/keyed-names.log
: > "$log"
export MORCA_WAREHOUSE=build/keyed-names-wh MORCA_LOGS="$(pwd)/build/keyed-names-logs" MORCA_STALE_SECONDS=10
rm -rf "$MORCA_WAREHOUSE" "$MORCA_LOGS"
sched=
ui=
stop() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    sched=
    ui=
}
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    stop
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
slashes() { ./build/kn-slashes "$@" 2>> "$log"; }

./build/kn-names "$MORCA_WAREHOUSE" 5 slashed 0 2 org/repo- > /dev/null || fail "names"
want=$(printf 'org/repo-0%s\n' 0 1 2 3 4)
[ "$(slashes keys | tail -n +2)" = "$want" ] || fail "keys: $(slashes keys | tr '\n' ' ')"

# In memory: @all, a list, a file.
out=$(env -u MORCA_DATABASE_URL -u MORCA_LOGS ./build/kn-slashes run --partitions @all 2>> "$log") || fail "run @all"
[ "$(echo "$out" | grep -c '^touch org/repo-0[0-4] succeeded$')" -eq 5 ] || fail "@all: $out"
printf 'org/repo-04\norg/repo-00\n' > build/keyed-names-list.txt
out=$(env -u MORCA_DATABASE_URL -u MORCA_LOGS ./build/kn-slashes run --partitions @file:build/keyed-names-list.txt 2>> "$log") \
    || fail "run @file"
[ "$(echo "$out" | grep '^touch ' | tr '\n' ' ')" = "touch org/repo-04 succeeded touch org/repo-00 succeeded " ] \
    || fail "the file's two: $out"

# Refused with exit code 2.
for bad in /org/repo-01 org/repo-01/ org//repo-01 org/../repo-01; do
    code=0
    env -u MORCA_DATABASE_URL ./build/kn-slashes run --partitions "$bad" > build/keyed-names-bad.txt 2>&1 || code=$?
    [ "$code" -eq 2 ] || fail "--partitions '$bad' exited $code: $(cat build/keyed-names-bad.txt)"
done

# Recorded, with a log per name in one directory each.
out=$(slashes run --partitions org/repo-01,org/repo-03) || fail "a recorded run: $out"
run=$(echo "$out" | sed -n 's/^run \([0-9]*\)$/\1/p' | head -1)
[ "$(q "SELECT partitions FROM orca.runs WHERE id = $run")" = "org/repo-01,org/repo-03" ] || fail "run $run's partitions"
dir="$MORCA_LOGS/slashes/$run/touch"
for k in 01 03; do
    grep -q "touched org/repo-$k" "$dir/org%2Frepo-$k/1.log" || fail "the log of org/repo-$k: $(ls -R "$dir")"
done
[ "$(ls "$dir" | sort | tr '\n' ' ')" = "org%2Frepo-01 org%2Frepo-03 " ] || fail "the step's directory: $(ls "$dir")"
[ "$(q "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND a.log LIKE '%/org\%2Frepo-0_/1.log'")" -eq 2 ] \
    || fail "the attempts' log paths: $(q "SELECT a.log FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")"

# Backfilled by the scheduler, with one told to fail.
./build/morca register ./build/kn-slashes >> "$log" 2>&1 || fail "register"
MORCA_SLASH_FAIL=org/repo-02 ./build/morca scheduler >> "$log" 2>&1 &
sched=$!
out=$(./build/morca backfill slashes @all 2> build/keyed-names-backfill.err) \
    || fail "the backfill exited $?: $out $(cat build/keyed-names-backfill.err)"
[ "$out" = "queued 1 runs" ] || fail "the backfill: $out"
# Why the rest weren't queued goes to stderr (#61).
grep -qx "skipped 2 done: org/repo-01, org/repo-03" build/keyed-names-backfill.err \
    || fail "the backfill's skipped keys: $(cat build/keyed-names-backfill.err)"
i=0
until [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'slashes' AND status IN ('queued', 'running')")" -eq 0 ]; do
    i=$((i + 1)); [ "$i" -lt 1200 ] || fail "the backfill's run didn't finish"; sleep 0.1
done
[ "$(q "SELECT partitions FROM orca.runs WHERE pipeline = 'slashes' ORDER BY id DESC LIMIT 1")" = "org/repo-00,org/repo-02,org/repo-04" ] \
    || fail "the backfill's keys: $(q "SELECT partitions FROM orca.runs WHERE pipeline = 'slashes' ORDER BY id DESC LIMIT 1")"

# The UI: a key's page at its name with %2F, and the failed one linked there.
free_port port
base="http://127.0.0.1:$port"
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
done
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }
[ "$(json /pipelines/slashes | jq -c '.keys | [.total, .done, .failed]')" = '[5,4,["org/repo-02"]]' ] \
    || fail "the keys: $(json /pipelines/slashes | jq -c .keys)"
curl -fsS "$base/pipelines/slashes" | grep -qF 'href="/pipelines/slashes/keys/org%2Frepo-02"' \
    || fail "the pipeline page doesn't link org/repo-02's page"
[ "$(json /pipelines/slashes/keys/org%2Frepo-02 | jq -c '[.done, [.steps[] | "\(.step):\(.status)"]]')" = '[false,["touch:failed"]]' ] \
    || fail "org/repo-02's page: $(json /pipelines/slashes/keys/org%2Frepo-02)"
[ "$(json /pipelines/slashes/keys/org%2Frepo-01 | jq -c '[.done, [.steps[] | .status]]')" = '[true,["succeeded"]]' ] \
    || fail "org/repo-01's page: $(json /pipelines/slashes/keys/org%2Frepo-01)"
stop
echo "ok   keyed names with a /"
