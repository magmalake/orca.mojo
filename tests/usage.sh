#!/bin/sh
# What a run's processes use (orca.usage), over tests/usage/hog.mojo, whose
# step holds 200 MiB while a child of its own holds 100 MiB more, each
# burning a core for 4 s:
#
# - a run on the local runner and a control-placed one, which the scheduler
#   samples each tick, and one on `orca worker`, which samples it and sends
#   the samples with /api/worker/exited, each record on their attempt a
#   peak within a margin of what was held (on Linux the tree's, both
#   processes; on macOS wait4's largest process), their CPU seconds, and
#   on Linux the most cores they kept busy;
# - the run's page, the attempt's and the pipeline's (its Resources card:
#   p50 and max of each) show them, and their JSON has them; `orca status
#   hog` and `hog status <run>` print them;
# - from the three, the request they suggest (orca.recommend): the largest
#   peak plus 15% in quarter GBs, on the card, in the JSON and in `orca
#   config show`, and applied nowhere.
#
# Needs ORCA_DATABASE_URL (pixi run test-usage starts a throwaway server),
# curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC tests/usage/hog.mojo -o build/hog &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
b2=$!
wait "$b1"
wait "$b2"
log=build/usage.log
: > "$log"
rm -rf build/usage-stage build/usage-*.out
export ORCA_STALE_SECONDS=5 ORCA_START_SECONDS=20 ORCA_SAMPLE_SECONDS=0
export ORCA_CONTROL_MEMORY_MB=2048
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/usage-*.out; do [ -f "$f" ] && { echo "== $f"; tail -15 "$f"; }; done
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
is() { [ "$(q "$1")" = "$2" ]; }
linux=false
[ "$(uname)" = Linux ] && linux=true
MB=1048576

# check RUN WHERE: the run succeeded, and its one attempt's usage is within
# the margins.
check() {
    run=$1; where=$2
    until_ 60 "$where: run $run didn't succeed: $(q "SELECT id, status, error FROM orca.runs WHERE id = $run")" \
        is "SELECT status FROM orca.runs WHERE id = $run" succeeded
    # Recorded when the process is reaped, just after it ends.
    until_ 10 "$where: no usage: $(q "SELECT * FROM orca.attempts")" \
        is "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run AND a.cpu_seconds IS NOT NULL" 1
    row=$(q "SELECT a.peak_rss_bytes, a.cpu_seconds, coalesce(a.peak_cpus, -1) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $run")
    echo "$where: run $run: $row" >> "$log"
    peak=$(echo "$row" | cut -d'|' -f1)
    cpu=$(echo "$row" | cut -d'|' -f2)
    cores=$(echo "$row" | cut -d'|' -f3)
    if $linux; then
        # Both processes' 300 MiB, sampled together, and the runtimes'.
        lo=$((300 * MB)); hi=$((500 * MB))
    else
        # wait4's largest process: the step's 200 MiB.
        lo=$((200 * MB)); hi=$((400 * MB))
    fi
    [ "$peak" -ge "$lo" ] && [ "$peak" -le "$hi" ] \
        || fail "$where: run $run's peak $peak isn't within $lo..$hi"
    awk -v c="$cpu" 'BEGIN { exit !(c >= 6 && c <= 14) }' \
        || fail "$where: run $run's CPU $cpu s isn't within 6..14 (2 cores for 4 s)"
    if $linux; then
        awk -v c="$cores" 'BEGIN { exit !(c >= 1.3 && c <= 3.0) }' \
            || fail "$where: run $run's peak cores $cores isn't within 1.3..3.0"
    else
        [ "$cores" = -1 ] || fail "$where: macOS gave run $run peak cores: $cores"
    fi
}

free_port port
base="http://127.0.0.1:$port"
./build/orca register ./build/hog >> "$log" 2>&1 || fail "register"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "orca ui didn't start" curl -fs "$base/" -o /dev/null
./build/orca scheduler > build/usage-sched.out 2>&1 &
pids="$pids $!"

# On the local runner.
./build/orca backfill hog 2024-01 >> "$log" 2>&1 || fail "backfill 2024-01"
r1=$(q "SELECT max(id) FROM orca.runs")
check "$r1" local

# On the control host.
./build/orca register --placement control ./build/hog >> "$log" 2>&1 || fail "register control"
./build/orca backfill hog 2024-02 >> "$log" 2>&1 || fail "backfill 2024-02"
r2=$(q "SELECT max(id) FROM orca.runs")
check "$r2" control
is "SELECT placement FROM orca.runs WHERE id = $r2" control \
    || fail "run $r2 wasn't placed on the control host: $(q "SELECT id, placement FROM orca.runs")"

# On a remote worker, whose samples reach the database through the UI.
./build/orca register --placement worker:u ./build/hog >> "$log" 2>&1 || fail "register worker"
tok=$(./build/orca worker-token create --pool u --label usage 2>> "$log") || fail "worker-token create"
./build/orca backfill hog 2024-03 >> "$log" 2>&1 || fail "backfill 2024-03"
r3=$(q "SELECT max(id) FROM orca.runs")
env -u ORCA_DATABASE_URL -u ORCA_PGDATA ORCA_WORKER_TOKEN="$tok" \
    ./build/orca worker --pool u --remote "$base" --poll-seconds 0.3 \
    --dir "$PWD/build/usage-stage" > build/usage-worker.out 2>&1 &
pids="$pids $!"
check "$r3" worker
is "SELECT placement FROM orca.runs WHERE id = $r3" worker:u \
    || fail "run $r3 didn't run on the worker: $(q "SELECT id, placement FROM orca.runs")"

# The pages and their JSON.
curl -fs "$base/runs/$r1" > build/usage-run.out || fail "GET /runs/$r1"
grep -q 'Peak memory<span class="help">' build/usage-run.out || fail "the run's page has no Peak memory"
grep -q 'CPU used<span class="help">' build/usage-run.out || fail "the run's page has no CPU used"
curl -fs -H 'Accept: application/json' "$base/runs/$r1" > build/usage-run.json || fail "GET /runs/$r1 JSON"
jq -e '.run.usage.peak_rss_bytes > 0 and .run.usage.cpu_seconds > 0 and .steps[0].usage.peak_rss_bytes == .run.usage.peak_rss_bytes' \
    build/usage-run.json > /dev/null || fail "the run's JSON: $(cat build/usage-run.json)"
a1=$(q "SELECT a.id FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $r1")
curl -fs -H 'Accept: application/json' "$base/runs/$r1/attempts/$a1" > build/usage-attempt.json \
    || fail "GET the attempt's JSON"
jq -e '.attempt.usage.peak_rss_bytes > 0' build/usage-attempt.json > /dev/null \
    || fail "the attempt's JSON: $(cat build/usage-attempt.json)"
curl -fs "$base/runs/$r1/attempts/$a1" | grep -q 'Peak memory<span class="help">' \
    || fail "the attempt's page has no Peak memory"
curl -fs "$base/pipelines/hog" > build/usage-pipeline.out || fail "GET /pipelines/hog"
grep -q 'id="resources"><h2>Resources' build/usage-pipeline.out || fail "the pipeline's page has no Resources card"
grep -q 'its last 3 runs' build/usage-pipeline.out || fail "the Resources card: $(grep -o 'id="resources".*' build/usage-pipeline.out | head -c 2000)"
curl -fs -H 'Accept: application/json' "$base/pipelines/hog" > build/usage-pipeline.json \
    || fail "GET /pipelines/hog JSON"
jq -e '.usage.runs == 3 and .usage.peak_rss_bytes.max >= .usage.peak_rss_bytes.p50 and .usage.peak_rss_bytes.p50 > 0 and .usage.cpu_seconds.max > 0' \
    build/usage-pipeline.json > /dev/null || fail "the pipeline's JSON: $(jq .usage build/usage-pipeline.json)"
if $linux; then
    jq -e '.usage.peak_cpus.max > 1' build/usage-pipeline.json > /dev/null \
        || fail "the pipeline's peak cores: $(jq .usage build/usage-pipeline.json)"
fi
# The request learned from them: of three runs, p95 is the largest.
grep -qE 'Learned<span class="help">.*learned from 3 runs: [0-9.]+ GB' build/usage-pipeline.out \
    || fail "the Resources card's Learned: $(grep -o 'id="resources".*' build/usage-pipeline.out | head -c 3000)"
jq -e '.learned.runs == 3 and .learned.applied == false and .learned.clamped == null
    and .learned.memory_mb == ((.usage.peak_rss_bytes.max / 1048576 * 1.15 / 256 | ceil) * 256)' \
    build/usage-pipeline.json > /dev/null || fail "the pipeline's learned: $(jq '{usage, learned}' build/usage-pipeline.json)"
if $linux; then
    jq -e '.learned.cpus >= 1 and .learned.cpus <= 3' build/usage-pipeline.json > /dev/null \
        || fail "the learned CPUs: $(jq .learned build/usage-pipeline.json)"
else
    jq -e '.learned.cpus == null' build/usage-pipeline.json > /dev/null \
        || fail "macOS learned CPUs: $(jq .learned build/usage-pipeline.json)"
fi

# What the commands print.
./build/orca status hog > build/usage-status.out 2>&1 || fail "orca status hog"
grep -qE "^  $r1 succeeded backfill 2024-01 .* peak [0-9]+ MB, CPU [0-9.]+ s" build/usage-status.out \
    || fail "orca status: $(cat build/usage-status.out)"
./build/hog status "$r2" > build/usage-binary.out 2>&1 || fail "hog status"
grep -qE "^hog 2024-02 succeeded \(peak [0-9]+ MB, CPU [0-9.]+ s" build/usage-binary.out \
    || fail "hog status: $(cat build/usage-binary.out)"
./build/orca config show hog > build/usage-config.out 2>&1 || fail "orca config show hog"
grep -qE "^  learned from 3 runs: [0-9.]+ GB.*not applied \(#269\)$" build/usage-config.out \
    || fail "config show: $(cat build/usage-config.out)"
[ "$(./build/orca config show hog --json | jq -r .learned.memory_mb)" = "$(jq -r .learned.memory_mb build/usage-pipeline.json)" ] \
    || fail "config show --json: $(./build/orca config show hog --json)"
echo "usage: ok"
