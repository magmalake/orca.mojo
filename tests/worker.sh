#!/bin/sh
# Remote workers (orca.worker, orca.workers, orca.http_store): orca ui, the
# scheduler and `orca worker --pool t`, whose environment has no
# ORCA_DATABASE_URL, over tests/worker/wk.mojo placed worker:t:
#
# - the scheduler starts none of its runs: they wait, held for a worker;
#   a worker with ORCA_DATABASE_URL set refuses to start;
# - the worker claims them (audited), and their steps, attempts and
#   heartbeats reach Postgres through the UI's API; Ops and the run's page
#   say where they ran;
# - killing the worker (its exact pid) stops its runs' processes, and the
#   scheduler requeues the runs once their heartbeats are stale;
# - a token for another pool can neither claim them nor make their store
#   calls, a revoked token is refused, and a worker of another image is
#   given nothing and the runs say why;
# - a worker of the right image finishes them: their commits to the local
#   Iceberg warehouse are on record, with lineage, and the driver output
#   is in ORCA_LOGS.
#
# Needs ORCA_DATABASE_URL (pixi run -e data test-worker starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src -I tests/lineage $DEP_INC tests/worker/wk.mojo -o build/wk &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
b2=$!
wait "$b1"
wait "$b2"
log=build/worker.log
: > "$log"
export ORCA_WAREHOUSE="$PWD/build/worker-wh" ORCA_LOGS="$PWD/build/worker-logs"
rm -rf "$ORCA_WAREHOUSE" "$ORCA_LOGS" build/worker-stage build/worker-*.out
export ORCA_STALE_SECONDS=3 ORCA_START_SECONDS=10 ORCA_SAMPLE_SECONDS=0
hold="$PWD/build/worker-hold"
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
    rm -f "$hold"
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/worker-*.out; do [ -f "$f" ] && { echo "== $f"; tail -15 "$f"; }; done
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
# until_ SECONDS WHAT CONDITION...: waits for the condition, or fails.
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
is() { [ "$(q "$1")" = "$2" ]; }

./build/orca register --placement worker:t --image orca:test-1 --max-runs 2 ./build/wk >> "$log" 2>&1 || fail "register"
is "SELECT placement || ' ' || image FROM orca.pipelines WHERE name = 'wk'" "worker:t orca:test-1" \
    || fail "the registration: $(q "SELECT placement, image FROM orca.pipelines")"
free_port port
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
pids="$pids $ui"
until_ 10 "orca ui didn't start" curl -fs "$base/" -o /dev/null

tok=$(./build/orca worker-token create --pool t --label laptop 2>> "$log") || fail "worker-token create"
case "$tok" in orca_*) ;; *) fail "not a token: $tok" ;; esac
is "SELECT count(*) FROM orca.workers WHERE token_hash = '$tok'" 0 || fail "the token is kept as it is"
other=$(./build/orca worker-token create --pool other 2>> "$log") || fail "another pool's token"
gone=$(./build/orca worker-token create --pool t --label gone 2>> "$log") || fail "a third token"
gone_id=$(q "SELECT id FROM orca.workers WHERE label = 'gone'")
./build/orca worker-token revoke "$gone_id" >> "$log" 2>&1 || fail "revoke"
./build/orca worker-token list > build/worker-list.out 2>&1 || fail "worker-token list"
grep -q "^$gone_id t gone revoked " build/worker-list.out || fail "the list: $(cat build/worker-list.out)"
grep -q " t laptop live - seen never " build/worker-list.out || fail "the list: $(cat build/worker-list.out)"

./build/orca scheduler > build/worker-sched.out 2>&1 &
sched=$!
pids="$pids $sched"
./build/orca backfill wk 2024-01..2024-02 >> "$log" 2>&1 || fail "backfill"
r1=$(q "SELECT min(id) FROM orca.runs")
r2=$(q "SELECT max(id) FROM orca.runs")
# The scheduler leaves them to a worker, saying so.
until_ 10 "the runs aren't held for a worker: $(q "SELECT id, status, held FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND held = 'held: waiting for a worker of pool t'" 2
sleep 2
is "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND starts = 0" 2 \
    || fail "the scheduler started a worker's run: $(q "SELECT id, status, starts, owner FROM orca.runs")"
curl -fs "$base/pipelines/wk" | grep -q 'its runs wait for a worker of pool t' || fail "the pipeline's page"

# A worker with the database in its environment refuses to start.
code=0
ORCA_WORKER_TOKEN=$tok ./build/orca worker --pool t --remote "$base" > build/worker-db.out 2>&1 || code=$?
[ "$code" = 2 ] || fail "a worker with ORCA_DATABASE_URL exited $code"
grep -q 'ORCA_DATABASE_URL is set' build/worker-db.out || fail "it said: $(cat build/worker-db.out)"

# worker OUT TOKEN IMAGE POOL [ARGS]: a worker with no database in the
# background, its pid in $w.
worker() {
    out=$1; tk=$2; img=$3; pool=$4; shift 4
    env -u ORCA_DATABASE_URL -u ORCA_PGDATA ORCA_WORKER_TOKEN="$tk" ORCA_IMAGE="$img" WK_HOLD="$hold" \
        ./build/orca worker --pool "$pool" --remote "$base" --poll-seconds 0.3 \
        --dir "$PWD/build/worker-stage" --max-runs 2 "$@" > "build/worker-$out.out" 2>&1 &
    w=$!
    pids="$pids $w"
}

# The first worker claims both runs, which wait in their step.
touch "$hold"
worker w1 "$tok" orca:test-1 t --name w-one
w1=$w
until_ 20 "the worker didn't start both runs: $(q "SELECT id, status, owner, heartbeat FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE status = 'running' AND heartbeat IS NOT NULL AND owner LIKE '%:%'" 2
kill -0 "$w1" || fail "the worker exited"
grep -q 'ORCA_DATABASE_URL' "/proc/$w1/environ" 2> /dev/null && fail "the worker has ORCA_DATABASE_URL"
wid=$(q "SELECT id FROM orca.workers WHERE label = 'laptop'")
is "SELECT count(*) FROM orca.runs WHERE worker = $wid AND placement = 'worker:t' AND starts = 1" 2 \
    || fail "the runs' worker: $(q "SELECT id, worker, placement, starts FROM orca.runs")"
is "SELECT count(*) FROM orca.audit WHERE username = 'worker:laptop' AND source = 'worker' AND action = '/api/worker/claim'" 2 \
    || fail "the claims' audit: $(q "SELECT * FROM orca.audit")"
until_ 10 "the attempts aren't recorded: $(q "SELECT * FROM orca.attempts")" \
    is "SELECT count(*) FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run IN ($r1, $r2) AND a.status = 'running' AND s.status = 'running'" 2
# Each run's process is the worker's child, and beats.
p1=$(sed -n "s/^started wk run $r1 start 1 pid \([0-9]*\)$/\1/p" build/worker-w1.out)
p2=$(sed -n "s/^started wk run $r2 start 1 pid \([0-9]*\)$/\1/p" build/worker-w1.out)
[ -n "$p1" ] && [ -n "$p2" ] || fail "the worker didn't say what it started: $(cat build/worker-w1.out)"
q "SELECT owner FROM orca.runs WHERE id = $r1" | grep -q ":$p1\$" \
    || fail "run $r1's owner isn't pid $p1: $(q "SELECT owner FROM orca.runs WHERE id = $r1")"
hb=$(q "SELECT heartbeat FROM orca.runs WHERE id = $r1")
moved() { [ "$(q "SELECT heartbeat FROM orca.runs WHERE id = $r1")" != "$hb" ]; }
until_ 5 "the heartbeat doesn't move" moved
log1=$(q "SELECT a.log FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $r1")
grep -q "database: none, state: $base" "$log1" || fail "the step's process had a database: $(cat "$log1")"
curl -fs -H 'Accept: application/json' "$base/ops/workers" > build/worker-ops.out || fail "GET /ops/workers"
jq -e --arg r "$r1,$r2" '.workers[] | select(.label == "laptop") | select(.pool == "t" and .name == "w-one" and .image == "orca:test-1" and (.running | split(",") | sort | join(",")) == $r and .cpus > 0)' \
    build/worker-ops.out > /dev/null || fail "Ops' workers: $(cat build/worker-ops.out)"
curl -fs "$base/ops/workers" | grep -q '<h1>Workers</h1>' || fail "the Workers page"
curl -fs "$base/runs/$r1" | grep -q 'Ran on<span class="help">.*</dt><dd>w-one (token' || fail "the run's page doesn't say where it ran"

# Another pool's token can't make the runs' store calls, nor claim them.
code=$(curl -s -o build/worker-body.out -w '%{http_code}' -H "Authorization: Bearer $other" \
    -d "{\"run\": $r1}" "$base/api/worker/store/run_info")
[ "$code" = 403 ] || fail "another pool's store call answered $code: $(cat build/worker-body.out)"
code=$(curl -s -o build/worker-body.out -w '%{http_code}' -H "Authorization: Bearer $tok" -H 'Origin: http://evil.example' \
    -d '{"pool": "other", "slots": 1}' "$base/api/worker/claim")
[ "$code" = 403 ] || fail "a claim for another pool answered $code: $(cat build/worker-body.out)"
code=$(curl -s -o /dev/null -w '%{http_code}' -d '{"slots": 1}' "$base/api/worker/claim")
[ "$code" = 401 ] || fail "a claim with no token answered $code"

# Killing the worker stops its runs' processes, and the runs are queued
# again once their heartbeats are stale.
kill -9 "$w1"
until_ 10 "run $r1's process outlived its worker" sh -c "! kill -0 $p1 2> /dev/null"
until_ 10 "run $r2's process outlived its worker" sh -c "! kill -0 $p2 2> /dev/null"
until_ 30 "the runs weren't requeued: $(q "SELECT id, status, heartbeat, held FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND starts = 1 AND held = 'held: waiting for a worker of pool t'" 2
grep -q "requeued .* runs of wk for pool t" build/worker-sched.out || fail "the scheduler didn't say so"

# Refused: another pool's token for pool t, a revoked token, and a worker
# of another pool or image is given nothing.
code=0
env -u ORCA_DATABASE_URL ORCA_WORKER_TOKEN="$other" ./build/orca worker --pool t --remote "$base" \
    > build/worker-other.out 2>&1 || code=$?
[ "$code" = 3 ] || fail "a worker with another pool's token exited $code: $(cat build/worker-other.out)"
grep -q 'this worker token is for pool other, not t' build/worker-other.out || fail "it said: $(cat build/worker-other.out)"
code=0
env -u ORCA_DATABASE_URL ORCA_WORKER_TOKEN="$gone" ./build/orca worker --pool t --remote "$base" \
    > build/worker-gone.out 2>&1 || code=$?
[ "$code" = 3 ] || fail "a worker with a revoked token exited $code: $(cat build/worker-gone.out)"
grep -q 'unknown, expired or revoked' build/worker-gone.out || fail "it said: $(cat build/worker-gone.out)"
worker elsewhere "$other" orca:test-1 other
w_other=$w
worker old "$tok" orca:test-0 t
w_old=$w
until_ 10 "the old image wasn't refused: $(cat build/worker-old.out)" grep -q 'not given a run: the run needs image orca:test-1 and the worker is orca:test-0' build/worker-old.out
until_ 10 "the runs don't say why: $(q "SELECT held FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE held = 'held: worker:laptop can''t run it: the run needs image orca:test-1 and the worker is orca:test-0'" 2
sleep 1
is "SELECT count(*) FROM orca.runs WHERE status = 'queued' AND starts = 1" 2 \
    || fail "a run went to the wrong worker: $(q "SELECT id, status, worker FROM orca.runs")"
kill "$w_other" "$w_old"
grep -q '^started' build/worker-elsewhere.out build/worker-old.out && fail "a refused worker started a run"

# The right worker finishes them.
rm -f "$hold"
worker w2 "$tok" orca:test-1 t --name w-two
until_ 60 "the runs didn't succeed: $(q "SELECT id, status, error FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE status = 'succeeded' AND starts = 2" 2
is "SELECT string_agg(s.status || ' ' || (SELECT count(*) FROM orca.attempts a WHERE a.step = s.id), ',') FROM orca.steps s WHERE s.run = $r1" "succeeded 2" \
    || fail "run $r1's steps: $(q "SELECT * FROM orca.steps")"
is "SELECT a.error FROM orca.attempts a JOIN orca.steps s ON s.id = a.step WHERE s.run = $r1 AND a.attempt = 1" "interrupted: the process running it stopped" \
    || fail "the killed attempt: $(q "SELECT attempt, status, error FROM orca.attempts")"
# The commits, on record with their dataset, and the lineage they make.
is "SELECT count(*) FROM orca.commits c JOIN orca.attempts a ON a.id = c.attempt JOIN orca.steps s ON s.id = a.step WHERE s.run IN ($r1, $r2) AND c.dataset = 'lake.worker'" 2 \
    || fail "the commits: $(q "SELECT * FROM orca.commits")"
out=$(./build/orca lineage lake.worker) || fail "orca lineage"
echo "$out" >> "$log"
echo "$out" | grep -qE "^  0 lake.worker@[0-9-]+ by wk run ($r1|$r2) load 2024-0[12] code 0x[0-9a-f]+\$" \
    || fail "the lineage: $out"
# Once the process has ended, keep-log appends its output.
kept() { grep -q '^run succeeded' "$ORCA_LOGS/wk/$r1/driver.log" 2> /dev/null; }
until_ 20 "no driver log: $(cat "$ORCA_LOGS/wk/$r1/driver.log" 2>&1)" kept
grep -q -- '--- start 2' "$ORCA_LOGS/wk/$r1/driver.log" || fail "the driver log: $(cat "$ORCA_LOGS/wk/$r1/driver.log" 2>&1)"
grep -q "$tok" "$log" build/worker-*.out "$ORCA_LOGS/wk/$r1/driver.log" && fail "the token is in a log"
is "SELECT count(*) FROM orca.workers WHERE label = 'laptop' AND name = 'w-two' AND last_seen > now() - interval '5 seconds'" 1 \
    || fail "the worker's heartbeat: $(q "SELECT * FROM orca.workers")"
echo "worker: ok"
