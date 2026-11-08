#!/bin/sh
# Fills on a worker pool (#177): orca ui, the scheduler and two `orca
# worker`s of pool t, with no database, over tests/pool/fillwk.mojo
# (monthly from 2024-01; ORCA_NOW puts its range at 2024-01..2024-06),
# placed worker:t with fills = "all", fill_order = "oldest" and
# fill_max_runs = 2:
#
# - with no worker, the scheduler queues 2 fill runs, oldest first, held
#   for a worker, and no more: queued fill runs count against
#   fill_max_runs, as the Fill card and the calendar show;
# - two workers of --max-runs 1 claim one each, and each run's machine
#   metrics are kept (metrics/worker-<name>.jsonl);
# - killing one worker (its exact pid) queues its run again for the pool,
#   audited, and the other worker finishes the fill: every partition done,
#   one fill run each, never more than 2 queued or running at once;
# - a UI run on a worker that restarts (killed, and started again with its
#   token) is queued again rather than failed "interrupted", and succeeds;
# - a run a worker claimed and never started (a claim made with curl, by a
#   token whose worker is gone) is queued again with its start given back.
#
# Needs ORCA_DATABASE_URL (pixi run test-pool-fills starts a throwaway
# server), curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC tests/pool/fillwk.mojo -o build/fillwk &
b1=$!
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
b2=$!
wait "$b1"
wait "$b2"
log=build/pool-fills.log
: > "$log"
proj=$PWD/build/pool-fills-project
export ORCA_LOGS="$PWD/build/pool-fills-logs"
rm -rf "$proj" "$ORCA_LOGS" build/pool-fills-stage build/pool-fills-*.out
mkdir -p "$proj"
export ORCA_STALE_SECONDS=3 ORCA_START_SECONDS=10 ORCA_SAMPLE_SECONDS=1 ORCA_WATCHDOG_SECONDS=0
export ORCA_NOW=2024-07-15T12:00Z
unset ORCA_IMAGE
hold="$PWD/build/pool-fills-hold"
pids=
cleanup() {
    for p in $pids; do kill "$p" 2> /dev/null || true; done
    rm -f "$hold"
}
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    for f in build/pool-fills-*.out; do [ -f "$f" ] && { echo "== $f"; tail -15 "$f"; }; done
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
. "$PWD/tests/port.sh"
free_port port
base="http://127.0.0.1:$port"
fills() { q "SELECT coalesce(string_agg(partitions, ',' ORDER BY id), '') FROM orca.runs WHERE trigger = 'fill'"; }
fill() { curl -fsS "$base/api/pipelines/fillwk" | jq -r ".fill.$1" || fail "GET /api/pipelines/fillwk"; }

cat > "$proj/orca.toml" << 'EOF'
[pipelines.fillwk]
placement = "worker:t"
fills = "all"
fill_order = "oldest"
fill_max_runs = 2
EOF
./build/orca register --config "$proj/orca.toml" ./build/fillwk >> "$log" 2>&1 || fail "register"
is "SELECT placement || ' ' || fills || ' ' || fill_max_runs FROM orca.pipelines WHERE name = 'fillwk'" "worker:t all 2" \
    || fail "the registration: $(q "SELECT placement, fills, fill_max_runs FROM orca.pipelines")"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "orca ui didn't start" curl -fs "$base/" -o /dev/null
hetzner=$(./build/orca worker-token create --pool t --label hetzner 2>> "$log") || fail "worker-token create"
bgent=$(./build/orca worker-token create --pool t --label bgent 2>> "$log") || fail "worker-token create"

# --- No worker yet: fill_max_runs fill runs, queued and held, no more ---
./build/orca scheduler > build/pool-fills-sched.out 2>&1 &
sched=$!
pids="$pids $sched"
until_ 10 "no fill runs queued: $(q "SELECT id, status, trigger, held FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE trigger = 'fill' AND status = 'queued' AND held = 'held: waiting for a worker of pool t'" 2
sleep 2
[ "$(fills)" = "2024-01,2024-02" ] || fail "the queued fill runs: $(fills)"
is "SELECT count(*) FROM orca.runs WHERE starts > 0" 0 || fail "the scheduler started a pool's run"
[ "$(fill queued)" = 2 ] && [ "$(fill running)" = 0 ] && [ "$(fill total)" = 6 ] \
    || fail "the API's fill: $(curl -fsS "$base/api/pipelines/fillwk" | jq -c .fill)"
case "$(fill idle)" in "at fill_max_runs: 2 of 2 going (2 waiting for a worker)") ;; *) fail "idle: $(fill idle)" ;; esac
curl -fsS "$base/pipelines/fillwk" | grep -q '0 / 6 done (0%), 2 fill runs queued, 2 in live runs' \
    || fail "the page's fill line: $(curl -fsS "$base/pipelines/fillwk" | grep -o '[0-9]* / 6 done[^<]*')"
c=$(curl -fsS "$base/pipelines/fillwk/calendar?month=2024")
echo "$c" | jq -e '.counts.live == 2 and .counts.open == 5' > /dev/null || fail "the calendar: $(echo "$c" | jq -c .counts)"
echo "ok   with no worker, fill_max_runs fill runs wait queued, oldest first"

# Never more than fill_max_runs fill runs queued or running, from here on.
peak=build/pool-fills-peak.out
: > "$peak"
(
    while :; do
        q "SELECT count(*) FROM orca.runs WHERE trigger = 'fill' AND status IN ('queued', 'running')" >> "$peak" 2> /dev/null || true
        sleep 0.2
    done
) &
watch=$!
pids="$pids $watch"

# worker OUT TOKEN NAME: a worker of pool t with no database, one run at
# once, in the background; its pid in $w.
worker() {
    env -u ORCA_DATABASE_URL -u ORCA_PGDATA ORCA_WORKER_TOKEN="$2" FILLWK_HOLD="$hold" \
        ./build/orca worker --pool t --remote "$base" --poll-seconds 0.3 \
        --dir "$PWD/build/pool-fills-stage" --max-runs 1 --name "$3" > "build/pool-fills-$1.out" 2>&1 &
    w=$!
    pids="$pids $w"
}

# --- Two workers take one each; one is killed; the other finishes ---
touch "$hold"
worker w1 "$hetzner" hetzner
w1=$w
worker w2 "$bgent" bgent
w2=$w
until_ 20 "the workers didn't start a fill run each: $(q "SELECT id, status, worker, heartbeat FROM orca.runs")" \
    is "SELECT count(DISTINCT worker) FROM orca.runs WHERE trigger = 'fill' AND status = 'running' AND heartbeat IS NOT NULL" 2
ra=$(q "SELECT r.id FROM orca.runs r JOIN orca.workers w ON w.id = r.worker WHERE w.label = 'hetzner' AND r.status = 'running'")
rb=$(q "SELECT r.id FROM orca.runs r JOIN orca.workers w ON w.id = r.worker WHERE w.label = 'bgent' AND r.status = 'running'")
sampled() { [ -s "$ORCA_LOGS/fillwk/$ra/metrics/worker-hetzner.jsonl" ]; }
until_ 10 "no machine metrics for run $ra: $(ls -R "$ORCA_LOGS" 2>&1)" sampled
jq -e '.cores > 0 and .mem.total > 0' "$ORCA_LOGS/fillwk/$ra/metrics/worker-hetzner.jsonl" > /dev/null \
    || fail "the samples: $(head -2 "$ORCA_LOGS/fillwk/$ra/metrics/worker-hetzner.jsonl")"
sleep 1
[ "$(fills)" = "2024-01,2024-02" ] || fail "a fill run was queued beyond fill_max_runs: $(fills)"
[ "$(fill running)" = 2 ] && [ "$(fill queued)" = 0 ] || fail "the API: $(curl -fsS "$base/api/pipelines/fillwk" | jq -c .fill)"
echo "ok   two workers take a fill run each, with machine metrics"

kill -9 "$w1"
until_ 30 "run $ra wasn't queued again: $(q "SELECT id, status, starts, held FROM orca.runs")" \
    is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $ra" "queued 1"
is "SELECT count(*) FROM orca.audit WHERE run = $ra AND action = '/api/worker/requeue' AND username = 'scheduler' AND detail LIKE 'queued again: worker hetzner (token %'" 1 \
    || fail "the requeue's audit: $(q "SELECT username, action, run, detail FROM orca.audit WHERE run = $ra")"
grep -q "requeued run $ra of fillwk for pool t: worker hetzner" build/pool-fills-sched.out || fail "the scheduler didn't say so"
case "$(fill idle)" in "at fill_max_runs: 2 of 2 going (1 waiting for a worker)") ;; *) fail "idle: $(fill idle)" ;; esac
rm -f "$hold"
until_ 90 "the fill didn't complete: $(q "SELECT id, partitions, status, starts, error FROM orca.runs")" \
    is "SELECT count(*) FROM orca.runs WHERE trigger = 'fill' AND status = 'succeeded'" 6
sleep 2
[ "$(fills)" = "2024-01,2024-02,2024-03,2024-04,2024-05,2024-06" ] || fail "the fill runs: $(fills)"
is "SELECT starts FROM orca.runs WHERE id = $ra" 2 || fail "run $ra's starts: $(q "SELECT starts FROM orca.runs WHERE id = $ra")"
is "SELECT count(*) FROM orca.runs WHERE trigger = 'fill' AND id <> $ra AND starts <> 1" 0 \
    || fail "a fill run started twice: $(q "SELECT id, starts FROM orca.runs")"
kill "$watch"
most=$(sort -n "$peak" | tail -1)
[ "$most" -le 2 ] || fail "$most fill runs queued or running at once"
[ "$(fill done)" = 6 ] && [ "$(fill idle)" = complete ] || fail "the API: $(curl -fsS "$base/api/pipelines/fillwk" | jq -c .fill)"
curl -fsS "$base/pipelines/fillwk" | grep -q '6 / 6 done (100%)' \
    || fail "the page's fill line: $(curl -fsS "$base/pipelines/fillwk" | grep -o '[0-9]* / 6 done[^<]*')"
c=$(curl -fsS "$base/pipelines/fillwk/calendar?month=2024")
echo "$c" | jq -e '.counts.done == 6 and .counts.live == 0' > /dev/null || fail "the calendar: $(echo "$c" | jq -c .counts)"
echo "ok   a killed worker's fill run is queued again, and the other worker completes the fill (at most $most at once)"

# --- A UI run on a worker that restarts is queued again, not failed ---
touch "$hold"
ui_run() {
    q "INSERT INTO orca.runs (pipeline, registry_hash, partitions, trigger, status) SELECT name, registry_hash, '$1', 'ui', 'queued' FROM orca.pipelines WHERE name = 'fillwk' RETURNING id" | head -1
}
rm_=$(ui_run 2024-03)
until_ 20 "worker bgent didn't start run $rm_: $(q "SELECT id, status, heartbeat FROM orca.runs WHERE id = $rm_")" \
    is "SELECT count(*) FROM orca.runs WHERE id = $rm_ AND status = 'running' AND heartbeat IS NOT NULL" 1
kill -9 "$w2"
worker w3 "$bgent" bgent
w3=$w
until_ 30 "run $rm_ wasn't queued again: $(q "SELECT id, status, starts, error FROM orca.runs WHERE id = $rm_")" \
    is "SELECT count(*) FROM orca.audit WHERE run = $rm_ AND action = '/api/worker/requeue'" 1
until_ 20 "worker bgent didn't start run $rm_ again: $(q "SELECT id, status, starts FROM orca.runs WHERE id = $rm_")" \
    is "SELECT count(*) FROM orca.runs WHERE id = $rm_ AND status = 'running' AND starts = 2" 1
rm -f "$hold"
until_ 30 "run $rm_ didn't succeed: $(q "SELECT id, status, starts, error FROM orca.runs WHERE id = $rm_")" \
    is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $rm_" "succeeded 2"
is "SELECT count(*) FROM orca.runs WHERE error LIKE 'interrupted%'" 0 || fail "a run failed interrupted"
echo "ok   a UI run whose worker restarted is queued again, and succeeds"

# --- A claim never started is queued again with its start given back ---
kill "$w3"
sleep 1
rc=$(ui_run 2024-04)
got=$(curl -fsS -H "Authorization: Bearer $hetzner" -d '{"pool": "t", "slots": 1, "name": "gone", "image": "", "running": ""}' \
    "$base/api/worker/claim") || fail "the claim"
[ "$(echo "$got" | jq -r .run.run)" = "$rc" ] || fail "the claim got: $got"
is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $rc" "running 1" || fail "the claim didn't start it"
until_ 30 "the unstarted run $rc wasn't queued again: $(q "SELECT id, status, starts FROM orca.runs WHERE id = $rc")" \
    is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $rc" "queued 0"
is "SELECT count(*) FROM orca.audit WHERE run = $rc AND action = '/api/worker/requeue' AND detail LIKE '%before starting it'" 1 \
    || fail "the audit: $(q "SELECT detail FROM orca.audit WHERE run = $rc")"
worker w4 "$hetzner" hetzner
until_ 30 "run $rc didn't succeed: $(q "SELECT id, status, starts, error FROM orca.runs WHERE id = $rc")" \
    is "SELECT status || ' ' || starts FROM orca.runs WHERE id = $rc" "succeeded 1"
is "SELECT count(*) FROM orca.runs WHERE status <> 'succeeded'" 0 || fail "runs: $(q "SELECT id, status, error FROM orca.runs")"
grep -q "$hetzner" "$log" build/pool-fills-*.out && fail "a token is in a log"
echo "pool fills: ok"
