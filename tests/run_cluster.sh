#!/bin/sh
# One run over two workers of a pool (morca.run_cluster, #268): fanout,
# registered with --workers 1 on pool t, is started by worker a as a dista
# cluster (head, a dista worker, the driver); worker b, with room and
# nothing to start, joins it as a member; both run its tasks:
#
# - a's run reports its head, b joins it, and b's dista worker runs tasks;
# - the result is the one a run on one machine gets;
# - a dista worker without the run's cluster token can't join;
# - once the run ends, b's member is ended and its process gone.
#
# Needs MORCA_DATABASE_URL (pixi run test-run-cluster).
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca
log=build/run-cluster.log
: > "$log"
dir=$PWD/build/run-cluster
rm -rf "$dir"
mkdir -p "$dir"
export MORCA_WAREHOUSE="$dir/wh" MORCA_LOGS="$dir/logs"
export MORCA_STALE_SECONDS=5 MORCA_START_SECONDS=20 MORCA_SAMPLE_SECONDS=0 MORCA_WATCHDOG_SECONDS=0
export MORCA_FANOUT_SLEEP=4 MORCA_FANOUT_SHOW_HOST=1
# No node store: results go between the workers over sockets, as between
# two hosts, so the token guards those fetches too.
export DISTA_SHM=0
unset MORCA_RUNNER MORCA_CONFIG MORCA_WORKER_ADVERTISE
pids=
cleanup() { for p in $pids; do kill "$p" 2> /dev/null || true; done; }
trap cleanup EXIT
fail() {
    echo "FAIL: $*"
    tail -20 "$log"
    for f in build/run-cluster-*.out; do [ -f "$f" ] && { echo "== $f"; tail -12 "$f"; }; done
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
is() { [ "$(q "$1")" = "$2" ]; }
until_() {
    t=$1; what=$2; shift 2
    i=0
    until "$@"; do
        i=$((i + 1)); [ "$i" -lt $((t * 10)) ] || fail "$what"; sleep 0.1
    done
}
free_port uport
base="http://127.0.0.1:$uport"
MORCA_UI_ADDR="127.0.0.1:$uport" ./build/morca ui >> "$log" 2>&1 &
pids="$pids $!"
until_ 10 "morca ui didn't start" curl -fs "$base/" -o /dev/null
./build/morca register --no-config --placement worker:t --workers 1 ./build/fanout >> "$log" 2>&1 || fail "register"
is "SELECT ec2_workers FROM orca.pipelines WHERE name = 'fanout'" 1 || fail "fanout's workers"
ta=$(./build/morca worker-token create --pool t --label a 2>> "$log") || fail "token a"
tb=$(./build/morca worker-token create --pool t --label b 2>> "$log") || fail "token b"
./build/morca scheduler > build/run-cluster-sched.out 2>&1 &
pids="$pids $!"
worker() { # NAME TOKEN
    env -u MORCA_DATABASE_URL HOSTNAME="w$1" MORCA_WORKER_TOKEN="$2" \
        ./build/morca worker --pool t --remote "$base" --poll-seconds 0.3 --name "$1" \
        --advertise 127.0.0.1 --max-runs 2 --dir "$dir/$1" > "build/run-cluster-$1.out" 2>&1 &
    pids="$pids $!"
}

# a starts the run as a cluster.
worker a "$ta"
./build/morca backfill fanout 2024-01 >> "$log" 2>&1 || fail "backfill"
run=$(q "SELECT max(id) FROM orca.runs")
head_known() { [ -n "$(q "SELECT coalesce(cluster_head, '') FROM orca.runs WHERE id = $run")" ]; }
until_ 30 "no head reported: $(q "SELECT status, held FROM orca.runs WHERE id = $run")" head_known
head=$(q "SELECT cluster_head FROM orca.runs WHERE id = $run")
case "$head" in 127.0.0.1:*) ;; *) fail "the head: $head" ;; esac
echo "ok   worker a starts the run as a cluster and reports its head ($head)"
# b, with room and nothing to start, joins.
worker b "$tb"

# A dista worker without the token can't join.
rogue=0
env -u MORCA_DATABASE_URL DISTA_ROLE=worker DISTA_HEAD="$head" DISTA_CONNECT_TIMEOUT=3 \
    ./build/fanout > build/run-cluster-rogue.out 2>&1 || rogue=$?
[ "$rogue" != 0 ] || fail "a dista worker without the token ran: $(tail -3 build/run-cluster-rogue.out)"
grep -q "trips on" build/run-cluster-rogue.out && fail "the rogue ran tasks"
echo "ok   a dista worker without the run's token can't join (exit $rogue)"

joined() { is "SELECT count(*) FROM orca.run_members m JOIN orca.workers w ON w.id = m.worker WHERE m.run = $run AND w.label = 'b'" 1; }
until_ 30 "b didn't join: $(q "SELECT id, worker, ended, why FROM orca.run_members")" joined
until_ 120 "the run didn't succeed: $(q "SELECT status, error FROM orca.runs WHERE id = $run")" \
    is "SELECT status FROM orca.runs WHERE id = $run" succeeded
b_ran=$(cat "$dir"/b/fanout-"$run"-*-member-*.log 2> /dev/null | grep -c "trips on wb" || true)
a_ran=$(cat "$dir"/a/fanout-"$run"-*-worker.log 2> /dev/null | grep -c "trips on wa" || true)
[ "${b_ran:-0}" -gt 0 ] || fail "b's member ran no task (a ran $a_ran)"
[ "${a_ran:-0}" -gt 0 ] || fail "a's dista worker ran no task (b ran $b_ran)"
echo "ok   b joins as a member; both run tasks (a $a_ran, b $b_ran of 31)"

# The result: what any one machine computes, fanout's sum of its days.
# The driver's log is kept once its process has ended, a moment after.
logged() { grep -q "trips in 1 months" "$MORCA_LOGS/fanout/$run/driver.log" 2> /dev/null; }
until_ 30 "no total in run $run's driver log" logged
spread=$(grep -ho "[0-9]* trips in 1 months" "$MORCA_LOGS/fanout/$run/driver.log" | head -1)
want=$(awk 'BEGIN { t = 0; for (d = 1; d <= 31; d++) t += (2024 * 13 + 31 + d * 7) % 1000; print t " trips in 1 months" }')
[ "$spread" = "$want" ] || fail "spread: '$spread', one machine: '$want'"
echo "ok   the same result as on one machine ($spread)"

# The member ended with its run, and its process is gone.
ended() { is "SELECT count(*) FROM orca.run_members WHERE run = $run AND ended IS NULL" 0; }
until_ 30 "the member wasn't ended: $(q "SELECT id, ended, why FROM orca.run_members")" ended
gone() { ! pgrep -f "DISTA_ROLE=worker" > /dev/null 2>&1 || ! grep -q "joined fanout" "build/run-cluster-b.out" || grep -qE "^(member [0-9]+ pid [0-9]+ exited|stopping member)" build/run-cluster-b.out; }
until_ 30 "b's member is still running" gone
echo "ok   once the run ends, the member is ended ($(q "SELECT why FROM orca.run_members WHERE run = $run"))"
# The run's page and JSON show its cluster: the head, with its address, then
# the member.
page=$(curl -fsS "$base/runs/$run") || fail "GET /runs/$run"
echo "$page" | grep -q 'id="cluster"' || fail "no Cluster card on the run's page"
echo "$page" | grep -qF "$head" || fail "the Cluster card doesn't show the head's address"
curl -fsS -H 'Accept: application/json' "$base/runs/$run" > "$dir/run.json" || fail "the run's JSON"
jq -e --arg h "$head" '.cluster | length == 2 and .[0].role == "head" and .[0].address == $h and .[0].worker == "a" and .[1].role == "member" and .[1].worker == "b" and .[1].ended != null and .[1].why != null' "$dir/run.json" > /dev/null \
    || fail "the run's JSON cluster: $(jq -c .cluster "$dir/run.json")"
alone_page=$(curl -fsS -H 'Accept: application/json' "$base/runs/$(q "SELECT min(id) FROM orca.runs WHERE cluster_head IS NULL")" 2> /dev/null || echo '{"cluster": []}')
echo "$alone_page" | jq -e '.cluster == [] or .cluster == null' > /dev/null || fail "a run on one machine shows a cluster"
echo "ok   the run's page and JSON show its cluster: the head and its member"
echo "run cluster: ok"
