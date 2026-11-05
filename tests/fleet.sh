#!/bin/sh
# The fleet page (orca.fleet): the scheduler keeps the last half hour of
# each live instance's samples, and the control instance's, in Postgres
# (a pass again adds nothing, an instance whose file is missing is skipped,
# old samples go); /fleet, its fragment and /api/fleet render hosts, cost,
# runs and errors from those rows and from `orca.instances`, with no S3 or
# file reads of their own; the home page has a strip of the same.
# Needs ORCA_DATABASE_URL (pixi run test-fleet starts a throwaway server),
# curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/fleet.log
: > "$log"
export ORCA_LOGS=build/fleet-logs
rm -rf "$ORCA_LOGS"
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    exit 1
}

# Two runs, one failing: they are what the instances belong to and what the
# errors per hour count.
ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/toy run --partitions 2024-01 >> "$log" 2>&1 || fail "the first run"
ORCA_TOY_FAIL=2024-01 ./build/toy run --partitions 2024-01 >> "$log" 2>&1 && fail "the second run didn't fail"
run=$(q "SELECT max(id) FROM orca.runs")

# A head, two workers (one drained), one that hasn't uploaded, one with a
# type that has no price, and one that ended.
q "INSERT INTO orca.instances (id, run, start, role, instance_type, market, launched, drained) VALUES
   ('i-head', $run, 1, 'head', 'c7g.xlarge', 'on-demand', now() - interval '3 hours 5 minutes', NULL),
   ('i-w1', $run, 1, 'worker', 'c7g.large', 'spot', now() - interval '50 minutes', NULL),
   ('i-w2', $run, 1, 'worker', 'c7g.large', 'spot', now() - interval '50 minutes', now()),
   ('i-w3', $run, 1, 'worker', 'm7g.large', 'spot', now() - interval '20 seconds', NULL)" > /dev/null
q "INSERT INTO orca.instances (id, run, start, role, instance_type, market, launched, ended, why) VALUES
   ('i-old', $run, 1, 'worker', 'c7g.large', 'spot', now() - interval '5 hours', now() - interval '4 hours', 'shut itself down')" > /dev/null

m=$ORCA_LOGS/toy/$run/metrics
mkdir -p "$m"
python3 - "$m" << 'PY'
import json, sys, time
now = time.time()
def line(t, cpu, mem, net_in, net_out):
    return json.dumps({"v": 1, "t": t, "dt": 5.0, "host": "x", "cores": 4,
        "cpu": {"user": cpu, "system": 2.0, "iowait": 0, "steal": 0, "busiest": min(cpu * 2, 100)},
        "mem": {"total": 8000000000, "used": int(mem * 80000000), "available": 1, "cache": 0, "swap_total": 0, "swap_used": 0, "rss": 0},
        "disk": [{"path": "/tmp", "total": 100000000000, "used": 30000000000, "free": 70000000000}],
        "io": {"read": 0, "write": 0}, "net": {"in": net_in, "out": net_out}, "self_cpu": 0.0})
out = sys.argv[1]
# 8 minutes at 5 s: 96 lines for each, the last a few seconds old.
for name, cpu, net in [("head-i-head", 90.0, (1000000, 500000)), ("worker-i-w1", 60.0, (4000000, 2000000)), ("worker-i-w2", 30.0, (0, 0))]:
    with open(f"{out}/{name}.jsonl", "w") as f:
        for k in range(96):
            f.write(line(now - 8 - 5 * (95 - k), cpu + (k % 5), 50.0, *net) + "\n")
# One older than the keep window: dropped on the way in.
with open(f"{out}/worker-i-old.jsonl", "w") as f:
    for k in range(10):
        f.write(line(now - 3 * 3600 + 5 * k, 90.0, 50.0, 1, 1) + "\n")
PY
control=$PWD/build/fleet-control.jsonl
rm -f "$control"
./build/orca sample --out "$control" --count 6 --seconds 0.1 >> "$log" 2>&1 || fail "the control sampler"
export ORCA_CONTROL_SAMPLES=$control ORCA_CONTROL_TYPE=t4g.medium

tick() { ORCA_NOW=2024-01-15T00:00Z ORCA_RUNNER=local ORCA_FLEET_SECONDS=1 ./build/orca tick >> "$log" 2>&1 || fail "tick"; }
tick
[ "$(q "SELECT count(*) FROM orca.fleet_samples WHERE host = 'i-head'")" -eq 96 ] \
    || fail "the head's samples: $(q "SELECT host, count(*) FROM orca.fleet_samples GROUP BY host")"
[ "$(q "SELECT count(*) FROM orca.fleet_samples WHERE host = 'control'")" -eq 6 ] || fail "the control's samples"
[ "$(q "SELECT count(*) FROM orca.fleet_samples WHERE host = 'i-w3'")" -eq 0 ] || fail "samples for an instance with no file"
[ "$(q "SELECT count(*) FROM orca.fleet_samples WHERE host = 'i-old'")" -eq 0 ] || fail "samples older than the window were kept"
hour=$(q "SELECT sum(seconds)::int FROM orca.fleet_hourly WHERE role = 'worker'")
[ "$hour" -eq 960 ] || fail "the workers' seconds in the hour: $hour"
# The same pass again: nothing is added twice, to the samples or the totals.
tick
[ "$(q "SELECT count(*) FROM orca.fleet_samples")" -eq $((96 * 3 + 6)) ] || fail "a second pass added samples"
[ "$(q "SELECT sum(seconds)::int FROM orca.fleet_hourly WHERE role = 'worker'")" -eq 960 ] || fail "a second pass added to the hour"
# Old samples go on a pass, and the hourly rows of 8 days ago.
q "INSERT INTO orca.fleet_samples VALUES ('i-head', now() - interval '31 minutes', 4, 1, 1, 1, 1, 1, 1)" > /dev/null
q "INSERT INTO orca.fleet_hourly VALUES (now() - interval '9 days', 'worker', 1, 1, 1, 1, 1)" > /dev/null
sleep 1
tick
[ "$(q "SELECT count(*) FROM orca.fleet_samples WHERE t < now() - interval '30 minutes'")" -eq 0 ] || fail "old samples were kept"
[ "$(q "SELECT count(*) FROM orca.fleet_hourly WHERE hour < now() - interval '8 days'")" -eq 0 ] || fail "old hours were kept"
echo "ok   the scheduler keeps the instances' samples, once, for 30 minutes"

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

# Remove the logs: the pages must not read a file or a bucket.
rm -rf "$ORCA_LOGS" "$control"

api=$(curl -fsS "$base/api/fleet")
echo "$api" | jq -e '.summary | .heads == 1 and .workers == 3 and .control == true and .hosts == 5
    and .unpriced_hosts == 1 and .runs_per_hour == 2 and .runs_running == 0
    and (.cost_per_hour > 0.3235 and .cost_per_hour < 0.3237)
    and .failed_runs_per_hour == 1 and .failed_attempts_per_hour >= 1
    and (.types | map(select(.type == "c7g.large"))[0].count == 2)
    and (.runs_per_hour_24h | length) == 24 and (.errors_per_hour_24h | length) == 24' > /dev/null \
    || fail "the summary: $(echo "$api" | jq -c .summary)"
[ "$(curl -fsS -o /dev/null -w '%{content_type}' "$base/api/fleet")" = "application/json" ] || fail "/api/fleet's content type"
echo "$api" | jq -e '[.hosts[] | .id] | sort == ["control", "i-head", "i-w1", "i-w2", "i-w3"]' > /dev/null || fail "the hosts: $(echo "$api" | jq -c '[.hosts[].id]')"
echo "$api" | jq -e '(.hosts[] | select(.id == "i-w1")) | .state == "running" and .run == '"$run"' and .role == "worker"
    and .cpu > 60 and .cpu < 67 and .mem_pct > 49.9 and .mem_pct < 50.1 and .disk_pct == 30
    and .net_in_bytes_per_second == 4000000 and (.cpu_last_10_min | length) == 96' > /dev/null \
    || fail "a worker's row: $(echo "$api" | jq -c '.hosts[] | select(.id == "i-w1")')"
echo "$api" | jq -e '[.hosts[] | {(.id): .state}] | add | .["i-w2"] == "draining" and .["i-w3"] == "starting" and .control == "control" and .["i-head"] == "running"' > /dev/null \
    || fail "the states: $(echo "$api" | jq -c '[.hosts[] | [.id, .state]]')"
echo "$api" | jq -e '.hosts[] | select(.id == "i-w3") | .sampled == false' > /dev/null || fail "a host with no samples"
echo "$api" | jq -e '.charts | .hours == 24 and (.labels_utc | length) == 24 and (.hosts.heads | length) == 24 and (.runs.started | add) == 2
    and (.hosts.heads[-1] == 1) and (.hosts.workers[-1] == 3) and (.hosts.control[-1] == 1)
    and (.cpu["vCPUs busy"][-1] > 0) and (.network["in (MB/s)"][-1] > 0)' > /dev/null \
    || fail "the charts: $(echo "$api" | jq -c '.charts' | cut -c1-600)"
curl -fsS "$base/api/fleet?range=7d" | jq -e '.charts.hours == 168 and (.charts.labels_utc | length) == 168' > /dev/null || fail "?range=7d"
echo "ok   /api/fleet"

page=$(html /fleet)
echo "$page" | grep -qF '<a href="/fleet" aria-current="page">Fleet</a>' || fail "Fleet isn't in the nav, current"
echo "$page" | grep -qF 'data-poll="/fleet/hosts?sort=&amp;dir=asc&amp;range=24h" data-poll-every="10"' || fail "the hosts don't poll every 10 s"
echo "$page" | grep -qF '<span class="stat__v">5</span>' || fail "hosts running"
echo "$page" | grep -qF '1 head, 3 workers, control' || fail "heads and workers"
echo "$page" | grep -qF 'c7g.large x2' || fail "the instance types"
echo "$page" | grep -qF '~$0.32/h' || fail "the cost"
echo "$page" | grep -qF 'estimate, on-demand us-east-2; 1 unpriced' || fail "the cost isn't labelled an estimate"
echo "$page" | grep -qF 'step attempts, 1 runs' || fail "errors: attempts and runs"
echo "$page" | grep -qF "<a class=\"chip\" href=\"/runs/$run\">$run</a>" || fail "the run link"
echo "$page" | grep -qF '<code>head-i-head</code>' || fail "the head's row"
echo "$page" | grep -qF '<code>control</code>' || fail "the control's row"
echo "$page" | grep -q '<td class="num">3h 05m</td>' || fail "the head's uptime"
echo "$page" | grep -q 'no samples yet' || fail "the starting host's row"
[ "$(echo "$page" | grep -o '<svg class="spark" viewBox="0 0 80 22"[^>]*aria-label="CPU over the last 10 minutes"><polyline' | wc -l | tr -d ' ')" -eq 4 ] \
    || fail "not a CPU sparkline per sampled host"
echo "$page" | grep -qF 'aria-label="Hosts over time"' || fail "no hosts chart"
echo "$page" | grep -qF 'aria-label="Runs per hour"' || fail "no runs chart"
echo "$page" | grep -qF 'aria-label="Fleet CPU"' || fail "no CPU chart"
echo "$page" | grep -qF 'aria-label="Fleet network"' || fail "no network chart"
echo "$page" | grep -qF 'class="fl--bad"' || fail "no failures overlaid on the runs"
echo "$page" | grep -qF '<strong>24h</strong> <a href="/fleet?range=7d">7d</a>' || fail "the range choice"
html '/fleet?range=7d' | grep -qF '<strong>7d</strong>' || fail "the 7d range"
echo "ok   /fleet"

# Sorting, by query parameter, in the page and its fragment.
order() { html "$1" | grep -o '<code>[a-z]*-\{0,1\}[a-z0-9-]*</code>' | sed 's/<[^>]*>//g' | tr '\n' ' '; }
want="control head-i-head worker-i-w1 worker-i-w2 worker-i-w3 "
[ "$(order /fleet)" = "$want" ] || fail "default order: $(order /fleet)"
# The control instance's CPU is this machine's, so only the rest is fixed.
want="head-i-head worker-i-w1 worker-i-w2 "
[ "$(order '/fleet?sort=cpu&dir=desc' | cut -d' ' -f1-3 | sed 's/$/ /')" = "$want" ] || fail "by CPU, highest first: $(order '/fleet?sort=cpu&dir=desc')"
[ "$(order '/fleet/hosts?sort=cpu&dir=desc' | cut -d' ' -f1-3 | sed 's/$/ /')" = "$want" ] || fail "the fragment's order"
# A host with no samples counts as 0%: first or second, after an idle control.
order '/fleet?sort=cpu&dir=asc' | awk '{print $1, $2}' | grep -q "worker-i-w3" || fail "by CPU, lowest first: a host with no samples is lowest"
[ "$(order '/fleet/hosts?sort=uptime&dir=desc' | cut -d' ' -f1)" = "head-i-head" ] || fail "by uptime, longest first"
[ "$(order '/fleet?sort=nonsense')" = "$(order /fleet)" ] || fail "an unknown column is the default order"
html '/fleet?sort=cpu&dir=desc' | grep -qF '<th class="num" aria-sort="descending"><a class="sortby" href="/fleet?sort=cpu&amp;dir=asc&amp;range=24h#hosts">CPU ▼</a>' \
    || fail "the sorted header"
html '/fleet?sort=cpu&dir=desc' | grep -qF 'data-poll="/fleet/hosts?sort=cpu&amp;dir=desc&amp;range=24h"' || fail "the poll keeps the sort"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/fleet/hosts")" = 200 ] || fail "the fragment doesn't answer 200 forever"
html /fleet/hosts | grep -q '<html' && fail "the fragment is a page"
echo "ok   /fleet sorts, and its fragment keeps the sort"

# The home page: a strip of the same numbers, each linking to its detail.
page=$(html /)
echo "$page" | grep -qF '<div class="stats stats--compact">' || fail "no strip on /"
echo "$page" | grep -qF '<a class="stat" href="/fleet#hosts"><span class="stat__k">Hosts' || fail "the strip's hosts link"
echo "$page" | grep -qF 'href="/runs?status=failed"' || fail "the strip's errors link"
json / | jq -e '.fleet.hosts == 5' > /dev/null || fail "/'s JSON has no fleet"
json /fleet | jq -e '.summary.hosts == 5 and (.hosts | length) == 5' > /dev/null || fail "/fleet's JSON via Accept"

# A value from the database is escaped.
q "UPDATE orca.instances SET instance_type = '<b>x</b>' WHERE id = 'i-w3'" > /dev/null
html /fleet | grep -qF '<b>x</b>' && fail "an instance type's markup reached the page"
html /fleet | grep -qF '&lt;b&gt;x&lt;/b&gt;' || fail "the escaped instance type"
kill "$pid"
echo "ok   fleet"
