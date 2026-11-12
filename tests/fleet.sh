#!/bin/sh
# The fleet (morca.fleet) and Ops' Pools (morca.ui.pools): the scheduler keeps
# the last half hour of each live instance's samples, and the control
# instance's, in Postgres (a pass again adds nothing, an instance whose file
# is missing is skipped, old samples go); /api/fleet renders hosts, cost,
# runs and errors from those rows and from `orca.instances`, with no S3 or
# file reads of its own; /ops/pools lists those instances and remote
# workers' machines a row each, filtered by pool and kind, with Fleet's
# cards and charts, refreshed from its fragment; a machine's page has its
# samples; /ops/fleet and /ops/workers redirect to it and keep their JSON;
# on the Kubernetes runner a live run's pods are a machine; the home page
# has a strip of the same.
# Needs MORCA_DATABASE_URL (pixi run test-fleet starts a throwaway server),
# curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/fleet.log
: > "$log"
export MORCA_LOGS=build/fleet-logs
rm -rf "$MORCA_LOGS"
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    exit 1
}

# Two runs, one failing: they are what the instances belong to and what the
# errors per hour count.
MORCA_NOW=2024-01-15T00:00Z ./build/morca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/toy run --partitions 2024-01 >> "$log" 2>&1 || fail "the first run"
MORCA_TOY_FAIL=2024-01 ./build/toy run --partitions 2024-01 >> "$log" 2>&1 && fail "the second run didn't fail"
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

m=$MORCA_LOGS/toy/$run/metrics
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
./build/morca sample --out "$control" --count 6 --seconds 0.1 >> "$log" 2>&1 || fail "the control sampler"
export MORCA_CONTROL_SAMPLES=$control MORCA_CONTROL_TYPE=t4g.medium

tick() { MORCA_NOW=2024-01-15T00:00Z MORCA_RUNNER=local MORCA_FLEET_SECONDS=1 ./build/morca tick >> "$log" 2>&1 || fail "tick"; }
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

free_port port
base="http://127.0.0.1:$port"
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
pid=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
done
html() { curl -fsS "$base$1" || fail "GET $1"; }
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1 as JSON"; }

# Self-hosted machines: a worker of pool bgent that has said who it is and
# what it runs, and one of pool gh that never has.
wtok=$(./build/morca worker-token create --pool bgent --label lap 2>> "$log") || fail "worker-token create"
./build/morca worker-token create --pool gh --label box > /dev/null 2>> "$log" || fail "a second worker token"
wid=$(q "SELECT id FROM orca.workers WHERE label = 'lap'")
bid=$(q "SELECT id FROM orca.workers WHERE label = 'box'")
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $wtok" \
    -d "{\"name\": \"w-lap\", \"slots\": 0, \"cpus\": 8, \"memory_mb\": 16384, \"image\": \"morca:t1\", \"running\": \"$run\"}" \
    "$base/api/worker/claim")
[ "$code" = 200 ] || fail "the worker's heartbeat answered $code"

# A machine's page: an instance's samples over its run, from the file its
# sampler uploaded (as the run page's Machines card reads it).
mpage=$(html /ops/pools/ec2/i-w1)
echo "$mpage" | grep -qF '<p class="crumb"><a href="/ops/pools">Pools</a> · <a href="/ops/pools?pool=ec2">ec2</a></p><h1>i-w1' \
    || fail "the machine page's heading"
echo "$mpage" | grep -qF '<a href="/ops/pools" aria-current="page">Pools</a>' || fail "the machine page isn't under Pools"
echo "$mpage" | grep -q 'mx__legend' || fail "no samples chart on the machine page"
echo "$mpage" | grep -qF "<a class=\"chip\" href=\"/runs/$run\">$run</a>" || fail "the machine page's run"
echo "$mpage" | grep -qF '4 GiB' || fail "the machine page's memory"
json /ops/pools/ec2/i-w1 | jq -e '.machine | .state == "live" and .kind == "ec2" and .cpus == 2 and .memory_mb == 4096 and .process == "worker"' > /dev/null \
    || fail "the machine's JSON: $(json /ops/pools/ec2/i-w1)"
wpage=$(html "/ops/pools/self-hosted/$wid")
echo "$wpage" | grep -qF '<h1>w-lap <span data-tip=' || fail "the worker's page: $wpage"
echo "$wpage" | grep -qF '<code>morca:t1</code>' || fail "the worker's image"
echo "$wpage" | grep -qF "It hasn't claimed a run." || fail "the worker's runs"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/ops/pools/ec2/i-nope")" = 404 ] || fail "an unknown instance"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/ops/pools/self-hosted/x")" = 404 ] || fail "a worker that isn't a number"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/ops/pools/nope/1")" = 404 ] || fail "an unknown kind"
echo "ok   a machine's page"

# Remove the logs: the pages must not read a file or a bucket.
rm -rf "$MORCA_LOGS" "$control"

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
    and (.hosts["self-hosted"][-1] == 1) and (.hosts["self-hosted"][0] == 0)
    and (.cpu["vCPUs busy"][-1] > 0) and (.network["in (MB/s)"][-1] > 0)' > /dev/null \
    || fail "the charts: $(echo "$api" | jq -c '.charts' | cut -c1-600)"
curl -fsS "$base/api/fleet?range=7d" | jq -e '.charts.hours == 168 and (.charts.labels_utc | length) == 168' > /dev/null || fail "?range=7d"
echo "ok   /api/fleet"

# One that ended ten minutes ago: Pools lists it, gone, for an hour.
q "INSERT INTO orca.instances (id, run, start, role, instance_type, market, launched, ended, why) VALUES
   ('i-gone', $run, 1, 'worker', 'c7g.large', 'spot', now() - interval '1 hour', now() - interval '10 minutes', 'drained')" > /dev/null

page=$(html /ops/pools)
echo "$page" | grep -qF '<a href="/ops/pools" aria-current="page">Pools</a>' || fail "Pools isn't in the Ops menu, current"
echo "$page" | grep -qF '<a href="/ops" aria-current="page" aria-haspopup="true">Ops</a>' || fail "Ops isn't current"
echo "$page" | grep -qF '<a href="/ops/alerts">Alerts</a><a href="/ops/control">Control</a><a href="/ops/pools" aria-current="page">Pools</a><a href="/watchdog">Watchdog</a></div>' \
    || fail "the Ops menu isn't Alerts, Control, Pools and Watchdog"
echo "$page" | grep -q 'the instances morca launches for runs' || fail "the Pools intro"
echo "$page" | grep -qF 'data-poll="/ops/pools/live" data-poll-every="10"' || fail "the machines don't poll every 10 s"
# Fleet's cards and charts, with EC2 in view.
echo "$page" | grep -qF '<span class="stat__v">5</span>' || fail "hosts running"
echo "$page" | grep -qF '1 head, 3 workers, control' || fail "heads and workers"
echo "$page" | grep -qF 'c7g.large x2' || fail "the instance types"
echo "$page" | grep -qF '~$0.32/h' || fail "the cost"
echo "$page" | grep -qF 'estimate, on-demand us-east-2; 1 unpriced' || fail "the cost isn't labelled an estimate"
echo "$page" | grep -qF 'step attempts, 1 runs' || fail "errors: attempts and runs"
for c in 'Hosts over time' 'Runs per hour' 'Fleet CPU' 'Fleet network'; do
    echo "$page" | grep -qF "aria-label=\"$c\"" || fail "no $c chart"
done
echo "$page" | grep -qF 'class="fl--bad"' || fail "no failures overlaid on the runs"
echo "$page" | grep -qF '<strong>24h</strong> <a href="/ops/pools?range=7d">7d</a>' || fail "the range choice"
html '/ops/pools?range=7d' | grep -qF '<strong>7d</strong>' || fail "the 7d range"
html '/ops/pools?kind=ec2&range=7d' | grep -qF '<a href="/ops/pools?kind=ec2&amp;range=24h">24h</a>' || fail "the range keeps the filter"
# A row per machine, both kinds: the live instances and one that ended ten
# minutes ago, and the workers' machines; not the control host, which is
# Control.
rows() { html "$1" | grep -o '<a href="/ops/pools/[a-z0-9-]*/[a-zA-Z0-9-]*"><code>[^<]*</code>' | sed 's/.*<code>//; s/<.code>//' | tr '\n' ' '; }
# The default view leaves out `box`, a worker that never reported (gone for
# good); i-gone ended ten minutes ago, so it stays; ?gone=1 lists box too.
want="i-head i-w1 i-w2 i-w3 i-gone w-lap "
all="${want}box "
[ "$(rows /ops/pools)" = "$want" ] || fail "the machines: $(rows /ops/pools)"
[ "$(rows '/ops/pools?gone=1')" = "$all" ] || fail "the machines with ?gone=1: $(rows '/ops/pools?gone=1')"
echo "$page" | grep -qF '<code>control</code>' && fail "the control host is a row"
echo "$page" | grep -qF "<a href=\"/ops/pools/ec2/i-head\"><code>i-head</code></a></td><td><a href=\"/ops/pools?pool=ec2\">ec2</a></td><td><span class=\"term\"" \
    || fail "the head's row"
echo "$page" | grep -qF "<a href=\"/ops/pools/self-hosted/$wid\"><code>w-lap</code></a></td><td><a href=\"/ops/pools?pool=worker%3Abgent\">worker:bgent</a>" \
    || fail "the worker's row"
echo "$page" | grep -qF '<code>morca worker</code> <span class="muted" title="expires ' || fail "the worker process's columns"
echo "$page" | grep -qF 'token '"$wid"' · lap' || fail "the worker's token in its row"
echo "$page" | grep -qF '<td class="mono">morca:t1</td><td class="num">8</td><td class="num">16 GiB</td>' || fail "the worker's image, CPUs and memory"
echo "$page" | grep -q 'no samples yet' || fail "the starting host's row"
[ "$(echo "$page" | grep -o '<svg class="spark" viewBox="0 0 80 22"[^>]*aria-label="CPU over the last 10 minutes"><polyline' | wc -l | tr -d ' ')" -eq 3 ] \
    || fail "not a CPU sparkline per sampled instance"
# Help on every column, from morca.ui.terms.
for c in Machine Pool Kind State Image CPUs Memory Runs 'Last heartbeat' CPU Worker; do
    echo "$page" | grep -qF "aria-label=\"About $c\"" || fail "no help on the $c column"
done
# The API: the same machines, and the pools they are in.
api=$(curl -fsS "$base/api/pools")
[ "$(curl -fsS -o /dev/null -w '%{content_type}' "$base/api/pools")" = "application/json" ] || fail "/api/pools' content type"
echo "$api" | jq -e '[.machines[] | {(.name): .state}] | add | .["i-head"] == "live" and .["i-w2"] == "draining"
    and .["i-w3"] == "live" and .["i-gone"] == "gone" and .["w-lap"] == "live" and .box == "gone"' > /dev/null \
    || fail "the states: $(echo "$api" | jq -c '[.machines[] | [.name, .state, .detail]]')"
echo "$api" | jq -e '[.machines[] | {(.name): .detail}] | add | .["i-w3"] == "starting" and .["i-head"] == "running" and .["i-gone"] == "ended" and .box == "not seen" and .["w-lap"] == "running"' > /dev/null \
    || fail "the details: $(echo "$api" | jq -c '[.machines[] | [.name, .detail]]')"
echo "$api" | jq -e --argjson r "$run" '.machines[] | select(.name == "w-lap") | .pool == "worker:bgent" and .kind == "self-hosted" and .runs == [$r]
    and .cpus == 8 and .memory_mb == 16384 and .image == "morca:t1" and .label == "lap" and .seen_seconds_ago >= 0' > /dev/null \
    || fail "the worker's machine: $(echo "$api" | jq -c '.machines[] | select(.name == "w-lap")')"
echo "$api" | jq -e '.machines[] | select(.name == "i-gone") | .ended | test("Z: drained$")' > /dev/null || fail "the ended instance"
echo "$api" | jq -e '.pools == [{"pool": "ec2", "kind": "ec2", "machines": 5, "live": 4}, {"pool": "worker:bgent", "kind": "self-hosted", "machines": 1, "live": 1}, {"pool": "worker:gh", "kind": "self-hosted", "machines": 1, "live": 0}]' > /dev/null \
    || fail "the pools: $(echo "$api" | jq -c .pools)"
json /ops/pools | jq -e '(.machines | length) == 7' > /dev/null || fail "/ops/pools' JSON via Accept"
echo "ok   /ops/pools lists every pool's machines, EC2 and self-hosted"

# Filters, as query parameters with plain links.
[ "$(rows '/ops/pools?kind=self-hosted')" = "w-lap " ] || fail "kind=self-hosted: $(rows '/ops/pools?kind=self-hosted')"
[ "$(rows '/ops/pools?kind=self-hosted&gone=1')" = "w-lap box " ] || fail "kind=self-hosted&gone=1"
[ "$(rows '/ops/pools?kind=ec2')" = "i-head i-w1 i-w2 i-w3 i-gone " ] || fail "kind=ec2"
[ "$(rows '/ops/pools?pool=ec2')" = "i-head i-w1 i-w2 i-w3 i-gone " ] || fail "pool=ec2"
[ "$(rows '/ops/pools?pool=worker:bgent')" = "w-lap " ] || fail "pool=worker:bgent"
[ "$(rows '/ops/pools?pool=worker%3Agh&kind=self-hosted&gone=1')" = "box " ] || fail "pool and kind"
[ "$(rows '/ops/pools?pool=worker:bgent&kind=ec2')" = "" ] || fail "a pool of another kind"
[ "$(rows '/ops/pools?kind=nonsense')" = "$want" ] || fail "an unknown kind is all"
sh=$(html '/ops/pools?kind=self-hosted')
echo "$sh" | grep -qF '<a class="ptabs__tab" href="/ops/pools?kind=self-hosted" aria-current="page">Self-hosted <span class="muted">2</span></a>' \
    || fail "the Self-hosted tab"
echo "$sh" | grep -qF '<a class="ptabs__tab" href="/ops/pools">All <span class="muted">7</span></a>' || fail "the All tab"
echo "$sh" | grep -qF '<a href="/ops/pools?pool=worker%3Abgent&amp;kind=self-hosted">worker:bgent</a> <span class="muted">1/1</span>' \
    || fail "the pool links keep the kind"
echo "$sh" | grep -qF 'data-poll="/ops/pools/live?kind=self-hosted"' || fail "the poll keeps the filter"
echo "$sh" | grep -q 'class="stats' && fail "Fleet's cards without EC2 in view"
echo "$sh" | grep -q 'aria-label="Fleet CPU"' && fail "Fleet's charts without EC2 in view"
[ "$(curl -fsS "$base/api/pools?kind=ec2" | jq -c '[.machines[].kind] | unique')" = '["ec2"]' ] || fail "/api/pools?kind=ec2"
[ "$(curl -fsS "$base/api/pools?pool=worker:bgent" | jq -c '[.filters.pool, [.machines[].name]]')" = '["worker:bgent",["w-lap"]]' ] \
    || fail "/api/pools?pool=worker:bgent"
echo "ok   Pools filters by pool and kind"

# Live machines by default: the gone ones left out sit behind a link that
# keeps the filters, a machine gone for under an hour stays, dimmed, and
# a pool with nothing live says so.
sh=$(html '/ops/pools?kind=self-hosted')
echo "$sh" | grep -qF '<p class="muted">1 gone machine, never seen · <a href="/ops/pools?kind=self-hosted&amp;gone=1">show all</a></p>' \
    || fail "the show-all line: $(echo "$sh" | grep -o '[0-9] gone machine[^<]*<a[^>]*>[^<]*')"
echo "$sh" | grep -qF 'data-poll="/ops/pools/live?kind=self-hosted"' || fail "the poll without ?gone"
sh=$(html '/ops/pools?pool=worker%3Abgent&kind=self-hosted')
echo "$sh" | grep -q 'gone machine' && fail "a gone line for a pool with none"
echo "$sh" | grep -qF '<code>w-lap</code>' || fail "the live worker"
sh=$(html '/ops/pools?kind=self-hosted&gone=1')
echo "$sh" | grep -qF '<a href="/ops/pools?kind=self-hosted">hide gone</a>' || fail "the hide-gone link"
echo "$sh" | grep -qF 'data-poll="/ops/pools/live?kind=self-hosted&amp;gone=1"' || fail "the poll keeps ?gone=1"
echo "$sh" | grep -qF '<a class="ptabs__tab" href="/ops/pools?gone=1">All <span class="muted">7</span></a>' || fail "the tabs keep ?gone=1"
echo "$sh" | grep -qF '<a href="/ops/pools?pool=worker%3Abgent&amp;kind=self-hosted&amp;gone=1">worker:bgent</a>' || fail "the pool links keep ?gone=1"
echo "$(html '/ops/pools?pool=ec2')" | grep -qF '<tr class="pools__gone"><td><a href="/ops/pools/ec2/i-gone">' || fail "the recently ended instance isn't a dimmed row"
# The empty state: a pool whose only machine is gone for good.
sh=$(html '/ops/pools?pool=worker%3Agh')
echo "$sh" | grep -qF '<p class="card__none">No machines running in worker:gh. 1 ran here before (never seen): <a href="/ops/pools?pool=worker%3Agh&amp;gone=1">show them</a>.</p>' \
    || fail "the empty state: $(echo "$sh" | grep -o 'No machines[^/]*/a>')"
echo "$sh" | grep -q '<table class="hosts pools">' && fail "a table with nothing live"
# A machine heard from under an hour ago stays after it goes gone; an old
# one leaves.
q "UPDATE orca.workers SET revoked = now() WHERE id = $bid" > /dev/null
q "UPDATE orca.workers SET last_seen = now() - interval '3 hours' WHERE id = $bid" > /dev/null
sh=$(html '/ops/pools?pool=worker%3Agh')
echo "$sh" | grep -qF 'last seen 3 h ago' || fail "the old gone worker's last seen: $(echo "$sh" | grep -o 'No machines[^/]*/a>')"
q "UPDATE orca.workers SET last_seen = now() - interval '5 minutes' WHERE id = $bid" > /dev/null
sh=$(html '/ops/pools?pool=worker%3Agh')
echo "$sh" | grep -qF '<tr class="pools__gone"><td><a href="/ops/pools/self-hosted/'"$bid"'">' || fail "a worker gone 5 minutes ago isn't shown, dimmed"
echo "$sh" | grep -q 'gone machine' && fail "a gone line for a recent one"
q "UPDATE orca.workers SET revoked = NULL, last_seen = NULL WHERE id = $bid" > /dev/null
# /api/pools has every machine; ?gone=0 matches the default view.
[ "$(curl -fsS "$base/api/pools" | jq '.machines | length')" = 7 ] || fail "/api/pools isn't every machine"
[ "$(curl -fsS "$base/api/pools?gone=0" | jq -r '[.machines[].name] | join(" ")')" = "i-head i-w1 i-w2 i-w3 i-gone w-lap" ] \
    || fail "/api/pools?gone=0: $(curl -fsS "$base/api/pools?gone=0" | jq -c '[.machines[].name]')"
[ "$(curl -fsS "$base/api/pools?gone=0" | jq -c '.pools | length')" = 3 ] || fail "/api/pools?gone=0 drops pools' counts"
echo "ok   Pools lists live machines, gone ones behind ?gone=1; /api/pools?gone=0"

# The live part, as a fragment that never ends.
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/ops/pools/live")" = 200 ] || fail "the fragment doesn't answer 200"
html /ops/pools/live | grep -q '<html' && fail "the fragment is a page"
html /ops/pools/live | grep -qF '<span class="stat__v">5</span>' || fail "the fragment's cards"
[ "$(rows '/ops/pools/live?kind=self-hosted')" = "w-lap " ] || fail "the fragment's filter"
[ "$(rows '/ops/pools/live?kind=self-hosted&gone=1')" = "w-lap box " ] || fail "the fragment's ?gone=1"
html '/ops/pools/live?kind=self-hosted' | grep -qF 'aria-label="About Last heartbeat"' || fail "the fragment's column help"
q "UPDATE orca.workers SET revoked = now() WHERE id = $wid" > /dev/null
html '/ops/pools/live?kind=self-hosted' | grep -qF '<span class="muted">revoked</span>' || fail "the fragment doesn't follow a revoke"
q "UPDATE orca.workers SET revoked = NULL WHERE id = $wid" > /dev/null
echo "ok   /ops/pools/live"

# Fleet and Workers moved to Pools for good (the range kept); asked for
# JSON, they answer what they did; the API paths didn't move.
for pair in '/ops/fleet|/ops/pools?kind=ec2' '/ops/fleet?range=7d&sort=cpu|/ops/pools?kind=ec2&range=7d' '/ops/fleet/hosts?sort=cpu|/ops/pools/live?kind=ec2' \
    '/ops/workers|/ops/pools?kind=self-hosted' '/ops/workers/live|/ops/pools/live?kind=self-hosted' \
    '/fleet|/ops/pools?kind=ec2' '/fleet?range=7d&sort=cpu|/ops/pools?kind=ec2&range=7d' '/fleet/hosts?sort=cpu|/ops/pools/live?kind=ec2' \
    '/system?range=7d|/ops/control?range=7d' '/ops|/ops/alerts'; do
    from=${pair%%|*}
    to=${pair##*|}
    got=$(curl -s -o /dev/null -w '%{http_code} %{redirect_url}' "$base$from")
    code=301
    [ "$from" != /ops ] || code=302
    [ "$got" = "$code $base$to" ] || fail "$from redirects to: $got"
done
curl -fsS -L "$base/ops/workers" | grep -qF '<h1>Pools</h1>' || fail "/ops/workers doesn't land on Pools"
[ "$(curl -s -o /dev/null -w '%{http_code}' -H 'Accept: application/json' "$base/ops/fleet")" = 200 ] || fail "/ops/fleet as JSON redirects"
json /ops/fleet | jq -e '.summary.hosts == 5 and (.hosts | length) == 5 and .charts.hours == 24' > /dev/null || fail "/ops/fleet's JSON via Accept"
[ "$(json /ops/fleet | jq -c '[.summary.hosts, [.hosts[].id]]')" \
    = "$(curl -fsS "$base/api/fleet" | jq -c '[.summary.hosts, [.hosts[].id]]')" ] || fail "/ops/fleet's JSON isn't /api/fleet's"
[ "$(curl -fsS "$base/ops/fleet?format=json&range=7d" | jq .charts.hours)" = 168 ] || fail "/ops/fleet?format=json&range=7d"
json '/ops/fleet?sort=cpu&dir=desc' | jq -e '[.hosts[] | select(.id != "control") | .id] == ["i-head", "i-w1", "i-w2", "i-w3"]' > /dev/null \
    || fail "/ops/fleet's JSON sorted: $(json '/ops/fleet?sort=cpu&dir=desc' | jq -c '[.hosts[].id]')"
json /ops/workers | jq -e '.workers | map(.label) | sort == ["box", "lap"]' > /dev/null || fail "/ops/workers' JSON"
[ "$(json /ops/workers | jq -c .)" = "$(curl -fsS "$base/api/workers" | jq -c .)" ] || fail "/ops/workers' JSON isn't /api/workers'"
curl -fsS "$base/api/workers" | jq -e '.workers[] | select(.label == "lap") | .name == "w-lap" and .cpus == 8 and .pool == "bgent"' > /dev/null \
    || fail "/api/workers: $(curl -fsS "$base/api/workers")"
echo "ok   /ops/fleet and /ops/workers redirect to Pools and keep their JSON; /fleet, /fleet/hosts and /system redirect; /ops goes to Control"

# The home page: a strip of the same numbers, each linking to its detail.
page=$(html /)
echo "$page" | grep -qF '<div class="stats stats--compact">' || fail "no strip on /"
echo "$page" | grep -qF '<a class="stat" href="/ops/pools?kind=ec2#machines"><span class="stat__k">Hosts' || fail "the strip's hosts link"
echo "$page" | grep -qF 'href="/runs?status=failed"' || fail "the strip's errors link"
json / | jq -e '.fleet.hosts == 5' > /dev/null || fail "/'s JSON has no fleet"

# A value from the database is escaped.
q "UPDATE orca.instances SET instance_type = '<b>x</b>' WHERE id = 'i-w3'" > /dev/null
q "UPDATE orca.workers SET name = '<i>w</i>' WHERE id = $bid" > /dev/null
html /ops/pools | grep -qF '<b>x</b>' && fail "an instance type's markup reached the page"
html /ops/pools | grep -qF '&lt;b&gt;x&lt;/b&gt;' || fail "the escaped instance type"
html '/ops/pools?gone=1' | grep -qF '<i>w</i>' && fail "a worker's name's markup reached the page"
html '/ops/pools?gone=1' | grep -qF '&lt;i&gt;w&lt;/i&gt;' || fail "the escaped worker name"
# On the local runner the page says that no instances are launched; on others
# it doesn't.
html /ops/pools | grep -q 'launches no instances' && fail "the local-runner notice on a scheduler with no runner set"
free_port lport
MORCA_FIXED_COST="€8.49/month" MORCA_RUNNER=local MORCA_UI_ADDR="127.0.0.1:$lport" ./build/morca ui >> "$log" 2>&1 &
lpid=$!
i=0
until curl -fs "http://127.0.0.1:$lport/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { kill "$lpid"; fail "the second morca ui didn't start"; }; sleep 0.1
done
lpage=$(curl -fsS "http://127.0.0.1:$lport/ops/pools")
lhome=$(curl -fsS "http://127.0.0.1:$lport/")
kill "$lpid"
wait "$lpid" 2> /dev/null || true
echo "$lpage" | grep -q 'runner is <code>local</code>: it runs every run on its own machine and launches no instances' \
    || fail "no local-runner notice on /ops/pools"
for pg in "$lpage" "$lhome"; do
    echo "$pg" | grep -q 'stat__k">Cost</span><span class="stat__v">€8.49/month</span><span class="stat__s">fixed price: the local runner launches no instances' \
        || fail "the local runner's cost tile"
    echo "$pg" | grep -q 'us-east-2\|Cost now\|on-demand' && fail "EC2 price text on the local runner's page"
done
html /ops/pools | grep -q 'Cost now' || fail "the EC2 cost tile is gone on another runner"
echo "ok   Ops > Pools says the local runner launches no instances"

# On the Kubernetes runner, a live run of the default placement is a
# machine of pool kubernetes: its pods, as the runner names them.
q "UPDATE orca.runs SET status = 'running', started = now(), heartbeat = now(), cpu_millis = 1500 WHERE id = $run" > /dev/null
starts=$(q "SELECT starts FROM orca.runs WHERE id = $run")
free_port kport
MORCA_RUNNER=kubernetes MORCA_UI_ADDR="127.0.0.1:$kport" ./build/morca ui >> "$log" 2>&1 &
kpid=$!
i=0
until curl -fs "http://127.0.0.1:$kport/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || { kill "$kpid"; fail "the third morca ui didn't start"; }; sleep 0.1
done
kapi=$(curl -fsS "http://127.0.0.1:$kport/api/pools?kind=k8s")
kpage=$(curl -fsS "http://127.0.0.1:$kport/ops/pools?pool=kubernetes")
kmachine=$(curl -fsS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$kport/ops/pools/k8s/morca-$run-$starts")
kill "$kpid"
wait "$kpid" 2> /dev/null || true
echo "$kapi" | jq -e --arg n "morca-$run-$starts" --argjson r "$run" '(.machines | length) == 1 and (.machines[0] | .name == $n and .pool == "kubernetes"
    and .state == "live" and .cpus == 2 and .runs == [$r] and .process == "pods")' > /dev/null || fail "the kubernetes pool: $kapi"
echo "$kpage" | grep -qF "<code>morca-$run-$starts</code>" || fail "the pods' row"
[ "$kmachine" = 200 ] || fail "the pods' page answered $kmachine"
[ "$(curl -fsS "$base/api/pools?kind=k8s" | jq '.machines | length')" = 0 ] || fail "pods on another runner"
echo "ok   Pools lists a Kubernetes run's pods"
kill "$pid"
echo "ok   fleet"
