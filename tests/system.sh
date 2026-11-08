#!/bin/sh
# The control instance's own health (orca.system): the scheduler rolls the
# lines of the control sampler's file into a row a minute in Postgres (a pass
# again adds nothing, old rows go) and appends them to a file per day under
# ORCA_LOGS/control; /ops/control, /api/system, /metrics and the warning on / are
# drawn from those rows alone (the test deletes both files before fetching).
# Needs ORCA_DATABASE_URL (pixi run test-system starts a throwaway server),
# curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/system.log
: > "$log"
export ORCA_LOGS=build/system-logs
rm -rf "$ORCA_LOGS"
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    exit 1
}

control=$PWD/build/system-control.jsonl
rm -f "$control" "$control.next"
# $1 more lines, 5 s apart, each batch following the one before and the
# first starting 1700 s ago, so the last ends about now: CPU 30%, memory $2 %,
# disk $3 %, and five containers (one with markup in its name).
lines() {
    python3 - "$1" "$2" "$3" "$control.next" >> "$control" << 'PY'
import json, os, sys, time
n, mem, disk, nxt = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
start = float(open(nxt).read()) if os.path.exists(nxt) else time.time() - 1700
for k in range(n):
    t = start + 5 * k
    print(json.dumps({"v": 1, "t": t, "dt": 5.0, "host": "ctl", "cores": 2,
        "cpu": {"user": 28.0, "system": 2.0, "iowait": 0, "steal": 0, "busiest": 60},
        "mem": {"total": 4000000000, "used": int(40000000 * mem), "available": 1, "cache": 0, "swap_total": 0, "swap_used": 0, "rss": 0},
        "disk": [{"path": "/var/lib/orca/stage", "total": 20000000000, "used": int(200000000 * disk), "free": 1}],
        "io": {"read": 0, "write": 0}, "net": {"in": 2000000, "out": 500000},
        "containers": [{"name": "postgres", "mem": 600000000 + k}, {"name": "scheduler", "mem": 300000000},
                       {"name": "ui", "mem": 150000000}, {"name": "caddy", "mem": 40000000},
                       {"name": "<b>app</b>", "mem": 90000000}],
        "self_cpu": 0.0}))
open(nxt, "w").write(str(start + 5 * n))
PY
}
total_lines() { cat "$ORCA_LOGS"/control/*.jsonl 2> /dev/null | wc -l | tr -d ' '; }

# Runs, for the counts on /metrics.
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
ORCA_NOW=2024-01-15T00:00Z ./build/orca register ./build/toy >> "$log" 2>&1 || fail "register"
./build/toy run --partitions 2024-01 >> "$log" 2>&1 || fail "the first run"
ORCA_TOY_FAIL=2024-01 ./build/toy run --partitions 2024-01 >> "$log" 2>&1 && fail "the second run didn't fail"

# 25 minutes of a healthy machine, up to a few seconds ago.
lines 300 50 40
export ORCA_CONTROL_SAMPLES=$control ORCA_CONTROL_TYPE=t4g.medium
tick() { ORCA_NOW=2024-01-15T00:00Z ORCA_RUNNER=local ORCA_FLEET_SECONDS=1 ./build/orca tick >> "$log" 2>&1 || fail "tick"; }
tick
[ "$(q "SELECT sum(n) FROM orca.system_minutes")" -eq 300 ] || fail "samples in the minutes: $(q "SELECT sum(n) FROM orca.system_minutes")"
rows=$(q "SELECT count(*) FROM orca.system_minutes")
{ [ "$rows" -ge 25 ] && [ "$rows" -le 27 ]; } || fail "$rows minutes for 25"
[ "$(q "SELECT count(DISTINCT name) FROM orca.system_containers")" -eq 5 ] || fail "containers"
[ "$(q "SELECT max(mem) FROM orca.system_containers WHERE name = 'postgres'")" -eq 600000299 ] || fail "a container's peak in its minute"
[ "$(q "SELECT round(avg(cpu_sum / n)) FROM orca.system_minutes")" -eq 30 ] || fail "the CPU average"
[ "$(total_lines)" -eq 300 ] || fail "the day files have $(total_lines) lines"
for f in "$ORCA_LOGS"/control/*.jsonl; do
    case "$f" in */????-??-??.jsonl) ;; *) fail "a day file named $f" ;; esac
done
# The same pass again adds nothing, to the rows or the files.
tick
[ "$(q "SELECT sum(n) FROM orca.system_minutes")" -eq 300 ] || fail "a second pass added samples"
[ "$(total_lines)" -eq 300 ] || fail "a second pass appended to the day files"
# More lines are added to their minute and the day.
lines 12 50 40
tick
[ "$(q "SELECT sum(n) FROM orca.system_minutes")" -eq 312 ] || fail "new samples: $(q "SELECT sum(n) FROM orca.system_minutes")"
[ "$(total_lines)" -eq 312 ] || fail "the day files have $(total_lines) lines, not 312"
echo "ok   the scheduler keeps the control instance's samples, once, by the minute and the day"

# Old rows go on a pass, and old local day files.
q "INSERT INTO orca.system_minutes VALUES (now() - interval '15 days', 1, now() - interval '15 days', 1, 1, 1, 1, 1, 1, 1, 1, 1, '/', 1, 1)" > /dev/null
q "INSERT INTO orca.system_containers VALUES (now() - interval '15 days', 'old', 1)" > /dev/null
echo '{}' > "$ORCA_LOGS/control/2000-01-01.jsonl"
tick
[ "$(q "SELECT count(*) FROM orca.system_minutes WHERE minute < now() - interval '14 days'")" -eq 0 ] || fail "old minutes were kept"
[ "$(q "SELECT count(*) FROM orca.system_containers WHERE minute < now() - interval '14 days'")" -eq 0 ] || fail "old containers were kept"
[ ! -e "$ORCA_LOGS/control/2000-01-01.jsonl" ] || fail "an old day file was kept"
echo "ok   rows and day files older than 14 days are dropped"

free_port port
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pid=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
html() { curl -fsS "$base$1" || fail "GET $1"; }

# Remove the files: the pages must not read one.
rm -rf "$ORCA_LOGS" "$control"

api=$(curl -fsS "$base/api/system")
echo "$api" | jq -e '.now | .age_seconds < 600 and (.cpu_pct > 29 and .cpu_pct < 31) and (.mem_pct > 49.9 and .mem_pct < 50.1)
    and (.disk_pct > 39.9 and .disk_pct < 40.1) and .disk_path == "/var/lib/orca/stage" and .disk_total_bytes == 20000000000
    and .mem_total_bytes == 4000000000 and .net_in_bytes_per_second == 2000000
    and .containers.postgres >= 600000000 and .containers.caddy == 40000000 and (.containers | length) == 5' > /dev/null \
    || fail "now: $(echo "$api" | jq -c .now)"
echo "$api" | jq -e '.warnings == []' > /dev/null || fail "a warning for a healthy machine"
echo "$api" | jq -e '.charts | .hours == 24 and .bucket_seconds == 300 and (.labels_utc | length) == 288
    and (.cpu_pct.average | length) == 288 and (.cpu_pct.average | max) > 29 and (.cpu_pct.peak | max) == 30
    and (.mem_pct.average | max) > 49 and (.disk_pct["fullest disk"] | max) > 39
    and (.network_mb_per_second["in (MB/s)"] | max) == 2 and (.container_mem_mb.postgres | max) >= 600
    and (.container_mem_mb | keys | length) == 5' > /dev/null \
    || fail "the charts: $(echo "$api" | jq -c '.charts | del(.labels_utc) | map_values(if type == "object" then map_values(max) else . end)')"
curl -fsS "$base/api/system?range=7d" | jq -e '.charts | .hours == 168 and .bucket_seconds == 1800 and (.labels_utc | length) == 336' > /dev/null || fail "?range=7d"
curl -fsS "$base/api/system?range=14d" | jq -e '.charts | .hours == 336 and .bucket_seconds == 3600 and (.labels_utc | length) == 336 and (.cpu_pct.peak | max) == 30' > /dev/null || fail "?range=14d"
[ "$(curl -fsS -o /dev/null -w '%{content_type}' "$base/api/system")" = "application/json" ] || fail "/api/system's content type"
echo "ok   /api/system"

page=$(html /ops/control)
echo "$page" | grep -qF '<a href="/ops/control" aria-current="page">Control</a>' || fail "Control isn't in the Ops menu, current"
echo "$page" | grep -q 'The one machine running the scheduler, this UI, Postgres and control-placed runs' || fail "the Control intro"
echo "$page" | grep -qF '<a href="/ops/pools">Pools</a>' || fail "the intro doesn't link Pools"
echo "$page" | grep -qF '<a href="/watchdog">Watchdog</a>' || fail "Control doesn't link /watchdog"
echo "$page" | grep -qF '<h1>System</h1>' && fail "the page is still called System"
echo "$page" | grep -qF '<span class="stat__k">CPU</span><span class="stat__v">30%</span>' || fail "the CPU card"
echo "$page" | grep -qF '<span class="stat__v">50%</span><span class="stat__s">2.0 GB of 4.0 GB' || fail "the memory card: $(echo "$page" | grep -o 'stat__k">Memory.\{0,150\}')"
echo "$page" | grep -qF '<span class="stat__v">40%</span><span class="stat__s">8.0 GB of 20 GB, under /var/lib/orca/stage' || fail "the disk card"
echo "$page" | grep -qF '<td>postgres</td>' || fail "the containers table"
echo "$page" | grep -qF '<b>app</b>' && fail "a container's markup reached the page"
echo "$page" | grep -qF '&lt;b&gt;app&lt;/b&gt;' || fail "the escaped container name"
for c in CPU Memory Disk Network 'Container memory'; do
    echo "$page" | grep -qF "aria-label=\"$c\"" || fail "no $c chart"
done
echo "$page" | grep -qF '<strong>24h</strong> <a href="/ops/control?range=7d">7d</a> <a href="/ops/control?range=14d">14d</a>' || fail "the range choice"
html '/ops/control?range=14d' | grep -qF '<strong>14d</strong>' || fail "the 14d range"
html '/ops/control?range=bogus' | grep -qF '<strong>24h</strong>' || fail "an unknown range is 24h"
echo "$page" | grep -q 'class="alert"' && fail "an alert for a healthy machine"
echo "ok   /ops/control"

metrics=$(curl -fsS "$base/metrics")
[ "$(curl -fsS -o /dev/null -w '%{content_type}' "$base/metrics")" = "text/plain; version=0.0.4; charset=utf-8" ] || fail "/metrics' content type"
want() { echo "$metrics" | grep -qE "$1" || fail "/metrics has no $1: $(echo "$metrics" | head -50)"; }
want '^# TYPE orca_control_cpu_percent gauge$'
want '^orca_control_cpu_percent 30\.00$'
want '^orca_control_memory_used_bytes 2000000000$'
want '^orca_control_memory_total_bytes 4000000000$'
want '^orca_control_memory_percent 50\.00$'
want '^orca_control_disk_used_bytes 8000000000$'
want '^orca_control_disk_total_bytes 20000000000$'
want '^orca_control_disk_percent 40\.00$'
want '^orca_control_network_in_bytes_per_second 2000000$'
want '^orca_control_network_out_bytes_per_second 500000$'
want '^orca_control_container_memory_bytes\{container="postgres"\} 6000[0-9]{5}$'
want '^orca_control_container_memory_bytes\{container="<b>app</b>"\} 90000000$'
want '^orca_control_sample_age_seconds [0-9]+$'
want '^orca_runs\{status="running"\} 0$'
want '^orca_runs\{status="queued"\} 0$'
want '^orca_fleet_hosts\{role="control"\} 1$'
want '^orca_fleet_hosts\{role="head"\} 0$'
want '^orca_runs_started_last_hour 2$'
want '^orca_errors_last_hour\{kind="runs"\} 1$'
want '^orca_errors_last_hour\{kind="step_attempts"\} [1-9]'
# Every sample line is `name{labels} value`, every name has its TYPE.
echo "$metrics" | grep -v '^#' | grep -vE '^[a-z_][a-z0-9_]*(\{[a-z_]+="[^"]*"\})? -?[0-9.]+$' && fail "a malformed line in /metrics"
for n in $(echo "$metrics" | grep -v '^#' | sed 's/[{ ].*//' | sort -u); do
    echo "$metrics" | grep -qxF "# TYPE $n gauge" || fail "no TYPE for $n"
done
echo "ok   /metrics"

# The home page warns about a full disk, and about memory, and not otherwise.
html / | grep -q 'class="alert"' && fail "a warning on / for a healthy machine"
curl -fsS -H 'Accept: application/json' "$base/" | jq -e '.warnings == []' > /dev/null || fail "/'s JSON warnings"
lines 12 50 95
export ORCA_CONTROL_SAMPLES=$control
tick
page=$(html /)
echo "$page" | grep -qF '<div class="alert" role="alert"><div>The control instance&#39;s disk is 95% full (19 GB of 20 GB)' \
    || fail "no disk warning on / for a disk 95% full: $(echo "$page" | grep -o 'class="alert".\{0,200\}')"
echo "$page" | grep -qF '<a href="/ops/control">Control health</a>' || fail "the warning doesn't link to /ops/control"
echo "$page" | grep -qi 'memory is' && fail "a memory warning at 50%"
curl -fsS -H 'Accept: application/json' "$base/" | jq -e '.warnings | length == 1' > /dev/null || fail "/'s JSON warnings for a full disk"
html /ops/control | grep -qF 'class="alert"' || fail "no warning on /ops/control"
curl -fsS "$base/metrics" | grep -qE '^orca_control_disk_percent (8[0-9]|9[0-9])\.' || fail "/metrics' disk gauge after the full disk: $(curl -fsS "$base/metrics" | grep disk_percent)"
lines 12 93 95
tick
curl -fsS -H 'Accept: application/json' "$base/" | jq -e '.warnings | length == 2 and (.[] | select(test("memory"))) != null' > /dev/null \
    || fail "no memory warning at 93%: $(curl -fsS -H 'Accept: application/json' "$base/" | jq -c .warnings)"
# Below the limits, there are none: 85% disk and 90% memory are not over.
q "UPDATE orca.system_minutes SET disk_max = 85, mem_sum = 90 * n, mem_max = 90 WHERE minute = (SELECT max(minute) FROM orca.system_minutes)" > /dev/null
curl -fsS -H 'Accept: application/json' "$base/" | jq -e '.warnings == []' > /dev/null || fail "a warning at exactly the limits: $(curl -fsS -H 'Accept: application/json' "$base/" | jq -c .warnings)"
echo "ok   a full disk or memory warns on /, at the thresholds and not below them"

# A sampler that has stopped: no warning for a stale full disk, and no gauges.
q "UPDATE orca.system_minutes SET disk_max = 99, last_t = now() - interval '20 minutes'" > /dev/null
html / | grep -q 'class="alert"' && fail "a warning from a stale sample"
curl -fsS "$base/metrics" | grep -q '^orca_control_' && fail "control gauges from a stale sample"
curl -fsS "$base/metrics" | grep -q '^orca_runs{status="running"} 0$' || fail "the run counts go with it"
html /ops/control | grep -qF 'minutes old: the sampler or the scheduler has stopped' || fail "/ops/control doesn't say its samples are old"
# And none at all: the page says how to start one.
q "TRUNCATE orca.system_minutes, orca.system_containers" > /dev/null
html /ops/control | grep -qF 'has not been sampled yet' || fail "/ops/control without samples"
curl -fsS "$base/api/system" | jq -e '.now == null and .warnings == []' > /dev/null || fail "/api/system without samples"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$base/metrics")" = 200 ] || fail "/metrics without samples"
echo "ok   stale and missing samples"
kill "$pid"
echo "ok   system"
