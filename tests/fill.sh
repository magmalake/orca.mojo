#!/bin/sh
# Fills (morca.fill, #155): a pipeline filling its whole range by itself,
# within limits, with MORCA_NOW stepping time. Over examples/daily.mojo
# (from 2024-01-01, 02:30 New York time for the day before, max_runs 1) on
# the local runner:
#
# - a backfill's queued runs start first, and the fill waits for them;
# - fill runs go newest first (fill_order), fill_max_runs at once on top of
#   max_runs, skip a day a manual run did, and a fire's scheduled run
#   starts beside them; the fill ends complete, no done day run again;
# - outside fill_window nothing starts, inside it does;
# - paused by a person, or by the watchdog, no fill run starts;
# - over fill_budget_per_day (an instance a fill run had today) none
#   starts, and the next UTC day they do again;
# - fills = "all" is refused for a pipeline with no first partition, with
#   the file and line;
# - `morca config show` and `config export` have the keys, and
#   `/api/pipelines/<name>` the fill's numbers and why it's idle;
# - with fill_lag a day ended less than the lag ago neither fills nor
#   counts, and the page says when it fills.
#
# Then over examples/toy.mojo on the ec2 runner against the stand-in EC2
# (tests/fake_ec2.py): an instance type with no price, or no budget at
# all, starts nothing (the budget fails closed); a budget smaller than a
# run's estimate starts nothing; a big enough one starts a fill run on its
# own head instance, which succeeds.
#
# Needs MORCA_DATABASE_URL (pixi run test-fill starts a throwaway server),
# curl, jq and python3.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
one_cluster_test
mkdir -p build
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/daily
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/fanout.mojo -o build/fanout
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/fill.log
: > "$log"
proj=$PWD/build/fill-project
dir=$PWD/build/fill-ec2
rm -rf "$proj" "$dir" build/fill-logs
mkdir -p "$proj" "$dir"
export MORCA_WATCHDOG_SECONDS=0 MORCA_SAMPLE_SECONDS=0
ui=
fake=
sched=
stop() {
    [ -z "$sched" ] || kill "$sched" 2> /dev/null || true
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    [ -z "$fake" ] || kill "$fake" 2> /dev/null || true
    sched=
    ui=
    fake=
}
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    stop
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
now=
tick() {
    now=$1
    MORCA_NOW="$1" ./build/morca tick >> "$log" 2>&1 || fail "tick at $1 exited $?"
    ui_at
}
toml() { cat > "$proj/morca.toml"; }
reg() {  # reg TIME BINARY [FLAGS...]
    t=$1
    shift
    MORCA_NOW="$t" ./build/morca register --config "$proj/morca.toml" "$@" >> "$log" 2>&1 \
        || fail "register $* at $t"
}
fills() { q "SELECT coalesce(string_agg(partitions, ',' ORDER BY id), '') FROM orca.runs WHERE pipeline = '$1' AND trigger = 'fill'"; }
running() { q "SELECT count(*) FROM orca.runs WHERE pipeline = '$1' AND status = 'running'"; }
quiet() {  # until no run is running
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE status = 'running'")" -gt 0 ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "runs didn't finish"; sleep 0.1
    done
}
zero() { awk -v x="$1" 'BEGIN { exit !(x + 0 == 0) }'; }
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('running', 'queued')")" -gt 0 ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "runs didn't finish"; sleep 0.1
    done
}

# The UI on the last tick's clock: started again, on a new port, when
# that moves. Called in this shell, never in a $(...) subshell, so $ui is
# always the one running.
ui_now=
base=
ui_at() {
    [ "$ui_now" != "$now" ] || return 0
    [ -z "$ui" ] || { kill "$ui" 2> /dev/null || true; wait "$ui" 2> /dev/null || true; }
    free_port port
    base="http://127.0.0.1:$port"
    MORCA_NOW="$now" MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
    ui=$!
    ui_now=$now
    i=0
    until curl -fs "$base/" > /dev/null 2>&1; do
        i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
    done
}
# fill PIPELINE FIELD: the fill's field in /api/pipelines/<pipeline>.
fill() {
    curl -fsS "$base/api/pipelines/$1" | jq -r ".fill.$2" || fail "GET /api/pipelines/$1"
}
idle_has() {  # idle_has PIPELINE TEXT
    got=$(fill "$1" idle)
    case "$got" in *"$2"*) ;; *) fail "$1's fill is idle for '$got', not '$2'" ;; esac
}

# --- Order, max runs, other runs first, a scheduled run beside them ---
toml << 'EOF'
[pipelines.daily]
fills = "all"
fill_order = "newest"
fill_max_runs = 2
EOF
T0=2024-01-08T12:00Z  # 07:00 in New York: the range is 2024-01-01..07
reg "$T0" ./build/daily
grep -q "daily fills its range with no fill_budget_per_day" "$log" || fail "no warning of no budget"
./build/daily run --partitions 2024-01-03 >> "$log" 2>&1 || fail "the manual run of 2024-01-03"
[ "$(./build/morca backfill daily 2024-01-01..2024-01-02 2>> "$log")" = "queued 2 runs" ] || fail "the backfill"
export MORCA_DAILY_SLEEP=3
tick "$T0"
[ "$(fills daily)" = "" ] || fail "fill runs started before the queued backfill: $(fills daily)"
idle_has daily "1 scheduled or backfill runs are queued"
quiet
tick 2024-01-08T12:01Z
[ "$(fills daily)" = "2024-01-07,2024-01-06" ] || fail "the first fill runs: $(fills daily)"
[ "$(running daily)" -eq 3 ] || fail "$(running daily) running, not a backfill run and 2 fill runs"
# Each pass's fill runs, and the backfill's, are a batch (#354).
[ "$(q "SELECT count(DISTINCT batch) || ' ' || count(*) FILTER (WHERE batch = (SELECT min(id) FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'fill')) FROM orca.runs WHERE pipeline = 'daily' AND trigger IN ('fill', 'backfill')")" = "2 2" ] \
    || fail "the fill and backfill batches: $(q "SELECT string_agg(id || ':' || trigger || ':' || coalesce(batch::text, '-'), ' ' ORDER BY id) FROM orca.runs WHERE pipeline = 'daily'")"
tick 2024-01-08T12:02Z
[ "$(fills daily)" = "2024-01-07,2024-01-06" ] || fail "more than fill_max_runs: $(fills daily)"
idle_has daily "at fill_max_runs: 2 of 2"
[ "$(fill daily running)" -eq 2 ] || fail "the API's running"
settle
# The fire for 2024-01-08 starts its run; the fill goes on beside it.
tick 2024-01-09T07:31Z
[ "$(q "SELECT status FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'schedule'")" = running ] \
    || fail "the scheduled run didn't start beside the fill"
[ "$(fills daily)" = "2024-01-07,2024-01-06,2024-01-05,2024-01-04" ] || fail "the next fill runs: $(fills daily)"
settle
tick 2024-01-09T07:32Z
[ "$(fills daily)" = "2024-01-07,2024-01-06,2024-01-05,2024-01-04" ] || fail "a done day filled again: $(fills daily)"
[ "$(fill daily done)" -eq 8 ] && [ "$(fill daily total)" -eq 8 ] || fail "done $(fill daily done) of $(fill daily total)"
idle_has daily complete
[ "$(q "SELECT count(*) FROM orca.runs WHERE trigger = 'fill' AND status <> 'succeeded'")" -eq 0 ] \
    || fail "a fill run didn't succeed"
zero "$(fill daily rate_per_hour)" && fail "no rate"
curl -fsS "$base/pipelines/daily" | grep -q 'aria-label="About Fill".*</dt><dd>8 / 8 done (100%)' || fail "the page's fill line"
curl -fsS "$base/pipelines/daily/progress" | grep -q '</dt><dd>8 / 8 done (100%)' || fail "the progress fragment's fill line"
echo "ok   fills newest first, 2 at once beside max_runs, after a backfill, beside a fire, never a done day"

# --- The window ---
toml << 'EOF'
[pipelines.daily]
fills = "all"
fill_max_runs = 3
fill_window = "22..6"
EOF
T2=2024-01-20T12:00Z  # the range is now 2024-01-01..19; the fire is 2024-01-19's
reg "$T2" ./build/daily
tick "$T2"
[ "$(fills daily | tr ',' '\n' | wc -l | tr -d ' ')" -eq 4 ] || fail "fill runs outside the window: $(fills daily)"
idle_has daily "outside its window: fill runs start 22:00–06:00 UTC"
settle
tick 2024-01-20T23:00Z
[ "$(fills daily)" = "2024-01-07,2024-01-06,2024-01-05,2024-01-04,2024-01-09,2024-01-10,2024-01-11" ] \
    || fail "oldest first in the window: $(fills daily)"
echo "ok   nothing starts outside fill_window, oldest first inside it"

# --- Pausing, by a person and by the watchdog ---
settle
./build/morca pause daily >> "$log" 2>&1 || fail "pause"
tick 2024-01-20T23:10Z
[ "$(fills daily | tr ',' '\n' | wc -l | tr -d ' ')" -eq 7 ] || fail "a fill run started while paused"
idle_has daily paused
./build/morca unpause daily >> "$log" 2>&1 || fail "unpause"
tick 2024-01-20T23:11Z
[ "$(fills daily | tr ',' '\n' | wc -l | tr -d ' ')" -eq 10 ] || fail "no fill after unpausing: $(fills daily)"
settle
# A rule over its limit: more than -1 launches in the last hour.
MORCA_WATCHDOG_SECONDS=60 MORCA_WATCHDOG_LAUNCHES=-1 MORCA_NOW=2024-01-20T23:20Z ./build/morca tick >> "$log" 2>&1 \
    || fail "the watchdog's tick"
now=2024-01-20T23:20Z
ui_at
[ "$(q "SELECT paused FROM orca.pipelines WHERE name = 'daily'")" = t ] || fail "the watchdog didn't pause daily"
[ "$(fills daily | tr ',' '\n' | wc -l | tr -d ' ')" -eq 10 ] || fail "a fill run started after the watchdog tripped"
idle_has daily "paused by the watchdog (watchdog rule launches"
./build/morca unpause daily >> "$log" 2>&1 || fail "unpause after the watchdog"
echo "ok   no fill run starts while paused, by a person or by the watchdog"

# --- The budget, a UTC day at a time ---
toml << 'EOF'
[pipelines.daily]
fills = "all"
fill_max_runs = 3
fill_window = "22..6"
fill_budget_per_day = 0.05
EOF
reg 2024-01-20T23:30Z ./build/daily
# A fill run's instance today: 2 hours of a c7g.xlarge, $0.29, $0.35 with
# the margin.
q "INSERT INTO orca.instances (id, run, start, role, instance_type, market, launched, ended)
   SELECT 'i-fill', min(id), 1, 'head', 'c7g.xlarge', 'on-demand',
     to_timestamp(1705793400) - interval '2 hours', to_timestamp(1705793400)
   FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'fill'" > /dev/null
tick 2024-01-20T23:30Z
[ "$(fills daily | tr ',' '\n' | wc -l | tr -d ' ')" -eq 10 ] || fail "a fill run started over the budget"
idle_has daily 'budget: $0.35 of $0.05 spent today (UTC)'
[ "$(fill daily spent_today)" = 0.348 ] || fail "spent today: $(fill daily spent_today)"
tick 2024-01-21T00:30Z
[ "$(fills daily | tr ',' '\n' | wc -l | tr -d ' ')" -eq 13 ] || fail "no fill on the next day: $(fills daily)"
zero "$(fill daily spent_today)" || fail "spent on the next day: $(fill daily spent_today)"
settle
echo "ok   over fill_budget_per_day nothing starts, and the next UTC day it does"

# --- What registration refuses, and what config shows ---
printf '[pipelines.fanout]\n\nfills = "all"\n' | toml
MORCA_NOW=$T0 ./build/morca register --config "$proj/morca.toml" ./build/fanout > build/fill.out 2>&1 \
    && fail "fills = all registered for a pipeline with no first partition"
grep -q "morca.toml:3: \[pipelines.fanout\] fills = \"all\" can't be registered: fanout has no first partition" build/fill.out \
    || fail "the refusal: $(cat build/fill.out)"
./build/morca config show daily > build/fill.out 2>> "$log" || fail "config show"
grep -Eq "^  fills +all +morca.toml \[pipelines.daily\]$" build/fill.out || fail "show's fills: $(cat build/fill.out)"
grep -Eq "^  fill_window +22..6 +morca.toml \[pipelines.daily\]$" build/fill.out || fail "show's fill_window"
grep -Eq "^  fill_order +oldest +code default" build/fill.out || fail "show's fill_order"
./build/morca config export --out - > build/fill.out 2>> "$log" || fail "config export"
grep -q '^fill_budget_per_day = 0.05$' build/fill.out && grep -q '^fill_window = "22..6"$' build/fill.out \
    || fail "export: $(cat build/fill.out)"
echo "ok   fills refused with no first partition; config show and export"

# --- The lag: a day fills fill_lag after it ends ---
toml << 'EOF'
[pipelines.daily]
fills = "all"
fill_max_runs = 3
fill_lag = "120m"
EOF
# New York's 2024-01-22 00:00 is 05:00Z: 2024-01-21 ended then, and fills
# from 07:00Z.
reg 2024-01-22T06:59:59Z ./build/daily
tick 2024-01-22T06:59:59Z
settle
tick 2024-01-22T06:59:59Z
case ",$(fills daily)," in *,2024-01-21,*) fail "2024-01-21 filled inside its lag" ;; esac
[ "$(fill daily total)" -eq 20 ] || fail "the range in the lag: $(fill daily total) days"
[ "$(fill daily waits_for_lag.partition)" = 2024-01-21 ] || fail "waits_for_lag: $(fill daily waits_for_lag)"
[ "$(fill daily waits_for_lag.fillable_at)" = 2024-01-22T07:00:00Z ] || fail "fillable_at: $(fill daily waits_for_lag)"
curl -fsS "$base/pipelines/daily" | grep -q 'Next: 2024-01-21 fills from 2024-01-22 07:00 UTC (fill_lag 2h)' \
    || fail "the page's Next line"
[ "$(curl -fsS "$base/api/pipelines/daily" | jq -r .pipeline.coverage.total)" -eq 20 ] || fail "coverage counts the lagged day"
i=0
until case ",$(fills daily)," in *,2024-01-21,*) true ;; *) false ;; esac; do
    i=$((i + 1)); [ "$i" -lt 10 ] || fail "2024-01-21 didn't fill after its lag: $(fills daily)"
    settle
    tick "2024-01-22T07:0${i}:00Z"
done
[ "$(fill daily waits_for_lag)" = null ] || fail "still waits: $(fill daily waits_for_lag)"
[ "$(fill daily total)" -eq 21 ] || fail "the range after the lag: $(fill daily total) days"
settle
./build/morca config show daily > build/fill.out 2>> "$log" || fail "config show"
grep -Eq "^  fill_lag +2h +morca.toml \[pipelines.daily\]$" build/fill.out || fail "show's fill_lag: $(cat build/fill.out)"
./build/morca config export --out - > build/fill.out 2>> "$log" || fail "config export"
grep -q '^fill_lag = "2h"$' build/fill.out || fail "export's fill_lag: $(cat build/fill.out)"
echo "ok   fill_lag: a day fills two hours after it ends, not before; show and export"

# --- The ec2 runner: the budget fails closed ---
free_port ecport
export MORCA_RUNNER=ec2 MORCA_EC2_MODE=direct MORCA_EC2_ENDPOINT="http://127.0.0.1:$ecport"
export AWS_REGION=us-east-2 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test MORCA_EC2_AMI=ami-test
export MORCA_EC2_POOL_SECONDS=0 MORCA_LOGS=$PWD/build/fill-logs MORCA_START_SECONDS=60
python3 tests/fake_ec2.py "$ecport" "$dir" >> "$log" 2>&1 &
fake=$!
i=0
until curl -s -o /dev/null "$MORCA_EC2_ENDPOINT/"; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "the stand-in EC2 didn't start"; sleep 0.1
done
launches() { grep -c '"RunInstances"' "$dir/calls.jsonl" 2> /dev/null || true; }
T3=2024-03-15T00:00Z  # the range is 2024-01..02
toml << 'EOF'
[pipelines.toy]
placement = "ec2"
head_type = "m8g.xlarge"
fills = "all"
fill_budget_per_day = 1.0
EOF
reg "$T3" --image morca:test ./build/toy
tick "$T3"
[ "$(fills toy)" = "" ] || fail "a fill run started at an unknown price"
idle_has toy "price unknown: no price for m8g.xlarge in morca.fleet's table"
[ "$(fill toy price_per_hour)" = null ] || fail "a price for m8g.xlarge"
toml << 'EOF'
[pipelines.toy]
placement = "ec2"
head_type = "c7g.large"
fills = "all"
EOF
reg "$T3" --image morca:test ./build/toy
tick "$T3"
[ "$(fills toy)" = "" ] || fail "a fill run started on ec2 with no budget"
idle_has toy "no budget: its runs on ec2 cost money"
printf '[pipelines.toy]\nplacement = "ec2"\nhead_type = "c7g.large"\nfills = "all"\nfill_budget_per_day = 0.05\n' | toml
reg "$T3" --image morca:test ./build/toy
tick "$T3"
[ "$(fills toy)" = "" ] || fail "a fill run started with a budget below its estimate"
# An hour of a c7g.large ($0.0725) with the margin, before any has ended.
idle_has toy 'budget: $0.00 of $0.05 spent today (UTC), and the runs going and one more may cost $0.09'
[ "$(launches)" -eq 0 ] || fail "instances launched over the budget"
printf '[pipelines.toy]\nplacement = "ec2"\nhead_type = "c7g.large"\nfills = "all"\nfill_budget_per_day = 1\n' | toml
reg "$T3" --image morca:test ./build/toy
tick "$T3"
[ "$(fills toy)" = "2024-01" ] || fail "no fill run on ec2 within the budget: $(fills toy)"
[ "$(launches)" -eq 1 ] || fail "$(launches) launches for the fill run"
grep '"RunInstances"' "$dir/calls.jsonl" | grep -q '"InstanceType": "c7g.large"' || fail "the head's type"
MORCA_NOW=$T3 ./build/morca scheduler >> "$log" 2>&1 &
sched=$!
i=0
until [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'toy' AND trigger = 'fill' AND status = 'succeeded'")" -eq 2 ]; do
    i=$((i + 1)); [ "$i" -lt 600 ] || fail "the toy's fill runs on ec2: $(q "SELECT id, partitions, status, error FROM orca.runs WHERE pipeline = 'toy'")"; sleep 0.2
done
[ "$(fills toy)" = "2024-01,2024-02" ] || fail "the toy's fill: $(fills toy)"
idle_has toy complete
spent=$(fill toy spent_today)
[ "$spent" != null ] || fail "the toy's spend is unknown"
echo "ok   ec2: an unknown price or no budget starts nothing, a budget below the estimate neither, one above it does"
stop
echo "ok   fill"
