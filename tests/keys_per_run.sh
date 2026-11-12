#!/bin/sh
# keys_per_run for sensors and fills (#240), set in orca.toml over the code's:
#
# - `orca config show` gives it, from the file; without it in the file, the
#   code's (1) again;
# - a time-partition sensor (tests/sensors' clean, monthly, kept paused so
#   its runs stay queued) puts a new snapshot's keys in its queued run that
#   hasn't started, up to keys_per_run, and only then queues another; a
#   snapshot of more keys than fit makes runs of keys_per_run, in key order;
# - with keys_per_run 1 a snapshot of many keys is still one run (as before);
# - a fill (examples/daily, fills = "all") takes keys_per_run partitions a
#   run, in fill_order, fill_max_runs runs at once, and fills its range.
#
# Needs ORCA_DATABASE_URL (pixi run -e data test-keys-per-run starts a throwaway
# server).
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
pids=
for p in source clean; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src -I tests/sensors -I tests/lineage $DEP_INC "tests/sensors/$p.mojo" -o "build/kpr-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC examples/daily.mojo -o build/kpr-daily &
pids="$pids $!"
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/orca &
for pid in $pids $!; do wait "$pid"; done
log=build/keys-per-run.log
: > "$log"
proj=$PWD/build/kpr-project
rm -rf "$proj" build/kpr-wh build/kpr-logs
mkdir -p "$proj"
export ORCA_WAREHOUSE=$PWD/build/kpr-wh ORCA_LOGS=$PWD/build/kpr-logs
export ORCA_SENSE_SECONDS=0 ORCA_WATCHDOG_SECONDS=0 ORCA_SAMPLE_SECONDS=0
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
sensed() { q "SELECT coalesce(string_agg(partitions, ' ' ORDER BY id), '') FROM orca.runs WHERE pipeline = 'clean' AND trigger = 'sensor'"; }
source_commit() { ./build/kpr-source "$ORCA_WAREHOUSE" "$@" > /dev/null 2>> "$log" || fail "source $*"; }
tick() { ORCA_NOW=${1:-2024-12-15T00:00Z} ./build/orca tick >> "$log" 2>&1 || fail "tick exited $?"; }
reg() { ./build/orca register --config "$proj/orca.toml" "$@" >> "$log" 2>&1 || fail "register $*"; }

# --- A sensor's runs: topped up, then batched ---
source_commit 2024-01
cat > "$proj/orca.toml" << 'EOF'
[pipelines.clean]
keys_per_run = 3
EOF
reg ./build/kpr-clean
./build/orca config show clean > build/kpr-show.txt 2>&1 || fail "config show"
grep -q "keys_per_run.*3.*orca.toml" build/kpr-show.txt || fail "config show: $(grep keys_per_run build/kpr-show.txt)"
./build/orca pause clean >> "$log" 2>&1 || fail "pause"
source_commit 2024-02
tick
[ "$(sensed)" = "2024-02" ] || fail "the first sensed run: $(sensed)"
source_commit 2024-03
tick
source_commit 2024-04
tick
[ "$(sensed)" = "2024-02,2024-03,2024-04" ] || fail "not topped up to 3: $(sensed)"
grep -q "takes 2024-03" "$log" || fail "no log of the top-up"
source_commit 2024-05
tick
[ "$(sensed)" = "2024-02,2024-03,2024-04 2024-05" ] || fail "a full run topped up: $(sensed)"
# One snapshot of more keys than fit: the queued run's room, then runs of 3.
source_commit 2024-06,2024-07,2024-08,2024-09,2024-10,2024-11
tick
[ "$(sensed)" = "2024-02,2024-03,2024-04 2024-05,2024-06,2024-07 2024-08,2024-09,2024-10 2024-11" ] \
    || fail "batches of a big snapshot: $(sensed)"
# The pass's new runs are one batch (#354); the one topped up isn't in it.
b=$(q "SELECT id FROM orca.runs WHERE pipeline = 'clean' AND partitions = '2024-08,2024-09,2024-10'")
[ "$(q "SELECT string_agg(coalesce(batch::text, '-'), ' ' ORDER BY id) FROM orca.runs WHERE pipeline = 'clean' AND trigger = 'sensor'")" = "- - $b $b" ] \
    || fail "the sensor pass's batch: $(q "SELECT string_agg(id || ':' || coalesce(batch::text, '-'), ' ' ORDER BY id) FROM orca.runs WHERE pipeline = 'clean'")"
# A started run isn't topped up: the next key gets a run of its own.
q "UPDATE orca.runs SET starts = 1 WHERE pipeline = 'clean' AND partitions = '2024-11'" > /dev/null
source_commit 2024-12
tick
[ "$(sensed)" = "2024-02,2024-03,2024-04 2024-05,2024-06,2024-07 2024-08,2024-09,2024-10 2024-11 2024-12" ] \
    || fail "a started run was topped up: $(sensed)"

# Without it in the file: the code's 1, and a snapshot of many keys is one
# run, as before #240.
clear_runs() { q "UPDATE orca.sensed SET run = NULL WHERE pipeline = 'clean'; DELETE FROM orca.runs WHERE pipeline = 'clean'" > /dev/null; }
clear_runs
cat > "$proj/orca.toml" << 'EOF'
[pipelines.clean]
max_runs = 1
EOF
reg ./build/kpr-clean
./build/orca config show clean > build/kpr-show.txt 2>&1 || fail "config show"
grep -q "keys_per_run.*1" build/kpr-show.txt || fail "config show without it: $(grep keys_per_run build/kpr-show.txt)"
clear_runs
source_commit 2024-02,2024-03
tick
source_commit 2024-04
tick
[ "$(sensed)" = "2024-02,2024-03 2024-04" ] || fail "keys_per_run 1: $(sensed)"

# --- A fill: keys_per_run partitions a run, newest first ---
cat > "$proj/orca.toml" << 'EOF'
[pipelines.daily]
fills = "all"
fill_order = "newest"
fill_max_runs = 2
keys_per_run = 3
EOF
ORCA_NOW=2024-01-08T12:00Z ./build/orca register --config "$proj/orca.toml" ./build/kpr-daily >> "$log" 2>&1 \
    || fail "register daily"
fills() { q "SELECT coalesce(string_agg(partitions, ' ' ORDER BY id), '') FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'fill'"; }
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'daily' AND status IN ('running', 'queued')")" -gt 0 ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "daily's runs didn't finish"; sleep 0.1
    done
}
tick 2024-01-08T12:00Z
[ "$(fills)" = "2024-01-05,2024-01-06,2024-01-07 2024-01-02,2024-01-03,2024-01-04" ] \
    || fail "the first fill runs: $(fills)"
settle
tick 2024-01-08T12:01Z
[ "$(fills)" = "2024-01-05,2024-01-06,2024-01-07 2024-01-02,2024-01-03,2024-01-04 2024-01-01" ] \
    || fail "the last fill run: $(fills)"
settle
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'daily' AND trigger = 'fill' AND status <> 'succeeded'")" -eq 0 ] \
    || fail "a fill run didn't succeed: $(q "SELECT id, partitions, status, error FROM orca.runs WHERE pipeline = 'daily'")"
echo "ok   keys_per_run: sensors top up and batch, fills batch"
