#!/bin/sh
# Fills follow the data version (#297): a step's `version=`. Over
# tests/fill_version/vdaily.mojo (daily from 2024-01-01, fills = "all") on
# the local runner, with MORCA_NOW fixed:
#
# - the fill runs each day once; a day whose fill run failed (no run
#   retries) is left alone;
# - new code with the same data version (a read added: a new hash) fills
#   nothing again;
# - a raised version= fills every day again, the failed one included,
#   and then nothing more.
#
# Needs MORCA_DATABASE_URL (pixi run test-fill-version starts a throwaway
# server) and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
scripts/pipelines.sh vdaily-v0 vdaily-other vdaily-v1
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca

log=build/fill-version.log
: > "$log"
proj=$PWD/build/fill-version-project
rm -rf "$proj"
mkdir -p "$proj"
export MORCA_WATCHDOG_SECONDS=0 MORCA_SAMPLE_SECONDS=0
fail() {
    echo "FAIL: $*"
    tail -40 "$log"
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }
T=2024-01-06T12:00Z  # the range is 2024-01-01..05
tick() { MORCA_NOW=$T ./build/morca tick >> "$log" 2>&1 || fail "tick exited $?"; }
reg() { MORCA_NOW=$T ./build/morca register --config "$proj/morca.toml" "$@" >> "$log" 2>&1 || fail "register $*"; }
fills() { q "SELECT coalesce(string_agg(partitions, ',' ORDER BY partitions), '') FROM orca.runs WHERE pipeline = 'vdaily' AND trigger = 'fill' AND id > ${1:-0}"; }
last() { q "SELECT coalesce(max(id), 0) FROM orca.runs"; }
settle() {
    i=0
    while [ "$(q "SELECT count(*) FROM orca.runs WHERE status IN ('running', 'queued')")" -gt 0 ]; do
        i=$((i + 1)); [ "$i" -lt 600 ] || fail "runs didn't finish"; sleep 0.1
    done
}
dv() { q "SELECT coalesce(v.data_version, '') FROM orca.pipelines p JOIN orca.pipeline_versions v ON v.id = p.version WHERE p.name = 'vdaily'"; }

cat > "$proj/morca.toml" << 'TOML'
[pipelines.vdaily]
fills = "all"
fill_max_runs = 5
TOML

# --- v0: each day once; a day that failed is left alone ---
reg ./build/vdaily-v0
[ "$(dv)" = "" ] || fail "v0's data version is '$(dv)', not empty"
export VDAILY_FAIL=2024-01-03
tick
settle
unset VDAILY_FAIL
[ "$(fills)" = "2024-01-01,2024-01-02,2024-01-03,2024-01-04,2024-01-05" ] || fail "v0's fill: $(fills)"
[ "$(q "SELECT status FROM orca.runs WHERE pipeline = 'vdaily' AND partitions = '2024-01-03'")" = failed ] \
    || fail "2024-01-03's run didn't fail"
mark=$(last)
tick
settle
[ "$(fills "$mark")" = "" ] || fail "v0 filled again: $(fills "$mark")"
echo "ok   v0 fills each day once and leaves the failed one"

# --- The same data version, a new hash: nothing again ---
h0=$(q "SELECT registry_hash FROM orca.pipelines WHERE name = 'vdaily'")
reg ./build/vdaily-other
grep -q "version= is now" "$log" && fail "register reported a data version change for the same one"
[ "$(q "SELECT registry_hash FROM orca.pipelines WHERE name = 'vdaily'")" != "$h0" ] || fail "the read didn't change the hash"
[ "$(dv)" = "" ] || fail "the other code's data version is '$(dv)'"
tick
settle
[ "$(fills "$mark")" = "" ] || fail "new code of the same data version filled again: $(fills "$mark")"
echo "ok   new code with the same data version fills nothing again"

# --- version=1: every day again, the cancelled one too, then nothing ---
reg ./build/vdaily-v1
[ "$(dv)" = "make=1" ] || fail "v1's data version is '$(dv)', not make=1"
grep -q "morca: vdaily its steps' version= is now make=1: its fill runs its partitions again" "$log" \
    || fail "register didn't say the fill runs again"
tick
settle
[ "$(fills "$mark")" = "2024-01-01,2024-01-02,2024-01-03,2024-01-04,2024-01-05" ] || fail "v1's fill: $(fills "$mark")"
[ "$(q "SELECT count(*) FROM orca.runs WHERE pipeline = 'vdaily' AND id > $mark AND status <> 'succeeded'")" -eq 0 ] \
    || fail "a v1 fill run didn't succeed"
mark=$(last)
tick
settle
[ "$(fills "$mark")" = "" ] || fail "v1 filled again: $(fills "$mark")"
echo "ok   version=1 fills every day again, then nothing"
echo "ok   fill version"
