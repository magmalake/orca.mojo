#!/bin/sh
# Lineage over three pipelines on warehouse tables (tests/lineage/):
# ingest writes lake.raw a month at a time, clean reads it and writes
# lake.clean, and report reads that and writes lake.report. After ingest
# runs 2024-01 and then 2024-02, clean runs both and report runs once:
#
# - back from report's snapshot: it, the clean snapshot it read, and the raw
#   snapshot that one read, each with its run, step, partition and code, and
#   each one's inputs what its own summary's orca.reads says;
# - forward from that raw snapshot: both clean snapshots, then report's;
# - nothing was made from the first raw snapshot, which clean never read;
# - `orca lineage` and the UI's pages say the same.
#
# Needs ORCA_DATABASE_URL (pixi run -e data test-lineage starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
for p in ingest clean report; do
    # shellcheck disable=SC2086
    mojo build -I src -I tests/lineage $DEP_INC "tests/lineage/$p.mojo" -o "build/ln-$p"
done
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca
log=build/lineage.log
: > "$log"
export ORCA_WAREHOUSE=build/lineage-wh
rm -rf "$ORCA_WAREHOUSE"
ui=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    exit 1
}
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }

./build/orca register ./build/ln-ingest ./build/ln-clean ./build/ln-report >> "$log" 2>&1 || fail "register"
./build/ln-ingest run --partitions 2024-01 >> "$log" 2>&1 || fail "ingest 2024-01"
./build/ln-ingest run --partitions 2024-02 >> "$log" 2>&1 || fail "ingest 2024-02"
./build/ln-clean run --partitions 2024-01..2024-02 >> "$log" 2>&1 || fail "clean"
./build/ln-report run --partitions 2024-01..2024-02 >> "$log" 2>&1 || fail "report"

# What the tables themselves say: each snapshot's orca.reads, by dataset.
meta() { ls "$ORCA_WAREHOUSE/lake/$1/metadata/"*.metadata.json | sort | tail -1; }
current() { jq -r '."current-snapshot-id"' "$(meta "$1")"; }
# reads_of TABLE SNAPSHOT: its summary's orca.reads as dataset@snapshot, sorted.
reads_of() {
    jq -r --arg id "$2" '.snapshots[] | select((."snapshot-id" | tostring) == $id) | .summary["orca.reads"]' "$(meta "$1")" \
        | tr ',' '\n' | sed "s|^$ORCA_WAREHOUSE/||; s|/|.|; s|/*@|@|" | grep . | sort | tr '\n' ' '
}
R=$(current report)
C=$(reads_of report "$R" | sed -n 's/^lake\.clean@\([0-9-]*\) $/\1/p')
[ -n "$C" ] || fail "report's snapshot doesn't say it read one clean snapshot: $(reads_of report "$R")"
S=$(reads_of clean "$C" | sed -n 's/^lake\.raw@\([0-9-]*\) $/\1/p')
[ -n "$S" ] || fail "clean's snapshot doesn't say it read one raw snapshot: $(reads_of clean "$C")"
S1=$(jq -r '.snapshots[0]."snapshot-id"' "$(meta raw)")
[ "$S1" != "$S" ] || fail "clean read the first raw snapshot"

# The command line, back and forward.
out=$(./build/orca lineage lake.report) || fail "orca lineage exited $?"
echo "$out" >> "$log"
code=$(./build/ln-report describe | jq -r .registry_hash)
echo "$out" | grep -qF "0 lake.report@$R by report run " || fail "back doesn't start at report's snapshot"
echo "$out" | grep -qE "0 lake.report@$R by report run [0-9]+ report code $code from lake.clean@$C\$" \
    || fail "report's snapshot, its code and its input"
echo "$out" | grep -qE "1 lake.clean@$C by clean run [0-9]+ clean 2024-0[12] code .* from lake.raw@$S\$" \
    || fail "the clean snapshot it read"
echo "$out" | grep -qE "2 lake.raw@$S by ingest run [0-9]+ load 2024-02 code .*[0-9a-f]\$" \
    || fail "the raw snapshot clean read"
[ "$(./build/orca lineage lake.report "$R")" = "$out" ] || fail "lineage of the latest isn't lineage of $R"
out=$(./build/orca lineage lake.raw "$S1") || fail "orca lineage of the first raw snapshot"
echo "$out" | sed -n '/made from it:/,$p' | grep -qx "  nothing" || fail "something was made from the first raw snapshot"

# The UI: the same walks as JSON, and their pages.
port=$((22000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1"; }

back=$(json "/datasets/lake.report/snapshots/$R" | jq -r '[.back[] | "\(.depth) \(.dataset)@\(.snapshot)"] | join(",")')
[ "$back" = "0 lake.report@$R,1 lake.clean@$C,2 lake.raw@$S" ] || fail "the back walk: $back"
# Each snapshot's inputs, as the walk has them, are what its summary says.
bad=$(json "/datasets/lake.report/snapshots/$R" | jq -r '.back[] | "\(.dataset) \(.snapshot) \([.inputs[] | "\(.dataset)@\(.snapshot)"] | sort | join(" "))"' \
    | while read -r ds id inputs; do
        want=$(reads_of "${ds#lake.}" "$id" | sed 's/ $//')
        [ "$inputs" = "$want" ] || echo "$ds@$id's inputs are '$inputs'; its summary says '$want'"
    done)
[ -z "$bad" ] || fail "$bad"
fwd=$(json "/datasets/lake.raw/snapshots/$S" | jq -r '[.forward[] | "\(.depth) \(.dataset)"] | join(",")')
[ "$fwd" = "1 lake.clean,1 lake.clean,2 lake.report" ] || fail "the forward walk: $fwd"
[ "$(json "/datasets/lake.raw/snapshots/$S" | jq -r '.forward[] | select(.dataset == "lake.report") | .snapshot')" = "$R" ] \
    || fail "forward from raw doesn't reach report's snapshot"
[ "$(json "/datasets/lake.raw/snapshots/$S1" | jq '.forward | length')" -eq 0 ] || fail "forward from the first raw snapshot"

[ "$(json /datasets | jq -r '[.datasets[] | "\(.name):\(.writers | join("+")):\(.readers | join("+"))"] | join(" ")')" \
    = "lake.clean:clean:report lake.raw:ingest:clean lake.report:report:" ] || fail "/datasets: $(json /datasets)"
[ "$(json /datasets/lake.clean | jq '.snapshots | length')" -eq 2 ] || fail "lake.clean hasn't 2 snapshots"
[ "$(json /datasets/lake.clean | jq -r '.snapshots[0].pipeline')" = clean ] || fail "lake.clean's snapshots"
for path in /datasets /datasets/lake.clean "/datasets/lake.report/snapshots/$R"; do
    curl -fsS "$base$path" | grep -q 'class="graph"' || fail "$path has no graph"
done
curl -fsS "$base/datasets/lake.report/snapshots/$R" | grep -q "href=\"/datasets/lake.raw/snapshots/$S\"" \
    || fail "the lineage page doesn't link the raw snapshot"
run=$(q "SELECT max(id) FROM orca.runs WHERE pipeline = 'report'")
curl -fsS "$base/runs/$run" | grep -q "href=\"/datasets/lake.clean/snapshots/$C\"" || fail "the run doesn't link what it read"
[ "$(json "/runs/$run" | jq -r '[.tables[] | .dataset] | sort | join(",")')" = "lake.clean,lake.report" ] \
    || fail "the run's tables' datasets"
[ "$(json /pipelines/clean | jq -c '.steps[0] | [.reads, .writes]')" = '[["lake.raw"],["lake.clean"]]' ] \
    || fail "the pipeline's steps' datasets"
for path in /datasets/nope "/datasets/lake.raw/snapshots/1" /datasets/lake.raw/snapshots/x; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$base$path")
    [ "$code" = 404 ] || fail "$path answered $code"
done
kill "$ui"
echo "ok   lineage"
