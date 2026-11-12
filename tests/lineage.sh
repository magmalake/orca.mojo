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
# - `morca lineage` and the UI's pages say the same.
#
# With MORCA_TEST_S3 set, the warehouse and the attempts' logs are in the
# bucket MORCA_S3_BUCKET names (pixi run -e data test-lineage-s3 starts a
# throwaway MinIO), and the UI shows an attempt's log read from it.
#
# Needs MORCA_DATABASE_URL (pixi run -e data test-lineage starts a throwaway
# server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
. "$PWD/tests/port.sh"
mkdir -p build
# The pipelines and morca compile at once (scripts/mojo-build.sh).
pids=
for p in ingest clean report; do
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src -I tests/lineage $DEP_INC "tests/lineage/$p.mojo" -o "build/ln-$p" &
    pids="$pids $!"
done
# shellcheck disable=SC2086
scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca &
for pid in $pids $!; do wait "$pid"; done
log=build/lineage.log
: > "$log"
if [ -n "${MORCA_TEST_S3:-}" ]; then
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src $DEP_INC tests/s3cat.mojo -o build/s3cat
    export MORCA_WAREHOUSE="$MORCA_S3_BUCKET/lineage-$$" MORCA_LOGS="$MORCA_S3_BUCKET/lineage-logs-$$"
else
    export MORCA_WAREHOUSE=build/lineage-wh
    rm -rf "$MORCA_WAREHOUSE"
fi
ui=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$ui" ] || kill "$ui" 2> /dev/null || true
    exit 1
}
q() { psql "$MORCA_DATABASE_URL" -Atc "$1"; }

./build/morca register ./build/ln-ingest ./build/ln-clean ./build/ln-report >> "$log" 2>&1 || fail "register"
./build/ln-ingest run --partitions 2024-01 >> "$log" 2>&1 || fail "ingest 2024-01"
./build/ln-ingest run --partitions 2024-02 >> "$log" 2>&1 || fail "ingest 2024-02"
./build/ln-clean run --partitions 2024-01..2024-02 >> "$log" 2>&1 || fail "clean"
./build/ln-report run --partitions 2024-01..2024-02 >> "$log" 2>&1 || fail "report"

# What the tables themselves say: each snapshot's orca.reads, by dataset.
meta() {
    case "$MORCA_WAREHOUSE" in
        s3://*)
            m=$(./build/s3cat ls "$MORCA_WAREHOUSE/lake/$1/metadata/" | grep '\.metadata\.json$' | sort | tail -1)
            ./build/s3cat get "$m" > "build/lineage-meta-$1.json"
            echo "build/lineage-meta-$1.json"
            ;;
        *) ls "$MORCA_WAREHOUSE/lake/$1/metadata/"*.metadata.json | sort | tail -1 ;;
    esac
}
current() { jq -r '."current-snapshot-id"' "$(meta "$1")"; }
# reads_of TABLE SNAPSHOT: its summary's orca.reads as dataset@snapshot, sorted.
reads_of() {
    jq -r --arg id "$2" '.snapshots[] | select((."snapshot-id" | tostring) == $id) | .summary["orca.reads"]' "$(meta "$1")" \
        | tr ',' '\n' | sed "s|^$MORCA_WAREHOUSE/||; s|/|.|; s|/*@|@|" | grep . | sort | tr '\n' ' '
}
R=$(current report)
C=$(reads_of report "$R" | sed -n 's/^lake\.clean@\([0-9-]*\) $/\1/p')
[ -n "$C" ] || fail "report's snapshot doesn't say it read one clean snapshot: $(reads_of report "$R")"
S=$(reads_of clean "$C" | sed -n 's/^lake\.raw@\([0-9-]*\) $/\1/p')
[ -n "$S" ] || fail "clean's snapshot doesn't say it read one raw snapshot: $(reads_of clean "$C")"
S1=$(jq -r '.snapshots[0]."snapshot-id"' "$(meta raw)")
[ "$S1" != "$S" ] || fail "clean read the first raw snapshot"

# The command line, back and forward.
out=$(./build/morca lineage lake.report) || fail "morca lineage exited $?"
echo "$out" >> "$log"
code=$(./build/ln-report describe | jq -r .registry_hash)
echo "$out" | grep -qF "0 lake.report@$R by report run " || fail "back doesn't start at report's snapshot"
echo "$out" | grep -qE "0 lake.report@$R by report run [0-9]+ report code $code from lake.clean@$C\$" \
    || fail "report's snapshot, its code and its input"
echo "$out" | grep -qE "1 lake.clean@$C by clean run [0-9]+ clean 2024-0[12] code .* from lake.raw@$S\$" \
    || fail "the clean snapshot it read"
echo "$out" | grep -qE "2 lake.raw@$S by ingest run [0-9]+ load 2024-02 code .*[0-9a-f]\$" \
    || fail "the raw snapshot clean read"
[ "$(./build/morca lineage lake.report "$R")" = "$out" ] || fail "lineage of the latest isn't lineage of $R"
out=$(./build/morca lineage lake.raw "$S1") || fail "morca lineage of the first raw snapshot"
echo "$out" | sed -n '/made from it:/,$p' | grep -qx "  nothing" || fail "something was made from the first raw snapshot"

# The UI: the same walks as JSON, and their pages.
free_port port
base="http://127.0.0.1:$port"
MORCA_UI_ADDR="127.0.0.1:$port" ./build/morca ui >> "$log" 2>&1 &
ui=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "morca ui didn't start"; sleep 0.1
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
page=$(curl -fsS "$base/runs/$run")
echo "$page" | grep -qF '<section class="card" id="tables"><h2>Snapshots<span class="help">' || fail "the run's Snapshots card"
echo "$page" | grep -q 'tabindex="0">read</span></td>' && echo "$page" | grep -q 'tabindex="0">wrote</span></td>' || fail "the run's snapshots don't say read and wrote"
# The snapshot page: the diagram with its legend and help, Upstream and
# Downstream (back and forward in the JSON), each with its help.
page=$(curl -fsS "$base/datasets/lake.report/snapshots/$R")
echo "$page" | grep -qF '<section class="card" id="lineage"><h2>Lineage<span class="help">' || fail "the lineage diagram's help"
echo "$page" | grep -qF '<p class="muted graph__legend">' || fail "the lineage diagram's legend"
for h in Upstream Downstream; do
    echo "$page" | grep -qF "<h2 id=\"$(echo $h | tr A-Z a-z)\">$h<span class=\"help\">" || fail "no $h with its help"
done
echo "$page" | grep -q 'Made from' && fail "the snapshot page still says Made from"
echo "$page" | grep -qF '<th>Written by<span class="help">' && echo "$page" | grep -qF '<th>Read<span class="help">' || fail "the lineage tables' columns"
[ "$(json "/runs/$run" | jq -r '[.tables[] | .dataset] | sort | join(",")')" = "lake.clean,lake.report" ] \
    || fail "the run's tables' datasets"
[ "$(json /pipelines/clean | jq -c '.steps[0] | [.reads, .writes]')" = '[["lake.raw"],["lake.clean"]]' ] \
    || fail "the pipeline's steps' datasets"
# The Rebuild section: the chain a rebuild of lake.raw runs, a preview of
# its runs by level that queues nothing, and the same command line.
json /datasets/lake.raw | jq -e '.downstream == [{"pipeline": "clean", "level": 0, "reads": ["lake.raw"]},
    {"pipeline": "report", "level": 1, "reads": ["lake.clean"]}]' > /dev/null \
    || fail "lake.raw's downstream: $(json /datasets/lake.raw | jq -c .downstream)"
page=$(curl -fsS "$base/datasets/lake.raw")
echo "$page" | grep -qF 'Rebuild reruns everything downstream of this dataset for the partitions you give, in dependency order.' \
    || fail "the rebuild's explanation"
echo "$page" | grep -qF '<li class="chain__level"><span class="chain__step"><a href="/pipelines/clean">clean</a> <span class="muted">(reads <code>lake.raw</code>)</span></span></li><li class="chain__level"><span class="chain__step"><a href="/pipelines/report">report</a>' \
    || fail "the chain on lake.raw's page"
curl -fsS "$base/datasets/lake.report" | grep -qF '<p class="empty">Nothing reads this dataset.</p>' \
    || fail "lake.report's page doesn't say nothing reads it"
before=$(q "SELECT (SELECT count(*) FROM orca.runs) || ' ' || (SELECT count(*) FROM orca.rebuilds)")
p=$(json '/datasets/lake.raw?preview=1&partitions=2024-01..2024-02')
echo "$p" | jq -e '.preview | .refused == null and .command == "morca rebuild lake.raw 2024-01..2024-02"
    and .levels == [[{"pipeline": "clean", "partitions": "2024-01,2024-02"}], [{"pipeline": "report", "partitions": "2024-01,2024-02"}]]' > /dev/null \
    || fail "the rebuild preview: $(echo "$p" | jq -c .preview)"
page=$(curl -fsS "$base/datasets/lake.raw?preview=1&partitions=2024-01..2024-02")
echo "$page" | grep -qF '<p data-preview="rebuild">This queues 2 runs in 2 levels, over 2 partitions:</p><ol><li>1 run: clean over 2024-01,2024-02</li><li>1 run: report over 2024-01,2024-02</li></ol>' \
    || fail "the rebuild preview's lines"
echo "$page" | grep -qF '<code data-cmd="rebuild">morca login &lt;url&gt; &amp;&amp; morca --remote &lt;url&gt; rebuild lake.raw 2024-01..2024-02</code>' || fail "the rebuild's command"
echo "$page" | grep -qF 'title="morca rebuild lake.raw 2024-01..2024-02">Rebuild downstream</button>' || fail "the rebuild button"
json '/datasets/lake.raw?preview=1&partitions=2023-13' | jq -e '.preview.refused != null' > /dev/null \
    || fail "a bad range's rebuild preview wasn't refused"
[ "$(q "SELECT (SELECT count(*) FROM orca.runs) || ' ' || (SELECT count(*) FROM orca.rebuilds)")" = "$before" ] \
    || fail "a rebuild preview queued something"
for path in /datasets/nope "/datasets/lake.raw/snapshots/1" /datasets/lake.raw/snapshots/x; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$base$path")
    [ "$code" = 404 ] || fail "$path answered $code"
done
if [ -n "${MORCA_TEST_S3:-}" ]; then
    # An attempt's log, kept in the bucket and read back by the UI.
    aid=$(json "/runs/$run" | jq -r '.steps[0].attempt_id')
    [ "$(json "/runs/$run/attempts/$aid" | jq -r '.attempt.log')" = \
        "$MORCA_LOGS/report/$run/report/_run/1.log" ] || fail "the attempt's log location"
    json "/runs/$run/attempts/$aid" | jq -e '.text | contains("attempt 1 succeeded")' > /dev/null \
        || fail "the attempt's log from the bucket: $(json "/runs/$run/attempts/$aid")"
    ./build/s3cat get "$MORCA_LOGS/report/$run/report/_run/1.log" | grep -q "attempt 1 succeeded" \
        || fail "the log isn't in the bucket"
fi
kill "$ui"
echo "ok   lineage${MORCA_TEST_S3:+ on S3}"
