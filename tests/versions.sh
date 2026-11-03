#!/bin/sh
# Pipeline versions (orca.source): the toy built from two commits and once
# without a source. Each registration and each run records the version it
# is, the pipeline page lists all three with their runs, and each run's page
# links its own commit. Needs ORCA_DATABASE_URL (pixi run test-versions
# starts a throwaway server), curl and jq.
set -eu
cd "$(dirname "$0")/.."
mkdir -p build
repo=https://github.com/magmalake/orca.mojo
c1=1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a
c2=2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b
# shellcheck disable=SC2086
mojo build -I src $DEP_INC -D ORCA_SOURCE=$repo.git -D ORCA_COMMIT=$c1 examples/toy.mojo -o build/toy-c1
# shellcheck disable=SC2086
mojo build -I src $DEP_INC -D ORCA_SOURCE=git@github.com:magmalake/orca.mojo.git -D ORCA_COMMIT=$c2 \
    -D ORCA_DIRTY=1 examples/toy.mojo -o build/toy-c2
# shellcheck disable=SC2086
mojo build -I src $DEP_INC examples/toy.mojo -o build/toy
# shellcheck disable=SC2086
mojo build -I src $DEP_INC bin/scheduler.mojo -o build/orca

log=build/versions.log
: > "$log"
q() { psql "$ORCA_DATABASE_URL" -Atc "$1"; }
pid=
fail() {
    echo "FAIL: $*"
    tail -30 "$log"
    [ -z "$pid" ] || kill "$pid" 2> /dev/null || true
    exit 1
}

./build/toy-c1 describe | jq -e ".source == {\"repo\": \"$repo.git\", \"commit\": \"$c1\", \"dirty\": false}" > /dev/null \
    || fail "describe's source: $(./build/toy-c1 describe | jq -c .source)"
./build/toy describe | jq -e '.source == {"repo": "", "commit": "", "dirty": false}' > /dev/null \
    || fail "describe without a source"
[ "$(./build/toy-c1 describe | jq -r .registry_hash)" = "$(./build/toy describe | jq -r .registry_hash)" ] \
    || fail "the source changed the registry hash"

./build/orca locate ./build/toy-c1 --root . >> "$log" 2>&1 || fail "locate c1"
./build/orca register --image orca:c1 ./build/toy-c1 >> "$log" 2>&1 || fail "register c1"
ORCA_IMAGE=orca:c1 ./build/toy-c1 run --partitions 2024-01 >> "$log" 2>&1 || fail "run c1"
./build/orca register ./build/toy-c2 >> "$log" 2>&1 || fail "register c2"
ORCA_IMAGE=orca:c1 ./build/toy-c2 run --partitions 2024-02 >> "$log" 2>&1 || fail "run c2"
./build/toy run --partitions 2024-03 >> "$log" 2>&1 || fail "run without a source"
r1=$(q "SELECT min(id) FROM orca.runs")
r3=$(q "SELECT max(id) FROM orca.runs")
r2=$(q "SELECT id FROM orca.runs WHERE id > $r1 AND id < $r3")

[ "$(q "SELECT count(*) FROM orca.pipeline_versions")" -eq 3 ] \
    || fail "versions: $(q "SELECT commit_sha, dirty, image FROM orca.pipeline_versions")"
[ "$(q "SELECT v.commit_sha FROM orca.pipelines p JOIN orca.pipeline_versions v ON v.id = p.version")" = "" ] \
    || fail "the pipeline's version isn't the last binary's"
# A resume records the version that resumed it.
q "UPDATE orca.runs SET status = 'failed', heartbeat = NULL WHERE id = $r3" > /dev/null
ORCA_IMAGE=orca:c1 ./build/toy-c1 resume "$r3" >> "$log" 2>&1 || fail "resume with c1"
[ "$(q "SELECT v.commit_sha FROM orca.runs r JOIN orca.pipeline_versions v ON v.id = r.version WHERE r.id = $r3")" = $c1 ] \
    || fail "the resumed run's version"
q "UPDATE orca.runs SET status = 'failed', heartbeat = NULL WHERE id = $r3" > /dev/null
./build/toy resume "$r3" >> "$log" 2>&1 || fail "resume without a source"

port=$((20000 + $$ % 2000))
base="http://127.0.0.1:$port"
ORCA_UI_ADDR="127.0.0.1:$port" ./build/orca ui >> "$log" 2>&1 &
pid=$!
i=0
until curl -fs "$base/" > /dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || fail "orca ui didn't start"; sleep 0.1
done
json() { curl -fsS -H 'Accept: application/json' "$base$1" || fail "GET $1 as JSON"; }
html() { curl -fsS "$base$1" || fail "GET $1"; }

json "/runs/$r1" | jq -e ".version.commit == \"$c1\" and .version.image == \"orca:c1\" and .version.commit_url == \"$repo/tree/$c1\" and (.version.dirty | not)" > /dev/null \
    || fail "run $r1's version: $(json "/runs/$r1" | jq -c .version)"
json "/runs/$r2" | jq -e ".version.commit_url == \"$repo/tree/$c2\" and .version.dirty" > /dev/null \
    || fail "run $r2's version: $(json "/runs/$r2" | jq -c .version)"
json "/runs/$r3" | jq -e '.version.commit == "" and .version.commit_url == null' > /dev/null \
    || fail "run $r3's version: $(json "/runs/$r3" | jq -c .version)"
html "/runs/$r1" | grep -q "<a href=\"$repo/tree/$c1\" target=\"_blank\" rel=\"noopener\">1a1a1a1a1a1a</a>" || fail "run $r1's page links its commit"
html "/runs/$r2" | grep -q "plus uncommitted changes" || fail "run $r2's page says it was dirty"
html "/runs/$r3" | grep -q "not recorded" || fail "run $r3's page says it has no source"
# The steps' code, at the commit, for the version orca locate mapped.
line=$(grep -n '^def days(' examples/toy.mojo | cut -d: -f1)
html "/runs/$r1" | grep -q "<a href=\"$repo/blob/$c1/examples/toy.mojo#L$line\" target=\"_blank\" rel=\"noopener\">days</a>" \
    || fail "run $r1's page links its steps' code"
json "/runs/$r1" | jq -e '.version.locations["toy::measure"] | startswith("examples/toy.mojo#L")' > /dev/null \
    || fail "run $r1's locations"

# Newest first: the bare binary, then c2 (registered without --image, so
# with the image the pipeline has), then c1.
got=$(json /pipelines/toy | jq -c '[.versions[] | [.commit[0:1], .image, .runs]]')
[ "$got" = '[["","",1],["2","orca:c1",1],["1","orca:c1",1]]' ] || fail "the pipeline's versions: $got"
html /pipelines/toy | grep -q "<h2>Versions" || fail "the pipeline page's versions"
kill "$pid"
echo "ok   versions: a run links the commit it ran, the pipeline lists its versions"
