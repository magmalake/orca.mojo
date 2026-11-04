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
# A run by hand records its version but leaves the registered one (#59).
[ "$(q "SELECT v.commit_sha FROM orca.pipelines p JOIN orca.pipeline_versions v ON v.id = p.version")" = $c2 ] \
    || fail "the pipeline's version isn't the last registered binary's"
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
html "/runs/$r1" | grep -qF "<a class=\"src\" href=\"$repo/tree/$c1\" target=\"_blank\" rel=\"noopener\" title=\"View source on GitHub at 1a1a1a1\"><svg class=\"src__icon\"" \
    || fail "run $r1's page links its commit"
html "/runs/$r2" | grep -q "plus uncommitted changes" || fail "run $r2's page says it was dirty"
html "/runs/$r3" | grep -q "not recorded" || fail "run $r3's page says it has no source"
# The steps' code, at the commit, for the version orca locate mapped.
line=$(grep -n '^def days(' examples/toy.mojo | cut -d: -f1)
# A code link: the icon, out of the screen reader's way, the file and line
# in monospace, and an arrow out, in a new tab; unlike orca's own links.
icon='<svg class="src__icon" viewBox="0 0 16 16" width="12" height="12" aria-hidden="true" focusable="false">'
code_link() { # url, commit
    printf '<a class="src" href="%s" target="_blank" rel="noopener" title="View source on GitHub at %.7s">%s' "$1" "$2" "$icon"
}
code_at() { printf '<span class="src__at">%s</span><span class="src__out" aria-hidden="true">↗</span></a>' "$1"; }
page=$(html "/runs/$r1")
echo "$page" | grep -qF "<td>days $(code_link "$repo/blob/$c1/examples/toy.mojo#L$line" $c1)" \
    || fail "run $r1's page links its steps' code"
echo "$page" | grep -qF "$(code_at "examples/toy.mojo:$line")" || fail "the code link doesn't show the file and line"
echo "$page" | grep -qF '<a class="src" href="/' && fail "an internal link took the code link's look"
echo "$page" | grep -o '<a class="src"[^>]*>' | grep -vq 'target="_blank" rel="noopener" title="View source' \
    && fail "a code link without its target or title"
curl -fsS "$base/static/orca.css" | grep -q '^a.src {' || fail "the stylesheet has no code link style"
att=$(json "/runs/$r1" | jq -r '.steps[0].attempt_id')
html "/runs/$r1/attempts/$att" | grep -qF "<dt>Code</dt><dd>$(code_link "$repo/blob/$c1/examples/toy.mojo#L$line" $c1)" \
    || fail "the attempt's page doesn't link its step's code"
json "/runs/$r1" | jq -e '.version.locations["toy::measure"] | startswith("examples/toy.mojo#L")' > /dev/null \
    || fail "run $r1's locations"

# Newest first: the bare binary, then c2 (registered without --image, so
# with the image the pipeline has), then c1.
got=$(json /pipelines/toy | jq -c '[.versions[] | [.commit[0:1], .image, .runs]]')
[ "$got" = '[["","",1],["2","orca:c1",1],["1","orca:c1",1]]' ] || fail "the pipeline's versions: $got"
html /pipelines/toy | grep -q "<h2>Versions" || fail "the pipeline page's versions"

# A dataset's page links the steps that write and read it to their lines:
# the writer at the commit of the run that committed the snapshot (c1), the
# reader, which no run has read with, at the one registered now (c2).
for c in 1 2; do
    eval "sha=\$c$c"
    src=$repo.git
    [ "$c" = 2 ] && src=git@github.com:magmalake/orca.mojo.git
    # shellcheck disable=SC2086
    mojo build -I src $DEP_INC -D ORCA_SOURCE=$src -D ORCA_COMMIT="$sha" tests/datasets/first.mojo -o "build/ds-first-c$c"
    ./build/orca locate "./build/ds-first-c$c" --root . >> "$log" 2>&1 || fail "locate first c$c"
done
./build/orca register ./build/ds-first-c1 >> "$log" 2>&1 || fail "register first c1"
./build/ds-first-c1 run --partitions 2024-01 >> "$log" 2>&1 || fail "run first c1"
q "INSERT INTO orca.commits (attempt, table_name, snapshot_id, dataset) SELECT a.id, 'a', 7, 'lake.a' FROM orca.attempts a JOIN orca.steps s ON s.id = a.step JOIN orca.runs r ON r.id = s.run WHERE r.pipeline = 'first'" > /dev/null
./build/orca register ./build/ds-first-c2 >> "$log" 2>&1 || fail "register first c2"
fline=$(grep -n '^def step(' tests/datasets/first.mojo | cut -d: -f1)
page=$(html /datasets/lake.a)
echo "$page" | grep -qF "<dt>Written by</dt><dd><a href=\"/pipelines/first\">first</a> <span class=\"steps-of\">step step $(code_link "$repo/blob/$c1/tests/datasets/first.mojo#L$fline" $c1)" \
    || fail "the dataset's page doesn't link its writer's code at the run's commit: $(echo "$page" | grep -o '<dt>Written by.*</dl>')"
echo "$page" | grep -qF "$(code_at "tests/datasets/first.mojo:$fline")" || fail "the writer's code link doesn't show its file and line"
echo "$page" | grep -qF "<td>step $(code_link "$repo/blob/$c1/tests/datasets/first.mojo#L$fline" $c1)" \
    || fail "the snapshot doesn't link its step's code"
html /datasets/lake.c | grep -qF "<dt>Read by</dt><dd><a href=\"/pipelines/first\">first</a> <span class=\"steps-of\">step step $(code_link "$repo/blob/$c2/tests/datasets/first.mojo#L$fline" $c2)" \
    || fail "the dataset's page doesn't link its reader's code at the registered commit"
json /datasets/lake.a | jq -e '.writers == ["first"] and .snapshots[0].snapshot == 7' > /dev/null \
    || fail "the dataset's JSON changed: $(json /datasets/lake.a | jq -c .)"
kill "$pid"
echo "ok   versions: a run links the commit it ran, the pipeline lists its versions"
