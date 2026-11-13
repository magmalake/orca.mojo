#!/bin/sh
# SQL lives in morca.db (#362): no string in src/ outside src/morca/db/ and
# src/morca/migrations.mojo holds a statement, but in the files
# tests/sql_allow.txt still lists, which haven't moved theirs yet. A listed
# file that no longer has any fails too, so the list only shrinks.
set -eu
cd "$(dirname "$0")/.."
pattern='"([^"]*[^A-Za-z_"])?(SELECT|INSERT INTO|UPDATE [a-z_.]+( [a-z]+)? SET|DELETE FROM|ON CONFLICT)([^A-Za-z_]|$)'
have=$(mktemp "${TMPDIR:-/tmp}/sql-have.XXXXXX")
listed=$(mktemp "${TMPDIR:-/tmp}/sql-listed.XXXXXX")
trap 'rm -f "$have" "$listed"' EXIT INT TERM
git grep -lE "$pattern" -- 'src/*.mojo' ':!src/morca/db/' \
    ':!src/morca/migrations.mojo' | LC_ALL=C sort > "$have" || true
grep -v '^#' tests/sql_allow.txt | grep . | LC_ALL=C sort > "$listed" || true
fail=0
new=$(LC_ALL=C comm -23 "$have" "$listed")
if [ -n "$new" ]; then
    for f in $new; do
        git grep -nE "$pattern" -- "$f"
    done
    echo "FAIL: SQL outside src/morca/db/ (#362): move it behind a function there"
    fail=1
fi
gone=$(LC_ALL=C comm -13 "$have" "$listed")
if [ -n "$gone" ]; then
    echo "$gone"
    echo "FAIL: these have no SQL left: take them off tests/sql_allow.txt"
    fail=1
fi
if [ "$fail" = 0 ]; then
    echo "ok   sql-in-db"
fi
exit "$fail"
