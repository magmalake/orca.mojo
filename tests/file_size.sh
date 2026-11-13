#!/bin/sh
# No source file in src/ is over 1,500 lines (#363): long files are hard to
# review and collect merge conflicts. Split one along its responsibilities
# before it grows past the limit. tests/file_size_allow.txt lists files
# allowed over it, and starts empty; one listed that is under the limit fails
# too, so the list only shrinks.
set -eu
cd "$(dirname "$0")/.."
limit=1500
over=$(mktemp "${TMPDIR:-/tmp}/size-over.XXXXXX")
listed=$(mktemp "${TMPDIR:-/tmp}/size-listed.XXXXXX")
trap 'rm -f "$over" "$listed"' EXIT INT TERM
git ls-files 'src/*.mojo' | while read -r f; do
    n=$(wc -l < "$f")
    [ "$n" -le "$limit" ] || echo "$f"
done | LC_ALL=C sort > "$over"
{ grep -v '^#' tests/file_size_allow.txt 2> /dev/null || true; } | grep . | LC_ALL=C sort > "$listed" || true
fail=0
for f in $(LC_ALL=C comm -23 "$over" "$listed"); do
    echo "$f: $(wc -l < "$f" | tr -d ' ') lines, over $limit: split it along its responsibilities (#363)"
    fail=1
done
for f in $(LC_ALL=C comm -13 "$over" "$listed"); do
    echo "$f is $limit lines or fewer now: take it off tests/file_size_allow.txt"
    fail=1
done
[ "$fail" = 1 ] || echo "ok   file-size (largest: $(git ls-files 'src/*.mojo' | xargs wc -l | grep -v ' total$' | sort -rn | head -1 | awk '{print $2, $1}'))"
exit "$fail"
