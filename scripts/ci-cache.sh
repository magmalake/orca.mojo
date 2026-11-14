#!/bin/sh
# Keeps a CI build cache (Mojo's compile cache, build/.mojo-build) to the
# files used in the last three days, so the copy saved to GitHub's Actions
# cache, which holds 10 GB a repository, doesn't grow with every commit.
#
#   scripts/ci-cache.sh start <dir>...   after the cache is restored
#   scripts/ci-cache.sh end <dir>...     after the tests, before it's saved
#
# A file's modification time is when it was last used: `start` sets the
# access time of every restored file to 2000 (Linux's relatime then
# updates it on the next read) and leaves a marker; `end` sets the
# modification time of each restored file read since to now, then removes
# files three days old. A file the job wrote is new anyway, and
# scripts/mojo-build.sh touches the entries it reuses. Where access times
# aren't kept, a file goes three days after it was written.
set -eu

mode=$1
shift
marker=${RUNNER_TEMP:-${TMPDIR:-/tmp}}/ci-cache-marker

size() { du -sh "$@" 2> /dev/null | tr '\t\n' '  '; echo; }

case $mode in
start)
    for d in "$@"; do
        mkdir -p "$d"
        find "$d" -type f -exec touch -a -t 200001010000 {} + 2> /dev/null || true
    done
    sleep 1
    touch "$marker"
    echo "ci-cache: restored $(size "$@")"
    ;;
end)
    [ -f "$marker" ] || { echo "ci-cache: no marker, nothing pruned"; exit 0; }
    for d in "$@"; do
        [ -d "$d" ] || continue
        read=$(find "$d" -type f ! -newer "$marker" -anewer "$marker" | wc -l)
        find "$d" -type f ! -newer "$marker" -anewer "$marker" -exec touch -m {} + 2> /dev/null || true
        old=$(find "$d" -type f -mtime +3 | wc -l)
        find "$d" -type f -mtime +3 -delete 2> /dev/null || true
        find "$d" -mindepth 1 -type d -empty -delete 2> /dev/null || true
        echo "ci-cache: $d: $(echo $read) restored files read, $(echo $old) unused for three days removed"
    done
    echo "ci-cache: saved $(size "$@")"
    ;;
*)
    echo "usage: $0 start|end <dir>..." >&2
    exit 2
    ;;
esac
