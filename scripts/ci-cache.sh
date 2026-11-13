#!/bin/sh
# Keeps a CI build cache (Mojo's compile cache, build/.mojo-build) to what
# the job used, so the copy saved to GitHub's Actions cache, which holds
# 10 GB a repository, doesn't grow with every commit.
#
#   scripts/ci-cache.sh start <dir>...   after the cache is restored
#   scripts/ci-cache.sh end <dir>...     after the tests, before it's saved
#
# `start` sets the access time of every restored file to 2000 (Linux's
# relatime then updates it on the next read) and leaves a marker; `end`
# removes the restored files nothing read since. A file the job wrote is
# newer than the marker and stays. Where access times aren't kept (no
# restored file was read, yet some were there), `end` removes only files a
# week old, as before.
set -eu

mode=$1
shift
marker=${RUNNER_TEMP:-${TMPDIR:-/tmp}}/ci-cache-marker

size() { du -sh "$@" 2> /dev/null | tr '\n' ' '; echo; }

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
    echo "ci-cache: before pruning $(size "$@")"
    for d in "$@"; do
        [ -d "$d" ] || continue
        restored=$(find "$d" -type f ! -newer "$marker" | wc -l)
        read=$(find "$d" -type f ! -newer "$marker" -anewer "$marker" | wc -l)
        if [ "$restored" -gt 0 ] && [ "$read" -eq 0 ]; then
            echo "ci-cache: $d: none of $restored restored files read (no access times?): only week-old files go"
            find "$d" -type f -mtime +7 -delete 2> /dev/null || true
        else
            echo "ci-cache: $d: $read of $restored restored files read, the rest go"
            find "$d" -type f ! -newer "$marker" ! -anewer "$marker" -delete 2> /dev/null || true
        fi
        find "$d" -mindepth 1 -type d -empty -delete 2> /dev/null || true
    done
    echo "ci-cache: after pruning $(size "$@")"
    ;;
*)
    echo "usage: $0 start|end <dir>..." >&2
    exit 2
    ;;
esac
