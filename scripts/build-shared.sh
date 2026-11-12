#!/bin/sh
# Builds the binaries most tests use, at once, with the arguments the tests
# build them with, so each test's own build of them is a copy
# (scripts/mojo-build.sh). One build of morca uses under two cores; four at
# once keep a 4-core CI runner busy.
#
# Usage: scripts/build-shared.sh [morca] [toy] [fanout] [daily]   (all four
# when none is named)
set -eu
cd "$(dirname "$0")/.."
[ "$#" -gt 0 ] || set -- morca toy fanout daily
pids=
for b in "$@"; do
    case "$b" in
    morca) src=bin/scheduler.mojo ;;
    *) src=examples/$b.mojo ;;
    esac
    # shellcheck disable=SC2086
    scripts/mojo-build.sh -I src $DEP_INC "$src" -o "build/$b" &
    pids="$pids $!"
done
for pid in $pids; do wait "$pid"; done
