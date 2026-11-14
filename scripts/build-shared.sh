#!/bin/sh
# Builds the binaries most tests use, at once, with the arguments the tests
# build them with, so each test's own build of them is a copy
# (scripts/mojo-build.sh): morca, and the test and example pipelines as one
# multi-call binary (scripts/pipelines.sh). Each build keeps about two cores
# busy.
#
# Usage: scripts/build-shared.sh [morca] [pipelines]   (both when none is
# named)
set -eu
cd "$(dirname "$0")/.."
[ "$#" -gt 0 ] || set -- morca pipelines
pids=
for b in "$@"; do
    case "$b" in
    # shellcheck disable=SC2086
    morca) scripts/mojo-build.sh -I src $DEP_INC bin/scheduler.mojo -o build/morca & ;;
    pipelines) scripts/pipelines.sh & ;;
    *)
        echo "build-shared.sh: no $b" >&2
        exit 1
        ;;
    esac
    pids="$pids $!"
done
for pid in $pids; do wait "$pid"; done
