#!/bin/sh
# liblake, the lake lib-tin (#454): morca.tables' code, with iceberg,
# parquet and objectstore, as one shared library behind a C ABI
# (src/morca/_lake/liblake.mojo is its root). morca.tables loads it at run time.
#
# Built with -g1, as the package builds it (pixi.toml), so a crash inside it
# is symbolised: on Linux the .so keeps its line tables; on macOS `mojo
# build` writes them to liblake.dylib.dSYM beside it (a build reused from
# scripts/mojo-build.sh's cache has none). The install name is
# @rpath/liblake.dylib.
#
# Usage: scripts/build-liblake.sh [OUT_DIR]   (default build, beside the
# test binaries, where morca.tables looks next to the executable)
# Needs DEP_INC (the data environment's, or the package build's -I flags).
set -eu
cd "$(dirname "$0")/.."
out=${1:-build}
mkdir -p "$out"
case "$(uname -s)" in
Darwin) lib=$out/liblake.dylib ;;
*) lib=$out/liblake.so ;;
esac
# scripts/mojo-build.sh reuses an identical earlier build, and adds
# --target-cpu when MOJO_TARGET_CPU is set.
case "$(uname -s)" in
Darwin)
    # shellcheck disable=SC2086
    scripts/mojo-build.sh --emit shared-lib -g1 -I src $DEP_INC \
        -Xlinker -install_name -Xlinker @rpath/liblake.dylib \
        src/morca/_lake/liblake.mojo -o "$lib"
    ;;
*)
    # shellcheck disable=SC2086
    scripts/mojo-build.sh --emit shared-lib -g1 -I src $DEP_INC \
        src/morca/_lake/liblake.mojo -o "$lib"
    ;;
esac
