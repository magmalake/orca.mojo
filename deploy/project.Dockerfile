# The image `orca aws push` builds for a project that added orca with
# `pixi shelf add orca-mojo`, on orca's runtime image,
# ghcr.io/magmalake/orca-runtime:<ORCA_VERSION> (deploy/runtime.Dockerfile).
# The runtime has orca's own `orca` and `orca_maintain`, git, libcurl, the CA
# bundles, llvm-symbolizer and the /opt/orca layout and environment; this
# adds only the project's programs:
#
# - its apps, every bin/*.mojo, at /opt/orca/bin/<name> (`orca aws app` runs
#   them). bin/scheduler.mojo, the two lines calling orca.command, becomes
#   /opt/orca/bin/orca in place of the runtime's, and bin/orca_maintain.mojo
#   /opt/orca/bin/orca_maintain; a project without them runs the runtime's.
# - its pipelines, every pipelines/*.mojo, at /opt/orca/pipelines/<name>,
#   with the source map `orca locate` writes beside each.
# - the pixi environment's libraries those link or dlopen, in /opt/orca/lib
#   over the runtime's, so the project's own versions win.
#
# `src/` is on the import path if the project has one. A project with a
# Dockerfile.orca of its own builds that instead, and can start it the same
# way: `ARG ORCA_VERSION` and `FROM ghcr.io/magmalake/orca-runtime:${ORCA_VERSION}`.
#
# ORCA_VERSION is the orca-mojo the project pins, which `orca aws push`
# reads from pixi.lock; the runtime and the project's pipelines are then the
# same orca. By hand:
#
#     docker build -f project.Dockerfile --build-arg ORCA_VERSION=0.24.0 \
#         -t ghcr.io/<owner>/<project>:<version> .
#
# Name the image after the project (ghcr.io/magmalake/mojourney, not
# ghcr.io/magmalake/orca), and set ORCA_IMAGE in deploy/hetzner's
# hetzner.env to it. ORCA_RUNTIME names another runtime repository, such as
# one built locally (`docker build -f deploy/runtime.Dockerfile -t
# orca-runtime:0.24.0 .` in orca.mojo, then --build-arg ORCA_RUNTIME=orca-runtime).
#
# ORCA_SOURCE and ORCA_COMMIT say what the pipelines were built from, which
# each run records and orca ui links.
#
# Everything is built with line tables (-g1) and the image has
# llvm-symbolizer, so a crash's stack dump in a run's driver.log names each
# frame's function, file and line (orca.crash). The line tables, compressed,
# make a binary about half as big again (mojo-census's gharchive: 4.2 MB,
# 6.5 MB with them, 25 MB uncompressed) and leave its speed as it was.
#
# The binaries are built for MOJO_TARGET_CPU, not for the CPU of the machine
# building them (`mojo build` defaults to that). GitHub's arm64 runners are
# Neoverse N2, which has SVE2; Hetzner's CAX servers are Neoverse N1, which
# has not, so a binary built there dies on the server with SIGILL (exit
# 132). N1 code runs on N2, V1, V2 and Graviton. Build for something else
# with --build-arg MOJO_TARGET_CPU=<cpu> (x86-64-v3 for linux/amd64).
# No default version: the error for a build without one names what's missing.
ARG ORCA_VERSION=needs-build-arg-orca-version
ARG ORCA_RUNTIME=ghcr.io/magmalake/orca-runtime
FROM ${ORCA_RUNTIME}:${ORCA_VERSION} AS runtime

FROM ghcr.io/prefix-dev/pixi:0.78.0-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src/project
COPY pixi.toml pixi.lock ./
RUN pixi install --locked
# The runtime's orca, for `orca locate` when the project has no bin/scheduler.mojo.
COPY --from=runtime /opt/orca /opt/orca-runtime
# Declared after `pixi install`: a RUN sees every ARG declared before it, so a
# new ORCA_COMMIT on each commit would otherwise rerun it.
ARG MOJO_TARGET_CPU=neoverse-n1
ARG ORCA_SOURCE=""
ARG ORCA_COMMIT=""
COPY . .
RUN mkdir -p /out/bin /out/pipelines \
    && pixi run sh -c 'inc=""; [ -d src ] && inc="-I src"; \
        for f in bin/*.mojo; do [ -f "$f" ] || continue; n=$(basename "$f" .mojo); [ "$n" = scheduler ] && n=orca; \
            mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" $inc "$f" -o "/out/bin/$n" || exit 1; done; \
        for p in pipelines/*.mojo; do [ -f "$p" ] || continue; n=$(basename "$p" .mojo); \
            mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" $inc -D ORCA_SOURCE='"${ORCA_SOURCE}"' -D ORCA_COMMIT='"${ORCA_COMMIT}"' \
                "$p" -o "/out/pipelines/$n" || exit 1; done; \
        orca=/out/bin/orca; [ -x "$orca" ] || orca=/opt/orca-runtime/bin/orca; \
        if ls /out/pipelines/* > /dev/null 2>&1; then \
            LD_LIBRARY_PATH="$CONDA_PREFIX/lib:/opt/orca-runtime/lib" "$orca" locate /out/pipelines/* --root .; fi'
# The line tables compressed, which llvm-symbolizer reads as they are.
RUN for b in /out/bin/* /out/pipelines/*; do [ -x "$b" ] || continue; \
        objcopy --compress-debug-sections=zlib "$b" || exit 1; done
# Only the pixi environment's libraries the binaries link or dlopen: libpq
# (postgres), flare's shims (orca ui, apps), and every tin's shim, such as
# objectstore's and the codecs' (zstd, for Iceberg writes). Not conda's
# libpsl and ICU, which only libcurl links: the runtime has Debian's libpsl5.
RUN env=/src/project/.pixi/envs/default && mkdir -p /out/lib \
    && loaded=$(ls $env/lib/libpq.so* $env/lib/libflare_*.so $env/lib/lib*mojo*.so 2>/dev/null) \
    && ldd /out/bin/* /out/pipelines/* $loaded 2> /dev/null | awk -v e="$env" '$3 ~ "^"e {print $3}' \
        | grep -v -e '/libicu' -e '/libpsl' | sort -u | xargs -r cp -L -t /out/lib \
    && cp -L -t /out/lib $loaded

FROM runtime
COPY --from=build /out/lib /opt/orca/lib
COPY --from=build /out/bin /opt/orca/bin
COPY --from=build /out/pipelines /opt/orca/pipelines
