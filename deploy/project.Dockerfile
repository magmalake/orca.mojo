# The image `orca aws push` builds for a project that added orca with
# `pixi shelf add orca-mojo`: its own `orca` (bin/scheduler.mojo, the two
# lines calling orca.command), its apps (every other bin/*.mojo, at
# /opt/orca/bin/<name>, which `orca aws app` runs; `orca_maintain`, the
# snapshot expiry `orca expire` runs, is made for a project whose pipelines
# use orca.tables and that has no bin/orca_maintain.mojo of its own) and its
# pipelines (every
# pipelines/*.mojo, at /opt/orca/pipelines/<name>), with `src/` on the
# import path if the project has one. A project with a Dockerfile.orca of
# its own builds that instead.
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
# with --build-arg MOJO_TARGET_CPU=<cpu>; this is for linux/arm64 images.
FROM ghcr.io/prefix-dev/pixi:0.78.0 AS build
ARG MOJO_TARGET_CPU=neoverse-n1
ARG ORCA_SOURCE=""
ARG ORCA_COMMIT=""
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src/project
COPY pixi.toml pixi.lock ./
RUN pixi install --locked
COPY . .
RUN mkdir -p /out/bin /out/pipelines \
    && pixi run sh -c 'inc=""; [ -d src ] && inc="-I src"; \
        [ -f bin/scheduler.mojo ] || { echo "orca aws push: no bin/scheduler.mojo calling orca.command"; exit 1; }; \
        for f in bin/*.mojo; do n=$(basename "$f" .mojo); [ "$n" = scheduler ] && n=orca; \
            mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" $inc "$f" -o "/out/bin/$n" || exit 1; done; \
        if [ ! -f bin/orca_maintain.mojo ] && grep -rqs "orca.tables" pipelines src; then \
            printf "from orca.maintain import maintain_main\n\n\ndef main() raises:\n    maintain_main()\n" > /tmp/orca_maintain.mojo; \
            mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" $inc /tmp/orca_maintain.mojo -o /out/bin/orca_maintain || exit 1; fi; \
        for p in pipelines/*.mojo; do [ -f "$p" ] || continue; n=$(basename "$p" .mojo); \
            mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" $inc -D ORCA_SOURCE='"${ORCA_SOURCE}"' -D ORCA_COMMIT='"${ORCA_COMMIT}"' \
                "$p" -o "/out/pipelines/$n" || exit 1; done; \
        if ls /out/pipelines/* > /dev/null 2>&1; then /out/bin/orca locate /out/pipelines/* --root .; fi'
# The line tables compressed, which llvm-symbolizer reads as they are.
RUN for b in /out/bin/* /out/pipelines/*; do [ -x "$b" ] || continue; \
        objcopy --compress-debug-sections=zlib "$b" || exit 1; done
# Only the pixi environment's libraries the binaries link or dlopen: libpq
# (postgres), flare's shims (orca ui, apps), and every tin's shim, such as
# objectstore's and the codecs' (zstd, for Iceberg writes); and
# llvm-symbolizer, which Mojo's runtime runs to name a crash's frames, with
# what it links.
RUN env=/src/project/.pixi/envs/default && mkdir -p /out/lib /out/libexec \
    && cp -L $env/bin/llvm-symbolizer /out/libexec/ \
    && loaded=$(ls $env/lib/libpq.so* $env/lib/libflare_*.so $env/lib/lib*mojo*.so 2>/dev/null) \
    && ldd /out/bin/* /out/pipelines/* /out/libexec/* $loaded 2> /dev/null | awk -v e="$env" '$3 ~ "^"e {print $3}' \
        | sort -u | xargs -r cp -L -t /out/lib \
    && cp -L -t /out/lib $loaded

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/lib /opt/orca/lib
COPY --from=build /out/bin /opt/orca/bin
COPY --from=build /out/pipelines /opt/orca/pipelines
COPY --from=build /out/libexec /opt/orca/libexec
ENV CONDA_PREFIX=/opt/orca LD_LIBRARY_PATH=/opt/orca/lib PATH=/opt/orca/bin:/opt/orca/pipelines:/usr/bin:/bin \
    LLVM_SYMBOLIZER_PATH=/opt/orca/libexec/llvm-symbolizer
ENTRYPOINT ["/opt/orca/bin/orca"]
