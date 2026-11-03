# The image `orca aws push` builds for a project that added orca with
# `pixi shelf add orca-mojo`: its own `orca` (bin/scheduler.mojo, the two
# lines calling orca.command), its apps (every other bin/*.mojo, at
# /opt/orca/bin/<name>, which `orca aws app` runs) and its pipelines (every
# pipelines/*.mojo, at /opt/orca/pipelines/<name>), with `src/` on the
# import path if the project has one. A project with a Dockerfile.orca of
# its own builds that instead.
#
# ORCA_SOURCE and ORCA_COMMIT say what the pipelines were built from, which
# each run records and orca ui links.
FROM ghcr.io/prefix-dev/pixi:0.78.0 AS build
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
            mojo build $inc "$f" -o "/out/bin/$n" || exit 1; done; \
        for p in pipelines/*.mojo; do [ -f "$p" ] || continue; n=$(basename "$p" .mojo); \
            mojo build $inc -D ORCA_SOURCE='"${ORCA_SOURCE}"' -D ORCA_COMMIT='"${ORCA_COMMIT}"' \
                "$p" -o "/out/pipelines/$n" || exit 1; done; \
        if ls /out/pipelines/* > /dev/null 2>&1; then /out/bin/orca locate /out/pipelines/* --root .; fi'
# Only the pixi environment's libraries the binaries link or dlopen: libpq
# (postgres), flare's shims (orca ui, apps), objectstore's (dista).
RUN env=/src/project/.pixi/envs/default && mkdir -p /out/lib \
    && loaded=$(ls $env/lib/libpq.so* $env/lib/libflare_*.so $env/lib/libobjectstoremojo.so 2>/dev/null) \
    && ldd /out/bin/* /out/pipelines/* $loaded 2> /dev/null | awk -v e="$env" '$3 ~ "^"e {print $3}' \
        | sort -u | xargs -r cp -L -t /out/lib \
    && cp -L -t /out/lib $loaded

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/lib /opt/orca/lib
COPY --from=build /out/bin /opt/orca/bin
COPY --from=build /out/pipelines /opt/orca/pipelines
ENV CONDA_PREFIX=/opt/orca LD_LIBRARY_PATH=/opt/orca/lib PATH=/opt/orca/bin:/opt/orca/pipelines:/usr/bin:/bin
ENTRYPOINT ["/opt/orca/bin/orca"]
