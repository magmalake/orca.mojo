# The image orca runs from on Kubernetes: the `orca` binary (scheduler and
# UI) and pipeline binaries, built with pixi, on a slim Debian with only the
# libraries they load. Build it from the repository root:
#
#     docker build -f deploy/Dockerfile -t orca:dev .
#
# PIPELINES are the Mojo files to build into /opt/orca/pipelines/, each
# named after its file. ORCA_SOURCE and ORCA_COMMIT say what they were built
# from, which each run records and orca ui links (orca.source):
#
#     docker build --build-arg ORCA_SOURCE=$(git remote get-url origin) \
#         --build-arg ORCA_COMMIT=$(git rev-parse HEAD) ...
#
# dista and k8s compile from source at DISTA_REV and K8S_REV, the revisions
# CI checks out, cloned next to orca as pixi.toml expects.
FROM ghcr.io/prefix-dev/pixi:0.78.0 AS build
ARG DISTA_REV=0a388e3f4afc0f6e5b45f84516eb766d4414e730
ARG K8S_REV=b7977c8f7c3b7ffa95815be981f673380e596100
ARG PIPELINES="examples/toy.mojo examples/fanout.mojo"
ARG ORCA_SOURCE=""
ARG ORCA_COMMIT=""
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN git clone -q https://github.com/magmalake/dista.mojo /src/dista.mojo \
    && git -C /src/dista.mojo checkout -q ${DISTA_REV} \
    && git clone -q https://github.com/magmalake/k8s.mojo /src/k8s.mojo \
    && git -C /src/k8s.mojo checkout -q ${K8S_REV}
WORKDIR /src/orca.mojo
COPY pixi.toml pixi.lock ./
RUN pixi install --locked -e default
COPY src src
COPY bin bin
COPY examples examples
RUN mkdir -p /out/bin /out/pipelines \
    && pixi run -e default sh -c 'mojo build -I src $DEP_INC bin/scheduler.mojo -o /out/bin/orca \
        && for p in '"${PIPELINES}"'; do n=$(basename "$p" .mojo); \
            mojo build -I src $DEP_INC -D ORCA_SOURCE='"${ORCA_SOURCE}"' -D ORCA_COMMIT='"${ORCA_COMMIT}"' \
                "$p" -o "/out/pipelines/$n" || exit 1; done \
        && /out/bin/orca locate /out/pipelines/* --root .'
# What the binaries link, and what they dlopen from $CONDA_PREFIX/lib at run
# time (libpq for postgres.mojo, flare's shims for orca ui, objectstore's
# for dista) with what those link: only libraries from the pixi
# environment, as the system's are in the runtime image already.
RUN env=/src/orca.mojo/.pixi/envs/default && mkdir -p /out/lib \
    && loaded=$(ls $env/lib/libpq.so* $env/lib/libflare_*.so $env/lib/libobjectstoremojo.so 2>/dev/null) \
    && ldd /out/bin/orca /out/pipelines/* $loaded | awk -v e="$env" '$3 ~ "^"e {print $3}' \
        | sort -u | xargs -r cp -L -t /out/lib \
    && cp -L -t /out/lib $loaded

FROM debian:bookworm-slim
# tzdata: schedules read the system's TZif files.
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/lib /opt/orca/lib
COPY --from=build /out/bin /opt/orca/bin
COPY --from=build /out/pipelines /opt/orca/pipelines
# postgres.mojo and flare find what they dlopen under $CONDA_PREFIX/lib.
ENV CONDA_PREFIX=/opt/orca LD_LIBRARY_PATH=/opt/orca/lib PATH=/opt/orca/bin:/opt/orca/pipelines:/usr/bin:/bin
ENTRYPOINT ["/opt/orca/bin/orca"]
