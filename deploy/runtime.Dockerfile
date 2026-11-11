# orca's runtime image, ghcr.io/magmalake/orca-runtime:<orca version>, which
# a project's image builds FROM (deploy/project.Dockerfile): orca's own
# `orca` (bin/scheduler.mojo) and `orca_maintain` (bin/orca_maintain.mojo),
# git, libcurl, the CA bundles where libcurl, OpenSSL and flare look for
# them, llvm-symbolizer, and the /opt/orca layout and environment that
# deploy/hetzner and `orca aws` run. orca's CI builds and tests it on every
# pull request and pushes it for each release (.github/workflows/
# runtime-image.yml). Build it from the repository root:
#
#     docker build -f deploy/runtime.Dockerfile -t orca-runtime:dev .
#
# The binaries are built in deploy/runtime/pixi.toml, a project that adds
# this checkout of orca-mojo, so they link the same packages a project's
# pipelines do. Like the project's image, everything has line tables (-g1),
# compressed, and is built for MOJO_TARGET_CPU, not for the CPU of the
# machine building it: GitHub's arm64 runners are Neoverse N2, with SVE2,
# and Hetzner's CAX servers Neoverse N1, where an N2 binary dies with
# SIGILL (exit 132). N1 code runs on N2, V1, V2 and Graviton. CI builds
# amd64 for x86-64-v3 (AVX2).
#
# The stages are ordered for the layer cache: `env` is the pixi environment
# and depends only on orca's pixi.toml and src/; `curl` and `git` start from
# it, so a change to bin/ alone reuses `pixi install` and git as they were.
FROM ghcr.io/prefix-dev/pixi:0.78.0 AS env
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY pixi.toml /src/orca.mojo/pixi.toml
COPY src /src/orca.mojo/src
# At /src/project, the directory deploy/project.Dockerfile builds a project
# in, so the CA bundle paths compiled into conda's libcurl and OpenSSL are
# the same in both (below).
COPY deploy/runtime/pixi.toml /src/project/pixi.toml
WORKDIR /src/project
RUN pixi install

FROM env AS build
ARG MOJO_TARGET_CPU=neoverse-n1
# Beside the checkout, not in it: pixi would see orca-mojo's source change
# and build the package again.
COPY bin /src/bin
RUN mkdir -p /out/bin /out/share && cp pixi.lock /out/share/pixi.lock \
    && pixi run sh -c 'mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" /src/bin/scheduler.mojo -o /out/bin/orca \
        && mojo build -g1 --target-cpu "$MOJO_TARGET_CPU" /src/bin/orca_maintain.mojo -o /out/bin/orca_maintain'
# The line tables compressed, which llvm-symbolizer reads as they are.
RUN for b in /out/bin/*; do objcopy --compress-debug-sections=zlib "$b" || exit 1; done
# Only the pixi environment's libraries the binaries link or dlopen: libpq
# (postgres), libsqlite3 (orca.sqlite_store), flare's shims (orca ui, apps)
# and every tin's shim, such as objectstore's and the codecs' (zstd, for
# Iceberg writes); and llvm-symbolizer, which Mojo's runtime runs to name a
# crash's frames, with what it links. Stripped of their symbol tables. Not
# conda's libpsl, which brings ICU (36 MB) and which libcurl alone links:
# the image has Debian's libpsl5. OpenSSL stays conda's: libcurl and libpq
# need 3.2.
RUN env=/src/project/.pixi/envs/default && mkdir -p /out/lib /out/libexec \
    && cp -L $env/bin/llvm-symbolizer /out/libexec/ \
    && loaded=$(ls $env/lib/libpq.so.5 $env/lib/libsqlite3.so.0 $env/lib/libflare_*.so $env/lib/lib*mojo*.so 2>/dev/null) \
    && ldd /out/bin/* /out/libexec/* $loaded 2> /dev/null \
        | awk -v e="$env" '$3 ~ "^"e {print $3}' | grep -v -e '/libicu' -e '/libpsl' \
        | sort -u | xargs -r cp -L -t /out/lib \
    && cp -L -t /out/lib $loaded \
    && ln -s libpq.so.5 /out/lib/libpq.so \
    && for f in /out/lib/*; do [ -L "$f" ] || strip --strip-unneeded "$f" || exit 1; done

# git, for pipelines that clone or fetch (mojo-census's mojo_history and
# mojo_versions): only the `git` binary (every builtin) and
# git-remote-https, without Perl, Tcl/Tk, gettext, Python or expat (Debian's
# git package and what it pulls in: 90 MB; this, 5 MB). Built on the image's
# Debian against the pixi environment's libcurl, which /opt/orca/lib ships
# anyway (objectstore links it). `curl` gathers libcurl, the pixi libraries
# it links (not libpsl or ICU, as above) and its headers from `env`, so git
# is built again only when the pixi environment changes.
FROM env AS curl
RUN env=/src/project/.pixi/envs/default && mkdir -p /deps/lib /deps/include \
    && cp -L $env/lib/libcurl.so.4 /deps/lib/ \
    && ldd $env/lib/libcurl.so.4 | awk -v e="$env" '$3 ~ "^"e {print $3}' \
        | grep -v -e '/libicu' -e '/libpsl' | sort -u | xargs -r cp -L -t /deps/lib \
    && cp -r $env/include/curl /deps/include/

FROM debian:bookworm-slim AS git
ARG GIT_VERSION=2.51.2
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gcc make libc6-dev \
      zlib1g-dev libpsl5 xz-utils \
    && rm -rf /var/lib/apt/lists/*
COPY --from=curl /deps /deps
RUN curl -fsSL https://mirrors.edge.kernel.org/pub/software/scm/git/git-$GIT_VERSION.tar.xz | tar xJ -C /usr/src \
    && cd /usr/src/git-$GIT_VERSION && ln -s libcurl.so.4 /deps/lib/libcurl.so \
    && make -j"$(nproc)" prefix=/opt/orca gitexecdir=/opt/orca/libexec/git \
        template_dir=/opt/orca/libexec/git/templates \
        NO_PERL=1 NO_TCLTK=1 NO_GETTEXT=1 NO_PYTHON=1 NO_EXPAT=1 NO_OPENSSL=1 NO_INSTALL_HARDLINKS=1 \
        CURL_CFLAGS=-I/deps/include CURL_LDFLAGS='-L/deps/lib -lcurl -Wl,-rpath-link,/deps/lib' \
        git git-remote-http \
    && mkdir -p /opt/orca/bin /opt/orca/libexec/git/templates \
    && install -s git /opt/orca/libexec/git/git \
    && install -s git-remote-http /opt/orca/libexec/git/git-remote-https \
    && ln -s ../libexec/git/git /opt/orca/bin/git

FROM debian:bookworm-slim
# tzdata: schedules read the system's TZif files. libpsl5 for libcurl (see
# above).
RUN apt-get update && apt-get install -y --no-install-recommends tzdata ca-certificates libpsl5 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/lib /opt/orca/lib
COPY --from=build /out/bin /opt/orca/bin
COPY --from=build /out/libexec /opt/orca/libexec
COPY --from=build /out/share /opt/orca/share
COPY --from=git /opt/orca /opt/orca
# libcurl (objectstore, git) and OpenSSL (flare) look for CA certificates
# under the build environment's prefix, compiled in when conda relocated
# them: /src/project/.pixi/envs/default/ssl, here and in a project's image
# alike. A fetch that hands flare $CONDA_PREFIX/ssl/cacert.pem finds the
# same bundle at /opt/orca/ssl. /opt/orca/pipelines is where a project's
# image puts its pipelines.
COPY --from=build /src/project/.pixi/envs/default/ssl /opt/orca/ssl
RUN mkdir -p /opt/orca/pipelines /src/project/.pixi/envs/default \
    && ln -s /opt/orca/ssl /src/project/.pixi/envs/default/ssl
ARG ORCA_VERSION=""
LABEL org.opencontainers.image.title="orca-runtime" org.opencontainers.image.version="${ORCA_VERSION}"
# postgres.mojo and flare find what they dlopen under $CONDA_PREFIX/lib.
ENV CONDA_PREFIX=/opt/orca LD_LIBRARY_PATH=/opt/orca/lib PATH=/opt/orca/bin:/opt/orca/pipelines:/usr/bin:/bin \
    LLVM_SYMBOLIZER_PATH=/opt/orca/libexec/llvm-symbolizer GIT_EXEC_PATH=/opt/orca/libexec/git
ENTRYPOINT ["/opt/orca/bin/orca"]
