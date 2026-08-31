# syntax=docker/dockerfile:1
FROM ubuntu:26.04 AS package

ARG PACKAGE_VERSION
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_NET_GIT_FETCH_WITH_CLI=true \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:/usr/local/rustup/toolchains/1.91.1-x86_64-unknown-linux-gnu/bin:${PATH} \
    CC=gcc-15 \
    CXX=g++-15 \
    LIBCLANG_PATH=/usr/lib/llvm-19/lib \
    CMAKE_GENERATOR=Ninja \
    TRIEDB_TARGET=triedb_driver \
    ASMFLAGS=-march=haswell \
    CFLAGS=-march=haswell \
    CXXFLAGS=-march=haswell

# build-essential carries the packaging tools (including dpkg-shlibdeps);
# patchelf removes build-tree runpaths before dependency analysis.
RUN apt-get update && apt-get install -y --no-install-recommends \
      binutils build-essential ca-certificates clang-19 cmake curl g++-15 gcc-15 git \
      libabsl-dev libarchive-dev libbenchmark-dev libboost-all-dev libbrotli-dev libcap-dev \
      libcgroup-dev libclang-19-dev libcli11-dev libcrypto++-dev libgmp-dev \
      libgmock-dev libgtest-dev libhugetlbfs-dev libmagicenum-dev libmimalloc-dev \
      libsecp256k1-dev libssl-dev \
      libstdc++-15-dev libtbb-dev liburing-dev libzstd-dev ninja-build patchelf pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain 1.91.1

# .dockerignore strips .git, so scripts/package-version cannot run here.
RUN test -n "$PACKAGE_VERSION"

WORKDIR /src
COPY . .

RUN --mount=type=cache,id=monad-services-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=monad-services-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=monad-services-target,target=/src/target \
    PACKAGE_VERSION="${PACKAGE_VERSION}" DEB_OUTPUT_DIR=/out ./scripts/build-deb

FROM scratch
COPY --from=package /out /out
