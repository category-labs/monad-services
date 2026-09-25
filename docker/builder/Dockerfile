FROM ubuntu:26.04

ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:${PATH} \
    CC=gcc-15 \
    CXX=g++-15 \
    LIBCLANG_PATH=/usr/lib/llvm-19/lib \
    CMAKE_GENERATOR=Ninja \
    TRIEDB_TARGET=triedb_driver \
    ASMFLAGS=-march=haswell \
    CFLAGS=-march=haswell \
    CXXFLAGS=-march=haswell

RUN apt-get update && apt-get install -y --no-install-recommends \
      binutils ca-certificates clang-19 cmake curl g++-15 gcc-15 git \
      libabsl-dev libarchive-dev libbenchmark-dev libboost-all-dev libbrotli-dev libcap-dev \
      libcgroup-dev libclang-19-dev libcli11-dev libcrypto++-dev libgmp-dev \
      libgmock-dev libgtest-dev libhugetlbfs-dev libmagicenum-dev libmimalloc-dev \
      libsecp256k1-dev libssl-dev \
      libstdc++-15-dev libtbb-dev liburing-dev libzstd-dev ninja-build pkg-config \
    && rm -rf /var/lib/apt/lists/*

# The toolchain version comes from rust-toolchain.toml, which the rustup
# shims on PATH also consult at cargo time, so image and build can't drift.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none
COPY rust-toolchain.toml /tmp/rust-toolchain/rust-toolchain.toml
RUN cd /tmp/rust-toolchain && rustup toolchain install && rm -rf /tmp/rust-toolchain
