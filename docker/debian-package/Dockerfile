# syntax=docker/dockerfile:1

# Toolchain comes from docker/builder/Dockerfile. `make deb` builds it
# locally; override to reuse a prebuilt copy.
ARG BUILDER_IMAGE=monad-services-builder:local
FROM ${BUILDER_IMAGE} AS package

ARG PACKAGE_VERSION
ENV DEBIAN_FRONTEND=noninteractive \
    CARGO_NET_GIT_FETCH_WITH_CLI=true

# Only what docker/builder does not already carry: build-essential for the
# packaging tools (including dpkg-shlibdeps) and patchelf for runpaths.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential patchelf \
    && rm -rf /var/lib/apt/lists/*

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
