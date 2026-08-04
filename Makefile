SHELL := /bin/sh

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
PACKAGE_VERSION ?= 0.0-$(shell git log -1 --format=%cd --date=format:%Y%m%d%H%M)-$(shell git rev-parse --short=12 HEAD)
DEB_OUTPUT_DIR ?= dist

.PHONY: build deb deb-container clean

build:
	cargo build --locked --release --package monad-archive --bins

deb:
	PACKAGE_VERSION=$(PACKAGE_VERSION) DEB_OUTPUT_DIR=$(DEB_OUTPUT_DIR) ./scripts/build-deb

deb-container:
	@test -n "$(CONTAINER_ENGINE)" || { echo "Install Docker or Podman, or set CONTAINER_ENGINE."; exit 1; }
	$(CONTAINER_ENGINE) build --build-arg PACKAGE_VERSION=$(PACKAGE_VERSION) -t monad-services-package:$(PACKAGE_VERSION) -f containers/Containerfile .
	@mkdir -p $(DEB_OUTPUT_DIR); container_id=$$($(CONTAINER_ENGINE) create monad-services-package:$(PACKAGE_VERSION) true); trap '$(CONTAINER_ENGINE) rm -f $$container_id >/dev/null' EXIT; $(CONTAINER_ENGINE) cp $$container_id:/out/. $(DEB_OUTPUT_DIR)

clean:
	rm -rf $(DEB_OUTPUT_DIR)
