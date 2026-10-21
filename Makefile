SHELL := /bin/sh

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
PACKAGE_VERSION ?= $(shell ./debian/scripts/dev-version)
DEB_OUTPUT_DIR ?= dist
STRIP_SYMBOLS ?= 1

export CONTAINER_ENGINE PACKAGE_VERSION DEB_OUTPUT_DIR STRIP_SYMBOLS

.PHONY: deb deb-host clean

deb:
	./debian/scripts/package-deb-containerized

deb-host:
	./debian/scripts/package-deb

clean:
	rm -rf '$(DEB_OUTPUT_DIR)'
