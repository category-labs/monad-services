SHELL := /bin/sh

CONTAINER_ENGINE ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
PACKAGE_VERSION ?= $(shell ./debian/scripts/dev-version)
DEB_OUTPUT_DIR ?= dist
STRIP_SYMBOLS ?= 1

export CONTAINER_ENGINE PACKAGE_VERSION DEB_OUTPUT_DIR STRIP_SYMBOLS

.PHONY: deb deb-unchecked deb-host deb-host-unchecked clean

deb:
	LINTIAN_CHECK=1 ./debian/scripts/package-deb-containerized

deb-unchecked:
	LINTIAN_CHECK=0 ./debian/scripts/package-deb-containerized

deb-host: deb-host-unchecked
	lintian '$(DEB_OUTPUT_DIR)/monad-services_$(PACKAGE_VERSION)_amd64.deb'

deb-host-unchecked:
	./debian/scripts/package-deb

clean:
	rm -rf '$(DEB_OUTPUT_DIR)'
