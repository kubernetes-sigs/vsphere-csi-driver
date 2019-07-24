all: build

# Get the absolute path and name of the current directory.
PWD := $(abspath .)
BASE_DIR := $(notdir $(PWD))

# BUILD_OUT is the root directory containing the build output.
export BUILD_OUT ?= .build

# BIN_OUT is the directory containing the built binaries.
export BIN_OUT ?= $(BUILD_OUT)/bin

# DIST_OUT is the directory containting the distribution packages
export DIST_OUT ?= $(BUILD_OUT)/dist

-include hack/make/docker.mk

################################################################################
##                             VERIFY GO VERSION                              ##
################################################################################
# Go 1.11+ required for Go modules.
GO_VERSION_EXP := "go1.11"
GO_VERSION_ACT := $(shell a="$$(go version | awk '{print $$3}')" && test $$(printf '%s\n%s' "$${a}" "$(GO_VERSION_EXP)" | sort | tail -n 1) = "$${a}" && printf '%s' "$${a}")
ifndef GO_VERSION_ACT
$(error Requires Go $(GO_VERSION_EXP)+ for Go module support)
endif
MOD_NAME := $(shell head -n 1 <go.mod | awk '{print $$2}')

################################################################################
##                             VERIFY BUILD PATH                              ##
################################################################################
ifneq (on,$(GO111MODULE))
export GO111MODULE := on
# should not be cloned inside the GOPATH.
GOPATH := $(shell go env GOPATH)
ifeq (/src/$(MOD_NAME),$(subst $(GOPATH),,$(PWD)))
$(warning This project uses Go modules and should not be cloned into the GOPATH)
endif
endif

################################################################################
##                                DEPENDENCIES                                ##
################################################################################
# Verify the dependencies are in place.
.PHONY: deps
deps:
	go mod download && go mod verify

################################################################################
##                                VERSIONS                                    ##
################################################################################
# Ensure the version is injected into the binaries via a linker flag.
export VERSION ?= $(shell git describe --always --dirty)

# Load the image registry include.
include hack/make/login-to-image-registry.mk

# Define the images.
IMAGE_CSI := $(REGISTRY)/vsphere-csi
.PHONY: print-image

# Printing the image version is defined early so Go modules aren't forced.
print-image:
	@echo $(IMAGE_CSI):$(VERSION)

################################################################################
##                                BUILD DIRS                                  ##
################################################################################
.PHONY: build-dirs
build-dirs:
	@mkdir -p $(BIN_OUT)
	@mkdir -p $(DIST_OUT)

################################################################################
##                              BUILD BINARIES                                ##
################################################################################
# Unless otherwise specified the binaries should be built for linux-amd64.
GOOS ?= linux
GOARCH ?= amd64

LDFLAGS := $(shell cat hack/make/ldflags.txt)
LDFLAGS_CSI := $(LDFLAGS) -X "$(MOD_NAME)/pkg/csi/service.version=$(VERSION)"

# The CSI binary.
CSI_BIN_NAME := vsphere-csi
CSI_BIN := $(BIN_OUT)/$(CSI_BIN_NAME).$(GOOS)_$(GOARCH)
build-csi: $(CSI_BIN)
ifndef CSI_BIN_SRCS
CSI_BIN_SRCS := cmd/$(CSI_BIN_NAME)/main.go go.mod go.sum
CSI_BIN_SRCS += $(addsuffix /*.go,$(shell go list -f '{{ join .Deps "\n" }}' ./cmd/$(CSI_BIN_NAME) | grep $(MOD_NAME) | sed 's~$(MOD_NAME)~.~'))
export CSI_BIN_SRCS
endif
$(CSI_BIN): $(CSI_BIN_SRCS)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags '$(LDFLAGS_CSI)' -o $(abspath $@) $<
	@touch $@

# The default build target.
build build-bins: $(CSI_BIN)
build-with-docker:
	hack/make.sh

################################################################################
##                                   DIST                                     ##
################################################################################
DIST_CSI_NAME := vsphere-csi-$(VERSION)
DIST_CSI_TGZ := $(DIST_OUT)/$(DIST_CSI_NAME)-$(GOOS)_$(GOARCH).tar.gz
dist-csi-tgz: build-dirs $(DIST_CSI_TGZ)
$(DIST_CSI_TGZ): $(CSI_BIN)
	_temp_dir=$$(mktemp -d) && cp $< "$${_temp_dir}/$(CSI_BIN_NAME)" && \
	tar czf $(abspath $@) README.md LICENSE -C "$${_temp_dir}" "$(CSI_BIN_NAME)" && \
	rm -fr "$${_temp_dir}"

DIST_CSI_ZIP := $(DIST_OUT)/$(DIST_CSI_NAME)-$(GOOS)_$(GOARCH).zip
dist-csi-zip: build-dirs $(DIST_CSI_ZIP)
$(DIST_CSI_ZIP): $(CSI_BIN)
	_temp_dir=$$(mktemp -d) && cp $< "$${_temp_dir}/$(CSI_BIN_NAME)" && \
	zip -j $(abspath $@) README.md LICENSE "$${_temp_dir}/$(CSI_BIN_NAME)" && \
	rm -fr "$${_temp_dir}"

dist: dist-csi-tgz dist-csi-zip

################################################################################
##                                DEPLOY                                      ##
################################################################################
# The deploy target is for use by Prow.
.PHONY: deploy
deploy: | $(DOCKER_SOCK)
	$(MAKE) build-bins
	$(MAKE) unit-test
	$(MAKE) build-images
	$(MAKE) push-images

################################################################################
##                                 CLEAN                                      ##
################################################################################
.PHONY: clean
clean:
	@rm -f Dockerfile*
	rm -f $(CSI_BIN) vsphere-csi-*.tar.gz vsphere-csi-*.zip \
		image-*.tar image-*.d $(DIST_OUT)/* $(BIN_OUT)/*
	GO111MODULE=off go clean -i -x . ./cmd/$(CSI_BIN_NAME)

.PHONY: clean-d
clean-d:
	@find . -name "*.d" -type f -delete

################################################################################
##                                CROSS BUILD                                 ##
################################################################################

# Defining X_BUILD_DISABLED prevents the cross-build and cross-dist targets
# from being defined. This is to improve performance when invoking x-build
# or x-dist targets that invoke this Makefile. The nested call does not need
# to provide cross-build or cross-dist targets since it's the result of one.
ifndef X_BUILD_DISABLED

export X_BUILD_DISABLED := 1

# Modify this list to add new cross-build and cross-dist targets.
X_TARGETS ?= darwin_amd64 linux_amd64 linux_386 linux_arm linux_arm64 linux_ppc64le

X_TARGETS := $(filter-out $(GOOS)_$(GOARCH),$(X_TARGETS))

X_CSI_BINS := $(addprefix $(CSI_BIN_NAME).,$(X_TARGETS))
$(X_CSI_BINS):
	GOOS=$(word 1,$(subst _, ,$(subst $(CSI_BIN_NAME).,,$@))) GOARCH=$(word 2,$(subst _, ,$(subst $(CSI_BIN_NAME).,,$@))) $(MAKE) build-csi

x-build-csi: $(CSI_BIN) $(X_CSI_BINS)

x-build: x-build-csi

################################################################################
##                                CROSS DIST                                  ##
################################################################################

X_DIST_CSI_TARGETS := $(X_TARGETS)
X_DIST_CSI_TARGETS := $(addprefix $(DIST_CSI_NAME)-,$(X_DIST_CSI_TARGETS))
X_DIST_CSI_TGZS := $(addsuffix .tar.gz,$(X_DIST_CSI_TARGETS))
X_DIST_CSI_ZIPS := $(addsuffix .zip,$(X_DIST_CSI_TARGETS))
$(X_DIST_CSI_TGZS):
	GOOS=$(word 1,$(subst _, ,$(subst $(DIST_CSI_NAME)-,,$@))) GOARCH=$(word 2,$(subst _, ,$(subst $(DIST_CSI_NAME)-,,$(subst .tar.gz,,$@)))) $(MAKE) dist-csi-tgz
$(X_DIST_CSI_ZIPS):
	GOOS=$(word 1,$(subst _, ,$(subst $(DIST_CSI_NAME)-,,$@))) GOARCH=$(word 2,$(subst _, ,$(subst $(DIST_CSI_NAME)-,,$(subst .zip,,$@)))) $(MAKE) dist-csi-zip

x-dist-csi-tgzs: $(DIST_CSI_TGZ) $(X_DIST_CSI_TGZS)
x-dist-csi-zips: $(DIST_CSI_ZIP) $(X_DIST_CSI_ZIPS)
x-dist-csi: x-dist-csi-tgzs x-dist-csi-zips

x-dist: x-dist-csi

################################################################################
##                               CROSS CLEAN                                  ##
################################################################################

X_CLEAN_TARGETS := $(addprefix clean-,$(X_TARGETS))
.PHONY: $(X_CLEAN_TARGETS)
$(X_CLEAN_TARGETS):
	GOOS=$(word 1,$(subst _, ,$(subst clean-,,$@))) GOARCH=$(word 2,$(subst _, ,$(subst clean-,,$@))) $(MAKE) clean

.PHONY: x-clean
x-clean: clean $(X_CLEAN_TARGETS)

endif # ifndef X_BUILD_DISABLED

################################################################################
##                                 TESTING                                    ##
################################################################################
ifndef PKGS_WITH_TESTS
export PKGS_WITH_TESTS := $(sort $(shell find . -name "*_test.go" -type f -exec dirname \{\} \;))
endif
TEST_FLAGS ?= -v
.PHONY: unit build-unit-tests
unit unit-test:
	go test $(TEST_FLAGS) $(PKGS_WITH_TESTS)
build-unit-tests:
	$(foreach pkg,$(PKGS_WITH_TESTS),go test $(TEST_FLAGS) -c $(pkg); )

# The default test target.
.PHONY: test build-tests
test: unit
build-tests: build-unit-tests

.PHONY: cover
cover: TEST_FLAGS += -cover
cover: test

################################################################################
##                                 LINTING                                    ##
################################################################################
.PHONY: fmt vet lint
fmt:
	hack/check-format.sh

vet:
	hack/check-vet.sh

lint:
	hack/check-lint.sh

.PHONY: check
check: fmt vet lint

################################################################################
##                                 BUILD IMAGES                               ##
################################################################################
IMAGE_CSI_D := image-csi-$(VERSION).d
build-csi-image csi-image: $(IMAGE_CSI_D)
$(IMAGE_CSI): $(IMAGE_CSI_D)
ifneq ($(GOOS),linux)
$(IMAGE_CSI_D):
	$(error Please set GOOS=linux for building $@)
else
$(IMAGE_CSI_D): $(CSI_BIN) | $(DOCKER_SOCK)
	cp -f $< cluster/images/csi/vsphere-csi
	docker build -t $(IMAGE_CSI):$(VERSION) cluster/images/csi
	docker tag $(IMAGE_CSI):$(VERSION) $(IMAGE_CSI):latest
	@rm -f cluster/images/csi/vsphere-csi && touch $@
endif

build-images images: build-csi-image

################################################################################
##                                  PUSH IMAGES                               ##
################################################################################
.PHONY: push-$(IMAGE_CSI) upload-$(IMAGE_CSI)
push-csi-image upload-csi-image: upload-$(IMAGE_CSI)
push-$(IMAGE_CSI) upload-$(IMAGE_CSI): $(IMAGE_CSI_D) login-to-image-registry | $(DOCKER_SOCK)
	docker push $(IMAGE_CSI):$(VERSION)
	docker push $(IMAGE_CSI):latest

.PHONY: push-images upload-images
push-images upload-images: upload-csi-image

################################################################################
##                                  CI IMAGE                                  ##
################################################################################
build-ci-image:
	$(MAKE) -C hack/images/ci build

push-ci-image:
	$(MAKE) -C hack/images/ci push

print-ci-image:
	@$(MAKE) --no-print-directory -C hack/images/ci print

################################################################################
##                               PRINT VERISON                                ##
################################################################################
.PHONY: version
version:
	@echo $(VERSION)

################################################################################
##                                TODO(akutz)                                 ##
################################################################################
TODO := docs godoc releasenotes translation
.PHONY: $(TODO)
$(TODO):
	@echo "$@ not yet implemented"
