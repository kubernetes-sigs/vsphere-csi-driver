all: build

# Get the absolute path and name of the current directory.
PWD := $(abspath .)
BASE_DIR := $(notdir $(PWD))
IMAGE_CI := $(shell $(MAKE) --no-print-directory -C hack/images/ci print)

# PROJECT_ROOT is used when host access is required when running
# Docker-in-Docker (DinD).
export PROJECT_ROOT ?= $(PWD)

# BUILD_OUT is the root directory containing the build output.
export BUILD_OUT ?= .build

# BIN_OUT is the directory containing the built binaries.
export BIN_OUT ?= $(BUILD_OUT)/bin

# DIST_OUT is the directory containting the distribution packages
export DIST_OUT ?= $(BUILD_OUT)/dist

# ARTIFACTS is the directory containing artifacts uploaded to the Kubernetes
# test grid at the end of the job.
export ARTIFACTS ?= $(BUILD_OUT)/artifacts

# K8S_VERSION is the specific version of Kubernetes test binaries.
export K8S_VERSION ?= ci/latest

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
export VERSION ?= $(shell git describe --exact-match 2>/dev/null || git describe --match=$$(git rev-parse --short=8 HEAD) --always --dirty --abbrev=8)

# Load the image registry include.
include hack/make/login-to-image-registry.mk

# Define the images.
IMAGE_CSI := $(REGISTRY)/vsphere-csi
IMAGE_SYNCER := $(REGISTRY)/syncer
.PHONY: print-csi-image print-syncer-image

# Printing the image version is defined early so Go modules aren't forced.
print-csi-image:
	@echo $(IMAGE_CSI):$(VERSION)
print-syncer-image:
	@echo $(IMAGE_SYNCER):$(VERSION)

################################################################################
##                                BUILD DIRS                                  ##
################################################################################
.PHONY: build-dirs
build-dirs:
	@mkdir -p $(BIN_OUT)
	@mkdir -p $(DIST_OUT)
	@mkdir -p $(ARTIFACTS)

################################################################################
##                              BUILD BINARIES                                ##
################################################################################
# Unless otherwise specified the binaries should be built for linux-amd64.
GOOS ?= linux
GOARCH ?= amd64

LDFLAGS := $(shell cat hack/make/ldflags.txt)
LDFLAGS_CSI := $(LDFLAGS) -X "$(MOD_NAME)/pkg/csi/service.version=$(VERSION)"
LDFLAGS_SYNCER := $(LDFLAGS)


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

# The Syncer binary.
SYNCER_BIN_NAME := syncer
SYNCER_BIN := $(BIN_OUT)/$(SYNCER_BIN_NAME).$(GOOS)_$(GOARCH)
build-syncer: $(SYNCER_BIN)
ifndef SYNCER_BIN_SRCS
SYNCER_BIN_SRCS := cmd/$(SYNCER_BIN_NAME)/main.go go.mod go.sum
SYNCER_BIN_SRCS += $(addsuffix /*.go,$(shell go list -f '{{ join .Deps "\n" }}' ./cmd/$(SYNCER_BIN_NAME) | grep $(MOD_NAME) | sed 's~$(MOD_NAME)~.~'))
export SYNCER_BIN_SRCS
endif
$(SYNCER_BIN): $(SYNCER_BIN_SRCS)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -ldflags '$(LDFLAGS_SYNCER)' -o $(abspath $@) $<
	@touch $@

# The default build target.
build build-bins: $(CSI_BIN) $(SYNCER_BIN)
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

dist-csi: dist-csi-tgz dist-csi-zip

DIST_SYNCER_NAME := vsphere-syncer-$(VERSION)
DIST_SYNCER_TGZ := $(BUILD_OUT)/dist/$(DIST_SYNCER_NAME)-$(GOOS)_$(GOARCH).tar.gz
dist-syncer-tgz: $(DIST_SYNCER_TGZ)
$(DIST_SYNCER_TGZ): $(SYNCER_BIN)
	_temp_dir=$$(mktemp -d) && cp $< "$${_temp_dir}/$(SYNCER_BIN_NAME)" && \
	tar czf $(abspath $@) README.md LICENSE -C "$${_temp_dir}" "$(SYNCER_BIN_NAME)" && \
	rm -fr "$${_temp_dir}"
DIST_SYNCER_ZIP := $(BUILD_OUT)/dist/$(DIST_SYNCER_NAME)-$(GOOS)_$(GOARCH).zip
dist-syncer-zip: $(DIST_SYNCER_ZIP)
$(DIST_SYNCER_ZIP): $(SYNCER_BIN)
	_temp_dir=$$(mktemp -d) && cp $< "$${_temp_dir}/$(SYNCER_BIN_NAME)" && \
	zip -j $(abspath $@) README.md LICENSE "$${_temp_dir}/$(SYNCER_BIN_NAME)" && \
	rm -fr "$${_temp_dir}"
dist-syncer: dist-syncer-tgz dist-syncer-zip

dist: dist-csi dist-syncer

################################################################################
##                                DEPLOY                                      ##
################################################################################
# The deploy target is for use by Prow.
.PHONY: deploy
deploy:
	$(MAKE) check
	$(MAKE) build-bins
	$(MAKE) unit-test
	$(MAKE) build-images
	$(MAKE) integration-test
	$(MAKE) push-images

################################################################################
##                                 CLEAN                                      ##
################################################################################
.PHONY: clean
clean:
	@rm -f Dockerfile*
	rm -f	$(CSI_BIN) vsphere-csi-*.tar.gz vsphere-csi-*.zip \
			$(SYNCER_BIN) vsphere-syncer-*.tar.gz vsphere-syncer-*.zip \
			image-*.tar image-*.d $(DIST_OUT)/* $(BIN_OUT)/* $(ARTIFACTS)/*
	GO111MODULE=off go clean -i -x . ./cmd/$(CSI_BIN_NAME) ./cmd/$(SYNCER_BIN_NAME)

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
export PKGS_WITH_TESTS := $(sort $(shell find . -path ./tests -prune -o -name "*_test.go" -type f -exec dirname \{\} \;))
endif
TEST_FLAGS ?= -v -count=1
.PHONY: unit build-unit-tests
unit unit-test:
	env -u VSPHERE_SERVER -u VSPHERE_PASSWORD -u VSPHERE_USER -u VSPHERE_STORAGE_POLICY_NAME -u KUBECONFIG go test $(TEST_FLAGS) $(PKGS_WITH_TESTS)
build-unit-tests:
	$(foreach pkg,$(PKGS_WITH_TESTS),go test $(TEST_FLAGS) -c $(pkg); )

.PHONY: integration-unit-test
integration-unit-test:
ifndef VSPHERE_VCENTER
	$(error Requires VSPHERE_VCENTER from a deployed testbed to run integration-unit-test)
endif
ifndef VSPHERE_USER
	$(error Requires VSPHERE_USER from a deployed testbed to run integration-unit-test)
endif
ifndef VSPHERE_PASSWORD
	$(error Requires VSPHERE_PASSWORD from a deployed testbed to run integration-unit-test)
endif
ifndef VSPHERE_DATACENTER
	$(error Requires VSPHERE_DATACENTER from a deployed testbed to run integration-unit-test)
endif
ifndef VSPHERE_DATASTORE_URL
	$(error Requires VSPHERE_DATASTORE_URL from a deployed testbed to run integration-unit-test)
endif
ifndef VSPHERE_INSECURE
	$(error Requires VSPHERE_INSECURE from a deployed testbed to run integration-unit-test)
endif
	    go test $(TEST_FLAGS) -tags=integration-unit ./pkg/syncer ./pkg/csi/service/block/vanilla ./pkg/csi/service/block/wcp

# The default test target.
.PHONY: test build-tests
test: unit
build-tests: build-unit-tests

.PHONY: cover
cover: TEST_FLAGS += -cover
cover: test

KUBETEST_COMMON_FLAGS := --provider=skeleton --check-version-skew=false
.PHONY: conformance-test
conformance-test: | $(DOCKER_SOCK)
ifndef KUBECONFIG
	$(error no KUBECONFIG provided to run conformance test!)
else
ifeq (,$(wildcard $(KUBECONFIG)))
	$(error $(KUBECONFIG) does not exist!)
endif
endif
ifeq (true,$(DOCKER_IN_DOCKER_ENABLED))
	@export KUBERNETES_CONFORMANCE_TEST=y && \
	kubetest --extract=$(K8S_VERSION) && \
	cd kubernetes && \
	kubetest $(KUBETEST_COMMON_FLAGS) --ginkgo-parallel=5 \
	  --test --test_args="--ginkgo.focus=\[Conformance\] --ginkgo.skip=\[Serial\]|\[Flaky\]|\[Disruptive\]" && \
	kubetest $(KUBETEST_COMMON_FLAGS) \
	  --test --test_args="--ginkgo.focus=\[Serial\].*\[Conformance\] --ginkgo.skip=\[Flaky\]|\[Disruptive\]"
else
	@docker run -it --rm  \
	  -e "PROJECT_ROOT=$(PROJECT_ROOT)" \
	  -v $(DOCKER_SOCK):$(DOCKER_SOCK) \
	  -v "$(PWD)":/go/src/sigs.k8s.io/vsphere-csi-driver \
	  -e "K8S_VERSION=$${K8S_VERSION}" \
	  -e "ARTIFACTS=/artifacts"    -v "$(abspath $(ARTIFACTS))":/artifacts \
	  -e "KUBECONFIG=/kubeconfig"	 -v $(KUBECONFIG):/kubeconfig:ro \
	  $(IMAGE_CI) \
	  make conformance-test
endif

.PHONY: test-e2e
test-e2e:
	hack/run-e2e-test.sh

################################################################################
##                                 LINTING                                    ##
################################################################################
FMT_FLAGS ?= -d -e -s -w
.PHONY: fmt
fmt:
	f="$$(mktemp)" && \
	find . -name "*.go" | grep -v vendor | xargs gofmt $(FMT_FLAGS) | tee "$${f}"; \
	test -z "$$(head -n 1 "$${f}")"

.PHONY: vet
vet:
	go vet ./...

HAS_LINT := $(shell command -v golint 2>/dev/null)
.PHONY: lint
lint:
ifndef HAS_LINT
	cd / && GO111MODULE=off go get -u github.com/golang/lint/golint
endif
	f=$$(mktemp); \
	go list ./... | xargs golint -set_exit_status 2>&1 >"$${f}" || \
	{ x="$${?}"; sed 's~$(PWD)~.~' 1>&2 <"$${f}"; }; \
	rm -f "$${f}"; exit "$${x:-0}"

.PHONY: check
check: build-dirs
	JUNIT_REPORT="$(abspath $(ARTIFACTS)/junit_check.xml)" hack/check.sh

.PHONY: check-warn
check-warn:
	-$(MAKE) check

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

IMAGE_SYNCER_D := image-syncer-$(VERSION).d
build-syncer-image syncer-image: $(IMAGE_SYNCER_D)
$(IMAGE_SYNCER): $(IMAGE_SYNCER_D)
ifneq ($(GOOS),linux)
$(IMAGE_SYNCER_D):
	$(error Please set GOOS=linux for building $@)
else
$(IMAGE_SYNCER_D): $(SYNCER_BIN) | $(DOCKER_SOCK)
	cp -f $< cluster/images/syncer/vsphere-syncer
	docker build -t $(IMAGE_SYNCER):$(VERSION) cluster/images/syncer
	docker tag $(IMAGE_SYNCER):$(VERSION) $(IMAGE_SYNCER):latest
	@rm -f cluster/images/syncer/vsphere-syncer && touch $@
endif

build-images images: build-csi-image build-syncer-image

################################################################################
##                                  PUSH IMAGES                               ##
################################################################################
.PHONY: push-$(IMAGE_CSI) upload-$(IMAGE_CSI)
push-csi-image upload-csi-image: upload-$(IMAGE_CSI)
push-$(IMAGE_CSI) upload-$(IMAGE_CSI): $(IMAGE_CSI_D) login-to-image-registry | $(DOCKER_SOCK)
	docker push $(IMAGE_CSI):$(VERSION)
	docker push $(IMAGE_CSI):latest
.PHONY: push-$(IMAGE_SYNCER) upload-$(IMAGE_SYNCER)
push-syncer-image upload-syncer-image: upload-$(IMAGE_SYNCER)
push-$(IMAGE_SYNCER) upload-$(IMAGE_SYNCER): $(IMAGE_SYNCER_D) login-to-image-registry | $(DOCKER_SOCK)
	docker push $(IMAGE_SYNCER):$(VERSION)
	docker push $(IMAGE_SYNCER):latest
.PHONY: push-images upload-images
push-images upload-images: upload-csi-image upload-syncer-image

################################################################################
##                                  CI IMAGE                                  ##
################################################################################
.PHONY: build-ci-image
build-ci-image:
	$(MAKE) -C hack/images/ci build

.PHONY: push-ci-image
push-ci-image:
	$(MAKE) -C hack/images/ci push

.PHONY: print-ci-image
print-ci-image:
	@echo $(IMAGE_CI)

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

