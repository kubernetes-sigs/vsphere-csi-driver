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
# Go 1.13 required for Go modules.
GO_VERSION_EXP := "go1.13"
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
# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
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

.PHONY: version
version:
	@echo $(VERSION)

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
LDFLAGS_CSI := $(LDFLAGS) -X "$(MOD_NAME)/pkg/csi/service.Version=$(VERSION)"
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

syncer_manifest: controller-gen
	$(CONTROLLER_GEN) crd:trivialVersions=true paths=./pkg/syncer/storagepool/... output:crd:dir=pkg/apis/storagepool/config
# find or download controller-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	@{ \
	set -e ;\
	CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$CONTROLLER_GEN_TMP_DIR ;\
	go mod init tmp ;\
	go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.2.5 ;\
	rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif

$(SYNCER_BIN): $(SYNCER_BIN_SRCS) syncer_manifest
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
deploy: | $(DOCKER_SOCK)
	$(MAKE) build-bins
	$(MAKE) unit-test
	$(MAKE) push-images

################################################################################
##                                 CLEAN                                      ##
################################################################################
.PHONY: clean
clean:
	@rm -f Dockerfile*
	rm -f $(CSI_BIN) vsphere-csi-*.tar.gz vsphere-csi-*.zip \
		$(SYNCER_BIN) vsphere-syncer-*.tar.gz vsphere-syncer-*.zip \
		image-*.tar image-*.d $(DIST_OUT)/* $(BIN_OUT)/*
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
	env -u VSPHERE_SERVER -u VSPHERE_DATACENTER -u VSPHERE_PASSWORD -u VSPHERE_USER -u VSPHERE_STORAGE_POLICY_NAME -u KUBECONFIG -u WCP_ENDPOINT -u WCP_PORT -u WCP_NAMESPACE -u TOKEN -u CERTIFICATE go test $(TEST_FLAGS) $(PKGS_WITH_TESTS)
build-unit-tests:
	$(foreach pkg,$(PKGS_WITH_TESTS),go test $(TEST_FLAGS) -c $(pkg); )

INTEGRATION_TEST_PKGS ?=
.PHONY: integration-unit-test
integration-unit-test:
ifndef TYPE
	$(error Requires TYPE from a deployed testbed to run integration-unit-test)
else
    ifeq ($(TYPE), guestcluster)
        ifndef WCP_ENDPOINT
            $(error Requires WCP_ENDPOINT from a deployed testbed to run integration-unit-test)
        endif
        ifndef WCP_NAMESPACE
            $(error Requires WCP_NAMESPACE from a deployed testbed to run integration-unit-test)
        endif
        ifndef SUPERVISOR_STORAGE_CLASS
            $(error Requires SUPERVISOR_STORAGE_CLASS from a deployed testbed to run integration-unit-test)
        endif
        ifndef TOKEN
            $(error Requires TOKEN from a deployed testbed to run integration-unit-test)
        endif
        ifndef CERTIFICATE
            $(error Requires CERTIFICATE from a deployed testbed to run integration-unit-test)
        else
	        $(eval INTEGRATION_TEST_PKGS += ./pkg/csi/service/wcpguest)
        endif
    else
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
        ifeq ($(TYPE), supervisorcluster)
	        $(eval INTEGRATION_TEST_PKGS += ./pkg/csi/service/wcp ./pkg/syncer)
        else
            ifndef VSPHERE_K8S_NODE
                $(error Requires VSPHERE_K8S_NODE from a deployed testbed to run integration-unit-test)
            endif
            ifndef KUBECONFIG
                $(error Requires KUBECONFIG from a deployed testbed to run integration-unit-test)
            else
		$(eval INTEGRATION_TEST_PKGS += ./pkg/csi/service/vanilla ./pkg/syncer)
            endif
        endif
    endif
endif
	go test $(TEST_FLAGS) -tags=integration-unit $(INTEGRATION_TEST_PKGS)

# The default test target.
.PHONY: test build-tests
test: unit
build-tests: build-unit-tests

.PHONY: cover
cover: TEST_FLAGS += -cover
cover: test

# The default test target.
.PHONY: test build-tests
test: unit
build-tests: build-unit-tests

.PHONY: cover
cover: TEST_FLAGS += -cover
cover: test

.PHONY: test-e2e
test-e2e:
	hack/run-e2e-test.sh
################################################################################
##                                 LINTING                                    ##
################################################################################
.PHONY: check fmt lint mdlint shellcheck vet
check: fmt lint mdlint shellcheck staticcheck vet

fmt:
	hack/check-format.sh

lint:
	hack/check-lint.sh

mdlint:
	hack/check-mdlint.sh

golangci-lint:
	docker run --rm -v $(PWD):/app -w /app golangci/golangci-lint:v1.27.0 golangci-lint run -v --timeout=300s

shellcheck:
	hack/check-shell.sh

staticcheck:
	hack/check-staticcheck.sh

vet:
	hack/check-vet.sh

################################################################################
##                                 BUILD IMAGES                               ##
################################################################################
.PHONY: images
images: | $(DOCKER_SOCK)
	hack/release.sh

################################################################################
##                                  PUSH IMAGES                               ##
################################################################################
.PHONY: push-images upload-images
push-images: | $(DOCKER_SOCK)
	hack/release.sh -p

################################################################################
##                                  CI IMAGE                                  ##
################################################################################
build-ci-image:
	$(MAKE) -C images/ci build

push-ci-image:
	$(MAKE) -C images/ci push

print-ci-image:
	@$(MAKE) --no-print-directory -C images/ci print
