all: build

include ../../../hack/make/login-to-image-registry.mk

VERSION ?= $(shell git describe --always --dirty)
IMAGE := $(REGISTRY)/csi-ci
IMAGE_D := $(VERSION).d

build: $(IMAGE_D)
$(IMAGE_D): Dockerfile
	docker build -t $(IMAGE):$(VERSION) -f $< ../../..
	docker tag $(IMAGE):$(VERSION) $(IMAGE):latest
	@touch $@

.PHONY: rebuild
rebuild: MAKEFLAGS += --always-make
rebuild:
	$(MAKE) build

.PHONY: push
push: $(IMAGE_D) login-to-image-registry
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest

.PHONY: ls-images
ls-images:
	docker images --filter=reference=$(IMAGE):*

.PHONY: clean
DOCKER_RMI_FLAGS := --no-prune
clean:
	rm -f $(IMAGE_D)
	docker rmi $(DOCKER_RMI_FLAGS) $(IMAGE):$(VERSION) $(IMAGE):latest 2>/dev/null || true

.PHONY: clobber
clobber: DOCKER_RMI_FLAGS :=
clobber: clean
	rm -f *.d
	docker rmi $$(docker images -qf reference=$(IMAGE):*) 2>/dev/null || true

.PHONY: print
print:
	@echo $(IMAGE):$(VERSION)
