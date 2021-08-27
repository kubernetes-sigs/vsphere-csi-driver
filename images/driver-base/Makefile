# Copyright 2019 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

all: build

include ../../hack/make/login-to-image-registry.mk

IMAGE_NAME ?= $(REGISTRY)/extra/csi-driver-base
IMAGE_NAME_WITH_TAG ?= $(IMAGE_NAME):$(shell git log -1 --format=%h)

build:
	docker build -t $(IMAGE_NAME_WITH_TAG) --build-arg GIT_COMMIT=$(shell git log -1 --format=%H) .
	docker tag $(IMAGE_NAME_WITH_TAG) $(IMAGE_NAME):latest
.PHONY: build

push: login-to-image-registry
	docker push $(IMAGE_NAME_WITH_TAG)
	docker push $(IMAGE_NAME):latest
.PHONY: push

.PHONY: clean
DOCKER_RMI_FLAGS := --no-prune
clean:
	docker rmi $(DOCKER_RMI_FLAGS) $(IMAGE_NAME_WITH_TAG) $(IMAGE_NAME):latest 2>/dev/null || true

.PHONY: clobber
clobber: DOCKER_RMI_FLAGS :=
clobber: clean
	docker rmi $$(docker images -qf reference=$(IMAGE_NAME):*) 2>/dev/null || true

.PHONY: print
print:
	@echo $(IMAGE_NAME_WITH_TAG)