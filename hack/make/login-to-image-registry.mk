REGISTRY ?= gcr.io/cloud-provider-vsphere

.PHONY: login-to-image-registry
login-to-image-registry:
# Push images to a gcr.io registry.
ifneq (,$(findstring gcr.io,$(REGISTRY))) # begin gcr.io
# Log into the registry with a gcr.io JSON key file.
ifneq (,$(strip $(GCR_KEY_FILE))) # begin gcr.io-key
	@echo "logging into gcr.io registry with key file"
	docker login -u _json_key --password-stdin https://gcr.io <"$(GCR_KEY_FILE)"
# Log into the registry with a Docker gcloud auth helper.
else # end gcr.io-key / begin gcr.io-gcloud
	@command -v gcloud >/dev/null 2>&1 || \
	  { echo 'gcloud auth helper unavailable' 1>&2; exit 1; }
	@grep -F 'gcr.io": "gcloud"' "$(HOME)/.docker/config.json" >/dev/null 2>&1 || \
	  { gcloud auth configure-docker --quiet || \
	    { echo 'gcloud helper registration failed' 1>&2; exit 1; }; }
	@echo "logging into gcr.io registry with gcloud auth helper"
endif # end gcr.io-gcloud / end gcr.io
# Push images to a Docker registry.
else # begin docker
# Log into the registry with a Docker username and password.
ifneq (,$(strip $(DOCKER_USERNAME))) # begin docker-username
ifneq (,$(strip $(DOCKER_PASSWORD))) # begin docker-password
	@echo "logging into docker registry with username and password"
	docker login -u="$(DOCKER_USERNAME)" -p="$(DOCKER_PASSWORD)"
endif # end docker-password
endif # end docker-username
endif # end docker