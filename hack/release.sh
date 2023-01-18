#!/bin/bash

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

# This script is used build new container images of the CAPV manager and
# clusterctl. When invoked without arguments, the default behavior is to build
# new ci images

set -o errexit
set -o nounset
set -o pipefail
set -x

DO_WINDOWS_BUILD=${DO_WINDOWS_BUILD_ENV:-true}

# BASE_REPO is the root path of the image repository
readonly BASE_IMAGE_REPO=gcr.io/cloud-provider-vsphere

# Release images
readonly CSI_IMAGE_RELEASE=${BASE_IMAGE_REPO}/csi/release/driver
readonly SYNCER_IMAGE_RELEASE=${BASE_IMAGE_REPO}/csi/release/syncer

# PR images
readonly CSI_IMAGE_PR=${BASE_IMAGE_REPO}/csi/pr/driver
readonly SYNCER_IMAGE_PR=${BASE_IMAGE_REPO}/csi/pr/syncer

# CI images
readonly CSI_IMAGE_CI=${BASE_IMAGE_REPO}/csi/ci/driver
readonly SYNCER_IMAGE_CI=${BASE_IMAGE_REPO}/csi/ci/syncer

PUSH=
LATEST=
CSI_IMAGE_NAME=
SYNCER_IMAGE_NAME=
if [[ "$(git rev-parse --abbrev-ref HEAD)" =~ "master" ]]; then
  VERSION="$(git log -1 --format=%h)"
else
  VERSION="$(git describe --always 2>/dev/null)"
fi
GIT_COMMIT="$(git log -1 --format=%H)"
GCR_KEY_FILE="${GCR_KEY_FILE:-}"
GOPROXY="${GOPROXY:-https://proxy.golang.org}"
BUILD_RELEASE_TYPE="${BUILD_RELEASE_TYPE:-}"

# CUSTOM_REPO_FOR_GOLANG can be used to pass custom repository for golang builder image.
# Please ensure it ends with a '/'.
# Example: CUSTOM_REPO_FOR_GOLANG=harbor-repo.vmware.com/dockerhub-proxy-cache/library/
GOLANG_IMAGE=${CUSTOM_REPO_FOR_GOLANG:-}golang:1.19

ARCH=amd64
OSVERSION=1809
# OS Version for the Windows images: 1809, 20H2, ltsc2022
OSVERSION_WIN=(1809 20H2 ltsc2022)
# The output type could either be docker (local), or registry.
# If it is registry, it will also allow us to push the Windows images.
WINDOWS_IMAGE_OUTPUT="type=tar,dest=.build/windows-driver.tar"
LINUX_IMAGE_OUTPUT="type=docker"

REGISTRY=

# The manifest command is still experimental as of Docker 18.09.3
export DOCKER_CLI_EXPERIMENTAL=enabled

# If BUILD_RELEASE_TYPE is not set then check to see if this is a PR
# or release build. This may still be overridden below with the "-t" flag.
if [ -z "${BUILD_RELEASE_TYPE}" ]; then
  if hack/match-release-tag.sh >/dev/null 2>&1; then
    BUILD_RELEASE_TYPE=release
  else
    BUILD_RELEASE_TYPE=ci
  fi
fi

USAGE="
usage: ${0} [FLAGS]
  Builds and optionally pushes new images for vSphere CSI driver

  Honored environment variables:
  GCR_KEY_FILE
  GOPROXY
  BUILD_RELEASE_TYPE

FLAGS
  -h    show this help and exit
  -k    path to GCR key file. Used to login to registry if specified
        (defaults to: ${GCR_KEY_FILE})
  -l    tag the images as \"latest\" in addition to their version
        when used with -p, both tags will be pushed
  -p    push the images to the public container registry
  -t    the build/release type (defaults to: ${BUILD_RELEASE_TYPE})
        one of [ci,pr,release]
  -r    push the image to custom registry, specify the registry to be used
"


function lcase () {
    tr '[:upper:]' '[:lower:]' <<< "${*}"
}


# Change directories to the parent directory of the one in which this
# script is located.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

function error() {
  local exit_code="${?}"
  echo "${@}" 1>&2
  return "${exit_code}"
}

function fatal() {
  error "${@}" || exit 1
}


function build_driver_images_windows() {
  docker buildx use vsphere-csi-builder-win || docker buildx create --driver-opt image=moby/buildkit:v0.10.6 --name vsphere-csi-builder-win --platform windows/amd64 --use
  echo "building ${CSI_IMAGE_NAME}:${VERSION} for windows"
  # some registry do not allow uppercase tags
  osv=$(lcase ${OSVERSION})
  tag="${CSI_IMAGE_NAME}-windows-${osv}-${ARCH}:${VERSION}"
  docker buildx build \
   --platform "windows" \
   --output "${WINDOWS_IMAGE_OUTPUT}" \
   --file images/windows/driver/Dockerfile \
   --tag "${tag}" \
   --build-arg "VERSION=${VERSION}" \
   --build-arg "OSVERSION=${OSVERSION}" \
   --build-arg "GOPROXY=${GOPROXY}" \
   --build-arg "GIT_COMMIT=${GIT_COMMIT}" \
   --build-arg "GOLANG_IMAGE=${GOLANG_IMAGE}" \
   .
   docker buildx rm vsphere-csi-builder-win || echo "builder instance not found, safe to proceed"
}

function build_driver_images_linux() {
  echo "building ${CSI_IMAGE_NAME}:${VERSION} for linux"
  docker buildx rm vsphere-csi-builder-win || echo "builder instance not found, safe to proceed"
  tag="${CSI_IMAGE_NAME}-linux-${ARCH}:${VERSION}"
  docker buildx build \
   --platform "linux/$ARCH" \
   --output "${LINUX_IMAGE_OUTPUT}" \
   --file images/driver/Dockerfile \
   --tag "${tag}" \
   --build-arg ARCH=amd64 \
   --build-arg "VERSION=${VERSION}" \
   --build-arg "GOPROXY=${GOPROXY}" \
   --build-arg "GIT_COMMIT=${GIT_COMMIT}" \
   --build-arg "GOLANG_IMAGE=${GOLANG_IMAGE}" \
   .
}

function build_syncer_image_linux() {
  echo "building ${SYNCER_IMAGE_NAME}:${VERSION} for linux"
  docker build \
      -f images/syncer/Dockerfile \
      -t "${SYNCER_IMAGE_NAME}":"${VERSION}" \
      --build-arg "VERSION=${VERSION}" \
      --build-arg "GOPROXY=${GOPROXY}" \
      --build-arg "GIT_COMMIT=${GIT_COMMIT}" \
      --build-arg "GOLANG_IMAGE=${GOLANG_IMAGE}" \
  .

  if [ "${LATEST}" ]; then
    echo "tagging image ${SYNCER_IMAGE_NAME}:${VERSION} as latest"
    docker tag "${SYNCER_IMAGE_NAME}":"${VERSION}" "${SYNCER_IMAGE_NAME}":latest
  fi
}

function build_images() {
  case "${BUILD_RELEASE_TYPE}" in
    ci)
      # A non-PR, non-release build. This is usually a build off of master
      CSI_IMAGE_NAME=${CSI_IMAGE_CI}
      SYNCER_IMAGE_NAME=${SYNCER_IMAGE_CI}
      LATEST="latest"
      ;;
    pr)
      # A PR build
      CSI_IMAGE_NAME=${CSI_IMAGE_PR}
      SYNCER_IMAGE_NAME=${SYNCER_IMAGE_PR}
      ;;
    release)
      # On an annotated tag
      CSI_IMAGE_NAME=${CSI_IMAGE_RELEASE}
      SYNCER_IMAGE_NAME=${SYNCER_IMAGE_RELEASE}
      ;;
  esac

  # build images for linux platform
  build_driver_images_linux
  build_syncer_image_linux

  if [ "$DO_WINDOWS_BUILD" = true ]; then
    # build images for windows platform
    build_driver_images_windows
  fi
}

function push_manifest_driver() {
  IMAGE_TAG="${CSI_IMAGE_NAME}":"${VERSION}"
  IMAGE_TAG_LATEST="${CSI_IMAGE_NAME}":latest

  echo "creating manifest ${IMAGE_TAG}"
  linux_tags="${CSI_IMAGE_NAME}-linux-${ARCH}:${VERSION}"
  for OSVERSION in "${OSVERSION_WIN[@]}"
  do 
    osv=$(lcase "${OSVERSION}")
    all_tags+=( "${CSI_IMAGE_NAME}-windows-${osv}-${ARCH}:${VERSION}" )
  done
  all_tags+=( "${linux_tags}" )
  docker manifest create --amend "${IMAGE_TAG}" "${all_tags[@]}"

  # add "os.version" field to windows images (based on https://github.com/kubernetes/kubernetes/blob/master/build/pause/Makefile)
  echo "adding os.version to manifest"
  for OSVERSION in "${OSVERSION_WIN[@]}"
  do 
    osv=$(lcase "${OSVERSION}")
    BASEIMAGE=mcr.microsoft.com/windows/nanoserver:${OSVERSION}; 
    full_version=$(docker manifest inspect "${BASEIMAGE}" | jq -r '.manifests[0].platform["os.version"]'); 
    echo "fullversion for ${BASEIMAGE} : ${full_version}"
    echo "annotating ${IMAGE_TAG} for ${OSVERSION}"
    docker manifest annotate --os windows --arch "$ARCH" --os-version "${full_version}" "${IMAGE_TAG}" "${CSI_IMAGE_NAME}-windows-${osv}-${ARCH}:${VERSION}";
  done
  echo "pushing manifest for tag ${IMAGE_TAG}"
  docker manifest push --purge "${IMAGE_TAG}"
  docker manifest inspect "${IMAGE_TAG}"
  if [ "${LATEST}" ]; then
    echo "creating manifest for tag ${IMAGE_TAG_LATEST}"
    docker manifest create --amend "${IMAGE_TAG_LATEST}" "${all_tags[@]}"
    echo "adding os.version to manifest"
    
    for OSVERSION in "${OSVERSION_WIN[@]}"
    do 
      osv=$(lcase "${OSVERSION}")
      BASEIMAGE=mcr.microsoft.com/windows/nanoserver:${OSVERSION}; 
      full_version=$(docker manifest inspect "${BASEIMAGE}" | jq -r '.manifests[0].platform["os.version"]'); 
      echo "fullversion for ${BASEIMAGE} : ${full_version}"      
      echo "annotating ${IMAGE_TAG_LATEST} for ${OSVERSION}"
      docker manifest annotate --os windows --arch "$ARCH" --os-version "${full_version}" "${IMAGE_TAG_LATEST}" "${CSI_IMAGE_NAME}-windows-${osv}-${ARCH}:${VERSION}";
    done
    echo "pushing manifest for tag ${IMAGE_TAG_LATEST}"
    docker manifest push --purge "${IMAGE_TAG_LATEST}"
    docker manifest inspect "${IMAGE_TAG_LATEST}"
  fi
}

function login() {
  # If GCR_KEY_FILE is set, use that service account to login
  if [ "${GCR_KEY_FILE}" ]; then
    docker login -u _json_key --password-stdin https://gcr.io <"${GCR_KEY_FILE}" || fatal "unable to login"
  fi
}

function push_syncer_images() {
  [ "${SYNCER_IMAGE_NAME}" ] || fatal "SYNCER_IMAGE_NAME not set"

  echo "pushing ${SYNCER_IMAGE_NAME}:${VERSION}"
  if [ "${REGISTRY}" ]
  then
    TAG="${REGISTRY}"syncer:"${VERSION}"
    TAG_LATEST="${REGISTRY}"syncer:latest
    docker tag "${SYNCER_IMAGE_NAME}":"${VERSION}" "${TAG}"
    docker push "${TAG}"
    if [ "${LATEST}" ]; then
      docker tag "${SYNCER_IMAGE_NAME}":"${VERSION}" "${TAG_LATEST}"
      echo "also pushing ${TAG_LATEST} as latest"
      docker push "${TAG_LATEST}"
    fi
  else
    docker push "${SYNCER_IMAGE_NAME}":"${VERSION}"
    if [ "${LATEST}" ]; then
      echo "also pushing ${SYNCER_IMAGE_NAME}:${VERSION} as latest"
      docker push "${SYNCER_IMAGE_NAME}":latest
    fi
  fi
}

# Start of main script
while getopts ":hk:lptr:" opt; do
  case ${opt} in
    h)
      error "${USAGE}" && exit 1
      ;;
    k)
      GCR_KEY_FILE="${OPTARG}"
      ;;
    l)
      LATEST=1
      ;;
    p)
      PUSH=1
      ;;
    t)
      BUILD_RELEASE_TYPE="${OPTARG}"
      ;;
    r)
      REGISTRY="${OPTARG}"
      ;;
    \?)
      error "invalid option: -${OPTARG} ${USAGE}" && exit 1
      ;;
    :)
      error "option -${OPTARG} requires an argument" && exit 1
      ;;
  esac
done
shift $((OPTIND-1))

# Verify the GCR_KEY_FILE exists if defined
if [ "${GCR_KEY_FILE}" ]; then
  [ -e "${GCR_KEY_FILE}" ] || fatal "key file ${GCR_KEY_FILE} does not exist"
fi

# Validate build/release type.
case "${BUILD_RELEASE_TYPE}" in
  ci|pr|release)
    # do nothing
    ;;
  *)
    fatal "invalid BUILD_RELEASE_TYPE: ${BUILD_RELEASE_TYPE}"
    ;;
esac

mkdir -p .build

# make sure that Docker is available
docker ps >/dev/null 2>&1 || fatal "Docker not available"

# build container images, this will build linux images for backward compatibility
build_images

# Optionally push artifacts
if [ "${PUSH}" ]; then
  login
  # if registry is provided take that name
  if [ "${REGISTRY}" ]; then
    CSI_IMAGE_NAME="${REGISTRY}driver"
  fi

  if [ "$DO_WINDOWS_BUILD" = true ]; then
    # build windows images and push them to registry as currently windows images are build only when push is enabled
    WINDOWS_IMAGE_OUTPUT="type=registry"
    for osversion in "${OSVERSION_WIN[@]}"
    do
      OSVERSION="$osversion"
      build_driver_images_windows
    done
  fi
  # tag linux images with linux and push them to registry
  LINUX_IMAGE_OUTPUT="type=registry"
  build_driver_images_linux
  if [ "$DO_WINDOWS_BUILD" = true ]; then
    #create and push manifest for driver
    push_manifest_driver
  fi
  #push syncer images
  push_syncer_images
fi

