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

#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -x

DO_WINDOWS_BUILD=${DO_WINDOWS_BUILD_ENV:-true}
readonly BASE_IMAGE_REPO=us-central1-docker.pkg.dev/k8s-staging-images/csi-vsphere
readonly CSI_IMAGE_CI=${BASE_IMAGE_REPO}/driver
readonly SYNCER_IMAGE_CI=${BASE_IMAGE_REPO}/syncer

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

GOLANG_IMAGE=${CUSTOM_REPO_FOR_GOLANG:-}golang:1.24

ARCHES=("amd64" "arm64")
OSVERSION_WIN=(1809 20H2 ltsc2022)
WINDOWS_IMAGE_OUTPUT="type=tar,dest=.build/windows-driver.tar"
LINUX_IMAGE_OUTPUT="type=docker"

REGISTRY=
BASE_IMAGE="${BASE_IMAGE:=photon:5.0}"
export DOCKER_CLI_EXPERIMENTAL=enabled

function lcase () {
    tr '[:upper:]' '[:lower:]' <<< "${*}"
}

cd "$(dirname "${BASH_SOURCE[0]}")/.."

function build_driver_images_windows() {
  docker buildx use vsphere-csi-builder-win || docker buildx create --driver-opt image=moby/buildkit:v0.10.6 --name vsphere-csi-builder-win --platform windows/amd64 --use
  echo "building ${CSI_IMAGE_NAME}:${VERSION} for windows"
  osv=$(lcase "${OSVERSION}")
  tag="${CSI_IMAGE_NAME}-windows-${osv}-amd64:${VERSION}"
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
  for ARCH in "${ARCHES[@]}"; do
    echo "building ${CSI_IMAGE_NAME}:${VERSION} for linux/$ARCH"
    tag="${CSI_IMAGE_NAME}-linux-${ARCH}:${VERSION}"
    docker buildx build \
     --platform "linux/$ARCH" \
     --output "${LINUX_IMAGE_OUTPUT}" \
     --file images/driver/Dockerfile \
     --tag "${tag}" \
     --build-arg ARCH="${ARCH}" \
     --build-arg "VERSION=${VERSION}" \
     --build-arg "GOPROXY=${GOPROXY}" \
     --build-arg "GIT_COMMIT=${GIT_COMMIT}" \
     --build-arg "GOLANG_IMAGE=${GOLANG_IMAGE}" \
     --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
     .
  done
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
      --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
      .

  if [ "${LATEST}" ]; then
    docker tag "${SYNCER_IMAGE_NAME}":"${VERSION}" "${SYNCER_IMAGE_NAME}":latest
  fi
}

function build_images() {
  CSI_IMAGE_NAME=${CSI_IMAGE_CI}
  SYNCER_IMAGE_NAME=${SYNCER_IMAGE_CI}
  LATEST="latest"

  build_driver_images_linux
  build_syncer_image_linux

  if [ "$DO_WINDOWS_BUILD" = true ]; then
    build_driver_images_windows
  fi
}

function push_manifest_driver() {
  IMAGE_TAG="${CSI_IMAGE_NAME}":"${VERSION}"
  IMAGE_TAG_LATEST="${CSI_IMAGE_NAME}":latest

  linux_tags=()
  for ARCH in "${ARCHES[@]}"; do
    linux_tags+=( "${CSI_IMAGE_NAME}-linux-${ARCH}:${VERSION}" )
  done

  all_tags=("${linux_tags[@]}")
  for OSVERSION in "${OSVERSION_WIN[@]}"; do
    osv=$(lcase "${OSVERSION}")
    all_tags+=( "${CSI_IMAGE_NAME}-windows-${osv}-amd64:${VERSION}" )
  done

  docker manifest create --amend "${IMAGE_TAG}" "${all_tags[@]}"

  for OSVERSION in "${OSVERSION_WIN[@]}"; do
    osv=$(lcase "${OSVERSION}")
    BASEIMAGE=mcr.microsoft.com/windows/nanoserver:${OSVERSION}
    full_version=$(docker manifest inspect "${BASEIMAGE}" | grep '"os.version"' | sed 's/.*"os.version": "\(.*\)".*/\1/')
    docker manifest annotate --os windows --arch "amd64" --os-version "${full_version}" "${IMAGE_TAG}" "${CSI_IMAGE_NAME}-windows-${osv}-amd64:${VERSION}"
  done

  docker manifest push --purge "${IMAGE_TAG}"

  if [ "${LATEST}" ]; then
    docker manifest create --amend "${IMAGE_TAG_LATEST}" "${all_tags[@]}"
    for OSVERSION in "${OSVERSION_WIN[@]}"; do
      osv=$(lcase "${OSVERSION}")
      BASEIMAGE=mcr.microsoft.com/windows/nanoserver:${OSVERSION}
      full_version=$(docker manifest inspect "${BASEIMAGE}" | grep '"os.version"' | sed 's/.*"os.version": "\(.*\)".*/\1/')
      docker manifest annotate --os windows --arch "amd64" --os-version "${full_version}" "${IMAGE_TAG_LATEST}" "${CSI_IMAGE_NAME}-windows-${osv}-amd64:${VERSION}"
    done
    docker manifest push --purge "${IMAGE_TAG_LATEST}"
  fi
}

function login() {
  if [ "${GCR_KEY_FILE}" ]; then
    docker login -u _json_key --password-stdin https://gcr.io <"${GCR_KEY_FILE}" || exit 1
  fi
}

function push_syncer_images() {
  docker push "${SYNCER_IMAGE_NAME}":"${VERSION}"
  if [ "${LATEST}" ]; then
    docker push "${SYNCER_IMAGE_NAME}":latest
  fi
}

while getopts ":hk:lpr:" opt; do
  case ${opt} in
    h) exit 0 ;;
    k) GCR_KEY_FILE="${OPTARG}" ;;
    l) LATEST=1 ;;
    p) PUSH=1 ;;
    r) REGISTRY="${OPTARG}" ;;
    *) exit 1 ;;
  esac
done
shift $((OPTIND-1))

[ "${GCR_KEY_FILE}" ] && [ ! -e "${GCR_KEY_FILE}" ] && exit 1

case "${BUILD_RELEASE_TYPE}" in
  ci|pr|release) ;; 
  *) exit 1 ;;
esac

mkdir -p .build
docker ps >/dev/null 2>&1 || exit 1

build_images

if [ "${PUSH}" ]; then
  login
  [ "${REGISTRY}" ] && CSI_IMAGE_NAME="${REGISTRY}driver"
  if [ "$DO_WINDOWS_BUILD" = true ]; then
    WINDOWS_IMAGE_OUTPUT="type=registry"
    for osversion in "${OSVERSION_WIN[@]}"; do
      OSVERSION="$osversion"
      build_driver_images_windows
    done
  fi
  LINUX_IMAGE_OUTPUT="type=registry"
  build_driver_images_linux
  if [ "$DO_WINDOWS_BUILD" = true ]; then
    push_manifest_driver
  fi
  push_syncer_images
fi
