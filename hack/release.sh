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
VERSION=$(git describe --dirty --always 2>/dev/null)
GCR_KEY_FILE="${GCR_KEY_FILE:-}"
GOPROXY="${GOPROXY:-https://proxy.golang.org}"
BUILD_RELEASE_TYPE="${BUILD_RELEASE_TYPE:-}"

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
"

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

function build_images() {
  case "${BUILD_RELEASE_TYPE}" in
    ci)
      # A non-PR, non-release build. This is usually a build off of master
      CSI_IMAGE_NAME=${CSI_IMAGE_CI}
      SYNCER_IMAGE_NAME=${SYNCER_IMAGE_CI}
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

  echo "building ${CSI_IMAGE_NAME}:${VERSION}"
  echo "GOPROXY=${GOPROXY}"
  docker build \
    -f images/driver/Dockerfile \
    -t "${CSI_IMAGE_NAME}":"${VERSION}" \
    --build-arg "VERSION=${VERSION}" \
    --build-arg "GOPROXY=${GOPROXY}" \
    .

  echo "building ${SYNCER_IMAGE_NAME}:${VERSION}"
  docker build \
      -f images/syncer/Dockerfile \
      -t "${SYNCER_IMAGE_NAME}":"${VERSION}" \
      --build-arg "VERSION=${VERSION}" \
      --build-arg "GOPROXY=${GOPROXY}" \
      .
  if [ "${LATEST}" ]; then
    echo "tagging image ${CSI_IMAGE_NAME}:${VERSION} as latest"
    docker tag "${CSI_IMAGE_NAME}":"${VERSION}" "${CSI_IMAGE_NAME}":latest
    echo "tagging image ${SYNCER_IMAGE_NAME}:${VERSION} as latest"
    docker tag "${SYNCER_IMAGE_NAME}":"${VERSION}" "${SYNCER_IMAGE_NAME}":latest

  fi
}

function login() {
  # If GCR_KEY_FILE is set, use that service account to login
  if [ "${GCR_KEY_FILE}" ]; then
    docker login -u _json_key --password-stdin https://gcr.io <"${GCR_KEY_FILE}" || fatal "unable to login"
  fi
}

function push_images() {
  [ "${CSI_IMAGE_NAME}" ] || fatal "CSI_IMAGE_NAME not set"
  [ "${SYNCER_IMAGE_NAME}" ] || fatal "SYNCER_IMAGE_NAME not set"

  login

  echo "pushing ${CSI_IMAGE_NAME}:${VERSION}"
  docker push "${CSI_IMAGE_NAME}":"${VERSION}"
  echo "pushing ${SYNCER_IMAGE_NAME}:${VERSION}"
  docker push "${SYNCER_IMAGE_NAME}":"${VERSION}"

  if [ "${LATEST}" ]; then
    echo "also pushing ${CSI_IMAGE_NAME}:${VERSION} as latest"
    docker push "${CSI_IMAGE_NAME}":latest
    echo "also pushing ${SYNCER_IMAGE_NAME}:${VERSION} as latest"
    docker push "${SYNCER_IMAGE_NAME}":latest

  fi
}

# Start of main script
while getopts ":hk:lpt:" opt; do
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

# make sure that Docker is available
docker ps >/dev/null 2>&1 || fatal "Docker not available"

# build container images
build_images

# Optionally push artifacts
if [ "${PUSH}" ]; then
  push_images
fi
