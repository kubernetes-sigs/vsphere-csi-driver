#!/bin/bash

# Copyright 2025 The Kubernetes Authors.
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

# This script builds coverage-enabled binaries locally and creates container images
# This approach is much faster than building inside Docker

set -o errexit
set -o nounset
set -o pipefail
set -x

# BASE_REPO is the root path of the image repository
readonly BASE_IMAGE_REPO=us-central1-docker.pkg.dev/k8s-staging-images/csi-vsphere

# Coverage images
readonly CSI_COVER_IMAGE_CI=${BASE_IMAGE_REPO}/driver-cover
readonly SYNCER_COVER_IMAGE_CI=${BASE_IMAGE_REPO}/syncer-cover

PUSH=
LATEST=
CSI_COVER_IMAGE_NAME=
SYNCER_COVER_IMAGE_NAME=
if [[ "$(git rev-parse --abbrev-ref HEAD)" =~ "master" ]]; then
  VERSION="$(git log -1 --format=%h)"
else
  VERSION="$(git describe --always 2>/dev/null)"
fi
GIT_COMMIT="$(git log -1 --format=%H)"
GCR_KEY_FILE="${GCR_KEY_FILE:-}"

ARCH=amd64
REGISTRY=

# set base image if not given already
BASE_IMAGE="${BASE_IMAGE:=photon:5.0}"

USAGE="
usage: ${0} [FLAGS]
  Builds and optionally pushes coverage-enabled images for vSphere CSI driver

  This script builds coverage-enabled binaries locally (fast!) and then creates
  minimal Docker images. This approach is much faster than multi-stage Docker builds.

  Prerequisites:
    - Go 1.25 or later
    - Docker (running)
    - Git
    - Network access to download Go dependencies

  Honored environment variables:
    GCR_KEY_FILE  - Path to GCR service account key for authentication
    BASE_IMAGE    - Base image for containers (default: photon:5.0)

FLAGS
  -h    show this help and exit
  -k    path to GCR key file. Used to login to registry if specified
        (defaults to: ${GCR_KEY_FILE})
  -l    tag the images as \"latest\" in addition to their version
        when used with -p, both tags will be pushed
  -p    push the images to the container registry
  -r    push the image to custom registry, specify the registry to be used
        Example: -r myregistry.com/myproject/

Examples:
  # Build coverage images locally
  ${0}

  # Build and push to custom registry
  ${0} -p -l -r myregistry.com/myproject/
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


function build_coverage_binaries() {
  echo "Building coverage-enabled binaries locally..."
  
  # Download Go dependencies if needed
  echo "Ensuring Go dependencies are available..."
  go mod download || fatal "Failed to download Go dependencies"
  
  # Build driver coverage binary
  echo "Building vsphere-csi-cover binary..."
  rm -f ./bin/vsphere-csi-cover
  if ! CGO_ENABLED=0 GOFIPS=1 GOEXPERIMENT=boringcrypto GOOS=linux GOARCH=amd64 \
    go build -cover -coverpkg=./... -o ./bin/vsphere-csi-cover ./cmd/vsphere-csi; then
    fatal "Failed to build vsphere-csi-cover binary"
  fi
  
  # Verify binary was created
  if [ ! -f "./bin/vsphere-csi-cover" ]; then
    fatal "vsphere-csi-cover binary was not created"
  fi
  echo "✓ vsphere-csi-cover binary built successfully"
  
  # Build syncer coverage binary
  echo "Building vsphere-syncer-cover binary..."
  rm -f ./bin/vsphere-syncer-cover
  if ! CGO_ENABLED=0 GOFIPS=1 GOEXPERIMENT=boringcrypto GOOS=linux GOARCH=amd64 \
    go build -cover -coverpkg=./... -o ./bin/vsphere-syncer-cover ./cmd/syncer; then
    fatal "Failed to build vsphere-syncer-cover binary"
  fi
  
  # Verify binary was created
  if [ ! -f "./bin/vsphere-syncer-cover" ]; then
    fatal "vsphere-syncer-cover binary was not created"
  fi
  echo "✓ vsphere-syncer-cover binary built successfully"
  
  echo "All coverage binaries built successfully!"
}

function build_driver_cover_image() {
  echo "building ${CSI_COVER_IMAGE_NAME}:${VERSION} for linux (coverage enabled)"
  
  # Use the Dockerfile that copies the pre-built binary
  docker build \
   --platform "linux/$ARCH" \
   -f images/driver/Dockerfile.cover \
   -t "${CSI_COVER_IMAGE_NAME}:${VERSION}" \
   --build-arg "GIT_COMMIT=${GIT_COMMIT}" \
   .
  
  if [ "${LATEST}" ]; then
    echo "tagging image ${CSI_COVER_IMAGE_NAME}:${VERSION} as latest"
    docker tag "${CSI_COVER_IMAGE_NAME}:${VERSION}" "${CSI_COVER_IMAGE_NAME}:latest"
  fi
}

function build_syncer_cover_image() {
  echo "building ${SYNCER_COVER_IMAGE_NAME}:${VERSION} for linux (coverage enabled)"
  
  # Use the Dockerfile that copies the pre-built binary
  docker build \
   --platform "linux/$ARCH" \
   -f images/syncer/Dockerfile.cover \
   -t "${SYNCER_COVER_IMAGE_NAME}:${VERSION}" \
   --build-arg "GIT_COMMIT=${GIT_COMMIT}" \
   .

  if [ "${LATEST}" ]; then
    echo "tagging image ${SYNCER_COVER_IMAGE_NAME}:${VERSION} as latest"
    docker tag "${SYNCER_COVER_IMAGE_NAME}:${VERSION}" "${SYNCER_COVER_IMAGE_NAME}:latest"
  fi
}

function build_cover_images() {
  CSI_COVER_IMAGE_NAME=${CSI_COVER_IMAGE_CI}
  SYNCER_COVER_IMAGE_NAME=${SYNCER_COVER_IMAGE_CI}
  LATEST="latest"

  # Build binaries locally first (fast!)
  build_coverage_binaries
  
  # Build minimal container images
  build_driver_cover_image
  build_syncer_cover_image
}

function login() {
  # If GCR_KEY_FILE is set, use that service account to login
  if [ "${GCR_KEY_FILE}" ]; then
    docker login -u _json_key --password-stdin https://gcr.io <"${GCR_KEY_FILE}" || fatal "unable to login"
  fi
}

function push_driver_cover_images() {
  [ "${CSI_COVER_IMAGE_NAME}" ] || fatal "CSI_COVER_IMAGE_NAME not set"

  echo "pushing ${CSI_COVER_IMAGE_NAME}:${VERSION}"
  docker push "${CSI_COVER_IMAGE_NAME}:${VERSION}"
  
  if [ "${LATEST}" ]; then
    echo "also pushing ${CSI_COVER_IMAGE_NAME}:latest"
    docker push "${CSI_COVER_IMAGE_NAME}:latest"
  fi
}

function push_syncer_cover_images() {
  [ "${SYNCER_COVER_IMAGE_NAME}" ] || fatal "SYNCER_COVER_IMAGE_NAME not set"

  echo "pushing ${SYNCER_COVER_IMAGE_NAME}:${VERSION}"
  docker push "${SYNCER_COVER_IMAGE_NAME}:${VERSION}"
  
  if [ "${LATEST}" ]; then
    echo "also pushing ${SYNCER_COVER_IMAGE_NAME}:latest"
    docker push "${SYNCER_COVER_IMAGE_NAME}:latest"
  fi
}

# Start of main script
while getopts ":hk:lpr:" opt; do
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

# Verify prerequisites
echo "Checking prerequisites..."

# Check if Go is installed
if ! command -v go &> /dev/null; then
  fatal "Go is not installed. Please install Go 1.25 or later."
fi

# Check Go version
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "Found Go version: ${GO_VERSION}"

# Check if git is available
if ! command -v git &> /dev/null; then
  fatal "Git is not installed. Please install git."
fi

# Check if Docker is available
if ! command -v docker &> /dev/null; then
  fatal "Docker is not installed. Please install Docker."
fi

# Check if Docker daemon is running
if ! docker ps >/dev/null 2>&1; then
  fatal "Docker daemon is not running. Please start Docker."
fi

# Create necessary directories
mkdir -p .build
mkdir -p ./bin

# Verify we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  fatal "Not in a git repository. Please clone the repository properly."
fi

# Verify go.mod exists
if [ ! -f "go.mod" ]; then
  fatal "go.mod not found. Please run this script from the repository root."
fi

# Verify Dockerfiles exist
if [ ! -f "images/driver/Dockerfile.cover" ]; then
  fatal "images/driver/Dockerfile.cover not found. Repository may be incomplete."
fi

if [ ! -f "images/syncer/Dockerfile.cover" ]; then
  fatal "images/syncer/Dockerfile.cover not found. Repository may be incomplete."
fi

echo "All prerequisites satisfied!"

# build coverage-enabled container images
build_cover_images

# Optionally push artifacts
if [ "${PUSH}" ]; then
  login
  
  # if registry is provided, retag images with custom registry
  if [ "${REGISTRY}" ]; then
    # Tag with custom registry names
    CUSTOM_DRIVER_IMAGE="${REGISTRY}vsphere-csi-driver-cover"
    CUSTOM_SYNCER_IMAGE="${REGISTRY}vsphere-syncer-cover"
    
    docker tag "${CSI_COVER_IMAGE_NAME}:${VERSION}" "${CUSTOM_DRIVER_IMAGE}:${VERSION}"
    docker tag "${SYNCER_COVER_IMAGE_NAME}:${VERSION}" "${CUSTOM_SYNCER_IMAGE}:${VERSION}"
    
    if [ "${LATEST}" ]; then
      docker tag "${CSI_COVER_IMAGE_NAME}:${VERSION}" "${CUSTOM_DRIVER_IMAGE}:latest"
      docker tag "${SYNCER_COVER_IMAGE_NAME}:${VERSION}" "${CUSTOM_SYNCER_IMAGE}:latest"
    fi
    
    # Update names for push
    CSI_COVER_IMAGE_NAME="${CUSTOM_DRIVER_IMAGE}"
    SYNCER_COVER_IMAGE_NAME="${CUSTOM_SYNCER_IMAGE}"
  fi
  
  # push coverage images
  push_driver_cover_images
  push_syncer_cover_images
fi
