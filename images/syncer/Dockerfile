# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################
##                               BUILD ARGS                                   ##
################################################################################
# This build arg allows the specification of a custom Golang image.
ARG GOLANG_IMAGE=golang:1.27.1

# This build arg allows the specification of a custom base image.
ARG BASE_IMAGE=photon:5.0

################################################################################
##                              BUILD STAGE                                   ##
################################################################################
FROM ${GOLANG_IMAGE} AS builder

# This build arg is the version to embed in the CSI binary
ARG VERSION=unknown
# Add Git commit and branch args with default "unknown"
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown

ARG GOPROXY

WORKDIR /build

COPY go.mod go.sum ./

COPY pkg/    pkg/

COPY cmd/    cmd/

ENV CGO_ENABLED=0

ENV GOPROXY=${GOPROXY:-https://proxy.golang.org}

ENV GOFIPS=1

ENV GOEXPERIMENT="boringcrypto"

# Inject Version, Commit, and Branch into the binary via ldflags
RUN go build -a -ldflags="-w -s -extldflags=static \
    -X sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service.Version=${VERSION} \
    -X sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service.Commit=${GIT_COMMIT} \
    -X sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service.Branch=${GIT_BRANCH}" \
    -o vsphere-syncer ./cmd/syncer

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${BASE_IMAGE}

# Re-declare build args for MAIN stage to receive passed values from hack/release.sh
ARG VERSION
ARG GIT_COMMIT
ARG GIT_BRANCH

# These labels will be overridden from driver base image
LABEL version=$VERSION
LABEL git_commit=$GIT_COMMIT
LABEL git_branch=$GIT_BRANCH

RUN tdnf -y upgrade

COPY --from=builder /build/vsphere-syncer /bin/vsphere-syncer

# Remove cached data
RUN tdnf clean all

ENTRYPOINT ["/bin/vsphere-syncer"]