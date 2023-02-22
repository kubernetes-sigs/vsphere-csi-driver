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
ARG GOLANG_IMAGE=golang:1.19

# This build arg allows the specification of a custom base image.
ARG BASE_IMAGE=gcr.io/cloud-provider-vsphere/extra/csi-driver-base:latest

################################################################################
##                              BUILD STAGE                                   ##
################################################################################
FROM ${GOLANG_IMAGE} as builder

# This build arg is the version to embed in the CSI binary
ARG VERSION=unknown

ARG GOPROXY

WORKDIR /build

COPY go.mod go.sum ./

COPY pkg/    pkg/

COPY cmd/    cmd/

ENV CGO_ENABLED=0

ENV GOPROXY ${GOPROXY:-https://proxy.golang.org}

RUN go build -a -ldflags="-w -s -extldflags=static -X sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer.Version=${VERSION}" -o vsphere-syncer ./cmd/syncer

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${BASE_IMAGE}

# This build arg is the git commit to embed in the CSI binary
ARG GIT_COMMIT

# This label will be overridden from driver base image
LABEL git_commit=$GIT_COMMIT

COPY --from=builder /build/vsphere-syncer /bin/vsphere-syncer

ENTRYPOINT ["/bin/vsphere-syncer"]
