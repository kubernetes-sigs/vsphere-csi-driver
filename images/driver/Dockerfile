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
# Build the manager as a statically compiled binary so it has no dependencies
# libc, muscl, etc.
FROM ${GOLANG_IMAGE} as builder

# This build arg is the version to embed in the CSI binary
ARG VERSION=unknown

# This build arg controls the GOPROXY setting
ARG GOPROXY

WORKDIR /build
COPY go.mod go.sum ./
COPY pkg/    pkg/
COPY cmd/    cmd/
ENV CGO_ENABLED=0
ENV GOPROXY ${GOPROXY:-https://proxy.golang.org}
RUN go build -a -ldflags="-w -s -extldflags=static -X sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service.Version=${VERSION}" -o vsphere-csi ./cmd/vsphere-csi

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${BASE_IMAGE}

# This build arg is the git commit to embed in the CSI binary
ARG GIT_COMMIT

# This label will be overridden from driver base image
LABEL git_commit=$GIT_COMMIT
LABEL "maintainers"="Divyen Patel <divyenp@vmware.com>, Sandeep Pissay Srinivasa Rao <ssrinivas@vmware.com>, Xing Yang <yangxi@vmware.com>"

# install nfs-utils, util-linux and e2fsprogs
# nfs-utils  : The nfs-utils package contains simple nfs client service.
# util-linux : Utilities for handling file systems, consoles, partitions.
# e2fsprogs  : The E2fsprogs package contains the utilities for handling the ext file system.
# xfsprogs   : The xfsprogs package contains administration and debugging tools for the XFS file system

RUN tdnf -y install \
  nfs-utils \
  util-linux \
  e2fsprogs \
  xfsprogs


# Remove cached data
RUN tdnf clean all

COPY --from=builder /build/vsphere-csi /bin/vsphere-csi

ENTRYPOINT ["/bin/vsphere-csi"]
