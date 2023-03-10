# Copyright 2021 The Kubernetes Authors.
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
ARG OSVERSION
ARG ARCH=amd64

################################################################################
##                              BUILD STAGE                                   ##
################################################################################
# Build the manager as a statically compiled binary so it has no dependencies
# libc, muscl, etc.
FROM --platform=linux/amd64 ${GOLANG_IMAGE} as builder

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
RUN GOOS=windows GOARCH=amd64 go build -a -ldflags="-w -s -extldflags=static -X sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service.Version=${VERSION}" -o ./bin/vsphere-csi.windows_amd64 cmd/vsphere-csi/main.go


################################################################################
##                              MAIN STAGE                                   ##
################################################################################
FROM --platform=linux/amd64 gcr.io/k8s-staging-e2e-test-images/windows-servercore-cache:1.0-linux-${ARCH}-${OSVERSION} as core

FROM mcr.microsoft.com/windows/nanoserver:${OSVERSION}
COPY --from=core /Windows/System32/netapi32.dll /Windows/System32/netapi32.dll

USER ContainerAdministrator
LABEL description="vSphere CSI Driver Windows Plugin"

COPY --from=builder /build/bin/vsphere-csi.windows_amd64 ./csi.exe
ENTRYPOINT ["/csi.exe"]
