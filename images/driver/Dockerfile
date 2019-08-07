################################################################################
##                               BUILD ARGS                                   ##
################################################################################
# This build arg allows the specification of a custom Golang image.
ARG GOLANG_IMAGE=golang:1.12.6

# This build arg allows the specification of a custom base image.
ARG BASE_IMAGE=gcr.io/cloud-provider-vsphere/extra/csi-driver-base:v0.2.1-10-gb2fb75e

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
ENV GOPROXY ${GOPROXY:-}
RUN go build -a -ldflags='-w -s -extldflags=static -X sigs.k8s.io/vsphere-csi-driver/pkg/csi/service.version=${VERSION}' -o vsphere-csi ./cmd/vsphere-csi

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${BASE_IMAGE}
LABEL "maintainer" "Travis Rhoden <trhoden@vmware.com>"

COPY --from=builder /build/vsphere-csi /bin/vsphere-csi

CMD ["/bin/vsphere-csi"]
