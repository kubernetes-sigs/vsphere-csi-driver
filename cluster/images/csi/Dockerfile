################################################################################
##                               BUILD ARGS                                   ##
################################################################################
# This build arg allows the specification of a custom Golang image.
ARG GOLANG_IMAGE=golang:1.12.6

# This build arg allows the specification of a custom Photon image.
ARG PHOTON_IMAGE=photon:2.0

# This build arg is the version to embed in the CSI binary
ARG VERSION=unknown

################################################################################
##                              BUILD STAGE                                   ##
################################################################################
# Build the manager as a statically compiled binary so it has no dependencies
# libc, muscl, etc.
FROM ${GOLANG_IMAGE} as builder
WORKDIR /build
COPY go.mod go.sum ./
COPY pkg/    pkg/
COPY cmd/    cmd/
ENV CGO_ENABLED=0
RUN go build -a -ldflags='-w -s -extldflags=static -X sigs.k8s.io/vsphere-csi-driver/pkg/csi/service.version=${VERSION}' -o vsphere-csi ./cmd/vsphere-csi

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${PHOTON_IMAGE}
LABEL "maintainer" "Travis Rhoden <trhoden@vmware.com>"

RUN tdnf -y remove toybox

RUN tdnf -y install \
  util-linux \
  e2fsprogs \
  xfsprogs \
  btrfs-progs

RUN tdnf clean all

COPY --from=builder /build/vsphere-csi /bin/vsphere-csi

CMD ["/bin/vsphere-csi"]
