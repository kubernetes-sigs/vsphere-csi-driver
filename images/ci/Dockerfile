################################################################################
##                               BUILD ARGS                                   ##
################################################################################
# The golang image is used to create the project's module and build caches
# and is also the image on which this image is based.
ARG GOLANG_IMAGE=golang:1.13

################################################################################
##                            GO MOD CACHE STAGE                              ##
################################################################################
# Create a Go module cache.
FROM ${GOLANG_IMAGE} as mod-cache
WORKDIR /build
COPY go.mod go.sum ./
COPY pkg ./pkg/
COPY cmd ./cmd/
ARG GOOS
ARG GOARCH
ENV GOOS=${GOOS:-linux} GOARCH=${GOARCH:-amd64}

ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/ && \
    echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa && \
    chmod -R 600 /root/.ssh/id_rsa && \
    ssh-keyscan -t rsa gitlab.eng.vmware.com >> /root/.ssh/known_hosts
RUN git config --global --add url."git@gitlab.eng.vmware.com:".insteadOf "https://gitlab.eng.vmware.com/"
RUN go mod download && go mod verify
RUN rm /root/.ssh/id_rsa && \
    rm /root/.ssh/known_hosts

################################################################################
##                           GO BUILD CACHE STAGE                             ##
################################################################################
# Create a Go build cache. Please note the reason the Makefile is not used and
# "go build" is invoked directly is to avoid having to rebuild this stage as a
# result of the Makefile changing.
FROM ${GOLANG_IMAGE} as build-cache
WORKDIR /build
COPY --from=mod-cache /go/pkg/mod /go/pkg/mod/
COPY go.mod go.sum hack/make/ldflags.txt ./
COPY pkg ./pkg/
COPY cmd ./cmd/
ARG GOOS
ARG GOARCH
ENV CGO_ENABLED=0 GOOS=${GOOS:-linux} GOARCH=${GOARCH:-amd64}
RUN LDFLAGS=$(cat ldflags.txt) && \
    go build -ldflags "${LDFLAGS}" ./cmd/vsphere-csi && \
    go build -ldflags "${LDFLAGS}" ./cmd/syncer

################################################################################
##                               LINT STAGE                                   ##
################################################################################
FROM ${GOLANG_IMAGE} as lint
RUN go get -u golang.org/x/lint/golint

################################################################################
##                               MAIN STAGE                                   ##
################################################################################
FROM ${GOLANG_IMAGE}
LABEL "maintainer" "Cheng Cheng <chengch@vmware.com>"

################################################################################
##                            CONFIGURE LOCALE                                ##
################################################################################
# Set the locale so that the gist command is happy.
ENV LANG=en_US.UTF-8 LC_ALL=C.UTF-8

################################################################################
##                             DOCKER-IN-DOCKER                               ##
################################################################################
# Again, copied from test-infra's bootstrap image:
# https://github.com/kubernetes/test-infra/blob/master/images/bootstrap/Dockerfile

# Install Docker deps, some of these are already installed in the image but
# that's fine since they won't re-install and we can reuse the code below
# for another image someday.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg2 \
      software-properties-common \
      lsb-release && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/*

# Add the Docker apt-repository
RUN curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "${ID}")/gpg | apt-key add - && \
    add-apt-repository \
      "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "${ID}") \
      $(lsb_release -cs) stable"

# Install Docker
# TODO: the `sed` is a bit of a hack, look into alternatives.
# Why this exists: `docker service start` on debian runs a `cgroupfs_mount` method,
# We're already inside docker though so we can be sure these are already mounted.
# Trying to remount these makes for a very noisy error block in the beginning of
# the pod logs, so we just comment out the call to it... :shrug:
# TODO(benthelder): update docker version. This is pinned because of
# https://github.com/kubernetes/test-infra/issues/6187
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker-ce=18.06.* && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/* && \
    sed -i 's/cgroupfs_mount$/#cgroupfs_mount\n/' /etc/init.d/docker

# Move Docker's storage location
RUN echo 'DOCKER_OPTS="${DOCKER_OPTS} --data-root=/docker-graph"' | \
    tee --append /etc/default/docker

# NOTE this should be mounted and persisted as a volume ideally (!)
# We will make a fallback one now just in case
RUN mkdir /docker-graph

# Setting this environment variable is an easy way for processes running
# in the container to know DinD is enabled.
ENV DOCKER_IN_DOCKER_ENABLED=true

################################################################################
##                             INSTALL LINT                                   ##
################################################################################
COPY --from=lint /go/bin/golint /usr/local/bin/

################################################################################
##                         PRIME GO MOD & BUILD CACHES                        ##
################################################################################
COPY --from=mod-cache   /go/pkg/mod           /go/pkg/mod/
COPY --from=build-cache /root/.cache/go-build /root/.cache/go-build/

################################################################################
##                              ADD WORKDIR                                   ##
################################################################################
# Map the sources into the project's traditional Gopath location in the
# image.
WORKDIR /go/src/sigs.k8s.io/vsphere-csi-driver