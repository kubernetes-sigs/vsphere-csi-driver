# vSphere CSI Driver -Driver Development

## How to build this project

- Create a fork of [https://github.com/kubernetes-sigs/vsphere-csi-driver](https://github.com/kubernetes-sigs/vsphere-csi-driver)

- Clone the project

  ``` sh
  git clone https://github.com/your_github_user_name/vsphere-csi-driver.git
  ```

- Create a branch to the issues you are working on

  ``` sh
  git checkout -b my_fix
  ```

- Make code changes for CSI driver

- Build CSI driver

  ``` sh
  make build
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-extldflags "-static" -w -s -X "sigs.k8s.io/vsphere-csi-driver/pkg/csi/service.Version=v0.2.1-359-g167910f-dirty"' -o /Users/lipingx/go/src/vsphere-csi-driver/.build/bin/vsphere-csi.linux_amd64 cmd/vsphere-csi/main.go
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-extldflags "-static" -w -s' -o /Users/lipingx/go/src/vsphere-csi-driver/.build/bin/syncer.linux_amd64 cmd/syncer/main.go
  ```

- Run required check on the code

  ``` sh
  make check
  hack/check-format.sh
  hack/check-mdlint.sh
  hack/check-shell.sh
  hack/check-staticcheck.sh
  ...
  ```

## How to test CSI driver in a Kubernetes cluster

- Please make sure docker is installed before building the driver images.
- Build driver images, tag them in your local registry and then push images to your container registry.
vSphere CSI driver includes two images:

  - CSI controller (image name is "driver")
  - CSI Metadata Syncer (image name is "syncer")

  ``` sh
  make images
  hack/release.sh
  building gcr.io/cloud-provider-vsphere/csi/ci/driver:v0.2.1-359-g167910f-dirty
  GOPROXY=https://proxy.golang.org
  Sending build context to Docker daemon  101.3MB
  Step 1/16 : ARG GOLANG_IMAGE=golang:1.13
  Step 2/16 : ARG BASE_IMAGE=gcr.io/cloud-provider-vsphere/extra/csi-driver-base:v1.0.2-10-ga6fc92a
  Step 3/16 : FROM ${GOLANG_IMAGE} as builder
  ---> a80eb3fa7138
  Step 4/16 : ARG VERSION=unknown
  ---> Using cache
  ---> 0eb451bf6004
  Step 5/16 : ARG GOPROXY
  ---> Using cache
  ---> fe9a9519efb6
  Step 6/16 : WORKDIR /build
  ---> Using cache
  ---> 84c9add0b289
  Step 7/16 : COPY go.mod go.sum ./
  ---> Using cache
  ---> 71b9dc81d439
  Step 8/16 : COPY pkg/    pkg/
  ---> Using cache
  ---> 72b325fb07b6
  Step 9/16 : COPY cmd/    cmd/
  ---> Using cache
  ---> a7ae30863745
  Step 10/16 : ENV CGO_ENABLED=0
  ---> Using cache
  ---> 0a869496e9e4
  Step 11/16 : ENV GOPROXY ${GOPROXY:-https://proxy.golang.org}
  ---> Using cache
  ---> 941f37953062
  Step 12/16 : RUN go build -a -ldflags='-w -s -extldflags=static -X sigs.k8s.io/vsphere-csi-driver/pkg/csi/service.Version=${VERSION}' -o vsphere-csi ./cmd/vsphere-csi
  ---> Using cache
  ---> 48fa690544af
  Step 13/16 : FROM ${BASE_IMAGE}
  ---> 988063a2c19e
  Step 14/16 : LABEL "maintainers"="Divyen Patel <divyenp@vmware.com>, Sandeep Pissay Srinivasa Rao <ssrinivas@vmware.com>, Xing Yang <yangxi@vmware.com>"
  ---> Using cache
  ---> 8cf158ea7748
  Step 15/16 : COPY --from=builder /build/vsphere-csi /bin/vsphere-csi
  ---> Using cache
  ---> eab55395791b
  Step 16/16 : ENTRYPOINT ["/bin/vsphere-csi"]
  ---> Using cache
  ---> 89d604dff584
  Successfully built 89d604dff584
  Successfully tagged gcr.io/cloud-provider-vsphere/csi/ci/driver:v0.2.1-359-g167910f-dirty
  building gcr.io/cloud-provider-vsphere/csi/ci/syncer:v0.2.1-359-g167910f-dirty
  Sending build context to Docker daemon  101.3MB
  Step 1/14 : ARG GOLANG_IMAGE=golang:1.13
  Step 2/14 : ARG BASE_IMAGE=photon:3.0
  Step 3/14 : FROM ${GOLANG_IMAGE} as builder
  ---> a80eb3fa7138
  Step 4/14 : ARG GOPROXY
  ---> Using cache
  ---> 0d622b3a219f
  Step 5/14 : WORKDIR /build
  ---> Using cache
  ---> bc071741c18a
  Step 6/14 : COPY go.mod go.sum ./
  ---> Using cache
  ---> 26ae8c9b3aab
  Step 7/14 : COPY pkg/    pkg/
  ---> Using cache
  ---> fbbb3c5c2949
  Step 8/14 : COPY cmd/    cmd/
  ---> Using cache
  ---> 9a089bbb20db
  Step 9/14 : ENV CGO_ENABLED=0
  ---> Using cache
  ---> d037da2b2257
  Step 10/14 : ENV GOPROXY ${GOPROXY:-https://proxy.golang.org}
  ---> Using cache
  ---> b89e3eb8766b
  Step 11/14 : RUN go build -o vsphere-syncer ./cmd/syncer
  ---> Using cache
  ---> b66879fd246a
  Step 12/14 : FROM ${BASE_IMAGE}
  ---> 7495a0332b43
  Step 13/14 : COPY --from=builder /build/vsphere-syncer /bin/vsphere-syncer
  ---> Using cache
  ---> 205b7018aff4
  Step 14/14 : ENTRYPOINT ["/bin/vsphere-syncer"]
  ---> Using cache
  ---> 2f14f007470b
  [Warning] One or more build-args [VERSION] were not consumed
  Successfully built 2f14f007470b
  Successfully tagged gcr.io/cloud-provider-vsphere/csi/ci/syncer:v0.2.1-359-g167910f-dirty

  docker tag gcr.io/cloud-provider-vsphere/csi/ci/driver:v0.2.1-359-g167910f-dirty [your-repo]/vsphere-csi:latest
  docker push [your-repo]/vsphere-csi:latest

  docker tag gcr.io/cloud-provider-vsphere/csi/ci/syncer:v0.2.1-359-g167910f-dirty [your-repo]/syncer:latest
  docker push  [your-repo]/syncer:latest
  ```

- Replace image in yaml file
  - Replace images for `vsphere-csi-controller` and `vsphere-syncer` containers in `vsphere-csi-controller-deployment.yaml`.
  - Replace image for `vsphere-csi-node` container in `vsphere-csi-node-ds.yaml`
  - Follow [instruction](driver-deployment/installation.md) to install vSphere CSI driver.

## Testing

- Running unit test

  ``` sh
  make unit
  ```

- Running E2E test

  Follow this [instruction](https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/master/tests/e2e/README.md) to run E2E test.

## Contributing

Please see [CONTRIBUTING.md](https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/master/CONTRIBUTING.md) for instructions on how to contribute.
