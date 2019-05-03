# Container Storage Interface (CSI) driver for vSphere

This repository provides tools and scripts for building and testing `csi-vsphere`.
The driver is an **alpha** state and should not be used in production.

On Kubernetes, the CSI driver is for use in conjunction with the out of tree vSphere
[CCM](https://github.com/kubernetes/cloud-provider-vsphere).

The driver has been tested with Kubernetes 1.13 and 1.14, but due to internal K8s API
changes during that cycle, an upgrade path for using `csi-vsphere` while moving from
1.13 to 1.14 has not been documented.

## Building the CSI driver

This section outlines how to build the driver with and without Docker.

### Building locally

Build locally with the following command:

```shell
$ git clone https://github.com/kubernetes-sigs/vsphere-csi-driver && \
  make -C vsphere-csi-driver
```

The project uses [Go modules](https://github.com/golang/go/wiki/Modules) and:
* Requires Go 1.11+
* Should not be cloned into the `$GOPATH`

### Building with Docker

It is also possible to build the driver with Docker in order to ensure a clean build environment:

```shell
$ git clone https://github.com/kubernetes-sigs/vsphere-csi-driver && \
  make -C vsphere-csi-driver build-with-docker
```

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to contribute.
