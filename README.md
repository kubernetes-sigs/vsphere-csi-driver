# Container Storage Interface (CSI) driver for vSphere

This repository provides tools and scripts for building and testing the vSphere CSI provider. This driver is in a stable `GA` state and is suitable for production use. It currently requires vSphere 6.7 U3 or higher in order to operate.

The CSI driver, when used on Kubernetes, also requires the use of the out-of-tree vSphere Cloud Provider Interface [CPI](https://github.com/kubernetes/cloud-provider-vsphere).

The driver has been tested with, and is supported on, K8s 1.14 and above.

## Installation

Install instructions for the CSI driver are available here:

* <https://cloud-provider-vsphere.sigs.k8s.io/tutorials/kubernetes-on-vsphere-with-kubeadm.html>

## CSI Driver Images

The CSI driver container images are available here:

* <https://hub.docker.com/r/vmware/vsphere-block-csi-driver>
* <https://hub.docker.com/r/vmware/volume-metadata-syncer>

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to contribute.
