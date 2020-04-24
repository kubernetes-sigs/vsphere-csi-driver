# Container Storage Interface (CSI) driver for vSphere

This repository provides tools and scripts for building and testing the vSphere CSI provider. This driver is in a stable `GA` state and is suitable for production use. It currently requires vSphere 6.7 U3 or higher in order to operate.

The CSI driver, when used on Kubernetes, also requires the use of the out-of-tree vSphere Cloud Provider Interface [CPI](https://github.com/kubernetes/cloud-provider-vsphere).

The driver has been tested with, and is supported on, K8s 1.14 and above.

## Documentation

Documentation for vSphere CSI Driver is available here:

* <https://vsphere-csi-driver.sigs.k8s.io>

## vSphere CSI Driver Images

Please use appropriate deployment yaml files available here - <https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests>

Note: `v1.0.2`, deployment yamls files are compatible with `v1.0.1`.
For `v2.0.0-rc.1` and above use `v2.0.0` deployment yamls.

### v2.0.0-rc-1

* gcr.io/cloud-provider-vsphere/csi/release/driver:v2.0.0-rc.1
* gcr.io/cloud-provider-vsphere/csi/release/syncer:v2.0.0-rc.1

### v1.0.2

* gcr.io/cloud-provider-vsphere/csi/release/driver:v1.0.2
* gcr.io/cloud-provider-vsphere/csi/release/syncer:v1.0.2

### v1.0.1

* gcr.io/cloud-provider-vsphere/csi/release/driver:v1.0.1
* gcr.io/cloud-provider-vsphere/csi/release/syncer:v1.0.1

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to contribute.
