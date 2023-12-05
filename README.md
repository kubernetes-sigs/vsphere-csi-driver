<!-- markdownlint-disable MD034 -->
# Container Storage Interface (CSI) driver for vSphere

The vSphere CSI Driver is a Kubernetes plugin that allows persistent storage for containerized workloads running on vSphere infrastructure. It enables dynamic provisioning of storage volumes and provides features like snapshots, cloning, and dynamic expansion of volumes. The vSphere CSI Driver replaces the [in-tree vSphere volume plugin]( https://kubernetes.io/docs/concepts/storage/volumes/#vspherevolume) and offers integration with vSphere with better scale and performance.

This driver is in a stable `GA` state and is suitable for production use.  

It is recommended to install an out-of-tree Cloud Provider Interface like [vSphere Cloud Provider Interface](https://github.com/kubernetes/cloud-provider-vsphere) in the Kubernetes cluster to keep the Kubernetes cluster fully operational.

## Documentation

Documentation for vSphere CSI Driver is available here:

* [vSphere CSI Driver Concepts](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/3.0/vmware-vsphere-csp-getting-started/GUID-74AF02D7-1562-48BD-A9FE-C81A53342AC3.html)
* [vSphere CSI Driver Features](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/3.0/vmware-vsphere-csp-getting-started/GUID-D4AAD99E-9128-40CE-B89C-AD451DA8379D.html#GUID-E59B13F5-6F49-4619-9877-DF710C365A1E)
* [vSphere CSI Driver Deployment Guide](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/3.0/vmware-vsphere-csp-getting-started/GUID-6DBD2645-FFCF-4076-80BE-AD44D7141521.html)
* [vSphere CSI Driver User Guide](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/3.0/vmware-vsphere-csp-getting-started/GUID-6DBD2645-FFCF-4076-80BE-AD44D7141521.html)

## vSphere CSI Driver Releases

* [Release 3.1](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/3.0/rn/vmware-vsphere-container-storage-plugin-30-release-notes/index.html#vSphere%20Container%20Storage%20Plug-in%203.1.x)
* [Release 3.0](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/3.0/rn/vmware-vsphere-container-storage-plugin-30-release-notes/index.html)
* [Release 2.7](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/2.7/rn/vmware-vsphere-container-storage-plugin-27-release-notes/index.html)
* [Release 2.6](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/2.6/rn/vmware-vsphere-container-storage-plugin-26-release-notes/index.html)
* [Release 2.5](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/2.5/rn/vmware-vsphere-container-storage-plugin-25-release-notes/index.html)

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to contribute.

## Contact

* [Slack](https://kubernetes.slack.com/messages/provider-vsphere)
