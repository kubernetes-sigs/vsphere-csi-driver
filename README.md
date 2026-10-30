<!-- markdownlint-disable MD034 -->
# Container Storage Interface (CSI) driver for vSphere

The vSphere CSI Driver is a Kubernetes plugin that allows persistent storage for containerized workloads running on vSphere infrastructure. It enables dynamic provisioning of storage volumes and provides features like snapshots, cloning, and dynamic expansion of volumes. The vSphere CSI Driver replaces the [in-tree vSphere volume plugin]( https://kubernetes.io/docs/concepts/storage/volumes/#vspherevolume) and offers integration with vSphere with better scale and performance.

This driver is in a stable `GA` state and is suitable for production use.  

It is recommended to install an out-of-tree Cloud Provider Interface like [vSphere Cloud Provider Interface](https://github.com/kubernetes/cloud-provider-vsphere) in the Kubernetes cluster to keep the Kubernetes cluster fully operational.

## Documentation

Documentation for vSphere CSI Driver is available here:

* [vSphere CSI Driver Concepts](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/getting-started-with-vmware-vsphere-container-storage-plug-in-3-0/vsphere-container-storage-plug-in-concepts.html)
* [vSphere CSI Driver Features](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/getting-started-with-vmware-vsphere-container-storage-plug-in-3-0/vsphere-container-storage-plug-in-concepts/vsphere-functionality-supported-by-vsphere-container-storage-plug-in.html)
* [vSphere CSI Driver Deployment Guide](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/getting-started-with-vmware-vsphere-container-storage-plug-in-3-0/vsphere-container-storage-plug-in-deployment.html)
* [vSphere CSI Driver User Guide](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/getting-started-with-vmware-vsphere-container-storage-plug-in-3-0/using-vsphere-container-storage-plug-in.html)

## vSphere CSI Driver Releases

For the latest release, its container images and its manifests, see the [GitHub releases page](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases). Each release refers to the manifests under `manifests/vanilla` at its tag, and images are published to `registry.k8s.io/csi-vsphere/driver` and `registry.k8s.io/csi-vsphere/syncer` with the release tag (for example `registry.k8s.io/csi-vsphere/driver:<release-tag>`).

For supported Kubernetes versions and End Of Life (EOL) dates, see the [compatibility matrix](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/getting-started-with-vmware-vsphere-container-storage-plug-in-3-0/vsphere-container-storage-plug-in-concepts/compatibility-matrix-for-vsphere-container-storage-plug-in.html). Only versions that have not reached their EOL date are supported.

Supported releases and their images (see the [GitHub releases page](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases) for the authoritative list):

* Release 3.8
  * [v3.8.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.8.0) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.8.0
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.8.0
    ```

* Release 3.7
  * [v3.7.4](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.7.4) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.7.4
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.7.4
    ```

  * [v3.7.3](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.7.3) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.7.3
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.7.3
    ```

  * [v3.7.2](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.7.2) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.7.2
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.7.2
    ```

  * [v3.7.1](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.7.1) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.7.1
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.7.1
    ```

  * [v3.7.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.7.0) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.7.0
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.7.0
    ```

* Release 3.6
  * [v3.6.3](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.6.3) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.6.3
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.6.3
    ```

  * [v3.6.2](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.6.2) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.6.2
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.6.2
    ```

  * [v3.6.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.6.0) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.6.0
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.6.0
    ```

* Release 3.5
  * [v3.5.1](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.5.1) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.5.1
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.5.1
    ```

  * [v3.5.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.5.0) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.5.0
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.5.0
    ```

* Release 3.4
  * [v3.4.0](https://github.com/kubernetes-sigs/vsphere-csi-driver/releases/tag/v3.4.0) images

    ```text
    registry.k8s.io/csi-vsphere/driver:v3.4.0
    ```

    ```text
    registry.k8s.io/csi-vsphere/syncer:v3.4.0
    ```

Release notes for earlier releases (3.3 and older have reached or are past their EOL date):

* [Release 3.3](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/release-notes/vmware-vsphere-container-storage-plugin-30-release-notes.html#GUID-0dda29e0-50c2-42c1-885a-5c5758ee91ab-en_id-af77b3cd-47a8-468d-9d39-be353b90022f)
* [Release 3.2](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/release-notes/vmware-vsphere-container-storage-plugin-30-release-notes.html#GUID-0dda29e0-50c2-42c1-885a-5c5758ee91ab-en_id-0d835538-6ab6-423e-8a7b-c521b3fd1458)
* [Release 3.1](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/release-notes/vmware-vsphere-container-storage-plugin-30-release-notes.html#GUID-0dda29e0-50c2-42c1-885a-5c5758ee91ab-en_id-8c9a81c3-e1c0-49ab-a34c-15326ab546b2)
* [Release 3.0](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/3-0/release-notes/vmware-vsphere-container-storage-plugin-30-release-notes.html#GUID-0dda29e0-50c2-42c1-885a-5c5758ee91ab-en_id-e109e279-1fd8-4026-a9ec-b733c1d24b4d)
* [Release 2.7](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/2-0/release-notes/vmware-vsphere-container-storage-plugin-27-release-notes.html)
* [Release 2.6](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/2-0/release-notes/vmware-vsphere-container-storage-plugin-26-release-notes.html)
* [Release 2.5](https://techdocs.broadcom.com/us/en/vmware-cis/vsphere/container-storage-plugin/2-0/release-notes/vmware-vsphere-container-storage-plugin-25-release-notes.html)

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to contribute.

## Contact

* [Slack](https://kubernetes.slack.com/messages/provider-vsphere)
