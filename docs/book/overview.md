<!-- markdownlint-disable MD033 -->
# Overview

## vSphere CSI Driver Components

The vSphere CSI driver includes the following components:

- [vSphere CSI Controller](#vsphere_csi_controller)
- [vSphere CSI Node](#vsphere_csi_node)
- [Syncer](#syncer)
  - [Metadata Syncer](#metadata_syncer)
  - [Full Sync](#full_sync)
  - [CnsOperator](#cns_operator)

### vSphere CSI Controller<a id="vsphere_csi_controller"></a>

The vSphere Container Storage Interface (CSI) Controller provides a CSI interface used by Container Orchestrators to manage the lifecycle of vSphere volumes.

The vSphere CSI Controller is responsible for volume provisioning, attaching and detaching the volume to VMs, mounting, formatting and unmounting volumes from the pod within the node VM, and so on.

### vSphere CSI Node<a id="vsphere_csi_node"></a>

The vSphere CSI Node is responsible for formatting, mounting the volumes to nodes, and using bind mounts for the volumes inside the pod. The vSphere CSI Node runs as a deamonset inside the cluster.

### Syncer<a id="syncer"></a>

#### Metadata Syncer<a id="metadata_syncer"></a>

The syncer is responsible for pushing PV, PVC, and pod metadata to CNS. The metadata appears in the CNS dashboard in the vSphere Client. The data helps vSphere administrators to determine which Kubernetes clusters, apps, pods, PVCs, and PVs are using the volume.

#### Full Sync<a id="full_sync"></a>

Full sync is responsible for keeping CNS up to date with Kubernetes volume metadata information, such as PVs, PVCs, pods, and so on. The following are several cases where full sync is helpful:

- CNS goes down
- CSI pod goes down
- API server goes down or Kubernetes core services goes down
- vCenter Server is restored to a backup point
- etcd is restored to a backup point

#### CnsOperator<a id="cns_operator"></a>

CnsOperator is used in the context of vSphere with Kubernetes, also known as Project Pacific. CnsOperator is responsible for the following operations:

- Attach and detach volumes to Tanzu Kubernetes cluster VMs.
- Deliver PV, PVC, and pod metadata from the Tanzu Kubernetes cluster to CNS.

## Types of the vSphere CSI Driver

The vSphere CSI driver supports the following:

- [Vanilla Kubernetes](https://github.com/kubernetes/kubernetes)
  - Support for block volumes.
  - Support for file volumes.

- [vSphere with Kubernetes](https://blogs.vmware.com/vsphere/2019/08/introducing-project-pacific.html)
  - In vSphere with Kubernetes, the vSphere CSI driver is called CNS-CSI driver.
  - Support for block volumes.

- [Tanzu Kubernetes Cluster](https://blogs.vmware.com/vsphere/2020/03/vsphere-7-tanzu-kubernetes-clusters.html)
  - In Tanzu Kubernetes clusters, the vSphere CSI driver is called Paravirtual CSI (pvCSI) driver.
  - Support for block volumes.
