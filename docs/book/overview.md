<!-- markdownlint-disable MD033 -->
# Overview

## vSphere CSI Driver Components

The vSphere CSI driver includes the following components:

- [vSphere CSI Controller](#vsphere_csi_controller)
- [vSphere CSI Node](#vsphere_csi_node)
- [Syncer](#syncer)
  - [Metadata Syncer](#metadata_syncer)
  - [Full Sync](#full_sync)

### vSphere CSI Controller<a id="vsphere_csi_controller"></a>

The vSphere Container Storage Interface (CSI) Controller provides a CSI interface used by Container Orchestrators to manage the lifecycle of vSphere volumes.
The vSphere CSI Controller is responsible for creating, expanding and deleting volumes, attaching and detaching the volumes to Node VMs.

### vSphere CSI Node<a id="vsphere_csi_node"></a>

The vSphere CSI Node is responsible for formatting, mounting the volumes to nodes, and using bind mounts for the volumes inside the pod.
Before volume needs to be detached, CSI Node is helping to unmount volume from the node.
The vSphere CSI Node runs as a daemonset inside the cluster.

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
