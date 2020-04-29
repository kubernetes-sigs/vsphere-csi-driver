<!-- markdownlint-disable MD033 -->
# Overview

## Driver components

The driver consists on 3 main components

- [vSphere CSI Controller](#vsphere_csi_controller)
- [vSphere CSI Node](#vsphere_csi_node)
- [Syncer](#syncer)
  - [Metadata Syncer](#metadata_syncer)
  - [Full Sync](#full_sync)
  - [CnsOperator](#cns_operator)

### vSphere CSI Controller<a id="vsphere_csi_controller"></a>

The vSphere Container Storage Interface (CSI) Controller provides a CSI interface used by Container Orchestrators to manage the lifecycle of vSphere volumes.

vSphere CSI Controller is responsible for volume provisioning, attaching and detaching the volume to VMs, mounting, formatting and unmounting volumes from the pod within the node VM etc.

### vSphere CSI Node<a id="vsphere_csi_node"></a>

vSphere CSI Node is responsible for formatting, mounting the volumes to node and bindmount the volumes inside the pod. The vSphere CSI Node runs as a deamonset inside the cluster.

### Syncer<a id="syncer"></a>

#### Metadata Syncer<a id="metadata_syncer"></a>

The syncer is responsible for pushing PV, PVC and Pod metadata to CNS.
These metadata will be shown in CNS dashboard (aka CNS UI) to help vSphere admin to get insights into which Kubernetes cluster(s), which app(s), Pod(s), PVC(s), PV(s) are using the volume.

#### Full Sync<a id="full_sync"></a>

Full sync is responsible for keeping CNS up to date with Kubernetes volume metadata information like PV, PVC and Pod etc. Some cases where full sync would be helpful are listed below:

- CNS goes down
- CSI Pod goes down
- API server goes down / K8s core services goes down
- VC is restored to a backup point
- etcd is restored to a backup point

#### CnsOperator<a id="cns_operator"></a>

CnsOperator is used in the context of Project Pacific. CnsOperator is responsible for

- attach/detach volumes to Tanzu K8S Grid VMs
- pushing Tanzu K8S Grid PV, PVC and Pod metadata to CNS

## Driver flavors

vSphere CSI driver supports various flavors

- [Vanilla Kubernetes](https://github.com/kubernetes/kubernetes)
  - Support for block volumes.
  - Support for file volumes.

- [Project Pacific](https://blogs.vmware.com/vsphere/2019/08/introducing-project-pacific.html)
  - In Project Pacific, vSphere CSI driver is called CNS CSI driver.
  - Support for block volumes.

- [Tanzu Kubernetes Grid](https://blogs.vmware.com/vsphere/2020/03/vsphere-7-tanzu-kubernetes-clusters.html)
  - In Tanzu Kubernetes Grid, vSphere CSI driver is called pvCSI driver.
  - Support for block volumes.
