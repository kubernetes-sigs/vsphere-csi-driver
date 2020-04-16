# Kubernetes vSphere CSI Driver

vSphere Storage for Kubernetes (aka vSphere Cloud Provider) was the first vSphere storage solution for Kubernetes that was started in 2017. The main goal of that project was to expose vSphere storage and features to Kubernetes users. This is an in-tree volume driver and is being actively used by various Kubernetes as a service offerings/solution like PKS, OpenShift, GKE On-Prem, etc. Cloud Native Storage(CNS) is the evolution and productization of vSphere Storage for Kubernetes and is also enterprise-ready.

The main goal of CNS is to make vSphere and vSphere storage (including vSAN) a great platform to run stateful Kubernetes workloads. vSphere has a great data path that is highly reliable, highly performant and mature for enterprise use.  CNS enables access of this data path to Kubernetes and brings an understanding of Kubernetes volume, pod abstractions to vSphere. CNS was first released in vSphere 6.7 U3 release.

## CNS Architecture

![CNS_ARCHITECTURE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/CNS-Architecture.png)

There are 2 new components we built in this stack:

- CNS in vCenter
- vSphere Volume driver in Kubernetes

CNS control plane introduces a concept of volumes (aka container volumes, persistent volumes) in vSphere. It is the storage control plane for container volumes. CNS is responsible for managing the lifecycle (Create, Read, Update, Delete - CRUD) of volumes, managing volume metadata, snapshot/restore, volume copy/clone, monitoring the health and compliance of volumes. These volumes are independent of the virtual machine lifecycle and have their own identity in vSphere.

CNS leverages the existing SPBM (Storage Policy Based Management) volume provisioning functionality. In other words, the DevOps persona can use the storage policies, authored by the vSphere admin in vSphere, to specify the storage SLAs for their application volumes within Kubernetes. This way, CNS enables the DevOps persona to self-provision storage for their apps with desired storage SLAs. CNS will honor these storage SLAs by provisioning the volume on a SPBM policy-compliant datastore in vSphere. Basically, the SPBM policy is applied at the granularity of a container volume.

CNS supports block volumes backed by FCDs (First Class Disk) and file volumes backed by vSAN file shares. A block volume can only be attached to one Kubernetes pod with ReadWriteOnce access mode at any point in time whereas, a file volume can be attached to one or more pods with ReadWriteMany/ReadOnlyMany access modes.

In Kubernetes, we built a volume driver that has 2 sub-components - CSI driver, syncer. CSI driver is responsible for volume provisioning, attaching and detaching the volume to VMs, mounting, formatting and unmounting volumes from the pod within the node VM, etc. This is built as an out of tree CSI plugin for Kubernetes. The syncer is responsible for pushing PV, PVC and Pod metadata to CNS, and it also has CNS operator which is used in the context of Project Pacific.

CNS solution support 3 flavors of Kubernetes:

- [Vanilla](https://github.com/kubernetes/kubernetes)
- [Project Pacific](https://blogs.vmware.com/vsphere/2019/08/introducing-project-pacific.html)
- [Tanzu Kubernetes Grid](https://blogs.vmware.com/vsphere/2020/03/vsphere-7-tanzu-kubernetes-clusters.html)
