# Kubernetes vSphere CSI Driver

vSphere Storage for Kubernetes, also called vSphere Cloud Provider, was introduced in 2017 and became the first vSphere storage solution for Kubernetes. The main goal of that project was to expose vSphere storage and features to Kubernetes users. The project offered an in-tree volume driver that has been actively used by various Kubernetes as a service solutions, such as TKGI, OpenShift, GKE On-Prem, and so on. Cloud Native Storage (CNS) is a result of evolution and productization of vSphere Storage for Kubernetes and is also enterprise ready.

The main goal of CNS is to make vSphere and vSphere storage, including vSAN, a platform to run stateful Kubernetes workloads. vSphere has a great data path that is highly reliable, highly performant and mature for enterprise use. CNS enables access of this data path to Kubernetes and brings an understanding of Kubernetes volume and pod abstractions to vSphere. CNS was first released in vSphere 6.7 Update 3.

## CNS Architecture

![CNS_ARCHITECTURE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/CNS-Architecture.png)

CNS offers the following two components:

- CNS in vCenter Server
- vSphere volume driver in Kubernetes

CNS control plane introduces a concept of volumes, such as container volumes and persistent volumes, in vSphere. It is the storage control plane for container volumes. CNS is responsible for managing the lifecycle of volumes, including such operations as create, read, update, and delete.  It is also responsible for managing volume metadata, snapshot and restore, volume copy and clone, as well as monitoring the health and compliance of volumes. These volumes are independent of the virtual machine lifecycle and have their own identity in vSphere.

CNS leverages the existing Storage Policy Based Management (SPBM) functionality for volume provisioning. In other words, the DevOps persona can use the storage policies, created by the vSphere administrator in vSphere, to specify the storage SLAs for the application volumes within Kubernetes. This way, CNS enables the DevOps persona to self-provision storage for their apps with desired storage SLAs. CNS honors these storage SLAs by provisioning the volume on an SPBM policy-compliant datastore in vSphere. As a result, the SPBM policy is applied at the granularity of a container volume.

CNS supports block volumes backed by First Class Disk (FCD) and file volumes backed by vSAN file shares. A block volume can only be attached to one Kubernetes pod with ReadWriteOnce access mode at any point in time. A file volume can be attached to one or more pods with ReadWriteMany/ReadOnlyMany access modes.

In Kubernetes, CNS provides a volume driver that has two sub-components – the CSI driver and the syncer. The CSI driver is responsible for volume provisioning, attaching and detaching the volume to VMs, mounting, formatting and unmounting volumes from the pod within the node VM, and so on. The CSI driver is built as an out-of-tree CSI plugin for Kubernetes. The syncer is responsible for pushing PV, PVC, and pod metadata to CNS.  It also has a CNS operator that is used in the context of vSphere with Kubernetes, formerly called Project Pacific.

CNS supports the following Kubernetes distributions:

- [Vanilla Kubernetes](https://github.com/kubernetes/kubernetes)
- [vSphere with Kubernetes](https://blogs.vmware.com/vsphere/2019/08/introducing-project-pacific.html) aka Supervisor Cluster. For more information, see [vSphere with Kubernetes Configuration and Management](https://docs.vmware.com/en/VMware-vSphere/7.0/vmware-vsphere-with-kubernetes/GUID-152BE7D2-E227-4DAA-B527-557B564D9718.html).
- [Tanzu Kubernetes Grid Service](https://blogs.vmware.com/vsphere/2020/03/vsphere-7-tanzu-kubernetes-clusters.html). For more information, see [Provisioning and Managing Tanzu Kubernetes Clusters Using the Tanzu Kubernetes Grid Service](https://docs.vmware.com/en/VMware-vSphere/7.0/vmware-vsphere-with-tanzu/GUID-2597788E-2FA4-420E-B9BA-9423F8F7FD9F.html).
