<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Known Issues

This section lists the major known issues with VMware vSphere CSI driver. For complete list of issues please check our [Github issues](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file an issue on the Github repository.

- [Filesystem resize is skipped.](#issue_1)
- [Volume cannot be resized in Statefulset or Deployment.](#issue_2)
- [Cannot recover from resize failure.](#issue_3)
- [CNS file volume has a limitation of 8k for metadata.](#issue_4)
- [CSI volume deletion get called before detach.](#issue_5)
- [Devops can modify the volume health status of a PVC manually](#issue_6)
- [Performance regression in Vanilla Kubernetes 1.17 and 1.18 and Supervisor Cluster 7.0 patch releases](#issue_7)
- [Migrated Volume Deleted by in-tree vSphere plugin remains on the CNS UI](#issue_8)
= [CnsRegisterVolume API does not validate if the volume to import is already imported or already present in the supervisor cluster](#issue_9)

Issue 1<a id="issue_1"></a>: Filesystem resize is skipped if the original PVC is deleted when FilesystemResizePending condition is still on the PVC, but PV and its associated volume on the storage system are not deleted due to the Retain policy.

- Impact: User may create a new PVC to statically bind to the undeleted PV. In this case, the volume on the storage system is resized but the filesystem is not resized accordingly. User may try to write to the volume whose filesystem is out of capacity.
- Upstream issue is tracked at: https://github.com/kubernetes/kubernetes/issues/88683
- Workaround: User can log into the container to manually resize the filesystem.

Issue 2<a id="issue_2"></a>: Volume cannot be resized in a Statefulset or other workload API such as Deployment.

- Impact: User cannot resize volume in a workload API such as StatefulSet.
- Upstream issue is tracked at: https://github.com/kubernetes/enhancements/pull/660
- Workaround: None

Issue 3<a id="issue_3"></a>: Recover from volume expansion failure.

- Impact: If volume expansion fails because storage system does not support it, there is no way to recover.
- Upstream issue is tracked at: https://github.com/kubernetes/enhancements/pull/1516
- Workaround: None

Issue 4<a id="issue_4"></a>: CNS file volume has a limitation of 8K for metadata.

- Impact: It is quite possible that we will not be able to push all the metadata to CNS file share as we need support a max of 64 clients per file volume.
- Workaround: None

Issue 5<a id="issue_5"></a>: The CSI delete volume is getting called before detach.

- Impact: There could be a possibility of CSI getting Delete Volume before ControllerUnpublish.
- Upstream issue is tracked at: https://github.com/kubernetes/kubernetes/issues/84226
- Workaround:

    1. Delete the Pod with force:
       `kubectl delete pods <pod> --grace-period=0 --force`
    2. Find VolumeAttachment for the volume that remained undeleted. Get Node from this VolumeAttachment.
    3. Manually detach the disk from the Node VM.
    4. Edit this VolumeAttachment and remove the finalizer. It will get deleted.
    5. Use `govc` to manually delete the FCD.
    6. Edit Pending PV and remove the finalizer. It will get deleted.

Issue 6<a id="issue_6"></a>: vSphere with Kubernetes Cluster Devops can modify the volume health status of a PVC manually since the volume health annotation is not a read-only field. Devops should avoid modifying the volume health annotation manually. If DevOps modifies the volume health to a random or incorrect health status, then any software dependent on this volume health will be affected.

- Impact:Any random volume health status set by the vSphere with Kubernetes Cluster Devops will get reflected in volume health status of PVC in Tanzu Kubernetes Grid Cluster as well.

Issue 7<a id="issue_7"></a>: Performance regression in Vanilla Kubernetes 1.17 and 1.18 and Supervisor Cluster 7.0 patch releases.

- Impact: Low throughput of attach and detach operations, especially at scale.
- Upstream issue is tracked at: https://github.com/kubernetes/kubernetes/issues/84169
- Workaround:  
  - For Vanilla Kubernetes, upgrade your Kubernetes minor version to 1.17.8 and above or 1.18.5 and above. These versions contain the upstream [fix](https://github.com/kubernetes/kubernetes/pull/91307) for this issue.
  - If upgrading the Kubernetes version is not possible, then there is a workaround that can be applied on your Kubernetes cluster. On each primary node, perform the following steps:
    1. Open kube-controller-manager manifest, located at `/etc/kubernetes/manifests/kube-controller-manager.yaml`
    2. Add `--disable-attach-detach-reconcile-sync` to `spec.containers.command`
    3. Since kube-controller-manager is a static pod, Kubelet will restart it whenever a new flag is added. Make sure the kube-controller-manager pod is up and running.

Issue 8<a id="issue_8"></a>: Migrated in-tree vSphere volume deleted by in-tree vSphere plugin remains on the CNS UI

- Impact: Migrated in-tree vSphere volumes deleted by in-tree vSphere plugin remains on the CNS UI.
- Workaround: Admin needs to manually reconcile discrepancies in the Managed Virtual Disk Catalog. Admin needs to follow this [KB article](https://kb.vmware.com/s/article/2147750)

Issue 9<a id="issue_9"></a>: CnsRegisterVolume API does not validate if the volume to import is already imported or already present in the supervisor cluster

- Impact: One of the PVC using the CNS volume will be usable at any point in time. Usage of any other PVC will lead to attach failures.
- Workaround: None
