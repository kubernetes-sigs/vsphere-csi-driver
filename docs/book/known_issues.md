<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Known Issues

This section lists the major known issues with VMware vSphere CSI driver. For complete list of issues please check our [Github issues](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file an issue on the Github repository.

- [Filesystem resize is skipped.](#issue_1)
- [Volume cannot be resized in Statefulset or Deployment.](#issue_2)
- [Cannot recover from resize failure.](#issue_3)
- [CNS file volume has a limitation of 8k for metadata.](#issue_4)
- [CSI volume deletion get called before detach.](#issue_5)

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
- Workaround: None
