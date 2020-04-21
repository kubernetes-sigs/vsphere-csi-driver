# vSphere CSI Driver - Known Issues

This section lists the major known issues with VMware vSphere CSI driver. For complete list of issues please check our Github issues(https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file a bug on the Github repo.

- [Issue 1](issue_1)
- [Issue 2](issue_2)
- [Issue 3](issue_3)
- [Issue 4](issue_4)
- [Issue 5](issue_5)




Issue 1<a id="issue_1"></a>: Filesystem resize is skipped if the original PVC is deleted when FilesystemResizePending condition is still on the PVC, but PV and its associated volume on the storage system are not deleted due to the Retain policy. Issue is opened: https://github.com/kubernetes/kubernetes/issues/88683

   - Impact: User may create a new PVC to statically bind to the undeleted PV.  In this case, the volume on the storage system is resized but the filesystem is not resized accordingly. User may try to write to the volume whose filesystem is out of capacity.
   - Workaround: User can log into the container to manually resize the filesystem.

Issue 2<a id="issue_2"></a>: Volume cannot be resized in a Statefulset or other workload API such as Deployment. Issue is opened in https://github.com/kubernetes/enhancements/pull/660

   - Impact: User cannot resize volume in a workload API such as StatefulSet.
   - Workaround: None
    
Issue 3<a id="issue_3"></a>: Recover from volume expansion failure.

   - Impact: If volume expansion fails because storage system does not support it, there is no way to recover. Issue is    opened in: https://github.com/kubernetes/enhancements/pull/1516
   - Workaround: None

Issue 4<a id="issue_4"></a>: Instructions on setting the parameters for retaining CSI driver logs are missing in the release documentation. 

   - Impact:  The CSI driver logs are not retained across failures. Old logs would be very useful for debugging issues that customers raise.
   - Workaround: Set the following kubelet parameters to retain csi driver logs:
   ```bash
   minimum-container-ttl-duration=5m0s maximum-dead-containers-per-container=5
   ```
    
Issue 5<a id="issue_5"></a>: CNS file volume has a limitation of 8K for metadata

   - Impact:  It is quite possible that we will not be able to push all the metadata to CNS file share as we need support a max of 64 clients per file volume.
   - Workaround: None
