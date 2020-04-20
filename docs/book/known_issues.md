# vSphere CSI Driver - Known Issues

This section lists the major known issues with VMware vSphere CSI driver. For complete list of issues please check our Github issues(https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file a bug on the Github repo.

- [Known Issues for Resize](#known_issues_for_resize)
- [Known Issues for Release Documentation](#known_issues_for_release_documentation)

## Known Issues for Resize<a id="known_issues_for_resize"></a>

1. Filesystem resize is skipped if the original PVC is deleted when FilesystemResizePending condition is still on the PVC, but PV and its associated volume on the storage system are not deleted due to the Retain policy. Issue is opened: https://github.com/kubernetes/kubernetes/issues/88683

   - Impact: User may create a new PVC to statically bind to the undeleted PV.  In this case, the volume on the storage system is resized but the filesystem is not resized accordingly. User may try to write to the volume whose filesystem is out of capacity.
   - Workaround: User can log into the container to manually resize the filesystem.

2. Volume cannot be resized in a Statefulset or other workload API such as Deployment. Issue is opened in https://github.com/kubernetes/enhancements/pull/660

    - Impact: User cannot resize volume in a workload API such as StatefulSet.
    - Workaround: None
    
3. Recover from volume expansion failure.

    - Impact: If volume expansion fails because storage system does not support it, there is no way to recover. Issue is    opened in: https://github.com/kubernetes/enhancements/pull/1516
    - Workaround: None
    
## Known Issues for Release Documentation<a id="known_issues_for_release_documentation"></a>
1. Instructions on setting the parameters for retaining CSI driver logs are missing in the release documentation. 

    - Impact:  The CSI driver logs are not retained across failures. Old logs would be very useful for debugging issues that customers raise.
    - Workaround: Set the following kubelet parameters to retain csi driver logs:
    ```bash
    minimum-container-ttl-duration=5m0s maximum-dead-containers-per-container=5
    ```
2. The permission table from the documentation is incomplete when using a dedicated account for CSI deployment. Link:https://docs.vmware.com/en/VMware-vSphere/6.7/Cloud-Native-Storage/GUID-AEB07597-F303-4FDD-87D9-0FDA4836E5BB.html  It is missing the "Propagate to Child Object" information, but not only, it is missing location when Environment uses "Host and Cluster" folders, or when using Zone/Region labels.

    - Impact:  The user might face the privilege issue when the permission table is not completed.
    - Workaround: The updated table for CNS - User permissions is in the vSphere CSI Driver - Prerequisites
    
3. CNS file volume has a limitation of 8K for metadata

    - Impact:  It is quite possible that we will not be able to push all the metadata to CNS file share as we need support a max of 64 clients per file volume.
    - Workaround: None
