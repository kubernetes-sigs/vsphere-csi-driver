<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Known Issues

For complete list of issues please check our [Github issues](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file an issue on the Github repository.

Please refer to release notes to learn about known issues in each release.

Following listing is for issues observed in Kubernetes.

## Multi-Attach error for RWO (Block) volume when Node VM is shutdown before Pods are evicted and Volumes are detached from Node VM

Note: This Issue is present in all Kubernetes Releases

- Impact: After Node is shutdown, Pod running on that Node does not come up on the new Node. Events on the Pod will have a warning message for "FailedAttachVolume". Error Message: `Multi-Attach error for volume "pvc-uuid" Volume is already exclusively attached to one node and can't be attached to another.`
- Upstream Issue:  Kubernetes is being enhanced to fix this issue. Here is the Kubernetes Enhancement Proposals (KEP) PR you can refer for more detail - https://github.com/kubernetes/enhancements/pull/1116
- Workaround:
  - Pods stuck in this state can be recovered by following steps.
    1. Find the Node VM in the vCenter Inventory. Make sure the correct VM associated with the Node is used for further instructions.
    2. Detach all Persistent Volumes Disk attached to this Node VM. Note: Do not detach Primary disks used by the Guest OS.
        1. Right-click a virtual machine in the inventory and select Edit Settings.
        2. From Virtual Hardware find all Hard Disks for Persistent Volumes and remove them. (Do not select - Delete files from datastore)
        3. Click on OK to reconfigure VM to detach all Persistent Volumes disks from shutdown/powered off Node VM.
    3. Execute `kubectl get volumeattachments` and find all volumeattachments objects associated with shutdown Node VM.
    4. Edit `volumeattachment` object with `kubectl edit volumeattachments <volumeattachments-object-name>` and remove finalizers.
    5. Check if the `volumeattachment` object is deleted by Kubernetes. If this object remains on the system, you can safely delete this with `kubectl delete volumeattachments <volumeattachments-object-name>`.
    6. Wait for some time for Pod to come up on a new Node.

## Performance regression in Kubernetes 1.17 and 1.18

- Impact: Low throughput of attach and detach operations, especially at scale.
- Upstream issue is tracked at: https://github.com/kubernetes/kubernetes/issues/84169
- Workaround:
  - Upgrade Kubernetes minor version to 1.17.8 and above or 1.18.5 and above. These versions contain the upstream fix for this issue.
  - If upgrading the Kubernetes version is not possible, then there is a workaround that can be applied on your Kubernetes cluster. On each primary node, perform the following steps:
       1. Open kube-controller-manager manifest, located at `/etc/kubernetes/manifests/kube-controller-manager.yaml`
       2. Add `--disable-attach-detach-reconcile-sync` to spec.containers.command
       3. Since kube-controller-manager is a static pod, Kubelet will restart it whenever a new flag is added. Make sure the kube-controller-manager pod is up and running.

## Volume resize incomplete if PVC is deleted before filesystem can be resized

- Issue: Filesystem resize is skipped if the original PVC is deleted when FilesystemResizePending condition is still on the PVC, but PV and its associated volume on the storage system are not deleted due to the Retain policy. Refer to https://github.com/kubernetes/kubernetes/issues/88683 for more details.
- Impact: User may create a new PVC to statically bind to the undeleted PV. In this case, the volume on the storage system is resized but the filesystem is not resized accordingly. User may try to write to the volume whose filesystem is out of capacity.
- Workaround: User can log into the container to manually resize the filesystem.

## Volume associated with a Statefulset cannot be resized

- Issue: https://github.com/kubernetes/enhancements/issues/661
- Impact: User cannot resize volume in a StatefulSet.
- Workaround:
  - Upstream solution is being tracked at https://github.com/kubernetes/enhancements/pull/2842.
  - If the statefulset is not managed by an operator, there is a slightly risky workaround which the user can use on their own discretion depending upon their use case. Please refer to https://serverfault.com/a/989665 for more details. Kindly note that VMware does not support this option.

## Recovery from volume expansion failure

- Impact: If a user tries to expand a PVC to a size which may not be supported by the underlying storage system, volume expansion will keep failing and there is no way to recover.
- Issue: https://github.com/kubernetes/enhancements/issues/1790
- Workaround is being tracked at https://github.com/kubernetes/enhancements/blob/master/keps/sig-storage/1790-recover-resize-failure/README.md

## Multipath and VMware Virtual Disks

When following generic multipath setup guides (e.g. [NetApp Trident worker node prep](https://docs.netapp.com/us-en/trident/trident-use/worker-node-prep.html)), multipath may be enabled for all disks, including VMware virtual disks. This can cause issues with the vsphere-csi-driver:

- VMware virtual disks are presented as device-mapper devices (e.g. `/dev/dm-*`).
- The vsphere-csi-driver expects a physical device, not a virtual device.
- This results in errors like: `/sys/devices/virtual/block/dm-<N>/device: no such file or directory`

**Root cause:** Multipath is configured for VMware virtual disks, but multipathing should only be used for iSCSI/NFS (e.g. with Trident). For VMware, multipathing is handled at the ESXi layer.

**Solution:**
Blacklist VMware virtual disks in your `multipath.conf` to prevent multipath from claiming them:

```conf
defaults { ... }
blacklist {
  device {
    vendor "VMware"
    product "Virtual disk"
  }
}
```

You can verify the vendor and product with:

```shell
dmsetup ls --tree
# mpathz (250:99)
# └─ (34:128)

multipath -l mpathz
# mpathz (56100d29f0cf860b171204fb181814bbd) dm-99 VMware,Virtual disk
```
