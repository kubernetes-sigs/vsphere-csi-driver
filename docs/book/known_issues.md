<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Known Issues

For complete list of issues please check our [Github issues](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file an issue on the Github repository.

Please refer to release notes to learn known issues in each release.

Following listing is for issues observed in the Kubernetes.

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

## Kubernetes 1.17 and 1.18 issues

1. Performance regression in Kubernetes 1.17 and 1.18
   - Impact: Low throughput of attach and detach operations, especially at scale.
   - Upstream issue is tracked at: https://github.com/kubernetes/kubernetes/issues/84169
   - Workaround:
     - Upgrade Kubernetes minor version to 1.17.8 and above or 1.18.5 and above. These versions contain the upstream fix for this issue.
     - If upgrading the Kubernetes version is not possible, then there is a workaround that can be applied on your Kubernetes cluster. On each primary node, perform the following steps:
       1. Open kube-controller-manager manifest, located at `/etc/kubernetes/manifests/kube-controller-manager.yaml`
       2. Add `--disable-attach-detach-reconcile-sync` to spec.containers.command
       3. Since kube-controller-manager is a static pod, Kubelet will restart it whenever a new flag is added. Make sure the kube-controller-manager pod is up and running.
