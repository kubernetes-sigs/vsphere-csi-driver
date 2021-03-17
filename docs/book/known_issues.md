<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Known Issues

Please refer to release notes to learn known issues in each release.

For complete list of issues please check our [Github issues](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file an issue on the Github repository.

Following listing is for issues observed in vSphere with Tanzu – Supervisor Cluster and vSphere with Tanzu – TKG Service ‘Guest’ Cluster and generic Kubernetes issues.

## vSphere 7.0u1

1. Supervisor devops can modify the volume health status of a PVC manually since the volume health annotation is not a read-only field. Devops should avoid modifying the volume health annotation manually. If DevOps modifies the volume health to a random or incorrect health status, then any software dependent on this volume health will be affected.
    - Impact: Any random volume health status set by the vSphere with Kubernetes Cluster Devops will get reflected in volume health status of PVC in Tanzu Kubernetes Grid Cluster as well.
    - Workaround: None
2. CnsRegisterVolume API does not validate if the volume to import is already imported or already present in the supervisor cluster
    - Impact: One of the PVC using the CNS volume will be usable at any point in time. Usage of any other PVC will lead to attach failures.
    - Workaround: None
3. Online Volume Expansion is not yet supported.
    - Impact: Users can resize the PVC and create a pod using that PVC simultaneously. In this case, pod creation might be completed first using the PVC with original size. Volume expansion will fail because online resize is not supported in vSphere 7.0 Update1.
    - Workaround: Wait for the PVC to reach FileVolumeResizePending condition before attaching a pod to it.
4. Supervisor devops can manually expand the PVC in Supervisor namespace in vSphere 7.0 Update1.
    - Impact: Supervisor devops can manually expand the PVC in Supervisor namespace, but the file system will not be expanded. It is not a supported use case and a current limitation now.
    - Workaround: Create a static PVC in Tanzu Kubernetes Grid Service Cluster using the SVC PVC, and expand the TKGS PVC to a size equal to or greater than the previously requested size and then attach to a Pod for the underlying filesystem to resize.

## Kubernetes issues

1. Performance regression in Vanilla Kubernetes 1.17, 1.18, 1.19 and Supervisor Cluster 7.0 patch releases.
   - Impact: Low throughput of attach and detach operations, especially at scale.
   - Upstream issue is tracked at: https://github.com/kubernetes/kubernetes/issues/84169
   - Workaround:
     - On Vanilla Kubernetes, for a short-term workaround, perform **either** of the following modifications in the kube-controller-manager manifest located at `/etc/kubernetes/manifests/kube-controller-manager.yaml` on each of your primary nodes:
       1. Set `--attach-detach-reconcile-sync-period` in spec.containers.command section to a value greater than `60s` depending upon the scale at which you are noticing this issue (higher the scale, higher the value) 
       2. Add `--disable-attach-detach-reconcile-sync` to the spec.containers.command section.      
     - Since kube-controller-manager is a static pod, Kubelet will restart it whenever a new flag is added. Make sure the kube-controller-manager pod is up and running.
