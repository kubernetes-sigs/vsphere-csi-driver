<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Known Issues

For complete list of issues please check our [Github issues](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues) page. If you notice an issue not listed in Github issues page, please do file an issue on the Github repository.

Please refer to release notes to learn known issues in each release.

Following listing is for issues observed in the Kubernetes.

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
