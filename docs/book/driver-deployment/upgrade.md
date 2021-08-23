<!-- markdownlint-disable MD033 -->
# vSphere CSI Driver - Upgrade

**Note: To upgrade "vSphere with Tanzu" follow vSphere documentation. Following instructions are applicable to native vanilla Kubernetes Cluster deployed on vSphere.**

Before upgrading driver, please make sure to keep roles and privileges up to date as mentioned in the [roles and privileges requirement](https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/prerequisites.html#roles_and_privileges).
Each release of vSphere CSI driver has a different set of RBAC rules and deployment YAML files depending on the version of the vCenter on which Kubernetes Cluster is deployed.

## If you have RWM volumes backed by vSAN file service deployed using vSphere CSI Driver, please refer to the following steps before upgrading vSphere CSI Driver

vSAN file share volumes (Read-Write-Many/Read-Only-Many vSphere CSI Volumes) become inaccessible after upgrading the CSI driver. Refer to this [issue](https://github.com/kubernetes-sigs/vsphere-csi-driver/issues/1216) for more detail.
This issue is present in vSphere CSI Driver v2.0.0 to v2.2.1. Fix for this issue is provided in [v2.3.0](../releases/v2.3.0.md) release. New Pods with file share volumes created using v2.3.0 will not run into this issue.
The workaround for this issue is to remount vSAN file services volumes used by the application pods at the same location from the Node VM Guest OS directly. Please perform the following steps before upgrading the driver.

**Warning:** Following steps should be performed in a maintenance window and, it might disrupt active IOs on the file share volumes used application Pods. If you have multiple replicas of the Pod accessing the same file share volume, it is recommended to perform the below steps on each mount point serially to minimize downtime and disruptions.

1. Find all RWM volumes on the Cluster.

    ```bash
    # kubectl get pv -o wide | grep 'RWX\|ROX'
    pvc-7e43d1d3-2675-438d-958d-41315f97f42e   1Gi        RWX            Delete           Bound    default/www-web-0   file-sc                 107m   Filesystem
    ```

2. Find all Nodes to which RWM volume is attached. In here below example, we can see volume is attached and mounted on the `k8s-worker3` node.

    ```bash
    # kubectl get volumeattachment | grep pvc-7e43d1d3-2675-438d-958d-41315f97f42e
    csi-3afe670705e55e0679cba3f013c78ff9603333fdae6566745ea5f0cb9d621b20   csi.vsphere.vmware.com   pvc-7e43d1d3-2675-438d-958d-41315f97f42e   k8s-worker3   true       22s
    ```

3. Log in to the `k8s-worker3` node VM and execute the following command to know where the volume is mounted to.

    ```bash
    root@k8s-worker3:~# mount | grep pvc-7e43d1d3-2675-438d-958d-41315f97f42e
    10.83.28.38:/52d7e15c-d282-3bae-f64d-8851ad9d352c on /var/lib/kubelet/pods/43686ba4-d765-4378-807a-74049fca39ee/volumes/kubernetes.io~csi/pvc-7e43d1d3-2675-438d-958d-41315f97f42e/mount type nfs4 (rw,relatime,vers=4.1,rsize=1048576,wsize=1048576,namlen=255,hard,proto=tcp,timeo=600,retrans=2,sec=sys,clientaddr=10.244.3.134,local_lock=none,addr=10.83.28.38)
    ```

4. Unmount and remount volume at the same location with the same mount options used for mounting volume. For this step, the `nfs-common` package needs to be pre-installed on the worker VMs.

   1. Umount the volume
      - Use the `umount -fl` command to unmount the volume.

         ```bash
         root@k8s-worker3:~# umount -fl /var/lib/kubelet/pods/43686ba4-d765-4378-807a-74049fca39ee/volumes/kubernetes.io~csi/pvc-7e43d1d3-2675-438d-958d-41315f97f42e/mount
         ```

   2. Remount the volume with the same mount options used originally.

        ```bash
         root@k8s-worker3:~# mount -t nfs4 -o sec=sys,minorversion=1  10.83.28.38:/52d7e15c-d282-3bae-f64d-8851ad9d352c /var/lib/kubelet/pods/43686ba4-d765-4378-807a-74049fca39ee/volumes/kubernetes.io~csi/pvc-7e43d1d3-2675-438d-958d-41315f97f42e/mount
        ```

5. Confirm mount point is accessible from the Node VM

    ```bash
    root@k8s-worker3:~# mount | grep pvc-7e43d1d3-2675-438d-958d-41315f97f42e
    10.83.28.38:/52d7e15c-d282-3bae-f64d-8851ad9d352c on /var/lib/kubelet/pods/43686ba4-d765-4378-807a-74049fca39ee/volumes/kubernetes.io~csi/pvc-7e43d1d3-2675-438d-958d-41315f97f42e/mount type nfs4 (rw,relatime,vers=4.1,rsize=1048576,wsize=1048576,namlen=255,hard,proto=tcp,timeo=600,retrans=2,sec=sys,clientaddr=10.83.26.244,local_lock=none,addr=10.83.28.38)

    root@k8s-worker3:~# ls -la /var/lib/kubelet/pods/43686ba4-d765-4378-807a-74049fca39ee/volumes/kubernetes.io~csi/pvc-7e43d1d3-2675-438d-958d-41315f97f42e/mount
    total 4
    drwxrwxrwx 3 root root    0 Aug  9 16:48 .
    drwxr-x--- 3 root root 4096 Aug  9 18:40 ..
    -rw-r--r-- 1 root root    6 Aug  9 16:48 test
    ```

After all vSAN file share volumes are remounted on the worker VMs, you can proceed with upgrading the driver.

We are in the process of automating the above steps so until we have automation available we have to execute the workaround step mentioned above manually.

## Upgrade vSphere CSI Driver

To upgrade from one release to another release of vSphere CSI driver, we recommend to completely delete the driver using the original YAML files, you have used to deploy the driver and then use new YAML files to re-install the vSphere CSI driver.

To upgrade driver from `v2.0.0` release to `v2.3.0` release follow the process mentioned below.

### Uninstall `v2.0.0` driver

```bash
kubectl delete -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.0/vsphere-67u3/deploy/vsphere-csi-controller-deployment.yaml
kubectl delete -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.0/vsphere-67u3/deploy/vsphere-csi-node-ds.yaml
kubectl delete -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.0/vsphere-67u3/rbac/vsphere-csi-controller-rbac.yaml
```

Wait for vSphere CSI Controller Pod and vSphere CSI Nodes Pods to be completely deleted.

### Install `v2.3.0` driver

Refer to the [release notes](../releases/v2.3.0.md) of `v2.3.0` for detail.

#### Pre-requiresites for `v2.3.0`

`v2.3.0` release of the driver requires DNS forwarding configuration in CoreDNS ConfigMap to help resolve vSAN file share hostname.
Follow steps mentioned [here](https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/prerequisites.html#coredns) before installing v2.3.0 release of the driver.

#### Install v2.3.0 release of the driver

1. Create a new namespace for vSphere CSI Driver.

   ```bash
   kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/namespace.yaml
   ```

2. Copy `vsphere-config-secret` secret from `kube-system` namespace to the new namespace `vmware-system-csi`.

   ```bash
   kubectl get secret vsphere-config-secret --namespace=kube-system -o yaml | sed 's/namespace: .*/namespace: vmware-system-csi/' | kubectl apply -f -
   ```

3. Delete `vsphere-config-secret` secret from `kube-system` namespace.

   ```bash
   kubectl delete secret vsphere-config-secret --namespace=kube-system
   ```

4. Install Driver

   ```bash
   kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/vsphere-csi-driver.yaml
   ```

We also recommend you to follow our [installation guide](installation.md) to refer to updated instruction and encourage you to go through all [pre-requisites](prerequisites.md) for newer releases of the vSphere CSI driver.
