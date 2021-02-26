# vSphere CSI Driver - Volume Expansion

CSI Volume Expansion was introduced as an alpha feature in Kubernetes 1.14 and it was promoted to beta in Kubernetes 1.16. The vSphere CSI driver supports volume expansion for dynamically/statically created **block** volumes only. Kubernetes supports two modes of volume expansion - offline and online. When the PVC is being used by a Pod i.e it is mounted on a node, the resulting volume expansion operation is termed as an online expansion. In all other cases, it is an offline expansion. Depending upon the kubernetes flavor and the mode of volume expansion required for your use case, refer to the table below to know the minimum version of the vSphere CSI driver to be used.

| vSphere CSI flavor    (minimum versions required)                                          | Vanilla                     |      Supervisor cluster                    | Tanzu Kubernetes Grid Service (TKGS) |
|-----------------------------------------------------|-----------------------------------|--------------------------------------|----------------------------|
| Offline volume expansion support                                               | vSphere CSI driver v2.0; vCenter 7.0; ESXi 7.0| vCenter 7.0U2; ESXi 7.0U2 | vCenter 7.0U1;  ESXi 7.0U1             |
|         Online volume expansion support                                            |    vSphere CSI driver v2.2; vCenter 7.0U2; ESXi 7.0U2           |    vCenter 7.0U2; ESXi 7.0U2                                  |     vCenter 7.0U2; ESXi 7.0U2                       |                       |

**NOTE**: vSphere CSI driver v2.2 is not yet released.

For more information, check the [supported features](../supported_features_matrix.md) section to verify if your environment conforms to all the required versions and the [known issues](../known_issues.md) section to see if this feature caters to your requirement.

## Feature Gate

Expand CSI Volumes feature was promoted to beta in kubernetes 1.16, therefore it is enabled by default. For Kubernetes releases before 1.16, ExpandCSIVolumes feature gate needs to be enabled for this feature to support volume expansion in CSI drivers.

## Sidecar Container

An external-resizer sidecar container implements the logic of watching the Kubernetes API for Persistent Volume claim edits, issuing the ControllerExpandVolume RPC call against a CSI endpoint and updating the PersistentVolume object to reflect the new size. This container has already been deployed for you as a part of the vsphere-csi-controller pod.

## Requirements

If you are either on the supervisor cluster or on TKGS, check if your environment adheres to the required kubernetes and vSphere CSI driver versions mentioned above and skip this section to directly proceed to the `Expand PVC` section below to use this feature.

However, in order to try this feature out on the vanilla kubernetes driver, you need to modify the StorageClass definition in your environment as mentioned below.

### StorageClass

Create a new StorageClass or edit the existing StorageClass to set `allowVolumeExpansion` to true.

```yaml
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: example-block-sc
provisioner: csi.vsphere.vmware.com
allowVolumeExpansion: true
```

Proceed to create/edit a PVC by using this storage class.

## Expand PVC

Prior to increasing the size of a PVC make sure that the PVC is in `Bound` state. If you are using a statically provisioned PVC, ensure that the PVC and the PV specs have the `storageClassName` parameter pointing to a storage class which has `allowVolumeExpansion` set to true.

### Online mode

Consider a scenario where you deployed a PVC with a StorageClass in which `allowVolumeExpansion` is set to `true` and then created a pod to use this PVC.

```bash
$ kubectl get pvc,pv,pod
NAME                                      STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS           AGE
persistentvolumeclaim/example-block-pvc   Bound    pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd   1Gi        RWO            example-block-sc       8m5s

NAME                                                        CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                       STORAGECLASS           REASON   AGE
persistentvolume/pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd   1Gi        RWO            Delete           Bound    default/example-block-pvc   example-block-sc                7m59s

NAME                    READY   STATUS    RESTARTS   AGE
pod/example-block-pod   1/1     Running   0          7m1s
```

Patch the PVC to increase its requested storage size (in this case, to `2Gi`):

```bash
$ kubectl patch pvc example-block-pvc -p '{"spec": {"resources": {"requests": {"storage": "2Gi"}}}}'
persistentvolumeclaim/example-block-pvc patched
```

This will trigger an expansion in the volume associated with the PVC in vSphere Cloud Native Storage.

The PVC and the PV will reflect the increase in size after the volume underneath has expanded. The `describe` output of the PVC will look similar to the following:

```bash
$ kubectl describe pvc example-block-pvc
Name:          example-block-pvc
Namespace:     default
StorageClass:  example-block-sc
Status:        Bound
Volume:        pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd
Labels:        <none>
Annotations:   pv.kubernetes.io/bind-completed: yes
               pv.kubernetes.io/bound-by-controller: yes
               volume.beta.kubernetes.io/storage-provisioner: csi.vsphere.vmware.com
Finalizers:    [kubernetes.io/pvc-protection]
Capacity:      2Gi
Access Modes:  RWO
VolumeMode:    Filesystem
Mounted By:    example-block-pod
Events:
  Type     Reason                      Age   From                                                                                                Message
  ----     ------                      ----  ----                                                                                                -------
  Normal   ExternalProvisioning        19m   persistentvolume-controller                                                                         waiting for a volume to be created, either by external provisioner "csi.vsphere.vmware.com" or manually created by system administrator
  Normal   Provisioning                19m   csi.vsphere.vmware.com_vsphere-csi-controller-5d8c5c7d6-9r9kv_7adc4efc-10a6-4615-b90b-790032cc4569  External provisioner is provisioning volume for claim "default/example-block-pvc"
  Normal   ProvisioningSucceeded       19m   csi.vsphere.vmware.com_vsphere-csi-controller-5d8c5c7d6-9r9kv_7adc4efc-10a6-4615-b90b-790032cc4569  Successfully provisioned volume pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd
  Warning  ExternalExpanding           75s   volume_expand                                                                                       Ignoring the PVC: didn't find a plugin capable of expanding the volume; waiting for an external controller to process this PVC.
  Normal   Resizing                    75s   external-resizer csi.vsphere.vmware.com                                                             External resizer is resizing volume pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd
  Normal   FileSystemResizeRequired    69s   external-resizer csi.vsphere.vmware.com                                                             Require file system resize of volume on node
  Normal   FileSystemResizeSuccessful  6s    kubelet, k8s-node-072                                                                               MountVolume.NodeExpandVolume succeeded for volume "pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd"
```

The PVC will go through events `Resizing` to `FileSystemResizeRequired` to finally `FileSystemResizeSuccessful`.

The PV will also reflect the expanded size.

```bash
$ kubectl get pv
NAME                                       CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                       STORAGECLASS           REASON   AGE
pvc-84c89bf9-8455-4633-a8c8-cd623e155dbd   2Gi        RWO            Delete           Bound    default/example-block-pvc   example-block-sc                25m
```

This marks the completion of the online volume expansion operation.

### Offline mode

Consider a scenario where you deployed a PVC with a StorageClass in which `allowVolumeExpansion` is set to `true`.

```bash
$ kubectl get pvc,pv
NAME                                      STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS           AGE
persistentvolumeclaim/example-block-pvc   Bound    pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   1Gi        RWO            example-block-sc       5m5s

NAME                                                        CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                       STORAGECLASS           REASON   AGE
persistentvolume/pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   1Gi        RWO            Delete           Bound    default/example-block-pvc   example-block-sc                5m18s
```

Patch the PVC to increase its requested storage size (in this case, to `2Gi`):

```bash
$ kubectl patch pvc example-block-pvc -p '{"spec": {"resources": {"requests": {"storage": "2Gi"}}}}'
persistentvolumeclaim/example-block-pvc patched
```

This will trigger an expansion in the volume associated with the PVC in vSphere Cloud Native Storage which finally gets reflected on the capacity of the corresponding PV object. Note that the capacity of the PVC will not change until the PVC is used by a Pod i.e mounted on a node.

```bash
$ kubectl get pv
NAME                                       CAPACITY ACCESS MODES RECLAIM POLICY STATUS   CLAIM                       STORAGECLASS           REASON AGE
pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   2Gi           RWO         Delete     Bound    default/example-block-pvc   example-block-sc              6m44s

$ kubectl get pvc
NAME                STATUS VOLUME                                     CAPACITY ACCESS MODES   STORAGECLASS       AGE
example-block-pvc   Bound  pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   1Gi           RWO       example-block-sc   6m57s
```

As you can see above, the capacity of the PVC is unchanged. You will also notice a `FilesystemResizePending` condition applied on the PVC when you `describe` it.

Now create a pod to use the PVC:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: example-block-pod
spec:
  containers:
  - name: test-container
    image: gcr.io/google_containers/busybox:1.24
    command: ["/bin/sh", "-c", "echo 'hello' > /mnt/volume1/index.html  && chmod o+rX /mnt /mnt/volume1/index.html && while true ; do sleep 2 ; done"]
    volumeMounts:
    - name: test-volume
      mountPath: /mnt/volume1
  restartPolicy: Never
  volumes:
  - name: test-volume
    persistentVolumeClaim:
      claimName: example-block-pvc
```

```bash
$ kubectl create -f example-pod.yaml
pod/example-block-pod created
```

The Kubelet on the node will trigger the filesystem expansion on the volume when the PVC is attached to the Pod.

```bash
$ kubectl get pod
NAME               READY STATUS  RESTARTS AGE
example-block-pod   1/1  Running 0        65s
```

```bash
$ kubectl get pvc
NAME                STATUS VOLUME                                    CAPACITY ACCESS MODES STORAGECLASS     AGE
example-block-pvc   Bound  pvc-24114458-9753-428e-9c90-9f568cb25788   2Gi         RWO      example-block-sc 2m12s

$ kubectl get pv
NAME                                       CAPACITY ACCESS MODES RECLAIM POLICY STATUS   CLAIM                     STORAGECLASS           REASON AGE
pvc-24114458-9753-428e-9c90-9f568cb25788   2Gi           RWO        Delete      Bound    default/example-block-pvc example-block-sc              2m3s
```

You will notice that the capacity of PVC has been modified and the `FilesystemResizePending` condition has been removed from the PVC. Offline volume expansion is complete.
