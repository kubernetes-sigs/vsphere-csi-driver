# vSphere CSI Driver - Offline Volume Expansion

CSI Volume Expansion was introduced as an alpha feature in Kubernetes 1.14 and it was promoted to beta in Kubernetes 1.16. The vSphere CSI driver currently extends this support for dynamically/statically created offline block volumes only i.e allows a block volume to be extended when it is not attached to a node. Check the [supported features](../supported_features_matrix.md) section to verify if your environment conforms to all the required versions. Note that offline volume expansion is available from vSphere CSI v2.0 onwards in Vanilla Kubernetes and v2.1 onwards in Tanzu Kubernetes Grid Service (TKGS). Volume expansion is currently not supported in the Supervisor cluster.

## Feature Gate

Expand CSI Volumes feature was promoted to beta in kubernetes 1.16, therefore it is enabled by default. For Kubernetes releases before 1.16, ExpandCSIVolumes feature gate needs to be enabled for this feature to support volume expansion in CSI drivers.

## Sidecar Container

An external-resizer sidecar container implements the logic of watching the Kubernetes API for Persistent Volume claim edits, issuing the ControllerExpandVolume RPC call against a CSI endpoint and updating the PersistentVolume object to reflect the new size. This container has already been deployed for you as a part of the vsphere-csi-controller pod.

## Requirements

If you are using TKGS and your environment adheres to the required kubernetes and vSphere CSI driver versions mentioned above skip this section and directly proceed to the `Expand PVC Example` section below to use this feature.

However, in order to try out this feature using the vanilla kubernetes driver, modify the StorageClass definition as mentioned below in your environment.

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

## Expand PVC Example

Prior to increasing the size of a PVC make sure that the PVC is bound and is not attached to a Pod as only offline volume expansion is supported.

Patch the PVC to increase its request size:

```bash
kubectl patch pvc example-block-pvc -p '{"spec": {"resources": {"requests": {"storage": "2Gi"}}}}'
```

This will trigger an expansion in the volume associated with the PVC in vSphere Cloud Native Storage which finally gets reflected on the capacity of the corresponding PV object. Note that the capacity of PVC will not change until the PVC is attached to a node i.e used by a Pod.

```bash
kubectl get pv
NAME                                       CAPACITY ACCESS MODES RECLAIM POLICY STATUS   CLAIM                       STORAGECLASS           REASON AGE
pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   2Gi           RWO         Delete     Bound    default/example-block-pvc   example-block-sc              6m44s

kubectl get pvc
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
kubectl create -f example-pod.yaml
pod/example-block-pod created
```

The Kubelet on the node will trigger the filesystem expansion on the volume when the PVC is attached to the Pod.

```bash
kubectl get pod
NAME               READY STATUS  RESTARTS AGE
example-block-pod   1/1  Running 0        65s
```

```bash
kubectl get pvc
NAME                STATUS VOLUME                                    CAPACITY ACCESS MODES STORAGECLASS     AGE
example-block-pvc   Bound  pvc-24114458-9753-428e-9c90-9f568cb25788   2Gi         RWO      example-block-sc 2m12s

kubectl get pv
NAME                                       CAPACITY ACCESS MODES RECLAIM POLICY STATUS   CLAIM                     STORAGECLASS           REASON AGE
pvc-24114458-9753-428e-9c90-9f568cb25788   2Gi           RWO        Delete      Bound    default/example-block-pvc example-block-sc              2m3s
```

You will notice that the capacity of PVC has been modified and the `FilesystemResizePending` condition has been removed from the PVC. Volume expansion is complete.
