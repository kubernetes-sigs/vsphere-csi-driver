# vSphere CSI Driver - Volume Health

Volume Health feature provides the health status of persistent volume. Volume Health is supported for Block Volumes in vSphere with Kubernetes Cluster and Tanzu Kubernetes Grid Cluster releases for vSphere 7.0 Update 1.

## Health Status of Persistent Volume

Volume Health reports the health status for Persistent Volumes. For each Persistent Volume in `bound` state, the health status of this Persistent Volume will be exposed as an annotation of Persistent Volume Claim which is bound to the Persistent Volume. The key of the annotation is `volumehealth.storage.kubernetes.io/messages:`. There are two possible values for health status.

1. `accessible`:  The Persistent Volume is accessible and avaialbe for use.
2. `inaccessible`: The Persistent Volume is inaccessible, user should avoid using this Persistent Volume. Persistent Volume goes to `inaccessible` if the datastore that hosts the Persistent Volume is not accessible from all hosts on which the datastore is present.

Volume Health status is refreshed every 5 minutes.

## Volume Health Status Example

Create a PVC in the Tanzu Kubernetes Grid Cluster.

```bash
cat example-pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: example-block-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Mi
  storageClassName: gc-storage-profile
```

```bash
kubectl apply -f example-pvc.yaml
```

After the PVC in the Tanzu Kubernetes Grid Cluster is bound to the PV, check the corresponding PVC in the vSphere with Kubernetes Cluster to make sure that PVC is also bound to a PV.

```bash
kubectl get pvc --all-namespaces
NAMESPACE             NAME                                                                        STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS         AGE
test-gc-e2e-demo-ns   8dee9083-cf01-4ab0-a1a7-b6fce5e17133-07367416-d8b9-4a6d-922e-79362ef08277   Bound    pvc-c6f6e1a9-66e6-405c-b9d5-f2716c5630b6   1Mi        RWO            gc-storage-profile   4m53s
```

Check the volume health annotation on the above PVC, it shows `accessible`.

```bash
kubectl describe pvc 8dee9083-cf01-4ab0-a1a7-b6fce5e17133-07367416-d8b9-4a6d-922e-79362ef08277 -n test-gc-e2e-demo-ns
Name:          8dee9083-cf01-4ab0-a1a7-b6fce5e17133-07367416-d8b9-4a6d-922e-79362ef08277
Namespace:     test-gc-e2e-demo-ns
StorageClass:  gc-storage-profile
Status:        Bound
Volume:        pvc-c6f6e1a9-66e6-405c-b9d5-f2716c5630b6
Labels:        <none>
Annotations:   pv.kubernetes.io/bind-completed: yes
               pv.kubernetes.io/bound-by-controller: yes
               volume.beta.kubernetes.io/storage-provisioner: csi.vsphere.vmware.com
               volumehealth.storage.kubernetes.io/messages: accessible
Finalizers:    [kubernetes.io/pvc-protection]
Capacity:      1Mi
Access Modes:  RWO
VolumeMode:    Filesystem
Mounted By:    <none>
...
```

Then check the volume health annotation on the PVC in Tanzu Kubernetes Grid Cluster, it also shows `accessible`.

```bash
kubectl describe pvc example-block-pvc
Name:          example-block-pvc
Namespace:     default
StorageClass:  gc-storage-profile
Status:        Bound
Volume:        pvc-07367416-d8b9-4a6d-922e-79362ef08277
Labels:        <none>
Annotations:   pv.kubernetes.io/bind-completed: yes
               pv.kubernetes.io/bound-by-controller: yes
               volume.beta.kubernetes.io/storage-provisioner: csi.vsphere.vmware.com
               volumehealth.storage.kubernetes.io/messages: accessible
Finalizers:    [kubernetes.io/pvc-protection]
Capacity:      1Mi
Access Modes:  RWO
VolumeMode:    Filesystem
Mounted By:    <none>
...
```
