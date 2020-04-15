# vSphere CSI Driver - Offline Volume Expansion

CSI Volume Expansion was introduced as an alpha feature in Kubernetes 1.14 and it was promoted to beta in Kubernetes 1.16. The Vanilla vSphere CSI driver supports offline volume expansion which allows a volume to be extended when it is not attached to a node. Currently the support is available only for block volumes.

## Feature Gate

Expand CSI Volumes feature is promoted to beta in 1.16 so it is enabled by default. For Kubernetes release before 1.16, ExpandCSIVolumes feature gate needs to be enabled for this feature to support volume expansion in CSI drivers.

## Sidecar Container

An external-resizer sidecar container implements the logic for watching the Kubernetes API for Persistent Volume claim edits and issuing ControllerExpandVolume RPC call against a CSI endpoint and updating PersistentVolume object to reflect new size.

The resize sidecar container will be deployed in the vsphere-csi-controller pod.

If EXPAND_VOLUME node capability is implemented, NodeExpandVolume will be called by Kubelet on the node if NodeExpansionRequired is set to true in ControllerExpandVolumeResponse.

## RBAC Rules

The following  RBAC rules need to be added to support volume expansion:

```bash
- apiGroups: [""]
    resources: ["persistentvolumes"]
    # NOTE: added "patch"
    verbs: ["get", "list", "watch", "update", "create", "delete", "patch"]
- apiGroups: [""]
    # NOTE: added "persistentvolumeclaims/status"
    resources: ["persistentvolumeclaims/status"]
    verbs: ["update", "patch"]
```

## StorageClass

In the StorageClass, set allowVolumeExpansion to true.

```bash
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: example-vanilla-block-sc
  namespace: kube-system
  annotations:
    storageclass.kubernetes.io/is-default-class: "false"
provisioner: csi.vsphere.vmware.com
allowVolumeExpansion: true
parameters:
  # DatastoreURL: "ds:///vmfs/volumes/vsan:52cdfa80721ff516-ea1e993113acfc77/" # Optional Parameter
  # StoragePolicyName: "vSAN Default Storage Policy"  # Optional Parameter
```

## Expand PVC Example

Create a PVC with a StorageClass that allows volume expansion.

```bash
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: example-vanilla-block-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  storageClassName: example-vanilla-block-sc
```

```bash
kubectl apply -f example-pvc.yaml
```

Increase the size of the PVC. Note that PVC cannot be attached to a Pod as only offline volume expansion is supported.

```bash
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: example-vanilla-block-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 2Gi
  storageClassName: example-vanilla-block-sc
```

When the PVC is not used by any pod, modify the PVC configuration to trigger volume expansion.

```bash
kubectl apply -f example-pvc.yaml
```

The Cloud Native Volume size in vCenter will be changed to the new size. Size of PV will also be changed with that.

Note: The size of PVC won’t be changed until the PVC is used by a new Pod.

```bash
kubectl get pv
NAME                                       CAPACITY ACCESS MODES RECLAIM POLICY STATUS   CLAIM STORAGECLASS           REASON AGE
pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   2Gi        RWO       Delete   Bound default/example-vanilla-block-pvc   example-vanilla-block-sc 6m44s

kubectl get pvc
NAME                        STATUS VOLUME                         CAPACITY ACCESS MODES STORAGECLASS AGE
example-vanilla-block-pvc   Bound pvc-9e9a325d-ee1c-11e9-a223-005056ad1fc1   1Gi        RWO       example-vanilla-block-sc   6m57s
```

Note: Size of the PVC is unchanged. There is also a `FilesystemResizePending` condition on the PVC.

Create a pod to use the PVC.

```bash
apiVersion: v1
kind: Pod
metadata:
  name: example-vanilla-block-pod
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
      claimName: example-vanilla-block-pvc
```

Kubelet will trigger the volume expansion of filesystem when the PVC is used by the Pod.

```bash
kubectl create -f example-pod.yaml
pod/example-vanilla-block-pod created
```

```bash
kubectl get pod
NAME                        READY STATUS RESTARTS   AGE
example-vanilla-block-pod   0/1 ContainerCreating 0          37s

kubectl get pod
NAME                        READY STATUS RESTARTS AGE
example-vanilla-block-pod   1/1 Running 0 65s
```

```bash
kubectl get pvc
NAME                        STATUS VOLUME                         CAPACITY ACCESS MODES STORAGECLASS AGE
example-vanilla-block-pvc   Bound pvc-24114458-9753-428e-9c90-9f568cb25788   2Gi RWO example-vanilla-block-sc 2m12s

kubectl get pv
NAME                                       CAPACITY ACCESS MODES RECLAIM POLICY STATUS   CLAIM STORAGECLASS           REASON AGE
pvc-24114458-9753-428e-9c90-9f568cb25788   2Gi RWO Delete Bound    default/example-vanilla-block-pvc example-vanilla-block-sc            2m3s
```

Note: The size of PVC is changed to the new size. The `FilesystemResizePending` condition is removed from the PVC. Volume expansion is complete.

Note: the "allowVolumeExpansion" field in StorageClass can be modified after a StorageClass is created.
