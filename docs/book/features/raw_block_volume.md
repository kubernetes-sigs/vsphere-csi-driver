# vSphere CSI Driver - Single-Access, Block Based Volume (Raw Block Volume)

[Raw Block Volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#raw-block-volume-support) feature in Kubernetes was promoted to stable in Kubernetes 1.18.
vSphere CSI Driver release `v2.3.0` onwards has Raw Block Volume feature released as `Alpha`. We do not recommend `Alpha` features for production use. Raw Block Volume feature is only supported for linux based nodes.

This feature allows persistent volumes to be exposed inside containers as a block device instead of as a mounted file system.

There are some specialized applications that require direct access to a block device because the file system layer introduces unneeded overhead.
The ability to use a raw block device without a filesystem will allow Kubernetes better support for high-performance applications that are capable of consuming and manipulating block storage for their needs. The most common case is databases (MongoDB, Cassandra) that require consistent I/O performance and low latency, which prefer to organize their data directly on the underlying storage.

## Creating a new raw block PVC

Create a storage class.

```yaml
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: example-raw-block-sc
provisioner: csi.vsphere.vmware.com
```

To request a raw block PersistentVolumeClaim, volumeMode = "Block" must be specified in the PersistentVolumeClaimSpec.
Raw Block Volume should be created using accessModes `ReadWriteOnce`. vSphere CSI Driver does not support creating raw block volume using `ReadWriteMany` accessModes.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: example-raw-block-pvc
spec:
  volumeMode: Block
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: example-raw-block-sc
```

## Using a raw block PVC

When you use the PVC in a pod definition, you get to choose the device path for the block device rather than the mount path for the file system.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: example-raw-block-pod
spec:
  containers:
    - name: test-container
      image: gcr.io/google_containers/busybox:1.24
      command: ["/bin/sh", "-c", "while true ; do sleep 2 ; done"]
      volumeDevices:
        - devicePath: /dev/xvda
          name: data
  restartPolicy: Never
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: example-raw-block-pvc
```
