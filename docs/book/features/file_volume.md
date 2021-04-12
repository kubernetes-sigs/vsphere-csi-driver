# vSphere CSI Driver - File Volume

The vSphere Vanilla CSI driver version 2.0 and above supports file volumes backed by vSAN File shares to be statically/dynamically created and mounted by stateful containerized applications. This feature allows you to reference the same shared data among multiple pods spread across different clusters making it an absolute necessity for applications that need shareability.

Before you proceed, keep in mind that the file volumes feature doesn't work in conjunction with the topology aware zones, extend volume and encryption features.

Proceed to the requirements section below to enable this feature in your environment.

## Requirements

Check the [CSI vSphere compatibility matrix](../compatiblity_matrix.md) and the [supported features](../supported_features_matrix.md) section to verify if your environment conforms to all the required versions. Also check the [limits](../limits.md) section to understand if this feature can cater to your needs.
In order to utilize this feature in your vSphere environment, you need to make sure of the following:

- Enable and configure the file service in your vSAN cluster configuration. You must configure the necessary file service domains, IP pools, network etc in order to create file share volumes. Refer to [vSphere 7.0 vSAN File service](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.vsan.doc/GUID-82565B82-C911-42F7-85B1-E9EF973EE90C.html) documentation to get started.

- Establish a dedicated file share network connecting all the kubernetes nodes and make sure this network is routable to the vSAN File Share network.  Refer to [Network Access of vSAN File Share](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.storage.doc/GUID-EFC00FFF-E720-44F1-B229-4C13687E6B85.html) to understand the setup better.

- Configure the kubernetes nodes to use the DNS server which was used to configure the file services in the vSAN cluster. This will help the nodes resolve the file share access points with Fully Qualified Domain Name (FQDN) while mounting the file volume to the Pod. In order to retrieve the vSAN file service DNS configuration, head over to the `Configure` tab of your vSAN cluster and navigate to the File Service section as shown in the screenshot below.

![CONFIGURE-FILE-SERVICES](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/LOCATE-FILE-SERVICE-FQDN.png)

- Configure the kubernetes secret named `vsphere-config-secret` to specify network permissions and placement of volumes for your vSAN file shares in your vSphere environment. This step is completely optional. Refer to the [CSI vSphere driver installation](../driver-deployment/installation.md) instructions on `vSphere configuration file for file volumes` to learn more. If not specified, it is upto the CSI driver to use its discretion to place the file share volumes in any of your vSAN datastores with File services enabled. In such a scenario, the file volumes backed by vSAN file shares using the NFSv4 protocol will assume default network permissions i.e Read-Write privilege and root access to all the IP ranges.

Before you start using file services in your environment keep in mind that if you have file shares being shared across more than one clusters in your vCenter, deleting a PVC with reclaim policy set to `Delete` in any one cluster may delete the underlying file share causing the volume to be unavailable for the rest of the clusters.

After you are done setting up file services, to get started with vSphere CSI file services integration with your applications, check this [video](https://youtu.be/GUtG-4urGFA). We have also provided few [Try-out YAMLs](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/example/vanilla-k8s-file-driver) for Storage Class, PersistentVolumeClaim, PersistentVolume and Pod specs for your convenience.

The next section on CSI file services integration will explain some of the spec changes mandatory for file share volumes.

## File services integration with your application

### Dynamic Provisioning of file volumes

To give this example a try, you can first pick the Storage Class spec from [here](https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/master/example/vanilla-k8s-file-driver/example-sc.yaml). To create a file volume PVC spec, set `accessModes` to either `ReadWriteMany` or `ReadOnlyMany` depending upon your requirement.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: example-vanilla-file-pvc
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: example-vanilla-file-sc
 ```

```bash
kubectl apply -f example-pvc.yaml
```

After the PVC is bound, if you describe the corresponding PV it will look similar to this:

```bash
Name:            pvc-45cea491-8399-11ea-883a-005056b61591
Labels:          <none>
Annotations:     pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com
Finalizers:      [kubernetes.io/pv-protection]
StorageClass:    example-vanilla-file-sc
Status:          Bound
Claim:           default/example-vanilla-file-pvc
Reclaim Policy:  Delete
Access Modes:    RWX
VolumeMode:      Filesystem
Capacity:        1Gi
Node Affinity:   <none>
Message:
Source:
    Type:              CSI (a Container Storage Interface (CSI) volume source)
    Driver:            csi.vsphere.vmware.com
    VolumeHandle:      file:53bf6fb7-fe9f-4bf8-9fd8-7a589bf77760
    ReadOnly:          false
    VolumeAttributes:      storage.kubernetes.io/csiProvisionerIdentity=1587430348006-8081-csi.vsphere.vmware.com
                           type=vSphere CNS File Volume
Events:                <none>
```

The `VolumeHandle` associated with the PV should have a prefix of `file:` for file volumes.

### Pod with Read-Write access to PVC

Create a Pod to use the PVC from above example.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: example-vanilla-file-pod1
spec:
  containers:
    - name: test-container
      image: gcr.io/google_containers/busybox:1.24
      command: ["/bin/sh", "-c", "echo 'Hello! This is Pod1' >> /mnt/volume1/index.html && while true ; do sleep 2 ; done"]
      volumeMounts:
        - name: test-volume
          mountPath: /mnt/volume1
  restartPolicy: Never
  volumes:
    - name: test-volume
      persistentVolumeClaim:
        claimName: example-vanilla-file-pvc
```

If you need to read the same file share from multiple pods, specify the PVC associated with the file share in the `ClaimName` in all the Pod specifications.

### Pod with Read-Only access to PVC

If you need to create a Pod with Read-Only access to the above PVC, you need to explicitly mention `readOnly` as `true` in the `persistentVolumeClaim` section as shown below. Note that just setting the `accessModes` to `ReadOnlyMany` in the PVC spec will not make the PVC read-only to the Pods.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: example-vanilla-file-pod2
spec:
  containers:
    - name: test-container
      image: gcr.io/google_containers/busybox:1.24
      command: ["/bin/sh", "-c", "while true ; do sleep 2 ; done"]
      volumeMounts:
        - name: test-volume
          mountPath: /mnt/volume1
  restartPolicy: Never
  volumes:
    - name: test-volume
      persistentVolumeClaim:
        claimName: example-vanilla-file-pvc
        readOnly: true
```

If you `exec` into this pod and try to create a file in the `mountPath` which is `/mnt/volume1`, you will get an error.

```bash
$ kubectl exec -it example-vanilla-file-pod2 -c test-container -- /bin/sh
/ # cd /mnt/volume1/
/mnt/volume1 # touch abc.txt
touch: abc.txt: Read-only file system
```

### Static Provisioning for file volumes

If you have an existing persistent storage file volume in your VC, you can use static provisioning to make the storage instance available to your cluster.
Define a PVC and a PV as shown below

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: static-file-share-pv-name
  annotations:
    pv.kubernetes.io/provisioned-by: csi.vsphere.vmware.com
  labels:
    "static-pv-label-key": "static-pv-label-value"
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  csi:
    driver: "csi.vsphere.vmware.com"
    volumeAttributes:
      type: "vSphere CNS File Volume"
    "volumeHandle": "file:236b3e6b-cfb0-4b73-a271-2591b2f31b4c"
---
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: static-file-share-pvc-name
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  selector:
    matchLabels:
      static-pv-label-key: static-pv-label-value
  storageClassName: ""
```

The `labels` key-value pair `static-pv-label-key: static-pv-label-value` used in PV `metadata` and PVC `selector` aid in matching the PVC to the PV during static provisioning. Also, remember to retain the `file:` prefix of the vSAN file share while filling up the `volumeHandle` field in PV spec.

**NOTE:** For File volumes, CNS supports multiple PV's referring to the same file-share volume.
