<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Volume Snapshot & Restore

- [Introduction](#introduction)
- [Prerequisite](#prereq)
- [How to enable Volume Snapshot & Restore feature in vSphere CSI](#how-to-deploy)
- [How to use Volume Snapshot & Restore feature](#how-to-use)
- [Configuration - Maximum Number of Snapshots per Volume](#config-param)

## Introduction <a id="introduction"></a>

CSI Volume Snapshot & Restore feature was introduced as an alpha feature in Kubernetes 1.12, promoted to beta in Kubernetes 1.17 and moved to GA in Kubernetes 1.20.
Volume Snapshot & Restore feature will be added in vSphere CSI driver 2.4 as an Alpha feature.

Known limitations for the Alpha feature in vSphere CSI driver 2.4 are listed below.

1. It is only supported in ReadWriteOnce volumes based on First Class Disk, i.e., FCD or CNS block volume, while not yet supported in ReadWriteMany volumes based on vSAN file service.
2. It is only supported in [Vanilla Kubernetes](https://github.com/kubernetes/kubernetes) cluster now, while not yet supported in either [vSphere with Kubernetes](https://blogs.vmware.com/vsphere/2019/08/introducing-project-pacific.html) cluster aka Supervisor Cluster or [Tanzu Kubernetes Grid Service](https://blogs.vmware.com/vsphere/2020/03/vsphere-7-tanzu-kubernetes-clusters.html) cluster aka Guest Cluster.
3. Volume restore can only create a PVC with the same storage capacity as the source VolumeSnapshot.
4. vSphere CSI introduces a constraint on the maximum number of snapshots per ReadWriteOnce volume. The maximum is configurable but set to 3 by default. Please refer to the section of [Configuration - Maximum Number of Snapshots per Volume](#config-param) for more detail.
5. It is not supported to expand/delete volumes with snapshots.
6. It is not supported to snapshot CSI migrated volumes.

## Prerequisite <a id="prereq"></a>

In addition to prerequisites mentioned [here](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/2.0/vmware-vsphere-csp-getting-started/GUID-0AB6E692-AA47-4B6A-8CEA-38B754E16567.html), following prerequisites to be fulfilled to support Volume Snapshot & Restore feature in vSphere CSI:

1. Minimum kubernetes version required is 1.20.
2. Minimum CSI upstream external-snapshotter/snapshot-controller version required is 4.1.
3. Minimum vSphere CSI driver version required is 2.4.
4. Minimum vSphere version required is 7.0U3. (The minimum version applies to both vCenter version and ESXi version)

## How to enable Volume Snapshot & Restore feature in vSphere CSI <a id="how-to-deploy"></a>

- Install vSphere CSI driver 2.4 by following https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/installation.html
- To enable Volume Snapshot feature, patch the configmap to enable `block-volume-snapshot` feature switch by running following command:
  
  ```bash
  $ kubectl patch configmap/internal-feature-states.csi.vsphere.vmware.com \
  -n vmware-system-csi \
  --type merge \
  -p '{"data":{"block-volume-snapshot":"true"}}'
  ```

- To deploy required components for CSI volume snapshot feature, the following script is available for an easy deployment.
To get to know the step-by-step workflow of the script, please check out using the command `bash deploy-csi-snapshot-components.sh -h`.

  ```bash
  $ wget https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.4.0/manifests/vanilla/deploy-csi-snapshot-components.sh
  $ bash deploy-csi-snapshot-components.sh
  ✅ Verified that block-volume-snapshot feature is enabled
  ...
  ✅ Successfully deployed all components for CSI Snapshot feature.
  ```
  
  Below is the expected view in `vmware-system-csi` namespace when the deployment is completed in a single-master cluster:
  
  ```bash
  $ kubectl -n vmware-system-csi get pod,deploy
  NAME                                          READY   STATUS    RESTARTS   AGE
  pod/vsphere-csi-controller-6c46964474-bcx5t   7/7     Running   0          164m
  pod/vsphere-csi-node-8pspp                    3/3     Running   0          3h55m
  pod/vsphere-csi-node-lgthd                    3/3     Running   0          3h55m
  pod/vsphere-csi-node-nzvx8                    3/3     Running   0          3h55m
  pod/vsphere-csi-node-x4zch                    3/3     Running   0          3h55m
  
  NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
  deployment.apps/vsphere-csi-controller   1/1     1            1           3h55m
  ```
  
  At this point, you are good to try out CSI Volume Snapshot & Restore feature in vSphere CSI driver.

## How to use Volume Snapshot & Restore feature <a id="how-to-use"></a>

To use volume snapshot and restore feature in vSphere CSI, please refer to example yaml files for FileSystem volumes, [vanilla-k8s-RWO-filesystem-volumes](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/example/vanilla-k8s-RWO-filesystem-volumes), and example yaml files for Raw Block volumes, [vanilla-k8s-RWO-Block-Volumes](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/example/vanilla-k8s-RWO-Block-Volumes). Below is an example for FileSystem volumes.

### Volume Snapshot

#### Dynamic-provisioned Snapshot

Below is an example StorageClass yaml from [vanilla-k8s-RWO-filesystem-volumes/example-sc.yaml](https://github.com/kubernetes-sigs/vsphere-csi-driver/blob/master/example/vanilla-k8s-RWO-filesystem-volumes/example-sc.yaml), with optional parameters being commented out.

```yaml
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: example-vanilla-rwo-filesystem-sc
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"  # Optional
provisioner: csi.vsphere.vmware.com
allowVolumeExpansion: true  # Optional: only applicable to vSphere 7.0U1 and above
#parameters:
#  datastoreurl: "ds:///vmfs/volumes/vsan:52cdfa80721ff516-ea1e993113acfc77/"  # Optional Parameter
#  storagepolicyname: "vSAN Default Storage Policy"  # Optional Parameter
#  csi.storage.k8s.io/fstype: "ext4"  # Optional Parameter
```

Create a StorageClass.

```bash
$ kubectl apply -f example-sc.yaml
$ kubectl get sc
NAME                                          PROVISIONER              RECLAIMPOLICY   VOLUMEBINDINGMODE   ALLOWVOLUMEEXPANSION   AGE
example-vanilla-rwo-filesystem-sc (default)   csi.vsphere.vmware.com   Delete          Immediate           true                   2s
```

Create a PVC:

```bash
$ kubectl apply -f example-pvc.yaml
$ kubectl get pvc
NAME                      STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS                        AGE
example-vanilla-rwo-pvc   Bound    pvc-2dc37ea0-dee0-4ad3-96ca-82f0159d7532   5Gi        RWO            example-vanilla-rwo-filesystem-sc   7s
```

Create a VolumeSnapshotClass:

```bash
$ kubectl apply -f example-snapshotclass.yaml
$ kubectl get volumesnapshotclass
NAME                                           DRIVER                   DELETIONPOLICY   AGE
example-vanilla-rwo-filesystem-snapshotclass   csi.vsphere.vmware.com   Delete           4s
```

Create a VolumeSnapshot:

```bash
$ kubectl apply -f example-snapshot.yaml
$ kubectl get volumesnapshot
NAME                                      READYTOUSE   SOURCEPVC                 SOURCESNAPSHOTCONTENT   RESTORESIZE   SNAPSHOTCLASS                                  SNAPSHOTCONTENT                                    CREATIONTIME   AGE
example-vanilla-rwo-filesystem-snapshot   true         example-vanilla-rwo-pvc                           5Gi           example-vanilla-rwo-filesystem-snapshotclass   snapcontent-a7c00b7f-f727-4010-9b1a-d546df9a8bab   57s            58s
```

#### Static-provisioned Snapshot

Below are prerequisites for creating static-provisioned snapshot:

1. Make sure a FCD snapshot on which the static-provisioned snapshot is created is available in your vSphere.
2. Construct the snapshotHandle based on the combination of FCD Volume ID and FCD Snapshot ID of the snapshot. For example, FCD Volume ID and FCD Snapshot ID of a FCD snapshot are `4ef058e4-d941-447d-a427-438440b7d306` and `766f7158-b394-4cc1-891b-4667df0822fa`. Then, the constructed snapshotHandle is `4ef058e4-d941-447d-a427-438440b7d306+766f7158-b394-4cc1-891b-4667df0822fa`.
3. Update the `spec.source.snapshotHandle` field in the VolumeSnapshotContent object of example-static-snapshot.yaml with the constructed snapshotHandle in step 2.

Create a static-provisioned VolumeSnapshot:

```bash
$ kubectl apply -f example-static-snapshot.yaml
$ kubectl get volumesnapshot static-vanilla-rwo-filesystem-snapshot
NAME                                     READYTOUSE   SOURCEPVC   SOURCESNAPSHOTCONTENT                           RESTORESIZE   SNAPSHOTCLASS   SNAPSHOTCONTENT                                 CREATIONTIME   AGE
static-vanilla-rwo-filesystem-snapshot   true                     static-vanilla-rwo-filesystem-snapshotcontent   5Gi                           static-vanilla-rwo-filesystem-snapshotcontent   76m            22m
```

### Volume Restore

Make sure the VolumeSnapshot to be restored is available in the current Kubernetes cluster.

```bash
$ kubectl get volumesnapshot
NAME                                      READYTOUSE   SOURCEPVC                 SOURCESNAPSHOTCONTENT   RESTORESIZE   SNAPSHOTCLASS                                  SNAPSHOTCONTENT                                    CREATIONTIME   AGE
example-vanilla-rwo-filesystem-snapshot   true         example-vanilla-rwo-pvc                           5Gi           example-vanilla-rwo-filesystem-snapshotclass   snapcontent-a7c00b7f-f727-4010-9b1a-d546df9a8bab   22m            22m
```

Create a PVC from a VolumeSnapshot:

```bash
$ kubectl create -f example-restore.yaml
$ kubectl get pvc
NAME                                     STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS                        AGE
example-vanilla-rwo-filesystem-restore   Bound    pvc-202c1dfc-78be-4835-89d5-110f739a87dd   5Gi        RWO            example-vanilla-rwo-filesystem-sc   78s
```

## Configuration - Maximum Number of Snapshots per Volume <a id="config-param"></a>

Per the [best practices for using VMware snapshots](https://kb.vmware.com/s/article/1025279), it is recommended to use only 2 to 3 snapshots per virtual disk for a better performance.
So, we make the global constraint, i.e., maximum number of snapshots per volume, configurable and meanwhile set the default to 3.
Additionally, the best-practice guideline only applies to virtual disks on VMFS and NFS datastores while not to those on VVOL and VSAN.
Therefore, we also introduces granular configuration parameters on the constraint, apart from the global configuration parameter.

Below are configuration parameters available:

- `global-max-snapshots-per-block-volume`: Global configuration parameter that applies to volumes on all kinds of datastores. By default, it is set to 3.
- `granular-max-snapshots-per-block-volume-vsan`: Granular configuration parameter on VSAN datastore only. It overrides the global constraint if set, while it falls back to the global constraint if unset.
- `granular-max-snapshots-per-block-volume-vvol`: Granular configuration parameter on VVOL datastore only. It overrides the global constraint if set, while it falls back to the global constraint if unset.

**Note**: Users only need to configure it when the default constraint does not work for their user cases. For others, just skip the configuration below.

Here is an example of vSphere CSI about how to configure the constraints. Firstly, delete the Secret that stores vSphere config. (Kubernetes doesn't allow to update Secret resources in place)

```bash
kubectl delete secret vsphere-config-secret --namespace=vmware-system-csi
```

Secondly, update the config file of vSphere CSI and add configuration parameters for snapshot feature under the `[Snapshot]` section.

```bash
$ cat /etc/kubernetes/csi-vsphere.conf
[Global]
...

[Snapshot]
global-max-snapshots-per-block-volume = 5 # optional, set to 3 if unset
granular-max-snapshots-per-block-volume-vsan = 7 # optional, fall back to the global constraint if unset
granular-max-snapshots-per-block-volume-vvol = 8 # optional, fall back to the global constraint if unset
...
```

Finally, create a new Secret with the updated config file.

```bash
kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf --namespace=vmware-system-csi
```
