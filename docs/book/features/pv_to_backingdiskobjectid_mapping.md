<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# PV to BackingDiskObjectId Mapping

- [Introduction](#introduction)
- [Prerequisite](#prereq)
- [How to enable PV to BackingDiskObjectId Mapping feature in vSphere CSI](#how-to-enable)
- [How to use PV to BackingDiskObjectId Mapping feature](#how-to-use)
- [Known limitations](#limitations)

## Introduction <a id="introduction"></a>

PV to BackingDiskObjectId Mapping feature will be added in vSphere CSI driver 2.5 as an Alpha feature. This feature adds PV to BackingDiskObjectId mapping as an annotation on PVC.

## Prerequisite <a id="prereq"></a>

- vSphere version required is 7.0 U2 or above.

## How to enable PV to BackingDiskObjectId Mapping feature in vSphere CSI <a id="how-to-enable"></a>

- To enable PV to BackingDiskObjectId Mapping feature, patch the configmap to enable `pv-to-backingdiskobjectid-mapping` feature switch by running following command:
  
  ```bash
  $ kubectl patch configmap/internal-feature-states.csi.vsphere.vmware.com \
  -n vmware-system-csi \
  --type merge \
  -p '{"data":{"pv-to-backingdiskobjectid-mapping":"true"}}'
  ```
  
  Verify that `pv-to-backingdiskobjectid-mapping` is enabled:
  
  ```bash
  # kubectl edit configmap internal-feature-states.csi.vsphere.vmware.com -n vmware-system-csi
  apiVersion: v1
  data:
    ...
    pv-to-backingdiskobjectid-mapping: "true"
    ...
  ```
  
## How to use PV to BackingDiskObjectId Mapping feature <a id="how-to-use"></a>

- Here is an example of the vsphere-csi-driver deployment:

  ```bash
    $ kubectl -n vmware-system-csi get pod,deploy
    NAME                                          READY   STATUS    RESTARTS   AGE
    pod/vsphere-csi-controller-5547b8bd54-bdtwz   6/6     Running   0          6h40m
    pod/vsphere-csi-node-l4zmm                    3/3     Running   0          6h44m
    pod/vsphere-csi-node-m2l7x                    3/3     Running   0          6h44m
    pod/vsphere-csi-node-phdqk                    3/3     Running   0          6h44m
    pod/vsphere-csi-node-r7r2v                    3/3     Running   0          6h44m
    pod/vsphere-csi-node-rzk9q                    3/3     Running   0          6h44m
    pod/vsphere-csi-node-sg56b                    3/3     Running   0          6h44m

    NAME                                     READY   UP-TO-DATE   AVAILABLE   AGE
    deployment.apps/vsphere-csi-controller   1/1     1            1           15h
  ```

- After `pv-to-backingdiskobjectid-mapping` feature is enabled, restart vsphere-csi-controller pod.

  ```bash
  kubectl -n vmware-system-csi scale deploy/vsphere-csi-controller --replicas=0
  kubectl -n vmware-system-csi scale deploy/vsphere-csi-controller --replicas=1
  ```  

- Check PVC annotation to find the pv to backingdiskobjectid mapping info:

  ```yaml
  root@k8-master-657:~# kubectl get pvc -n test-ns-swmiznp -o yaml
  apiVersion: v1
  items:
  - apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      annotations:
        cns.vmware.com/pv-to-backingdiskobjectid-mapping: 61ffa393-b5da-4963-a4c0-7ead9b3696c0:6795fc61-3458-fe61-5e85-020078562642
        kubectl.kubernetes.io/last-applied-configuration: |
          {"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"annotations":{},"name":"etcd0-pv-claim","namespace":"test-ns-swmiznp"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}},"storageClassName":"kibishii-storage-class"}}
        pv.kubernetes.io/bind-completed: "yes"
        pv.kubernetes.io/bound-by-controller: "yes"
        volume.beta.kubernetes.io/storage-provisioner: csi.vsphere.vmware.com
      creationTimestamp: "2022-02-04T02:54:07Z"
  ```

  In this example, PV is provisioned on vSAN datastore:

  - PV UID: 61ffa393-b5da-4963-a4c0-7ead9b3696c0
  - BackingDiskObjectId: 6795fc61-3458-fe61-5e85-020078562642

## Known limitations <a id="limitations"></a>

Known limitations for the Alpha feature in vSphere CSI driver 2.5 are listed below.

1. Retrieving backingDiskObjectId is only supported for vVOl and vSAN, so it is only supported for PVs that are provisioned on vSAN and vVOL datastores.
2. It is only supported in vanilla cluster, currently it will not be supported in Supervisor/Guest cluster.
