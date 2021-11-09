<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
# vSphere CSI Driver - Windows Support

- [Introduction](#introduction)
- [Prerequisite](#prereq)
- [How to enable vSphere CSI with windows nodes](#how-to-enable-vsphere-csi-win)
- [Examples to Deploy a Windows pod with PVC mount](#examples)

## Introduction <a id="introduction"></a>

Windows node support is added in vSphere CSI driver v2.4.0 as an Alpha feature.

Following features are not supported for Windows Node:

1. ReadWriteMany volumes based on vSAN file service are not supported on Windows Node.
2. [Raw Block Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#raw-block-volume-support) are not supported.
3. Windows Nodes will be used as Worker nodes only. vSphere CSI will not support a mixture of Linux worker nodes and Windows Worker Nodes.

## Prerequisite <a id="prereq"></a>

In addition to prerequisites mentioned [here](https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/2.0/vmware-vsphere-csp-getting-started/GUID-0AB6E692-AA47-4B6A-8CEA-38B754E16567.html), following needs to be fullfilled to support windows in vSphere CSI:

1. Minimum kubernetes version required is 1.20.
2. Minimum vSphere CSI driver version required is 2.4.
3. Master nodes should be running Linux.
4. Worker nodes should have Windows server 2019. Other windows server version are not supported. Please refer [this](https://kubernetes.io/docs/tasks/administer-cluster/kubeadm/adding-windows-nodes/)
5. if containerd is used in nodes, containerd version should be greater than or equal to 1.5, refer: https://github.com/containerd/containerd/issues/5405
6. `CSI Proxy` should be installed in each of the Windows nodes. To install csi proxy follow steps from https://github.com/kubernetes-csi/csi-proxy#installation

## How to enable vSphere CSI with Windows nodes <a id="how-to-enable-vsphere-csi-win"></a>

- Install vSphere CSI driver 2.4 by following https://docs.vmware.com/en/VMware-vSphere-Container-Storage-Plug-in/2.0/vmware-vsphere-csp-getting-started/GUID-A1982536-F741-4614-A6F2-ADEE21AA4588.html
- To enable windows support, patch the configmap to enable csi-windows-support feature switch by running following command:
  
  ```bash
  kubectl patch configmap/internal-feature-states.csi.vsphere.vmware.com \
  -n vmware-system-csi \
  --type merge \
  -p '{"data":{"csi-windows-support":"true"}}'
  ```

- vSphere CSI driver v2.4.0 introduces a new node daemonset which will be running on all windows nodes. To verify this run:

  ```bash
  $ kubectl get daemonsets vsphere-csi-node-windows --namespace=vmware-system-csi
  NAME                       DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR              AGE
  vsphere-csi-node-windows   1         1         1       1            1           kubernetes.io/os=windows   7m10s
  ```

## Examples <a id="examples"></a>

- Define a storage class example-windows-sc.yaml as defined [here](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/example/vanilla-k8s-RWO-filesystem-volumes/example-windows-sc.yaml)

  ```bash
  kind: StorageClass
  apiVersion: storage.k8s.io/v1
  metadata:
    name: example-windows-sc
  provisioner: csi.vsphere.vmware.com
  allowVolumeExpansion: true  # Optional: only applicable to vSphere 7.0U1 and above
  parameters:
    storagepolicyname: "vSAN Default Storage Policy"  # Optional Parameter
    #datastoreurl: "ds:///vmfs/volumes/vsan:52cdfa80721ff516-ea1e993113acfc77/"  # Optional Parameter
    #csi.storage.k8s.io/fstype: "ntfs"  # Optional Parameter
  ```

  'csi.storage.k8s.io/fstype' is an optional parameter. From the Windows file systems, only ntfs can be set to its value, as vSphere CSI Driver can only support the NTFS filesystem on Windows Nodes.
  
- Import this `StorageClass` into `Vanilla Kubernetes` cluster:
  
  ```bash
  kubectl create -f example-sc.yaml
  ```

- Define a `PersistentVolumeClaim` request example-windows-pvc.yaml as shown in the spec [here](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/example/vanilla-k8s-RWO-filesystem-volumes/example-windows-pvc.yaml)
  
  ```bash
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: example-windows-pvc
  spec:
    accessModes:
      - ReadWriteOnce
    resources:
      requests:
        storage: 5Gi
    storageClassName: example-windows-sc
    volumeMode: Filesystem
  ```
  
  The pvc definition is same as Linux and only 'ReadWriteOnce' can be specified as accessModes.

- Import this `PersistentVolumeClaim` into `Vanilla Kubernetes` cluster:

    ```bash
    kubectl create -f example-windows-pvc.yaml
    ```

- Verify a `PersistentVolume` was created successfully
  
  The `Status` should say `Bound`.
  
  ```bash
  $ kubectl describe pvc example-windows-pvc
  Name:          example-windows-pvc
  Namespace:     default
  StorageClass:  example-windows-sc
  Status:        Bound
  Volume:        pvc-e64e0716-ff63-47b8-8ee4-d1eb4f86dcd7
  Labels:        <none>
  Annotations:   pv.kubernetes.io/bind-completed: yes
                pv.kubernetes.io/bound-by-controller: yes
                volume.beta.kubernetes.io/storage-provisioner: csi.vsphere.vmware.com
  Finalizers:    [kubernetes.io/pvc-protection]
  Capacity:      5Gi
  Access Modes:  RWO
  VolumeMode:    Filesystem
  Used By:       example-windows-pod
  Events:        <none>
  ```

- To use the above pvc in a Windows pod, you can create a pod spec example-windows-pod.yaml like [this](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/example/vanilla-k8s-RWO-filesystem-volumes/example-windows-pod.yaml)
  
  ```bash
  apiVersion: v1
  kind: Pod
  metadata:
    name: example-windows-pod
  spec:
    nodeSelector:
      kubernetes.io/os: windows
    containers:
      - name: test-container
        image: mcr.microsoft.com/windows/servercore:ltsc2019
        command:
          - "powershell.exe"
          - "-Command"
          - "while (1) { Add-Content -Encoding Ascii C:\\test\\data.txt $(Get-Date -Format u); sleep 1 }"
        volumeMounts:
          - name: test-volume
            mountPath: "/test/"
            readOnly: false
    volumes:
      - name: test-volume
        persistentVolumeClaim:
          claimName: example-windows-pvc
  ```

- Create the pod
  
  ```bash
  kubectl create -f example-windows-pod.yaml
  ```

- Verify pod was created succussfully
  
  ```bash
  $ kubectl get pod example-windows-pod
  NAME                  READY   STATUS    RESTARTS   AGE
  example-windows-pod   1/1     Running   0          4m13s
  ```
  
  In this example, example-windows-pvc is formatted as NTFS file system and is mounted to "C:\\test" directory.
