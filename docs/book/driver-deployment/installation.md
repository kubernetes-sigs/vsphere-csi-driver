<!-- markdownlint-disable MD033 -->
# vSphere CSI Driver - Installation

This section contains steps to install vSphere CSI Driver. Please visit the [Prerequisite](prerequisites.md) section before proceeding.

Note that this installation guide only applies to Vanilla Kubernetes clusters. Project Pacific and Tanzu Kubernetes Grid clusters come with vSphere CSI Driver pre installed.

The following steps need to be performed on the k8s node where the vSphere CSI drivers controller will be deployed. We recommend installing vSphere CSI driver on your k8s master node.

- [Taint Master Node](#taint_master_node)
- [Create a configuration file with vSphere credentials](#create_csi_vsphereconf)
  - [vSphere configuration file for block volumes](#vsphereconf_for_block)
  - [vSphere configuration file for file volumes](#vsphereconf_for_file)
- [Create a kubernetes secret for vSphere credentials](#create_k8s_secret)
- [Create Roles, ServiceAccount and ClusterRoleBinding for vSphere CSI Driver](#csi_service_account)
- [Install vSphere CSI driver](#install)
- [Verify that CSI has been successfully deployed](#verify)

## Taint Master Node <a id="taint_master_node"></a>

Make sure that the  Master Node is tainted with `node-role.kubernetes.io/master=:NoSchedule`

```cgo
$ kubectl describe nodes | egrep "Taints:|Name:"
Name:               <k8s-master-name>
Taints:             node-role.kubernetes.io/master:NoSchedule
Name:               <k8s-worker1-name>
Taints:             <none>
Name:               <k8s-worker2-name>
Taints:             <none>
Name:               <k8s-worker3-name>
Taints:             <none>
Name:               <k8s-worker4-name>
Taints:             <none>
```

If it is not tainted, you may do so by running the command:

```cgo
kubectl taint nodes <k8s-master-name> node-role.kubernetes.io/master=:NoSchedule
```

## Create a configuration file with vSphere credentials <a id="create_csi_vsphereconf"></a>

Create a configuration file that will contain details to connect to vSphere.

The default file to write these configuration details is the `csi-vsphere.conf` file. If you would like to use a file with another name, change the environment variable `VSPHERE_CSI_CONFIG` in the deployment YAMLs described in the section [Install vSphere CSI driver](#install) below.

### vSphere configuration file for block volumes <a id="vsphereconf_for_block"></a>

Here is an example vSphere configuration file for block volumes, with dummy values:

```cgo
$ cat /etc/kubernetes/csi-vsphere.conf
[Global]
cluster-id = "<cluster-id>"

[VirtualCenter "<IP or FQDN>"]
insecure-flag = "<true or false>"
user = "<username>"
password = "<password>"
port = "<port>"
ca-file = <ca file path> # optional, use with insecure-flag set to false
datacenters = "<datacenter-name1>, <datacenter-name2>, ..."
```

Where the entries have the following meaning:

- `cluster-id` - represents the unique cluster identifier. Each kubernetes cluster should have it's own unique cluster-id set in the configuration file.

- `VirtualCenter` - section defines vCenter IP address / FQDN.

- `insecure-flag` - should be set to true to use self-signed certificate for login.

- `user` - vCenter username.

- `password` - password for vCenter user specified with user.

- `port` - vCenter Server Port. The default is 443 if not specified.

- `ca-file` - path to a CA certificate in PEM format. It is an optional parameter.

- `datacenters` - list of all comma separated datacenters where kubernetes node VMs are present.

### vSphere configuration file for file volumes <a id="vsphereconf_for_file"></a>

For file volumes, there are some extra parameters added to the config to help specify network permissions and placement of volumes. A sample config file for file volumes is shown below.

```cgo
$ cat /etc/kubernetes/csi-vsphere.conf
[Global]
cluster-id = "<cluster-id>"

[NetPermissions "A"]
ips = "*"
permissions = "READ_WRITE"
rootsquash = false

[NetPermissions "B"]
ips = "10.20.20.0/24"
permissions = "READ_ONLY"
rootsquash = true

[NetPermissions "C"]
ips = "10.30.30.0/24"
permissions = "NO_ACCESS"

[NetPermissions "D"]
ips = "10.30.10.0/24"
rootsquash = true

[NetPermissions "E"]
ips = "10.30.1.0/24"

[VirtualCenter "<IP or FQDN>"]
insecure-flag = "<true or false>"
user = "<username>"
password = "<password>"
port = "<port>"
ca-file = <ca file path> # optional, use with insecure-flag set to false
datacenters = "<datacenter-name1>, <datacenter-name2>, ..."
targetvSANFileShareDatastoreURLs = "ds:///vmfs/volumes/vsan:52635b9067079319-95a7473222c4c9cd/" # Optional
```

Some of the parameters have been explained in the previous section for block volumes.  

`targetvSANFileShareDatastoreURLs` and `NetPermissions` section are exclusive to file volumes and are optional.

- `targetvSANFileShareDatastoreURLs` - optional parameter. It comes in handy when you have an environment with file service enabled vSAN cluster(s) and you intend to limit the creation of file share volumes to only a select few vSAN datastores. This field contains a comma separated list of datastore URLs where you want to deploy the file share volumes.

- `NetPermissions` - optional parameter. Set of parameters used to restrict the network capabilities of all the file share volumes created under this vSphere configuration. If the complete set of `NetPermissions` are not mentioned for a given IP range, defaults are assumed for the missing parameters. You can define as many `NetPermissions` sections as you want and each of these sections is uniquely identified by the string which follows.

The parameters grouped by `NetPermissions` are as follows:

- `Ips` - defines the IP range or IP subnet to which these restrictions will be levied upon. The default value for `Ips` is "*" which means all the IPs.

- `Permissions` - can either be "READ_WRITE", "READ_ONLY" or "NO_ACCESS". The default value for `Permissions` is "READ_WRITE" for the given IP range.

- `RootSquash` - defines the security access level for the file share volume. The default for `RootSquash` is `false` i.e allow root access to the all the file share volumes created within the given IP range.

If the `NetPermissions` section is completely omitted, the defaults for each of the parameters above are assumed.

## Create a kubernetes secret for vSphere credentials <a id="create_k8s_secret"></a>

Create a Kubernetes secret that will contain configuration details to connect to vSphere.

Create the secret by running the following command:

```bash
kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf --namespace=kube-system
```

Verify that the credential secret is successfully created in the kube-system namespace.

```bash
$ kubectl get secret vsphere-config-secret --namespace=kube-system
NAME                    TYPE     DATA   AGE
vsphere-config-secret   Opaque   1      43s
```

For security purposes, it is advised to remove this configuration file.

```bash
rm csi-vsphere.conf
```

## Create Roles, ServiceAccount and ClusterRoleBinding for vSphere CSI Driver <a id="csi_service_account"></a>

Create ClusterRole, ServiceAccounts and ClusterRoleBinding needed for installation of vSphere CSI Driver

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/manifests/v2.0.0/vsphere-7.0/vanilla/rbac/vsphere-csi-controller-rbac.yaml
```

Click [here](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/v2.0.0/vsphere-7.0/vanilla/rbac) to view the roles assigned to vSphere CSI Driver.

## Install vSphere CSI driver <a id="install"></a>

Our CSI Controller runs as a Kubernetes deployment, with a replica count of 1. For version `v2.0.0`, the `vsphere-csi-controller` Pod consists of 6 containers – the CSI controller, External Provisioner, External Attacher, External Resizer, Liveness probe and [vSphere Syncer](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/pkg/syncer).

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/manifests/v2.0.0/vsphere-7.0/vanilla/deploy/vsphere-csi-controller-deployment.yaml
```

There is also a CSI node Daemonset to be deployed, that will run on every node.

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/manifests/v2.0.0/vsphere-7.0/vanilla/deploy/vsphere-csi-node-ds.yaml
```

Click [here](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/v2.0.0/vsphere-7.0/vanilla/deploy) to view the deployment manifest for vSphere CSI driver.

NOTE: Visit [vSphere CSI Driver - Deployment with Topology](deploying_csi_with_zones.md) to deploy your kubernetes cluster with topology aware provisioning feature.

## Verify that CSI has been successfully deployed <a id="verify"></a>

To verify that the CSI driver has been successfully deployed, you should observe that there is one instance of the vsphere-csi-controller running on the master node and that an instance of the vsphere-csi-node is running on each of the worker nodes.

```bash
$ kubectl get deployment --namespace=kube-system
NAME                          READY   AGE
vsphere-csi-controller        1/1     2m58s
$ kubectl get daemonsets vsphere-csi-node --namespace=kube-system
NAME               DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR   AGE
vsphere-csi-node   4         4         4       4            4           <none>          3m51s
```

Verify that the vSphere CSI driver has been registered with Kubernetes

```bash
$ kubectl describe csidrivers
  Name:         csi.vsphere.vmware.com
  Namespace:
  Labels:       <none>
  Annotations:  <none>
  API Version:  storage.k8s.io/v1beta1
  Kind:         CSIDriver
  Metadata:
    Creation Timestamp:  2020-04-14T20:46:07Z
    Resource Version:    2382881
    Self Link:           /apis/storage.k8s.io/v1beta1/csidrivers/csi.vsphere.vmware.com
    UID:                 19afbecd-bc2f-4806-860f-b29e20df3074
  Spec:
    Attach Required:    true
    Pod Info On Mount:  false
    Volume Lifecycle Modes:
      Persistent
  Events:  <none>

```

Verify that the CSINodes have been created

```bash
$ kubectl get CSINode
NAME                 CREATED AT
<k8s-worker1-name>   2020-04-14T12:30:29Z
<k8s-worker2-name>   2020-04-14T12:30:38Z
<k8s-worker3-name>   2020-04-14T12:30:21Z
<k8s-worker4-name>   2020-04-14T12:30:26Z
```
