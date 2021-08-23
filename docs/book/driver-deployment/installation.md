<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->
<!-- markdownlint-disable MD014 -->
# vSphere CSI Driver - Installation

This section contains steps to install vSphere CSI Driver. Please visit the [Prerequisite](prerequisites.md) section before proceeding.

Note that this installation guide only applies to Vanilla Kubernetes clusters. Project Pacific and Tanzu Kubernetes Grid clusters come with vSphere CSI Driver pre installed.

The following steps need to be performed on the k8s node where the vSphere CSI drivers controller will be deployed. We recommend installing vSphere CSI driver on your k8s master node.

- [Create vmware-system-csi namespace](#create_namespace)
- [Taint Master Node](#taint_master_node)
- [Create a configuration file with vSphere credentials](#create_csi_vsphereconf)
  - [vSphere configuration file for block volumes](#vsphereconf_for_block)
  - [vSphere configuration file for file volumes](#vsphereconf_for_file)
- [Create a kubernetes secret for vSphere credentials](#create_k8s_secret)
- [Install vSphere CSI driver](#install)
- [Verify that vSphere CSI Driver has been deployed successfully](#verify)

## Create `vmware-system-csi` namespace for vSphere CSI Driver<a id="create_namespace"></a>

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/namespace.yaml
```

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

For deployment with zones refer to https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/deploying_csi_with_zones.html

### vSphere configuration file for block volumes <a id="vsphereconf_for_block"></a>

Here is an example vSphere configuration file for block volumes, with dummy values:

```cgo
$ cat /etc/kubernetes/csi-vsphere.conf
[Global]
cluster-id = "<cluster-id>"
cluster-distribution = "<cluster-distribution>"
ca-file = <ca file path> # optional, use with insecure-flag set to false
thumbprint = "<cert thumbprint>" # optional, use with insecure-flag set to false without providing ca-file

[VirtualCenter "<IP or FQDN>"]
insecure-flag = "<true or false>"
user = "<username>"
password = "<password>"
port = "<port>"
datacenters = "<datacenter1-path>, <datacenter2-path>, ..."
```

Where the entries have the following meaning:

- `cluster-id` - represents the unique cluster identifier. Each kubernetes cluster should have it's own unique cluster-id set in the configuration file. The cluster ID should not exceed 64 characters.

- `cluster-distribution` - represents the distribution of the kubernetes cluster. This parameter is optional but will be made mandatory in a future release. Examples are `Openshift`, `Anthos` and `TKGI`.

  - values with special character `\r` causes vSphere CSI controller to go into CrashLoopBackOff state.

  - values of more than 128 characters will cause the PVC creation to be stuck in `Pending` state.

- `VirtualCenter` - section defines vCenter IP address / FQDN.

- `insecure-flag` - should be set to true to use self-signed certificate for login.

- `user` - vCenter username. Specify username along with domain name for example - `user = "userName@domainName"` or  `user = "domainName\\username"` . Note: For active directory user if domain name is not specified, vSphere CSI driver will not function properly.

- `password` - password for vCenter user specified with user.

- `port` - vCenter Server Port. The default is 443 if not specified.

- `ca-file` - path to a CA certificate in PEM format. It is an optional parameter.

- `Thumbprint` - the certificate thumbprint. It is an optional parameter and is ignored when you're using insecure setups or when you provide `ca-file`

- `datacenters` - list of all comma separated datacenter paths where kubernetes node VMs are present. When datacenter is located at the root, the name of datacenter is enough but when datacenter is placed in the folder, path needs to be specified as `folder/datacenter-name`.
Please note since comma is used as a delimiter, the datacenter name itself must not contain a comma.

**Note:** To deploy CSI driver for block volume in VMC environment, in the vSphere configuration file, need to specifiy cloudadmin user in `user` field and cloudadmin password in `password` field.

### vSphere configuration file for file volumes <a id="vsphereconf_for_file"></a>

For file volumes, there are some extra parameters added to the config to help specify network permissions and placement of volumes. A sample config file for file volumes is shown below.

```cgo
$ cat /etc/kubernetes/csi-vsphere.conf
[Global]
cluster-id = "<cluster-id>"
cluster-distribution = "<cluster-distribution>"
ca-file = <ca file path> # optional, use with insecure-flag set to false

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
datacenters = "<datacenter1-path>, <datacenter2-path>, ..."
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
kubectl create secret generic vsphere-config-secret --from-file=csi-vsphere.conf --namespace=vmware-system-csi
```

Verify that the credential secret is successfully created in the kube-system namespace.

```bash
$ kubectl get secret vsphere-config-secret --namespace=vmware-system-csi
NAME                    TYPE     DATA   AGE
vsphere-config-secret   Opaque   1      43s
```

For security purposes, it is advised to remove this configuration file.

```bash
rm csi-vsphere.conf
```

## Install vSphere CSI driver <a id="install"></a>

Before deploying the vSphere CSI driver, refer to the [Compatibility](../compatiblity_matrix.md) page to view the supported Kubernetes versions and [feature support](../supported_features_matrix.md) page.

NOTE: Refer [vSphere CSI Driver - Deployment with Topology](deploying_csi_with_zones.md) to deploy your kubernetes cluster with topology aware provisioning feature.

### Pre-requisites

Make sure to perform Prerequisites steps mentioned [here](https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/prerequisites.html) before installing vSphere CSI Driver.

### Install vSphere CSI Driver

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/v2.3.0/manifests/vanilla/vsphere-csi-driver.yaml
```

### Verify that vSphere CSI Driver has been deployed successfully <a id="verify"></a>

To verify that the CSI driver has been successfully deployed, you should observe that there is one instance of the vsphere-csi-controller running on the master node and that an instance of the vsphere-csi-node is running on each of the worker nodes.

```bash
$ kubectl get deployment --namespace=vmware-system-csi
NAME                          READY   AGE
vsphere-csi-controller        1/1     2m58s
$ kubectl get daemonsets vsphere-csi-node --namespace=vmware-system-csi
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
  API Version:  storage.k8s.io/v1
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
