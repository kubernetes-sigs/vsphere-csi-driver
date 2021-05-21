<!-- markdownlint-disable MD033 -->
# vSphere CSI Driver - Prerequisites

- [Compatible vSphere and ESXi versions](#compatible_vsphere_esxi_versions)
- [vSphere Roles and Privileges](#roles_and_privileges)
- [Setting up the management network](#setup_management_network)
- [Virtual Machine Configuration](#vm_configuration)
- [vSphere Cloud Provider Interface (CPI)](#vsphere_cpi)

## Compatible vSphere and ESXi versions <a id="compatible_vsphere_esxi_versions"></a>

VMware's Cloud Native Storage (CNS) solution is delivered on vSphere version 6.7U3 and above. The volume driver interacts with CNS and hence only connects to vSphere 6.7U3 and above. When upgrading an older version of vSphere to 6.7U3 or above, make sure to upgrade the individual ESXi hosts that are part of the cluster to 6.7U3 or above as well. Note that the vSphere versions and ESXi versions should match.

## vSphere Roles and Privileges <a id="roles_and_privileges"></a>

The vSphere user for CSI driver requires a set of privileges to perform Cloud Native Storage operations.

To know how to create and assign a role, refer the [vSphere Security documentation](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.security.doc/GUID-41E5E52E-A95B-4E81-9724-6AD6800BEF78.html).

The following roles need to be created with sets of privileges.

| Role                    | Privileges for the role                                                                                                                                                                                                                                                                                         | Required on                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CNS-DATASTORE           | ![ROLE-CNS-DATASTORE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/ROLE-CNS-DATASTORE.png)<br><pre lang="bash">govc role.ls CNS-DATASTORE<br>Datastore.FileManagement<br>System.Anonymous<br>System.Read<br>System.View<br></pre>                                    | Shared datastores where persistent volumes need to be provisioned.<br><br>Note: Before CSI v2.2.0, we require all shared datastores to have Datastore.FileManagement privilege. From CSI v2.2.0, this requirement has been relaxed. We do not require all shared datastores to have Datastore.FileManagement privilege. CSI will skip those shared datastores which do not have Datastore.FileManagement privilege during volume provisioning and not provisioning volume on those datastores.                                                                                                                                                                                                                                                                                                                                                                                  |
| CNS-HOST-CONFIG-STORAGE | ![ROLE-CNS-HOST-CONFIG-STORAGE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/ROLE-CNS-HOST-CONFIG-STORAGE.png)<br><pre lang="bash">% govc role.ls CNS-HOST-CONFIG-STORAGE<br>Host.Config.Storage<br>System.Anonymous<br>System.Read<br>System.View<br></pre>         | Required on vSAN file service enabled vSAN cluster. Required for file volume only.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| CNS-VM                  | ![ROLE-CNS-VM](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/ROLE-CNS-VM.png)<br><pre lang="bash">% govc role.ls CNS-VM<br>System.Anonymous<br>System.Read<br>System.View<br>VirtualMachine.Config.AddExistingDisk<br>VirtualMachine.Config.AddRemoveDevice<br></pre> | All node VMs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| CNS-SEARCH-AND-SPBM     | ![ROLE-CNS-SEARCH-AND-SPBM](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/ROLE-CNS-SEARCH-AND-SPBM.png)<br><pre lang="bash">% govc role.ls CNS-SEARCH-AND-SPBM<br>Cns.Searchable<br>StorageProfile.View<br>System.Anonymous<br>System.Read<br>System.View<br></pre>   | Root vCenter Server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ReadOnly                | This role is already available in the vCenter.<br><pre lang="bash">% govc role.ls ReadOnly<br>System.Anonymous<br>System.Read<br></pre>                                                                                                                                                                         | Users with the `ReadOnly` role for an object are allowed to view the state of the object and details about the object.<br><br>For example, users with this role can find the shared datastores accessible to all node VMs. <br><br>For zone and topology-aware environments, all ancestors of node VMs such as a host, cluster, and datacenter must have the `ReadOnly` role set for the vSphere user configured to use the CSI driver and CPI.<br>This is required to allow reading tags and categories to prepare the nodes' topology.  |

Roles need to be assigned to the vSphere objects participating in the Cloud Native Storage environment.

To understand roles assignment to vSphere objects, consider we have following vSphere inventory.

```bash
sc2-rdops-vm06-dhcp-215-129.eng.vmware.com (vCenter Server)
|
|- datacenter (Data Center)
    |
    |-vSAN-cluster (cluster)
      |
      |-10.192.209.1 (ESXi Host)
      | |
      | |-k8s-master (node-vm)
      |
      |-10.192.211.250 (ESXi Host)
      | |
      | |-k8s-node1 (node-vm)
      |
      |-10.192.217.166 (ESXi Host)
      | |
      | |-k8s-node2 (node-vm)
      | |
      |-10.192.218.26 (ESXi Host)
      | |
      | |-k8s-node3 (node-vm)
```

Consider each host has the following shared datastores along with some local VMFS datastores.

- shared-vmfs
- shared-nfs
- vsanDatastore

Considering the above inventory, roles should be assigned as specified below:

| Role  | Usage  |
|---|---|
| ReadOnly | ![READ-ONLY-USAGE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/READ-ONLY-USAGE.png)|
| CNS-HOST-CONFIG-STORAGE | ![HOST-CONFIG-STORAGE-USAGE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/HOST-CONFIG-STORAGE-USAGE.png)|
| CNS-DATASTORE | ![CNS-DATASTORE-USAGE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/CNS-DATASTORE-USAGE.png)|
| CNS-VM | ![CNS-VM-USAGE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/CNS-VM-USAGE.png)|
| CNS-SEARCH-AND-SPBM | ![CNS-SEARCH-AND-SPBM-USAGE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/CNS-SEARCH-AND-SPBM-USAGE.png)|

**Note: When the new entity (Node VM, Datastore) is added in the vCenter inventory for the Kubernetes cluster, roles need to be applied to them.**

## Setting up the management network <a id="setup_management_network"></a>

By default, CPI and CSI Pods are scheduled on k8s master nodes. In this case, for non-topology aware Kubernetes clusters, it is sufficient to provide the k8s master node(s) credentials to the vCenter that this cluster runs on.

For topology-aware clusters, every k8s node needs to discover its topology by communicating with the vCenter. This is needed to utilize the topology-aware provisioning and late binding feature.

Refer to the [Deployment with Zones](deploying_csi_with_zones.md) to understand how to provide vCenter credentials access to Kubernetes nodes.

## Virtual Machine Configuration <a id="vm_configuration"></a>

Make sure to configure all the VMs that form the Kubernetes cluster with the following:

- We recommend using the VMware Paravirtual SCSI controller for Primary Disk on the Node VMs.
- Set the `disk.EnableUUID` parameter to `TRUE` for each node VM. This step is necessary so that the VMDK always presents a consistent UUID to the VM, thus allowing the disk to be mounted properly.
  - This can be done on the VirtualCenter User Interface by right-clicking on the VM → Edit Settings → VM Options → Advanced → Edit Configuration.
- VM Hardware version must be 15 or higher.
  - This can be done on the VirtualCenter User Interface by right-clicking on the VM → Compatibility → Upgrade VM Compatibility.

The VMs can also be configured by using the `govc` command-line tool.

- Install `govc` on your devbox/workstation.
- Get the VM Paths

    ```bash
    $ export GOVC_INSECURE=1
    $ export GOVC_URL='https://<VC_Admin_User>:<VC_Admin_Passwd>@<VC_IP>'

    $ govc ls
    /<datacenter-name>/vm
    /<datacenter-name>/network
    /<datacenter-name>/host
    /<datacenter-name>/datastore

    // To retrieve all Node VMs
    $ govc ls /<datacenter-name>/vm
    /<datacenter-name>/vm/<vm-name1>
    /<datacenter-name>/vm/<vm-name2>
    /<datacenter-name>/vm/<vm-name3>
    /<datacenter-name>/vm/<vm-name4>
    /<datacenter-name>/vm/<vm-name5>
    ```

- Enable disk UUID

    Run the below command for all Node VMs that are part of the Kubernetes cluster.

    ```bash
    govc vm.change -vm '/<datacenter-name>/vm/<vm-name1>' -e="disk.enableUUID=1"
    ```

- Upgrade VM hardware version of node VMs to 15 or higher.

  Run the below command for all Node VMs that are part of the Kubernetes cluster.

    ```bash
    govc vm.upgrade -version=15 -vm '/<datacenter-name>/vm/<vm-name1>'
    ```

## vSphere Cloud Provider Interface (CPI) <a id="vsphere_cpi"></a>

Follow the steps described under “Install the vSphere Cloud Provider Interface” in [https://github.com/kubernetes/cloud-provider-vsphere/blob/master/docs/book/tutorials/kubernetes-on-vsphere-with-kubeadm.md](https://github.com/kubernetes/cloud-provider-vsphere/blob/master/docs/book/tutorials/kubernetes-on-vsphere-with-kubeadm.md) to deploy CPI.

Installation steps for vSphere CPI is briefly described here

Step-1: Taint nodes.

Before installing CPI, verify all nodes (including master nodes) are tainted with "node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule".

To taint nodes use following command

```bash
kubectl taint node <node-name> node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule
```

When the kubelet is started with an “external” cloud provider, this taint is set on a node to mark it as unusable. After a controller from the cloud-controller-manager initializes this node, the kubelet removes this taint.

Step-2: Create a cloud-config configmap of vSphere configuration. Note: This is used for CPI. There is separate secret required for vSphere CSI driver. Here is an example configuration file with dummy values:

```bash
tee /etc/kubernetes/vsphere.conf >/dev/null <<EOF
[Global]
insecure-flag = "true"

[VirtualCenter "IP or FQDN"]
user = "username@vsphere.local"
password = "password"
port = "port"
datacenters = "<datacenter1-path>, <datacenter2-path>, ..."

EOF
```

Here in vsphere.conf file,

- `insecure-flag` - should be set to true to use self-signed certificate for login.
- `VirtualCenter` - section defines vCenter IP address / FQDN.
- `user` - vCenter username.
- `password` - password for vCenter user specified with user.
- `port` - vCenter Server Port.
- `datacenters` - list of all comma separated datacenter paths where kubernetes node VMs are present. When datacenter is located at the root, the name of datacenter is enough but when datacenter is placed in the folder, path needs to be specified as folder/datacenter-name. Please note since comma is used as a delimiter, the datacenter name itself must not contain a comma.

```bash
cd /etc/kubernetes
kubectl create configmap cloud-config --from-file=vsphere.conf --namespace=kube-system
```

```bash
kubectl get configmap cloud-config --namespace=kube-system
NAME           DATA   AGE
cloud-config   1      82s
```

Remove vsphere.conf file created at /etc/kubernetes/

Step-3: Create Roles, Roles Bindings, Service Account, Service and cloud-controller-manager Pod

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/cloud-provider-vsphere/master/manifests/controller-manager/cloud-controller-manager-roles.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes/cloud-provider-vsphere/master/manifests/controller-manager/cloud-controller-manager-role-bindings.yaml
kubectl apply -f https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/controller-manager/vsphere-cloud-controller-manager-ds.yaml
```

Step-4: Verify `ProviderID` is set for all nodes.

```bash
kubectl describe nodes | grep "ProviderID"
ProviderID: vsphere://<provider-id1>
ProviderID: vsphere://<provider-id2>
ProviderID: vsphere://<provider-id3>
ProviderID: vsphere://<provider-id4>
ProviderID: vsphere://<provider-id5>
```

vSphere CSI driver needs the `ProviderID` field to be set for all nodes.
