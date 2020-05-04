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
| CNS-DATASTORE           | ![ROLE-CNS-DATASTORE](https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/master/docs/images/ROLE-CNS-DATASTORE.png)<br><pre lang="bash">govc role.ls CNS-DATASTORE<br>Datastore.FileManagement<br>System.Anonymous<br>System.Read<br>System.View<br></pre>                                    | Shared datastores where persistent volumes need to be provisioned.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
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

## Setting up the management network <a id="setup_management_network"></a>

By default, CPI and CSI Pods are scheduled on k8s master nodes. The control plane needs to be able to communicate with the vCenter.

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

vSphere CSI driver needs the `ProviderID` field to be set for all nodes.

This can be done by installing vSphere Cloud Provider Interface (CPI) on your k8s cluster.

Before installing CPI, verify that all nodes are tainted with "node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule".

```bash
$ kubectl describe nodes | egrep "Taints:|Name:"
Name:               k8s-master
Taints:             node-role.kubernetes.io/master:NoSchedule
Name:               k8s-node1
Taints:             node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule
Name:               k8s-node2
Taints:             node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule
Name:               k8s-node3
Taints:             node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule
Name:               k8s-node4
Taints:             node.cloudprovider.kubernetes.io/uninitialized=true:NoSchedule
```

When the kubelet is started with an “external” cloud provider, this taint is set on a node to mark it as unusable. After a controller from the cloud-controller-manager initializes this node, the kubelet removes this taint.

Follow the steps described under “Install the vSphere Cloud Provider Interface” in [https://github.com/kubernetes/cloud-provider-vsphere/blob/master/docs/book/tutorials/kubernetes-on-vsphere-with-kubeadm.md](https://github.com/kubernetes/cloud-provider-vsphere/blob/master/docs/book/tutorials/kubernetes-on-vsphere-with-kubeadm.md) to deploy CPI.

Verify `ProviderID` is set for all nodes.

```bash
$ kubectl describe nodes | grep "ProviderID"
ProviderID: vsphere://<provider-id1>
ProviderID: vsphere://<provider-id2>
ProviderID: vsphere://<provider-id3>
ProviderID: vsphere://<provider-id4>
ProviderID: vsphere://<provider-id5>
```
