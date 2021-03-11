<!-- markdownlint-disable MD033 -->
<!-- markdownlint-disable MD034 -->

# vSphere CSI Driver - VMware Cloud on AWS (VMC) support

- [Introduction](#introduction)
- [Deploy vSphere CSI driver on VMC](#deploy-csi-on-vmc)

**Note:** Feature to support vSphere CSI driver on VMC will be released with v2.2.0. v2.2.0 vSphere CSI driver on VMC will only support block volume. The minimum SDDC version to support this feature is 1.12. Please refer to [VMC release notes](https://docs.vmware.com/en/VMware-Cloud-on-AWS/0/rn/vmc-on-aws-relnotes.html) to get more details.

## Introduction <a id="introduction"></a>

[VMware Cloud™ on AWS](https://cloud.vmware.com/vmc-aws) brings VMware’s enterprise-class SDDC software to the AWS Cloud with optimized access to AWS services. Powered by VMware Cloud Foundation, VMware Cloud on AWS integrates our compute, storage and network virtualization products (VMware vSphere®, vSAN™ and NSX®) along with VMware vCenter management, optimized to run on dedicated, elastic, bare-metal AWS infrastructure.

VMware Cloud on AWS provides two vSAN datastores in each SDDC cluster: WorkloadDatastore, managed by the Cloud Administrator, and vsanDatastore, managed by VMware. Cloudadmin user does not have the privilege to create volume on vsanDatastore and only has the privilege to create volume on WorkloadDatastore.

Without this feature, cloudadmin user cannot provision PVs using vSphere CSI drvier on VMC due to no privilege to create volume on vSANDatastore.  This feature enables cloudadmin user to provision PV using vSphere CSI driver without specifying the datastore where the volume is provisioned.

## Deploy vSphere CSI driver on VMC <a id="deploy-csi-on-vmc"></a>

To deploy vSphere CSI driver on VMC, please make sure to keep roles and privileges up to date as mentioned in the [roles and privileges requirement](https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/prerequisites.html#roles_and_privileges). For cloudadmin user, CNS-DATASTORE role should only be assigned to WorkLoadDatastore.

Make sure to use cloudadmin user and password in `csi-vsphere.conf` file. This file will be used to create a Kubernetes secret for vSphere credentials, which is required to install vSphere CSI driver. The following is an sample `csi-vsphere.conf` file used to deploy vSphere CSI driver in VMC environment.

```bash
[Global]
cluster-id = "unique-kubernetes-cluster-id"

[VirtualCenter "1.2.3.4"]
insecure-flag = "true"
user = "cloudadmin vcenter username"
password = "cloudadmin vcenter password"
port = "443"
datacenters = "list of comma separated datacenter paths where node VMs are present"
```
