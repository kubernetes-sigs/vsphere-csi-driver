# Running e2e Tests

The section outlines how to set the env variable for running e2e test.

## Building e2eTest.conf

```
[Global]
insecure-flag = "true"
hostname = "<VC_IP>"
user = "<USER>"
password = "<PASSWORD>"
port = "443"
datacenters = "<Datacenter_Name>"

```
Please update the `hostname` and `datacenters` as per your testbed configuration.
datacenters should be comma separated if deployed on multi-datacenters

```
Note: For zone tests, it is recommended to setup VC with 3 clusters having the following topology values:
Cluster-1                 : region-a
ESXi hosts in Cluster-1   : zone-a

Cluster-2                 : region-b
ESXi hosts in Cluster-2   : zone-b

Cluster-3                 : region-c
ESXi hosts in Cluster-3   : zone-c
```

## Setting env variables for All e2e tests
```shell
$ export E2E_TEST_CONF_FILE="/path/to/e2eTest.conf"
$ export KUBECONFIG="~/.kube/config"
$ export SUPERVISOR_CLUSTER_KUBE_CONFIG="/path/to/supervisor-kube-config-file"
$ export CLUSTER_FLAVOR="VANILLA"                  # Default is set to VANILLA
$ export VOLUME_OPS_SCALE=5
$ export SVC_NAMESPACE="user-pods-ns"
$ export SHARED_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/5cf05d97-4aac6e02-2940-02003e89d50e/"
$ export NONSHARED_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/5cf05d98-b2c43515-d903-02003e89d50e/"
$ export DESTINATION_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/5ad05d98-c2d43415-a903-12003e89d50e/"
$ export STORAGE_POLICY_FOR_SHARED_DATASTORES="vSAN Default Storage Policy"
$ export STORAGE_POLICY_FOR_NONSHARED_DATASTORES="LocalDatastoresPolicy"
$ export TOPOLOGY_WITH_SHARED_DATASTORE="<region-1-shared-datastore>:<zone-1-with-shared-datastore>"
$ export TOPOLOGY_WITH_NO_SHARED_DATASTORE="<region-2-without-shared-datastore>:<zone-2-without-shared-datastore>"
$ export TOPOLOGY_WITH_ONLY_ONE_NODE="<region-3-with-only-one-node>:<zone-3-with-only-one-node>"
$ export STORAGE_POLICY_FROM_INACCESSIBLE_ZONE="PolicyNameInaccessibleToSelectedTopologyValues"
$ export INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL="DataStoreUrlInaccessibleToSelectedTopologyValues"
```
Please update the values as per your testbed configuration. You may want to set `CLUSTER_FLAVOR` to `VANILLA` for vanilla cluster testbed OR `WORKLOAD` for supervisor cluster testbed OR `GUEST_CLUSTER` for guest cluster testbed.

## To run full sync test, need do extra following steps

### Setting SSH keys for VC with your local machine to run full sync test

```
1.ssh-keygen -t rsa (ignore if you already have public key in the local env)
2.ssh root@vcip mkdir -p .ssh
3.cat ~/.ssh/id_rsa.pub | ssh root@vcip 'cat >> .ssh/authorized_keys'
4.ssh root@vcip "chmod 700 .ssh; chmod 640 .ssh/authorized_keys"
```

### Setting full sync time var
1. Add `X_CSI_FULL_SYNC_INTERVAL_MINUTES` in csi-driver-deploy.yaml for vsphere-csi-metadata-syncer
2. Setting time interval in the env
```shell
$ export FULL_SYNC_WAIT_TIME=350    // In seconds
$ export USER=root
```
Please update values as per your need.
Make sure env var FULL_SYNC_WAIT_TIME should be at least double of the manifest var in csi-driver-deploy.yaml

## Running tests
### To run all of the e2e tests, set GINKGO_FOCUS to empty string:
``` shell
$ export GINKGO_FOCUS=""
```
### To run a particular suite, set GINKGO_FOCUS to the string located after [csi-vanilla] in “Ginkgo.Describe()” for that suite:
To run the Disk Size test (located at https://gitlab.eng.vmware.com/hatchway/vsphere-csi-driver/blob/master/tests/e2e/vsphere_volume_disksize.go)
``` shell
$ export GINKGO_FOCUS="Volume\sDisk\sSize"
```
To run topology aware tests, setup CCM with instructions provided at https://github.com/kubernetes/cloud-provider-vsphere/blob/master/docs/book/tutorials/deploying_ccm_and_csi_with_multi_dc_vc_aka_zones.md
Set the GINKGO_FOCUS env variable:
``` shell
$ export GINKGO_FOCUS="Basic\sTopology\sAware\sProvisioning"
```
Note that specify spaces using “\s”.

## Running fail-over e2e test on multi-master K8s
### Prerequisite
To run those tests, a multi-master K8S testbed which has more than one master node is required. CSI driver must be deployed as a K8S deployment with 1 replica. Use manifest file at https://gitlab.eng.vmware.com/hatchway/vsphere-csi-driver/tree/master/manifests/1.14 to deploy CSI driver.

The following output shows a sample multi-master K8S testbed (with 3 master nodes and 3 worker nodes)
``` shell
root@master01:~# kubectl get node

NAME       STATUS   ROLES    AGE   VERSION

master01   Ready    master   15d   v1.14.2

master02   Ready    master   15d   v1.14.2

master03   Ready    master   15d   v1.14.2

worker01   Ready    <none>   15d   v1.14.2

worker02   Ready    <none>   15d   v1.14.2

worker03   Ready    <none>   15d   v1.14.2
```

Please verify ProviderID is set on all registered nodes.
``` shell
root@master01:~# kubectl describe nodes | egrep "ProviderID:|Name:"

Name:               master01

ProviderID:                  vsphere://4222b6fd-ae22-f4e7-96f1-aa8a78cf5b03

Name:               master02

ProviderID:                  vsphere://4222c427-14de-7156-33a8-9f28f309d984

Name:               master03

ProviderID:                  vsphere://422216f0-70a3-7c18-a79d-e298c8127e7f

Name:               worker01

ProviderID:                  vsphere://42227c4a-4e00-8c17-2b06-66b0aedbc985

Name:               worker02

ProviderID:                  vsphere://42223b4b-240f-82c8-2050-c703ac1f5f89

Name:               worker03

ProviderID:                  vsphere://422279f2-0510-b5ff-1129-f32e2b87a23d

```


### Setting SSH keys for K8S master node with your local machine

```
1.ssh-keygen -t rsa (ignore if you already have public key in the local env)
2.ssh root@k8s_master_ip mkdir -p .ssh
3.cat ~/.ssh/id_rsa.pub | ssh root@k8s_master_ip 'cat >> .ssh/authorized_keys'
4.ssh root@k8s_master_ip "chmod 700 .ssh; chmod 640 .ssh/authorized_keys"
```
SSH keys need to be configured properley for all three master nodes of K8S cluster using the above command.

### Setting env variables
``` shell
export E2E_TEST_CONF_FILE="/path/to/e2eTest.conf"
export CLUSTER_FLAVOR="VANILLA"
export SHARED_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/vsan:52e7e70e3b966d33-609dd50e5ac9d1b1/"
export STORAGE_POLICY_FOR_SHARED_DATASTORES="vSAN Default Storage Policy"
export USER=root
export GINKGO_FOCUS="csi-multi-master-block-e2e"
```

