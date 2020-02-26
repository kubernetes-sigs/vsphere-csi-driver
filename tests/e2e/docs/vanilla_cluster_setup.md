# E2E tests on Vanilla k8s

## Configuring VCenter

### Create a VM Storage Policy for local datastore

![Vanilla non shared DS 1](images/shared_ds_policy_step_1.png)

#### Assign a tag to local datastore

![Vanilla non shared DS 2](images/non-shared_ds_policy_step_1.png)

#### Create a tag based storage policy for a local datastore

![Vanilla non shared DS 3](images/non-shared_ds_policy_step_2.png)

## Running e2e Tests

The section outlines how to set the env variable for running e2e test.

### Building e2eTest.conf

    [Global]
    insecure-flag = "true"
    hostname = "<VC_IP>"
    user = "<USER>"
    password = "<PASSWORD>"
    port = "443"
    datacenters = "<Datacenter_Name>"
    targetvSANFileShareDatastoreURLs = "<comma separated datastore URLs>" # Optional parameter

* Please update the `hostname` and `datacenters` as per your testbed configuration. 
`datacenters` should be comma separated if deployed on multi-datacenters
* `targetvSANFileShareDatastoreURLs` is an optional parameter. It contains a comma separated 
list of datastore URLs where you want to deploy file share volumes. Retrieve this value from the
 secret named `vsphere-config-secret` in your testbed.

### Copy contents of ~/.kube/config from master node to your e2e test environment

    cat ~/.kube/config
    #PASTE CONTENTS OF ~/.kube/config FROM KUBERNETES MASTER NODE>

### Setting env variables

    # Setting env variables for non-zone e2e tests
    export E2E_TEST_CONF_FILE="/path/to/e2eTest.conf"
    export KUBECONFIG=~/.kube/config
    export VOLUME_OPS_SCALE=5
    export SHARED_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/5cf05d97-4aac6e02-2940-02003e89d50e/"
    export NONSHARED_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/5cf05d98-b2c43515-d903-02003e89d50e/"
    export DESTINATION_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/5ad05d98-c2d43415-a903-12003e89d50e/" # Any local datastore url
    export STORAGE_POLICY_FOR_SHARED_DATASTORES="vSAN Default Storage Policy"
    export STORAGE_POLICY_FOR_NONSHARED_DATASTORES="non-shared-ds-policy"
    # Make sure env var FULL_SYNC_WAIT_TIME should be at least double of the manifest variable FULL_SYNC_INTERVAL_MINUTES in csi-driver-deploy.yaml
    export FULL_SYNC_WAIT_TIME=350    # In seconds
    export USER=root
    export CLUSTER_FLAVOR="VANILLA"
    # To run e2e test for block volume, need to set the following env variable
    export GINKGO_FOCUS="csi-block-vanilla"
    # To run e2e test for file volume, need to set the following env variable
    export GINKGO_FOCUS="csi-file-vanilla"

### To run full sync test, need do extra following steps

#### Setting SSH keys for VC with your local machine to run full sync test

    1.ssh-keygen -t rsa (ignore if you already have public key in the local env)
    2.ssh root@<vc-ip-address> mkdir -p .ssh
    3.cat ~/.ssh/id_rsa.pub | ssh root@<vc-ip-address> 'cat >> .ssh/authorized_keys'
    4.ssh root@<vc-ip-address> "chmod 700 .ssh; chmod 640 .ssh/authorized_keys"

## Requirements
Go version: 1.13

Export the go binary in your PATH to run end-to-end tests

    echo $PATH
    <path-1>:<path-2>:...:/Users/<user-name>/go/bin/

### Run e2e tests

    make test-e2e

### Running specific e2e test :
To run a particular e2e test, set GINKGO_FOCUS to the string located “ginkgo.It()” for that test:

To run the Disk Size test (located at https://gitlab.eng.vmware.com/hatchway/vsphere-csi-driver/blob/master/tests/e2e/vsphere_volume_disksize.go)

    export GINKGO_FOCUS="Volume\sDisk\sSize"

Note that specify spaces using “\s”.
