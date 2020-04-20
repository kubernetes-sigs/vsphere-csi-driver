# E2E tests on Supervisor Cluster

## Configuring VCenter

### 1. Create namespace

#### Go to Workload Management

![Go to Workload Management](images/create_namespace_step_1.png)


#### Click on Create Namespace and enter necessary inputs

![Create Namespace](images/create_namespace_step_2.png)

### 2. Create a VM Storage Policy for shared datastore

#### Go to Policies and Profiles

![Shared DS 1](images/shared_ds_policy_step_1.png)

#### Assign tag to a shared datastore

![Shared DS 2](images/shared_ds_policy_step_2.png)

#### Create tag based Storage Policy using the tag created in previous step

![Shared DS 3](images/shared_ds_policy_step_3.png)

### 3. Create a VM Storage Policy for local datastore

![WCP non shared DS 1](images/shared_ds_policy_step_1.png)

#### Assign tag to a local datastore

![WCP non shared DS 2](images/non-shared_ds_policy_step_1.png)

#### Create tag based Storage Policy using the tag created in previous step for local datastore

![WCP non shared DS 3](images/non-shared_ds_policy_step_2.png)

### Fetch the shared-vsphere-datastore-url from VC
![WCP_non_shared_DS_4](images/shared_vsphere_datastore_url.png)

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

* Please update the `hostname` and `datacenters` as per your testbed configuration.
datacenters should be comma separated if deployed on multi-datacenters

### Copy contents of ~/.kube/config from Supervisor Cluster master node to your e2e test environment

    cat ~/.kube/config
    #PASTE CONTENTS OF ~/.kube/config FROM SUPERVISOR CLUSTER MASTER NODE>

### Setting env variables

    # Setting env variables for non-zone e2e tests
    export E2E_TEST_CONF_FILE="/path/to/e2eTest.conf"
    export KUBECONFIG=~/.kube/config
    export STORAGE_POLICY_FOR_SHARED_DATASTORES="shared-ds-policy"
    export STORAGE_POLICY_FOR_NONSHARED_DATASTORES="non-shared-ds-policy"
    # Make sure env var FULL_SYNC_WAIT_TIME should be at least double of the manifest variable X_CSI_FULL_SYNC_INTERVAL_MINUTES in csi-driver-deploy.yaml
    export FULL_SYNC_WAIT_TIME=350    # In seconds
    export USER=root
    export CLUSTER_FLAVOR="WORKLOAD"
    export GINKGO_FOCUS="csi-supervisor"
    export SVC_NAMESPACE="e2e-test-namespace"
    # Set the SHARED_VSPHERE_DATASTORE_URL to the url fetched from the Vcenter as mentioned in the steps above
    export SHARED_VSPHERE_DATASTORE_URL="<shared-vsphere-datastore-url>"

### To run full sync test, need do extra following steps

#### Setting SSH keys for VC with your local machine to run full sync test

    1.ssh-keygen -t rsa (ignore if you already have public key in the local env)
    2.ssh root@vcip mkdir -p .ssh
    3.cat ~/.ssh/id_rsa.pub | ssh root@vcip 'cat >> .ssh/authorized_keys'
    4.ssh root@vcip "chmod 700 .ssh; chmod 640 .ssh/authorized_keys"

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
