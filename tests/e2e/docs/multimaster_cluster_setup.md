# Running fail-over e2e test on multi-master K8s

## Prerequisite

To run those tests, a multi-master K8S testbed which has more than one master node is required. CSI driver must be deployed as a K8S deployment with 1 replica.

The following output shows a sample multi-master K8S testbed (with 3 master nodes and 3 worker nodes)

    root@master01:~# kubectl get node

    NAME       STATUS   ROLES    AGE   VERSION

    master01   Ready    master   15d   v1.14.2

    master02   Ready    master   15d   v1.14.2

    master03   Ready    master   15d   v1.14.2

    worker01   Ready    <none>   15d   v1.14.2

    worker02   Ready    <none>   15d   v1.14.2

    worker03   Ready    <none>   15d   v1.14.2

Please verify ProviderID is set on all registered nodes.

    root@master01:~# kubectl describe nodes | egrep "ProviderID:|Name:"
    Name                ProviderID 
    master01      vsphere://4222b6fd-ae22-f4e7-96f1-aa8a78cf5b03
    master02      vsphere://4222c427-14de-7156-33a8-9f28f309d984
    master03      vsphere://422216f0-70a3-7c18-a79d-e298c8127e7f
    worker01      vsphere://42227c4a-4e00-8c17-2b06-66b0aedbc985
    worker02      vsphere://42223b4b-240f-82c8-2050-c703ac1f5f89
    worker03      vsphere://422279f2-0510-b5ff-1129-f32e2b87a23d

## Setting SSH keys for K8S master node with your local machine

    1.ssh-keygen -t rsa (ignore if you already have public key in the local env)
    2.ssh root@k8s_master_ip mkdir -p .ssh
    3.cat ~/.ssh/id_rsa.pub | ssh root@k8s_master_ip 'cat >> .ssh/authorized_keys'
    4.ssh root@k8s_master_ip "chmod 700 .ssh; chmod 640 .ssh/authorized_keys"

SSH keys need to be configured properley for all three master nodes of K8S cluster using the above command.

## Setting env variables

    export E2E_TEST_CONF_FILE="/path/to/e2eTest.conf"
    export CLUSTER_FLAVOR="VANILLA"    # VANILLA / WORKLOAD
    export SHARED_VSPHERE_DATASTORE_URL="ds:///vmfs/volumes/vsan:52e7e70e3b966d33-609dd50e5ac9d1b1/"
    export STORAGE_POLICY_FOR_SHARED_DATASTORES="vSAN Default Storage Policy"
    export USER=root
    export GINKGO_FOCUS="csi-multi-master-block-e2e"
    export BUSYBOX_IMAGE="<image-used-to-deploy-pods>"
    # Need this for dcli, REST APIs and govc
    export VC_ADMIN_PWD="<password>"

## Requirements

Go version: 1.20

Export the go binary in your PATH to run end-to-end tests

    echo $PATH
    <path-1>:<path-2>:...:/Users/<user-name>/go/bin/

### Run e2e tests

    make test-e2e

### Running specific e2e test

To run a particular e2e test, set GINKGO_FOCUS to the string located “ginkgo.It()” for that test:

To run the Disk Size test (located at tests/e2e/vsphere_volume_disksize.go)

    export GINKGO_FOCUS="Volume\sDisk\sSize"

Note that specify spaces using “\s”.
