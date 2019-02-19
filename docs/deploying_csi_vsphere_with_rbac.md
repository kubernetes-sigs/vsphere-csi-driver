# Deploying `csi-vsphere` with RBAC

This document is designed to quickly get you up and running with the `csi-vsphere`.

## Requirements

The vSphere Container Storage Interface (CSI) driver depends on First Class Disks (FCDs) and as such, has the following requirements:

1. All vSphere versions prior to 6.5 are unsupported. Please use the in-tree cloud provider for these versions.
2. vSphere 6.5
  - Limitation: vSphere 6.5 does not support FCD on VSAN datastores
  - Limitation: vSphere 6.5 does not support snapshot functionality on FCD
3. vSphere 6.7
  - Limitation: vSphere 6.7 does not support FCD on VSAN datastores
4. vSphere 6.7u1
  - No known limitations

## Deployment Overview

Steps that will be covered in deploying `csi-vsphere`:

1. Setting up Kubernetes to enable CSI drivers
2. Configure your vsphere.conf file and create a `configmap` of your settings.
3. (Optional, but highly recommended) Storing vCenter creds in a Kubernetes Secret
4. Create the RBAC roles and bindings for the CSI controller
5. Create the RBAC roles and bindings for the CSI node
6. Deploy `csi-vsphere-controller`
7. Deploy `csi-vsphere-node`
8. Create CRDs for CSI driver registration
9. Create your StorageClass and PersistentVolumeClaim by Example

## Deploying `csi-vsphere`

#### 1. Setting up Kubernetes to enable CSI drivers

Depending on the version of Kubernetes you are running, you may need to enable certain options and [feature gates](https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/) on both the `api-server` and all `kubelets` in your cluster. This is in part because CSI may be at a certain release level (alpha, beta, GA) in which those options need to be explicitly enabled in order for CSI to function correctly. Please take a look at and also understand all the configuration options presented in the documentation located [here](https://kubernetes-csi.github.io/docs/deploying.html). You may need to apply all or only a subset of these options and feature gates depending on the version of Kubernetes being used, how the Kubernetes cluster was setup, the Kubernetes distribution being used, and etc. Please read through and understand which options are relevant to your particular version of your Kubernetes cluster.

For most installations, it should be sufficient set the following on Kubernetes:
- *api-server* (configuration file typically located at: /etc/kubernetes/manifests/kube-apiserver.yaml)
  - `--allow-privileged=true`
  - `--feature-gates=VolumeSnapshotDataSource=true,KubeletPluginsWatcher=true,CSINodeInfo=true,CSIDriverRegistry=true`
- *kubelet* (configuration file typically located at: /etc/sysconfig/kubelet)
  - `--allow-privileged=true`
  - `--feature-gates=VolumeSnapshotDataSource=true,KubeletPluginsWatcher=true,CSINodeInfo=true,CSIDriverRegistry=true`

For all Kubernetes nodes in your vSphere environment, you need to EnableUUID set to TRUE. To do that, please perform the following on all nodes:
1. Power off the guest.
2. Select the guest and select Edit Settings.
3. Select the Options tab on top.
4. Select General under the Advanced section.
5. Select the Configuration Parameters... on right hand side.
6. Check to see if the parameter disk.EnableUUID is set, if it is there then make sure it is set to TRUE.
7. If the parameter is not there, select Add Row and add it.
8. Power on the guest.

Again, please consult the CSI documentation if it's unclear how to setup the `csi-vsphere` for use in your Kubernetes cluster.

#### 2. Creating a `configmap` of your vSphere configuration

There are 2 methods for configuring the `csi-vsphere`:
- Using a Kubernetes Secret
- Within the vsphere.conf

It's highly recommended that you store your vCenter credentials in a Kubernetes secret for added security.

An example [vsphere.conf](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vsphere.conf) is located in the `cloud-provider-vsphere` repo in the [manifests/csi](https://github.com/kubernetes/cloud-provider-vsphere/tree/master/manifests/csi) directory for reference.

##### Method 1: Storing vCenter Credentials in a Kubernetes Secret

Example vsphere.conf contents if the vCenter credentials are going to be stored using a Kubernetes Secret:

```
[Global]
# properties in this section will be used for all specified vCenters unless overridden in VirtualCenter section.

secret-name = "Kubernetes Secret containing creds in the namespace below"
secret-namespace = "Kubernetes namespace for CCM deploy"
service-account = "csi-controller"

port = "443" #Optional
insecure-flag = "1" #set to 1 if the vCenter uses a self-signed cert
datacenters = "list of datacenters where Kubernetes node VMs are present"

[VirtualCenter "1.2.3.4"]
# Override specific properties for this Virtual Center.
        datacenters = "list of datacenters where Kubernetes node VMs are present"
        # port, insecure-flag will be used from Global section.

[VirtualCenter "10.0.0.1"]
# Override specific properties for this Virtual Center.
        port = "448"
        insecure-flag = "0"
        # datacenters will be used from Global section.
```

Configure your vsphere.conf file and create a `configmap` of your settings using the following command:

```bash
[k8suser@k8master ~]$ kubectl create configmap cloud-config --from-file=vsphere.conf --namespace=kube-system
```

##### Method 2: Storing vCenter Credentials in the vsphere.conf File

Example vsphere.conf contents if the vCenter credentials are going to be stored within the configuration file:

```
[Global]
# properties in this section will be used for all specified vCenters unless overriden in VirtualCenter section.

user = "vCenter username for cloud provider"
password = "password"
port = "443" #Optional
insecure-flag = "1" #set to 1 if the vCenter uses a self-signed cert
datacenters = "list of datacenters where Kubernetes node VMs are present"

[VirtualCenter "1.2.3.4"]
# Override specific properties for this Virtual Center.
        user = "vCenter username for cloud provider"
        password = "password"
        # port, insecure-flag, datacenters will be used from Global section.

[VirtualCenter "10.0.0.1"]
# Override specific properties for this Virtual Center.
        port = "448"
        insecure-flag = "0"
        # user, password, datacenters will be used from Global section.
```

Configure your vsphere.conf file and create a `configmap` of your settings using the following command:

```bash
[k8suser@k8master ~]$ kubectl create configmap cloud-config --from-file=vsphere.conf --namespace=kube-system
```

#### 3. (Optional, but recommended) Storing vCenter credentials in a Kubernetes Secret

If you choose to store your vCenter credentials within a Kubernetes Secret (method 1 above), an example [Secrets YAML](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vcsi-secret.yaml) is provided for reference. Both the vCenter username and password is base64 encoded within the secret. If you have multiple vCenters (as in the example vsphere.conf file), your Kubernetes Secret YAML will look like the following:

```
apiVersion: v1
kind: Secret
metadata:
  name: vcsi
  namespace: kube-system
data:
  1.2.3.4.username: "Replace with output from `echo -n YOUR_VCENTER_USERNAME | base64`"
  1.2.3.4.password: "Replace with output from `echo -n YOUR_VCENTER_PASSWORD | base64`"
  10.0.0.1.username: "Replace with output from `echo -n YOUR_VCENTER_USERNAME | base64`"
  10.0.0.1.password: "Replace with output from `echo -n YOUR_VCENTER_PASSWORD | base64`"
```

Create the secret by running the following command:

```bash
[k8suser@k8master ~]$ kubectl create -f vcsi-secret.yaml
```

#### 4. Create the RBAC roles and bindings for the CSI controller

You can find the RBAC roles and bindings required by the CSI controller in [vsphere-csi-controller-rbac.yaml](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vsphere-csi-controller-rbac.yaml).

To apply them to your Kubernetes cluster, run the following command:

```bash
[k8suser@k8master ~]$ kubectl create -f vsphere-csi-controller-rbac.yaml
```

#### 5. Create the RBAC roles and bindings for the CSI node

You can find the RBAC roles and bindings required by the CSI node manager in [vsphere-csi-node-rbac.yaml](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vsphere-csi-node-rbac.yaml).

To apply them to your Kubernetes cluster, run the following command:

```bash
[k8suser@k8master ~]$ kubectl create -f vsphere-csi-node-rbac.yaml
```

#### 6. Deploy `csi-vsphere-controller`

**IMPORTANT NOTES:**
- The YAML to deploy CSI controller assumes that your Kubernetes cluster was deployed using [kubeadm](https://kubernetes.io/docs/setup/independent/create-cluster-kubeadm/). If you deployed your cluster using alternate means, you will need to modify the YAML files in order to provide necessary files or paths based on your deployment.

The YAML to deploy `csi-vsphere-controller` can be found in [vsphere-csi-controller-ss.yaml](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vsphere-csi-controller-ss.yaml).

Run the following command:

```bash
[k8suser@k8master ~]$ kubectl create -f vsphere-csi-controller-ss.yaml
```

#### 7. Deploy `csi-vsphere-node`

The YAML to deploy `csi-vsphere-node` can be found in [vsphere-csi-node-ds.yaml](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vsphere-csi-node-ds.yaml).

Run the following command:

```bash
[k8suser@k8master ~]$ kubectl create -f vsphere-csi-node-ds.yaml
```

#### 8. Create CRDs for CSI driver registration

You can find the CRDs to register the CSI nodes in [vsphere-csi-crd.yaml](https://github.com/kubernetes/cloud-provider-vsphere/raw/master/manifests/csi/vsphere-csi-crd.yaml).

To apply them to your Kubernetes cluster, run the following command:

```bash
[k8suser@k8master ~]$ kubectl create -f vsphere-csi-crd.yaml
```

#### 9. Create your StorageClass and PersistentVolumeClaim by Example

Each StorageClass (SC) is going to be unique to each user as it depends on the vSphere configuration you have. The PersistentVolumeClaim (PVC) is also therefore unique since the PVC depends on the SC. You can find examples of each in the [manifests/csi](https://github.com/kubernetes/cloud-provider-vsphere/tree/master/manifests/csi) directory for reference. The important thing to note in the [StorageClass](https://github.com/kubernetes/cloud-provider-vsphere/tree/master/manifests/csi/example-vsphere-sc.yaml) as seen below is that you need to provide as paramters the type of datastore you will be using (`DatastoreCluster` or `Datastore`) and it's corresponding name.

```
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: my-vsphere-fcd-class
  namespace: kube-system
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: io.k8s.cloud-provider-vsphere.vsphere
parameters:
  parent_type: "ONLY_ACCEPTABLE_VALUES_ARE: DatastoreCluster OR Datastore"
  parent_name: "REPLACE_WITH_YOUR_DATATORECLUSTER_OR_DATASTORE_NAME"
```

Then create a [PersistentVolumeClaim](https://github.com/kubernetes/cloud-provider-vsphere/tree/master/manifests/csi/example-vsphere-pvc.yaml) to link to your SC. Example is below.

*NOTE:* Since the PVC references the SC, if you want to have multiple disks from various DatastoreClusters or Datastores, you need to have different SCs and PVCs.

```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-vsphere-csi-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: my-vsphere-fcd-class
```

To test, you can try running the following test pod:

```
kind: Pod
apiVersion: v1
metadata:
  name: my-csi-app
spec:
  containers:
    - name: my-frontend
      image: busybox
      volumeMounts:
      - mountPath: "/data"
        name: my-fcd-volume
      command: [ "sleep", "1000000" ]
  volumes:
    - name: my-fcd-volume
      persistentVolumeClaim:
        claimName: my-vsphere-csi-pvc
```

## Wrapping Up

That's it! Pretty straightforward. Questions, comments, concerns... please stop by the #sig-vmware channel at [kubernetes.slack.com](https://kubernetes.slack.com).
