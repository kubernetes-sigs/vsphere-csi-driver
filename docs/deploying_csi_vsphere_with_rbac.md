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

1. Setting up Kubernetes to enable CSI drivers (K8s 1.13 only)
2. (Optional, but highly recommended) Storing vCenter creds in a Kubernetes Secret
3. Configure your vsphere.conf file and create a `configmap` of your settings.
4. Create the RBAC roles and bindings
5. Deploy `csi-vsphere`
6. Create your StorageClass and PersistentVolumeClaim by Example

## Deploying `csi-vsphere`

### 1. Setting up Kubernetes to enable CSI drivers

Depending on the version of Kubernetes you are running, you may need to enable certain options and [feature gates](https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/) on both the `api-server` and all `kubelets` in your cluster. This is in part because CSI may be at a certain release level (alpha, beta, GA) in which those options need to be explicitly enabled in order for CSI to function correctly. Please take a look at and also understand all the configuration options presented in the documentation located [here](https://kubernetes-csi.github.io/docs/deploying.html). You may need to apply all or only a subset of these options and feature gates depending on the version of Kubernetes being used, how the Kubernetes cluster was setup, the Kubernetes distribution being used, and etc. Please read through and understand which options are relevant to your particular version of your Kubernetes cluster.

#### Kubernetes 1.14
In Kubernetes 1.14, the features for `KubeletPluginsWatcher`, `CSINodeInfo`, `CSIDriverRegistry` are all beta,
and therefore enabled by default. Unless you are explicitly disabling these with feature gates, no additional
flags should be necessary.

#### Kubernetes 1.13
For most installations, it should be sufficient set the following on Kubernetes:
- *api-server* (configuration file typically located at: /etc/kubernetes/manifests/kube-apiserver.yaml)
  - `--allow-privileged=true`
  - `--feature-gates=KubeletPluginsWatcher=true,CSINodeInfo=true,CSIDriverRegistry=true`
- *kubelet* (configuration file typically located at: /etc/sysconfig/kubelet)
  - `--allow-privileged=true`
  - `--feature-gates=KubeletPluginsWatcher=true,CSINodeInfo=true,CSIDriverRegistry=true`

Again, please consult the CSI documentation if it's unclear how to setup the `csi-vsphere` for use in your Kubernetes cluster.

#### vSphere
For all Kubernetes nodes in your vSphere environment, you need to set EnableUUID to TRUE. To do that, please perform the following on all nodes:
1. Power off the guest.
2. Select the guest and select Edit Settings.
3. Select the Options tab on top.
4. Select General under the Advanced section.
5. Select the Configuration Parameters... on right hand side.
6. Check to see if the parameter disk.EnableUUID is set, if it is there then make sure it is set to TRUE.
7. If the parameter is not there, select Add Row and add it.
8. Power on the guest.

### 2. (Optional, but recommended) Storing vCenter credentials in a Kubernetes Secret

If you choose to store your vCenter credentials within a Kubernetes Secret, an example [Secrets YAML](https://github.com/kubernetes-sigs/vsphere-csi-driver/raw/master/manifests/vcsi-secret.yaml) is provided for reference. Both the vCenter username and password is base64 encoded within the secret. If you have multiple vCenters (as in the example vsphere.conf file), your Kubernetes Secret YAML will look like the following:

```
apiVersion: v1
kind: Secret
metadata:
  name: vcsi
  namespace: kube-system
stringData:
  1.2.3.4.username: "<YOUR_VCENTER_USERNAME>"
  1.2.3.4.password: "<YOUR_VCENTER_PASSWORD>"
  10.0.0.1.username: "<YOUR_VCENTER_USERNAME>"
  10.0.0.1.password: "<YOUR_VCENTER_PASSWORD>"
```

Create the secret by running the following command:

```bash
$ kubectl create -f vcsi-secret.yaml
```

### 3. Creating a `configmap` of your vSphere configuration

There are 2 options for providing `csi-vsphere` with vCenter credentials:
- Using a Kubernetes Secret
- Within `vsphere.conf`

It's highly recommended that you store your vCenter credentials in a Kubernetes secret for added security.

An example [vsphere.conf](https://github.com/kubernetes-sigs/vsphere-csi-drivar/raw/master/manifests//vsphere.conf)
 in the [manifests](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/) directory for reference.

#### Method 1: Storing vCenter Credentials in a Kubernetes Secret

Example `vsphere.conf` contents if the vCenter credentials are going to be stored using a Kubernetes Secret:

```
[Global]
# properties in this section will be used for all specified vCenters unless overridden in VirtualCenter section.

secret-name = "Kubernetes Secret containing creds in the namespace below"
secret-namespace = "Kubernetes namespace for CSI deployment"
service-account = "vsphere-csi-controller"

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

Configure your `vsphere.conf` file and create a `configmap` of your settings using the following command:

```bash
$ kubectl create configmap csi-config --from-file=vsphere.conf --namespace=kube-system
```

#### Method 2: Storing vCenter Credentials in the vsphere.conf File

Example `vsphere.conf` contents if the vCenter credentials are going to be stored within the configuration file:

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

Configure your `vsphere.conf` file and create a `configmap` of your settings using the following command:

```bash
$ kubectl create configmap csi-config --from-file=vsphere.conf --namespace=kube-system
```

### 4. Create the RBAC roles and bindings

The needed RBAC roles changed from K8s 1.13 to 1.14.

For 1.14, apply the roles found in the [rbac directory](https://github.com/kubernetes-sigs/vsphere-csi-driver/raw/master/manifests/1.14/rbac) all at once with:

```bash
$ kubectl create -f manifests/1.14/rbac
```

For 1.13 you can find the RBAC roles and bindings required by the CSI controller and node [here](https://github.com/kubernetes-sigs/vsphere-csi-driver/raw/master/manifests/1.13/).

To apply them to your Kubernetes cluster, run the following command:

```bash
$ kubectl create -f manifests/1.13/vsphere-csi-controller-rbac.yaml -f manifests/1.13/vsphere-csi-node-rbac.yaml
```

### 5. Deploy `csi-vsphere`

**IMPORTANT NOTES:**
- The YAML to deploy the `csi-vsphere` controller assumes that your Kubernetes cluster was deployed using [kubeadm](https://kubernetes.io/docs/setup/independent/create-cluster-kubeadm/). If you deployed your cluster using alternate means, you will need to modify the YAML files in order to provide necessary files or paths based on your deployment.

For 1.14, the full deployment can be found in the [1.14/deploy](https://github.com/kubernetes-sigs/vsphere-csi-driver/raw/master/manifests/1.14/deploy) directory, and can be applied all at once with:

```bash
$ kubectl create -f manifests/1.14/deploy
```

For 1.13, look in the [manifests/1.13 directory](https://github.com/kubernetes-sigs/vsphere-csi-driver/raw/master/manifests/1.13) and apply the manifests as follows:

```bash
[k8suser@k8master ~]$ kubectl create -f manifests/1.13/vsphere-csi-controller-ss.yaml -f manifests/1.13/vsphere-csi-node-ds.yaml -f manifests/1.13/vsphere-csi-crd.yaml
```

### 6. Create your StorageClass and PersistentVolumeClaim by Example

Each StorageClass (SC) is going to be unique to each user as it depends on the vSphere configuration you have. The PersistentVolumeClaim (PVC) is also therefore unique since the PVC depends on the SC. You can find examples of each in the [manifests](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests) directory for reference. The important thing to note in the [StorageClass](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/example-vsphere-sc.yaml) as seen below is that you need to provide as paramters the type of datastore you will be using (`DatastoreCluster` or `Datastore`) and it's corresponding name.

```
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: my-vsphere-fcd-class
  namespace: kube-system
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: vsphere.csi.vmware.com
parameters:
  parent_type: "ONLY_ACCEPTABLE_VALUES_ARE: DatastoreCluster OR Datastore"
  parent_name: "REPLACE_WITH_YOUR_DATATORECLUSTER_OR_DATASTORE_NAME"
```

Then create a [PersistentVolumeClaim](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/example-vsphere-pvc.yaml) to link to your SC. Example is below.

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
