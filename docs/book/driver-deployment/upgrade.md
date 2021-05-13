<!-- markdownlint-disable MD033 -->
# vSphere CSI Driver - Upgrade

**Note: To upgrade "vSphere with Tanzu" follow vSphere documentation. Following instructions are applicable to native vanilla Kubernetes Cluster deployed on vSphere.**

Before upgrading driver, please make sure to keep roles and privileges up to date as mentioned in the [roles and privileges requirement](https://vsphere-csi-driver.sigs.k8s.io/driver-deployment/prerequisites.html#roles_and_privileges).

Each release of vSphere CSI driver has a different set of RBAC rules and deployment YAML files depending on the version of the vCenter on which Kubernetes Cluster is deployed.

To upgrade from one release to another release of vSphere CSI driver, we recommend to completely delete the driver using the original YAML files, you have used to deploy the driver and then use new YAML files to re-install the vSphere CSI driver.

For example, if you want to upgrade driver from `v2.0.0` release to `v2.0.1` release deployed on the Kubernetes Cluster installed on `vSphere 67u3` release

Uninstall `v2.0.0` driver

```bash
kubectl delete -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.0/vsphere-67u3/deploy/vsphere-csi-controller-deployment.yaml
kubectl delete -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.0/vsphere-67u3/deploy/vsphere-csi-node-ds.yaml
kubectl delete -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.0/vsphere-67u3/rbac/vsphere-csi-controller-rbac.yaml
```

Wait for vSphere CSI Controller Pod and vSphere CSI Nodes Pods to be completely deleted.

Install `v2.0.1` driver

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.1/vsphere-67u3/rbac/vsphere-csi-controller-rbac.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.1/vsphere-67u3/deploy/vsphere-csi-controller-deployment.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/vsphere-csi-driver/release-2.0/manifests/v2.0.1/vsphere-67u3/deploy/vsphere-csi-node-ds.yaml
```

We also recommend you to follow our [installation guide](installation.md) to refer to updated instruction and encourage you to go through all [pre-requisites](prerequisites.md) for newer releases of the vSphere CSI driver.
