# Container Storage Interface (CSI) driver for vSphere 7.x and OpenShift Container Platform
This repository provides scripts for deploying the vSphere CSI provider on an OCP environment. The driver in these files is 2.0 

# Installation

Please read the prerequisites from the documentation below. 

To create the vCenter roles you can use this script;

    https://github.com/saintdle/PowerCLI/blob/master/Create_CSI_Driver_vCenter_Roles.ps1 

Edit csi-vsphere-for-OCP.conf with your environment details.
Create a Kubernetes secret that will contain configuration details to connect to vSphere.

    Command: oc create secret generic vsphere-config-secret --from-file=csi-vsphere-for-OCP.conf --namespace=kube-system

Create the necessary RBAC roles, service account and elevated privileges.

    Command: oc create --from-file=vsphere-csi-controller-rbac-for-OCP.yaml

Install the vSphere CSI driver which is made up of a CSI Controller and CSI Node daemonset.

    Command: oc create --from-file=vsphere-csi-controller-deployment-for-OCP.yaml
    Command: oc create --from-file=vsphere-csi-node-ds-for-OCP.yaml

# Documentation

Official documentation for vSphere CSI Driver is available here:

    https://vsphere-csi-driver.sigs.k8s.io

# vSphere CSI Driver Images

v2.0.0

    gcr.io/cloud-provider-vsphere/csi/release/driver:v2.0.0
    gcr.io/cloud-provider-vsphere/csi/release/syncer:v2.0.0

# Source Manifests
The YAMLs in this repository are based on the manifests from the following locations;

    https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/v2.0.0/vsphere-7.0/vanilla
    https://github.com/dav1x/ocp-vsphere-csi
