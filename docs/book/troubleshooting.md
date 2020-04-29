# Troubleshooting

vSphere CSI Driver supports 2 levels of logging

- PRODUCTION where all INFO/ERROR logs are logged
- DEVELOPMENT where all INFO/ERROR/DEBUG logs are logged

The default log level for all the CSI flavors is PRODUCTION. To be able to examine problems related to your environment, modify the log level for the vSphere CSI driver from PRODUCTION to DEVELOPMENT.

**NOTE**: PRODUCTION/DEVELOPMENT log level is available in 2.0 release. For prior releases, use the argument "--v" to change the log level.

## Procedure to change log level

### Vanilla CSI Driver

- To examine the vSphere CSI controller related issues, update the log level for the vsphere-csi-controller and vsphere-syncer containers.
  - Open the YAML file used for the deployment of vSphere CSI controller and find the LOGGER_LEVEL option in the container's arguments.
  - Update the logger for this option from PRODUCTION to DEVELOPMENT which will give debug logs too.
  - Run the kubectl apply command to reload the pods with new configuration.

    ``` sh
    kubectl apply -f vsphere-csi-controller-deployment.yaml
    ```

- Also update the log level for vsphere-csi-node daemon set
  - Open the YAML file used for the vSphere CSI daemonset and find the LOGGER_LEVEL option in the container's arguments.
  - Update the logger for this option from PRODUCTION to DEVELOPMENT which will give debug logs too.
  - Run the kubectl apply command to reload the daemonset with new configuration.

    ``` sh
    kubectl apply -f vsphere-csi-node-ds.yaml
    ```

### CNS-CSI driver

- To examine the CNS CSI driver related issues, update the log level for the vsphere-csi-controller and vsphere-syncer containers.
  - CNS-CSI driver on Project Pacific Cluster is deployed on vmware-system-csi namespace as Kubernetes deployments.
  - Edit the deployment of vSphere CSI controller and find the LOGGER_LEVEL option in the container's arguments.

    ``` sh
    kubectl edit deployment vsphere-csi-controller -n vmware-system-csi
    ```

  - Update the logger for this option from PRODUCTION to DEVELOPMENT which will give debug logs too.

### pvCSI Driver

- To examine the pvCSI controller related issues, update the log level for the vsphere-csi-controller and vsphere-syncer.
  - pvCSI controller on Tanzu Kubernetes Grid is deployed on vmware-system-csi namespace as Kubernetes deployments.
  - Edit the deployment of pvCSI controller and find the LOGGER_LEVEL option in the container's arguments.

    ``` sh
    kubectl edit deployment vsphere-csi-controller -n vmware-system-csi
    ```

  - Update the logger for this option from PRODUCTION to DEVELOPMENT which will give debug logs too.

- To examine the pvCSI node daemon set related issues, update the log level for the vsphere-csi-node daemon set
  - pvCSI node daemon on Tanzu Kubernetes Grid is deployed on vmware-system-csi namespace as Kubernetes DaemonSet.
  - Edit the pvCSI node daemon and find the LOGGER_LEVEL option in the container's arguments.

    ``` sh
    kubectl edit daemonset vsphere-csi-node -n vmware-system-csi
    ```

  - Update the logger for this option from PRODUCTION to DEVELOPMENT which will give debug logs too.

## Procedure to view the logs

``` sh
kubectl logs -f <pod-name> -c <container-name> -n <namespace>

<pod-name> is the name of CSI controller pod
<container-name> is the name of the container
<namespace> is where the CSI driver is deployed
```
