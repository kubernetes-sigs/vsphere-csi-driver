# vSphere CSI Driver - Upgrade Support Matrix

vSphere CSI driver can be upgraded from v1.0.1 to v1.0.2 and v2.0.0 on vSphere 67u3.
When upgrading vSphere from 67u3 to 7.0, follow the upgrade support matrix.

|  vSphere Version/CSI Driver Version  |  To vSphere 7.0/CSI Driver v1.0.1  |  To vSphere 7.0/CSI Driver v1.0.2  |  To vSphere 7.0/CSI Driver v2.0.0  |
|---|---|---|---|
|  From vSphere 67u3 / CSI Driver v1.0.1  |  No  |  Yes  |  Yes  |
|  From vSphere 67u3 / CSI Driver v1.0.2  |  -  |  Yes  |  Yes  |

Rolling upgrade is supported for the vSphere CSI driver from v1.0.1 to v1.0.2.

With vSphere CSI driver v2.0.0, deployment is changed from StatefulSet to deployment to support leader election for multi-master. Due to this change, the driver pod cannot be upgraded with the upgrade strategy - RollingUpdate..

To upgrade from v1.0.1/v1.0.2 to v2.0.0, the driver needs to be removed and re-deployed using [new YAMLs](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/vsphere-7.0/vanilla).

Note: vSphere CSI driver v2.0.0 requires new set of roles and privileges on vSphere 7.0. Refer to updated [roles and privileges requirement](driver-deployment/prerequisites.md).
