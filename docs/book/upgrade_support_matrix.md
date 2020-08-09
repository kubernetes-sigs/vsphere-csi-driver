# vSphere CSI Driver - Upgrade Support Matrix

vSphere CSI driver can be upgraded from v1.0.1 to v1.0.2 and v2.0.0 on vSphere 67u3.
When vSphere is upgraded from 67u3 to 7.0, consider following upgrade support matrix.

|  vSphere Version / CSI Driver Version  |  To vSphere 7.0 / CSI Driver v1.0.1  |  To vSphere 7.0 / CSI Driver v1.0.2  |  To vSphere 7.0 / CSI Driver v2.0.0  |
|---|---|---|---|
|  From vSphere 67u3 / CSI Driver v1.0.1  |  No  |  Yes  |  Yes  |
|  From vSphere 67u3 / CSI Driver v1.0.2  |  -  |  Yes  |  Yes  |

Rolling upgrade is supported for vSphere CSI Driver from v1.0.1 to v1.0.2.

With vSphere CSI driver v2.0.0, deployment is changed from StatefulSet to Deployment to support leader election for multi-master. Due to this change, driver pod cannot be upgraded with upgrade strategy - RollingUpdate.

To upgrade driver from v1.0.1/v1.0.2 to v2.0.0, driver needs to be removed and re-deployed using [new YAMLs](https://github.com/kubernetes-sigs/vsphere-csi-driver/tree/master/manifests/vsphere-7.0/vanilla).

Note:

- vSphere CSI driver v2.0.0 requires new set of roles and privileges on vSphere 7.0, please refer updated [roles and privileges requirement](driver-deployment/prerequisites.md).
- vSphere CSI driver is not supported on Windows based vCenter.
