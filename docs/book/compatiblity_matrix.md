# Compatibility Matrix for vSphere CSI Driver

- vSphere CSI driver is released with specific version of CSI sidecar containers which dictates the minimum and maximum Kubernetes version requirement.

Note:

- VMware supports the deprecated release of the driver until the End Of Life date.

| vSphere CSI Driver | Minimum Kubernetes Release | Maximum Kubernetes Release            | Deprecated | End of Life  |
|--------------------|----------------------------|---------------------------------------| -----------|--------------|
| [v2.3.0](./releases/v2.3.0.md)             | 1.19                    | 1.21       | No         | -            |
| [v2.2.1](./releases/v2.2.1.md)             | 1.18                    | 1.20       | Yes        | August 2022  |
| [v2.2.0](./releases/v2.2.0.md)             | 1.18                    | 1.20       | Yes        | August 2022  |
| [v2.1.1](./releases/v2.1.1.md)             | 1.17                    | 1.19       | Yes        | August 2022  |
| [v2.1.0](./releases/v2.1.0.md)             | 1.17                    | 1.19       | Yes        | August 2022  |
| [v2.0.1](./releases/v2.0.1.md)             | 1.17                    | 1.19       | Yes        | January 2022 |
| [v2.0.0](./releases/v2.0.0.md)             | 1.16                    | 1.18       | Yes        | January 2022 |
| [v1.0.3](./releases/v1.0.3.md)             | 1.14                    | 1.16       | Yes        | June 2021    |
| [v1.0.2](./releases/v1.0.2.md)             | 1.14                    | 1.16       | Yes        | January 2021 |

- vSphere CSI driver is compatible with vSphere 67u3, vSphere 7.0, 7.0u1 and 7.0u2. If you have a newer vCenter version but older ESXi hosts, new features added in the newer vCenter will not work until all the ESXi hosts are upgraded to the newer version.
  - For bug fixes and performance improvements, user can deploy the latest vSphere CSI driver without upgrading vSphere.
  - Features added in the newer vSphere releases does not work on the older vSphere CSI driver. Refer to [feature matrix](supported_features_matrix.md) to learn about what features added in each release of vSphere and CSI driver.

- vSphere CSI driver is not supported on Windows based vCenter.
- vSphere CSI driver does not support Windows based nodes.
- vSphere CSI driver is not supported on vSAN stretch cluster.
- vSphere CSI driver does not support provisioning volumes on NFSv4 Datastore.
- vSphere CSI driver does not currently support vCenter HA.
- vSphere CSI driver does not currently support Storage vMotion.
