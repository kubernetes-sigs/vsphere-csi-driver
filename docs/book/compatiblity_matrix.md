# Compatibility Matrix for vSphere CSI Driver

- vSphere CSI driver is released with specific version of CSI sidecar containers which has the minimum kubernetes version requirement.

| vSphere CSI Driver | Minimum Kubernetes Version |
|--------------------|----------------------------|
| v1.0.2             | v1.14.0                    |
| v1.0.3             | v1.14.0                    |
| v2.0.0             | v1.16.0                    |
| v2.0.1             | v1.17.0                    |
| v2.1.0             | v1.17.0                    |

- vSphere CSI driver is compatible with vSphere 67u3, vSphere 7.0 and 7.0u1.
  - vSphere CSI Driver is backward and forward compatible to vSphere releases
  - For bug fixes and performance improvements, user can deploy the latest vSphere CSI driver without upgrading vSphere.
  - Features added in the newer vSphere releases does not work on the older vSphere CSI driver. Refer to [feature matrix](supported_features_matrix.md) to learn about what features added in each release of vSphere and CSI driver.

- vSphere CSI driver is not supported on Windows based vCenter.
- vSphere CSI driver is not supported on vSAN stretch cluster.
