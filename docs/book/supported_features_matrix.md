<!-- markdownlint-disable MD033 -->

# vSphere CSI Driver - Supported Features Matrix

The latest vSphere CSI Driver release is recommended for better stability and performance. Please refer to [Compatibility Matrix](compatiblity_matrix.md) for more detail.

| **Feature**                                                                                 | **Supported vSphere CSI Driver Releases** | vSphere 7.0u2 | vSphere 7.0u1 | vSphere 7.0 | vSphere 6.7U3 |
|---------------------------------------------------------------------------------------------|-------------------------------------------|---------------|---------------|-------------|---------------|
| Enhanced Object Health in UI for vSAN Datastores                                            | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| Dynamic Block PV support (`Read-Write-Once` Access Mode)                                    | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| Dynamic Virtual Volume (vVOL) PV support                                                    | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| Topology/Availability Zone support (beta) (Block Volume only)                               | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| Static PV Provisioning                                                                      | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| K8s Multi-node Control Plane support                                                        | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| [WaitForFirstConsumer](https://kubernetes.io/docs/concepts/storage/storage-classes/) (beta) | v1.0.3 to v2.2.1                          | ✅             | ✅             | ✅           | ✅             |
| Offline Volume Expansion support (beta) (Block Volume only)                                 | v2.0.0 to v2.2.1                          | ✅             | ✅             | ✅           | ❌             |
| Encryption support via VMcrypt (Block Volume only)                                          | v2.0.0 to v2.2.1                          | ✅             | ✅             | ✅           | ❌             |
| Dynamic File PV support through vSAN 7.0 File Services on vSAN Datastores                   | v2.0.0 to v2.2.1                          | ✅             | ✅             | ✅           | ❌             |
| In-tree vSphere volume migration to CSI (beta)                                              | v2.1.0 to v2.2.1                          | ✅             | ✅             | ❌           | ❌             |
| Online Volume Expansion support (beta) (Block Volume only)                                  | v2.2.0 to v2.2.1                          | ✅             | ❌             | ❌           | ❌             |

_Notes_:

* We do not recommend `beta` features for production use. Subsequent releases may introduce incompatible changes.
* vSphere CSI driver does not support Windows based vCenter.
* vSphere CSI driver currently does not support vSAN stretch cluster.
* vSphere CSI driver and Cloud Native Storage does not currently support Storage DRS feature in vSphere.
* Offline/Online volume expansion requires a minimum Kubernetes version of 1.16+.
* Online volume expansion requires vCenter and all ESXi hosts to be on vSphere 7.0u2 build.
* Multi-node Control Planes only supports having a single CSI Pod being active at any time (replica = 1 in controller manifest).
