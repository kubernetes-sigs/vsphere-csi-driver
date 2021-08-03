<!-- markdownlint-disable MD033 -->

# vSphere CSI Driver - Supported Features Matrix

The latest vSphere CSI Driver release is recommended for better stability and performance. Please refer to the [Compatibility Matrix](compatiblity_matrix.md) for more details.

| **Feature**                                                                                 | **Supported vSphere CSI Driver Releases** | vSphere 7.0u2 | vSphere 7.0u1 | vSphere 7.0 | vSphere 6.7U3 |
|---------------------------------------------------------------------------------------------|-------------------------------------------|---------------|---------------|-------------|---------------|
| Enhanced Object Health in UI for vSAN Datastores                                            | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| Dynamic Block PV support (`Read-Write-Once` Access Mode)                                    | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| Dynamic Virtual Volume (vVOL) PV support                                                    | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| Topology/Availability Zone support (beta) (Block Volume only)                               | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| Static PV Provisioning                                                                      | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| K8s Multi-node Control Plane support                                                        | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| [WaitForFirstConsumer](https://kubernetes.io/docs/concepts/storage/storage-classes/) (beta) | v1.0.3 to v2.3.0                          | ✅             | ✅             | ✅           | ✅             |
| Offline Volume Expansion support (beta) (Block Volume only)                                 | v2.0.0 to v2.3.0                          | ✅             | ✅             | ✅           | ❌             |
| Encryption support via VMcrypt (Block Volume only)                                          | v2.0.0 to v2.3.0                          | ✅             | ✅             | ✅           | ❌             |
| Dynamic File PV support through vSAN 7.0 File Services on vSAN Datastores                   | v2.0.0 to v2.3.0                          | ✅             | ✅             | ✅           | ❌             |
| In-tree vSphere volume migration to CSI (beta)                                              | v2.1.0 to v2.3.0                          | ✅             | ✅             | ❌           | ❌             |
| Online Volume Expansion support (beta) (Block Volume only)                                  | v2.2.0 to v2.3.0                          | ✅             | ❌             | ❌           | ❌             |
| XFS Filesystem support (alpha)                                                              | v2.3.0                                    | ✅             | ✅             | ✅           | ✅             |
| Raw Block Volume support (alpha)                                                            | v2.3.0                                    | ✅             | ✅             | ✅           | ✅             |

Each feature can be categorized based on its qualification and support level.

## Alpha

* Alpha features are developer-qualified and may run into unknown issues when used extensively.
* VMware's Global Support team does **not** support issues reported for these features. Developer support will only be best-effort and prioritised accordingly.
* Not recommended for production use.

## Beta

* Beta features are more extensively qualified than Alpha features.
* These features may not support every combination of deployment or configurations.  
* VMware's Global Support team does **not** support issues reported for these features. Developer support will only be best-effort and prioritised accordingly.
* Not recommended for production use.

## GA

* GA'd features are fully qualified by VMware Inc.
* Issues reported with these features can be routed through VMware's Global Support.
* Can be used in production.

_Notes_:

* We do not recommend `beta` or `alpha` features for production use. Subsequent releases may introduce incompatible changes.
* vSphere CSI driver does not support Windows based vCenter.
* vSphere CSI driver currently does not support vSAN stretch cluster.
* vSphere CSI driver and Cloud Native Storage does not currently support Storage DRS feature in vSphere.
* Offline/Online volume expansion requires a minimum Kubernetes version of 1.16+.
* Online volume expansion requires vCenter and all ESXi hosts to be on vSphere 7.0u2 build.
* Multi-node Control Planes only supports having a single CSI Pod being active at any time (replica = 1 in controller manifest).
