<!-- markdownlint-disable MD033 -->

# vSphere CSI Driver - Supported Features Matrix

| | **Native K8s on vSphere 6.7U3 (CSI `1.0.2` & `2.0`)** | **Native K8s on vSphere 7.0 (CSI `2.0`)** | **vSphere with Tanzu – Supervisor Cluster** | **vSphere with Tanzu – TKG Service ‘Guest’ Cluster** |
|---------------------------------------------------------|----------------------|------------------|--------------------|------------------|
| CNS UI Support                                          | Yes                  | Yes              | Yes                | Yes              |
| Enhanced Object Health in UI                            | Yes (_vSAN only_)    | Yes (_vSAN only_)| Yes (_vSAN only_)  | Yes (_vSAN only_)|
| Dynamic Block PV support (`Read-Write-Once` Access Mode)| Yes                  | Yes              | Yes                | Yes              |
| Dynamic File PV support (`Read-Write-Many` Access Mode) | No                   | Yes (_vSAN only_)| No                 | No               |
| Encryption support via VMcrypt                          | No                   | Yes (_Block PV_) | No                 | No               |
| Dynamic Virtual Volume PV support                       | No                   | Yes              | No                 | No               |
| Offline Volume Grow support (beta)                      | No                   | Yes (_Block PV_) | No                 | No               |
| Topology/Availability Zone support                      | Yes (_Block PV_)     | Yes (_Block PV_) | No                 | No               |
| Static PV Provisioning                                  | Yes                  | Yes              | No                 | Yes              |
| K8s Multi-node Control Plane support                    | Yes                  | Yes              | Yes                | Yes              |
| `WaitForFirstConsumer`                                  | Yes                  | Yes              | No                 | No               |

_Notes_:

* Native K8s is any distribution that uses vanilla upstream Kubernetes binaries and pods (e.g. VMware TKG, TKGI, etc)
* If the CSI version `2.0` driver is installed on K8s running on vSphere 6.7U3, the older CSI `1.0` driver features continue to work but the new CSI `2.0` features are not supported.
* If the CSI version `1.0.2` is installed on K8s running on vSphere 7.0, the CSI `1.0` driver features continue to work. CSI version `1.0.1` is not compatible with vSphere 7.
* CSI version `1.0.x` and CSI version `2.0` on vSphere 6.7U3, vSphere with K8s, TKG ‘Guest’ and PKS 1.7 only support dynamically provisioned block (`RWO`) volumes on vSphere storage.
* The dynamic creation of read-write-many (`RWX`) and read-only-many (`ROX`) file based Persistent Volumes is only available in vSphere 7.0 through vSAN 7.0 File Services.
* PV Encryption, offline volume grow and Topology/AZ support are only available on block based Persistent Volumes. These features are not available with PVs backed by file shares.
* Offline volume grow requires a minimum Kubernetes version of 1.16+, and is also a beta feature in this release.
* Multi-node Control Planes only supports having a single CSI Pod being active at any time (replica = 1 in controller manifest).
* [`WaitForFirstConsumer`](https://kubernetes.io/docs/concepts/storage/storage-classes/) is a K8s feature that delays volume binding until the Pod has been scheduled, and is used for Pod and PV placement.
