<!-- markdownlint-disable MD033 -->

# vSphere CSI Driver - Supported Features Matrix

In the below table,  `*` represents new features available in vSphere 7.0/CSI 2.0.

Note: vSphere with Kubernetes and Tanzu Kubernetes Grid Service support only block volumes.

| Feature                     | Vanilla Block                                                            | Vanilla File   | vSphere with Kubernetes Block | Tanzu Kubernetes Grid Service Block |
|-----------------------------|--------------------------------------------------------------------------|----------------|-----------------------|------------------------------------------|
| Dynamic provisioning       | Yes                                                                      | Yes`*`        | Yes`*`               | Yes`*`                                  |
| vSphere UI integration    | Yes                                                                      | Yes`*`        | Yes`*`               | Yes`*`                                  |
| Multi-master for CSI driver | Yes                                                                      | Yes`*`        | Yes`*`               | Yes`*`                                  |
| Datastores supported        | VMFS/NFS/vSAN    <br>vVol`*`                                     | vSAN`*`       | VMFS/NFS/vSAN`*` | VMFS/NFS/vSAN`*`                   |
| Static provisioning        | Yes                                                                      | Yes`*`        | No                    | Yes                                      |
| Access mode                 | RWO                                                                      | ROX/RWX`*`  | RWO`*`                 | RWO`*`                                  |
| Volume topology/zones     | Yes (beta)                                                               | No             | No                    | No                                       |
| Extend volume              | Yes (k8s 1.16+)`*`<br>beta                                                   | No             | No                    | No                                       |
| Encryption                  | Yes (SPBM policy driven)`*`                                                | No             | No                    | No                                       |
| K8s versions supported      | 1.14, 1.15 for CSI 1.0 features<br>1.15, 1.16, 1.17 for CSI 2.0 features | 1.16.x, 1.17.x | 1.16.x                | 1.16.x                                   |
