<!-- markdownlint-disable MD033 -->

# vSphere CSI Driver - Supported Features Matrix

In the below table - `*` represent features newly available in vSphere 7.0/CSI 2.0

Note: Project pacific and Tanzu Kubernetes Grid support only block volumes.

| Feature                     | Vanilla Block                                                            | Vanilla File   | Project pacific Block | Tanzu K8s Grid for Project pacific Block |
|-----------------------------|--------------------------------------------------------------------------|----------------|-----------------------|------------------------------------------|
| Dynamic Provisioning        | Yes                                                                      | Yes `*`        | Yes `*`               | Yes `*`                                  |
| vSphere UI integration      | Yes                                                                      | Yes `*`        | Yes `*`               | Yes `*`                                  |
| Multi-master for CSI driver | Yes                                                                      | Yes `*`        | Yes `*`               | Yes `*`                                  |
| Datastores supported        | vmfs/nfs/vSAN/(vVOL `*`)                                                 | vSAN `*`       | vmfs/nfs/vSAN `*`     | vmfs/nfs/vSAN `*`                        |
| Static Provisioning         | Yes                                                                      | Yes `*`        | No                    | Yes                                      |
| Access mode                 | RWO                                                                      | ROX,RWX `*`    | RWO*                  | RWO `*`                                  |
| Volume Topology/Zones       | Yes (beta)                                                               | No             | No                    | No                                       |
| Extend Volume               | Yes (k8s 1.16+)* <br>beta                                                   | No             | No                    | No                                       |
| Encryption                  | Yes (SPBM policy driven)*                                                | No             | No                    | No                                       |
| K8s versions supported      | 1.14, 1.15 for CSI 1.0 features<br>1.15, 1.16, 1.17 for CSI 2.0 features | 1.16.x, 1.17.x | 1.16.x                | 1.16.x                                   |
