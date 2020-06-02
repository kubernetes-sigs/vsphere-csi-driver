# vSphere CSI Driver - Limits

| Limits                                              | Vanilla Cluster Block             | Vanilla Cluster File                 | vSphere with Kubernetes Supervisor Cluster | Tanzu Kubernetes Grid Cluster |
| --------------------------------------------------- | --------------------------------- | ------------------------------------ | ------------------------------------------ | ----------------------------- |
| Scale                                               | 10000 volumes for vSAN, NFS, VMFS | 32 file shares (5 clients per share) | 7000 volumes                               | 7000 volumes                  |
|                                                     | 840 volumes for vVol              |                                      |                                            |                               |
| Number of PVCs per VM with 4 controllers            | Max 59                            | N/A                                  | Max 29                                     | Max 59                        |
| Multiple instances of CSI pods in multi-master mode | replica = 1                       | replica = 1                          | replica = 1                                | replica = 1                   |

Note: Only a single vCenter Server instance is supported by the vSphere CSI driver. To use the vSphere CSI driver, make sure node VMs do not spread across multiple vCenter Server instances.
