# vSphere CSI Driver - Limits

| Limits                                              | vanilla block                     | vanilla File                         | Pacific supervisor cluster | Tanzu Kubernetes Grid |
|-----------------------------------------------------|-----------------------------------|--------------------------------------|----------------------------|-----------------------|
| Scale                                               | 10000 volumes for vsan, nfs, vmfs | 32 File shares (5 clients per share) | 7000 volumes               | 7000 volumes          |
|                                                     | 840 volumes for vVOL              |                                      |                            |                       |
| Number of PVCs per VMwith 4 controllers             | Max 59                            | N/A                                  | Max 29                     | Max 59                |
| Multiple instances of CSI pods in Multi-master mode | replica = 1                       | replica = 1                          | replica = 1                | replica = 1           |

Note: Only a single vCenter is supported by vSphere CSI Driver. To use vSphere CSI driver, make sure node VMs do not spread across multiple vCenter servers.
