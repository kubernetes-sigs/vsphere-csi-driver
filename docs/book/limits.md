# vSphere CSI Driver - Limits

| Limits                                              | vanilla block                                                                                   | vanilla File                                          |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Number of Volumes                                   | 10000 volumes per vCenter for vSAN, NFSv3, VMFS type datastores and 840 volumes per vCenter for vVOL type datastores                                 | 32 File shares per vSAN Cluster (5 clients per share) |
| Number of Block PVs per VM with 4 controllers       | Max 59 (with 4 Paravirtual SCSI controllers on VM with 1 slot used for primary disk of Node VM) | N/A                                                   |
| Multiple instances of CSI pods in Multi-master mode | replica = 1                                                                                     | replica = 1                                           |

Note:

- Only a single vCenter is supported by vSphere CSI Driver. To use vSphere CSI driver, make sure node VMs do not spread across multiple vCenter servers.
- vSphere CSI driver only uses Paravirtual SCSI controllers to attach volumes to Node VM, so each non Paravirtual SCSI controller on the Node VM reduces the max limit for block volume per node by 15.
