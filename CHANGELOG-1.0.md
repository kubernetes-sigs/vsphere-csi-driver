# Changelog since v0.2.1

## v1.0.2

### New Features (v1.0.2)

- Implement the GetVolumeStats CSI node capability ([#108](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/108), [@flofourcade](https://github.com/flofourcade))

### Other Notable Changes (v1.0.2)

- Mitigate CVE-2019-11255 by bumping external-provisioner image version to v1.2.2. [CVE announcement](https://groups.google.com/forum/#!topic/kubernetes-security-announce/aXiYN0q4uIw) ([#98](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/98), [@zuzzas](https://github.com/zuzzas))
- Remove vsphere credentials from csi node daemonset ([#96](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/96), [@RaunakShah](https://github.com/RaunakShah))
- Support for running the CSI controller outside the controlled cluster. ([#102](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/102), [@MartinWeindel](https://github.com/MartinWeindel))
- Hotfix compatibility issue on 7.0. Using the correct field name to govmomi. ([#127](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/127), [@cchengleo](https://github.com/cchengleo))
- Invoke NodeRegister only when Provider Id change is observed. ([#132](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/132), [@chethanv28](https://github.com/chethanv28))
- removing pending status check for the pod Delete ([#117](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/117), [@yuyiying](https://github.com/yuyiying))
- Replace container images to match published manifests ([#105](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/105), [@RaunakShah](https://github.com/RaunakShah))
- updating deployment yamls with v1.0.1 images ([#86](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/86), [@divyenpatel](https://github.com/divyenpatel))

## v1.0.1

### New Features (v1.0.1)

- Added CNS CSI Driver and removed FCD CSI Driver ([#52](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/52), [@divyenpatel](https://github.com/divyenpatel))
- Common library for vSphere CNS CSI Driver ([#47](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/47), [@divyenpatel](https://github.com/divyenpatel))
- Metadata Syncer for vSphere CSI Driver ([#54](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/54), [@RaunakShah](https://github.com/RaunakShah))
- FullSync for vSphere CSI Driver ([#61](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/61), [@lipingxue](https://github.com/lipingxue))

### Other Notable Changes (v1.0.1)

- Removing hostNetwork setting from CSI controller and node pods ([#71](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/71), [@divyenpatel](https://github.com/divyenpatel))
- Change timeout to 300s for the attacher and provisioner ([#67](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/67), [@chethanv28](https://github.com/chethanv28))
- Using stable vSAN Client Version - vSAN 6.7U3 ([#84](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/84), [@divyenpatel](https://github.com/divyenpatel))
- Defer closing full sync wait groups ([#85](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/85), [@RaunakShah](https://github.com/RaunakShah))
- Update tests/e2e README ([#83](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/83), [@chethanv28](https://github.com/chethanv28))
- Fix full sync e2e test : Verify Multiple PVCs are deleted/updated after full sync ([#82](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/82), [@chethanv28](https://github.com/chethanv28))
- Changing the description string for non-zone e2e tests ([#77](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/77), [@chethanv28](https://github.com/chethanv28))
- Fixing commit for golang.org/x/tools ([#81](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/81), [@divyenpatel](https://github.com/divyenpatel))
- Fix full sync test : Bring down the controller and verify volume creation is picked up after FULL SYNC ([#64](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/64), [@chethanv28](https://github.com/chethanv28))
- Integration tests for syncer ([#63](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/63), [@RaunakShah](https://github.com/RaunakShah))
- Fix ENV variable to support customization of FULL SYNC interval ([#65](https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/65), [@chethanv28](https://github.com/chethanv28))
