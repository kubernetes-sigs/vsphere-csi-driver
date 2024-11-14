package common

import (
	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/vcenter"
)

type Options struct {
	ClusterFlavor   cnstypes.CnsClusterFlavor
	VCenterProvider vcenter.Provider
	CryptoClient    crypto.Client
	VolumeManager   volume.Manager
}
