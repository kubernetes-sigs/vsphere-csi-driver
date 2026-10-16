package volume_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

// TestDeleteVolumeIgnoresStaleCachedSuccess verifies that DeleteVolume does not
// trust a cached "delete succeeded" CnsVolumeOperationRequest when the volume
// still exists in CNS. This happens when the same volume ID is created or
// registered again (e.g. Data Protection restore) shortly after it was deleted.
func TestDeleteVolumeIgnoresStaleCachedSuccess(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	cfg, _ := unittestcommon.ConfigFromEnvOrVCSim(ctx, unittestcommon.VcsimParams{
		Datacenters:     1,
		Clusters:        1,
		HostsPerCluster: 2,
		VMsPerCluster:   2,
		Datastores:      1,
		Version:         "7.0.3",
		ApiVersion:      "7.0",
	}, false)
	cfg.Global.ClusterID = "test-cluster"

	vcConfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	require.NoError(t, err)
	vc, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, vcConfig, false)
	require.NoError(t, err)
	require.NoError(t, vc.ConnectCns(ctx))
	defer func() { _ = vc.Disconnect(ctx) }()

	opStore, err := unittestcommon.InitFakeVolumeOperationRequestInterface()
	require.NoError(t, err)
	mgr, err := cnsvolume.GetManager(ctx, vc, opStore, true, false, false,
		cnstypes.CnsClusterFlavorWorkload, "", "")
	require.NoError(t, err)
	// The list view is initialised asynchronously and CreateVolume/DeleteVolume
	// wait on it, so wait (bounded) for it to be ready to avoid flakiness.
	require.Eventually(t, mgr.IsListViewReady, 30*time.Second, 100*time.Millisecond,
		"list view did not become ready")

	datastores, err := find.NewFinder(vc.Client.Client).DatastoreList(ctx, "*")
	require.NoError(t, err)
	require.NotEmpty(t, datastores)
	var ds mo.Datastore
	require.NoError(t, vc.Client.RetrieveOne(ctx, datastores[0].Reference(), []string{"info"}, &ds))
	dcs, err := vc.GetDatacenters(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, dcs)
	dsInfo, err := dcs[0].GetDatastoreInfoByURL(ctx, ds.Info.GetDatastoreInfo().Url)
	require.NoError(t, err)

	createVolume := func(name string) string {
		info, _, err := mgr.CreateVolume(ctx, &cnstypes.CnsVolumeCreateSpec{
			Name:       name,
			VolumeType: "BLOCK",
			Datastores: []vimtypes.ManagedObjectReference{dsInfo.Datastore.Reference()},
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster: cnstypes.CnsContainerCluster{
					ClusterType: string(cnstypes.CnsClusterTypeKubernetes),
					ClusterId:   cfg.Global.ClusterID,
				},
				ContainerClusterArray: []cnstypes.CnsContainerCluster{{
					ClusterType: string(cnstypes.CnsClusterTypeKubernetes),
					ClusterId:   cfg.Global.ClusterID,
				}},
			},
			BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{CapacityInMb: 1024},
			},
		}, nil)
		require.NoError(t, err)
		return info.VolumeID.Id
	}
	volumeExists := func(volumeID string) bool {
		res, err := vc.CnsClient.QueryVolume(ctx, &cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
		})
		require.NoError(t, err)
		return len(res.Volumes) > 0
	}
	seedCachedDeleteSuccess := func(volumeID string) {
		name := "delete-" + volumeID
		require.NoError(t, opStore.StoreRequestDetails(ctx,
			cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(name, "", "", 0, nil,
				metav1.Now(), "task-1", "", "", "Success", "", "")))
	}

	t.Run("volume still in CNS: stale cached success is ignored and volume is deleted", func(t *testing.T) {
		volumeID := createVolume("stale-cache-vol")
		require.True(t, volumeExists(volumeID))
		seedCachedDeleteSuccess(volumeID)

		_, err := mgr.DeleteVolume(ctx, volumeID, true)
		assert.NoError(t, err)
		assert.False(t, volumeExists(volumeID), "volume must be removed from CNS, not leaked")
	})

	t.Run("volume absent from CNS: cached success is honoured", func(t *testing.T) {
		volumeID := createVolume("absent-vol")
		_, err := mgr.DeleteVolume(ctx, volumeID, true)
		require.NoError(t, err)
		require.False(t, volumeExists(volumeID))

		// The delete above stored a Success record. A repeat delete is a no-op.
		_, err = mgr.DeleteVolume(ctx, volumeID, true)
		assert.NoError(t, err)
	})
}
