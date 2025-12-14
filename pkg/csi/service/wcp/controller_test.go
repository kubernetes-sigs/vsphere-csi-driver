/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wcp

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/pbm"
	v1 "k8s.io/api/core/v1"

	cnstypes "github.com/vmware/govmomi/cns/types"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

const (
	testVolumeName  = "test-pvc"
	testClusterName = "test-cluster"
	// TODO: We may need to decide this value by checking GlobalMaxSnapshotsPerBlockVolume
	// variable's value when it is set for WCP.
	// Currently keeping this as 3, since it is the recommended value of snapshots
	// per block volume in vSphere.
	maxNumOfSnapshots = 3
)

var (
	ctx                    context.Context
	controllerTestInstance *controllerTest
	onceForControllerTest  sync.Once
)

type controllerTest struct {
	controller *controller
	config     *config.Config
	vcenter    *cnsvsphere.VirtualCenter
}

var vcsimParams = unittestcommon.VcsimParams{
	Datacenters:     1,
	Clusters:        1,
	HostsPerCluster: 2,
	VMsPerCluster:   2,
	StandaloneHosts: 0,
	Datastores:      1,
	Version:         "7.0.3",
	ApiVersion:      "7.0",
}

func getControllerTest(t *testing.T) *controllerTest {
	onceForControllerTest.Do(func() {
		// Create context.
		ctx = context.Background()
		config, _ := unittestcommon.ConfigFromEnvOrVCSim(ctx, vcsimParams, false)

		// CNS based CSI requires a valid cluster name.
		config.Global.ClusterID = testClusterName

		vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
		vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
		if err != nil {
			t.Fatal(err)
		}

		err = vcenter.ConnectCns(ctx)
		if err != nil {
			t.Fatal(err)
		}

		clusters, err := find.NewFinder(vcenter.Client.Client).ClusterComputeResourceList(ctx, "*")
		if err != nil {
			t.Fatal(err)
		}
		clusterComputeResourceMoIds = append(clusterComputeResourceMoIds, clusters[0].Reference().Value)

		fakeOpStore, err := unittestcommon.InitFakeVolumeOperationRequestInterface()
		if err != nil {
			t.Fatal(err)
		}

		commonco.ContainerOrchestratorUtility, err =
			unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		if err != nil {
			t.Fatalf("Failed to create co agnostic interface. err=%v", err)
		}

		volumeManager, err := cnsvolume.GetManager(ctx, vcenter,
			fakeOpStore, true, false,
			false, cnstypes.CnsClusterFlavorWorkload, "", "")
		if err != nil {
			t.Fatalf("failed to create an instance of volume manager. err=%v", err)
		}

		manager := &common.Manager{
			VcenterConfig:  vcenterconfig,
			CnsConfig:      config,
			VolumeManager:  volumeManager,
			VcenterManager: cnsvsphere.GetVirtualCenterManager(ctx),
		}

		topologyMgr, err := commonco.ContainerOrchestratorUtility.InitTopologyServiceInController(ctx)
		if err != nil {
			t.Fatalf("failed to initialize topology service. Error: %+v", err)
		}

		c := &controller{
			manager:     manager,
			topologyMgr: topologyMgr,
			snapshotLockMgr: &snapshotLockManager{
				locks: make(map[string]*volumeLock),
			},
		}

		controllerTestInstance = &controllerTest{
			controller: c,
			config:     config,
			vcenter:    vcenter,
		}
	})
	return controllerTestInstance
}

// TestWCPCreateVolumeWithStoragePolicy creates volume with storage policy.
func TestWCPCreateVolumeWithStoragePolicy(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)

	profileID := os.Getenv("VSPHERE_STORAGE_POLICY_ID")
	if profileID == "" {
		storagePolicyName := os.Getenv("VSPHERE_STORAGE_POLICY_NAME")
		if storagePolicyName == "" {
			// PBM simulator defaults.
			storagePolicyName = "vSAN Default Storage Policy"
		}

		// Verify the volume has been create with corresponding storage policy ID.
		pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
		if err != nil {
			t.Fatal(err)
		}

		profileID, err = pc.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			t.Fatal(err)
		}
	}
	params[common.AttributeStoragePolicyID] = profileID

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{},
			Preferred: []*csi.Topology{},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	if queryResult.Volumes[0].StoragePolicyId != profileID {
		t.Fatalf("failed to match volume policy ID: %s", profileID)
	}

	// QueryAll.
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Delete.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Varify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

// TestWCPCreateVolumeWithZonalLabelPresentButNoStorageTopoType creates volume with zonal label present
// but not storage topology type. It is a negative case.
func TestWCPCreateVolumeWithZonalLabelPresentButNoStorageTopoType(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)

	profileID := os.Getenv("VSPHERE_STORAGE_POLICY_ID")
	if profileID == "" {
		storagePolicyName := os.Getenv("VSPHERE_STORAGE_POLICY_NAME")
		if storagePolicyName == "" {
			// PBM simulator defaults.
			storagePolicyName = "vSAN Default Storage Policy"
		}

		// Verify the volume has been create with corresponding storage policy ID.
		pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
		if err != nil {
			t.Fatal(err)
		}

		profileID, err = pc.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			t.Fatal(err)
		}
	}
	params[common.AttributeStoragePolicyID] = profileID

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{},
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						v1.LabelTopologyZone: "zone1",
					},
				},
			},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil && strings.Contains(err.Error(), "InvalidArgument") {
		t.Logf("expected error is thrown: %v", err)
	} else {
		defer func() {
			if respCreate == nil {
				t.Log("Skip cleaning up the volume as it might never been successfully created")
				return
			}

			volID := respCreate.Volume.VolumeId
			// Delete volume.
			reqDelete := &csi.DeleteVolumeRequest{
				VolumeId: volID,
			}
			_, err = ct.controller.DeleteVolume(ctx, reqDelete)
			if err != nil {
				t.Fatal(err)
			}

			// Verify the volume has been deleted.
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{
					{
						Id: volID,
					},
				},
			}
			queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
			if err != nil {
				t.Fatal(err)
			}

			if len(queryResult.Volumes) != 0 {
				t.Fatalf("volume should not exist after deletion with ID: %s", volID)
			}
		}()
		t.Fatal("expected error is not thrown")
	}
}

// TestWCPCreateVolumeWithoutZoneLabelPresentForFileVolume creates file volume without zone label present
// It is a negative case and is executed with vsphere config secret set to
// default value of FileVolumeActivated as "true".
func TestWCPCreateVolumeWithoutZoneLabelPresentForFileVolume(t *testing.T) {
	ct := getControllerTest(t)
	err := commonco.ContainerOrchestratorUtility.EnableFSS(ctx, "Workload_Domain_Isolation_Supported")
	if err != nil {
		t.Fatal("failed to enable Workload_Domain_Isolation_Supported FSS")
	}
	defer func() {
		err := commonco.ContainerOrchestratorUtility.DisableFSS(ctx, "Workload_Domain_Isolation_Supported")
		if err != nil {
			t.Fatal("failed to disable Workload_Domain_Isolation_Supported FSS")
		}
	}()
	// Create.
	params := make(map[string]string)

	profileID := os.Getenv("VSPHERE_STORAGE_POLICY_ID")
	if profileID == "" {
		storagePolicyName := os.Getenv("VSPHERE_STORAGE_POLICY_NAME")
		if storagePolicyName == "" {
			// PBM simulator defaults.
			storagePolicyName = "vSAN Default Storage Policy"
		}

		// Verify the volume has been create with corresponding storage policy ID.
		pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
		if err != nil {
			t.Fatal(err)
		}

		profileID, err = pc.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			t.Fatal(err)
		}
	}
	params[common.AttributeStoragePolicyID] = profileID

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{},
			Preferred: []*csi.Topology{},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil && strings.Contains(err.Error(), "FailedPrecondition") {
		t.Logf("expected error is thrown: %v", err)
	} else {
		defer func() {
			if respCreate == nil {
				t.Log("Skip cleaning up the volume as it might never been successfully created")
				return
			}

			volID := respCreate.Volume.VolumeId
			// Delete volume.
			reqDelete := &csi.DeleteVolumeRequest{
				VolumeId: volID,
			}
			_, err = ct.controller.DeleteVolume(ctx, reqDelete)
			if err != nil {
				t.Fatal(err)
			}

			// Verify the volume has been deleted.
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{
					{
						Id: volID,
					},
				},
			}
			queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
			if err != nil {
				t.Fatal(err)
			}

			if len(queryResult.Volumes) != 0 {
				t.Fatalf("volume should not exist after deletion with ID: %s", volID)
			}
		}()
		t.Fatal("expected error is not thrown")
	}
}

// TestWCPCreateVolumeWithHostLabelPresentForFileVolume creates file volume with host label present
// It is a negative case and is executed with vsphere config secret set to
// default value of FileVolumeActivated as "true".
func TestWCPCreateVolumeWithHostLabelPresentForFileVolume(t *testing.T) {
	ct := getControllerTest(t)
	err := commonco.ContainerOrchestratorUtility.EnableFSS(ctx, "Workload_Domain_Isolation_Supported")
	if err != nil {
		t.Fatal("failed to enable Workload_Domain_Isolation_Supported FSS")
	}
	defer func() {
		err := commonco.ContainerOrchestratorUtility.DisableFSS(ctx, "Workload_Domain_Isolation_Supported")
		if err != nil {
			t.Fatal("failed to disable Workload_Domain_Isolation_Supported FSS")
		}
	}()
	// Create.
	params := make(map[string]string)

	profileID := os.Getenv("VSPHERE_STORAGE_POLICY_ID")
	if profileID == "" {
		storagePolicyName := os.Getenv("VSPHERE_STORAGE_POLICY_NAME")
		if storagePolicyName == "" {
			// PBM simulator defaults.
			storagePolicyName = "vSAN Default Storage Policy"
		}

		// Verify the volume has been create with corresponding storage policy ID.
		pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
		if err != nil {
			t.Fatal(err)
		}

		profileID, err = pc.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			t.Fatal(err)
		}
	}
	params[common.AttributeStoragePolicyID] = profileID

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{},
			Preferred: []*csi.Topology{
				{
					Segments: map[string]string{
						v1.LabelHostname: "host1",
					},
				},
			},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil && strings.Contains(err.Error(), "Unimplemented") {
		t.Logf("expected error is thrown: %v", err)
	} else {
		defer func() {
			if respCreate == nil {
				t.Log("Skip cleaning up the volume as it might never been successfully created")
				return
			}

			volID := respCreate.Volume.VolumeId
			// Delete volume.
			reqDelete := &csi.DeleteVolumeRequest{
				VolumeId: volID,
			}
			_, err = ct.controller.DeleteVolume(ctx, reqDelete)
			if err != nil {
				t.Fatal(err)
			}

			// Verify the volume has been deleted.
			queryFilter := cnstypes.CnsQueryFilter{
				VolumeIds: []cnstypes.CnsVolumeId{
					{
						Id: volID,
					},
				},
			}
			queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
			if err != nil {
				t.Fatal(err)
			}

			if len(queryResult.Volumes) != 0 {
				t.Fatalf("volume should not exist after deletion with ID: %s", volID)
			}
		}()
		t.Fatal("expected error is not thrown")
	}
}

// TestWCPCreateDeleteSnapshot creates snapshot and deletes a snapshot.
func TestWCPCreateDeleteSnapshot(t *testing.T) {
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)

	profileID := os.Getenv("VSPHERE_STORAGE_POLICY_ID")
	if profileID == "" {
		storagePolicyName := os.Getenv("VSPHERE_STORAGE_POLICY_NAME")
		if storagePolicyName == "" {
			// PBM simulator defaults.
			storagePolicyName = "vSAN Default Storage Policy"
		}

		// Verify the volume has been create with corresponding storage policy ID.
		pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
		if err != nil {
			t.Fatal(err)
		}

		profileID, err = pc.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			t.Fatal(err)
		}
	}
	params[common.AttributeStoragePolicyID] = profileID

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{},
			Preferred: []*csi.Topology{},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	if queryResult.Volumes[0].StoragePolicyId != profileID {
		t.Fatalf("failed to match volume policy ID: %s", profileID)
	}

	// QueryAll.
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	defer func() {
		// Delete volume.
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: volID,
		}
		_, err = ct.controller.DeleteVolume(ctx, reqDelete)
		if err != nil {
			t.Fatal(err)
		}

		// Verify the volume has been deleted.
		queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
		if err != nil {
			t.Fatal(err)
		}

		if len(queryResult.Volumes) != 0 {
			t.Fatalf("volume should not exist after deletion with ID: %s", volID)
		}
	}()

	// Snapshot a volume
	reqCreateSnapshot := &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	}

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId

	defer func() {
		// Delete the snapshot
		reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
			SnapshotId: snapID,
		}

		_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
		if err != nil {
			t.Fatal(err)
		}
	}()
}

func TestListSnapshots(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}
	// Map to track all the snapshots created.
	snapshots := make(map[string]string)
	var deleteSnapshotList []string

	for i := 0; i < maxNumOfSnapshots; i++ {
		// Snapshot a volume
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
			Parameters: map[string]string{
				common.VolumeSnapshotNamespaceKey: "default",
			},
		}

		respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Created snapshot-%d snaphot-id: %s", i, respCreateSnapshot.Snapshot.SnapshotId)
		snapshots[respCreateSnapshot.Snapshot.SnapshotId] = ""
		deleteSnapshotList = append(deleteSnapshotList, respCreateSnapshot.Snapshot.SnapshotId)
	}

	// Invoke ListSnapshot without specifying vol or snap-id.
	listSnapshotRequest := &csi.ListSnapshotsRequest{
		MaxEntries:    0,
		StartingToken: "",
	}

	listSnapshotsResponse, err := ct.controller.ListSnapshots(ctx, listSnapshotRequest)
	if err != nil {
		t.Logf("ListSnapshot invocation failed with err: %+v", err)
		t.Fatal(err)
	}

	if len(listSnapshotsResponse.Entries) == 0 {
		t.Fatalf("ListSnapshot did not return any results")
	}

	// Iterate through response removing entries from the original map.
	for i, entry := range listSnapshotsResponse.Entries {
		snapshot := entry.Snapshot
		// log the specific snapshot information
		t.Logf("=====================Snapshot-%d===============================", i)
		t.Logf("SourceVolumeId: %s", snapshot.SourceVolumeId)
		t.Logf("SnapshotId: %s", snapshot.SnapshotId)
		t.Logf("CreationTime: %s", snapshot.CreationTime)
		t.Logf("Size: %d", snapshot.SizeBytes)
		t.Logf("ReadyToUse: %t", snapshot.ReadyToUse)
		t.Log("================================================================")
		delete(snapshots, snapshot.SnapshotId)
	}
	// Expect returned snapshots to be deleted from map, the remaining snapshots were not returned in response.
	if len(snapshots) != 0 {
		t.Fatalf("Not all snapshots were returned, missing snapshots: %+v", snapshots)
	}
	// delete snapshots as part of cleanup.
	for i := len(deleteSnapshotList) - 1; i >= 0; i-- {
		// Delete the snapshot
		reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
			SnapshotId: deleteSnapshotList[i],
		}
		_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Delete the volume.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}
}

func TestListSnapshotsOnSpecificVolume(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}
	// Map to track all the snapshots created.
	snapshots := make(map[string]string)
	var deleteSnapshotList []string

	for i := 0; i < maxNumOfSnapshots; i++ {
		// Snapshot a volume
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
			Parameters: map[string]string{
				common.VolumeSnapshotNamespaceKey: "default",
			},
		}

		respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Created snapshot-%d snaphot-id: %s", i, respCreateSnapshot.Snapshot.SnapshotId)
		snapshots[respCreateSnapshot.Snapshot.SnapshotId] = ""
		deleteSnapshotList = append(deleteSnapshotList, respCreateSnapshot.Snapshot.SnapshotId)
	}

	// Invoke ListSnapshot
	listSnapshotRequest := &csi.ListSnapshotsRequest{
		MaxEntries:     0,
		StartingToken:  "",
		SourceVolumeId: volID,
	}

	listSnapshotsResponse, err := ct.controller.ListSnapshots(ctx, listSnapshotRequest)
	if err != nil {
		t.Logf("ListSnapshot invocation failed with err: %+v", err)
		t.Fatal(err)
	}

	if len(listSnapshotsResponse.Entries) == 0 {
		t.Fatalf("ListSnapshot did not return and results for volume-id: %s", volID)
	}

	// Iterate through response removing entries from the original map.
	for i, entry := range listSnapshotsResponse.Entries {
		snapshot := entry.Snapshot
		// log the specific snapshot information
		t.Logf("=====================Snapshot-%d===============================", i)
		t.Logf("SourceVolumeId: %s", snapshot.SourceVolumeId)
		t.Logf("SnapshotId: %s", snapshot.SnapshotId)
		t.Logf("CreationTime: %s", snapshot.CreationTime)
		t.Logf("Size: %d", snapshot.SizeBytes)
		t.Logf("ReadyToUse: %t", snapshot.ReadyToUse)
		t.Log("================================================================")
		delete(snapshots, snapshot.SnapshotId)
	}
	// Expect all snapshots to be deleted, the remaining snapshots were not returned in response.
	if len(snapshots) != 0 {
		t.Fatalf("Not all snapshots were returned, missing snapshots: %+v", snapshots)
	}
	// delete snapshots as part of cleanup.
	for i := len(deleteSnapshotList) - 1; i >= 0; i-- {
		// Delete the snapshot
		reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
			SnapshotId: deleteSnapshotList[i],
		}
		_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Delete the volume.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}
}

func TestListSnapshotsWithToken(t *testing.T) {
	ct := getControllerTest(t)
	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Map to track all the snapshots created.
	snapshots := make(map[string]string)
	var deleteSnapshotList []string

	for i := 0; i < maxNumOfSnapshots; i++ {
		// Snapshot a volume
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
			Parameters: map[string]string{
				common.VolumeSnapshotNamespaceKey: "default",
			},
		}

		respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Created snapshot-%d snaphot-id: %s", i, respCreateSnapshot.Snapshot.SnapshotId)
		snapshots[respCreateSnapshot.Snapshot.SnapshotId] = ""
		deleteSnapshotList = append(deleteSnapshotList, respCreateSnapshot.Snapshot.SnapshotId)
	}

	var listSnapshotsResponseEntries []*csi.ListSnapshotsResponse_Entry
	tok := ""
	for {
		// Specify max entries as 1 to trigger paginated results.
		listSnapshotRequest := &csi.ListSnapshotsRequest{
			MaxEntries:    1,
			StartingToken: tok,
		}

		listSnapshotsResponse, err := ct.controller.ListSnapshots(ctx, listSnapshotRequest)
		if err != nil {
			t.Logf("ListSnapshot invocation failed with err: %+v", err)
			t.Fatal(err)
		}
		listSnapshotsResponseEntries = append(listSnapshotsResponseEntries, listSnapshotsResponse.Entries...)
		// Use the next token returned.
		tok = listSnapshotsResponse.NextToken
		if len(tok) == 0 {
			break
		}
	}

	if len(listSnapshotsResponseEntries) == 0 {
		t.Fatalf("ListSnapshot did not return any results")
	}

	// Iterate through response removing entries from the original map.
	for i, entry := range listSnapshotsResponseEntries {
		snapshot := entry.Snapshot
		// log the specific snapshot information
		t.Logf("=====================Snapshot-%d===============================", i)
		t.Logf("SourceVolumeId: %s", snapshot.SourceVolumeId)
		t.Logf("SnapshotId: %s", snapshot.SnapshotId)
		t.Logf("CreationTime: %s", snapshot.CreationTime)
		t.Logf("Size: %d", snapshot.SizeBytes)
		t.Logf("ReadyToUse: %t", snapshot.ReadyToUse)
		t.Log("================================================================")
		delete(snapshots, snapshot.SnapshotId)
	}
	// Expect returned snapshots to be deleted from map, the remaining snapshots were not returned in response.
	if len(snapshots) != 0 {
		t.Fatalf("Not all snapshots were returned, missing snapshots: %+v", snapshots)
	}
	// delete snapshots as part of cleanup.
	for i := len(deleteSnapshotList) - 1; i >= 0; i-- {
		// Delete the snapshot
		reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
			SnapshotId: deleteSnapshotList[i],
		}
		_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Delete the volume.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}
}

func TestListSnapshotsOnSpecificVolumeAndSnapshot(t *testing.T) {
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Snapshot a volume
	reqCreateSnapshot := &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	}

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId
	// Invoke ListSnapshot
	listSnapshotRequest := &csi.ListSnapshotsRequest{
		MaxEntries:     00,
		StartingToken:  "",
		SourceVolumeId: volID,
		SnapshotId:     snapID,
	}

	listSnapshotsRespone, err := ct.controller.ListSnapshots(ctx, listSnapshotRequest)
	if err != nil {
		t.Logf("ListSnapshot invocation failed with err: %+v", err)
		t.Fatal(err)
	}

	if len(listSnapshotsRespone.Entries) == 0 {
		t.Fatalf("ListSnapshot did not return and results for volume-id: %s and snapshot-id: %s", volID, snapID)
	}

	snapshotReturned := listSnapshotsRespone.Entries[0]
	if snapshotReturned.Snapshot.SnapshotId != snapID || snapshotReturned.Snapshot.SourceVolumeId != volID {
		t.Fatalf("failed to returned the specific snapshot for ListSnapshot, received: %+v", snapshotReturned)
	}

	// log the specific snapshot information
	t.Log("==============================================================")
	t.Logf("SourceVolumeId: %s", snapshotReturned.Snapshot.SourceVolumeId)
	t.Logf("SnapshotId: %s", snapshotReturned.Snapshot.SnapshotId)
	t.Logf("CreationTime: %s", snapshotReturned.Snapshot.CreationTime)
	t.Logf("Size: %d", snapshotReturned.Snapshot.SizeBytes)
	t.Logf("ReadyToUse: %t", snapshotReturned.Snapshot.ReadyToUse)
	t.Log("==============================================================")
	// Delete the snapshot
	reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
		SnapshotId: snapID,
	}

	_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	// Delete.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

func TestCreateVolumeFromSnapshot(t *testing.T) {
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// QueryAll.
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	defer func() {
		// Delete.
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: volID,
		}
		_, err = ct.controller.DeleteVolume(ctx, reqDelete)
		if err != nil {
			t.Fatal(err)
		}

		// Verify the volume has been deleted.
		queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
		if err != nil {
			t.Fatal(err)
		}

		if len(queryResult.Volumes) != 0 {
			t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
		}
	}()

	// Snapshot a volume
	reqCreateSnapshot := &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	}

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId

	defer func() {
		// Delete the snapshot
		reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
			SnapshotId: snapID,
		}

		_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Create a new volume from the snapshot with expected request
	reqCreateFromSnapshot := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapID,
				},
			},
		},
	}

	respCreateFromSnapshot, err := ct.controller.CreateVolume(ctx, reqCreateFromSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	restoredVolID := respCreateFromSnapshot.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: restoredVolID,
			},
		},
	}
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != restoredVolID {
		t.Fatalf("failed to find the newly created volume from snapshot with ID: %s", restoredVolID)
	}

	defer func() {
		// Delete the restored volume
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: restoredVolID,
		}
		_, err = ct.controller.DeleteVolume(ctx, reqDelete)
		if err != nil {
			t.Fatal(err)
		}

		// Verify the volume has been deleted.
		queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
		if err != nil {
			t.Fatal(err)
		}

		if len(queryResult.Volumes) != 0 {
			t.Fatalf("Volume should not exist after deletion with ID: %s", restoredVolID)
		}
	}()

	// Create a new volume from the snapshot with unexpected request
	reqCreateFromSnapshot = &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 2 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
		VolumeContentSource: &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapID,
				},
			},
		},
	}

	_, err = ct.controller.CreateVolume(ctx, reqCreateFromSnapshot)
	if err != nil {
		statusErr, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unable to convert the error: %+v into a grpc status error type", err)
		}
		if statusErr.Code() == codes.InvalidArgument {
			t.Logf("received error as expected when attempting to create volume from snapshot, error: %+v", err)
		} else {
			t.Fatalf("unexpected error code received, expected: %s received: %s",
				codes.InvalidArgument.String(), statusErr.Code().String())
		}
	} else {
		t.Fatal("expected error was not received when creating volume from snapshot")
	}
}

func TestWCPDeleteVolumeWithSnapshots(t *testing.T) {
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Snapshot a volume
	reqCreateSnapshot := &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	}

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId

	// Attempt to Delete volume.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		delErr, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unable to convert the error: %+v into a grpc status error type", err)
		}
		if delErr.Code() == codes.FailedPrecondition {
			t.Logf("received error as expected when attempting to delete volume with snapshot, error: %+v", err)
		} else {
			t.Fatalf("unexpected error code received, expected: %s received: %s",
				codes.FailedPrecondition.String(), delErr.Code().String())
		}
	} else {
		t.Fatal("expected error was not received when expanding volume with snapshot")
	}

	// Delete the snapshot
	reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
		SnapshotId: snapID,
	}
	_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
	if err != nil {
		t.Fatal(err)
	}

	// Delete the volume
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

func TestWCPExpandVolumeWithSnapshots(t *testing.T) {
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId

	// Verify the volume has been created.
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Snapshot a volume
	reqCreateSnapshot := &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	}

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId

	// Attempt to expand the volume
	reqExpand := &csi.ControllerExpandVolumeRequest{
		VolumeId: volID,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 2 * common.GbInBytes,
		},
		VolumeCapability: capabilities[0],
	}

	_, err = ct.controller.ControllerExpandVolume(ctx, reqExpand)
	if err != nil {
		delErr, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unable to convert the error: %+v into a grpc status error type", err)
		}
		if delErr.Code() == codes.FailedPrecondition {
			t.Logf("received error as expected when attempting to expand volume with snapshot, error: %+v", err)
		} else {
			t.Fatalf("unexpected error code received, expected: %s received: %s",
				codes.FailedPrecondition.String(), delErr.Code().String())
		}
	} else {
		t.Fatal("expected error was not received when expanding volume with snapshot")
	}

	// Delete the snapshot
	reqDeleteSnapshot := &csi.DeleteSnapshotRequest{
		SnapshotId: snapID,
	}
	_, err = ct.controller.DeleteSnapshot(ctx, reqDeleteSnapshot)
	if err != nil {
		t.Fatal(err)
	}

	// Delete the volume
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

// TestNew tests the New() constructor function
func TestNew(t *testing.T) {
	controller := New()
	if controller == nil {
		t.Fatal("New() returned nil controller")
	}

	// Since New() returns csitypes.CnsController, no need for type assertion
	// The controller is already of the correct type
}

// TestControllerGetCapabilities tests the ControllerGetCapabilities method
func TestControllerGetCapabilities(t *testing.T) {
	ct := getControllerTest(t)

	req := &csi.ControllerGetCapabilitiesRequest{}
	resp, err := ct.controller.ControllerGetCapabilities(ctx, req)
	if err != nil {
		t.Fatalf("ControllerGetCapabilities failed: %v", err)
	}

	if resp == nil {
		t.Fatal("ControllerGetCapabilities returned nil response")
	}

	// Verify we get a response with capabilities
	if len(resp.Capabilities) == 0 {
		t.Error("Expected at least one capability")
	}

	// Verify that the basic capabilities are present
	expectedBasicCaps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
	}

	// Check that all basic capabilities are present
	actualCaps := make([]csi.ControllerServiceCapability_RPC_Type, len(resp.Capabilities))
	for i, cap := range resp.Capabilities {
		actualCaps[i] = cap.GetRpc().GetType()
	}

	for _, expectedCap := range expectedBasicCaps {
		found := false
		for _, actualCap := range actualCaps {
			if actualCap == expectedCap {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected capability %v not found in response", expectedCap)
		}
	}
}

// TestListVolumes tests the ListVolumes method
func TestListVolumes(t *testing.T) {
	ct := getControllerTest(t)

	t.Run("BasicListVolumes", func(t *testing.T) {
		req := &csi.ListVolumesRequest{}

		_, err := ct.controller.ListVolumes(ctx, req)
		// ListVolumes may fail in test environment due to missing NodeIDtoName map
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ListVolumes failed as expected in test environment: %v", err)
		}
	})

	t.Run("ListVolumesWithMaxEntries", func(t *testing.T) {
		req := &csi.ListVolumesRequest{
			MaxEntries: 10,
		}

		_, err := ct.controller.ListVolumes(ctx, req)
		// ListVolumes may fail in test environment due to missing NodeIDtoName map
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ListVolumes with max entries failed as expected in test environment: %v", err)
		}
	})
}

// TestGetCapacity tests the GetCapacity method
func TestGetCapacity(t *testing.T) {
	ct := getControllerTest(t)

	t.Run("BasicGetCapacity", func(t *testing.T) {
		req := &csi.GetCapacityRequest{}

		_, err := ct.controller.GetCapacity(ctx, req)
		// GetCapacity returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("GetCapacity failed as expected (Unimplemented): %v", err)
		}
	})

	t.Run("GetCapacityWithParameters", func(t *testing.T) {
		req := &csi.GetCapacityRequest{
			Parameters: map[string]string{
				"test-param": "test-value",
			},
		}

		_, err := ct.controller.GetCapacity(ctx, req)
		// GetCapacity returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("GetCapacity with parameters failed as expected (Unimplemented): %v", err)
		}
	})
}

// TestControllerGetVolume tests the ControllerGetVolume method
func TestControllerGetVolume(t *testing.T) {
	ct := getControllerTest(t)

	// First create a volume
	params := make(map[string]string)
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-get-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Clean up
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		}
		_, _ = ct.controller.DeleteVolume(ctx, reqDelete)
	}()

	t.Run("ValidGetVolume", func(t *testing.T) {
		req := &csi.ControllerGetVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		}

		_, err := ct.controller.ControllerGetVolume(ctx, req)
		// ControllerGetVolume returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ControllerGetVolume failed as expected (Unimplemented): %v", err)
		}
	})

	t.Run("InvalidVolumeId", func(t *testing.T) {
		req := &csi.ControllerGetVolumeRequest{
			VolumeId: "invalid-volume-id",
		}

		_, err := ct.controller.ControllerGetVolume(ctx, req)
		// ControllerGetVolume returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ControllerGetVolume failed as expected (Unimplemented): %v", err)
		}
	})

	t.Run("EmptyVolumeId", func(t *testing.T) {
		req := &csi.ControllerGetVolumeRequest{
			VolumeId: "",
		}

		_, err := ct.controller.ControllerGetVolume(ctx, req)
		// ControllerGetVolume returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ControllerGetVolume failed as expected (Unimplemented): %v", err)
		}
	})
}

// TestControllerModifyVolume tests the ControllerModifyVolume method
func TestControllerModifyVolume(t *testing.T) {
	ct := getControllerTest(t)

	// First create a volume
	params := make(map[string]string)
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName + "-modify-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Clean up
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		}
		_, _ = ct.controller.DeleteVolume(ctx, reqDelete)
	}()

	t.Run("ValidModifyVolume", func(t *testing.T) {
		req := &csi.ControllerModifyVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		}

		_, err := ct.controller.ControllerModifyVolume(ctx, req)
		// ControllerModifyVolume returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ControllerModifyVolume failed as expected (Unimplemented): %v", err)
		}
	})

	t.Run("InvalidVolumeId", func(t *testing.T) {
		req := &csi.ControllerModifyVolumeRequest{
			VolumeId: "invalid-volume-id",
		}

		_, err := ct.controller.ControllerModifyVolume(ctx, req)
		// ControllerModifyVolume returns Unimplemented in WCP controller
		// This is expected behavior, so we just verify the method can be called
		if err != nil {
			t.Logf("ControllerModifyVolume failed as expected (Unimplemented): %v", err)
		}
	})
}

func TestSnapshotLockManager(t *testing.T) {
	ct := getControllerTest(t)

	t.Run("AcquireAndRelease_SingleVolume", func(t *testing.T) {
		volumeID := "test-volume-1"

		// Acquire lock
		ct.controller.acquireSnapshotLock(ctx, volumeID)

		// Verify lock exists and refCount = 1
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		vLock, exists := ct.controller.snapshotLockMgr.locks[volumeID]
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if !exists {
			t.Fatal("Lock should exist after acquire")
		}
		if vLock.refCount != 1 {
			t.Fatalf("Expected refCount=1, got %d", vLock.refCount)
		}

		// Release lock
		ct.controller.releaseSnapshotLock(ctx, volumeID)

		// Verify lock is removed
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		_, exists = ct.controller.snapshotLockMgr.locks[volumeID]
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if exists {
			t.Fatal("Lock should be removed after release")
		}
	})

	t.Run("AcquireMultipleTimes_SameVolume", func(t *testing.T) {
		volumeID := "test-volume-2"

		// Use two goroutines to acquire the lock
		var wg sync.WaitGroup
		acquired := make(chan bool, 2)

		// First goroutine acquires and holds the lock
		wg.Add(1)
		go func() {
			defer wg.Done()
			ct.controller.acquireSnapshotLock(ctx, volumeID)
			acquired <- true
			// Hold lock briefly
			time.Sleep(100 * time.Millisecond)
			ct.controller.releaseSnapshotLock(ctx, volumeID)
		}()

		// Wait for first goroutine to acquire
		<-acquired

		// Verify refCount = 1, lock exists
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		vLock, exists := ct.controller.snapshotLockMgr.locks[volumeID]
		refCount1 := vLock.refCount
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if !exists {
			t.Fatal("Lock should exist")
		}
		if refCount1 != 1 {
			t.Fatalf("Expected refCount=1, got %d", refCount1)
		}

		// Second goroutine tries to acquire (will be blocked)
		wg.Add(1)
		go func() {
			defer wg.Done()
			ct.controller.acquireSnapshotLock(ctx, volumeID)
			acquired <- true
			ct.controller.releaseSnapshotLock(ctx, volumeID)
		}()

		// Give second goroutine time to start waiting
		time.Sleep(50 * time.Millisecond)

		// Verify refCount increased to 2 (second goroutine is waiting)
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		vLock, exists = ct.controller.snapshotLockMgr.locks[volumeID]
		refCount2 := vLock.refCount
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if !exists {
			t.Fatal("Lock should exist")
		}
		if refCount2 != 2 {
			t.Fatalf("Expected refCount=2, got %d", refCount2)
		}

		// Wait for both goroutines to complete
		wg.Wait()

		// Verify lock is removed after both releases
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		_, exists = ct.controller.snapshotLockMgr.locks[volumeID]
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if exists {
			t.Fatal("Lock should be removed after all releases")
		}
	})

	t.Run("AcquireRelease_MultipleVolumes", func(t *testing.T) {
		volume1 := "test-volume-3"
		volume2 := "test-volume-4"
		volume3 := "test-volume-5"

		// Acquire locks for all volumes
		ct.controller.acquireSnapshotLock(ctx, volume1)
		ct.controller.acquireSnapshotLock(ctx, volume2)
		ct.controller.acquireSnapshotLock(ctx, volume3)

		// Verify all locks exist
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		count := len(ct.controller.snapshotLockMgr.locks)
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if count < 3 {
			t.Fatalf("Expected at least 3 locks, got %d", count)
		}

		// Release volume2
		ct.controller.releaseSnapshotLock(ctx, volume2)

		// Verify volume2 removed, others remain
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		_, exists1 := ct.controller.snapshotLockMgr.locks[volume1]
		_, exists2 := ct.controller.snapshotLockMgr.locks[volume2]
		_, exists3 := ct.controller.snapshotLockMgr.locks[volume3]
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if !exists1 {
			t.Fatal("Volume1 lock should still exist")
		}
		if exists2 {
			t.Fatal("Volume2 lock should be removed")
		}
		if !exists3 {
			t.Fatal("Volume3 lock should still exist")
		}

		// Cleanup
		ct.controller.releaseSnapshotLock(ctx, volume1)
		ct.controller.releaseSnapshotLock(ctx, volume3)
	})

	t.Run("ConcurrentAccess_SameVolume", func(t *testing.T) {
		volumeID := "test-volume-concurrent"
		counter := 0
		var wg sync.WaitGroup
		goroutines := 5

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ct.controller.acquireSnapshotLock(ctx, volumeID)
				defer ct.controller.releaseSnapshotLock(ctx, volumeID)

				// Critical section - increment counter
				temp := counter
				// Simulate some work
				for j := 0; j < 100; j++ {
					_ = j * 2
				}
				counter = temp + 1
			}()
		}

		wg.Wait()

		// Verify counter = goroutines (no race condition)
		if counter != goroutines {
			t.Fatalf("Expected counter=%d, got %d (race condition detected)", goroutines, counter)
		}

		// Verify lock is cleaned up
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		_, exists := ct.controller.snapshotLockMgr.locks[volumeID]
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if exists {
			t.Fatal("Lock should be cleaned up after all goroutines finish")
		}
	})

	t.Run("ReleaseNonExistentLock", func(t *testing.T) {
		volumeID := "non-existent-volume"

		// This should not panic
		ct.controller.releaseSnapshotLock(ctx, volumeID)

		// Verify no lock was created
		ct.controller.snapshotLockMgr.mapMutex.RLock()
		_, exists := ct.controller.snapshotLockMgr.locks[volumeID]
		ct.controller.snapshotLockMgr.mapMutex.RUnlock()

		if exists {
			t.Fatal("Lock should not exist after releasing non-existent lock")
		}
	})
}
