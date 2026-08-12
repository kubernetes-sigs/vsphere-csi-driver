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
	"fmt"
	"os"
	"reflect"
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
	vim25types "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	cnstypes "github.com/vmware/govmomi/cns/types"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsvolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
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

		// Initialize a fake k8s client for tests with a default ConfigMap for snapshot limits
		defaultConfigMap := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      common.ConfigMapCSILimits,
				Namespace: "default",
			},
			Data: map[string]string{
				common.ConfigMapKeyMaxSnapshotsPerVolume: "32",
			},
		}
		fakeK8sClient := fake.NewClientset(defaultConfigMap)

		c := &controller{
			manager:     manager,
			topologyMgr: topologyMgr,
			snapshotLockMgr: &snapshotLockManager{
				locks: make(map[string]*volumeLock),
			},
			k8sClient: fakeK8sClient,
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

// TestGetDatastoresForHostLocalLinkedClone verifies the datastore resolution used by the host-local
// linked-clone-from-snapshot path: it must resolve to the single datastore of the source volume when
// the request is an eligible linked-clone-from-snapshot request, and return nil otherwise.
func TestGetDatastoresForHostLocalLinkedClone(t *testing.T) {
	ct := getControllerTest(t)

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
		Parameters:         map[string]string{},
		VolumeCapabilities: capabilities,
	}
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	defer func() {
		if _, err := ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volID}); err != nil {
			t.Fatal(err)
		}
	}()

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
		if _, err := ct.controller.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapID}); err != nil {
			t.Fatal(err)
		}
	}()

	contentSource := &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapID},
		},
	}

	t.Run("nil_when_no_content_source", func(t *testing.T) {
		datastores, err := ct.controller.getDatastoresForHostLocalLinkedClone(ctx,
			&csi.CreateVolumeRequest{}, true, true)
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if datastores != nil {
			t.Fatalf("expected nil datastores, got: %+v", datastores)
		}
	})

	t.Run("nil_when_linked_clone_support_disabled", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{VolumeContentSource: contentSource}
		datastores, err := ct.controller.getDatastoresForHostLocalLinkedClone(ctx, req, false, true)
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if datastores != nil {
			t.Fatalf("expected nil datastores, got: %+v", datastores)
		}
	})

	t.Run("nil_when_not_a_linked_clone_request", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{VolumeContentSource: contentSource}
		datastores, err := ct.controller.getDatastoresForHostLocalLinkedClone(ctx, req, true, false)
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if datastores != nil {
			t.Fatalf("expected nil datastores, got: %+v", datastores)
		}
	})

	t.Run("resolves_source_volume_datastore_for_eligible_request", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{VolumeContentSource: contentSource}
		datastores, err := ct.controller.getDatastoresForHostLocalLinkedClone(ctx, req, true, true)
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if len(datastores) != 1 {
			t.Fatalf("expected exactly one resolved datastore, got: %d", len(datastores))
		}

		expectedDatastore, err := ct.controller.getDatastoreForLinkedCloneRequest(ctx, snapID)
		if err != nil {
			t.Fatal(err)
		}
		if datastores[0].Info.Url != expectedDatastore.Info.Url {
			t.Fatalf("expected resolved datastore URL %q to match source volume's datastore URL %q",
				datastores[0].Info.Url, expectedDatastore.Info.Url)
		}
	})

	t.Run("error_on_malformed_snapshot_id", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{
			VolumeContentSource: &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: "malformed-snapshot-id"},
				},
			},
		}
		_, err := ct.controller.getDatastoresForHostLocalLinkedClone(ctx, req, true, true)
		if err == nil {
			t.Fatal("expected an error for a malformed snapshot ID, got nil")
		}
	})
}

// fakeCOOverrides layers a fixed active-cluster list and per-feature FSS values on top of the
// standard fake orchestrator, whose GetActiveClustersForNamespaceInRequestedZones always returns an
// empty list and whose feature set does not include linked-clone support.
type fakeCOOverrides struct {
	commonco.COCommonInterface
	clusters           []string
	err                error
	fssOverrides       map[string]bool
	nodeNameToHostMoID map[string]string
}

func (f *fakeCOOverrides) GetNodeNameToHostMoIDMap(ctx context.Context) map[string]string {
	if f.nodeNameToHostMoID == nil {
		return f.COCommonInterface.GetNodeNameToHostMoIDMap(ctx)
	}
	return f.nodeNameToHostMoID
}

func (f *fakeCOOverrides) GetActiveClustersForNamespaceInRequestedZones(ctx context.Context,
	targetNS string, requestedZones []string) ([]string, error) {
	return f.clusters, f.err
}

func (f *fakeCOOverrides) IsFSSEnabled(ctx context.Context, featureName string) bool {
	if enabled, ok := f.fssOverrides[featureName]; ok {
		return enabled
	}
	return f.COCommonInterface.IsFSSEnabled(ctx, featureName)
}

// TestGetAccessibleClustersForSnapshot verifies which vSphere clusters are handed to CNS as
// activeClusters when restoring a volume from a snapshot.
//
// A host-local volume lives on a host-exclusive datastore, which is never part of the all-hosts
// datastore intersection that GetCandidateDatastoresInCluster reports. Narrowing on datastore
// accessibility therefore matches no cluster at all, and returning that empty list left CNS with a
// CnsVolumeCreateSpec carrying no datastores, no activeClusters and no hosts - rejected with
// "A specified parameter was not correct: createSpecs.datastores". Falling back to every active
// cluster of the namespace lets CNS place the restored volume on a policy-compatible datastore.
func TestGetAccessibleClustersForSnapshot(t *testing.T) {
	ct := getControllerTest(t)

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
		Parameters:         map[string]string{},
		VolumeCapabilities: capabilities,
	}
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	defer func() {
		if _, err := ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volID}); err != nil {
			t.Fatal(err)
		}
	}()

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
		if _, err := ct.controller.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapID}); err != nil {
			t.Fatal(err)
		}
	}()

	// The datastore the snapshot's source volume resides on.
	sourceDatastore, err := ct.controller.getDatastoreForLinkedCloneRequest(ctx, snapID)
	if err != nil {
		t.Fatal(err)
	}

	activeClusters := []string{"domain-c8", "domain-c9"}
	originalCO := commonco.ContainerOrchestratorUtility
	originalGetCandidateDatastores := getCandidateDatastores
	originalHostLocalSupport := isHostLocalStorageSupportEnabled
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		getCandidateDatastores = originalGetCandidateDatastores
		isHostLocalStorageSupportEnabled = originalHostLocalSupport
	})
	commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
		COCommonInterface: originalCO,
		clusters:          activeClusters,
	}

	t.Run("no_matching_cluster_returns_empty_without_widening", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		// No active cluster reports the snapshot's datastore among its shared datastores.
		getCandidateDatastores = func(_ context.Context, _ *cnsvsphere.VirtualCenter, _ string,
			_ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			return []*cnsvsphere.DatastoreInfo{
				{
					Datastore: &cnsvsphere.Datastore{},
					Info:      &vim25types.DatastoreInfo{Url: "ds:///vmfs/volumes/some-other-shared-ds/"},
				},
			}, nil, nil
		}

		// Widening to every active cluster here is what let CNS place a restored volume on a
		// datastore no host shares with the source, which vpxd rejects with "No common host found
		// between source and target datastores". Host-exclusive sources never reach this function.
		clusters, err := ct.controller.getAccessibleClustersForSnapshot(ctx, snapID, "default", []string{"zone-1"})
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if len(clusters) != 0 {
			t.Fatalf("expected no clusters when the snapshot datastore matches none, got %v", clusters)
		}
	})

	t.Run("shared_source_is_still_narrowed_to_matching_clusters", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		// Only the first active cluster can see the snapshot's datastore.
		getCandidateDatastores = func(_ context.Context, _ *cnsvsphere.VirtualCenter, clusterMoRef string,
			_ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			if clusterMoRef == activeClusters[0] {
				return []*cnsvsphere.DatastoreInfo{sourceDatastore}, nil, nil
			}
			return nil, nil, nil
		}

		clusters, err := ct.controller.getAccessibleClustersForSnapshot(ctx, snapID, "default", []string{"zone-1"})
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if !reflect.DeepEqual(clusters, []string{activeClusters[0]}) {
			t.Fatalf("expected narrowing to %v, got %v", []string{activeClusters[0]}, clusters)
		}
	})

	t.Run("candidate_datastore_error_is_propagated", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		getCandidateDatastores = func(_ context.Context, _ *cnsvsphere.VirtualCenter, _ string,
			_ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			return nil, nil, fmt.Errorf("cluster unreachable")
		}

		if _, err := ct.controller.getAccessibleClustersForSnapshot(ctx, snapID, "default",
			[]string{"zone-1"}); err == nil {
			t.Fatal("expected the candidate datastore error to propagate, got nil")
		}
	})
}

// TestLinkedCloneOnHostExclusiveDatastoreRequiresHostLocalPolicy verifies that a linked clone whose
// source volume sits on a host-exclusive datastore is rejected when the request is not host-local.
// A linked clone is always created on the source volume's datastore, so a shared storage policy can
// never be satisfied; rejecting in the controller reports that reason instead of the misleading
// "not accessible to all nodes" PBM failure raised later by isDataStoreCompatible.
func TestLinkedCloneOnHostExclusiveDatastoreRequiresHostLocalPolicy(t *testing.T) {
	ct := getControllerTest(t)

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
		Parameters:         map[string]string{},
		VolumeCapabilities: capabilities,
	}
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	defer func() {
		if _, err := ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volID}); err != nil {
			t.Fatal(err)
		}
	}()

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId
	defer func() {
		if _, err := ct.controller.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapID}); err != nil {
			t.Fatal(err)
		}
	}()

	originalCO := commonco.ContainerOrchestratorUtility
	originalMultiCluster := IsMultipleClustersPerVsphereZoneFSSEnabled
	originalHostLocalSupport := isHostLocalStorageSupportEnabled
	originalHostMounts := datastoreHostMounts
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		IsMultipleClustersPerVsphereZoneFSSEnabled = originalMultiCluster
		isHostLocalStorageSupportEnabled = originalHostLocalSupport
		datastoreHostMounts = originalHostMounts
	})

	IsMultipleClustersPerVsphereZoneFSSEnabled = true
	commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
		COCommonInterface: originalCO,
		clusters:          []string{"domain-c8"},
		fssOverrides:      map[string]bool{common.LinkedCloneSupport: true},
	}

	// A linked clone request restoring from the snapshot above, with a zone-only accessibility
	// requirement - i.e. a StorageClass that is not host-local.
	linkedCloneReq := func() *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name: testVolumeName + "-" + uuid.New().String(),
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1 * common.GbInBytes,
			},
			Parameters: map[string]string{
				common.AttributePvcNamespace:        "default",
				common.AttributeIsLinkedCloneKey:    "true",
				common.AttributeStoragePolicyID:     "shared-policy-id",
				common.AttributeStorageTopologyType: "zonal",
			},
			VolumeCapabilities: capabilities,
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}},
				},
				Requisite: []*csi.Topology{
					{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}},
				},
			},
			VolumeContentSource: &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapID},
				},
			},
		}
	}

	hostMount := func(id string) vim25types.DatastoreHostMount {
		return vim25types.DatastoreHostMount{
			Key: vim25types.ManagedObjectReference{Type: "HostSystem", Value: id},
		}
	}

	t.Run("host_exclusive_source_is_rejected", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
			return []vim25types.DatastoreHostMount{hostMount("host-1")}, nil
		}

		_, _, err := ct.controller.createBlockVolume(ctx, linkedCloneReq(), true, clusterComputeResourceMoIds)
		if err == nil {
			t.Fatal("expected a linked clone of a host-exclusive datastore under a shared policy to be rejected")
		}
		statusErr, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unable to convert the error: %+v into a grpc status error type", err)
		}
		if statusErr.Code() != codes.InvalidArgument {
			t.Fatalf("expected %s, got %s: %v", codes.InvalidArgument, statusErr.Code(), err)
		}
		if !strings.Contains(err.Error(), "host-exclusive datastore") {
			t.Fatalf("expected the error to name the host-exclusive datastore, got: %v", err)
		}
	})

	t.Run("shared_source_passes_the_guard", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
			return []vim25types.DatastoreHostMount{hostMount("host-1"), hostMount("host-2")}, nil
		}

		// Provisioning may still fail further down against the simulator; all that matters here is
		// that it is not rejected by the host-exclusive guard.
		_, _, err := ct.controller.createBlockVolume(ctx, linkedCloneReq(), true, clusterComputeResourceMoIds)
		if err != nil && strings.Contains(err.Error(), "host-exclusive datastore") {
			t.Fatalf("a datastore mounted by several hosts must not be rejected as host-exclusive: %v", err)
		}
	})

	t.Run("guard_is_skipped_when_capability_is_disabled", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = false
		called := false
		datastoreHostMounts = func(_ context.Context,
			_ *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
			called = true
			return []vim25types.DatastoreHostMount{hostMount("host-1")}, nil
		}

		_, _, err := ct.controller.createBlockVolume(ctx, linkedCloneReq(), true, clusterComputeResourceMoIds)
		if called {
			t.Fatal("host mounts must not be fetched when supports_host_local_storage is disabled")
		}
		if err != nil && strings.Contains(err.Error(), "host-exclusive datastore") {
			t.Fatalf("the guard must not fire when supports_host_local_storage is disabled: %v", err)
		}
	})
}

// TestRestoreFromHostLocalSnapshotPinsPlacementToSourceHost covers the placement constraints a
// non-linked-clone restore must obey when the snapshot's source volume lives on a host-exclusive
// datastore (the backing of a host-local storage policy).
//
// The restore is a disk copy and vCenter can only run it on a host that mounts both the source and
// the destination datastore. Leaving CNS free to choose - via activeClusters for a shared target
// StorageClass, or via the full candidate host set for a host-local one - lets it pick a
// destination no host shares with the source, which vpxd rejects with "No common host found
// between source and target datastores".
func TestRestoreFromHostLocalSnapshotPinsPlacementToSourceHost(t *testing.T) {
	ct := getControllerTest(t)

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	respCreate, err := ct.controller.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: testVolumeName + "-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters:         map[string]string{},
		VolumeCapabilities: capabilities,
	})
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	defer func() {
		if _, err := ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volID}); err != nil {
			t.Fatal(err)
		}
	}()

	respCreateSnapshot, err := ct.controller.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
		Parameters: map[string]string{
			common.VolumeSnapshotNamespaceKey: "default",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapID := respCreateSnapshot.Snapshot.SnapshotId
	defer func() {
		if _, err := ct.controller.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapID}); err != nil {
			t.Fatal(err)
		}
	}()

	// Real simulator hosts, so the datastore lookups below hit actual inventory.
	hosts, err := ct.vcenter.GetHostsByCluster(ctx, clusterComputeResourceMoIds[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) == 0 {
		t.Fatal("expected at least one host in the simulator cluster")
	}
	sourceHostRef := hosts[0].Reference()

	originalCO := commonco.ContainerOrchestratorUtility
	originalMultiCluster := IsMultipleClustersPerVsphereZoneFSSEnabled
	originalHostLocalSupport := isHostLocalStorageSupportEnabled
	originalHostMounts := datastoreHostMounts
	originalGetCandidateDatastores := getCandidateDatastores
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		IsMultipleClustersPerVsphereZoneFSSEnabled = originalMultiCluster
		isHostLocalStorageSupportEnabled = originalHostLocalSupport
		datastoreHostMounts = originalHostMounts
		getCandidateDatastores = originalGetCandidateDatastores
	})

	hostMount := func(ref vim25types.ManagedObjectReference) vim25types.DatastoreHostMount {
		return vim25types.DatastoreHostMount{Key: ref}
	}
	// hostExclusiveSource reports the snapshot's datastore as mounted by exactly one host, which is
	// what a host-local storage policy produces.
	hostExclusiveSource := func(_ context.Context,
		_ *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
		return []vim25types.DatastoreHostMount{hostMount(sourceHostRef)}, nil
	}
	snapshotContentSource := &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snapID},
		},
	}

	t.Run("getHostExclusiveSnapshotSourceHost", func(t *testing.T) {
		t.Run("single_mount_returns_that_host", func(t *testing.T) {
			datastoreHostMounts = hostExclusiveSource
			host, err := ct.controller.getHostExclusiveSnapshotSourceHost(ctx, snapID)
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if host == nil || host.Value != sourceHostRef.Value {
				t.Fatalf("expected source host %q, got %+v", sourceHostRef.Value, host)
			}
		})

		t.Run("multiple_mounts_need_no_pinning", func(t *testing.T) {
			datastoreHostMounts = func(_ context.Context,
				_ *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
				return []vim25types.DatastoreHostMount{
					hostMount(sourceHostRef),
					hostMount(vim25types.ManagedObjectReference{Type: "HostSystem", Value: "host-other"}),
				}, nil
			}
			host, err := ct.controller.getHostExclusiveSnapshotSourceHost(ctx, snapID)
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if host != nil {
				t.Fatalf("expected no pinning for a shared datastore, got %+v", host)
			}
		})

		t.Run("no_mounts_errors", func(t *testing.T) {
			datastoreHostMounts = func(_ context.Context,
				_ *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
				return nil, nil
			}
			if _, err := ct.controller.getHostExclusiveSnapshotSourceHost(ctx, snapID); err == nil {
				t.Fatal("expected an error for a datastore with no host mounts")
			}
		})

		t.Run("malformed_snapshot_id_errors", func(t *testing.T) {
			datastoreHostMounts = hostExclusiveSource
			if _, err := ct.controller.getHostExclusiveSnapshotSourceHost(ctx, "malformed"); err == nil {
				t.Fatal("expected an error for a malformed snapshot ID")
			}
		})
	})

	t.Run("getSnapshotSourceHostToPinPlacement", func(t *testing.T) {
		datastoreHostMounts = hostExclusiveSource

		t.Run("nil_without_content_source", func(t *testing.T) {
			isHostLocalStorageSupportEnabled = true
			host, err := ct.controller.getSnapshotSourceHostToPinPlacement(ctx, &csi.CreateVolumeRequest{})
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if host != nil {
				t.Fatalf("expected no pinning without a content source, got %+v", host)
			}
		})

		t.Run("nil_when_capability_disabled", func(t *testing.T) {
			isHostLocalStorageSupportEnabled = false
			called := false
			datastoreHostMounts = func(c context.Context,
				d *cnsvsphere.DatastoreInfo) ([]vim25types.DatastoreHostMount, error) {
				called = true
				return hostExclusiveSource(c, d)
			}
			defer func() { datastoreHostMounts = hostExclusiveSource }()

			host, err := ct.controller.getSnapshotSourceHostToPinPlacement(ctx,
				&csi.CreateVolumeRequest{VolumeContentSource: snapshotContentSource})
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if host != nil {
				t.Fatalf("expected no pinning when supports_host_local_storage is disabled, got %+v", host)
			}
			if called {
				t.Fatal("host mounts must not be fetched when supports_host_local_storage is disabled")
			}
		})

		t.Run("resolves_source_host_for_a_restore", func(t *testing.T) {
			isHostLocalStorageSupportEnabled = true
			host, err := ct.controller.getSnapshotSourceHostToPinPlacement(ctx,
				&csi.CreateVolumeRequest{VolumeContentSource: snapshotContentSource})
			if err != nil {
				t.Fatalf("unexpected error: %+v", err)
			}
			if host == nil || host.Value != sourceHostRef.Value {
				t.Fatalf("expected source host %q, got %+v", sourceHostRef.Value, host)
			}
		})

		t.Run("non_snapshot_content_source_is_invalid_argument", func(t *testing.T) {
			isHostLocalStorageSupportEnabled = true
			// A VolumeContentSource of type Volume (not Snapshot) reaching a host-local or
			// zone-topology restore branch is a caller error, not an internal failure.
			_, err := ct.controller.getSnapshotSourceHostToPinPlacement(ctx, &csi.CreateVolumeRequest{
				VolumeContentSource: &csi.VolumeContentSource{
					Type: &csi.VolumeContentSource_Volume{
						Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: volID},
					},
				},
			})
			if err == nil {
				t.Fatal("expected an error for a non-snapshot content source")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected %s, got %s: %v", codes.InvalidArgument, status.Code(err), err)
			}
		})
	})

	t.Run("getDatastoresAccessibleToHost_returns_the_hosts_datastores", func(t *testing.T) {
		datastores, err := getDatastoresAccessibleToHost(ctx, ct.vcenter, sourceHostRef)
		if err != nil {
			t.Fatalf("unexpected error: %+v", err)
		}
		if len(datastores) == 0 {
			t.Fatal("expected the source host to mount at least one datastore")
		}
	})

	t.Run("shared_target_supplies_source_host_datastores_instead_of_clusters", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		IsMultipleClustersPerVsphereZoneFSSEnabled = true
		datastoreHostMounts = hostExclusiveSource
		commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
			COCommonInterface: originalCO,
			clusters:          []string{"domain-c8"},
		}
		clusterPathTaken := false
		getCandidateDatastores = func(c context.Context, vc *cnsvsphere.VirtualCenter, id string,
			b bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			clusterPathTaken = true
			return originalGetCandidateDatastores(c, vc, id, b)
		}

		// Provisioning may still fail further down against the simulator; what matters is that the
		// host-scoped datastore path was taken rather than the activeClusters one.
		_, _, err := ct.controller.createBlockVolume(ctx, &csi.CreateVolumeRequest{
			Name: testVolumeName + "-" + uuid.New().String(),
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1 * common.GbInBytes,
			},
			Parameters: map[string]string{
				common.AttributePvcNamespace:        "default",
				common.AttributeStorageTopologyType: "zonal",
			},
			VolumeCapabilities: capabilities,
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}},
				},
				Requisite: []*csi.Topology{
					{Segments: map[string]string{v1.LabelTopologyZone: "zone-1"}},
				},
			},
			VolumeContentSource: snapshotContentSource,
		}, true, clusterComputeResourceMoIds)
		if clusterPathTaken {
			t.Fatalf("a host-exclusive source must not fall through to the activeClusters path (err: %v)", err)
		}
	})

	hostLocalRestoreReq := func() *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{
			Name: testVolumeName + "-" + uuid.New().String(),
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1 * common.GbInBytes,
			},
			Parameters: map[string]string{
				common.AttributePvcNamespace:    "default",
				common.AttributeHostLocalPolicy: "true",
			},
			VolumeCapabilities: capabilities,
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						v1.LabelTopologyZone: "zone-1",
						v1.LabelHostname:     "node-a",
					}},
					{Segments: map[string]string{
						v1.LabelTopologyZone: "zone-1",
						v1.LabelHostname:     "node-b",
					}},
				},
			},
			VolumeContentSource: snapshotContentSource,
		}
	}

	t.Run("host_local_target_reports_invalid_argument_for_a_non_snapshot_content_source", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		IsMultipleClustersPerVsphereZoneFSSEnabled = true
		commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
			COCommonInterface: originalCO,
			clusters:          []string{"domain-c8"},
		}

		req := hostLocalRestoreReq()
		req.VolumeContentSource = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Volume{
				Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: volID},
			},
		}
		_, faultType, err := ct.controller.createBlockVolume(ctx, req, true, clusterComputeResourceMoIds)
		if err == nil {
			t.Fatal("expected an error for a non-snapshot content source")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected %s, got %s: %v", codes.InvalidArgument, status.Code(err), err)
		}
		if faultType != csifault.CSIInvalidArgumentFault {
			t.Fatalf("expected fault %q, got %q (err: %v)", csifault.CSIInvalidArgumentFault, faultType, err)
		}
	})

	t.Run("host_local_target_narrows_to_the_source_host", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		IsMultipleClustersPerVsphereZoneFSSEnabled = true
		datastoreHostMounts = hostExclusiveSource
		// node-b maps to the host owning the snapshot's source volume, so the candidate set
		// [node-a, node-b] must be narrowed to it rather than left for CNS to choose from.
		commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
			COCommonInterface: originalCO,
			clusters:          []string{"domain-c8"},
			nodeNameToHostMoID: map[string]string{
				"node-a": "host-not-the-source",
				"node-b": sourceHostRef.Value,
			},
		}

		// Provisioning may still fail further down against the simulator; what matters is that the
		// candidate set was narrowed rather than rejected.
		_, _, err := ct.controller.createBlockVolume(ctx, hostLocalRestoreReq(), true,
			clusterComputeResourceMoIds)
		if err != nil && strings.Contains(err.Error(), "not among the candidate hosts") {
			t.Fatalf("the source host is in the requirement and must not be rejected: %v", err)
		}
	})

	t.Run("host_local_target_rejects_a_source_host_outside_the_requirement", func(t *testing.T) {
		isHostLocalStorageSupportEnabled = true
		IsMultipleClustersPerVsphereZoneFSSEnabled = true
		datastoreHostMounts = hostExclusiveSource
		// The accessibility requirement allows only node-a, which maps to a different host than
		// the one owning the snapshot's source volume.
		commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
			COCommonInterface:  originalCO,
			clusters:           []string{"domain-c8"},
			nodeNameToHostMoID: map[string]string{"node-a": "host-not-the-source"},
		}

		_, _, err := ct.controller.createBlockVolume(ctx, &csi.CreateVolumeRequest{
			Name: testVolumeName + "-" + uuid.New().String(),
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: 1 * common.GbInBytes,
			},
			Parameters: map[string]string{
				common.AttributePvcNamespace:    "default",
				common.AttributeHostLocalPolicy: "true",
			},
			VolumeCapabilities: capabilities,
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{Segments: map[string]string{
						v1.LabelTopologyZone: "zone-1",
						v1.LabelHostname:     "node-a",
					}},
				},
			},
			VolumeContentSource: snapshotContentSource,
		}, true, clusterComputeResourceMoIds)
		if err == nil {
			t.Fatal("expected the restore to be rejected when the source host is outside the requirement")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected %s, got %s: %v", codes.InvalidArgument, status.Code(err), err)
		}
		if !strings.Contains(err.Error(), "not among the candidate hosts") {
			t.Fatalf("expected the error to explain the host mismatch, got: %v", err)
		}
	})
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

// fakeVolumeInfoService is a minimal stand-in for cnsvolumeinfo.VolumeInfoService
// used by the WCP ControllerModifyVolume tests. It models only what the
// poll-only controller needs: a Get of the CNSVolumeInfo CR. Patches are
// counted (so tests can assert the controller stays read-only) but otherwise
// no-op since the CSI Syncer is the sole writer in production.
type fakeVolumeInfoService struct {
	cnsvolumeinfo.VolumeInfoService
	volumeInfo *cnsvolumeinfov1alpha1.CNSVolumeInfo
	mu         sync.Mutex
	patchCount int
}

func (f *fakeVolumeInfoService) GetVolumeInfoForVolumeID(
	ctx context.Context, volumeID string,
) (*cnsvolumeinfov1alpha1.CNSVolumeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.volumeInfo != nil && f.volumeInfo.Spec.VolumeID == volumeID {
		return f.volumeInfo.DeepCopy(), nil
	}
	return nil, fmt.Errorf("volume not found")
}

func (f *fakeVolumeInfoService) PatchVolumeInfo(
	ctx context.Context, volumeID string, patchBytes []byte, retries int,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patchCount++
	return nil
}

func (f *fakeVolumeInfoService) PatchVolumeInfoStatus(
	ctx context.Context, volumeID string, patchBytes []byte, retries int,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patchCount++
	return nil
}

// setMigrationConditions atomically replaces the MigrationConditions list on
// the underlying CNSVolumeInfo (simulating what the CSI Syncer does in
// production based on the Mobility Operator's migration CR status).
func (f *fakeVolumeInfoService) setMigrationConditions(conds []metav1.Condition) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volumeInfo.Status.MigrationConditions = conds
}

// TestControllerModifyVolume covers the poll-only WCP CSI ControllerModifyVolume.
// Under the new design the controller does NOT mutate CNSVolumeInfo; it only
// polls Status.MigrationConditions. The CSI Syncer (simulated here by the
// test setting MigrationConditions directly on the fake) writes those
// transitions in production.
func TestControllerModifyVolume(t *testing.T) {
	ct := getControllerTest(t)
	// Enable the supervisor capability that gates ControllerModifyVolume.
	if err := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator).
		EnableFSS(ctx, common.VMPVCStoragePolicyMutability); err != nil {
		t.Fatalf("failed to enable %s FSS: %v", common.VMPVCStoragePolicyMutability, err)
	}
	defer func() {
		_ = commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator).
			DisableFSS(ctx, common.VMPVCStoragePolicyMutability)
	}()

	// Speed up polling for tests to avoid long waits
	originalPollInterval := modifyVolumePollInterval
	modifyVolumePollInterval = 1 * time.Millisecond
	defer func() {
		modifyVolumePollInterval = originalPollInterval
	}()

	// Create a volume to obtain a real CSI volume ID for test inputs.
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
		Parameters:         map[string]string{},
		VolumeCapabilities: capabilities,
	}
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		})
	}()

	newFakeVIS := func() *fakeVolumeInfoService {
		return &fakeVolumeInfoService{
			volumeInfo: &cnsvolumeinfov1alpha1.CNSVolumeInfo{
				Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
					VolumeID: respCreate.Volume.VolumeId,
				},
			},
		}
	}
	makeReq := func() *csi.ControllerModifyVolumeRequest {
		return &csi.ControllerModifyVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
			MutableParameters: map[string]string{
				common.AttributeStoragePolicyName: "new-policy-name",
				common.AttributeStoragePolicyID:   "new-policy-id",
			},
		}
	}

	t.Run("ImmediateComplete", func(t *testing.T) {
		// Pre-populated Complete: poll observes it on the first iteration / fast path.
		fakeVIS := newFakeVIS()
		fakeVIS.setMigrationConditions([]metav1.Condition{{
			Type:   cnsvolumeinfov1alpha1.MigrationConditionComplete,
			Status: metav1.ConditionTrue,
		}})
		volumeInfoService = fakeVIS

		timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if _, err := ct.controller.ControllerModifyVolume(timeoutCtx, makeReq()); err != nil {
			t.Fatalf("ControllerModifyVolume returned error on Complete fast-path: %v", err)
		}
		if fakeVIS.patchCount != 0 {
			t.Fatalf("Expected zero patches in poll-only design; got %d", fakeVIS.patchCount)
		}
	})

	t.Run("AsyncCompleteViaSyncerSimulation", func(t *testing.T) {
		fakeVIS := newFakeVIS()
		volumeInfoService = fakeVIS
		// Simulate the CSI Syncer transitioning MigrationConditions to Complete
		// after a short delay (as it would in production after observing the
		// Mobility Operator's migration CR reaching its terminal state).
		go func() {
			time.Sleep(1 * time.Second)
			fakeVIS.setMigrationConditions([]metav1.Condition{{
				Type:   cnsvolumeinfov1alpha1.MigrationConditionComplete,
				Status: metav1.ConditionTrue,
			}})
		}()
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := ct.controller.ControllerModifyVolume(timeoutCtx, makeReq()); err != nil {
			t.Fatalf("ControllerModifyVolume returned error: %v", err)
		}
		if fakeVIS.patchCount != 0 {
			t.Fatalf("Expected zero patches in poll-only design; got %d", fakeVIS.patchCount)
		}
	})

	t.Run("ErrorReturnsInvalidArgument", func(t *testing.T) {
		fakeVIS := newFakeVIS()
		fakeVIS.setMigrationConditions([]metav1.Condition{{
			Type:   cnsvolumeinfov1alpha1.MigrationConditionError,
			Status: metav1.ConditionTrue,
		}})
		volumeInfoService = fakeVIS
		timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err := ct.controller.ControllerModifyVolume(timeoutCtx, makeReq())
		if err == nil {
			t.Fatalf("expected InvalidArgument when MigrationConditions=Error")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected codes.InvalidArgument, got %v", status.Code(err))
		}
	})

	t.Run("InfeasibleReturnsInvalidArgument", func(t *testing.T) {
		fakeVIS := newFakeVIS()
		fakeVIS.setMigrationConditions([]metav1.Condition{{
			Type:   cnsvolumeinfov1alpha1.MigrationConditionInfeasible,
			Status: metav1.ConditionTrue,
		}})
		volumeInfoService = fakeVIS
		timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err := ct.controller.ControllerModifyVolume(timeoutCtx, makeReq())
		if err == nil {
			t.Fatalf("expected InvalidArgument when MigrationConditions=Infeasible")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected codes.InvalidArgument, got %v", status.Code(err))
		}
	})

	t.Run("ContextDeadlineReturnsDeadlineExceeded", func(t *testing.T) {
		fakeVIS := newFakeVIS()
		// No condition transitions: the controller will poll until the gRPC
		// context deadline fires, which should yield codes.DeadlineExceeded.
		volumeInfoService = fakeVIS
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := ct.controller.ControllerModifyVolume(timeoutCtx, makeReq())
		if err == nil {
			t.Fatalf("expected DeadlineExceeded when poll loop times out")
		}
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("expected codes.DeadlineExceeded, got %v", status.Code(err))
		}
	})

	t.Run("InvalidVolumeId", func(t *testing.T) {
		volumeInfoService = newFakeVIS()
		_, err := ct.controller.ControllerModifyVolume(ctx, &csi.ControllerModifyVolumeRequest{
			VolumeId: "invalid-volume-id",
			MutableParameters: map[string]string{
				common.AttributeStoragePolicyName: "new-policy-name",
			},
		})
		if err == nil {
			t.Fatalf("expected error for invalid volume id (Get returns not-found, controller maps to Internal)")
		}
		if status.Code(err) != codes.Internal {
			t.Fatalf("expected codes.Internal for unknown volumeID, got %v", status.Code(err))
		}
	})

	t.Run("MissingMutableParameters", func(t *testing.T) {
		volumeInfoService = newFakeVIS()
		_, err := ct.controller.ControllerModifyVolume(ctx, &csi.ControllerModifyVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		})
		if err == nil {
			t.Fatalf("expected InvalidArgument when MutableParameters is nil")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected codes.InvalidArgument, got %v", status.Code(err))
		}
	})

	t.Run("EmptyVolumeId", func(t *testing.T) {
		volumeInfoService = newFakeVIS()
		_, err := ct.controller.ControllerModifyVolume(ctx, &csi.ControllerModifyVolumeRequest{
			VolumeId:          "",
			MutableParameters: map[string]string{"x": "y"},
		})
		if err == nil {
			t.Fatalf("expected InvalidArgument when VolumeId is empty")
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("expected codes.InvalidArgument, got %v", status.Code(err))
		}
	})
}

// TestControllerModifyVolumeFSSDisabled verifies that when the
// supports_VM_PVC_storage_policy_mutability capability is disabled, the
// controller returns codes.Unimplemented immediately without touching
// volumeInfoService or otherwise progressing the call.
func TestControllerModifyVolumeFSSDisabled(t *testing.T) {
	ct := getControllerTest(t)
	// Ensure the FSS is disabled (it may have been enabled by a prior test).
	_ = commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator).
		DisableFSS(ctx, common.VMPVCStoragePolicyMutability)

	_, err := ct.controller.ControllerModifyVolume(ctx, &csi.ControllerModifyVolumeRequest{
		VolumeId:          "any-volume-id",
		MutableParameters: map[string]string{"x": "y"},
	})
	if err == nil {
		t.Fatalf("expected codes.Unimplemented when FSS disabled")
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected codes.Unimplemented, got %v", status.Code(err))
	}
}

// TestControllerGetCapabilitiesModifyVolumeGated verifies that MODIFY_VOLUME is
// advertised in ControllerGetCapabilities only when the
// supports_VM_PVC_storage_policy_mutability capability is enabled.
func TestControllerGetCapabilitiesModifyVolumeGated(t *testing.T) {
	ct := getControllerTest(t)
	fakeOrch := commonco.ContainerOrchestratorUtility.(*unittestcommon.FakeK8SOrchestrator)
	hasModifyVolume := func() bool {
		resp, err := ct.controller.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("ControllerGetCapabilities failed: %v", err)
		}
		for _, c := range resp.Capabilities {
			if rpc := c.GetRpc(); rpc != nil && rpc.Type == csi.ControllerServiceCapability_RPC_MODIFY_VOLUME {
				return true
			}
		}
		return false
	}

	_ = fakeOrch.DisableFSS(ctx, common.VMPVCStoragePolicyMutability)
	if hasModifyVolume() {
		t.Fatalf("MODIFY_VOLUME should NOT be advertised when FSS disabled")
	}
	_ = fakeOrch.EnableFSS(ctx, common.VMPVCStoragePolicyMutability)
	defer func() { _ = fakeOrch.DisableFSS(ctx, common.VMPVCStoragePolicyMutability) }()
	if !hasModifyVolume() {
		t.Fatalf("MODIFY_VOLUME should be advertised when FSS enabled")
	}
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

// TestCreateFileVolumeVACMutableParams tests the early-return in createFileVolume
// that rejects requests carrying mutable_parameters when the
// isVMPVCStoragePolicyMutabilityEnabled flag is set.
func TestCreateFileVolumeVACMutableParams(t *testing.T) {
	tests := []struct {
		name              string
		fssEnabled        bool
		mutableParams     map[string]string
		wantUnimplemented bool
	}{
		{
			name:              "FSS enabled with mutable_parameters rejects file volume",
			fssEnabled:        true,
			mutableParams:     map[string]string{"x": "y"},
			wantUnimplemented: true,
		},
		{
			name:              "FSS enabled with empty mutable_parameters does not reject",
			fssEnabled:        true,
			mutableParams:     map[string]string{},
			wantUnimplemented: false,
		},
		{
			name:              "FSS disabled with mutable_parameters does not reject",
			fssEnabled:        false,
			mutableParams:     map[string]string{"x": "y"},
			wantUnimplemented: false,
		},
	}

	ct := getControllerTest(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := isVMPVCStoragePolicyMutabilityEnabled
			isVMPVCStoragePolicyMutabilityEnabled = tt.fssEnabled
			defer func() { isVMPVCStoragePolicyMutabilityEnabled = orig }()

			req := &csi.CreateVolumeRequest{
				Name:              "test-file-vac-" + uuid.New().String(),
				CapacityRange:     &csi.CapacityRange{RequiredBytes: 1 * common.GbInBytes},
				Parameters:        map[string]string{},
				MutableParameters: tt.mutableParams,
				VolumeCapabilities: []*csi.VolumeCapability{
					{AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
					}},
				},
			}

			// Pass isWorkloadDomainIsolationEnabled=true so that, for the
			// non-early-return cases, the function exits via the topology path
			// (codes.FailedPrecondition) rather than reaching the nil authMgr.
			_, _, err := ct.controller.createFileVolume(ctx, req, true)

			gotUnimplemented := err != nil && status.Code(err) == codes.Unimplemented
			if gotUnimplemented != tt.wantUnimplemented {
				t.Fatalf("wantUnimplemented=%v but gotUnimplemented=%v (code=%v, err=%v)",
					tt.wantUnimplemented, gotUnimplemented, status.Code(err), err)
			}
		})
	}
}

// TestCreateBlockVolumeVACPolicyOverride tests that when
// isVMPVCStoragePolicyMutabilityEnabled is true the storagePolicyID from
// mutable_parameters overrides the one from parameters, so the CNS volume is
// provisioned under the VAC's storage policy, not the StorageClass's.
func TestCreateBlockVolumeVACPolicyOverride(t *testing.T) {
	ct := getControllerTest(t)

	orig := isVMPVCStoragePolicyMutabilityEnabled
	isVMPVCStoragePolicyMutabilityEnabled = true
	defer func() { isVMPVCStoragePolicyMutabilityEnabled = orig }()

	pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
	if err != nil {
		t.Fatal(err)
	}
	vacPolicyID, err := pc.ProfileIDByName(ctx, "vSAN Default Storage Policy")
	if err != nil {
		t.Fatal(err)
	}

	// parameters carries a fake SC policy; mutable_parameters carries the real
	// VAC policy. With FSS enabled the mutable policy must win, so the volume
	// is created under vacPolicyID.
	reqCreate := &csi.CreateVolumeRequest{
		Name:          testVolumeName + "-block-vac-" + uuid.New().String(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 * common.GbInBytes},
		Parameters: map[string]string{
			common.AttributeStoragePolicyID: "sc-storage-policy-id",
		},
		MutableParameters: map[string]string{
			common.AttributeStoragePolicyID: vacPolicyID,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			}},
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: []*csi.Topology{},
			Preferred: []*csi.Topology{},
		},
	}

	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatalf("expected success with mutable policy override; got: %v", err)
	}
	defer func() {
		_, _ = ct.controller.DeleteVolume(ctx, &csi.DeleteVolumeRequest{
			VolumeId: respCreate.Volume.VolumeId,
		})
	}()

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: respCreate.Volume.VolumeId}},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, &queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 {
		t.Fatalf("expected 1 volume in query result, got %d", len(queryResult.Volumes))
	}
	if queryResult.Volumes[0].StoragePolicyId != vacPolicyID {
		t.Fatalf("expected storage policy %q (VAC policy from mutable_parameters), got %q",
			vacPolicyID, queryResult.Volumes[0].StoragePolicyId)
	}
}
