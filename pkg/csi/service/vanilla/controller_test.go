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

package vanilla

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/pbm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnssim "github.com/vmware/govmomi/cns/simulator"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	pbmsim "github.com/vmware/govmomi/pbm/simulator"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	clientset "k8s.io/client-go/kubernetes"
	testclient "k8s.io/client-go/kubernetes/fake"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

const (
	testVolumeName  = "test-pvc"
	testClusterName = "test-cluster"
)

var (
	ctx                    context.Context
	controllerTestInstance *controllerTest
	onceForControllerTest  sync.Once
)

type FakeNodeManager struct {
	client             *vim25.Client
	sharedDatastoreURL string
	k8sClient          clientset.Interface
}

type FakeAuthManager struct {
	vcenter *cnsvsphere.VirtualCenter
}

type controllerTest struct {
	controller *controller
	config     *config.Config
	vcenter    *cnsvsphere.VirtualCenter
}

// configFromSim starts a vcsim instance and returns config for use against the
// vcsim instance. The vcsim instance is configured with an empty tls.Config.
func configFromSim() (*config.Config, func()) {
	return configFromSimWithTLS(new(tls.Config), true)
}

// configFromSimWithTLS starts a vcsim instance and returns config for use
// against the vcsim instance. The vcsim instance is configured with a
// tls.Config. The returned client config can be configured to allow/decline
// insecure connections.
func configFromSimWithTLS(tlsConfig *tls.Config, insecureAllowed bool) (*config.Config, func()) {
	cfg := &config.Config{}
	model := simulator.VPX()
	defer model.Remove()

	err := model.Create()
	if err != nil {
		log.Fatal(err)
	}

	model.Service.TLS = tlsConfig
	s := model.Service.NewServer()

	// CNS Service simulator.
	model.Service.RegisterSDK(cnssim.New())

	// PBM Service simulator.
	model.Service.RegisterSDK(pbmsim.New())
	cfg.Global.InsecureFlag = insecureAllowed

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	cfg.Global.User = s.URL.User.Username()
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"

	// Write values to test_vsphere.conf.
	os.Setenv("VSPHERE_CSI_CONFIG", "test_vsphere.conf")
	conf := []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort))
	err = ioutil.WriteFile("test_vsphere.conf", conf, 0644)
	if err != nil {
		log.Fatal(err)
	}

	cfg.VirtualCenter = make(map[string]*config.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &config.VirtualCenterConfig{
		User:         cfg.Global.User,
		Password:     cfg.Global.Password,
		VCenterPort:  cfg.Global.VCenterPort,
		InsecureFlag: cfg.Global.InsecureFlag,
		Datacenters:  cfg.Global.Datacenters,
	}

	// set up the default global maximum of number of snapshots if unset
	if cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume == 0 {
		cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume = config.DefaultGlobalMaxSnapshotsPerBlockVolume
	}

	return cfg, func() {
		s.Close()
		model.Remove()
	}
}

func configFromEnvOrSim() (*config.Config, func()) {
	cfg := &config.Config{}
	if err := config.FromEnv(ctx, cfg); err != nil {
		return configFromSim()
	}
	return cfg, func() {}
}

func (f *FakeNodeManager) Initialize(ctx context.Context) error {
	return nil
}

func (f *FakeNodeManager) GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error) {
	finder := find.NewFinder(f.client, false)

	var datacenterName string
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		datacenterName = v
	} else {
		datacenterName = simulator.Map.Any("Datacenter").(*simulator.Datacenter).Name
	}

	dc, _ := finder.Datacenter(ctx, datacenterName)
	finder.SetDatacenter(dc)

	datastores, err := finder.DatastoreList(ctx, "*")
	if err != nil {
		return nil, err
	}
	var dsList []types.ManagedObjectReference
	for _, ds := range datastores {
		dsList = append(dsList, ds.Reference())
	}
	var dsMoList []mo.Datastore
	pc := property.DefaultCollector(dc.Client())
	properties := []string{"info"}
	err = pc.Retrieve(ctx, dsList, properties, &dsMoList)
	if err != nil {
		return nil, err
	}

	var sharedDatastoreManagedObject *mo.Datastore
	for _, dsMo := range dsMoList {
		if dsMo.Info.GetDatastoreInfo().Url == f.sharedDatastoreURL {
			sharedDatastoreManagedObject = &dsMo
			break
		}
	}
	if sharedDatastoreManagedObject == nil {
		return nil, fmt.Errorf("failed to get shared datastores")
	}
	return []*cnsvsphere.DatastoreInfo{
		{
			Datastore: &cnsvsphere.Datastore{
				Datastore:  object.NewDatastore(nil, sharedDatastoreManagedObject.Reference()),
				Datacenter: nil},
			Info: sharedDatastoreManagedObject.Info.GetDatastoreInfo(),
		},
	}, nil
}

func (f *FakeNodeManager) GetNodeByName(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error) {
	var vm *cnsvsphere.VirtualMachine
	var t *testing.T
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		nodeUUID, err := k8s.GetNodeVMUUID(ctx, f.k8sClient, nodeName)
		if err != nil {
			t.Errorf("failed to get providerId from node: %q. Err: %v", nodeName, err)
			return nil, err
		}
		vm, err = cnsvsphere.GetVirtualMachineByUUID(ctx, nodeUUID, false)
		if err != nil {
			t.Errorf("Couldn't find VM instance with nodeUUID %s, failed to discover with err: %v", nodeUUID, err)
			return nil, err
		}
	} else {
		obj := simulator.Map.Any("VirtualMachine").(*simulator.VirtualMachine)
		vm = &cnsvsphere.VirtualMachine{
			VirtualMachine: object.NewVirtualMachine(f.client, obj.Reference()),
		}
	}
	return vm, nil
}

func (f *FakeNodeManager) GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error) {
	return nil, nil
}

func (f *FakeNodeManager) GetSharedDatastoresInTopology(ctx context.Context,
	topologyRequirement *csi.TopologyRequirement, tagManager *tags.Manager,
	zoneKey string, regionKey string) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error) {
	return nil, nil, nil
}

func (f *FakeAuthManager) GetDatastoreMapForBlockVolumes(ctx context.Context) map[string]*cnsvsphere.DatastoreInfo {
	datastoreMapForBlockVolumes := make(map[string]*cnsvsphere.DatastoreInfo)
	fmt.Print("FakeAuthManager: GetDatastoreMapForBlockVolumes")
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		datastoreMapForBlockVolumes, _ := common.GenerateDatastoreMapForBlockVolumes(ctx, f.vcenter)
		return datastoreMapForBlockVolumes
	}
	return datastoreMapForBlockVolumes
}

func (f *FakeAuthManager) GetFsEnabledClusterToDsMap(ctx context.Context) map[string][]*cnsvsphere.DatastoreInfo {
	fsEnabledClusterToDsMap := make(map[string][]*cnsvsphere.DatastoreInfo)
	fmt.Print("FakeAuthManager: GetClusterToFsEnabledDsMap")
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		fsEnabledClusterToDsMap, _ := common.GenerateFSEnabledClustersToDsMap(ctx, f.vcenter)
		return fsEnabledClusterToDsMap
	}
	return fsEnabledClusterToDsMap
}

func (f *FakeAuthManager) ResetvCenterInstance(ctx context.Context, vCenter *cnsvsphere.VirtualCenter) {
	f.vcenter = vCenter
}

func getControllerTest(t *testing.T) *controllerTest {
	onceForControllerTest.Do(func() {
		// Create context.
		ctx = context.Background()
		config, _ := configFromEnvOrSim()

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
		fakeOpStore, err := unittestcommon.InitFakeVolumeOperationRequestInterface()
		if err != nil {
			t.Fatal(err)
		}

		manager := &common.Manager{
			VcenterConfig:  vcenterconfig,
			CnsConfig:      config,
			VolumeManager:  cnsvolume.GetManager(ctx, vcenter, fakeOpStore, true),
			VcenterManager: cnsvsphere.GetVirtualCenterManager(ctx),
		}

		var sharedDatastoreURL string
		if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
			sharedDatastoreURL = v
		} else {
			sharedDatastoreURL = simulator.Map.Any("Datastore").(*simulator.Datastore).Info.GetDatastoreInfo().Url
		}

		var k8sClient clientset.Interface
		if k8senv := os.Getenv("KUBECONFIG"); k8senv != "" {
			k8sClient, err = k8s.CreateKubernetesClientFromConfig(k8senv)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			k8sClient = testclient.NewSimpleClientset()
		}

		c := &controller{
			manager: manager,
			nodeMgr: &FakeNodeManager{
				client:             vcenter.Client.Client,
				sharedDatastoreURL: sharedDatastoreURL,
				k8sClient:          k8sClient,
			},
			authMgr: &FakeAuthManager{
				vcenter: vcenter,
			},
		}
		commonco.ContainerOrchestratorUtility, err =
			unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		if err != nil {
			t.Fatalf("Failed to create co agnostic interface. err=%v", err)
		}
		controllerTestInstance = &controllerTest{
			controller: c,
			config:     config,
			vcenter:    vcenter,
		}
	})
	return controllerTestInstance
}

func TestCreateVolumeWithStoragePolicy(t *testing.T) {
	// Create context.
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		params[common.AttributeDatastoreURL] = v
	}

	// PBM simulator defaults.
	params[common.AttributeStoragePolicyName] = "vSAN Default Storage Policy"
	if v := os.Getenv("VSPHERE_STORAGE_POLICY_NAME"); v != "" {
		params[common.AttributeStoragePolicyName] = v
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
	// Verify the volume has been created with corresponding storage policy ID.
	pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
	if err != nil {
		t.Fatal(err)
	}

	profileID, err := pc.ProfileIDByName(ctx, params[common.AttributeStoragePolicyName])
	if err != nil {
		t.Fatal(err)
	}

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

// When the testbed has multiple shared datastores, but VC user which is used
// to deploy CSI does not have Datastore.FileManagement privilege on all shared
// datastores, the create volume should succeed. This test is to simulate CSI
// on VMC.
func TestCreateVolumeWithMultipleDatastores(t *testing.T) {
	// Create context.
	ct := getControllerTest(t)

	// Create.
	params := make(map[string]string)

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

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

func TestExtendVolume(t *testing.T) {
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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

	// Extend Volume.
	newSize := 2 * common.GbInBytes
	reqExpand := &csi.ControllerExpandVolumeRequest{
		VolumeId: volID,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: newSize,
		},
		VolumeCapability: capabilities[0],
	}
	t.Log(fmt.Sprintf("ControllerExpandVolume will be called with req +%v", *reqExpand))
	respExpand, err := ct.controller.ControllerExpandVolume(ctx, reqExpand)
	if err != nil {
		t.Fatal(err)
	}
	if respExpand.CapacityBytes < newSize {
		t.Fatalf("newly expanded volume size %d is smaller than requested size %d for volume with ID: %s",
			respExpand.CapacityBytes, newSize, volID)
	}
	t.Log(fmt.Sprintf("ControllerExpandVolume succeeded: volume is expanded to requested size %d", newSize))

	// Query volume after expand volume.
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the expanded volume with ID: %s", volID)
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
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

// TestMigratedExtendVolume helps test ControllerExpandVolume with VolumeId
// having migrated volume.
func TestMigratedExtendVolume(t *testing.T) {
	ct := getControllerTest(t)
	reqExpand := &csi.ControllerExpandVolumeRequest{
		VolumeId: "[vsanDatastore] 08281a5f-a21d-1eff-62d6-02009d0f19a1/004dbb1694f14e3598abef852b113e3b.vmdk",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1024,
		},
	}
	t.Log(fmt.Sprintf("ControllerExpandVolume will be called with req +%v", *reqExpand))
	_, err := ct.controller.ControllerExpandVolume(ctx, reqExpand)
	if err != nil {
		t.Logf("Expected error received. migrated volume with VMDK path can not be expanded")
	} else {
		t.Fatal("Expected error not received when ControllerExpandVolume is called with volume having vmdk path")
	}
}

func TestCompleteControllerFlow(t *testing.T) {
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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

	var NodeID string
	if v := os.Getenv("VSPHERE_K8S_NODE"); v != "" {
		NodeID = v
	} else {
		NodeID = simulator.Map.Any("VirtualMachine").(*simulator.VirtualMachine).Name
	}

	// Attach.
	reqControllerPublishVolume := &csi.ControllerPublishVolumeRequest{
		VolumeId:         volID,
		NodeId:           NodeID,
		VolumeCapability: capabilities[0],
		Readonly:         false,
	}
	t.Log(fmt.Sprintf("ControllerPublishVolume will be called with req +%v", *reqControllerPublishVolume))
	respControllerPublishVolume, err := ct.controller.ControllerPublishVolume(ctx, reqControllerPublishVolume)
	if err != nil {
		t.Fatal(err)
	}
	diskUUID := respControllerPublishVolume.PublishContext[common.AttributeFirstClassDiskUUID]
	t.Log(fmt.Sprintf("ControllerPublishVolume succeed, diskUUID %s is returned", diskUUID))

	// Detach.
	reqControllerUnpublishVolume := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volID,
		NodeId:   NodeID,
	}
	t.Log(fmt.Sprintf("ControllerUnpublishVolume will be called with req +%v", *reqControllerUnpublishVolume))
	_, err = ct.controller.ControllerUnpublishVolume(ctx, reqControllerUnpublishVolume)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("ControllerUnpublishVolume succeed")

	// Delete.
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = ct.controller.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the volume has been deleted.
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

func TestDeleteVolumeWithSnapshots(t *testing.T) {
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

	// Create Snapshot
	snapSpec := cnstypes.CnsSnapshotCreateSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volID,
		},
		Description: "Snap-1",
	}
	snapshotTask, err := ct.vcenter.CnsClient.CreateSnapshots(ctx, []cnstypes.CnsSnapshotCreateSpec{snapSpec})
	if err != nil {
		t.Fatalf("failed to create snapshot for volume: %s", volID)
	}
	snapTaskInfo, err := cns.GetTaskInfo(ctx, snapshotTask)
	if err != nil {
		t.Fatalf("failed to get snapshot taskinfo for volume: %s", volID)
	}
	taskResult, err := cns.GetTaskResult(ctx, snapTaskInfo)
	if err != nil {
		t.Fatalf("failed to get task result for snapshot operation on volume: %s", volID)
	}
	cnsSnapshot := taskResult.(*cnstypes.CnsSnapshotCreateResult)
	snapshotID := cnsSnapshot.Snapshot.SnapshotId.Id
	t.Logf("created cns snapshot: %s for volume: %s", snapshotID, volID)
	defer func() {
		deleteSnapSepc := cnstypes.CnsSnapshotDeleteSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volID,
			},
			SnapshotId: cnstypes.CnsSnapshotId{
				Id: snapshotID,
			},
		}
		_, err = ct.vcenter.CnsClient.DeleteSnapshots(ctx, []cnstypes.CnsSnapshotDeleteSpec{deleteSnapSepc})
		if err != nil {
			t.Errorf("failed to delete snapshot: %s for volume: %s", snapshotID, volID)
		}
	}()

	// Attempt to delete the volume
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
		t.Fatal("expected error was not received when deleting volume with snapshot")
	}
}

func TestCreateBlockVolumeSnapshot(t *testing.T) {
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
		// Delete volume.
		reqDelete := &csi.DeleteVolumeRequest{
			VolumeId: volID,
		}
		_, err = ct.controller.DeleteVolume(ctx, reqDelete)
		if err != nil {
			t.Fatal(err)
		}

		// Verify the volume has been deleted.
		queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
		if err != nil {
			t.Fatal(err)
		}

		if len(queryResult.Volumes) != 0 {
			t.Fatalf("volume should not exist after deletion with ID: %s", volID)
		}
	}()

	// Create the configured max number of snapshots on the source volume.
	configured_max_snapshot_num := ct.controller.manager.CnsConfig.Snapshot.GlobalMaxSnapshotsPerBlockVolume
	for i := 0; i < configured_max_snapshot_num; i++ {
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
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

	// error expected when create the fourth snapshot
	reqCreateSnapshot := &csi.CreateSnapshotRequest{
		SourceVolumeId: volID,
		Name:           "snapshot-" + uuid.New().String(),
	}
	expectedErr := fmt.Errorf("the number of snapshots on the source volume %s reaches "+
		"the configured maximum (%v)", volID, configured_max_snapshot_num)

	_, err = ct.controller.CreateSnapshot(ctx, reqCreateSnapshot)
	if err != nil {
		delErr, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unable to convert the error: %+v into a grpc status error type.", err)
		}
		if delErr.Code() == codes.FailedPrecondition && delErr.Message() == expectedErr.Error() {
			t.Logf("received error as expected when attempting to create snapshot on volume "+
				"when existing number of snapshots reaches the configured maximum, error: %+v.", err)
		} else {
			t.Fatalf("unexpected error received, expected: %s received: %s.",
				expectedErr.Error(), delErr.Message())
		}
	} else {
		t.Fatal("expected error was not received create snapshot on volume " +
			"with configured maximum number of snapshots.")
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
		queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
		queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}

func TestListSnapshotsOnSpecificVolume(t *testing.T) {
	ct := getControllerTest(t)
	numOfSnapshots := ct.config.Snapshot.GlobalMaxSnapshotsPerBlockVolume
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Map to track all the snapshots created.
	snapshots := make(map[string]string)
	var deleteSnapshotList []string

	for i := 0; i < numOfSnapshots; i++ {
		// Snapshot a volume
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
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

func TestListSnapshots(t *testing.T) {
	ct := getControllerTest(t)
	numOfSnapshots := ct.config.Snapshot.GlobalMaxSnapshotsPerBlockVolume
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Map to track all the snapshots created.
	snapshots := make(map[string]string)
	var deleteSnapshotList []string

	for i := 0; i < numOfSnapshots; i++ {
		// Snapshot a volume
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
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

func TestListSnapshotsWithToken(t *testing.T) {
	ct := getControllerTest(t)
	numOfSnapshots := ct.config.Snapshot.GlobalMaxSnapshotsPerBlockVolume
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("failed to find the newly created volume with ID: %s", volID)
	}

	// Map to track all the snapshots created.
	snapshots := make(map[string]string)
	var deleteSnapshotList []string

	for i := 0; i < numOfSnapshots; i++ {
		// Snapshot a volume
		reqCreateSnapshot := &csi.CreateSnapshotRequest{
			SourceVolumeId: volID,
			Name:           "snapshot-" + uuid.New().String(),
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

func TestExpandVolumeWithSnapshots(t *testing.T) {
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
	queryResult, err := ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
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
		expandErr, ok := status.FromError(err)
		if !ok {
			t.Fatalf("unable to convert the error: %+v into a grpc status error type", err)
		}
		if expandErr.Code() == codes.FailedPrecondition {
			t.Logf("received error as expected when attempting to expand volume with snapshot, error: %+v", err)
		} else {
			t.Fatalf("unexpected error code received, expected: %s received: %s",
				codes.FailedPrecondition.String(), expandErr.Code().String())
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
	queryResult, err = ct.vcenter.CnsClient.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 0 {
		t.Fatalf("Volume should not exist after deletion with ID: %s", volID)
	}
}
