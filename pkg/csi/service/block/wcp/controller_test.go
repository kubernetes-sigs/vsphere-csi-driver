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
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/pbm"
	pbmsim "github.com/vmware/govmomi/pbm/simulator"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	cnssim "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vmomi/simulator"
	cnstypes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vmomi/types"
	cspvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/block"
)

const (
	testVolumeName  = "test-volume"
	testClusterName = "test-cluster"
)

// configFromSim starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with an empty tls.Config.
func configFromSim() (*config.Config, func()) {
	return configFromSimWithTLS(new(tls.Config), true)
}

// configFromSimWithTLS starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with a tls.Config. The returned client
// config can be configured to allow/decline insecure connections.
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

	// CNS Service simulator
	model.Service.RegisterSDK(cnssim.New())

	// PBM Service simulator
	model.Service.RegisterSDK(pbmsim.New())
	cfg.Global.InsecureFlag = insecureAllowed

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	cfg.Global.User = s.URL.User.Username()
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"
	cfg.VirtualCenter = make(map[string]*config.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &config.VirtualCenterConfig{
		User:         cfg.Global.User,
		Password:     cfg.Global.Password,
		VCenterPort:  cfg.Global.VCenterPort,
		InsecureFlag: cfg.Global.InsecureFlag,
		Datacenters:  cfg.Global.Datacenters,
	}
	return cfg, func() {
		s.Close()
		model.Remove()
	}
}

func configFromEnvOrSim() (*config.Config, func()) {
	cfg := &config.Config{}
	if err := config.FromEnv(cfg); err != nil {
		return configFromSim()
	}
	return cfg, func() {}
}

type controllerTest struct {
	controller *controller
	config     *config.Config
	vcenter    *cnsvsphere.VirtualCenter
}

var (
	controllerTestInstance *controllerTest
	onceForControllerTest  sync.Once
)

func getControllerTest(t *testing.T) *controllerTest {
	onceForControllerTest.Do(func() {
		config, _ := configFromEnvOrSim()

		// CNS based CSI requires a valid cluster name
		config.Global.ClusterID = testClusterName

		ctx := context.Background()

		vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(config)
		if err != nil {
			t.Fatal(err)
		}
		vcManager := cnsvsphere.GetVirtualCenterManager()
		vcenter, err := vcManager.RegisterVirtualCenter(vcenterconfig)
		if err != nil {
			t.Fatal(err)
		}

		err = vcenter.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}

		manager := &block.Manager{
			VcenterConfig:  vcenterconfig,
			CnsConfig:      config,
			VolumeManager:  cspvolume.GetManager(vcenter),
			VcenterManager: cnsvsphere.GetVirtualCenterManager(),
		}

		c := &controller{
			manager: manager,
		}

		controllerTestInstance = &controllerTest{
			controller: c,
			config:     config,
			vcenter:    vcenter,
		}
	})
	return controllerTestInstance
}

func getFakeDatastores(ctx context.Context, controller *controller) ([]*cnsvsphere.DatastoreInfo, error) {
	var sharedDatastoreURL string
	if v := os.Getenv("VSPHERE_DATASTORE_URL"); v != "" {
		sharedDatastoreURL = v
	} else {
		sharedDatastoreURL = simulator.Map.Any("Datastore").(*simulator.Datastore).Info.GetDatastoreInfo().Url
	}

	var datacenterName string
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		datacenterName = v
	} else {
		datacenterName = simulator.Map.Any("Datacenter").(*simulator.Datacenter).Name
	}

	vc, _ := block.GetVCenter(ctx, controller.manager)
	finder := find.NewFinder(vc.Client.Client, false)
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
		if dsMo.Info.GetDatastoreInfo().Url == sharedDatastoreURL {
			sharedDatastoreManagedObject = &dsMo
			break
		}
	}
	if sharedDatastoreManagedObject == nil {
		return nil, fmt.Errorf("Failed to get shared datastores")
	}
	return []*cnsvsphere.DatastoreInfo{
		{
			&cnsvsphere.Datastore{
				object.NewDatastore(nil, sharedDatastoreManagedObject.Reference()),
				nil},
			sharedDatastoreManagedObject.Info.GetDatastoreInfo(),
		},
	}, nil
}

/*
 * TestCreateVolumeWithoutStoragePolicyWcp creates volume
 * with storage policy
 */
func TestWCPCreateVolumeWithStoragePolicy(t *testing.T) {
	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ct := getControllerTest(t)

	// Create
	params := make(map[string]string, 0)

	profileID := os.Getenv("VSPHERE_STORAGE_POLICY_ID")
	if profileID == "" {
		storagePolicyName := os.Getenv("VSPHERE_STORAGE_POLICY_NAME")
		if storagePolicyName == "" {
			// PBM simulator defaults
			storagePolicyName = "vSAN Default Storage Policy"
		}

		// Verify the volume has been create with corresponding storage policy ID
		pc, err := pbm.NewClient(ctx, ct.vcenter.Client.Client)
		if err != nil {
			t.Fatal(err)
		}

		profileID, err = pc.ProfileIDByName(ctx, storagePolicyName)
		if err != nil {
			t.Fatal(err)
		}
	}
	params[block.AttributeStoragePolicyID] = profileID

	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * block.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	getSharedDatastores = getFakeDatastores
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
	queryResult, err := ct.vcenter.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}

	if queryResult.Volumes[0].StoragePolicyId != profileID {
		t.Fatalf("Failed to match volume policy ID: %s", profileID)
	}

	// QueryAll
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}
}

/*
 * TestCreateVolumeWithoutStoragePolicyWcp creates volume
 * without storage policy
 */
func TestWCPCreateVolumeWithoutStoragePolicy(t *testing.T) {
	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ct := getControllerTest(t)

	// Create
	params := make(map[string]string, 0)
	capabilities := []*csi.VolumeCapability{
		{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}

	reqCreate := &csi.CreateVolumeRequest{
		Name: testVolumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * block.GbInBytes,
		},
		Parameters:         params,
		VolumeCapabilities: capabilities,
	}

	getSharedDatastores = getFakeDatastores
	respCreate, err := ct.controller.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatal(err)
	}
	volID := respCreate.Volume.VolumeId
	// Verify the volume has been created
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	queryResult, err := ct.vcenter.QueryVolume(ctx, queryFilter)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}

	// QueryAll
	queryFilter = cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{
			{
				Id: volID,
			},
		},
	}
	querySelection := cnstypes.CnsQuerySelection{}
	queryResult, err = ct.vcenter.QueryAllVolume(ctx, queryFilter, querySelection)
	if err != nil {
		t.Fatal(err)
	}

	if len(queryResult.Volumes) != 1 && queryResult.Volumes[0].VolumeId.Id != volID {
		t.Fatalf("Failed to find the newly created volume with ID: %s", volID)
	}
}
