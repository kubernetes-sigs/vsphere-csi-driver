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

package fcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/url"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	lookup "github.com/vmware/govmomi/lookup/simulator"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/simulator/vpx"
	sts "github.com/vmware/govmomi/sts/simulator"
	"github.com/vmware/govmomi/vapi/rest"
	vapi "github.com/vmware/govmomi/vapi/simulator"
	"github.com/vmware/govmomi/vapi/tags"

	vcfg "k8s.io/cloud-provider-vsphere/pkg/common/config"
	cm "k8s.io/cloud-provider-vsphere/pkg/common/connectionmanager"
	"k8s.io/cloud-provider-vsphere/pkg/common/vclib"
)

// configFromSim starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with an empty tls.Config.
func configFromSim(multiDc bool) (*vcfg.Config, func()) {
	return configFromSimWithTLS(new(tls.Config), true, multiDc)
}

// configFromSimWithTLS starts a vcsim instance and returns config for use against the vcsim instance.
// The vcsim instance is configured with a tls.Config. The returned client
// config can be configured to allow/decline insecure connections.
func configFromSimWithTLS(tlsConfig *tls.Config, insecureAllowed bool, multiDc bool) (*vcfg.Config, func()) {
	cfg := &vcfg.Config{}
	model := simulator.VPX()

	if multiDc {
		model.Datacenter = 2
		model.Datastore = 1
		model.Cluster = 1
		model.Host = 0
	}

	err := model.Create()
	if err != nil {
		log.Fatal(err)
	}

	model.Service.TLS = tlsConfig
	s := model.Service.NewServer()

	// STS simulator
	path, handler := sts.New(s.URL, vpx.Setting)
	model.Service.ServeMux.Handle(path, handler)

	// vAPI simulator
	path, handler = vapi.New(s.URL, nil)
	model.Service.ServeMux.Handle(path, handler)

	// Lookup Service simulator
	model.Service.RegisterSDK(lookup.New())

	cfg.Global.InsecureFlag = insecureAllowed

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	cfg.Global.User = s.URL.User.Username()
	cfg.Global.Password, _ = s.URL.User.Password()
	// Configure region and zone categories
	if multiDc {
		cfg.Global.Datacenters = "DC0,DC1"
	} else {
		cfg.Global.Datacenters = vclib.TestDefaultDatacenter
	}
	cfg.VirtualCenter = make(map[string]*vcfg.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &vcfg.VirtualCenterConfig{
		User:         cfg.Global.User,
		Password:     cfg.Global.Password,
		VCenterPort:  cfg.Global.VCenterPort,
		InsecureFlag: cfg.Global.InsecureFlag,
		Datacenters:  cfg.Global.Datacenters,
	}

	// Configure region and zone categories
	if multiDc {
		cfg.Labels.Region = "k8s-region"
		cfg.Labels.Zone = "k8s-zone"
	}

	return cfg, func() {
		s.Close()
		model.Remove()
	}
}

func configFromEnvOrSim(multiDc bool) (*vcfg.Config, func()) {
	cfg := &vcfg.Config{}
	if err := vcfg.ConfigFromEnv(cfg); err != nil {
		return configFromSim(multiDc)
	}
	return cfg, func() {}
}

func TestCompleteControllerFlow(t *testing.T) {
	config, cleanup := configFromEnvOrSim(false)
	defer cleanup()

	connMgr := cm.NewConnectionManager(config, nil)
	defer connMgr.Logout()

	c := &controller{
		cfg:     config,
		connMgr: connMgr,
	}

	//context
	ctx := context.Background()

	// Get a simulator VM
	myVM := simulator.Map.Any("VirtualMachine").(*simulator.VirtualMachine)
	vmName := myVM.Name
	myVM.Guest.HostName = strings.ToLower(vmName)

	// Get a simulator DS
	myds := simulator.Map.Any("Datastore").(*simulator.Datastore)

	err := connMgr.Connect(ctx, config.Global.VCenterIP)
	if err != nil {
		t.Errorf("Failed to Connect to vSphere: %s", err)
	}

	//create
	params := make(map[string]string, 0)
	params[AttributeFirstClassDiskParentType] = string(vclib.TypeDatastore)
	params[AttributeFirstClassDiskParentName] = myds.Name

	reqCreate := &csi.CreateVolumeRequest{
		Name: "test",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * GbInBytes,
		},
		Parameters: params,
	}

	respCreate, err := c.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volID := respCreate.Volume.VolumeId
	volName := respCreate.Volume.VolumeContext[AttributeFirstClassDiskName]
	t.Logf("ID: %s", volID)
	t.Logf("%s: %s", AttributeFirstClassDiskName, volName)
	t.Logf("%s: %s", AttributeFirstClassDiskType, respCreate.Volume.VolumeContext[AttributeFirstClassDiskType])
	t.Logf("%s: %s", AttributeFirstClassDiskParentType, respCreate.Volume.VolumeContext[AttributeFirstClassDiskParentType])
	t.Logf("%s: %s", AttributeFirstClassDiskParentName, respCreate.Volume.VolumeContext[AttributeFirstClassDiskParentName])

	if !strings.EqualFold("test", volName) {
		t.Errorf("[CREATE] Name of FCD does not match test != %s", volName)
	}

	//list
	respList, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Errorf("ListVolumes failed: %v", err)
	} else {
		if len(respList.Entries) != 1 {
			t.Error("There should only be a single FCD present")
		} else {
			name := respList.Entries[0].Volume.VolumeContext[AttributeFirstClassDiskName]
			if !strings.EqualFold("test", name) {
				t.Errorf("[LIST] Name of FCD does not match test != %s", name)
			}
		}
	}

	//publish
	reqPub := &csi.ControllerPublishVolumeRequest{
		VolumeId: volID,
		NodeId:   vmName,
	}
	respPub, err := c.ControllerPublishVolume(ctx, reqPub)
	if err != nil {
		t.Errorf("ControllerPublishVolume failed: %v", err)
	} else {
		pubCon := respPub.GetPublishContext()

		name := pubCon[AttributeFirstClassDiskName]
		if !strings.EqualFold("test", name) {
			t.Errorf("[PUB] Name of FCD does not match test != %s", name)
		}
	}

	//unpublish
	reqUnpub := &csi.ControllerUnpublishVolumeRequest{
		VolumeId: volID,
		NodeId:   vmName,
	}
	_, err = c.ControllerUnpublishVolume(ctx, reqUnpub)
	if err != nil {
		t.Errorf("ControllerUnpublishVolume failed: %v", err)
	}

	//delete
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = c.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Errorf("DeleteVolume failed: %v", err)
	}
}

func TestListBoundaries(t *testing.T) {
	config, cleanup := configFromEnvOrSim(false)
	defer cleanup()

	connMgr := cm.NewConnectionManager(config, nil)
	defer connMgr.Logout()

	c := &controller{
		cfg:     config,
		connMgr: connMgr,
	}

	//context
	ctx := context.Background()

	// Get a simulator DS
	myds := simulator.Map.Any("Datastore").(*simulator.Datastore)

	err := connMgr.Connect(ctx, config.Global.VCenterIP)
	if err != nil {
		t.Errorf("Failed to Connect to vSphere: %s", err)
	}

	//create
	params := make(map[string]string, 0)
	params[AttributeFirstClassDiskParentType] = string(vclib.TypeDatastore)
	params[AttributeFirstClassDiskParentName] = myds.Name

	for i := 1; i <= 11; i++ {
		volName := fmt.Sprintf("test%d", i)
		reqCreate := &csi.CreateVolumeRequest{
			Name: volName,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: int64(i) * GbInBytes,
			},
			Parameters: params,
		}

		respCreate, err := c.CreateVolume(ctx, reqCreate)
		if err != nil {
			t.Fatalf("CreateVolume failed: %v", err)
		}

		volNameRet := respCreate.Volume.VolumeContext[AttributeFirstClassDiskName]
		if !strings.EqualFold(volName, volNameRet) {
			t.Errorf("[CREATE] Name of FCD does not match %s != %s", volName, volNameRet)
		}
	}

	// get first 10
	resp, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 10})
	if err != nil {
		t.Errorf("ListVolumes [0, 9] failed: %v", err)
	} else {
		count := len(resp.Entries)
		if count != 10 {
			t.Errorf("Invalid number of volumes listed. Excepting 10 got %d", count)
		}
		next := resp.NextToken
		if !strings.EqualFold("10", next) {
			t.Errorf("Incorrect next token. Excepting 10 got next=%s", next)
		}
	}

	// get the next set which just happens to be the last one
	resp, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{StartingToken: resp.NextToken})
	if err != nil {
		t.Errorf("ListVolumes [10] failed: %v", err)
	} else {
		count := len(resp.Entries)
		if count != 1 {
			t.Errorf("Invalid number of volumes listed. Excepting 1 got %d", count)
		}
		next := resp.NextToken
		if len(next) != 0 {
			t.Errorf("Incorrect next token. Excepting empty string got next=%s", next)
		}
	}

	// get just the first (index 0)
	resp, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{StartingToken: "0", MaxEntries: 1})
	if err != nil {
		t.Errorf("ListVolumes [0] failed: %v", err)
	} else {
		count := len(resp.Entries)
		if count != 1 {
			t.Errorf("Invalid number of volumes listed. Excepting 1 got %d", count)
		}
		next := resp.NextToken
		if !strings.EqualFold("1", next) {
			t.Errorf("Incorrect next token. Excepting 1 got next=%s", next)
		}
	}

	// get just the fifth (index 4)
	resp, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{StartingToken: "4", MaxEntries: 1})
	if err != nil {
		t.Errorf("ListVolumes [4] failed: %v", err)
	} else {
		count := len(resp.Entries)
		if count != 1 {
			t.Errorf("Invalid number of volumes listed. Excepting 1 got %d", count)
		}
		next := resp.NextToken
		if !strings.EqualFold("5", next) {
			t.Errorf("Incorrect next token. Excepting 5 got next=%s", next)
		}
	}

	// break into two even-ish sets... this one starting from 0 with a max of 6 (total 6)
	resp, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 6})
	if err != nil {
		t.Errorf("ListVolumes [0-5] failed: %v", err)
	} else {
		count := len(resp.Entries)
		if count != 6 {
			t.Errorf("Invalid number of volumes listed. Excepting 6 got %d", count)
		}
		next := resp.NextToken
		if !strings.EqualFold("6", next) {
			t.Errorf("Incorrect next token. Excepting 6 got next=%s", next)
		}
	}

	// break into two even-ish sets... this one starting from 6 with a max of 10 (total 5)
	resp, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{StartingToken: resp.NextToken, MaxEntries: 6})
	if err != nil {
		t.Errorf("ListVolumes [6-10] failed: %v", err)
	} else {
		count := len(resp.Entries)
		if count != 5 {
			t.Errorf("Invalid number of volumes listed. Excepting 5 got %d", count)
		}
		next := resp.NextToken
		if len(next) != 0 {
			t.Errorf("Incorrect next token. Excepting emptyy string got next=%s", next)
		}
	}
}

func TestListOrder(t *testing.T) {
	config, cleanup := configFromEnvOrSim(false)
	defer cleanup()

	connMgr := cm.NewConnectionManager(config, nil)
	defer connMgr.Logout()

	c := &controller{
		cfg:     config,
		connMgr: connMgr,
	}

	//context
	ctx := context.Background()

	// Get a simulator DS
	myds := simulator.Map.Any("Datastore").(*simulator.Datastore)

	err := connMgr.Connect(ctx, config.Global.VCenterIP)
	if err != nil {
		t.Errorf("Failed to Connect to vSphere: %s", err)
	}

	//create
	params := make(map[string]string, 0)
	params[AttributeFirstClassDiskParentType] = string(vclib.TypeDatastore)
	params[AttributeFirstClassDiskParentName] = myds.Name

	for i := 1; i <= 11; i++ {
		volName := fmt.Sprintf("test%d", i)
		reqCreate := &csi.CreateVolumeRequest{
			Name: volName,
			CapacityRange: &csi.CapacityRange{
				RequiredBytes: int64(i) * GbInBytes,
			},
			Parameters: params,
		}

		respCreate, err := c.CreateVolume(ctx, reqCreate)
		if err != nil {
			t.Fatalf("CreateVolume failed: %v", err)
		}

		volNameRet := respCreate.Volume.VolumeContext[AttributeFirstClassDiskName]
		if !strings.EqualFold(volName, volNameRet) {
			t.Errorf("[CREATE] Name of FCD does not match %s != %s", volName, volNameRet)
		}
	}

	// order should be repeatable
	// first call
	respFirst, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Errorf("[FIRST] ListVolumes failed: %v", err)
	}
	countFirst := len(respFirst.Entries)
	if countFirst != 11 {
		t.Errorf("There should 11 FCD present count=%d", countFirst)
	}

	// second call
	respSecond, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Errorf("[SECOND] ListVolumes failed: %v", err)
	}
	countSecond := len(respSecond.Entries)
	if countFirst != 11 {
		t.Errorf("There should 11 FCD present count=%d", countSecond)
	}

	for i := 0; i < 11; i++ {
		firstName := respFirst.Entries[i].Volume.VolumeContext[AttributeFirstClassDiskName]
		secondName := respSecond.Entries[i].Volume.VolumeContext[AttributeFirstClassDiskName]

		if !strings.EqualFold(firstName, secondName) {
			t.Errorf("FirstName(%s) != SecondName(%s)", firstName, secondName)
		}
	}

	// Add one more... thus changing the order
	reqCreate := &csi.CreateVolumeRequest{
		Name: "test12",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: int64(12) * GbInBytes,
		},
		Parameters: params,
	}

	respCreate, err := c.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	volNameRet := respCreate.Volume.VolumeContext[AttributeFirstClassDiskName]
	if !strings.EqualFold("test12", volNameRet) {
		t.Errorf("[CREATE] Name of FCD does not match test12 != %s", volNameRet)
	}

	// Now retest the list order so that it's repeatable
	// first call
	respFirst, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Errorf("[FIRST] ListVolumes failed: %v", err)
	}
	countFirst = len(respFirst.Entries)
	if countFirst != 12 {
		t.Errorf("There should 12 FCD present count=%d", countFirst)
	}

	// second call
	respSecond, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Errorf("[SECOND] ListVolumes failed: %v", err)
	}
	countSecond = len(respSecond.Entries)
	if countFirst != 12 {
		t.Errorf("There should 12 FCD present count=%d", countSecond)
	}

	for i := 0; i < 12; i++ {
		firstName := respFirst.Entries[i].Volume.VolumeContext[AttributeFirstClassDiskName]
		secondName := respSecond.Entries[i].Volume.VolumeContext[AttributeFirstClassDiskName]

		if !strings.EqualFold(firstName, secondName) {
			t.Errorf("FirstName(%s) != SecondName(%s)", firstName, secondName)
		}
	}

}

func TestZoneSupport(t *testing.T) {
	// context
	ctx := context.Background()

	// Start vcsim
	config, cleanup := configFromEnvOrSim(true)
	defer cleanup()

	connMgr := cm.NewConnectionManager(config, nil)
	defer connMgr.Logout()

	c := &controller{
		cfg:     config,
		connMgr: connMgr,
	}

	err := connMgr.Connect(ctx, config.Global.VCenterIP)
	if err != nil {
		t.Errorf("Failed to Connect to vSphere: %s", err)
	}

	// Get the vSphere Instance
	vsi := connMgr.VsphereInstanceMap[config.Global.VCenterIP]

	// Tag manager instance
	restClient := rest.NewClient(vsi.Conn.Client)
	user := url.UserPassword(vsi.Conn.Username, vsi.Conn.Password)
	if err := restClient.Login(ctx, user); err != nil {
		t.Fatalf("Rest login failed. err=%v", err)
	}

	m := tags.NewManager(restClient)

	/*
	 * START SETUP
	 */
	// Create a region category
	regionID, err := m.CreateCategory(ctx, &tags.Category{Name: config.Labels.Region})
	if err != nil {
		t.Fatal(err)
	}

	// Create a region tag
	regionID, err = m.CreateTag(ctx, &tags.Tag{CategoryID: regionID, Name: "k8s-region-US"})
	if err != nil {
		t.Fatal(err)
	}

	// Create a zone category
	zoneID, err := m.CreateCategory(ctx, &tags.Category{Name: config.Labels.Zone})
	if err != nil {
		t.Fatal(err)
	}

	// Create a zone tags
	zoneIDwest, err := m.CreateTag(ctx, &tags.Tag{CategoryID: zoneID, Name: "k8s-zone-US-west"})
	if err != nil {
		t.Fatal(err)
	}
	zoneIDeast, err := m.CreateTag(ctx, &tags.Tag{CategoryID: zoneID, Name: "k8s-zone-US-east"})
	if err != nil {
		t.Fatal(err)
	}

	// Setup a multi-DC environment with zones!
	// Setup DC0
	dc0, err := vclib.GetDatacenter(ctx, vsi.Conn, "DC0")
	if err != nil {
		t.Fatal(err)
	}

	// Attach tag to DC0
	if err = m.AttachTag(ctx, regionID, dc0); err != nil {
		t.Fatal(err)
	}
	if err = m.AttachTag(ctx, zoneIDwest, dc0); err != nil {
		t.Fatal(err)
	}

	// Setup DC1
	dc1, err := vclib.GetDatacenter(ctx, vsi.Conn, "DC1")
	if err != nil {
		t.Fatal(err)
	}

	// Attach tag to DC1
	if err = m.AttachTag(ctx, regionID, dc1); err != nil {
		t.Fatal(err)
	}
	if err = m.AttachTag(ctx, zoneIDeast, dc1); err != nil {
		t.Fatal(err)
	}

	// Get a single Datastore to target
	stores, err := dc1.GetAllDatastores(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(stores) == 0 {
		t.Fatal("No Datastores Found")
	}

	// Get the first Datastore
	var datastoreName string
	for _, store := range stores {
		datastoreName, err = store.Datastore.GetName(ctx)
		if err != nil {
			continue //failed to get name. try the next one.
		}
		break
	}
	if len(datastoreName) == 0 {
		t.Fatal("Failed to get a Datastore name")
	}
	/*
	 * END SETUP
	 */

	//create
	params := make(map[string]string, 0)
	params[AttributeFirstClassDiskParentType] = string(vclib.TypeDatastore)
	params[AttributeFirstClassDiskParentName] = datastoreName

	reqCreate := &csi.CreateVolumeRequest{
		Name: "test",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * GbInBytes,
		},
		Parameters: params,
		AccessibilityRequirements: &csi.TopologyRequirement{
			Requisite: make([]*csi.Topology, 0),
		},
	}

	// Target the eastern zone
	topology := &csi.Topology{
		Segments: make(map[string]string, 0),
	}
	topology.Segments[LabelZoneRegion] = "k8s-region-US"
	topology.Segments[LabelZoneFailureDomain] = "k8s-zone-US-east"

	reqCreate.AccessibilityRequirements.Requisite = append(reqCreate.AccessibilityRequirements.Requisite, topology)

	respCreate, err := c.CreateVolume(ctx, reqCreate)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	volID := respCreate.Volume.VolumeId
	volName := respCreate.Volume.VolumeContext[AttributeFirstClassDiskName]
	t.Logf("ID: %s", volID)
	t.Logf("%s: %s", AttributeFirstClassDiskName, volName)
	t.Logf("%s: %s", AttributeFirstClassDiskType, respCreate.Volume.VolumeContext[AttributeFirstClassDiskType])
	t.Logf("%s: %s", AttributeFirstClassDiskParentType, respCreate.Volume.VolumeContext[AttributeFirstClassDiskParentType])
	t.Logf("%s: %s", AttributeFirstClassDiskParentName, respCreate.Volume.VolumeContext[AttributeFirstClassDiskParentName])

	if !strings.EqualFold("test", volName) {
		t.Errorf("[CREATE] Name of FCD does not match test != %s", volName)
	}

	//delete
	reqDelete := &csi.DeleteVolumeRequest{
		VolumeId: volID,
	}
	_, err = c.DeleteVolume(ctx, reqDelete)
	if err != nil {
		t.Errorf("DeleteVolume failed: %v", err)
	}
}
