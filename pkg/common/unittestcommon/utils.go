/*
Copyright 2020 The Kubernetes Authors.

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

package unittestcommon

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/simulator/vpx"
	"google.golang.org/grpc/codes"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	cnssim "github.com/vmware/govmomi/cns/simulator"
	pbmsim "github.com/vmware/govmomi/pbm/simulator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/migration"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

var mapVolumePathToID map[string]map[string]string

// GetFakeContainerOrchestratorInterface returns a dummy CO interface based on the CO type
func GetFakeContainerOrchestratorInterface(orchestratorType int) (commonco.COCommonInterface, error) {
	if orchestratorType == common.Kubernetes {
		fakeCO := &FakeK8SOrchestrator{
			featureStatesLock: &sync.RWMutex{},
			featureStates: map[string]string{
				"volume-extend":                     "true",
				"volume-health":                     "true",
				"csi-migration":                     "true",
				"file-volume":                       "true",
				"block-volume-snapshot":             "true",
				"tkgs-ha":                           "true",
				"list-volumes":                      "true",
				"csi-internal-generated-cluster-id": "true",
				"online-volume-extend":              "true",
				"async-query-volume":                "true",
				"csi-windows-support":               "true",
				"use-csinode-id":                    "true",
				"pv-to-backingdiskobjectid-mapping": "false",
				"cnsmgr-suspend-create-volume":      "true",
				"topology-preferential-datastores":  "true",
				"max-pvscsi-targets-per-vm":         "true",
				"multi-vcenter-csi-topology":        "true",
				"listview-tasks":                    "true",
				"storage-quota-m2":                  "false",
			},
		}
		return fakeCO, nil
	}
	return nil, fmt.Errorf("unrecognized CO type")

}

// IsFSSEnabled returns the FSS values for a given feature
func (c *FakeK8SOrchestrator) IsFSSEnabled(ctx context.Context, featureName string) bool {
	var featureState bool
	var err error
	c.featureStatesLock.RLock()
	if flag, ok := c.featureStates[featureName]; ok {
		c.featureStatesLock.RUnlock()
		featureState, err = strconv.ParseBool(flag)
		if err != nil {
			return false
		}
		return featureState
	}
	c.featureStatesLock.RUnlock()
	return false
}

// IsFakeAttachAllowed checks if the passed volume can be fake attached and mark it as fake attached.
func (c *FakeK8SOrchestrator) IsFakeAttachAllowed(
	ctx context.Context,
	volumeID string,
	volumeManager cnsvolume.Manager,
) (bool, error) {
	// TODO - This can be implemented if we add WCP controller tests for attach volume
	log := logger.GetLogger(ctx)
	return false, logger.LogNewErrorCode(log, codes.Unimplemented,
		"IsFakeAttachAllowed for FakeK8SOrchestrator is not yet implemented.")
}

// MarkFakeAttached marks the volume as fake attached.
func (c *FakeK8SOrchestrator) MarkFakeAttached(ctx context.Context, volumeID string) error {
	// TODO - This can be implemented if we add WCP controller tests for attach volume
	log := logger.GetLogger(ctx)
	return logger.LogNewErrorCode(log, codes.Unimplemented,
		"MarkFakeAttached for FakeK8SOrchestrator is not yet implemented.")
}

// ClearFakeAttached checks if the volume was fake attached, and unmark it as not fake attached.
func (c *FakeK8SOrchestrator) ClearFakeAttached(ctx context.Context, volumeID string) error {
	// TODO - This can be implemented if we add WCP controller tests for attach volume
	log := logger.GetLogger(ctx)
	return logger.LogNewErrorCode(log, codes.Unimplemented,
		"ClearFakeAttached for FakeK8SOrchestrator is not yet implemented.")
}

// GetNodeTopologyLabels fetches the topology information of a node from the CSINodeTopology CR.
func (nodeTopology *mockNodeVolumeTopology) GetNodeTopologyLabels(ctx context.Context, info *commoncotypes.NodeInfo) (
	map[string]string, error) {
	log := logger.GetLogger(ctx)
	return nil, logger.LogNewError(log, "GetNodeTopologyLabels is not yet implemented.")
}

// GetSharedDatastoresInTopology retrieves shared datastores of nodes which satisfy a given topology requirement.
func (cntrlTopology *mockControllerVolumeTopology) GetSharedDatastoresInTopology(ctx context.Context,
	reqParams interface{}) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	return nil, logger.LogNewError(log, "GetSharedDatastoresInTopology is not yet implemented.")
}

// GetAZClustersMap returns the zone to clusterMorefs map from the azClustersMap.
func (cntrlTopology *mockControllerVolumeTopology) GetAZClustersMap(ctx context.Context) map[string][]string {
	return nil
}

// GetTopologyInfoFromNodes retrieves the topology information of the given list of node names.
func (cntrlTopology *mockControllerVolumeTopology) GetTopologyInfoFromNodes(ctx context.Context,
	reqParams interface{}) ([]map[string]string, error) {
	log := logger.GetLogger(ctx)
	return nil, logger.LogNewError(log, "GetTopologyInfoFromNodes is not yet implemented.")
}

// InitTopologyServiceInController returns a singleton implementation of the
// commoncotypes.ControllerTopologyService interface for the FakeK8SOrchestrator.
func (c *FakeK8SOrchestrator) InitTopologyServiceInController(ctx context.Context) (
	commoncotypes.ControllerTopologyService, error) {
	// TODO: Mock the k8sClients, node manager and informer.
	return &mockControllerVolumeTopology{}, nil
}

// InitTopologyServiceInNode returns a singleton implementation of the
// commoncotypes.NodeTopologyService interface for the FakeK8SOrchestrator.
func (c *FakeK8SOrchestrator) InitTopologyServiceInNode(ctx context.Context) (
	commoncotypes.NodeTopologyService, error) {
	// TODO: Mock the custom k8sClients and watchers.
	return &mockNodeVolumeTopology{}, nil
}

// GetFakeVolumeMigrationService returns the mocked VolumeMigrationService
func GetFakeVolumeMigrationService(
	ctx context.Context,
	volumeManager *cnsvolume.Manager,
	cnsConfig *config.Config,
) (MockVolumeMigrationService, error) {
	// fakeVolumeMigrationInstance is a mocked instance of volumeMigration
	fakeVolumeMigrationInstance := &mockVolumeMigration{
		volumePathToVolumeID: sync.Map{},
		volumeManager:        volumeManager,
		cnsConfig:            cnsConfig,
	}
	// Storing a dummy volume migration instance in the mapping
	mapVolumePathToID = make(map[string]map[string]string)
	mapVolumePathToID = map[string]map[string]string{
		"dummy-vms-CR": {
			"VolumePath": "[vsanDatastore] 9c01ed5e/kubernetes-dynamic-pvc-d8698e54.vmdk",
			"VolumeID":   "191c6d51-ed59-4340-9841-638c09f642b7",
		},
	}
	return fakeVolumeMigrationInstance, nil
}

// GetVolumeID mocks the method with returns Volume Id for a given Volume Path
func (dummyInstance *mockVolumeMigration) GetVolumeID(
	ctx context.Context,
	volumeSpec *migration.VolumeSpec,
) (string, error) {
	return mapVolumePathToID["dummy-vms-CR"][volumeSpec.VolumePath], nil
}

// GetVolumePath mocks the method with returns Volume Path for a given Volume ID
func (dummyInstance *mockVolumeMigration) GetVolumePath(ctx context.Context, volumeID string) (string, error) {
	return mapVolumePathToID["dummy-vms-CR"]["VolumePath"], nil
}

// DeleteVolumeInfo mocks the method to delete mapping of volumePath to VolumeID for specified volumeID
func (dummyInstance *mockVolumeMigration) DeleteVolumeInfo(ctx context.Context, volumeID string) error {
	delete(mapVolumePathToID, "dummy-vms-CR")
	return nil
}

// InitFakeVolumeOperationRequestInterface returns a fake implementation
// of the VolumeOperationRequest interface.
func InitFakeVolumeOperationRequestInterface() (cnsvolumeoperationrequest.VolumeOperationRequest, error) {
	return &fakeVolumeOperationRequestInterface{
		volumeOperationRequestMap: make(map[string]*cnsvolumeoperationrequest.VolumeOperationRequestDetails),
	}, nil
}

// GetRequestDetails returns the VolumeOperationRequestDetails for the given
// name, if any, stored by the fake VolumeOperationRequest interface.
func (f *fakeVolumeOperationRequestInterface) GetRequestDetails(
	ctx context.Context,
	name string,
) (*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
	instance, ok := f.volumeOperationRequestMap[name]
	if !ok {
		return nil, apierrors.NewNotFound(cnsvolumeoprequestv1alpha1.Resource(
			"cnsvolumeoperationrequests"), name)
	}
	return instance, nil

}

// StoreRequestDetails stores the input fake VolumeOperationRequestDetails.
func (f *fakeVolumeOperationRequestInterface) StoreRequestDetails(
	ctx context.Context,
	instance *cnsvolumeoperationrequest.VolumeOperationRequestDetails,
) error {
	f.volumeOperationRequestMap[instance.Name] = instance
	return nil
}

// DeleteRequestDetails deletes the VolumeOperationRequestDetails for the given
// name, if any, stored by the fake VolumeOperationRequest interface.
func (f *fakeVolumeOperationRequestInterface) DeleteRequestDetails(
	ctx context.Context,
	name string,
) error {
	delete(f.volumeOperationRequestMap, name)
	return nil
}

// GetNodesForVolumes returns nodeNames to which the given volumeIDs are attached
func (c *FakeK8SOrchestrator) GetNodesForVolumes(ctx context.Context, volumeID []string) map[string][]string {
	nodeNames := make(map[string][]string)
	return nodeNames
}

// GetNodeIDtoNameMap returns a map containing the nodeID to node name
func (c *FakeK8SOrchestrator) GetNodeIDtoNameMap(ctx context.Context) map[string]string {
	nodeIDToNamesMap := make(map[string]string)
	return nodeIDToNamesMap
}

// GetFakeAttachedVolumes returns a map of volumeIDs to a bool, which is set
// to true if volumeID key is fake attached else false
func (c *FakeK8SOrchestrator) GetFakeAttachedVolumes(ctx context.Context, volumeID []string) map[string]bool {
	fakeAttachedVolumes := make(map[string]bool)
	return fakeAttachedVolumes
}

// GetVolumeAttachment returns the VA object by using the given volumeId & nodeName
func (c *FakeK8SOrchestrator) GetVolumeAttachment(ctx context.Context, volumeId string, nodeName string) (
	*storagev1.VolumeAttachment, error) {
	return nil, nil
}

// GetAllVolumes returns list of volumes in a bound state
func (c *FakeK8SOrchestrator) GetAllVolumes() []string {
	// TODO - This can be implemented if we add WCP controller tests for list volume
	return nil
}

// GetAllK8sVolumes returns list of volumes in a bound state, present in the K8s cluster
func (c *FakeK8SOrchestrator) GetAllK8sVolumes() []string {
	return nil
}

// AnnotateVolumeSnapshot annotates the volumesnapshot CR in k8s cluster
func (c *FakeK8SOrchestrator) AnnotateVolumeSnapshot(ctx context.Context, volumeSnapshotName string,
	volumeSnapshotNamespace string, annotations map[string]string) (bool, error) {
	return true, nil
}

// GetConfigMap checks if ConfigMap with given name exists in the given namespace.
// If it exists, this function returns ConfigMap data, otherwise returns error.
func (c *FakeK8SOrchestrator) GetConfigMap(ctx context.Context, name string,
	namespace string) (map[string]string, error) {
	return nil, nil
}

// CreateConfigMap creates the ConfigMap with given name, namespace, data and immutable
// parameter values.
func (c *FakeK8SOrchestrator) CreateConfigMap(ctx context.Context, name string, namespace string,
	data map[string]string, isImmutable bool) error {
	return nil
}

// GetCSINodeTopologyInstancesList lists CSINodeTopology instances for a given cluster.
func (c *FakeK8SOrchestrator) GetCSINodeTopologyInstancesList() []interface{} {
	return nil
}

// GetCSINodeTopologyInstanceByName fetches the CSINodeTopology instance for a given node name in the cluster.
func (c *FakeK8SOrchestrator) GetCSINodeTopologyInstanceByName(nodeName string) (
	item interface{}, exists bool, err error) {
	return nil, false, nil
}

// GetPVNameFromCSIVolumeID retrieves the pv name from volumeID.
func (c *FakeK8SOrchestrator) GetPVNameFromCSIVolumeID(volumeID string) (string, bool) {
	return "", false
}

// InitializeCSINodes creates CSINode instances for each K8s node with the appropriate topology keys.
func (c *FakeK8SOrchestrator) InitializeCSINodes(ctx context.Context) error {
	return nil
}

// configFromVCSim starts a vcsim instance and returns config for use against the
// vcsim instance. The vcsim instance is configured with an empty tls.Config.
func configFromVCSim(vcsimParams VcsimParams, isTopologyEnv bool) (*config.Config, func()) {
	return configFromVCSimWithTLS(new(tls.Config), vcsimParams, true, isTopologyEnv)
}

// configFromVCSimWithTLS starts a vcsim instance and returns config for use
// against the vcsim instance. The vcsim instance is configured with a
// tls.Config. The returned client config can be configured to allow/decline
// insecure connections.
func configFromVCSimWithTLS(tlsConfig *tls.Config, vcsimParams VcsimParams, insecureAllowed bool,
	isTopologyEnv bool) (*config.Config, func()) {
	cfg := &config.Config{}
	model := simulator.VPX()
	// Use user specified values for fields like datacenters, clusters, hosts, VMs, datastores etc.
	if vcsimParams.Datacenters != VCSimDefaultDatacenters {
		model.Datacenter = vcsimParams.Datacenters
	}
	if vcsimParams.Clusters != VCSimDefaultClusters {
		model.Cluster = vcsimParams.Clusters
	}
	if vcsimParams.HostsPerCluster != VCSimDefaultHostsPerCluster {
		model.ClusterHost = vcsimParams.HostsPerCluster
	}
	if vcsimParams.StandaloneHosts != VCSimDefaultStandalonHosts {
		model.Host = vcsimParams.StandaloneHosts
	}
	if vcsimParams.Datastores != VCSimDefaultDatastores {
		model.Datastore = vcsimParams.Datastores
	}
	if vcsimParams.VMsPerCluster != VCSimDefaultVMsPerCluster {
		model.Machine = vcsimParams.VMsPerCluster
	}

	serviceContent := vpx.ServiceContent
	serviceContent.About.Version = vcsimParams.Version
	serviceContent.About.ApiVersion = vcsimParams.ApiVersion
	model.ServiceContent = serviceContent

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
	cfg.Global.User = s.URL.User.Username() + "@vsphere.local"
	cfg.Global.Password, _ = s.URL.User.Password()

	datacenters := "DC0"
	for i := 1; i < vcsimParams.Datacenters; i++ {
		datacenters = datacenters + ", DC" + strconv.Itoa(i)
	}
	cfg.Global.Datacenters = datacenters

	if isTopologyEnv {
		cfg.Labels.TopologyCategories = "k8s-region, k8s-zone"
	}

	// Write values to test_vsphere.conf.
	var conf []byte
	os.Setenv("VSPHERE_CSI_CONFIG", "test_vsphere.conf")
	if isTopologyEnv {
		conf = []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
			"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"\n"+
			"[Labels]\ntopology-categories = \"%s\"",
			cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
			cfg.Global.Datacenters, cfg.Global.VCenterPort, cfg.Labels.TopologyCategories))
	} else {
		conf = []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
			"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"",
			cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
			cfg.Global.Datacenters, cfg.Global.VCenterPort))
	}
	err = os.WriteFile("test_vsphere.conf", conf, 0644)
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
		os.Unsetenv("VSPHERE_CSI_CONFIG")
		os.Remove("test_vsphere.conf")
	}
}

func ConfigFromEnvOrVCSim(ctx context.Context, vcsimParams VcsimParams, isTopologyEnv bool) (*config.Config, func()) {
	cfg := &config.Config{}
	if err := config.FromEnv(ctx, cfg); err != nil {
		return configFromVCSim(vcsimParams, isTopologyEnv)
	}
	return cfg, func() {}
}
