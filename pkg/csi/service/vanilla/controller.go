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
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/vmware/govmomi/cns"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"github.com/zekroTJA/timedmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

// NodeManagerInterface provides functionality to manage nodes.
type NodeManagerInterface interface {
	Initialize(ctx context.Context) error
	GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error)
	GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement, zoneKey string, regionKey string) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error)
	GetNodeByName(ctx context.Context, nodeName string) (*cnsvsphere.VirtualMachine, error)
	GetAllNodes(ctx context.Context) ([]*cnsvsphere.VirtualMachine, error)
}

type controller struct {
	manager *common.Manager
	nodeMgr NodeManagerInterface
}

// timedmap of deleted volumes. This map used to resolve race between detach and delete volume
// If volume is present in this map, then detach volume operation can be skipped.
// TODO: Remove this when https://github.com/kubernetes/kubernetes/issues/84226 is fixed
var deletedVolumes *timedmap.TimedMap

var (
	// VSAN67u3ControllerServiceCapability represents the capability of controller service
	// for VSAN67u3 release
	VSAN67u3ControllerServiceCapability = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}

	// VSAN7ControllerServiceCapability represents the capability of controller service
	// for VSAN 7.0 release
	VSAN7ControllerServiceCapability = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
)

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *config.Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	log.Infof("Initializing CNS controller")
	var err error
	// Get VirtualCenterManager instance and validate version
	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(config)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return err
	}
	vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
	vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
	if err != nil {
		log.Errorf("failed to register VC with virtualCenterManager. err=%v", err)
		return err
	}
	c.manager = &common.Manager{
		VcenterConfig:  vcenterconfig,
		CnsConfig:      config,
		VolumeManager:  cnsvolume.GetManager(ctx, vcenter),
		VcenterManager: vcManager,
	}

	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		log.Errorf("failed to get vcenter. err=%v", err)
		return err
	}

	if len(c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) > 0 {
		// Check if file service is enabled on datastore present in targetvSANFileShareDatastoreURLs.
		dsToFileServiceEnabledMap, err := common.IsFileServiceEnabled(ctx, c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs, c.manager)
		if err != nil {
			msg := fmt.Sprintf("file service enablement check failed for datastore specified in TargetvSANFileShareDatastoreURLs. err=%v", err)
			log.Errorf(msg)
			return errors.New(msg)
		}
		for _, targetFSDatastore := range c.manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
			isFSEnabled := dsToFileServiceEnabledMap[targetFSDatastore]
			if !isFSEnabled {
				msg := fmt.Sprintf("file service is not enabled on datastore %s specified in TargetvSANFileShareDatastoreURLs", targetFSDatastore)
				log.Errorf(msg)
				return errors.New(msg)
			}
		}
	}

	// Check vCenter API Version
	if err = common.CheckAPI(vc.Client.ServiceContent.About.ApiVersion); err != nil {
		log.Errorf("checkAPI failed for vcenter API version: %s, err=%v", vc.Client.ServiceContent.About.ApiVersion, err)
		return err
	}
	c.nodeMgr = &Nodes{}
	err = c.nodeMgr.Initialize(ctx)
	if err != nil {
		log.Errorf("failed to initialize nodeMgr. err=%v", err)
		return err
	}
	go cnsvolume.ClearTaskInfoObjects()
	cfgPath := common.GetConfigPath(ctx)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Errorf("failed to create fsnotify watcher. err=%v", err)
		return err
	}
	go func() {
		for {
			log.Debugf("Waiting for event on fsnotify watcher")
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Debugf("fsnotify event: %q", event.String())
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					log.Infof("Reloading Configuration")
					c.ReloadConfiguration(ctx)
					log.Infof("Successfully reloaded configuration from: %q", cfgPath)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Errorf("fsnotify error: %+v", err)
					return
				}
			}
			log.Debugf("fsnotify event processed")
		}
	}()
	cfgDirPath := filepath.Dir(cfgPath)
	log.Infof("Adding watch on path: %q", cfgDirPath)
	err = watcher.Add(cfgDirPath)
	if err != nil {
		log.Errorf("failed to watch on path: %q. err=%v", cfgDirPath, err)
		return err
	}
	// deletedVolumes timedmap with clean up interval of 1 minute to remove expired entries
	deletedVolumes = timedmap.New(1 * time.Minute)
	return nil
}

// ReloadConfiguration reloads configuration from the secret, and update controller's config cache
// and VolumeManager's VC Config cache.
func (c *controller) ReloadConfiguration(ctx context.Context) {
	log := logger.GetLogger(ctx)
	cfg, err := common.GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return
	}
	newVCConfig, err := cnsvsphere.GetVirtualCenterConfig(cfg)
	if err != nil {
		log.Errorf("failed to get VirtualCenterConfig. err=%v", err)
		return
	}
	if newVCConfig != nil {
		var vcenter *cnsvsphere.VirtualCenter
		if c.manager.VcenterConfig.Host != newVCConfig.Host ||
			c.manager.VcenterConfig.Username != newVCConfig.Username ||
			c.manager.VcenterConfig.Password != newVCConfig.Password {
			log.Debugf("Unregistering virtual center: %q from virtualCenterManager", c.manager.VcenterConfig.Host)
			err = c.manager.VcenterManager.UnregisterAllVirtualCenters(ctx)
			if err != nil {
				log.Errorf("failed to unregister vcenter with virtualCenterManager.")
				return
			}
			log.Debugf("Registering virtual center: %q with virtualCenterManager", newVCConfig.Host)
			vcenter, err = c.manager.VcenterManager.RegisterVirtualCenter(ctx, newVCConfig)
			if err != nil {
				log.Errorf("failed to register VC with virtualCenterManager. err=%v", err)
				return
			}
			c.manager.VcenterManager = cnsvsphere.GetVirtualCenterManager(ctx)
		} else {
			vcenter, err = c.manager.VcenterManager.GetVirtualCenter(ctx, newVCConfig.Host)
			if err != nil {
				log.Errorf("failed to get VirtualCenter. err=%v", err)
				return
			}
		}
		c.manager.VolumeManager.ResetManager(ctx, vcenter)
		c.manager.VolumeManager = cnsvolume.GetManager(ctx, vcenter)
		c.manager.VcenterConfig = newVCConfig
	}
	if cfg != nil {
		log.Debugf("Updating manager.CnsConfig")
		c.manager.CnsConfig = cfg
	}
}

// createBlockVolume creates a block volume based on the CreateVolumeRequest.
func (c *controller) createBlockVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters)
	if err != nil {
		msg := fmt.Sprintf("Parsing storage class parameters failed with error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.InvalidArgument, msg)
	}
	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB: volSizeMB,
		Name:       req.Name,
		ScParams:   scParams,
		VolumeType: common.BlockVolumeType,
	}

	var sharedDatastores []*cnsvsphere.DatastoreInfo
	var datastoreTopologyMap = make(map[string][]map[string]string)

	// Get accessibility
	topologyRequirement := req.GetAccessibilityRequirements()
	if topologyRequirement != nil {
		// Get shared accessible datastores for matching topology requirement
		if c.manager.CnsConfig.Labels.Zone == "" || c.manager.CnsConfig.Labels.Region == "" {
			// if zone and region label (vSphere category names) not specified in the config secret, then return
			// NotFound error.
			errMsg := "Zone/Region vsphere category names not specified in the vsphere config secret"
			log.Errorf(errMsg)
			return nil, status.Error(codes.NotFound, errMsg)
		}
		sharedDatastores, datastoreTopologyMap, err = c.nodeMgr.GetSharedDatastoresInTopology(ctx, topologyRequirement, c.manager.CnsConfig.Labels.Zone, c.manager.CnsConfig.Labels.Region)
		if err != nil || len(sharedDatastores) == 0 {
			msg := fmt.Sprintf("failed to get shared datastores in topology: %+v. Error: %+v", topologyRequirement, err)
			log.Errorf(msg)
			return nil, status.Error(codes.NotFound, msg)
		}
		log.Debugf("Shared datastores [%+v] retrieved for topologyRequirement [%+v] with datastoreTopologyMap [+%v]", sharedDatastores, topologyRequirement, datastoreTopologyMap)
		if createVolumeSpec.ScParams.DatastoreURL != "" {
			// Check datastoreURL specified in the storageclass is accessible from topology
			isDataStoreAccessible := false
			for _, sharedDatastore := range sharedDatastores {
				if sharedDatastore.Info.Url == createVolumeSpec.ScParams.DatastoreURL {
					isDataStoreAccessible = true
					break
				}
			}
			if !isDataStoreAccessible {
				errMsg := fmt.Sprintf("DatastoreURL: %s specified in the storage class is not accessible in the topology:[+%v]",
					createVolumeSpec.ScParams.DatastoreURL, topologyRequirement)
				log.Errorf(errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}
		}

	} else {
		sharedDatastores, err = c.nodeMgr.GetSharedDatastoresInK8SCluster(ctx)
		if err != nil || len(sharedDatastores) == 0 {
			msg := fmt.Sprintf("failed to get shared datastores in kubernetes cluster. Error: %+v", err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}
	volumeID, err := common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla, c.manager, &createVolumeSpec, sharedDatastores)
	if err != nil {
		msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}

	// Call QueryVolume API and get the datastoreURL of the Provisioned Volume
	var volumeAccessibleTopology = make(map[string]string)
	if len(datastoreTopologyMap) > 0 {
		volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: volumeIds,
		}
		queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			log.Errorf("QueryVolume failed for volumeID: %s", volumeID)
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(queryResult.Volumes) > 0 {
			// Find datastore topology from the retrieved datastoreURL
			datastoreAccessibleTopology := datastoreTopologyMap[queryResult.Volumes[0].DatastoreUrl]
			log.Debugf("Volume: %s is provisioned on the datastore: %s ", volumeID, queryResult.Volumes[0].DatastoreUrl)
			if len(datastoreAccessibleTopology) > 0 {
				rand.Seed(time.Now().Unix())
				volumeAccessibleTopology = datastoreAccessibleTopology[rand.Intn(len(datastoreAccessibleTopology))]
				log.Debugf("volumeAccessibleTopology: [%+v] is selected for datastore: %s ", volumeAccessibleTopology, queryResult.Volumes[0].DatastoreUrl)
			}
		}
	}
	if len(volumeAccessibleTopology) != 0 {
		volumeTopology := &csi.Topology{
			Segments: volumeAccessibleTopology,
		}
		resp.Volume.AccessibleTopology = append(resp.Volume.AccessibleTopology, volumeTopology)
	}
	return resp, nil
}

// createFileVolume creates a file volume based on the CreateVolumeRequest.
func (c *controller) createFileVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// ignore TopologyRequirement for file volume provisioning
	if req.GetAccessibilityRequirements() != nil {
		log.Info("Ignoring TopologyRequirement for file volume")
	}

	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	scParams, err := common.ParseStorageClassParams(ctx, req.Parameters)
	if err != nil {
		msg := fmt.Sprintf("Parsing storage class parameters failed with error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.InvalidArgument, msg)
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB: volSizeMB,
		Name:       req.Name,
		ScParams:   scParams,
		VolumeType: common.FileVolumeType,
	}

	volumeID, err := common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla, c.manager, &createVolumeSpec)
	if err != nil {
		msg := fmt.Sprintf("failed to create volume. Error: %+v", err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeFileVolume

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
			VolumeContext: attributes,
		},
	}
	return resp, nil
}

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("CreateVolume: called with args %+v", *req)
	err := validateVanillaCreateVolumeRequest(ctx, req)
	if err != nil {
		log.Errorf("failed to validate Create Volume Request with err: %v", err)
		return nil, err
	}

	if common.IsFileVolumeRequest(ctx, req.GetVolumeCapabilities()) {
		vsan67u3Release, err := isVsan67u3Release(ctx, c)
		if err != nil {
			log.Error("failed to get vcenter version to help identify if fileshare volume creation should be permitted or not. Error:%v", err)
			return nil, status.Error(codes.Internal, err.Error())
		}
		if vsan67u3Release {
			msg := "fileshare volume creation is not supported on vSAN 67u3 release"
			log.Error(msg)
			return nil, status.Error(codes.FailedPrecondition, msg)
		}
		return c.createFileVolume(ctx, req)
	}
	return c.createBlockVolume(ctx, req)
}

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("DeleteVolume: called with args: %+v", *req)
	var err error
	err = validateVanillaDeleteVolumeRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	err = common.DeleteVolumeUtil(ctx, c.manager, req.VolumeId, true)
	if err != nil {
		msg := fmt.Sprintf("failed to delete volume: %q. Error: %+v", req.VolumeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	deletedVolumes.Set(req.VolumeId, true, 5*time.Minute)
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches a volume to the Node VM.
// volume id and node name is retrieved from ControllerPublishVolumeRequest
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerPublishVolume: called with args %+v", *req)
	err := validateVanillaControllerPublishVolumeRequest(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	node, err := c.nodeMgr.GetNodeByName(ctx, req.NodeId)
	if err != nil {
		msg := fmt.Sprintf("failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	log.Debugf("Found VirtualMachine for node:%q.", req.NodeId)
	publishInfo := make(map[string]string)
	// Check whether its a block or file volume
	if common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{req.GetVolumeCapability()}) {
		// File Volume
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
		}
		queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			msg := fmt.Sprintf("QueryVolume failed for volumeID: %q. %+v", req.VolumeId, err.Error())
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		if len(queryResult.Volumes) == 0 {
			msg := fmt.Sprintf("volumeID %s not found in QueryVolume", req.VolumeId)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}

		vSANFileBackingDetails := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
		publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
		nfsv4AccessPointFound := false
		for _, kv := range vSANFileBackingDetails.AccessPoints {
			if kv.Key == common.Nfsv4AccessPointKey {
				publishInfo[common.Nfsv4AccessPoint] = kv.Value
				nfsv4AccessPointFound = true
				break
			}
		}
		if !nfsv4AccessPointFound {
			msg := fmt.Sprintf("failed to get NFSv4 access point for volume: %q."+
				" Returned vSAN file backing details : %+v", req.VolumeId, vSANFileBackingDetails)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	} else {
		// Block Volume
		diskUUID, err := common.AttachVolumeUtil(ctx, c.manager, node, req.VolumeId)
		if err != nil {
			msg := fmt.Sprintf("failed to attach disk: %+q with node: %q err %+v", req.VolumeId, req.NodeId, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
		publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
		publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
	}
	resp := &csi.ControllerPublishVolumeResponse{
		PublishContext: publishInfo,
	}
	return resp, nil
}

// ControllerUnpublishVolume detaches a volume from the Node VM.
// volume id and node name is retrieved from ControllerUnpublishVolumeRequest
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerUnpublishVolume: called with args %+v", *req)
	err := validateVanillaControllerUnpublishVolumeRequest(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	// Check for the race condition where DeleteVolume is called before ControllerUnpublishVolume
	if deletedVolumes.Contains(req.VolumeId) {
		log.Info("Skipping ControllerUnpublish for deleted volume ", req.VolumeId)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
	}
	queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, queryFilter)
	if err != nil {
		msg := fmt.Sprintf("QueryVolume failed for volumeID: %q. %+v", req.VolumeId, err.Error())
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}
	if len(queryResult.Volumes) == 0 {
		msg := fmt.Sprintf("volumeID %q not found in QueryVolume", req.VolumeId)
		log.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}
	if queryResult.Volumes[0].VolumeType != common.FileVolumeType {
		node, err := c.nodeMgr.GetNodeByName(ctx, req.NodeId)
		if err != nil {
			msg := fmt.Sprintf("failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		err = common.DetachVolumeUtil(ctx, c.manager, node, req.VolumeId)
		if err != nil {
			msg := fmt.Sprintf("failed to detach disk: %+q from node: %q err %+v", req.VolumeId, req.NodeId, err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
	} else {
		log.Info("Skipping ControllerUnpublish for file volume ", req.VolumeId)
	}

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ControllerExpandVolume expands a volume.
// volume id and size is retrieved from ControllerExpandVolumeRequest
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerExpandVolume: called with args %+v", *req)

	err := validateVanillaControllerExpandVolumeRequest(ctx, req)
	if err != nil {
		msg := fmt.Sprintf("validation for ExpandVolume Request: %+v has failed. Error: %v", *req, err)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	volumeID := req.GetVolumeId()
	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Check if volume is already expanded
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
	}
	queryResult, err := c.manager.VolumeManager.QueryVolume(ctx, queryFilter)
	if err != nil {
		log.Errorf("failed to call QueryVolume for volumeID: %q: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	var currentSize int64
	if len(queryResult.Volumes) > 0 {
		currentSize = queryResult.Volumes[0].BackingObjectDetails.(cnstypes.BaseCnsBackingObjectDetails).GetCnsBackingObjectDetails().CapacityInMb
	} else {
		msg := fmt.Sprintf("failed to find volume by querying volumeID: %q", volumeID)
		log.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	log.Debugf("volumeID: %q volume type: %q.", volumeID, queryResult.Volumes[0].VolumeType)
	// TODO(xyang): ControllerExpandVolumeRequest does not have VolumeCapability in
	// CSI 1.1.0 so we can't find out the fstype from that and determine
	// whether it is a file volume. VolumeCapability is added to ControllerExpandVolumeRequest
	// in CSI 1.2.0 but resizer sidecar is not released yet to support that. Currently
	// the latest resizer is v0.3.0 which supports CSI 1.1.0. When VolumeCapability
	// becomes available in ControllerExpandVolumeRequest, we should use that to find out
	// whether it is a file volume and do the validation in validateVanillaControllerExpandVolumeRequest.
	if queryResult.Volumes[0].VolumeType != common.BlockVolumeType {
		msg := fmt.Sprintf("volume type for volumeID %q is %q. Volume expansion is only supported for block volume type.", volumeID, queryResult.Volumes[0].VolumeType)
		log.Error(msg)
		return nil, status.Errorf(codes.Unimplemented, msg)
	}

	if currentSize >= volSizeMB {
		log.Infof("Volume size %d is greater than or equal to the requested size %d for volumeID: %q", currentSize, volSizeMB, volumeID)
	} else {
		// Check if volume is attached to a node
		log.Infof("Check if volume %q is attached to a node", volumeID)
		nodes, err := c.nodeMgr.GetAllNodes(ctx)
		if err != nil {
			msg := fmt.Sprintf("failed to find VirtualMachines for all registered nodes. Error: %v", err)
			log.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}

		for _, node := range nodes {
			// Check if volume is attached to any node. If so,
			// fail the operation as only offline volume expansion is supported.
			diskUUID, err := cnsvolume.IsDiskAttached(ctx, node, volumeID)
			if err != nil {
				msg := fmt.Sprintf("expand volume has failed with err: %q. Unable to check if volume: %q is attached to node: %+v",
					err, volumeID, node)
				log.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			} else if diskUUID != "" {
				msg := fmt.Sprintf("failed to expand volume: %+q to size: %d. Volume is attached to node %q. Only offline volume expansion is supported", volumeID, volSizeMB, node.UUID)
				log.Error(msg)
				return nil, status.Errorf(codes.FailedPrecondition, msg)
			}
		}

		log.Infof("Current volume size is %d, requested size is %d for volumeID: %q. Need volume expansion.", currentSize, volSizeMB, volumeID)

		err = common.ExpandVolumeUtil(ctx, c.manager, volumeID, volSizeMB)
		if err != nil {
			msg := fmt.Sprintf("failed to expand volume: %+q to size: %d err %+v", req.VolumeId, volSizeMB, err)
			log.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}

	// TODO(xyang): In CSI spec 1.2, ControllerExpandVolume will be
	// passing in VolumeCapability with access_type info indicating
	// whether it is BlockVolume (raw block mode) or MountVolume (file
	// system mode). Update this function when CSI spec 1.2 is supported
	// by external-resizer. For BlockVolume, set NodeExpansionRequired to
	// false; for MountVolume, set it to true.

	resp := &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(units.FileSize(volSizeMB * common.MbInBytes)),
		NodeExpansionRequired: true,
	}
	return resp, nil
}

// ValidateVolumeCapabilities returns the capabilities of the volume.
func (c *controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", *req)
	volCaps := req.GetVolumeCapabilities()
	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if common.IsValidVolumeCapabilities(ctx, volCaps) {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (c *controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ListVolumes: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("GetCapacity: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

// isVsan67u3Release returns true if controller is dealing with vSAN 67u3 Release of vCenter.
func isVsan67u3Release(ctx context.Context, c *controller) (bool, error) {
	log := logger.GetLogger(ctx)
	log.Debug("Checking if vCenter version is of vsan 67u3 release")
	if c.manager == nil || c.manager.VolumeManager == nil {
		return false, errors.New("cannot retrieve vcenter version. controller manager is not initialized")
	}
	vc, err := c.manager.VcenterManager.GetVirtualCenter(ctx, c.manager.VcenterConfig.Host)
	if err != nil || vc == nil {
		log.Errorf("failed to get vcenter version. Err: %v", err)
		return false, err
	}
	log.Debugf("vCenter version is :%q", vc.Client.Version)
	if vc.Client.Version == cns.ReleaseVSAN67u3 {
		return true, nil
	}
	return false, nil
}

func (c *controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ControllerGetCapabilities: called with args %+v", *req)

	var controllerCaps []csi.ControllerServiceCapability_RPC_Type

	vsan67u3Release, err := isVsan67u3Release(ctx, c)
	if err != nil {
		log.Error("failed to get vcenter version to help identify controller service capabilities")
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if vsan67u3Release {
		controllerCaps = VSAN67u3ControllerServiceCapability
	} else {
		controllerCaps = VSAN7ControllerServiceCapability
	}

	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (c *controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (
	*csi.CreateSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("CreateSnapshot: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("DeleteSnapshot: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("ListSnapshots: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}
