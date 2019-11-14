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
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/units"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

var (
	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
)

type NodeManagerInterface interface {
	Initialize() error
	GetSharedDatastoresInK8SCluster(ctx context.Context) ([]*cnsvsphere.DatastoreInfo, error)
	GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement, zoneKey string, regionKey string) ([]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error)
	GetNodeByName(nodeName string) (*cnsvsphere.VirtualMachine, error)
}

type controller struct {
	manager *common.Manager
	nodeMgr NodeManagerInterface
}

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *config.Config) error {
	klog.Infof("Initializing CNS controller")
	var err error
	// Get VirtualCenterManager instance and validate version
	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(config)
	if err != nil {
		klog.Errorf("Failed to get VirtualCenterConfig. err=%v", err)
		return err
	}
	vcManager := cnsvsphere.GetVirtualCenterManager()
	vcenter, err := vcManager.RegisterVirtualCenter(vcenterconfig)
	if err != nil {
		klog.Errorf("Failed to register VC with virtualCenterManager. err=%v", err)
		return err
	}
	c.manager = &common.Manager{
		VcenterConfig:  vcenterconfig,
		CnsConfig:      config,
		VolumeManager:  cnsvolume.GetManager(vcenter),
		VcenterManager: cnsvsphere.GetVirtualCenterManager(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		klog.Errorf("Failed to get vcenter. err=%v", err)
		return err
	}

	// Check vCenter API Version
	if err = common.CheckAPI(vc.Client.ServiceContent.About.ApiVersion); err != nil {
		klog.Errorf("checkAPI failed for vcenter API version: %s, err=%v", vc.Client.ServiceContent.About.ApiVersion, err)
		return err
	}
	c.nodeMgr = &Nodes{}
	err = c.nodeMgr.Initialize()
	if err != nil {
		klog.Errorf("Failed to initialize nodeMgr. err=%v", err)
		return err
	}
	return nil
}

// createBlockVolume creates a block volume based on the CreateVolumeRequest.
func (c *controller) createBlockVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {

	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	var datastoreURL string
	var storagePolicyName string
	var fsType string

	// Support case insensitive parameters
	for paramName := range req.Parameters {
		param := strings.ToLower(paramName)
		if param == common.AttributeDatastoreURL {
			datastoreURL = req.Parameters[paramName]
		} else if param == common.AttributeStoragePolicyName {
			storagePolicyName = req.Parameters[paramName]
		} else if param == common.AttributeFsType {
			fsType = req.Parameters[common.AttributeFsType]
		}
	}
	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:        volSizeMB,
		Name:              req.Name,
		DatastoreURL:      datastoreURL,
		StoragePolicyName: storagePolicyName,
		VolumeType:        common.BlockVolumeType,
	}

	var err error
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	var datastoreTopologyMap = make(map[string][]map[string]string)

	// Get accessibility
	topologyRequirement := req.GetAccessibilityRequirements()
	if topologyRequirement != nil {
		// Get shared accessible datastores for matching topology requirement
		if c.manager.CnsConfig.Labels.Zone == "" || c.manager.CnsConfig.Labels.Region == "" {
			// if zone and region label (vSphere category names) not specified in the config secret, then return
			// NotFound error.
			errMsg := fmt.Sprintf("Zone/Region vsphere category names not specified in the vsphere config secret")
			klog.Errorf(errMsg)
			return nil, status.Error(codes.NotFound, errMsg)
		}
		sharedDatastores, datastoreTopologyMap, err := c.nodeMgr.GetSharedDatastoresInTopology(ctx, topologyRequirement, c.manager.CnsConfig.Labels.Zone, c.manager.CnsConfig.Labels.Region)
		if err != nil || len(sharedDatastores) == 0 {
			msg := fmt.Sprintf("Failed to get shared datastores in topology: %+v. Error: %+v", topologyRequirement, err)
			klog.Errorf(msg)
			return nil, status.Error(codes.NotFound, msg)
		}
		klog.V(4).Infof("Shared datastores [%+v] retrieved for topologyRequirement [%+v] with datastoreTopologyMap [+%v]", sharedDatastores, topologyRequirement, datastoreTopologyMap)
		if createVolumeSpec.DatastoreURL != "" {
			// Check datastoreURL specified in the storageclass is accessible from topology
			isDataStoreAccessible := false
			for _, sharedDatastore := range sharedDatastores {
				if sharedDatastore.Info.Url == createVolumeSpec.DatastoreURL {
					isDataStoreAccessible = true
					break
				}
			}
			if !isDataStoreAccessible {
				errMsg := fmt.Sprintf("DatastoreURL: %s specified in the storage class is not accessible in the topology:[+%v]",
					createVolumeSpec.DatastoreURL, topologyRequirement)
				klog.Errorf(errMsg)
				return nil, status.Error(codes.InvalidArgument, errMsg)
			}
		}

	} else {
		sharedDatastores, err = c.nodeMgr.GetSharedDatastoresInK8SCluster(ctx)
		if err != nil || len(sharedDatastores) == 0 {
			msg := fmt.Sprintf("Failed to get shared datastores in kubernetes cluster. Error: %+v", err)
			klog.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}
	volumeID, err := common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla, c.manager, &createVolumeSpec, sharedDatastores)
	if err != nil {
		msg := fmt.Sprintf("Failed to create volume. Error: %+v", err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume

	if fsType == "" {
		klog.V(2).Infof("No fstype received. Defaulting to: %s", common.DefaultFsType)
		fsType = common.DefaultFsType
	}
	attributes[common.AttributeFsType] = fsType

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
		queryResult, err := c.manager.VolumeManager.QueryVolume(queryFilter)
		if err != nil {
			klog.Errorf("QueryVolume failed for volumeID: %s", volumeID)
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(queryResult.Volumes) > 0 {
			// Find datastore topology from the retrieved datastoreURL
			datastoreAccessibleTopology := datastoreTopologyMap[queryResult.Volumes[0].DatastoreUrl]
			klog.V(3).Infof("Volume: %s is provisioned on the datastore: %s ", volumeID, queryResult.Volumes[0].DatastoreUrl)
			if len(datastoreAccessibleTopology) > 0 {
				rand.Seed(time.Now().Unix())
				volumeAccessibleTopology = datastoreAccessibleTopology[rand.Intn(len(datastoreAccessibleTopology))]
				klog.V(3).Infof("volumeAccessibleTopology: [%+v] is selected for datastore: %s ", volumeAccessibleTopology, queryResult.Volumes[0].DatastoreUrl)
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

	// Fail the create workflow if topology requirements are set.
	if req.GetAccessibilityRequirements() != nil {
		msg := "Volume topology is not supported for file volumes."
		klog.Error(msg)
		return nil, status.Error(codes.Internal, msg)
	}

	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	var datastoreURL string
	var storagePolicyName string
	var fsType string

	// Support case insensitive parameters
	for paramName := range req.Parameters {
		param := strings.ToLower(paramName)
		if param == common.AttributeDatastoreURL {
			datastoreURL = req.Parameters[paramName]
		} else if param == common.AttributeStoragePolicyName {
			storagePolicyName = req.Parameters[paramName]
		} else if param == common.AttributeFsType {
			fsType = req.Parameters[common.AttributeFsType]
		}
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:        volSizeMB,
		Name:              req.Name,
		DatastoreURL:      datastoreURL,
		StoragePolicyName: storagePolicyName,
		VolumeType:        common.FileVolumeType,
	}

	volumeID, err := common.CreateFileVolumeUtil(ctx, cnstypes.CnsClusterFlavorVanilla, c.manager, &createVolumeSpec)
	if err != nil {
		msg := fmt.Sprintf("Failed to create volume. Error: %+v", err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeFileVolume

	if fsType == "" {
		klog.V(2).Infof("No fstype received. Defaulting to: %q", common.DefaultFsType)
		fsType = common.DefaultFsType
	}
	attributes[common.AttributeFsType] = fsType

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

	klog.V(4).Infof("CreateVolume: called with args %+v", *req)
	err := validateVanillaCreateVolumeRequest(req)
	if err != nil {
		klog.Errorf("Failed to validate Create Volume Request with err: %v", err)
		return nil, err
	}

	if common.IsFileVolumeRequest(req.GetVolumeCapabilities()) {
		return c.createFileVolume(ctx, req)
	} else {
		return c.createBlockVolume(ctx, req)
	}
}

// CreateVolume is deleting CNS Volume specified in DeleteVolumeRequest
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteVolume: called with args: %+v", *req)
	var err error
	err = validateVanillaDeleteVolumeRequest(req)
	if err != nil {
		return nil, err
	}
	err = common.DeleteVolumeUtil(ctx, c.manager, req.VolumeId, true)
	if err != nil {
		msg := fmt.Sprintf("Failed to delete volume: %q. Error: %+v", req.VolumeId, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches a volume to the Node VM.
// volume id and node name is retrieved from ControllerPublishVolumeRequest
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	klog.V(4).Infof("ControllerPublishVolume: called with args %+v", *req)
	err := validateVanillaControllerPublishVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	node, err := c.nodeMgr.GetNodeByName(req.NodeId)
	if err != nil {
		msg := fmt.Sprintf("Failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	klog.V(4).Infof("Found VirtualMachine for node:%q.", req.NodeId)
	publishInfo := make(map[string]string, 0)
	fsType, ok := req.VolumeContext[common.AttributeFsType]
	if !ok {
		fsType = common.DefaultFsType
		klog.V(3).Infof(fmt.Sprintf("fsType is not set in VolumeContext, use default type: %s", common.DefaultFsType))
	} else {
		klog.V(3).Infof(fmt.Sprintf("fsType from VolumeContext: %s", fsType))
	}
	// Check whether its a block or file volume
	if fsType == common.NfsV4FsType {
		// File Volume
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: req.VolumeId}},
		}
		queryResult, err := c.manager.VolumeManager.QueryVolume(queryFilter)
		if err != nil {
			msg := fmt.Sprintf("QueryVolume failed for volumeID: %q. %+v", req.VolumeId, err.Error())
			klog.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		if len(queryResult.Volumes) == 0 {
			msg := fmt.Sprintf("volumeID %s not found in QueryVolume", req.VolumeId)
			klog.Error(msg)
			return nil, status.Error(codes.Internal, msg)
		}
		nfsFileBackingDetails := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsNfsFileShareBackingDetails)
		publishInfo[common.AttributeDiskType] = common.DiskTypeFileVolume
		publishInfo[common.FileShareAddress] = nfsFileBackingDetails.Address
		publishInfo[common.FileShareName] = nfsFileBackingDetails.Name
	} else {
		// Block Volume
		diskUUID, err := common.AttachVolumeUtil(ctx, c.manager, node, req.VolumeId)
		if err != nil {
			msg := fmt.Sprintf("Failed to attach disk: %+q with node: %q err %+v", req.VolumeId, req.NodeId, err)
			klog.Error(msg)
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

	klog.V(4).Infof("ControllerUnpublishVolume: called with args %+v", *req)
	err := validateVanillaControllerUnpublishVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	node, err := c.nodeMgr.GetNodeByName(req.NodeId)
	if err != nil {
		msg := fmt.Sprintf("Failed to find VirtualMachine for node:%q. Error: %v", req.NodeId, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	err = common.DetachVolumeUtil(ctx, c.manager, node, req.VolumeId)
	if err != nil {
		msg := fmt.Sprintf("Failed to detach disk: %+q from node: %q err %+v", req.VolumeId, req.NodeId, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	resp := &csi.ControllerUnpublishVolumeResponse{}
	return resp, nil
}

// ControllerExpandVolume expands a volume.
// volume id and size is retrieved from ControllerExpandVolumeRequest
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).Infof("ControllerExpandVolume: called with args %+v", *req)

	err := validateVanillaControllerExpandVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("validation for ExpandVolume Request: %+v has failed. Error: %v", *req, err)
		klog.Error(msg)
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
	queryResult, err := c.manager.VolumeManager.QueryVolume(queryFilter)
	if err != nil {
		klog.Errorf("Failed to call QueryVolume for volumeID: %q: %v", volumeID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	var currentSize int64
	if len(queryResult.Volumes) > 0 {
		currentSize = queryResult.Volumes[0].BackingObjectDetails.(cnstypes.BaseCnsBackingObjectDetails).GetCnsBackingObjectDetails().CapacityInMb
	} else {
		msg := fmt.Sprintf("failed to find volume by querying volumeID: %q", volumeID)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	if currentSize >= volSizeMB {
		klog.Infof("Volume size %d is greater than or equal to the requested size %d for volumeID: %q", currentSize, volSizeMB, volumeID)
	} else {
		klog.V(4).Infof("Current volume size is %d, requested size is %d for volumeID: %q. Need volume expansion.", currentSize, volSizeMB, volumeID)

		err = common.ExpandVolumeUtil(ctx, c.manager, volumeID, volSizeMB)
		if err != nil {
			msg := fmt.Sprintf("failed to expand volume: %+q to size: %d err %+v", req.VolumeId, volSizeMB, err)
			klog.Error(msg)
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

	klog.V(4).Infof("ControllerGetCapabilities: called with args %+v", *req)
	volCaps := req.GetVolumeCapabilities()
	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if common.IsValidVolumeCapabilities(volCaps) {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (c *controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (
	*csi.ListVolumesResponse, error) {

	klog.V(4).Infof("ListVolumes: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (
	*csi.GetCapacityResponse, error) {

	klog.V(4).Infof("GetCapacity: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (
	*csi.ControllerGetCapabilitiesResponse, error) {

	klog.V(4).Infof("ControllerGetCapabilities: called with args %+v", *req)
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

	klog.V(4).Infof("CreateSnapshot: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (
	*csi.DeleteSnapshotResponse, error) {

	klog.V(4).Infof("DeleteSnapshot: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (c *controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (
	*csi.ListSnapshotsResponse, error) {

	klog.V(4).Infof("ListSnapshots: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}
