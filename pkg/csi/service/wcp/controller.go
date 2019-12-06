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
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/units"
	"golang.org/x/net/context"
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
	}
)

var getSharedDatastores = getSharedDatastoresInPodVMK8SCluster

type controller struct {
	manager *common.Manager
}

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *config.Config) error {
	klog.Infof("Initializing WCP CSI controller")
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
	return nil
}

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	klog.V(4).Infof("CreateVolume: called with args %+v", *req)
	err := validateWCPCreateVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for CreateVolume Request: %+v has failed. Error: %+v", *req, err)
		klog.Error(msg)
		return nil, err
	}

	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	var storagePolicyID string
	var fsType string
	var affineToHost string
	// Support case insensitive parameters
	for paramName := range req.Parameters {
		param := strings.ToLower(paramName)
		if param == common.AttributeStoragePolicyID {
			storagePolicyID = req.Parameters[paramName]
		} else if param == common.AttributeFsType {
			fsType = req.Parameters[common.AttributeFsType]
		} else if param == common.AttributeAffineToHost {
			affineToHost = req.Parameters[common.AttributeAffineToHost]
		}
	}

	var createVolumeSpec = common.CreateVolumeSpec{
		CapacityMB:      volSizeMB,
		Name:            req.Name,
		StoragePolicyID: storagePolicyID,
		ScParams:        &common.StorageClassParams{},
		AffineToHost:    affineToHost,
		VolumeType:      common.BlockVolumeType,
	}
	// Get shared datastores for the Kubernetes cluster
	sharedDatastores, err := getSharedDatastores(ctx, c)
	volumeID, err := common.CreateBlockVolumeUtil(ctx, cnstypes.CnsClusterFlavorWorkload, c.manager, &createVolumeSpec, sharedDatastores)
	if err != nil {
		msg := fmt.Sprintf("Failed to create volume. Error: %+v", err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeBlockVolume
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

// CreateVolume is deleting CNS Volume specified in DeleteVolumeRequest
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteVolume: called with args: %+v", *req)
	var err error
	err = validateWCPDeleteVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for DeleteVolume Request: %+v has failed. Error: %+v", *req, err)
		klog.Error(msg)
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
	err := validateWCPControllerPublishVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for PublishVolume Request: %+v has failed. Error: %v", *req, err)
		klog.Errorf(msg)
		return nil, err
	}

	vmuuid, err := getVMUUIDFromPodListenerService(req.VolumeId, req.NodeId)
	if err != nil {
		msg := fmt.Sprintf("Failed to get the pod vmuuid annotation from the pod listener service when processing attach for volumeID: %s on node: %s. Error: %+v", req.VolumeId, req.NodeId, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	vcdcMap, err := getDatacenterFromConfig(c.manager.CnsConfig)
	if err != nil {
		msg := fmt.Sprintf("Failed to get datacenter from config with error: %+v", err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	var vCenterHost, dcMorefValue string
	for key, value := range vcdcMap {
		vCenterHost = key
		dcMorefValue = value
	}
	vc, err := c.manager.VcenterManager.GetVirtualCenter(vCenterHost)
	if err != nil {
		msg := fmt.Sprintf("Cannot get virtual center %s from virtualcentermanager while attaching disk with error %+v",
			vc.Config.Host, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Connect to VC
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = vc.Connect(ctx)
	if err != nil {
		msg := fmt.Sprintf("Failed to connect to Virtual Center: %s", vc.Config.Host)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	podVM, err := getVMByInstanceUUIDInDatacenter(ctx, vc, dcMorefValue, vmuuid)
	if err != nil {
		msg := fmt.Sprintf("Failed to the PodVM Moref from the PodVM UUID: %s in datacenter: %s with err: %+v", vmuuid, dcMorefValue, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	// Attach the volume to the node
	diskUUID, err := common.AttachVolumeUtil(ctx, c.manager, podVM, req.VolumeId)
	if err != nil {
		msg := fmt.Sprintf("Failed to attach volume with volumeID: %s. Error: %+v", req.VolumeId, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	publishInfo := make(map[string]string, 0)
	publishInfo[common.AttributeDiskType] = common.DiskTypeBlockVolume
	publishInfo[common.AttributeFirstClassDiskUUID] = common.FormatDiskUUID(diskUUID)
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
	err := validateWCPControllerUnpublishVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for UnpublishVolume Request: %+v has failed. Error: %v", *req, err)
		klog.Error(msg)
		return nil, err
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
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

// ControllerExpandVolume expands a volume.
func (c *controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (
	*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).Infof("ControllerExpandVolume: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

// GetSharedDatastoresInPodVMK8SCluster gets the shared datastores for WCP PodVM cluster
func getSharedDatastoresInPodVMK8SCluster(ctx context.Context, c *controller) ([]*cnsvsphere.DatastoreInfo, error) {
	vc, err := common.GetVCenter(ctx, c.manager)
	if err != nil {
		klog.Errorf("Failed to get vCenter from Manager, err=%+v", err)
		return nil, err
	}
	hosts, err := vc.GetHostsByCluster(ctx, c.manager.CnsConfig.Global.ClusterID)
	if err != nil {
		klog.Errorf("Failed to get hosts from VC with err %+v", err)
		return nil, err
	}
	if len(hosts) == 0 {
		errMsg := fmt.Sprintf("Empty List of hosts returned from VC")
		klog.Errorf(errMsg)
		return make([]*cnsvsphere.DatastoreInfo, 0), fmt.Errorf(errMsg)
	}
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	for _, host := range hosts {
		klog.V(4).Infof("Getting accessible datastores for node %s", host.InventoryPath)
		accessibleDatastores, err := host.GetAllAccessibleDatastores(ctx)
		if err != nil {
			return nil, err
		}
		if len(sharedDatastores) == 0 {
			sharedDatastores = accessibleDatastores
		} else {
			var sharedAccessibleDatastores []*cnsvsphere.DatastoreInfo
			// Check if sharedDatastores is found in accessibleDatastores
			for _, sharedDs := range sharedDatastores {
				for _, accessibleDs := range accessibleDatastores {
					// Intersection is performed based on the datastoreUrl as this uniquely identifies the datastore.
					if sharedDs.Info.Url == accessibleDs.Info.Url {
						sharedAccessibleDatastores = append(sharedAccessibleDatastores, sharedDs)
						break
					}
				}
			}
			sharedDatastores = sharedAccessibleDatastores
		}
		if len(sharedDatastores) == 0 {
			return nil, fmt.Errorf("No shared datastores found in the Kubernetes cluster for host: %+v", host)
		}
	}
	klog.V(3).Infof("The list of shared datastores: %+v", sharedDatastores)
	return sharedDatastores, nil
}
