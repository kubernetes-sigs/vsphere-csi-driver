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

package wcpguest

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}
)

const (
	// prefix of the PVC Name in the supervisor cluster
	// TODO: This should be the Guest Cluster Unique ID
	pvcPrefix = "pvcsc-"
)

type controller struct {
	supervisorClient    clientset.Interface
	supervisorNamespace string
}

// New creates a CNS controller
func New() csitypes.CnsController {
	return &controller{}
}

// Init is initializing controller struct
func (c *controller) Init(config *config.Config) error {
	// connect to the CSI controller in supervisor cluster
	klog.V(1).Infof("Initializing WCPGC CSI controller")
	var err error
	c.supervisorNamespace = config.GC.Namespace
	c.supervisorClient, err = k8s.NewSupervisorClient(config.GC.Endpoint, config.GC.Port, config.GC.Certificate, config.GC.Token)
	if err != nil {
		return err
	}
	return nil
}

// CreateVolume is creating CNS Volume using volume request specified
// in CreateVolumeRequest
func (c *controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (
	*csi.CreateVolumeResponse, error) {
	klog.V(4).Infof("CreateVolume: called with args %+v", *req)
	err := validateGuestClusterCreateVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for CreateVolume Request: %+v has failed. Error: %+v", *req, err)
		klog.Error(msg)
		return nil, err
	}
	// Get PVC name and disk size for the supervisor cluster
	// We use default prefix 'pvc-' for pvc created in the guest cluster, it is mandatory.
	supervisorPVCName := pvcPrefix + req.Name[4:]

	// Volume Size - Default is 10 GiB
	volSizeBytes := int64(common.DefaultGbDiskSize * common.GbInBytes)
	if req.GetCapacityRange() != nil && req.GetCapacityRange().RequiredBytes != 0 {
		volSizeBytes = int64(req.GetCapacityRange().GetRequiredBytes())
	}
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// Get supervisorStorageClass and accessMode
	var supervisorStorageClass string
	for param := range req.Parameters {
		paramName := strings.ToLower(param)
		if paramName == common.AttributeSupervisorStorageClass {
			supervisorStorageClass = req.Parameters[param]
		}
	}
	var accessMode csi.VolumeCapability_AccessMode_Mode
	accessMode = req.GetVolumeCapabilities()[0].GetAccessMode().GetMode()
	pvc, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(supervisorPVCName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			diskSize := strconv.FormatInt(volSizeMB, 10) + "Mi"
			claim := getPersistentVolumeClaimSpecWithStorageClass(supervisorPVCName, c.supervisorNamespace, diskSize, supervisorStorageClass, getAccessMode(accessMode))
			klog.V(4).Infof("PVC claim spec is %+v", spew.Sdump(claim))
			pvc, err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Create(claim)
			if err != nil {
				msg := fmt.Sprintf("Failed to create pvc with name: %s on namespace: %s in supervisorCluster. Error: %+v", supervisorPVCName, c.supervisorNamespace, err)
				klog.Error(msg)
				return nil, status.Errorf(codes.Internal, msg)
			}
		} else {
			msg := fmt.Sprintf("Failed to get pvc with name: %s on namespace: %s from supervisorCluster. Error: %+v", supervisorPVCName, c.supervisorNamespace, err)
			klog.Error(msg)
			return nil, status.Errorf(codes.Internal, msg)
		}
	}
	isBound, err := isPVCInSupervisorClusterBound(c.supervisorClient, pvc, time.Duration(getProvisionTimeoutInMin())*time.Minute)
	if !isBound {
		msg := fmt.Sprintf("Failed to create volume on namespace: %s  in supervisor cluster. Error: %+v", c.supervisorNamespace, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	attributes := make(map[string]string)
	attributes[common.AttributeDiskType] = common.DiskTypeString
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      supervisorPVCName,
			CapacityBytes: int64(volSizeMB * common.MbInBytes),
			VolumeContext: attributes,
		},
	}
	return resp, nil
}

// DeleteVolume is deleting CNS Volume specified in DeleteVolumeRequest
func (c *controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (
	*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteVolume: called with args: %+v", *req)
	var err error
	err = validateGuestClusterDeleteVolumeRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Validation for Delete Volume Request: %+v has failed. Error: %+v", *req, err)
		klog.Error(msg)
		return nil, err
	}
	err = c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Delete(req.VolumeId, nil)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).Infof("PVC: %q not found in the Supervisor cluster. Assuming this volume to be deleted.", req.VolumeId)
			return &csi.DeleteVolumeResponse{}, nil
		}
		msg := fmt.Sprintf("DeleteVolume Request: %+v has failed. Error: %+v", *req, err)
		klog.Error(msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	klog.V(2).Infof("DeleteVolume: Volume deleted successfully. VolumeID: %q", req.VolumeId)
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume attaches a volume to the Node VM.
// volume id and node name is retrieved from ControllerPublishVolumeRequest
func (c *controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*csi.ControllerPublishVolumeResponse, error) {

	klog.V(4).Infof("ControllerPublishVolume: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerUnpublishVolume detaches a volume from the Node VM.
// volume id and node name is retrieved from ControllerUnpublishVolumeRequest
func (c *controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (
	*csi.ControllerUnpublishVolumeResponse, error) {

	klog.V(4).Infof("ControllerUnpublishVolume: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

// ValidateVolumeCapabilities returns the capabilities of the volume.
func (c *controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (
	*csi.ValidateVolumeCapabilitiesResponse, error) {

	klog.V(4).Infof("ValidateVolumeCapabilities: called with args %+v", *req)
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
