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
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
)

// validateVanillaDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for Vanilla CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateVanillaDeleteVolumeRequest(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	return common.ValidateDeleteVolumeRequest(ctx, req)

}

// validateControllerPublishVolumeRequest is the helper function to validate
// ControllerPublishVolumeRequest. Function returns error if validation fails
// otherwise returns nil.
func validateVanillaControllerPublishVolumeRequest(ctx context.Context,
	req *csi.ControllerPublishVolumeRequest) error {
	return common.ValidateControllerPublishVolumeRequest(ctx, req)
}

// validateControllerUnpublishVolumeRequest is the helper function to validate
// ControllerUnpublishVolumeRequest. Function returns error if validation fails
// otherwise returns nil.
func validateVanillaControllerUnpublishVolumeRequest(ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest) error {
	return common.ValidateControllerUnpublishVolumeRequest(ctx, req)
}

// validateVanillaControllerExpandVolumeRequest is the helper function to
// validate ExpandVolumeRequest for Vanilla CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateVanillaControllerExpandVolumeRequest(ctx context.Context,
	req *csi.ControllerExpandVolumeRequest, isOnlineExpansionEnabled, isOnlineExpansionSupported bool) error {
	log := logger.GetLogger(ctx)
	if err := common.ValidateControllerExpandVolumeRequest(ctx, req); err != nil {
		return err
	}

	// Check online extend FSS and vCenter support.
	if isOnlineExpansionEnabled && isOnlineExpansionSupported {
		return nil
	}

	// Check if it is an online expansion scenario and raise error.
	nodeManager := node.GetManager(ctx)
	nodes, err := nodeManager.GetAllNodes(ctx)
	if err != nil {
		msg := fmt.Sprintf("failed to find VirtualMachines for all registered nodes. Error: %v", err)
		log.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	return common.IsOnlineExpansion(ctx, req.GetVolumeId(), nodes)
}

// validateVanillaCreateSnapshotRequestRequest is the helper function to
// validate CreateSnapshotRequest for Vanilla CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateVanillaCreateSnapshotRequestRequest(ctx context.Context, req *csi.CreateSnapshotRequest) error {
	log := logger.GetLogger(ctx)
	volumeID := req.GetSourceVolumeId()
	if len(volumeID) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument,
			"CreateSnapshot Source Volume ID must be provided")
	}

	if len(req.Name) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument,
			"Snapshot name must be provided")
	}
	return nil
}

func validateVanillaListSnapshotRequest(ctx context.Context, req *csi.ListSnapshotsRequest) error {
	log := logger.GetLogger(ctx)
	maxEntries := req.MaxEntries
	if maxEntries < 0 {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"ListSnapshots MaxEntries: %d cannot be negative", maxEntries)
	}
	// validate the starting token by verifying that it can be converted to a int
	if req.StartingToken != "" {
		_, err := strconv.Atoi(req.StartingToken)
		if err != nil {
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"ListSnapshots StartingToken: %s cannot be parsed", req.StartingToken)
		}
	}
	// validate snapshot-id conforms to vSphere CSI driver format if specified.
	if req.SnapshotId != "" {
		// check for the delimiter "+" in the snapshot-id.
		check := strings.Contains(req.SnapshotId, common.VSphereCSISnapshotIdDelimiter)
		if !check {
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"ListSnapshots SnapshotId: %s is incorrectly formatted for vSphere CSI driver",
				req.StartingToken)
		}
	}
	return nil
}

func convertCnsVolumeType(ctx context.Context, cnsVolumeType string) string {
	volumeType := prometheus.PrometheusUnknownVolumeType
	if cnsVolumeType == common.BlockVolumeType {
		volumeType = prometheus.PrometheusBlockVolumeType
	} else if cnsVolumeType == common.FileVolumeType {
		volumeType = prometheus.PrometheusFileVolumeType
	}
	return volumeType
}

func getBlockVolumeIDToNodeUUIDMap(ctx context.Context, c *controller,
	allnodeVMs []*vsphere.VirtualMachine) (map[string]string, error) {
	var vCenters []*vsphere.VirtualCenter
	var err error

	log := logger.GetLogger(ctx)
	log.Debugf("getBlockVolumeIDToNodeUUIDMap called for Node VMs: %+v", allnodeVMs)
	volumeIDNodeUUIDMap := make(map[string]string)
	// Get VirtualCenter object(s)
	// For multi-VC configuration, create map for volumes in all vCenters
	if multivCenterCSITopologyEnabled {
		vCenters, err = common.GetVCenters(ctx, c.managers)
		if err != nil {
			log.Errorf("GetVcenters error %v", err)
			return nil, fmt.Errorf("failed to get vCenters from Managers, err: %v", err)
		}
	} else {
		vc, err := common.GetVCenter(ctx, c.manager)
		if err != nil {
			log.Errorf("GetVcenter error %v", err)
			return nil, fmt.Errorf("failed to get vCenter from Manager, err: %v", err)
		}
		vCenters = append(vCenters, vc)
	}

	vmRefsPervCenter := make(map[string][]types.ManagedObjectReference)
	vmMoListPervCenter := make(map[string][]mo.VirtualMachine)
	properties := []string{"runtime.host", "config.hardware", "config.uuid"}

	for _, nodeVM := range allnodeVMs {
		vmRefsPervCenter[nodeVM.VirtualCenterHost] = append(vmRefsPervCenter[nodeVM.VirtualCenterHost], nodeVM.Reference())
	}
	log.Debugf("vmRefsPervCenter: %+v to collect properties", vmRefsPervCenter)
	for _, vc := range vCenters {
		pc := property.DefaultCollector(vc.Client.Client)
		// Obtain host MoID and virtual disk ID
		var vmMoList []mo.VirtualMachine
		err = pc.Retrieve(ctx, vmRefsPervCenter[vc.Config.Host], properties, &vmMoList)
		if err != nil {
			log.Errorf("failed to get VM managed objects from VM objects, err: %v", err)
			return volumeIDNodeUUIDMap, err
		}
		vmMoListPervCenter[vc.Config.Host] = vmMoList
		for _, vmMo := range vmMoListPervCenter[vc.Config.Host] {
			log.Debugf("vCenter: %v, vmMo.Config.Uuid: %v, vmMo.Runtime.Host: %v",
				vc.Config.Host, vmMo.Config.Uuid, vmMo.Runtime.Host)
		}
	}

	for _, vc := range vCenters {
		// Iterate through all the VMs and build the vmMoIDToHostUUID map
		// and the volumeID to VMMoiD map
		for _, info := range vmMoListPervCenter[vc.Config.Host] {
			devices := info.Config.Hardware.Device
			vmDevices := object.VirtualDeviceList(devices)
			for _, device := range vmDevices {
				if vmDevices.TypeName(device) == "VirtualDisk" {
					if virtualDisk, ok := device.(*types.VirtualDisk); ok {
						if virtualDisk.VDiskId != nil {
							volumeIDNodeUUIDMap[virtualDisk.VDiskId.Id] = info.Config.Uuid
						}
					}
				}
			}
		}
	}
	log.Debugf("Volume ID to Node UUID Map: %+v", volumeIDNodeUUIDMap)
	return volumeIDNodeUUIDMap, nil
}

// getVCenterAndVolumeManagerForVolumeID returns vCenterHost & volume manager for the given volumeId.
// If multi-vcenter-csi-topology feature is disabled legacy volume manager is returned
// from `controller.manager.VolumeManager` & vCenter from `controller.manager.VCenterConfig.Host`.
// If multi-vcenter-csi-topology feature is enabled but volumeInfoService is not instantiated volume manager
// is returned from controller.managers.VolumeManagers for `controller.managers.CnsConfig.Global.VCenterIP`
// And vCenter from controller.managers.VCenterConfigs for `controller.managers.CnsConfig.Global.VCenterIP`.
// If multi-vcenter-csi-topology feature is enabled and volumeInfoService is instantiated volume manager is returned for
// the vCenter IP mapped to the requested VolumeID. vCenter for the requested volume is obtained from volumeInfoService.
func getVCenterAndVolumeManagerForVolumeID(ctx context.Context, controller *controller, volumeId string,
	volumeInfoService cnsvolumeinfo.VolumeInfoService) (string, cnsvolume.Manager, error) {
	log := logger.GetLogger(ctx)
	var (
		volumeManager      cnsvolume.Manager
		volumeManagerfound bool
		vCenter            string
		err                error
	)
	if multivCenterCSITopologyEnabled {
		if len(controller.managers.VcenterConfigs) > 1 {
			// Multi vCenter Deployment
			vCenter, err = volumeInfoService.GetvCenterForVolumeID(ctx, volumeId)
			if err != nil {
				return "", nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to get vCenter for the volumeID: %q with err=%+v", volumeId, err)
			}
			if volumeManager, volumeManagerfound = controller.managers.VolumeManagers[vCenter]; !volumeManagerfound {
				return vCenter, nil, logger.LogNewErrorCodef(log, codes.Internal,
					"could not get volume manager for the vCenter: %q", vCenter)
			}
		} else {
			// Single vCenter Deployment
			vCenterConfig, vCenterFound := controller.managers.VcenterConfigs[controller.managers.CnsConfig.Global.VCenterIP]
			if !vCenterFound {
				return "", nil, logger.LogNewErrorCodef(log, codes.Internal,
					"could not get vCenter config for the vCenter: %q",
					controller.managers.CnsConfig.Global.VCenterIP)
			}
			vCenter = vCenterConfig.Host
			if volumeManager, volumeManagerfound =
				controller.managers.VolumeManagers[vCenter]; !volumeManagerfound {
				return "", nil, logger.LogNewErrorCodef(log, codes.Internal,
					"could not get volume manager for the vCenter: %q", vCenter)
			}
		}
	} else {
		vCenter = controller.manager.VcenterConfig.Host
		volumeManager = controller.manager.VolumeManager
	}
	return vCenter, volumeManager, nil
}

// getVCenterManagerForVCenter returns vCenter manager for the given volumeId.
// If multi-vcenter-csi-topology feature is disabled, legacy vCenter manager is returned
// from `controller.manager.VCenterManager`.
// If multi-vcenter-csi-topology feature is enabled, vCenter manager is returned from
// `controller.managers.VCenterManager`.
func getVCenterManagerForVCenter(ctx context.Context, controller *controller) vsphere.VirtualCenterManager {
	var vCenterManager vsphere.VirtualCenterManager
	if multivCenterCSITopologyEnabled {
		vCenterManager = controller.managers.VcenterManager
	} else {
		vCenterManager = controller.manager.VcenterManager
	}
	return vCenterManager
}

// GetVolumeManagerFromVCHost retreives the volume manager associated with
// vCenterHost under managers. Error out if the vCenterHost does not exist.
func GetVolumeManagerFromVCHost(ctx context.Context, managers *common.Managers, vCenterHost string) (
	cnsvolume.Manager, error) {
	log := logger.GetLogger(ctx)
	volumeMgr, exists := managers.VolumeManagers[vCenterHost]
	if !exists {
		return nil, logger.LogNewErrorf(log, "failed to find vCenter %q under volume managers.", vCenterHost)
	}
	return volumeMgr, nil
}
