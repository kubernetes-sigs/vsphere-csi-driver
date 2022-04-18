/*
Copyright 2018 The Kubernetes Authors.

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

package service

import (
	"os"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/osutils"
)

const (
	// vCenter 8.0 supports attaching max 255 volumes to Node
	// Previous vSphere releases supports attaching a max of 59 volumes to Node VM.
	// Deployment YAML file for Node DaemonSet has ENV MAX_VOLUMES_PER_NODE set to 59 for vsphere-csi-node container
	// If Customer is using vSphere 8.0, they are allowed to set MAX_VOLUMES_PER_NODE to 255
	maxAllowedBlockVolumesPerNode = 255
)

var topologyService commoncotypes.NodeTopologyService

func (driver *vsphereCSIDriver) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeStageVolume: called with args %+v", *req)

	volumeID := req.GetVolumeId()
	volCap := req.GetVolumeCapability()
	// Check for block volume or file share.
	if common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{volCap}) {
		log.Infof("NodeStageVolume: Volume %q detected as a file share volume. Ignoring staging for file volumes.",
			volumeID)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	if volCap == nil {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"volume capability not provided")
	}
	caps := []*csi.VolumeCapability{volCap}
	if err := common.IsValidVolumeCapabilities(ctx, caps); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"volume capability not supported. Err: %+v", err)
	}

	var err error
	params := osutils.NodeStageParams{
		VolID: volumeID,
		// Retrieve accessmode - RO/RW.
		Ro: common.IsVolumeReadOnly(req.GetVolumeCapability()),
	}
	// TODO: Verify if volume exists and return a NotFound error in negative
	// scenario.

	// Check if this is a MountVolume or Raw BlockVolume.
	if _, ok := volCap.GetAccessType().(*csi.VolumeCapability_Mount); ok {
		// Mount Volume.
		// Extract mount volume details.
		log.Debug("NodeStageVolume: Volume detected as a mount volume")
		params.FsType, params.MntFlags, err = driver.osUtils.EnsureMountVol(ctx, log, volCap)
		if err != nil {
			return nil, err
		}

		// Check that staging path is created by CO and is a directory.
		params.StagingTarget = req.GetStagingTargetPath()
		if _, err = driver.osUtils.VerifyTargetDir(ctx, params.StagingTarget, true); err != nil {
			return nil, err
		}
	}
	return driver.osUtils.NodeStageBlockVolume(ctx, req, params)
}

func (driver *vsphereCSIDriver) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeUnstageVolume: called with args %+v", *req)

	stagingTarget := req.GetStagingTargetPath()

	// Figure out if the target path is present in mounts or not - Unstage is
	// not required for file volumes.
	targetFound, err := driver.osUtils.IsTargetInMounts(ctx, stagingTarget)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not retrieve existing mount points: %v", err)
	}

	if !targetFound {
		log.Infof("NodeUnstageVolume: Target path %q is not mounted. Skipping unstage.", stagingTarget)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	volID := req.GetVolumeId()
	dirExists, err := driver.osUtils.VerifyTargetDir(ctx, stagingTarget, false)
	if err != nil {
		return nil, err
	}
	// This will take care of idempotent requests.
	if !dirExists {
		log.Infof("NodeUnstageVolume: Target path %q does not exist. Assuming unstage is complete.", stagingTarget)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	if err := driver.osUtils.CleanupStagePath(ctx, stagingTarget, volID); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"UnStage failed: %v\nUnStage arguments: %s\n", err, stagingTarget)
	}

	log.Infof("NodeUnstageVolume successful for target %q for volume %q", stagingTarget, volID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (driver *vsphereCSIDriver) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodePublishVolume: called with args %+v", *req)
	var err error
	params := osutils.NodePublishParams{
		VolID:  req.GetVolumeId(),
		Target: req.GetTargetPath(),
		Ro:     req.GetReadonly(),
	}
	// TODO: Verify if volume exists and return a NotFound error in negative
	// scenario.

	params.StagingTarget = req.GetStagingTargetPath()
	if params.StagingTarget == "" {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"staging target path %q not set", params.StagingTarget)
	}
	if params.Target == "" {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"target path %q not set", params.Target)
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"volume capability not provided")
	}
	caps := []*csi.VolumeCapability{volCap}
	if err := common.IsValidVolumeCapabilities(ctx, caps); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"volume capability not supported. Err: %+v", err)
	}

	// Check if this is a MountVolume or BlockVolume.
	if !common.IsFileVolumeRequest(ctx, caps) {
		var dev *osutils.Device
		err = driver.osUtils.VerifyVolumeAttachedAndFillParams(ctx, req.GetPublishContext(), &params, &dev)
		if err != nil {
			log.Errorf("error filling all params. error: %v", err)
			return nil, err
		}

		// check for Block vs Mount.
		if _, ok := volCap.GetAccessType().(*csi.VolumeCapability_Block); ok {
			// bind mount device to target.
			return driver.osUtils.PublishBlockVol(ctx, req, dev, params)
		}
		// Volume must be a mount volume.
		return driver.osUtils.PublishMountVol(ctx, req, dev, params)
	}
	// Volume must be a file share.
	return driver.osUtils.PublishFileVol(ctx, req, params)
}

func (driver *vsphereCSIDriver) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (
	*csi.NodeUnpublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeUnpublishVolume: called with args %+v", *req)

	volID := req.GetVolumeId()
	target := req.GetTargetPath()

	if target == "" {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"target path %q not set", target)
	}

	if err := driver.osUtils.CleanupPublishPath(ctx, target, volID); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"Unmount failed: %v\nUnmounting arguments: %s\n", err, target)
	}

	log.Infof("NodeUnpublishVolume successful for volume %q", volID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (driver *vsphereCSIDriver) NodeGetVolumeStats(
	ctx context.Context,
	req *csi.NodeGetVolumeStatsRequest) (
	*csi.NodeGetVolumeStatsResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeGetVolumeStats: called with args %+v", *req)

	var err error
	targetPath := req.GetVolumePath()
	if targetPath == "" {
		return nil, logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"received empty targetpath %q", targetPath)
	}

	volMetrics, err := driver.osUtils.GetMetrics(ctx, targetPath)
	if err != nil {
		return nil, logger.LogNewErrorCode(log, codes.Internal, err.Error())
	}

	available, ok := (*(volMetrics.Available)).AsInt64()
	if !ok {
		log.Warn("failed to fetch available bytes")
	}
	capacity, ok := (*(volMetrics.Capacity)).AsInt64()
	if !ok {
		return nil, logger.LogNewErrorCode(log, codes.Unknown, "failed to fetch capacity bytes")
	}
	used, ok := (*(volMetrics.Used)).AsInt64()
	if !ok {
		log.Warn("failed to fetch used bytes")
	}
	inodes, ok := (*(volMetrics.Inodes)).AsInt64()
	if !ok {
		return nil, logger.LogNewErrorCode(log, codes.Unknown, "failed to fetch total number of inodes")
	}
	inodesFree, ok := (*(volMetrics.InodesFree)).AsInt64()
	if !ok {
		log.Warn("failed to fetch free inodes")
	}
	inodesUsed, ok := (*(volMetrics.InodesUsed)).AsInt64()
	if !ok {
		log.Warn("failed to fetch used inodes")
	}
	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: available,
				Total:     capacity,
				Used:      used,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: inodesFree,
				Total:     inodes,
				Used:      inodesUsed,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

func (driver *vsphereCSIDriver) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest) (
	*csi.NodeGetCapabilitiesResponse, error) {

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

// NodeGetInfo RPC returns the NodeGetInfoResponse with mandatory fields
// `NodeId` and `AccessibleTopology`. However, for sending `MaxVolumesPerNode`
// in the response, it is not straight forward since vSphere CSI driver
// supports both block and file volume. For block volume, max volumes to be
// attached is deterministic by inspecting SCSI controllers of the VM, but for
// file volume, this is not deterministic. We can not set this limit on
// MaxVolumesPerNode, since single driver is used for both block and file
// volumes.
func (driver *vsphereCSIDriver) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest) (
	*csi.NodeGetInfoResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeGetInfo: called with args %+v", *req)

	driver.osUtils.ShouldContinue(ctx)

	var nodeInfoResponse *csi.NodeGetInfoResponse

	var nodeID string
	var err error
	var clusterFlavor cnstypes.CnsClusterFlavor
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return nil, logger.LogNewErrorCode(log, codes.Internal,
			"ENV NODE_NAME is not set")
	}
	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.UseCSINodeId) {
		// Get VM UUID
		nodeID, err = driver.osUtils.GetSystemUUID(ctx)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get system uuid for node VM with error: %v", err)
		}
	} else {
		nodeID = nodeName
	}

	var maxVolumesPerNode int64
	if v := os.Getenv("MAX_VOLUMES_PER_NODE"); v != "" {
		if value, err := strconv.ParseInt(v, 10, 64); err == nil {
			if value < 0 {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"NodeGetInfo: MAX_VOLUMES_PER_NODE set in env variable %v is less than 0", v)
			} else if value > maxAllowedBlockVolumesPerNode {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"NodeGetInfo: MAX_VOLUMES_PER_NODE set in env variable %v is more than %v",
					v, maxAllowedBlockVolumesPerNode)
			} else {
				maxVolumesPerNode = value
				log.Infof("NodeGetInfo: MAX_VOLUMES_PER_NODE is set to %v", maxVolumesPerNode)
			}
		} else {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"NodeGetInfo: MAX_VOLUMES_PER_NODE set in env variable %v is invalid", v)
		}
	}

	var (
		accessibleTopology map[string]string
	)

	clusterFlavor, err = cnsconfig.GetClusterFlavor(ctx)
	if err != nil {
		return nil, err
	}

	if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA) {
			nodeInfoResponse = &csi.NodeGetInfoResponse{
				NodeId:             nodeID,
				MaxVolumesPerNode:  maxVolumesPerNode,
				AccessibleTopology: &csi.Topology{},
			}
			log.Infof("NodeGetInfo response: %v", nodeInfoResponse)
			return nodeInfoResponse, nil
		}

		// Initialize volume topology service if tkgs-ha is enabled in guest cluster.
		if err = initVolumeTopologyService(ctx); err != nil {
			return nil, err
		}
		// Fetch topology labels for given node.
		nodeInfo := commoncotypes.NodeInfo{
			NodeName: nodeName,
			NodeID:   nodeID,
		}
		accessibleTopology, err = topologyService.GetNodeTopologyLabels(ctx, &nodeInfo)
	} else if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.ImprovedVolumeTopology) {
			// Initialize volume topology service.
			if err = initVolumeTopologyService(ctx); err != nil {
				return nil, err
			}
			// Fetch topology labels for given node.
			nodeInfo := commoncotypes.NodeInfo{
				NodeName: nodeName,
				NodeID:   nodeID,
			}
			accessibleTopology, err = topologyService.GetNodeTopologyLabels(ctx, &nodeInfo)
		} else {
			// If ImprovedVolumeTopology is not enabled, use the VC credentials to
			// fetch node topology information.
			var cfg *cnsconfig.Config
			cfgPath = os.Getenv(cnsconfig.EnvVSphereCSIConfig)
			if cfgPath == "" {
				cfgPath = cnsconfig.DefaultCloudConfigPath
			}
			cfg, err = cnsconfig.GetCnsconfig(ctx, cfgPath)
			if err != nil {
				if os.IsNotExist(err) {
					log.Infof("Config file not provided to node daemonset. Assuming non-topology aware cluster.")
					nodeInfoResponse = &csi.NodeGetInfoResponse{
						NodeId:            nodeID,
						MaxVolumesPerNode: maxVolumesPerNode,
					}
					log.Infof("NodeGetInfo response: %v", nodeInfoResponse)
					return nodeInfoResponse, nil
				}
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"failed to read CNS config. Error: %v", err)
			}
			// Fetch topology labels using VC TagManager.
			accessibleTopology, err = driver.fetchTopologyLabelsUsingVCCreds(ctx, nodeID, cfg)
		}
	}

	if err != nil {
		return nil, err
	}

	topology := &csi.Topology{}
	if len(accessibleTopology) > 0 {
		topology.Segments = accessibleTopology
	}
	nodeInfoResponse = &csi.NodeGetInfoResponse{
		NodeId:             nodeID,
		MaxVolumesPerNode:  maxVolumesPerNode,
		AccessibleTopology: topology,
	}
	log.Infof("NodeGetInfo response: %v", nodeInfoResponse)
	return nodeInfoResponse, nil
}

// initVolumeTopologyService is a helper method to initialize
// TopologyService in node.
func initVolumeTopologyService(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	// This check prevents unnecessary RLocks on the volumeTopology instance.
	if topologyService != nil {
		return nil
	}
	// Initialize the TopologyService if not done already.
	var err error
	topologyService, err = commonco.ContainerOrchestratorUtility.InitTopologyServiceInNode(ctx)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"failed to init topology service. Error: %+v", err)
	}
	return nil
}

// fetchTopologyLabelsUsingVCCreds retrieves topology information of the nodes
// using VC credentials mounted on the nodes. This approach will be deprecated
// soon.
func (driver *vsphereCSIDriver) fetchTopologyLabelsUsingVCCreds(
	ctx context.Context, nodeID string, cfg *cnsconfig.Config) (
	map[string]string, error) {
	log := logger.GetLogger(ctx)

	// If zone or region are empty, return.
	if cfg.Labels.Zone == "" || cfg.Labels.Region == "" {
		return nil, nil
	}

	log.Infof("Config file provided to node daemonset contains zone and region info. " +
		"Assuming topology aware cluster.")
	vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get VirtualCenterConfig from cns config. err: %v", err)
	}
	vcManager := cnsvsphere.GetVirtualCenterManager(ctx)
	vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to register vcenter with virtualCenterManager. err: %v", err)
	}
	defer func() {
		if vcManager != nil {
			err = vcManager.UnregisterAllVirtualCenters(ctx)
			if err != nil {
				log.Errorf("UnregisterAllVirtualCenters failed. err: %v", err)
			}
		}
	}()

	// Connect to vCenter.
	err = vcenter.Connect(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to connect to vcenter host: %s. err: %v", vcenter.Config.Host, err)
	}
	// Get VM UUID.
	uuid, err := driver.osUtils.GetSystemUUID(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get system uuid for node VM. err: %v", err)
	}
	log.Debugf("Successfully retrieved uuid:%s  from the node: %s", uuid, nodeID)
	nodeVM, err := cnsvsphere.GetVirtualMachineByUUID(ctx, uuid, false)
	if err != nil || nodeVM == nil {
		log.Errorf("failed to get nodeVM for uuid: %s. err: %+v", uuid, err)
		uuid, err = driver.osUtils.ConvertUUID(uuid)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"convertUUID failed with error: %v", err)
		}
		nodeVM, err = cnsvsphere.GetVirtualMachineByUUID(ctx, uuid, false)
		if err != nil || nodeVM == nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to get nodeVM for uuid: %s. err: %+v", uuid, err)
		}
	}
	// Get a tag manager instance.
	tagManager, err := cnsvsphere.GetTagManager(ctx, vcenter)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to create tagManager. err: %v", err)
	}
	defer func() {
		err := tagManager.Logout(ctx)
		if err != nil {
			log.Errorf("failed to logout tagManager. err: %v", err)
		}
	}()

	// Fetch zone and region for given node.
	zone, region, err := nodeVM.GetZoneRegion(ctx, cfg.Labels.Zone, cfg.Labels.Region, tagManager)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get accessibleTopology for vm: %v, err: %v", nodeVM.Reference(), err)
	}
	log.Debugf("zone: [%s], region: [%s], Node VM: [%s]", zone, region, nodeID)

	if zone != "" && region != "" {
		accessibleTopology := make(map[string]string)
		accessibleTopology[v1.LabelZoneRegion] = region
		accessibleTopology[v1.LabelZoneFailureDomain] = zone
		return accessibleTopology, nil
	}
	return nil, nil
}

func (driver *vsphereCSIDriver) NodeExpandVolume(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest) (
	*csi.NodeExpandVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeExpandVolume: called with args %+v", *req)

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument, "volume id must be provided")
	} else if req.GetCapacityRange() == nil {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument, "capacity range must be provided")
	} else if req.GetCapacityRange().GetRequiredBytes() < 0 || req.GetCapacityRange().GetLimitBytes() < 0 {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument, "capacity ranges values cannot be negative")
	}

	reqVolSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	reqVolSizeMB := int64(common.RoundUpSize(reqVolSizeBytes, common.MbInBytes))

	// TODO(xyang): In CSI spec 1.2, NodeExpandVolume will be
	// passing in a staging_target_path which is more precise
	// than volume_path. Use the new staging_target_path
	// instead of the volume_path when it is supported by Kubernetes.

	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"volume path must be provided to expand volume on node")
	}

	// Look up block device mounted to staging target path.
	dev, err := driver.osUtils.GetDevFromMount(ctx, volumePath)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error getting block device for volume: %q, err: %v",
			volumeID, err)
	} else if dev == nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"volume %q is not mounted at the path %s",
			volumeID, volumePath)
	}
	log.Debugf("NodeExpandVolume: staging target path %s, getDevFromMount %+v", volumePath, *dev)

	// Fetch the current block size.
	currentBlockSizeBytes, err := driver.osUtils.GetBlockSizeBytes(ctx, dev.RealDev)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error when getting size of block volume at path %s: %v", dev.RealDev, err)
	}
	// Check if a rescan is required.
	if currentBlockSizeBytes < reqVolSizeBytes {
		// If a device is expanded while it is attached to a VM, we need to
		// rescan the device on the guest OS in order to see the modified size
		// on the Guest OS.
		// Refer to https://kb.vmware.com/s/article/1006371
		err = driver.osUtils.RescanDevice(ctx, dev)
		if err != nil {
			return nil, logger.LogNewErrorCode(log, codes.Internal, err.Error())
		}
	}

	// Resize file system.
	if err = driver.osUtils.ResizeVolume(ctx, dev.RealDev, volumePath, reqVolSizeBytes); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error when resizing filesystem on volume %q on node: %v", volumeID, err)
	}
	log.Debugf("NodeExpandVolume: Resized filesystem with devicePath %s volumePath %s", dev.RealDev, volumePath)

	log.Infof("NodeExpandVolume: expanded volume successfully. devicePath %s volumePath %s size %d",
		dev.RealDev, volumePath, int64(units.FileSize(reqVolSizeMB*common.MbInBytes)))
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: int64(units.FileSize(reqVolSizeMB * common.MbInBytes)),
	}, nil
}
