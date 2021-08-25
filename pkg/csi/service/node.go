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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/akutz/gofsutil"
	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/units"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svol "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/fs"
	mount "k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco/types"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
)

const (
	devDiskID                     = "/dev/disk/by-id"
	blockPrefix                   = "wwn-0x"
	dmiDir                        = "/sys/class/dmi"
	maxAllowedBlockVolumesPerNode = 59
)

var topologyService commoncotypes.NodeTopologyService

type nodeStageParams struct {
	// volID is the identifier for the underlying volume.
	volID string
	// fsType is the file system type - ext3, ext4, nfs, nfs4.
	fsType string
	// Staging Target path is used to mount the volume to the node.
	stagingTarget string
	// Mount flags/options intended to be used while running the mount command.
	mntFlags []string
	// Read-only flag.
	ro bool
}

type nodePublishParams struct {
	// volID is the identifier for the underlying volume.
	volID string
	// Target path is used to bind-mount a staged volume to the pod.
	target string
	// Staging Target path is used to mount the volume to the node.
	stagingTarget string
	// diskID is the identifier for the disk.
	diskID string
	// volumePath represents the sym-linked block volume full path.
	volumePath string
	// device represents the actual path of the block volume.
	device string
	// Read-only flag.
	ro bool
}

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
	params := nodeStageParams{
		volID: volumeID,
		// Retrieve accessmode - RO/RW.
		ro: common.IsVolumeReadOnly(req.GetVolumeCapability()),
	}
	// TODO: Verify if volume exists and return a NotFound error in negative
	// scenario.

	// Check if this is a MountVolume or Raw BlockVolume.
	if _, ok := volCap.GetAccessType().(*csi.VolumeCapability_Mount); ok {
		// Mount Volume.
		// Extract mount volume details.
		log.Debug("NodeStageVolume: Volume detected as a mount volume")
		params.fsType, params.mntFlags, err = ensureMountVol(ctx, log, volCap)
		if err != nil {
			return nil, err
		}

		// Check that staging path is created by CO and is a directory.
		params.stagingTarget = req.GetStagingTargetPath()
		if _, err = verifyTargetDir(ctx, params.stagingTarget, true); err != nil {
			return nil, err
		}
	}
	return nodeStageBlockVolume(ctx, req, params)
}

func nodeStageBlockVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest,
	params nodeStageParams) (
	*csi.NodeStageVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Block Volume.
	pubCtx := req.GetPublishContext()
	diskID, err := getDiskID(pubCtx, log)
	if err != nil {
		return nil, err
	}
	log.Infof("nodeStageBlockVolume: Retrieved diskID as %q", diskID)

	// Verify if the volume is attached.
	log.Debugf("nodeStageBlockVolume: Checking if volume is attached to diskID: %v", diskID)
	volPath, err := verifyVolumeAttached(ctx, diskID)
	if err != nil {
		log.Errorf("Error checking if volume %q is attached. Parameters: %v", params.volID, params)
		return nil, err
	}
	log.Debugf("nodeStageBlockVolume: Disk %q attached at %q", diskID, volPath)

	// Check that block device looks good.
	dev, err := getDevice(volPath)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error getting block device for volume: %q. Parameters: %v err: %v",
			params.volID, params, err)

	}
	log.Debugf("nodeStageBlockVolume: getDevice %+v", *dev)

	// Check if this is a MountVolume or BlockVolume.
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		// Volume is a block volume, so skip the rest of the steps.
		log.Infof("nodeStageBlockVolume: Skipping staging for block volume ID %q", params.volID)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Mount Volume.
	// Fetch dev mounts to check if the device is already staged.
	log.Debugf("nodeStageBlockVolume: Fetching device mounts")
	mnts, err := gofsutil.GetDevMounts(ctx, dev.RealDev)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not reliably determine existing mount status. Parameters: %v err: %v", params, err)
	}

	if len(mnts) == 0 {
		// Device isn't mounted anywhere, stage the volume.
		// If access mode is read-only, we don't allow formatting.
		if params.ro {
			log.Debugf("nodeStageBlockVolume: Mounting %q at %q in read-only mode with mount flags %v",
				dev.FullPath, params.stagingTarget, params.mntFlags)
			params.mntFlags = append(params.mntFlags, "ro")
			err := gofsutil.Mount(ctx, dev.FullPath, params.stagingTarget, params.fsType, params.mntFlags...)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"error mounting volume. Parameters: %v err: %v", params, err)
			}
			log.Infof("nodeStageBlockVolume: Device mounted successfully at %q", params.stagingTarget)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		// Format and mount the device.
		log.Debugf("nodeStageBlockVolume: Format and mount the device %q at %q with mount flags %v",
			dev.FullPath, params.stagingTarget, params.mntFlags)
		err := gofsutil.FormatAndMount(ctx, dev.FullPath, params.stagingTarget, params.fsType, params.mntFlags...)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error in formating and mounting volume. Parameters: %v err: %v", params, err)
		}
	} else {
		// If Device is already mounted. Need to ensure that it is already.
		// mounted to the expected staging target, with correct rw/ro perms.
		log.Debugf("nodeStageBlockVolume: Device already mounted. Checking mount flags %v for correctness.",
			params.mntFlags)
		for _, m := range mnts {
			if m.Path == params.stagingTarget {
				rwo := "rw"
				if params.ro {
					rwo = "ro"
				}
				log.Debugf("nodeStageBlockVolume: Checking for mount options %v", m.Opts)
				if contains(m.Opts, rwo) {
					// TODO make sure that all the mount options match.
					log.Infof("nodeStageBlockVolume: Device already mounted at %q with mount option %q",
						params.stagingTarget, rwo)
					return &csi.NodeStageVolumeResponse{}, nil
				}
				return nil, logger.LogNewErrorCodef(log, codes.AlreadyExists,
					"access mode conflicts with existing mount at %q", params.stagingTarget)
			}
		}
		return nil, logger.LogNewErrorCode(log, codes.Internal,
			"device already in use and mounted elsewhere")
	}
	log.Infof("nodeStageBlockVolume: Device mounted successfully at %q", params.stagingTarget)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (driver *vsphereCSIDriver) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodeUnstageVolume: called with args %+v", *req)

	stagingTarget := req.GetStagingTargetPath()
	// Fetch all the mount points.
	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not retrieve existing mount points: %v", err)
	}
	log.Debugf("NodeUnstageVolume: node mounts %+v", mnts)
	// Figure out if the target path is present in mounts or not - Unstage is
	// not required for file volumes.
	targetFound := common.IsTargetInMounts(ctx, stagingTarget, mnts)
	if !targetFound {
		log.Infof("NodeUnstageVolume: Target path %q is not mounted. Skipping unstage.", stagingTarget)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	volID := req.GetVolumeId()
	dirExists, err := verifyTargetDir(ctx, stagingTarget, false)
	if err != nil {
		return nil, err
	}
	// This will take care of idempotent requests.
	if !dirExists {
		log.Infof("NodeUnstageVolume: Target path %q does not exist. Assuming unstage is complete.", stagingTarget)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Block volume.
	isMounted, err := isBlockVolumeMounted(ctx, volID, stagingTarget)
	if err != nil {
		return nil, err
	}

	// Volume is still mounted. Unstage the volume.
	if isMounted {
		log.Infof("Attempting to unmount target %q for volume %q", stagingTarget, volID)
		if err := gofsutil.Unmount(ctx, stagingTarget); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error unmounting stagingTarget: %v", err)
		}
	}
	log.Infof("NodeUnstageVolume successful for target %q for volume %q", stagingTarget, volID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// isBlockVolumeMounted checks if the block volume is properly mounted or not.
// If yes, then the calling function proceeds to unmount the volume.
func isBlockVolumeMounted(
	ctx context.Context,
	volID string,
	stagingTargetPath string) (
	bool, error) {

	log := logger.GetLogger(ctx)
	// Look up block device mounted to target.
	// BlockVolume: Here we are relying on the fact that the CO is required to
	// have created the staging path per the spec, even for BlockVolumes. Even
	// though we don't use the staging path for block, the fact nothing will be
	// mounted still indicates that unstaging is done.
	dev, err := getDevFromMount(stagingTargetPath)
	if err != nil {
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"isBlockVolumeMounted: error getting block device for volume: %s, err: %s",
			volID, err.Error())
	}

	// No device found.
	if dev == nil {
		// Nothing is mounted, so unstaging is already done.
		log.Debugf("isBlockVolumeMounted: No device found. Assuming Unstage is "+
			"already done for volume %q and target path %q", volID, stagingTargetPath)
		// For raw block volumes, we do not get a device attached to the
		// stagingTargetPath. This path is just a dummy.
		return false, nil
	}
	log.Debugf("found device: volID: %q, path: %q, block: %q, target: %q",
		volID, dev.FullPath, dev.RealDev, stagingTargetPath)

	// Get mounts for device.
	mnts, err := gofsutil.GetDevMounts(ctx, dev.RealDev)
	if err != nil {
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"isBlockVolumeMounted: could not reliably determine existing mount status: %s",
			err.Error())
	}

	// device is mounted more than once. Should only be mounted to target.
	if len(mnts) > 1 {
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"isBlockVolumeMounted: volume: %s appears mounted in multiple places", volID)
	}

	// Since we looked up the block volume from the target path, we assume that
	// the one existing mount is from the block to the target.
	log.Debugf("isBlockVolumeMounted: Found single mount point for volume %q and target %q",
		volID, stagingTargetPath)
	return true, nil
}

func (driver *vsphereCSIDriver) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("NodePublishVolume: called with args %+v", *req)
	var err error
	params := nodePublishParams{
		volID:  req.GetVolumeId(),
		target: req.GetTargetPath(),
		ro:     req.GetReadonly(),
	}
	// TODO: Verify if volume exists and return a NotFound error in negative
	// scenario.

	params.stagingTarget = req.GetStagingTargetPath()
	if params.stagingTarget == "" {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"staging target path %q not set", params.stagingTarget)
	}
	if params.target == "" {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"target path %q not set", params.target)
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
		params.diskID, err = getDiskID(req.GetPublishContext(), log)
		if err != nil {
			log.Errorf("error fetching DiskID. Parameters: %v", params)
			return nil, err
		}

		log.Debugf("Checking if volume %q is attached to disk %q", params.volID, params.diskID)
		volPath, err := verifyVolumeAttached(ctx, params.diskID)
		if err != nil {
			log.Errorf("error checking if volume is attached. Parameters: %v", params)
			return nil, err
		}

		// Get underlying block device.
		dev, err := getDevice(volPath)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error getting block device for volume: %q. Parameters: %v err: %v", params.volID, params, err)
		}
		params.volumePath = dev.FullPath
		params.device = dev.RealDev

		// check for Block vs Mount.
		if _, ok := volCap.GetAccessType().(*csi.VolumeCapability_Block); ok {
			// bind mount device to target.
			return publishBlockVol(ctx, req, dev, params)
		}
		// Volume must be a mount volume.
		return publishMountVol(ctx, req, dev, params)
	}
	// Volume must be a file share.
	return publishFileVol(ctx, req, params)
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

	// Verify if the path exists.
	// NOTE: For raw block volumes, this path is a file. In all other cases,
	// it is a directory.
	_, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			// Target path does not exist, so we must be Unpublished.
			log.Infof("NodeUnpublishVolume: Target path %q does not exist. Assuming NodeUnpublish is complete", target)
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to stat target %q, err: %v", target, err)
	}

	// Fetch all the mount points.
	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not retrieve existing mount points: %q", err.Error())
	}
	log.Debugf("NodeUnpublishVolume: node mounts %+v", mnts)

	// Check if the volume is already unpublished.
	// Also validates if path is a mounted directory or not.
	isPresent := common.IsTargetInMounts(ctx, target, mnts)
	if !isPresent {
		log.Infof("NodeUnpublishVolume: Target %s not present in mount points. Assuming it is already unpublished.",
			target)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Figure out if the target path is a file or block volume.
	isFileMount, _ := common.IsFileVolumeMount(ctx, target, mnts)
	isPublished := true
	if !isFileMount {
		isPublished, err = isBlockVolumePublished(ctx, volID, target)
		if err != nil {
			return nil, err
		}
	}

	if isPublished {
		log.Infof("NodeUnpublishVolume: Attempting to unmount target %q for volume %q", target, volID)
		if err := gofsutil.Unmount(ctx, target); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error unmounting target %q for volume %q. %q", target, volID, err.Error())
		}
		log.Debugf("Unmount successful for target %q for volume %q", target, volID)
		// TODO Use a go routine here. The deletion of target path might not be a
		// good reason to error out. The SP is supposed to delete the files or
		// directory it created in this target path.
		if err := rmpath(ctx, target); err != nil {
			log.Debugf("failed to delete the target path %q", target)
			return nil, err
		}
		log.Debugf("Target path  %q successfully deleted", target)
	}
	log.Infof("NodeUnpublishVolume successful for volume %q", volID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// isBlockVolumePublished checks if the device backing block volume exists.
func isBlockVolumePublished(ctx context.Context, volID string, target string) (bool, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Look up block device mounted to target.
	dev, err := getDevFromMount(target)
	if err != nil {
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"error getting block device for volume: %s, err: %v", volID, err)
	}

	if dev == nil {
		// Nothing is mounted, so unpublish is already done. However, we also know
		// that the target path exists, and it is our job to remove it.
		log.Debugf("isBlockVolumePublished: No device found. Assuming Unpublish is "+
			"already complete for volume %q and target path %q", volID, target)
		if err := rmpath(ctx, target); err != nil {
			return false, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to delete the target path %q. Error: %v", target, err)
		}
		log.Debugf("isBlockVolumePublished: Target path %q successfully deleted", target)
		return false, nil
	}
	return true, nil
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

	volMetrics, err := getMetrics(targetPath)
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

// getMetrics helps get volume metrics using k8s fsInfo strategy.
func getMetrics(path string) (*k8svol.Metrics, error) {
	if path == "" {
		return nil, fmt.Errorf("no path given")
	}

	available, capacity, usage, inodes, inodesFree, inodesUsed, err := fs.Info(path)
	if err != nil {
		return nil, err
	}
	metrics := &k8svol.Metrics{Time: metav1.Now()}
	metrics.Available = resource.NewQuantity(available, resource.BinarySI)
	metrics.Capacity = resource.NewQuantity(capacity, resource.BinarySI)
	metrics.Used = resource.NewQuantity(usage, resource.BinarySI)
	metrics.Inodes = resource.NewQuantity(inodes, resource.BinarySI)
	metrics.InodesFree = resource.NewQuantity(inodesFree, resource.BinarySI)
	metrics.InodesUsed = resource.NewQuantity(inodesUsed, resource.BinarySI)
	return metrics, nil
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

	var nodeInfoResponse *csi.NodeGetInfoResponse

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return nil, logger.LogNewErrorCode(log, codes.Internal, "ENV NODE_NAME is not set")
	}
	nodeID := nodeName

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

	if cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor)) == cnstypes.CnsClusterFlavorGuest {
		nodeInfoResponse = &csi.NodeGetInfoResponse{
			NodeId:             nodeID,
			MaxVolumesPerNode:  maxVolumesPerNode,
			AccessibleTopology: &csi.Topology{},
		}
		log.Infof("NodeGetInfo response: %v", nodeInfoResponse)
		return nodeInfoResponse, nil
	}

	var (
		accessibleTopology map[string]string
		err                error
	)
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
		accessibleTopology, err = fetchTopologyLabelsUsingVCCreds(ctx, nodeID, cfg)
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
func fetchTopologyLabelsUsingVCCreds(ctx context.Context, nodeID string, cfg *cnsconfig.Config) (
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
	uuid, err := getSystemUUID(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get system uuid for node VM. err: %v", err)
	}
	log.Debugf("Successfully retrieved uuid:%s  from the node: %s", uuid, nodeID)
	nodeVM, err := cnsvsphere.GetVirtualMachineByUUID(ctx, uuid, false)
	if err != nil || nodeVM == nil {
		log.Errorf("failed to get nodeVM for uuid: %s. err: %+v", uuid, err)
		uuid, err = convertUUID(uuid)
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
	dev, err := getDevFromMount(volumePath)
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

	realMounter := mount.New("")
	realExec := utilexec.New()
	mounter := &mount.SafeFormatAndMount{
		Interface: realMounter,
		Exec:      realExec,
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.OnlineVolumeExtend) {
		// Fetch the current block size.
		currentBlockSizeBytes, err := getBlockSizeBytes(mounter, dev.RealDev)
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
			err = rescanDevice(ctx, dev)
			if err != nil {
				return nil, logger.LogNewErrorCode(log, codes.Internal, err.Error())
			}
		}
	}

	// Resize file system.
	resizer := mount.NewResizeFs(realExec)
	_, err = resizer.Resize(dev.RealDev, volumePath)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error when resizing filesystem on volume %q on node: %v", volumeID, err)
	}
	log.Debugf("NodeExpandVolume: Resized filesystem with devicePath %s volumePath %s", dev.RealDev, volumePath)

	// Check the block size.
	currentBlockSizeBytes, err := getBlockSizeBytes(mounter, dev.RealDev)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error when getting size of block volume at path %s: %v", dev.RealDev, err)
	}
	// NOTE(xyang): Make sure new size is greater than or equal to the
	// requested size. It is possible for volume size to be rounded up
	// and therefore bigger than the requested size.
	if currentBlockSizeBytes < reqVolSizeBytes {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"requested volume size was %d, but got volume with size %d", reqVolSizeBytes, currentBlockSizeBytes)
	}

	log.Infof("NodeExpandVolume: expanded volume successfully. devicePath %s volumePath %s size %d",
		dev.RealDev, volumePath, int64(units.FileSize(reqVolSizeMB*common.MbInBytes)))
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: int64(units.FileSize(reqVolSizeMB * common.MbInBytes)),
	}, nil
}

func getBlockSizeBytes(mounter *mount.SafeFormatAndMount, devicePath string) (int64, error) {
	cmdArgs := []string{"--getsize64", devicePath}
	cmd := mounter.Exec.Command("blockdev", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return -1, fmt.Errorf("error when getting size of block volume at path %s: output: %s, err: %v",
			devicePath, string(output), err)
	}
	strOut := strings.TrimSpace(string(output))
	gotSizeBytes, err := strconv.ParseInt(strOut, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size %s into int a size", strOut)
	}
	return gotSizeBytes, nil
}

func publishMountVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	dev *Device,
	params nodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	log.Infof("PublishMountVolume called with args: %+v", params)

	// Extract fs details.
	_, mntFlags, err := ensureMountVol(ctx, log, req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}

	// We are responsible for creating target dir, per spec, if not already
	// present.
	_, err = mkdir(ctx, params.target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unable to create target dir: %q, err: %v", params.target, err)
	}
	log.Debugf("PublishMountVolume: Created target path %q", params.target)

	// Verify if the Staging path already exists.
	if _, err := verifyTargetDir(ctx, params.stagingTarget, true); err != nil {
		return nil, err
	}

	// Get block device mounts. Check if device is already mounted.
	devMnts, err := getDevMounts(ctx, dev)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not reliably determine existing mount status. Parameters: %v err: %v", params, err)
	}
	log.Debugf("publishMountVol: device %+v, device mounts %q", *dev, devMnts)

	// We expect that block device is already staged, so there should be at
	// least 1 mount already. if it's > 1, it may already be published.
	if len(devMnts) > 1 {
		// check if publish is already there.
		for _, m := range devMnts {
			if m.Path == params.target {
				// volume already published to target.
				// If mount options look good, do nothing.
				rwo := "rw"
				if params.ro {
					rwo = "ro"
				}
				if !contains(m.Opts, rwo) {
					// TODO: make sure that all the mount options match.
					return nil, logger.LogNewErrorCode(log, codes.AlreadyExists,
						"volume previously published with different options")
				}

				// Existing mount satisfies request.
				log.Infof("Volume already published to target. Parameters: [%+v]", params)
				return &csi.NodePublishVolumeResponse{}, nil
			}
		}
	} else if len(devMnts) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"volume ID: %q does not appear staged to %q", req.GetVolumeId(), params.stagingTarget)
	}

	// Do the bind mount to publish the volume.
	if params.ro {
		mntFlags = append(mntFlags, "ro")
	}
	log.Debugf("PublishMountVolume: Attempting to bind mount %q to %q with mount flags %v",
		params.stagingTarget, params.target, mntFlags)
	if err := gofsutil.BindMount(ctx, params.stagingTarget, params.target, mntFlags...); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error mounting volume. Parameters: %v err: %v", params, err)
	}
	log.Infof("NodePublishVolume for %q successful to path %q", req.GetVolumeId(), params.target)
	return &csi.NodePublishVolumeResponse{}, nil
}

func publishBlockVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	dev *Device,
	params nodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("PublishBlockVolume called with args: %+v", params)

	// We are responsible for creating target file, per spec, if not already
	// present.
	_, err := mkfile(ctx, params.target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unable to create target file: %q, err: %v", params.target, err)
	}
	log.Debugf("publishBlockVol: Target %q created", params.target)

	// Read-only is not supported for BlockVolume. Doing a read-only
	// bind mount of the device to the target path does not prevent
	// the underlying block device from being modified, so don't
	// advertise a false sense of security.
	if params.ro {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"read only not supported for Block Volume")
	}

	// Get block device mounts.
	devMnts, err := getDevMounts(ctx, dev)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not reliably determine existing mount status. Parameters: %v err: %v", params, err)
	}
	log.Debugf("publishBlockVol: device %+v, device mounts %q", *dev, devMnts)

	// Check if device is already mounted.
	if len(devMnts) == 0 {
		// Do the bind mount.
		mntFlags := make([]string, 0)
		log.Debugf("PublishBlockVolume: Attempting to bind mount %q to %q with mount flags %v",
			dev.FullPath, params.target, mntFlags)
		if err := gofsutil.BindMount(ctx, dev.FullPath, params.target, mntFlags...); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error mounting volume. Parameters: %v err: %v", params, err)
		}
		log.Debugf("PublishBlockVolume: Bind mount successful to path %q", params.target)
	} else if len(devMnts) == 1 {
		// Already mounted, make sure it's what we want.
		if devMnts[0].Path != params.target {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				"device already in use and mounted elsewhere")
		}
		log.Debugf("Volume already published to target. Parameters: [%+v]", params)
	} else {
		return nil, logger.LogNewErrorCode(log, codes.AlreadyExists,
			"block volume already mounted in more than one place")
	}
	log.Infof("NodePublishVolume successful to path %q", params.target)
	// Existing or new mount satisfies request.
	return &csi.NodePublishVolumeResponse{}, nil
}

func publishFileVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	params nodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	log.Infof("PublishFileVolume called with args: %+v", params)

	// Extract mount details.
	fsType, mntFlags, err := ensureMountVol(ctx, log, req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}

	// We are responsible for creating target dir, per spec, if not already
	// present.
	_, err = mkdir(ctx, params.target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unable to create target dir: %q, err: %v", params.target, err)
	}
	log.Debugf("PublishFileVolume: Created target path %q", params.target)

	// Check if target already mounted.
	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not retrieve existing mount points: %v", err)
	}
	log.Debugf("PublishFileVolume: Mounts - %+v", mnts)
	for _, m := range mnts {
		if m.Path == params.target {
			// Volume already published to target.
			// If mount options look good, do nothing.
			rwo := "rw"
			if params.ro {
				rwo = "ro"
			}
			if !contains(m.Opts, rwo) {
				// TODO: make sure that all the mount options match.
				return nil, logger.LogNewErrorCode(log, codes.AlreadyExists,
					"volume previously published with different options")
			}

			// Existing mount satisfies request.
			log.Infof("Volume already published to target %q.", params.target)
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	// Check for read-only flag on Pod pvc spec.
	if params.ro {
		mntFlags = append(mntFlags, "ro")
	}
	if cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor)) == cnstypes.CnsClusterFlavorGuest {
		mntFlags = append(mntFlags, "hard")
	}
	// Retrieve the file share access point from publish context.
	mntSrc, ok := req.GetPublishContext()[common.Nfsv4AccessPoint]
	if !ok {
		return nil, logger.LogNewErrorCode(log, codes.Internal,
			"nfs v4 accesspoint not set in publish context")
	}
	// Directly mount the file share volume to the pod. No bind mount required.
	log.Debugf("PublishFileVolume: Attempting to mount %q to %q with fstype %q and mountflags %v",
		mntSrc, params.target, fsType, mntFlags)
	if err := gofsutil.Mount(ctx, mntSrc, params.target, fsType, mntFlags...); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error publish volume to target path: %v", err)
	}
	log.Infof("NodePublishVolume successful to path %q", params.target)
	return &csi.NodePublishVolumeResponse{}, nil
}

// Device is a struct for holding details about a block device.
type Device struct {
	FullPath string
	Name     string
	RealDev  string
}

// getDevice returns a Device struct with info about the given device, or
// an error if it doesn't exist or is not a block device.
func getDevice(path string) (*Device, error) {

	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	// Eval any symlinks and make sure it points to a device.
	d, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}

	ds, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	dm := ds.Mode()
	if dm&os.ModeDevice == 0 {
		return nil, fmt.Errorf(
			"%s is not a block device", path)
	}

	return &Device{
		Name:     fi.Name(),
		FullPath: path,
		RealDev:  d,
	}, nil
}

func rescanDevice(ctx context.Context, dev *Device) error {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	devRescanPath, err := getDeviceRescanPath(dev)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(devRescanPath, []byte{'1'}, 0666)
	if err != nil {
		return logger.LogNewErrorf(log, "error rescanning block device %q. %v", dev.RealDev, err)
	}
	return nil
}

func getDeviceRescanPath(dev *Device) (string, error) {
	// A typical dev.RealDev path looks like `/dev/sda`. To rescan a block
	// device we need to write into `/sys/block/$DEVICE/device/rescan`
	// Refer to https://kb.vmware.com/s/article/1006371
	parts := strings.Split(dev.RealDev, "/")
	if len(parts) == 3 && strings.HasPrefix(parts[1], "dev") {
		return filepath.EvalSymlinks(filepath.Join("/sys/block", parts[2], "device", "rescan"))
	}
	return "", fmt.Errorf("illegal path for device %q", dev.RealDev)
}

// The files parameter is optional for testing purposes.
func getDiskPath(id string, files []os.FileInfo) (string, error) {
	var (
		devs []os.FileInfo
		err  error
	)

	if files == nil {
		devs, err = ioutil.ReadDir(devDiskID)
		if err != nil {
			return "", err
		}
	} else {
		devs = files
	}
	targetDisk := blockPrefix + id

	for _, f := range devs {
		if f.Name() == targetDisk {
			return filepath.Join(devDiskID, f.Name()), nil
		}
	}

	return "", nil
}

func contains(list []string, item string) bool {
	for _, x := range list {
		if x == item {
			return true
		}
	}
	return false
}

func verifyVolumeAttached(ctx context.Context, diskID string) (string, error) {
	log := logger.GetLogger(ctx)
	// Check that volume is attached.
	volPath, err := getDiskPath(diskID, nil)
	if err != nil {
		return "", logger.LogNewErrorCodef(log, codes.Internal,
			"error trying to read attached disks: %v", err)
	}
	if volPath == "" {
		return "", logger.LogNewErrorCodef(log, codes.NotFound,
			"disk: %s not attached to node", diskID)
	}

	log.Debugf("found disk: disk ID: %q, volume path: %q", diskID, volPath)
	return volPath, nil
}

// verifyTargetDir checks if the target path is not empty, exists and is a
// directory. If targetShouldExist is set to false, then verifyTargetDir
// returns (false, nil) if the path does not exist. If targetShouldExist is
// set to true, then verifyTargetDir returns (false, err) if the path does
// not exist.
func verifyTargetDir(ctx context.Context, target string, targetShouldExist bool) (bool, error) {
	log := logger.GetLogger(ctx)
	if target == "" {
		return false, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"target path required")
	}

	tgtStat, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			if targetShouldExist {
				// Target path does not exist but targetShouldExist is set to true.
				return false, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
					"target: %s not pre-created", target)
			}
			// Target path does not exist but targetShouldExist is set to false,
			// so no error.
			return false, nil
		}
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to stat target, err: %v", err)
	}

	// This check is mandated by the spec, but this would/should fail if the
	// volume has a block accessType as we get a file for raw block volumes
	// during NodePublish/Unpublish. Do not use this function for Publish or
	// Unpublish.
	if !tgtStat.IsDir() {
		return false, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"existing path: %s is not a directory", target)
	}

	log.Debugf("Target path %s verification complete", target)
	return true, nil
}

// mkdir creates the directory specified by path if needed.
// Return pair is a bool flag of whether dir was created, and an error.
func mkdir(ctx context.Context, path string) (bool, error) {
	log := logger.GetLogger(ctx)
	log.Infof("creating directory :%q", path)
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(path, 0750); err != nil {
				log.Error("Unable to create dir")
				return false, err
			}
			log.Infof("created directory")
			return true, nil
		}
		return false, err
	}
	if !st.IsDir() {
		return false, fmt.Errorf("existing path is not a directory")
	}
	return false, nil
}

// mkfile creates a file specified by the path if needed.
// Return pair is a bool flag of whether file was created, and an error.
func mkfile(ctx context.Context, path string) (bool, error) {
	log := logger.GetLogger(ctx)
	log.Infof("creating file :%q", path)
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		file, err := os.OpenFile(path, os.O_CREATE, 0755)
		if err != nil {
			log.Error("Unable to create dir")
			return false, err
		}
		file.Close()
		log.Debug("created file")
		return true, nil
	}
	if st.IsDir() {
		return false, fmt.Errorf("existing path is a directory")
	}
	return false, nil
}

// rmpath removes the given target path, whether it is a file or a directory.
// For directories, an error is returned if the dir is not empty.
func rmpath(ctx context.Context, target string) error {
	log := logger.GetLogger(ctx)
	// Target should be empty.
	log.Debugf("removing target path: %q", target)
	if err := os.Remove(target); err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"unable to remove target path: %s, err: %v", target, err)
	}
	return nil
}

func ensureMountVol(ctx context.Context, log *zap.SugaredLogger,
	volCap *csi.VolumeCapability) (string, []string, error) {
	mountVol := volCap.GetMount()
	if mountVol == nil {
		return "", nil, logger.LogNewErrorCode(log, codes.InvalidArgument, "access type missing")
	}
	fs := common.GetVolumeCapabilityFsType(ctx, volCap)
	mntFlags := mountVol.GetMountFlags()

	// By default, xfs does not allow mounting of two volumes with the same filesystem uuid.
	// Force ignore this uuid to be able to mount volume + its clone / restored snapshot on the same node.
	if fs == "xfs" {
		mntFlags = append(mntFlags, "nouuid")
	}

	return fs, mntFlags, nil
}

// A wrapper around gofsutil.GetMounts that handles bind mounts.
func getDevMounts(ctx context.Context,
	sysDevice *Device) ([]gofsutil.Info, error) {

	devMnts := make([]gofsutil.Info, 0)

	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return devMnts, err
	}
	for _, m := range mnts {
		if m.Device == sysDevice.RealDev || (m.Device == "devtmpfs" && m.Source == sysDevice.RealDev) {
			devMnts = append(devMnts, m)
		}
	}
	return devMnts, nil
}

func getSystemUUID(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)
	idb, err := ioutil.ReadFile(path.Join(dmiDir, "id", "product_uuid"))
	if err != nil {
		return "", err
	}
	log.Debugf("uuid in bytes: %v", idb)
	id := strings.TrimSpace(string(idb))
	log.Debugf("uuid in string: %s", id)
	return strings.ToLower(id), nil
}

// convertUUID helps convert UUID to vSphere format, for example,
// Input uuid:    6B8C2042-0DD1-D037-156F-435F999D94C1
// Returned uuid: 42208c6b-d10d-37d0-156f-435f999d94c1
func convertUUID(uuid string) (string, error) {
	if len(uuid) != 36 {
		return "", errors.New("uuid length should be 36")
	}
	convertedUUID := fmt.Sprintf("%s%s%s%s-%s%s-%s%s-%s-%s",
		uuid[6:8], uuid[4:6], uuid[2:4], uuid[0:2],
		uuid[11:13], uuid[9:11],
		uuid[16:18], uuid[14:16],
		uuid[19:23],
		uuid[24:36])
	return strings.ToLower(convertedUUID), nil
}

func getDiskID(pubCtx map[string]string, log *zap.SugaredLogger) (string, error) {
	var diskID string
	var ok bool
	if diskID, ok = pubCtx[common.AttributeFirstClassDiskUUID]; !ok {
		return "", logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"attribute: %s required in publish context",
			common.AttributeFirstClassDiskUUID)
	}
	return diskID, nil
}

func getDevFromMount(target string) (*Device, error) {

	// Get list of all mounts on system.
	mnts, err := gofsutil.GetMounts(context.Background())
	if err != nil {
		return nil, err
	}

	// example for RAW block device.
	// Device:udev
	// Path:/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-098a7585-109c-11ea-94c1-005056825b1f
	// Source:/dev/sdb
	// Type:devtmpfs
	// Opts:[rw relatime]}

	// example for RAW block device on devtmpfs
	// Host Information:
	//   OS: Red Hat Enterprise Linux CoreOS release 4.5
	//   Platform: OpenShift 4.5.7
	//
	// Device:devtmpfs
	// Path:/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/
	//      pvc-4e248bb9-f82a-4001-a031-74fc03c9f630/108db26e-3946-469a-9a9c-dd9d6a600956
	// Source:/dev/sdb
	// Type:devtmpfs
	// Opts:[rw nosuid]}

	// example for Mounted block device
	// Device:/dev/sdb
	// Path:/var/lib/kubelet/pods/c46d6473-0810-11ea-94c1-005056825b1f/volumes/
	//      kubernetes.io~csi/pvc-9e3d1d08-080f-11ea-be93-005056825b1f/mount
	// Source:/var/lib/kubelet/plugins/kubernetes.io/csi/pv/pvc-9e3d1d08-080f-11ea-be93-005056825b1f/globalmount
	// Type:ext4
	// Opts:[rw relatime]

	// example for File Volume
	// Device:h10-186-38-214.vsanfs3.testdomain:/5231f3d8-f06b-b67c-8cd1-f3c126013ce4
	// Path:/var/lib/kubelet/pods/ba41b064-bd0b-4a47-afff-8d262ec6308a/volumes/
	//      kubernetes.io~csi/pvc-a425f631-f93c-4cb6-9d61-03bd2dd81b44/mount
	// Source:h10-186-38-214.vsanfs3.testdomain:/5231f3d8-f06b-b67c-8cd1-f3c126013ce4
	// Type:nfs4
	// Opts:[rw relatime]

	for _, m := range mnts {
		if m.Path == target {
			// Something is mounted to target, get underlying disk.
			d := m.Device
			if m.Device == "udev" || m.Device == "devtmpfs" {
				d = m.Source
			}
			dev, err := getDevice(d)
			if err != nil {
				return nil, err
			}
			return dev, nil
		}
	}

	// Did not identify a device mounted to target.
	return nil, nil
}
