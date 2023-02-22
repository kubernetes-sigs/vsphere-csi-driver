//go:build darwin || linux
// +build darwin linux

/*
Copyright 2021 The Kubernetes Authors.
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

package osutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/akutz/gofsutil"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svol "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/fs"
	"k8s.io/mount-utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/mounter"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	devDiskID   = "/dev/disk/by-id"
	blockPrefix = "wwn-0x"
	dmiDir      = "/sys/class/dmi"
	UUIDPrefix  = "VMware-"
)

// defaultFileMountOptions are the mount flag options used by default while publishing a file volume.
var defaultFileMountOptions = []string{"hard", "sec=sys", "vers=4", "minorversion=1"}

// NewOsUtils creates OsUtils with a linux specific mounter
func NewOsUtils(ctx context.Context) (*OsUtils, error) {
	log := logger.GetLogger(ctx)
	mounter, err := mounter.NewSafeMounter(ctx)
	if err != nil {
		log.Debugf("Could not create instance of Mounter %v", err)
		return nil, err
	}
	return &OsUtils{
		Mounter: mounter,
	}, nil
}

// NodeStageBlockVolume mounts mount volume or file volume to staging target
func (osUtils *OsUtils) NodeStageBlockVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest,
	params NodeStageParams) (
	*csi.NodeStageVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	// Block Volume.
	pubCtx := req.GetPublishContext()
	diskID, err := osUtils.GetDiskID(pubCtx, log)
	if err != nil {
		return nil, err
	}
	log.Infof("nodeStageBlockVolume: Retrieved diskID as %q", diskID)

	// Verify if the volume is attached.
	log.Debugf("nodeStageBlockVolume: Checking if volume is attached to diskID: %v", diskID)
	volPath, err := osUtils.VerifyVolumeAttached(ctx, diskID)
	if err != nil {
		log.Errorf("Error checking if volume %q is attached. Parameters: %v", params.VolID, params)
		return nil, err
	}
	log.Debugf("nodeStageBlockVolume: Disk %q attached at %q", diskID, volPath)

	// Check that block device looks good.
	dev, err := osUtils.GetDevice(ctx, volPath)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error getting block device for volume: %q. Parameters: %v err: %v",
			params.VolID, params, err)

	}
	log.Debugf("nodeStageBlockVolume: getDevice %+v", *dev)

	// Check if this is a MountVolume or BlockVolume.
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		// Volume is a block volume, so skip the rest of the steps.
		log.Infof("nodeStageBlockVolume: Skipping staging for block volume ID %q", params.VolID)
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
		if params.Ro {
			log.Debugf("nodeStageBlockVolume: Mounting %q at %q in read-only mode with mount flags %v",
				dev.FullPath, params.StagingTarget, params.MntFlags)
			params.MntFlags = append(params.MntFlags, "ro")
			err := gofsutil.Mount(ctx, dev.FullPath, params.StagingTarget, params.FsType, params.MntFlags...)
			if err != nil {
				return nil, logger.LogNewErrorCodef(log, codes.Internal,
					"error mounting volume. Parameters: %v err: %v", params, err)
			}
			log.Infof("nodeStageBlockVolume: Device mounted successfully at %q", params.StagingTarget)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		// Format and mount the device.
		log.Debugf("nodeStageBlockVolume: Format and mount the device %q at %q with mount flags %v",
			dev.FullPath, params.StagingTarget, params.MntFlags)
		err := gofsutil.FormatAndMount(ctx, dev.FullPath, params.StagingTarget, params.FsType, params.MntFlags...)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error in formating and mounting volume. Parameters: %v err: %v", params, err)
		}
	} else {
		// If Device is already mounted. Need to ensure that it is already.
		// mounted to the expected staging target, with correct rw/ro perms.
		log.Debugf("nodeStageBlockVolume: Device already mounted. Checking mount flags %v for correctness.",
			params.MntFlags)
		for _, m := range mnts {
			if unescape(ctx, m.Path) == params.StagingTarget {
				rwo := "rw"
				if params.Ro {
					rwo = "ro"
				}
				log.Debugf("nodeStageBlockVolume: Checking for mount options %v", m.Opts)
				if common.Contains(m.Opts, rwo) {
					// TODO make sure that all the mount options match.
					log.Infof("nodeStageBlockVolume: Device already mounted at %q with mount option %q",
						params.StagingTarget, rwo)
					return &csi.NodeStageVolumeResponse{}, nil
				}
				return nil, logger.LogNewErrorCodef(log, codes.AlreadyExists,
					"access mode conflicts with existing mount at %q", params.StagingTarget)
			}
		}
		return nil, logger.LogNewErrorCode(log, codes.Internal,
			"device already in use and mounted elsewhere")
	}
	log.Infof("nodeStageBlockVolume: Device mounted successfully at %q", params.StagingTarget)

	return &csi.NodeStageVolumeResponse{}, nil
}

// CleanupStagePath will unmount the volume from node and remove the stage directory
func (osUtils *OsUtils) CleanupStagePath(ctx context.Context, stagingTarget string, volID string) error {
	log := logger.GetLogger(ctx)
	/// Block volume.
	isMounted, err := osUtils.IsBlockVolumeMounted(ctx, volID, stagingTarget)
	if err != nil {
		return err
	}

	// Volume is still mounted. Unstage the volume.
	if isMounted {
		log.Infof("Attempting to unmount target %q for volume %q", stagingTarget, volID)
		if err := gofsutil.Unmount(ctx, stagingTarget); err != nil {
			return fmt.Errorf(
				"error unmounting stagingTarget: %v", err)
		}
	}
	return nil
}

// IsBlockVolumeMounted checks if the block volume is properly mounted or not.
// If yes, then the calling function proceeds to unmount the volume.
func (osUtils *OsUtils) IsBlockVolumeMounted(
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
	dev, err := osUtils.GetDevFromMount(ctx, stagingTargetPath)
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

// CleanupPublishPath will unmount and remove publish path
func (osUtils *OsUtils) CleanupPublishPath(ctx context.Context, target string, volID string) error {
	log := logger.GetLogger(ctx)
	// Verify if the path exists.
	// NOTE: For raw block volumes, this path is a file. In all other cases,
	// it is a directory.
	_, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			// Target path does not exist, so we must be Unpublished.
			log.Infof("NodeUnpublishVolume: Target path %q does not exist. Assuming NodeUnpublish is complete", target)
			return nil
		}
		return fmt.Errorf(
			"failed to stat target %q, err: %v", target, err)
	}

	// Fetch all the mount points.
	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return fmt.Errorf(
			"could not retrieve existing mount points: %q", err.Error())
	}
	log.Debugf("NodeUnpublishVolume: node mounts %+v", mnts)

	// Check if the volume is already unpublished.
	// Also validates if path is a mounted directory or not.
	isPresent := isTargetInMounts(ctx, target, mnts)
	if !isPresent {
		log.Infof("NodeUnpublishVolume: Target %s not present in mount points. Assuming it is already unpublished.",
			target)
		return nil
	}

	// Figure out if the target path is a file or block volume.
	isFileMount, _ := isFileVolumeMount(ctx, target, mnts)
	isPublished := true
	if !isFileMount {
		isPublished, err = osUtils.IsBlockVolumePublished(ctx, volID, target)
		if err != nil {
			return err
		}
	}

	if isPublished {
		log.Infof("NodeUnpublishVolume: Attempting to unmount target %q for volume %q", target, volID)
		if err := gofsutil.Unmount(ctx, target); err != nil {
			return fmt.Errorf(
				"error unmounting target %q for volume %q. %q", target, volID, err.Error())
		}
		log.Debugf("Unmount successful for target %q for volume %q", target, volID)
		// TODO Use a go routine here. The deletion of target path might not be a
		// good reason to error out. The SP is supposed to delete the files or
		// directory it created in this target path.
		if err := osUtils.Rmpath(ctx, target); err != nil {
			log.Debugf("failed to delete the target path %q", target)
			return err
		}
		log.Debugf("Target path  %q successfully deleted", target)
	}
	return nil
}

// IsBlockVolumePublished checks if the device backing block volume exists.
func (osUtils *OsUtils) IsBlockVolumePublished(ctx context.Context, volID string, target string) (bool, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Look up block device mounted to target.
	dev, err := osUtils.GetDevFromMount(ctx, target)
	if err != nil {
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"error getting block device for volume: %s, err: %v", volID, err)
	}

	if dev == nil {
		// check if target is mount point
		notMountPoint, err := mount.IsNotMountPoint(osUtils.Mounter, target)
		if err != nil {
			log.Errorf("error while checking target path %q is mount point err: %v", target, err)
			return false, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to verify mount point %q. Error: %v", target, err)
		}
		if !notMountPoint {
			log.Infof("target %q is mount point", target)
			return true, nil
		}
		// Nothing is mounted, so unpublish is already done. However, we also know
		// that the target path exists, and it is our job to remove it.
		log.Debugf("isBlockVolumePublished: No device found. Assuming Unpublish is "+
			"already complete for volume %q and target path %q", volID, target)
		if err := osUtils.Rmpath(ctx, target); err != nil {
			return false, logger.LogNewErrorCodef(log, codes.Internal,
				"failed to delete the target path %q. Error: %v", target, err)
		}
		log.Debugf("isBlockVolumePublished: Target path %q successfully deleted", target)
		return false, nil
	}
	return true, nil
}

// GetMetrics helps get volume metrics using k8s fsInfo strategy.
func (osUtils *OsUtils) GetMetrics(ctx context.Context, path string) (*k8svol.Metrics, error) {
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

// GetBlockSizeBytes returns the Block size in bytes
func (osUtils *OsUtils) GetBlockSizeBytes(ctx context.Context, devicePath string) (int64, error) {
	cmdArgs := []string{"--getsize64", devicePath}
	cmd := osUtils.Mounter.Exec.Command("blockdev", cmdArgs...)
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

// PublishBlockVol mounts block volume to publish target
func (osUtils *OsUtils) PublishMountVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	dev *Device,
	params NodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	log.Infof("PublishMountVolume called with args: %+v", params)

	// Extract fs details.
	_, mntFlags, err := osUtils.EnsureMountVol(ctx, req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}

	// We are responsible for creating target dir, per spec, if not already
	// present.
	_, err = osUtils.Mkdir(ctx, params.Target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unable to create target dir: %q, err: %v", params.Target, err)
	}
	log.Debugf("PublishMountVolume: Created target path %q", params.Target)

	// Verify if the Staging path already exists.
	if _, err := osUtils.VerifyTargetDir(ctx, params.StagingTarget, true); err != nil {
		return nil, err
	}

	// Get block device mounts. Check if device is already mounted.
	devMnts, err := osUtils.GetDevMounts(ctx, dev)
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
			if unescape(ctx, m.Path) == params.Target {
				// volume already published to target.
				// If mount options look good, do nothing.
				rwo := "rw"
				if params.Ro {
					rwo = "ro"
				}
				if !common.Contains(m.Opts, rwo) {
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
			"volume ID: %q does not appear staged to %q", req.GetVolumeId(), params.StagingTarget)
	}

	// Do the bind mount to publish the volume.
	if params.Ro {
		mntFlags = append(mntFlags, "ro")
	}
	log.Debugf("PublishMountVolume: Attempting to bind mount %q to %q with mount flags %v",
		params.StagingTarget, params.Target, mntFlags)
	if err := gofsutil.BindMount(ctx, params.StagingTarget, params.Target, mntFlags...); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error mounting volume. Parameters: %v err: %v", params, err)
	}
	log.Infof("NodePublishVolume for %q successful to path %q", req.GetVolumeId(), params.Target)
	return &csi.NodePublishVolumeResponse{}, nil
}

// PublishBlockVol mounts raw block device to publish target
func (osUtils *OsUtils) PublishBlockVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	dev *Device,
	params NodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("PublishBlockVolume called with args: %+v", params)

	// We are responsible for creating target file, per spec, if not already
	// present.
	_, err := osUtils.Mkfile(ctx, params.Target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unable to create target file: %q, err: %v", params.Target, err)
	}
	log.Debugf("publishBlockVol: Target %q created", params.Target)

	// Read-only is not supported for BlockVolume. Doing a read-only
	// bind mount of the device to the target path does not prevent
	// the underlying block device from being modified, so don't
	// advertise a false sense of security.
	if params.Ro {
		return nil, logger.LogNewErrorCode(log, codes.InvalidArgument,
			"read only not supported for Block Volume")
	}

	// Get block device mounts.
	devMnts, err := osUtils.GetDevMounts(ctx, dev)
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
			dev.FullPath, params.Target, mntFlags)
		if err := gofsutil.BindMount(ctx, dev.FullPath, params.Target, mntFlags...); err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error mounting volume. Parameters: %v err: %v", params, err)
		}
		log.Debugf("PublishBlockVolume: Bind mount successful to path %q", params.Target)
	} else if len(devMnts) == 1 {
		// Already mounted, make sure it's what we want.
		if unescape(ctx, devMnts[0].Path) != params.Target {
			return nil, logger.LogNewErrorCode(log, codes.Internal,
				"device already in use and mounted elsewhere")
		}
		log.Debugf("Volume already published to target. Parameters: [%+v]", params)
	} else {
		return nil, logger.LogNewErrorCode(log, codes.AlreadyExists,
			"block volume already mounted in more than one place")
	}
	log.Infof("NodePublishVolume successful to path %q", params.Target)
	// Existing or new mount satisfies request.
	return &csi.NodePublishVolumeResponse{}, nil
}

// PublishBlockVol mounts file volume to publish target
func (osUtils *OsUtils) PublishFileVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	params NodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	log.Infof("PublishFileVolume called with args: %+v", params)

	// Extract mount details.
	fsType, mntFlags, err := osUtils.EnsureMountVol(ctx, req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}

	// We are responsible for creating target dir, per spec, if not already
	// present.
	_, err = osUtils.Mkdir(ctx, params.Target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unable to create target dir: %q, err: %v", params.Target, err)
	}
	log.Debugf("PublishFileVolume: Created target path %q", params.Target)

	// Check if target already mounted.
	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"could not retrieve existing mount points: %v", err)
	}
	log.Debugf("PublishFileVolume: Mounts - %+v", mnts)
	for _, m := range mnts {
		if unescape(ctx, m.Path) == params.Target {
			// Volume already published to target.
			// If mount options look good, do nothing.
			rwo := "rw"
			if params.Ro {
				rwo = "ro"
			}
			if !common.Contains(m.Opts, rwo) {
				// TODO: make sure that all the mount options match.
				return nil, logger.LogNewErrorCode(log, codes.AlreadyExists,
					"volume previously published with different options")
			}

			// Existing mount satisfies request.
			log.Infof("Volume already published to target %q.", params.Target)
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	// Check for read-only flag on Pod pvc spec.
	if params.Ro {
		mntFlags = append(mntFlags, "ro")
	}
	// Add defaultFileMountOptions to the mntFlags.
	mntFlags = append(mntFlags, defaultFileMountOptions...)
	// Retrieve the file share access point from publish context.
	mntSrc, ok := req.GetPublishContext()[common.Nfsv4AccessPoint]
	if !ok {
		return nil, logger.LogNewErrorCode(log, codes.Internal,
			"nfs v4 accesspoint not set in publish context")
	}
	// Directly mount the file share volume to the pod. No bind mount required.
	log.Debugf("PublishFileVolume: Attempting to mount %q to %q with fstype %q and mountflags %v",
		mntSrc, params.Target, fsType, mntFlags)
	if err := gofsutil.Mount(ctx, mntSrc, params.Target, fsType, mntFlags...); err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"error publish volume to target path: %v", err)
	}
	log.Infof("NodePublishVolume successful to path %q", params.Target)
	return &csi.NodePublishVolumeResponse{}, nil
}

// GetDevice returns a Device struct with info about the given device, or
// an error if it doesn't exist or is not a block device.
func (osUtils *OsUtils) GetDevice(ctx context.Context, path string) (*Device, error) {
	log := logger.GetLogger(ctx)
	log.Infof("check path exits %s", path)
	fi, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = syscall.Access(path, syscall.F_OK)
			if err == nil {
				// The access syscall says the file exists, the stat syscall says it doesn't.
				// fake error and treat the path as existing but corrupted.
				log.Debugf("Potential stale file handle detected: %s", path)
				return nil, syscall.ESTALE
			}
			log.Infof("path: %v does not exists", err)
			return nil, nil
		} else if mount.IsCorruptedMnt(err) {
			log.Infof("mount is currupted %v", err)
			return nil, err
		}
		log.Infof("error checking path %v", err)
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

// RescanDevice rescans the device
func (osUtils *OsUtils) RescanDevice(ctx context.Context, dev *Device) error {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	devRescanPath, err := osUtils.GetDeviceRescanPath(dev)
	if err != nil {
		return err
	}

	err = os.WriteFile(devRescanPath, []byte{'1'}, 0666)
	if err != nil {
		msg := fmt.Sprintf("error rescanning block device %q. %v", dev.RealDev, err)
		log.Error(msg)
		return fmt.Errorf(msg)
	}
	return nil
}

// GetDeviceRescanPath is used to rescan the device
func (osUtils *OsUtils) GetDeviceRescanPath(dev *Device) (string, error) {
	// A typical dev.RealDev path looks like `/dev/sda`. To rescan a block
	// device we need to write into `/sys/block/$DEVICE/device/rescan`
	// Refer to https://kb.vmware.com/s/article/1006371
	parts := strings.Split(dev.RealDev, "/")
	if len(parts) == 3 && strings.HasPrefix(parts[1], "dev") {
		return filepath.EvalSymlinks(filepath.Join("/sys/block", parts[2], "device", "rescan"))
	}
	return "", fmt.Errorf("illegal path for device %q", dev.RealDev)
}

// GetDiskPath return the full DiskPath for diskID
func (osUtils *OsUtils) GetDiskPath(id string) (string, error) {
	var (
		devs []os.DirEntry
		err  error
	)
	devs, err = os.ReadDir(devDiskID)
	if err != nil {
		return "", err
	}
	targetDisk := blockPrefix + id

	for _, f := range devs {
		if f.Name() == targetDisk {
			return filepath.Join(devDiskID, f.Name()), nil
		}
	}

	return "", nil
}

// VerifyVolumeAttached verifies if the volume path exist for diskID
func (osUtils *OsUtils) VerifyVolumeAttached(ctx context.Context, diskID string) (string, error) {
	log := logger.GetLogger(ctx)
	// Check that volume is attached.
	volPath, err := osUtils.GetDiskPath(diskID)
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

// VerifyTargetDir checks if the target path is not empty, exists and is a
// directory. If targetShouldExist is set to false, then verifyTargetDir
// returns (false, nil) if the path does not exist. If targetShouldExist is
// set to true, then verifyTargetDir returns (false, err) if the path does
// not exist.
func (osUtils *OsUtils) VerifyTargetDir(ctx context.Context, target string, targetShouldExist bool) (bool, error) {
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

// Mkdir creates the directory specified by path if needed.
// Return pair is a bool flag of whether dir was created, and an error.
func (osUtils *OsUtils) Mkdir(ctx context.Context, path string) (bool, error) {
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

// Mkfile creates a file specified by the path if needed.
// Return pair is a bool flag of whether file was created, and an error.
func (osUtils *OsUtils) Mkfile(ctx context.Context, path string) (bool, error) {
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

// Rmpath removes the given target path, whether it is a file or a directory.
// For directories, an error is returned if the dir is not empty.
func (osUtils *OsUtils) Rmpath(ctx context.Context, target string) error {
	log := logger.GetLogger(ctx)
	// Target should be empty.
	log.Debugf("removing target path: %q", target)
	if err := os.Remove(target); err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"unable to remove target path: %s, err: %v", target, err)
	}
	return nil
}

// A wrapper around gofsutil.GetMounts that handles bind mounts.
func (osUtils *OsUtils) GetDevMounts(ctx context.Context,
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

// GetSystemUUID returns the UUID used to identify node vm
func (osUtils *OsUtils) GetSystemUUID(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)
	idb, err := os.ReadFile(path.Join(dmiDir, "id", "product_serial"))
	if err != nil {
		return "", err
	}
	uuidFromFile := string(idb[:])
	//strip leading and trailing white space and new line char
	uuid := strings.TrimSpace(uuidFromFile)
	log.Debugf("product_serial in string: %s", uuid)
	// check the uuid starts with "VMware-"
	if !strings.HasPrefix(uuid, UUIDPrefix) {
		return "", fmt.Errorf("failed to match Prefix, UUID read from the file is %s",
			uuidFromFile)
	}
	// Strip the prefix and while spaces and -
	uuid = strings.Replace(uuid[len(UUIDPrefix):], " ", "", -1)
	uuid = strings.Replace(uuid, "-", "", -1)
	if len(uuid) != 32 {
		return "", fmt.Errorf("length check failed, UUID read from the file is %v", uuidFromFile)
	}
	// need to add dashes, e.g. "564d395e-d807-e18a-cb25-b79f65eb2b9f"
	uuid = fmt.Sprintf("%s-%s-%s-%s-%s", uuid[0:8], uuid[8:12], uuid[12:16], uuid[16:20], uuid[20:32])
	log.Infof("UUID is %s", uuid)
	return uuid, nil
}

// convertUUID helps convert UUID to vSphere format, for example,
// Input uuid:    6B8C2042-0DD1-D037-156F-435F999D94C1
// Returned uuid: 42208c6b-d10d-37d0-156f-435f999d94c1
func (osUtils *OsUtils) ConvertUUID(uuid string) (string, error) {
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

// GetDevFromMount returns device info mounted on the target dir
func (osUtils *OsUtils) GetDevFromMount(ctx context.Context, target string) (*Device, error) {

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
		if unescape(ctx, m.Path) == target {
			// Something is mounted to target, get underlying disk.
			d := m.Device
			if m.Device == "udev" || m.Device == "devtmpfs" {
				d = m.Source
			}
			dev, err := osUtils.GetDevice(ctx, d)
			if err != nil {
				return nil, err
			}
			return dev, nil
		}
	}

	// Did not identify a device mounted to target.
	return nil, nil
}

// IsTargetInMounts checks if a path exists in the mounts
func (osUtils *OsUtils) IsTargetInMounts(ctx context.Context, path string) (bool, error) {
	log := logger.GetLogger(ctx)
	mnts, err := gofsutil.GetMounts(ctx)
	if err != nil {
		return false, err
	}
	log.Debugf("NodeUnstageVolume: node mounts %+v", mnts)
	return isTargetInMounts(ctx, path, mnts), nil
}

// GetVolumeCapabilityFsType retrieves fstype from VolumeCapability.
// Defaults to nfs4 for file volume and ext4 for block volume when empty string
// is observed. This function also ignores default ext4 fstype supplied by
// external-provisioner when none is specified in the StorageClass
func (osUtils *OsUtils) GetVolumeCapabilityFsType(ctx context.Context,
	capability *csi.VolumeCapability) (string, error) {
	log := logger.GetLogger(ctx)
	fsType := strings.ToLower(capability.GetMount().GetFsType())
	log.Infof("FsType received from Volume Capability: %q", fsType)
	isFileVolume := common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{capability})
	if isFileVolume {
		// For File volumes we only support nfs or nfs4 filesystem. External-provisioner sets default fstype
		// as ext4 when none is specified in StorageClass, hence overwrite it to nfs4 while mounting the volume.
		if fsType == "" || fsType == "ext4" {
			log.Infof("empty string or ext4 fstype observed for file volume. Defaulting to: %s",
				common.NfsV4FsType)
			fsType = common.NfsV4FsType
		} else if !(fsType == common.NfsFsType || fsType == common.NfsV4FsType) {
			return "", logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"unsupported fsType %q observed for file volume", fsType)
		}
	} else {
		// For Block volumes we only support following filesystems:
		// ext3, ext4 and xfs for Linux.
		if fsType == "" {
			log.Infof("empty string fstype observed for block volume. Defaulting to: %s",
				common.Ext4FsType)
			fsType = common.Ext4FsType
		} else if !(fsType == common.Ext4FsType || fsType == common.Ext3FsType || fsType == common.XFSType) {
			return "", logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"unsupported fsType %q observed for block volume", fsType)
		}
	}
	return fsType, nil
}

// ResizeVolume resizes the volume
func (osUtils *OsUtils) ResizeVolume(ctx context.Context, devicePath, volumePath string, reqVolSizeBytes int64) error {
	log := logger.GetLogger(ctx)
	resizer := mount.NewResizeFs(osUtils.Mounter.Exec)
	_, err := resizer.Resize(devicePath, volumePath)
	if err != nil {
		return fmt.Errorf(
			"error when resizing filesystem on devicePath %s and volumePath %s, err: %v ", devicePath, volumePath, err)
	}
	// Check the block size.
	currentBlockSizeBytes, err := osUtils.GetBlockSizeBytes(ctx, devicePath)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"error when getting size of block volume at path %s: %v", devicePath, err)
	}
	// NOTE(xyang): Make sure new size is greater than or equal to the
	// requested size. It is possible for volume size to be rounded up
	// and therefore bigger than the requested size.
	if currentBlockSizeBytes < reqVolSizeBytes {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"requested volume size was %d, but got volume with size %d", reqVolSizeBytes, currentBlockSizeBytes)
	}

	return nil
}

func (osUtils *OsUtils) VerifyVolumeAttachedAndFillParams(ctx context.Context,
	pubCtx map[string]string, params *NodePublishParams, dev **Device) error {
	log := logger.GetLogger(ctx)
	var err error
	params.DiskID, err = osUtils.GetDiskID(pubCtx, log)
	if err != nil {
		log.Errorf("error fetching DiskID. Parameters: %v", params)
		return err
	}
	log.Debugf("Checking if volume %q is attached to disk %q", params.VolID, params.DiskID)
	volPath, err := osUtils.VerifyVolumeAttached(ctx, params.DiskID)
	if err != nil {
		log.Errorf("error checking if volume is attached. Parameters: %v", params)
		return err
	}

	// Get underlying block device.
	*dev, err = osUtils.GetDevice(ctx, volPath)
	log.Debugf("Device: %v", dev)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"error getting block device for volume: %q. Parameters: %v err: %v", params.VolID, params, err)
	}
	params.VolumePath = (*dev).FullPath
	params.Device = (*dev).RealDev
	return nil
}

// IsFileVolumeMount loops through the list of mount points and
// checks if the target path mount point is a file volume type or not.
// Returns an error if the target path is not found in the mount points.
func isFileVolumeMount(ctx context.Context, target string, mnts []gofsutil.Info) (bool, error) {
	log := logger.GetLogger(ctx)
	for _, m := range mnts {
		if unescape(ctx, m.Path) == target {
			if m.Type == common.NfsFsType || m.Type == common.NfsV4FsType {
				log.Debug("IsFileVolumeMount: Found file volume")
				return true, nil
			}
			log.Debug("IsFileVolumeMount: Found block volume")
			return false, nil
		}
	}
	// Target path mount point not found in list of mounts.
	return false, fmt.Errorf("could not find target path %q in list of mounts", target)
}

// IsTargetInMounts checks if the given target path is present in list of
// mount points.
func isTargetInMounts(ctx context.Context, target string, mnts []gofsutil.Info) bool {
	log := logger.GetLogger(ctx)
	for _, m := range mnts {
		if unescape(ctx, m.Path) == target {
			log.Debugf("Found target %q in list of mounts", target)
			return true
		}
	}
	log.Debugf("Target %q not found in list of mounts", target)
	return false
}

// decides if node should continue
func (osUtils *OsUtils) ShouldContinue(ctx context.Context) {
	// no op for linux
}

// un-escapes "\nnn" sequences in /proc/self/mounts. For example, replaces "\040" with space " ".
func unescape(ctx context.Context, in string) string {
	log := logger.GetLogger(ctx)
	out := make([]rune, 0, len(in))
	s := in
	for len(s) > 0 {
		// Un-escape single character.
		// UnquoteChar will un-escape also \r, \n, \Unnnn and other sequences, but they should not be used in /proc/mounts.
		rune, _, tail, err := strconv.UnquoteChar(s, '"')
		if err != nil {
			log.Infof("Error parsing mount %q: %s", in, err)
			// Use escaped string as a fallback
			return in
		}
		out = append(out, rune)
		s = tail
	}
	return string(out)
}

// Check if device at given path is block device or not
func (osUtils *OsUtils) IsBlockDevice(ctx context.Context, volumePath string) (bool, error) {
	log := logger.GetLogger(ctx)

	deviceInfo, err := os.Stat(volumePath)
	if err != nil {
		return false, logger.LogNewErrorf(log, "IsBlockDevice: could not get device info for path %s", volumePath)
	}
	return deviceInfo.Mode()&os.ModeDevice == os.ModeDevice, nil
}
