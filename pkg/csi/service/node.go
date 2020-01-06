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
	csictx "github.com/rexray/gocsi/context"
	log "github.com/sirupsen/logrus"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"gitlab.eng.vmware.com/hatchway/govmomi/units"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"

	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/resizefs"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

const (
	devDiskID   = "/dev/disk/by-id"
	blockPrefix = "wwn-0x"
	dmiDir      = "/sys/class/dmi"
)

type nodeStageParams struct {
	// volID is the identifier for the underlying volume
	volID string
	// fsType is the file system type - ext3, ext4, nfs, nfs4
	fsType string
	// Staging Target path is used to mount the volume to the node
	stagingTarget string
	// Mount flags/options intended to be used while running the mount command
	mntFlags []string
	// Read-only flag
	ro bool
}

type nodePublishParams struct {
	// volID is the identifier for the underlying volume
	volID string
	// Target path is used to bind-mount a staged volume to the pod
	target string
	// Staging Target path is used to mount the volume to the node
	stagingTarget string
	// diskID is the identifier for the disk
	diskID string
	// volumePath represents the sym-linked block volume full path
	volumePath string
	// device represents the actual path of the block volume
	device string
	// Read-only flag
	ro bool
}

func (s *service) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {

	klog.V(4).Infof("NodeStageVolume: called with args %+v", *req)

	volumeID := req.GetVolumeId()
	volCap := req.GetVolumeCapability()
	// Check for block volume or file share
	if common.IsFileVolumeRequest([]*csi.VolumeCapability{volCap}) {
		klog.V(2).Infof("NodeStageVolume: Volume %q detected as a file share volume. Ignoring staging for file volumes.", volumeID)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	var err error
	params := nodeStageParams{
		volID: volumeID,
		// Retrieve accessmode - RO/RW
		ro: common.IsVolumeReadOnly(req.GetVolumeCapability()),
	}
	// TODO: Verify if volume exists and return a NotFound error in negative scenario

	// Check if this is a MountVolume or Raw BlockVolume
	if _, ok := volCap.GetAccessType().(*csi.VolumeCapability_Mount); ok {
		// Mount Volume
		// Extract mount volume details
		klog.V(4).Info("NodeStageVolume: Volume detected as a mount volume")
		params.fsType, params.mntFlags, err = ensureMountVol(volCap)
		if err != nil {
			return nil, err
		}

		// Check that staging path is created by CO and is a directory
		params.stagingTarget = req.GetStagingTargetPath()
		if _, err = verifyTargetDir(params.stagingTarget, true); err != nil {
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
	// Block Volume
	pubCtx := req.GetPublishContext()
	diskID, err := getDiskID(pubCtx)
	if err != nil {
		return nil, err
	}
	klog.V(4).Infof("NodeStageVolume: volID %q, published context %+v, diskID %q",
		params.volID, pubCtx, diskID)
	f := log.Fields{
		"volID":  params.volID,
		"diskID": diskID,
	}

	// Verify if the volume is attached
	klog.V(4).Infof("NodeStageVolume: Checking if volume is attached with diskID: %v", diskID)
	volPath, err := verifyVolumeAttached(diskID)
	if err != nil {
		return nil, err
	}
	klog.V(4).Infof("NodeStageVolume: Disk %q attached at %q", diskID, volPath)

	// Check that block device looks good
	dev, err := getDevice(volPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"error getting block device for volume: %q, err: %q",
			params.volID, err.Error())
	}
	klog.V(4).Infof("NodeStageVolume: getDevice %+v", *dev)
	f["device"] = dev.RealDev

	// Check if this is a MountVolume or BlockVolume
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		// Volume is a block volume, so skip the rest of the steps
		log.WithFields(f).Info("skipping staging for block access type")
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Mount Volume
	// Fetch dev mounts to check if the device is already staged
	klog.V(4).Infof("NodeStageVolume: Fetching device mounts")
	mnts, err := gofsutil.GetDevMounts(context.Background(), dev.RealDev)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"could not reliably determine existing mount status: %q",
			err.Error())
	}

	if len(mnts) == 0 {
		// Device isn't mounted anywhere, stage the volume
		// If access mode is read-only, we don't allow formatting
		if params.ro {
			klog.V(4).Infof("NodeStageVolume: Mounting %q at %q in read-only mode with mount flags %v",
				dev.FullPath, params.stagingTarget, params.mntFlags)
			params.mntFlags = append(params.mntFlags, "ro")
			if err := gofsutil.Mount(ctx, dev.FullPath, params.stagingTarget, params.fsType, params.mntFlags...); err != nil {
				return nil, status.Errorf(codes.Internal,
					"error with mount during staging: %q",
					err.Error())
			}
			klog.V(4).Infof("NodeStageVolume: Device mounted successfully at %q", params.stagingTarget)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		// Format and mount the device
		klog.V(4).Infof("NodeStageVolume: Format and mount the device %q at %q with mount flags %v",
			dev.FullPath, params.stagingTarget, params.mntFlags)
		if err := gofsutil.FormatAndMount(ctx, dev.FullPath, params.stagingTarget, params.fsType, params.mntFlags...); err != nil {
			return nil, status.Errorf(codes.Internal,
				"error with format and mount during staging: %q",
				err.Error())
		}
		klog.V(4).Infof("NodeStageVolume: Device mounted successfully at %q", params.stagingTarget)
		return &csi.NodeStageVolumeResponse{}, nil
	}
	// If Device is already mounted. Need to ensure that it is already
	// mounted to the expected staging target, with correct rw/ro perms
	klog.V(4).Infof("NodeStageVolume: Device already mounted. Checking mount flags %v for correctness.",
		params.mntFlags)
	mounted := false
	for _, m := range mnts {
		if m.Path == params.stagingTarget {
			mounted = true
			rwo := "rw"
			if params.ro {
				rwo = "ro"
			}
			klog.V(4).Infof("NodeStageVolume: Checking for mount options %v", m.Opts)
			if contains(m.Opts, rwo) {
				//TODO make sure that all the mount options match
				klog.V(4).Infof("NodeStageVolume: Device already mounted at %q with mount option %q",
					params.stagingTarget, rwo)
				return &csi.NodeStageVolumeResponse{}, nil
			}
			return nil, status.Error(codes.AlreadyExists,
				"access mode conflicts with existing mount")
		}
	}
	if !mounted {
		return nil, status.Error(codes.Internal,
			"device already in use and mounted elsewhere")
	}
	return nil, nil
}

func (s *service) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	klog.V(4).Infof("NodeUnstageVolume: called with args %+v", *req)

	stagingTarget := req.GetStagingTargetPath()
	// Fetch all the mount points
	mnts, err := gofsutil.GetMounts(context.Background())
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"could not retrieve existing mount points: %q",
			err.Error())
	}
	klog.V(4).Infof("NodeUnstageVolume: node mounts %+v", mnts)
	// Figure out if the target path is present in mounts or not - Unstage is not required for file volumes
	// NOTE: Raw block support might get broken due to this, will be fixed later
	targetFound := common.IsTargetInMounts(stagingTarget, mnts)
	if !targetFound {
		klog.V(4).Infof("NodeUnstageVolume: Target path %q is not mounted. Skipping unstage.", stagingTarget)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	volID := req.GetVolumeId()
	dirExists, err := verifyTargetDir(stagingTarget, false)
	if err != nil {
		return nil, err
	}
	// This will take care of idempotent requests
	if !dirExists {
		klog.V(4).Infof("NodeUnstageVolume: Target path %q does not exist. Assuming unstage is complete.", stagingTarget)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Block volume
	isMounted, err := isBlockVolumeMounted(volID, stagingTarget)
	if err != nil {
		return nil, err
	}

	// Volume is still mounted. Unstage the volume
	if isMounted {
		klog.V(4).Infof("Attempting to unmount target %q for volume %q", stagingTarget, volID)
		if err := gofsutil.Unmount(context.Background(), stagingTarget); err != nil {
			return nil, status.Errorf(codes.Internal,
				"Error unmounting stagingTarget: %s", err.Error())
		}
		klog.V(4).Infof("Unmount successful for target %q for volume %q", stagingTarget, volID)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// isBlockVolumeMounted checks if the block volume is properly mounted or not.
// If yes, then the calling function proceeds to unmount the volume
func isBlockVolumeMounted(
	volID string,
	stagingTargetPath string) (
	bool, error) {

	// Look up block device mounted to target
	// BlockVolume: Here we are relying on the fact that the CO is required to
	// have created the staging path per the spec, even for BlockVolumes. Even
	// though we don't use the staging path for block, the fact nothing will be
	// mounted still indicates that unstaging is done.
	dev, err := getDevFromMount(stagingTargetPath)
	if err != nil {
		return false, status.Errorf(codes.Internal,
			"isBlockVolumeMounted: error getting block device for volume: %s, err: %s",
			volID, err.Error())
	}

	// No device found
	if dev == nil {
		// Nothing is mounted, so unstaging is already done
		klog.V(4).Infof("isBlockVolumeMounted: No device found. Assuming Unstage is "+
			"already done for volume %q and target path %q", volID, stagingTargetPath)
		// For raw block volumes, we do not get a device attached to the stagingTargetPath. This path is just a dummy.
		return false, nil
	}

	f := log.Fields{
		"volID":  volID,
		"path":   dev.FullPath,
		"block":  dev.RealDev,
		"target": stagingTargetPath,
	}
	log.WithFields(f).Debug("found device")

	// Get mounts for device
	mnts, err := gofsutil.GetDevMounts(context.Background(), dev.RealDev)
	if err != nil {
		return false, status.Errorf(codes.Internal,
			"isBlockVolumeMounted: could not reliably determine existing mount status: %s",
			err.Error())
	}

	// device is mounted more than once. Should only be mounted to target
	if len(mnts) > 1 {
		return false, status.Errorf(codes.Internal,
			"isBlockVolumeMounted: volume: %s appears mounted in multiple places", volID)
	}

	// Since we looked up the block volume from the target path, we assume that
	// the one existing mount is from the block to the target
	klog.V(4).Infof("isBlockVolumeMounted: Found single mount point for volume %q and target %q",
		volID, stagingTargetPath)
	return true, nil
}

func (s *service) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {

	klog.V(4).Infof("NodePublishVolume: called with args %+v", *req)
	var err error
	params := nodePublishParams{
		volID:  req.GetVolumeId(),
		target: req.GetTargetPath(),
		ro:     req.GetReadonly(),
	}
	// TODO: Verify if volume exists and return a NotFound error in negative scenario

	params.stagingTarget = req.GetStagingTargetPath()
	if params.stagingTarget == "" {
		return nil, status.Error(codes.FailedPrecondition, "Staging Target Path not set")
	}

	// Check if this is a MountVolume or BlockVolume
	volCap := req.GetVolumeCapability()
	if !common.IsFileVolumeRequest([]*csi.VolumeCapability{volCap}) {
		params.diskID, err = getDiskID(req.GetPublishContext())
		if err != nil {
			return nil, err
		}

		klog.V(4).Infof("Checking if volume %q is attached to disk %q", params.volID, params.diskID)
		volPath, err := verifyVolumeAttached(params.diskID)
		if err != nil {
			return nil, err
		}

		// Get underlying block device
		dev, err := getDevice(volPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal,
				"error getting block device for volume: %q, err: %q",
				params.volID, err.Error())
		}
		params.volumePath = dev.FullPath
		params.device = dev.RealDev

		// check for Block vs Mount
		if _, ok := volCap.GetAccessType().(*csi.VolumeCapability_Block); ok {
			// bind mount device to target
			return publishBlockVol(ctx, req, dev, params)
		}
		// Volume must be a mount volume
		return publishMountVol(ctx, req, dev, params)
	}
	// Volume must be a file share
	return publishFileVol(ctx, req, params)
}

func (s *service) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (
	*csi.NodeUnpublishVolumeResponse, error) {
	klog.V(4).Infof("NodeUnpublishVolume: called with args %+v", *req)

	volID := req.GetVolumeId()
	target := req.GetTargetPath()

	// Verify if the path exists
	// NOTE: For raw block volumes, this path is a file. In all other cases, it is a directory
	_, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			// target path does not exist, so we must be Unpublished
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal,
			"failed to stat target, err: %q", err.Error())
	}

	// Fetch all the mount points
	mnts, err := gofsutil.GetMounts(context.Background())
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"could not retrieve existing mount points: %q",
			err.Error())
	}
	klog.V(4).Infof("NodeUnpublishVolume: node mounts %+v", mnts)

	// Check if the volume is already unpublished
	// Also validates if path is a mounted directory or not
	isPresent := common.IsTargetInMounts(target, mnts)
	if !isPresent {
		klog.V(4).Infof("Target %s not present in mount points. Assuming it is already unpublished.", target)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Figure out if the target path is a file or block volume
	isFileMount, _ := common.IsFileVolumeMount(target, mnts)
	isPublished := true
	if !isFileMount {
		isPublished, err = isBlockVolumePublished(volID, target)
		if err != nil {
			return nil, err
		}
	}

	if isPublished {
		klog.V(4).Infof("Attempting to unmount target %q for volume %q", target, volID)
		if err := gofsutil.Unmount(ctx, target); err != nil {
			mssg := fmt.Sprintf("Error unmounting target %q for volume %q. %q", target, volID, err.Error())
			klog.V(4).Info(mssg)
			return nil, status.Error(codes.Internal, mssg)
		}
		klog.V(4).Infof("Unmount successful for target %q for volume %q", target, volID)
		// TODO Use a go routine here. The deletion of target path might not be a good reason to error out
		// The SP is supposed to delete the files/directory it created in this target path
		if err := rmpath(target); err != nil {
			klog.V(4).Infof("Failed to delete the target path %q", target)
			return nil, err
		}
		klog.V(4).Infof("Target path  %q successfully deleted", target)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// isBlockVolumePublished checks if the device backing block volume exists.
func isBlockVolumePublished(
	volID string,
	target string) (
	bool, error) {
	// Look up block device mounted to target
	dev, err := getDevFromMount(target)
	if err != nil {
		return false, status.Errorf(codes.Internal,
			"error getting block device for volume: %s, err: %s",
			volID, err.Error())
	}

	if dev == nil {
		// Nothing is mounted, so unpublish is already done. However, we also know
		// that the target path exists, and it is our job to remove it.
		klog.V(4).Infof("isBlockVolumePublished: No device found. Assuming Unpublish is "+
			"already complete for volume %q and target path %q", volID, target)
		if err := rmpath(target); err != nil {
			mssg := fmt.Sprintf("Failed to delete the target path %q. Error: %q", target, err.Error())
			klog.V(4).Info(mssg)
			return false, status.Errorf(codes.Internal, mssg)
		}
		klog.V(4).Infof("isBlockVolumePublished: Target path %q successfully deleted", target)
		return false, nil
	}
	return true, nil
}

func (s *service) NodeGetVolumeStats(
	ctx context.Context,
	req *csi.NodeGetVolumeStatsRequest) (
	*csi.NodeGetVolumeStatsResponse, error) {

	return nil, nil
}

func (s *service) NodeGetCapabilities(
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
		},
	}, nil
}

/*
	NodeGetInfo RPC returns the NodeGetInfoResponse with mandatory fields `NodeId` and `AccessibleTopology`.
	However, for sending `MaxVolumesPerNode` in the response, it is not straight forward since vSphere CSI
	driver supports both block and file volume. For block volume, max volumes to be attached is deterministic
	by inspecting SCSI controllers of the VM, but for file volume, this is not deterministic.
	We can not set this limit on MaxVolumesPerNode, since single driver is used for both block and file volumes.
*/
func (s *service) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest) (
	*csi.NodeGetInfoResponse, error) {
	nodeId := os.Getenv("NODE_NAME")
	if nodeId == "" {
		return nil, status.Error(codes.Internal, "ENV NODE_NAME is not set")
	}
	if cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor)) == cnstypes.CnsClusterFlavorGuest {
		return &csi.NodeGetInfoResponse{
			NodeId:             nodeId,
			AccessibleTopology: &csi.Topology{},
		}, nil
	}
	var cfg *cnsconfig.Config
	cfgPath = csictx.Getenv(ctx, cnsconfig.EnvCloudConfig)
	if cfgPath == "" {
		cfgPath = cnsconfig.DefaultCloudConfigPath
	}
	cfg, err := cnsconfig.GetCnsconfig(cfgPath)
	if err != nil {
		klog.Errorf("Failed to read cnsconfig. Error: %v", err)
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	var accessibleTopology map[string]string
	topology := &csi.Topology{}

	if cfg.Labels.Zone != "" && cfg.Labels.Region != "" {
		vcenterconfig, err := cnsvsphere.GetVirtualCenterConfig(cfg)
		if err != nil {
			klog.Errorf("Failed to get VirtualCenterConfig from cns config. err=%v", err)
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		vcManager := cnsvsphere.GetVirtualCenterManager()
		vcenter, err := vcManager.RegisterVirtualCenter(vcenterconfig)
		if err != nil {
			klog.Errorf("Failed to register vcenter with virtualCenterManager.")
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		defer vcManager.UnregisterAllVirtualCenters()
		//Connect to vCenter
		err = vcenter.Connect(ctx)
		if err != nil {
			klog.Errorf("Failed to connect to vcenter host: %s. err=%v", vcenter.Config.Host, err)
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		// Get VM UUID
		uuid, err := getSystemUUID()
		if err != nil {
			klog.Errorf("Failed to get system uuid for node VM")
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		klog.V(4).Infof("Successfully retrieved uuid:%s  from the node: %s", uuid, nodeId)
		nodeVM, err := cnsvsphere.GetVirtualMachineByUUID(uuid, false)
		if err != nil || nodeVM == nil {
			klog.Errorf("Failed to get nodeVM for uuid: %s. err: %+v", uuid, err)
			uuid, err = convertUUID(uuid)
			if err != nil {
				klog.Errorf("convertUUID failed with error: %v", err)
				return nil, status.Errorf(codes.Internal, err.Error())
			}
			nodeVM, err = cnsvsphere.GetVirtualMachineByUUID(uuid, false)
			if err != nil || nodeVM == nil {
				klog.Errorf("Failed to get nodeVM for uuid: %s. err: %+v", uuid, err)
				return nil, status.Errorf(codes.Internal, err.Error())
			}
		}
		zone, region, err := nodeVM.GetZoneRegion(ctx, cfg.Labels.Zone, cfg.Labels.Region)
		if err != nil {
			klog.Errorf("Failed to get accessibleTopology for vm: %v, err: %v", nodeVM.Reference(), err)
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		klog.V(4).Infof("zone: [%s], region: [%s], Node VM: [%s]", zone, region, nodeId)
		if zone != "" && region != "" {
			accessibleTopology = make(map[string]string)
			accessibleTopology[csitypes.LabelRegionFailureDomain] = region
			accessibleTopology[csitypes.LabelZoneFailureDomain] = zone
		}
	}
	if len(accessibleTopology) > 0 {
		topology.Segments = accessibleTopology
	}

	return &csi.NodeGetInfoResponse{
		NodeId:             nodeId,
		AccessibleTopology: topology,
	}, nil
}

func (s *service) NodeExpandVolume(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest) (
	*csi.NodeExpandVolumeResponse, error) {
	klog.V(4).Infof("NodeExpandVolume: called with args %+v", *req)

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id must be provided")
	} else if req.GetCapacityRange() == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity range must be provided")
	} else if req.GetCapacityRange().GetRequiredBytes() < 0 || req.GetCapacityRange().GetLimitBytes() < 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity ranges values cannot be negative")
	}

	volSizeBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	volSizeMB := int64(common.RoundUpSize(volSizeBytes, common.MbInBytes))

	// TODO(xyang): In CSI spec 1.2, NodeExpandVolume will be
	// passing in a staging_target_path which is more precise
	// than volume_path. Use the new staging_target_path
	// instead of the volume_path when it is supported by Kubernetes.

	volumePath := req.GetVolumePath()
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume path must be provided to expand volume on node")
	}

	// Look up block device mounted to staging target path
	dev, err := getDevFromMount(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"error getting block device for volume: %q, err: %s",
			volumeID, err.Error())
	} else if dev == nil {
		return nil, status.Errorf(codes.Internal,
			"volume %q is not mounted at the path %s",
			volumeID, volumePath)
	}
	klog.V(4).Infof("NodeExpandVolume: staging target path %s, getDevFromMount %+v", volumePath, *dev)

	realMounter := mount.New("")
	realExec := mount.NewOsExec()
	mounter := &mount.SafeFormatAndMount{
		Interface: realMounter,
		Exec:      realExec,
	}
	resizer := resizefs.NewResizeFs(mounter)
	_, err = resizer.Resize(dev.RealDev, volumePath)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error when resizing filesystem on volume %q on node: %v", volumeID, err))
	}
	klog.V(4).Infof("NodeExpandVolume: Resized filesystem with devicePath %s volumePath %s", dev.RealDev, volumePath)

	// Check the block size
	gotBlockSizeBytes, err := getBlockSizeBytes(mounter, dev.RealDev)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("error when getting size of block volume at path %s: %v", dev.RealDev, err))
	}
	// NOTE(xyang): Make sure new size is greater than or equal to the
	// requested size. It is possible for volume size to be rounded up
	// and therefore bigger than the requested size.
	if gotBlockSizeBytes < volSizeBytes {
		return nil, status.Errorf(codes.Internal, "requested volume size was %v, but got volume with size %v", volSizeBytes, gotBlockSizeBytes)
	}

	klog.V(4).Infof("NodeExpandVolume: expanded volume successfully. devicePath %s volumePath %s size %d", dev.RealDev, volumePath, int64(units.FileSize(volSizeMB*common.MbInBytes)))

	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: int64(units.FileSize(volSizeMB * common.MbInBytes)),
	}, nil
}

func getBlockSizeBytes(mounter *mount.SafeFormatAndMount, devicePath string) (int64, error) {
	output, err := mounter.Exec.Run("blockdev", "--getsize64", devicePath)
	if err != nil {
		return -1, fmt.Errorf("error when getting size of block volume at path %s: output: %s, err: %v", devicePath, string(output), err)
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
	klog.V(4).Infof("PublishMountVolume called with args: %+v", params)

	// Extract fs details
	_, mntFlags, err := ensureMountVol(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}

	// We are responsible for creating target dir, per spec, if not already present
	_, err = mkdir(params.target)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"Unable to create target dir: %q, err: %v", params.target, err)
	}
	klog.V(4).Infof("PublishMountVolume: Created target path %q", params.target)

	// Verify if the Staging path already exists
	if _, err := verifyTargetDir(params.stagingTarget, true); err != nil {
		return nil, err
	}

	// get block device mounts
	// Check if device is already mounted
	devMnts, err := getDevMounts(dev)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"could not reliably determine existing mount status: %q",
			err.Error())
	}
	klog.V(4).Infof("publishMountVol: device %+v, device mounts %q", *dev, devMnts)

	// We expect that block device is already staged, so there should be at least 1
	// mount already. if it's > 1, it may already be published
	if len(devMnts) > 1 {
		// check if publish is already there
		for _, m := range devMnts {
			if m.Path == params.target {
				// volume already published to target
				// if mount options look good, do nothing
				rwo := "rw"
				if params.ro {
					rwo = "ro"
				}
				if !contains(m.Opts, rwo) {
					//TODO make sure that all the mount options match
					return nil, status.Error(codes.AlreadyExists,
						"volume previously published with different options")
				}

				// Existing mount satisfies request
				log.Infof("Volume already published to target. Parameters: [%+v]", params)
				return &csi.NodePublishVolumeResponse{}, nil
			}
		}
	} else if len(devMnts) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"Volume ID: %q does not appear staged to %q", req.GetVolumeId(), params.stagingTarget)
	}

	// Do the bind mount to publish the volume
	if params.ro {
		mntFlags = append(mntFlags, "ro")
	}
	klog.V(4).Infof("PublishMountVolume: Attempting to bind mount %q to %q with mount flags %v",
		params.stagingTarget, params.target, mntFlags)
	if err := gofsutil.BindMount(ctx, params.stagingTarget, params.target, mntFlags...); err != nil {
		return nil, status.Errorf(codes.Internal,
			"error publish volume to target path: %q",
			err.Error())
	}
	klog.V(4).Infof("PublishMountVolume: Bind mount successful to path %q", params.target)
	return &csi.NodePublishVolumeResponse{}, nil
}

func publishBlockVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	dev *Device,
	params nodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	klog.V(4).Infof("PublishBlockVolume called with args: %+v", params)

	// We are responsible for creating target file, per spec, if not already present
	_, err := mkfile(params.target)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"Unable to create target file: %q, err: %v", params.target, err)
	}
	klog.V(4).Infof("publishBlockVol: Target %q created", params.target)

	// Read-only is not supported for BlockVolume. Doing a read-only
	// bind mount of the device to the target path does not prevent
	// the underlying block device from being modified, so don't
	// advertise a false sense of security
	if params.ro {
		return nil, status.Error(codes.InvalidArgument,
			"read only not supported for Block Volume")
	}

	// get block device mounts
	devMnts, err := getDevMounts(dev)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"could not reliably determine existing mount status: %q",
			err.Error())
	}
	klog.V(4).Infof("publishBlockVol: device %+v, device mounts %q", *dev, devMnts)

	// check if device is already mounted
	if len(devMnts) == 0 {
		// do the bind mount
		mntFlags := make([]string, 0)
		klog.V(4).Infof("PublishBlockVolume: Attempting to bind mount %q to %q with mount flags %v",
			dev.FullPath, params.target, mntFlags)
		if err := gofsutil.BindMount(ctx, dev.FullPath, params.target, mntFlags...); err != nil {
			return nil, status.Errorf(codes.Internal,
				"error publish volume to target path: %q",
				err.Error())
		}
		klog.V(4).Infof("PublishBlockVolume: Bind mount successful to path %q", params.target)
	} else if len(devMnts) == 1 {
		// already mounted, make sure it's what we want
		if devMnts[0].Path != params.target {
			return nil, status.Error(codes.Internal,
				"device already in use and mounted elsewhere")
		}
		log.Infof("Volume already published to target. Parameters: [%+v]", params)
	} else {
		return nil, status.Error(codes.AlreadyExists,
			"block volume already mounted in more than one place")
	}
	klog.V(4).Infof("PublishBlockVolume successful at path %q", params.target)
	// existing or new mount satisfies request
	return &csi.NodePublishVolumeResponse{}, nil
}

func publishFileVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	params nodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	klog.V(4).Infof("PublishFileVolume called with args: %+v", params)

	// Extract mount details
	fsType, mntFlags, err := ensureMountVol(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}

	// We are responsible for creating target dir, per spec, if not already present
	_, err = mkdir(params.target)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"Unable to create target dir: %q, err: %v", params.target, err)
	}
	klog.V(4).Infof("PublishFileVolume: Created target path %q", params.target)

	// Check if target already mounted
	mnts, err := gofsutil.GetMounts(context.Background())
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"could not retrieve existing mount points: %q",
			err.Error())
	}
	klog.V(4).Infof("PublishFileVolume: Mounts - %+v", mnts)
	for _, m := range mnts {
		if m.Path == params.target {
			// volume already published to target
			// if mount options look good, do nothing
			rwo := "rw"
			if params.ro {
				rwo = "ro"
			}
			if !contains(m.Opts, rwo) {
				//TODO make sure that all the mount options match
				return nil, status.Error(codes.AlreadyExists,
					"volume previously published with different options")
			}

			// Existing mount satisfies request
			klog.V(4).Infof("Volume already published to target %q. Parameters: [%+v]", params.target, params)
			return &csi.NodePublishVolumeResponse{}, nil
		}
	}

	// Check for read-only flag on Pod pvc spec
	if params.ro {
		mntFlags = append(mntFlags, "ro")
	}
	// Retrieve the file share access point from publish context
	mntSrc, ok := req.GetPublishContext()[common.Nfsv4AccessPoint]
	if !ok {
		return nil, status.Error(codes.Internal, "NFSv4 accesspoint not set in publish context")
	}
	// Directly mount the file share volume to the pod. No bind mount required.
	klog.V(4).Infof("PublishFileVolume: Attempting to mount %q to %q with fstype %q and mountflags %v",
		mntSrc, params.target, fsType, mntFlags)
	if err := gofsutil.Mount(ctx, mntSrc, params.target, fsType, mntFlags...); err != nil {
		return nil, status.Errorf(codes.Internal,
			"error publish volume to target path: %q",
			err.Error())
	}
	klog.V(4).Infof("PublishFileVolume: Mount successful to path %q", params.target)
	return &csi.NodePublishVolumeResponse{}, nil
}

// Device is a struct for holding details about a block device
type Device struct {
	FullPath string
	Name     string
	RealDev  string
}

// getDevice returns a Device struct with info about the given device, or
// an error if it doesn't exist or is not a block device
func getDevice(path string) (*Device, error) {

	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	// eval any symlinks and make sure it points to a device
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

// The files parameter is optional for testing purposes
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

func verifyVolumeAttached(diskID string) (string, error) {

	// Check that volume is attached
	volPath, err := getDiskPath(diskID, nil)
	if err != nil {
		return "", status.Errorf(codes.Internal,
			"Error trying to read attached disks: %v", err)
	}
	if volPath == "" {
		return "", status.Errorf(codes.NotFound,
			"disk: %s not attached to node", diskID)
	}

	log.WithField("diskID", diskID).WithField("path", volPath).Debug("found disk")

	return volPath, nil
}

// verifyTargetDir checks if the target path is not empty, exists and is a directory
// if targetShouldExist is set to false, then verifyTargetDir returns (false, nil) if the path does not exist.
// if targetShouldExist is set to true, then verifyTargetDir returns (false, err) if the path does not exist.
func verifyTargetDir(target string, targetShouldExist bool) (bool, error) {
	if target == "" {
		return false, status.Error(codes.InvalidArgument,
			"target path required")
	}

	tgtStat, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			if targetShouldExist {
				// target path does not exist but targetShouldExist is set to true
				return false, status.Errorf(codes.FailedPrecondition,
					"target: %s not pre-created", target)
			}
			// target path does not exist but targetShouldExist is set to false, so no error
			return false, nil
		}
		return false, status.Errorf(codes.Internal,
			"failed to stat target, err: %s", err.Error())
	}

	// This check is mandated by the spec, but this would/should fail if the
	// volume has a block accessType as we get a file for raw block volumes
	// during NodePublish/Unpublish. Do not use this function for Publish/Unpublish
	if !tgtStat.IsDir() {
		return false, status.Errorf(codes.FailedPrecondition,
			"existing path: %s is not a directory", target)
	}

	klog.V(4).Infof("Target path %s verification complete", target)
	return true, nil
}

// mkdir creates the directory specified by path if needed.
// return pair is a bool flag of whether dir was created, and an error
func mkdir(path string) (bool, error) {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(path, 0750); err != nil {
				log.WithField("dir", path).WithError(
					err).Error("Unable to create dir")
				return false, err
			}
			log.WithField("path", path).Debug("created directory")
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
// return pair is a bool flag of whether file was created, and an error
func mkfile(path string) (bool, error) {
	st, err := os.Stat(path)
	if os.IsNotExist(err) {
		file, err := os.OpenFile(path, os.O_CREATE, 0755)
		if err != nil {
			log.WithField("dir", path).WithError(
				err).Error("Unable to create dir")
			return false, err
		}
		file.Close()
		log.WithField("path", path).Debug("created file")
		return true, nil
	}
	if st.IsDir() {
		return false, fmt.Errorf("existing path is a directory")
	}
	return false, nil
}

// rmpath removes the given target path, whether it is a file or a directory
// for directories, an error is returned if the dir is not empty
func rmpath(target string) error {
	// target should be empty
	log.WithField("path", target).Debug("removing target path")
	if err := os.Remove(target); err != nil {
		return status.Errorf(codes.Internal,
			"Unable to remove target path: %s, err: %v", target, err)
	}
	return nil
}

func ensureMountVol(volCap *csi.VolumeCapability) (string, []string, error) {
	mountVol := volCap.GetMount()
	if mountVol == nil {
		return "", nil, status.Error(codes.InvalidArgument,
			"access type missing")
	}
	fs := common.GetVolumeCapabilityFsType(volCap)
	mntFlags := mountVol.GetMountFlags()

	return fs, mntFlags, nil
}

// a wrapper around gofsutil.GetMounts that handles bind mounts
func getDevMounts(
	sysDevice *Device) ([]gofsutil.Info, error) {

	ctx := context.Background()
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

func getSystemUUID() (string, error) {
	idb, err := ioutil.ReadFile(path.Join(dmiDir, "id", "product_uuid"))
	if err != nil {
		return "", err
	}
	klog.V(4).Infof("uuid in bytes: %v", idb)
	id := strings.TrimSpace(string(idb))
	klog.V(4).Infof("uuid in string: %s", id)
	return strings.ToLower(id), nil
}

// convertUUID helps convert UUID to vSphere format
//input uuid:    6B8C2042-0DD1-D037-156F-435F999D94C1
//returned uuid: 42208c6b-d10d-37d0-156f-435f999d94c1
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

func getDiskID(pubCtx map[string]string) (string, error) {
	var diskID string
	var ok bool
	if diskID, ok = pubCtx[common.AttributeFirstClassDiskUUID]; !ok {
		return "", status.Errorf(codes.InvalidArgument,
			"Attribute: %s required in publish context",
			common.AttributeFirstClassDiskUUID)
	}
	return diskID, nil
}

func getDevFromMount(target string) (*Device, error) {

	// Get list of all mounts on system
	mnts, err := gofsutil.GetMounts(context.Background())
	if err != nil {
		return nil, err
	}

	// example for RAW block device
	// Device:udev
	// Path:/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/pvc-098a7585-109c-11ea-94c1-005056825b1f
	// Source:/dev/sdb
	// Type:devtmpfs
	// Opts:[rw relatime]}

	// example for Mounted block device
	// Device:/dev/sdb
	// Path:/var/lib/kubelet/pods/c46d6473-0810-11ea-94c1-005056825b1f/volumes/kubernetes.io~csi/pvc-9e3d1d08-080f-11ea-be93-005056825b1f/mount
	// Source:/var/lib/kubelet/plugins/kubernetes.io/csi/pv/pvc-9e3d1d08-080f-11ea-be93-005056825b1f/globalmount
	// Type:ext4
	// Opts:[rw relatime]

	for _, m := range mnts {
		if m.Path == target {
			// something is mounted to target, get underlying disk
			d := m.Device
			if m.Device == "udev" {
				d = m.Source
			}
			dev, err := getDevice(d)
			if err != nil {
				return nil, err
			}
			return dev, nil
		}
	}

	// Did not identify a device mounted to target
	return nil, nil
}
