//go:build windows
// +build windows

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
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svol "k8s.io/kubernetes/pkg/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/mounter"
)

const (
	UUIDPrefix = "VMware-"
)

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

func GetMounter(ctx context.Context, osUtils *OsUtils) (mounter.CSIProxyMounter, error) {
	if proxy, ok := osUtils.Mounter.Interface.(mounter.CSIProxyMounter); ok {
		return proxy, nil
	} else {
		return nil, fmt.Errorf("could not cast to csi proxy class")
	}
}

// NodeStageBlockVolume mounts mount volume or file volume to staging target
func (osUtils *OsUtils) NodeStageBlockVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest,
	params NodeStageParams) (
	*csi.NodeStageVolumeResponse, error) {

	log := logger.GetLogger(ctx)
	log.Debug("start nodeStageBlockVolume")

	// For windows NodeStageVolumeRequest comes like {VolumeId:b03f0b6e-cf29-4b98-9411-5168682ace82
	// PublishContext:map[diskUUID:6000c29a98d05e384a43f0ef189aaf5a type:vSphere CNS Block Volume]
	// StagingTargetPath:\var\lib\kubelet\plugins\kubernetes.io\csi\pv\pvc-baed6335-cc2c-44d8-a324-2ee00be0644d\globalmount
	// VolumeCapability:mount:<fs_type:"ext4" > access_mode:<mode:SINGLE_NODE_WRITER >
	// Secrets:map[] VolumeContext:map[storage.kubernetes.io/csiProvisionerIdentity:1632464002195-8081-csi.vsphere.vmware.com
	// type:vSphere CNS Block Volume] XXX_NoUnkeyedLiteral:{} XXX_unrecognized:[] XXX_sizecache:0}
	// Note: fs_type comes as ext4 if not specified in storage class else it takes value from storage class

	// Check if this is a MountVolume or BlockVolume.
	if _, ok := req.GetVolumeCapability().GetAccessType().(*csi.VolumeCapability_Block); ok {
		// Volume is a raw block volume.
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"Stage for raw block Volume access type is currently not supported for windows node")
	}

	// Block Volume with Mount access type.
	pubCtx := req.GetPublishContext()
	stagingTargetPath := req.GetStagingTargetPath()
	diskID, err := osUtils.GetDiskID(pubCtx, log)
	if err != nil {
		return nil, err
	}
	log.Infof("nodeStageBlockVolume: Retrieved diskID as %q", diskID)

	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return nil, err
	}
	// Get the windows specific disk number
	diskNumber, err := mounter.GetDiskNumber(ctx, diskID)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed to get Disk Number, err: %v", err)
	}
	log.Infof("nodeStageBlockVolume diskNumber %s, diskId %s,stagingTargetPath %s ", diskNumber, diskID, stagingTargetPath)

	mounted, err := osUtils.haveMountPoint(ctx, stagingTargetPath)
	if err != nil {
		return nil, err
	}
	if !mounted {
		log.Info("calling FormatAndMount")
		//currently proxy does not support read only mount or filesystems other than ntfs
		err := mounter.FormatAndMount(ctx, diskNumber, stagingTargetPath, params.FsType, params.MntFlags)
		if err != nil {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"error mounting volume. Parameters: %v err: %v", params, err)
		}
		log.Infof("nodeStageBlockVolume: Device mounted successfully at %q", params.StagingTarget)
	} else {
		log.Infof("nodeStageBlockVolume: Device already mounted.")
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (osUtils *OsUtils) haveMountPoint(ctx context.Context, target string) (bool, error) {
	log := logger.GetLogger(ctx)
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return false, err
	}
	// check if disk is mounted and formatted correctly, here the path should exist as it is created by CO and already checked
	notMounted, err := mounter.IsLikelyNotMountPoint(target)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, logger.LogNewErrorCodef(log, codes.Internal,
			"Could not determine if staging path is already mounted, err: %v", err)
	}
	if notMounted {
		return false, nil
	}
	// testing original mount point, make sure the mount link is valid
	if _, err := os.ReadDir(target); err != nil {
		// mount link is invalid, now unmount and remount later
		log.Warnf("haveMountPoint: ReadDir %s failed with %v, unmounting", target, err)
		if err := mounter.Unmount(target); err != nil {
			log.Errorf("haveMountPoint: Unmount directory %s failed with %v", target, err)
			return true, err
		}
		return false, nil
	}
	return true, nil
}

// CleanupStagePath will unmount the volume from node and remove the stage directory
func (osUtils *OsUtils) CleanupStagePath(ctx context.Context, stagingTarget string, volID string) error {
	log := logger.GetLogger(ctx)

	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return err
	}

	// unmount Block volume.
	log.Infof("Attempting to unmount target %q for volume %q", stagingTarget, volID)
	err = mounter.Unmount(stagingTarget)
	if err != nil {
		return fmt.Errorf(
			"error unmounting stagingTarget: %v", err)
	}
	return nil
}

// CleanupPublishPath will unmount and remove publish path
func (osUtils *OsUtils) CleanupPublishPath(ctx context.Context, target string, volID string) error {
	//log := logger.GetLogger(ctx)
	// for windows, unpublish means removing symlink only
	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return err
	}
	// no need to check if target exist first as rmdir do not throw error if path does not exists.
	err = mounter.Rmdir(ctx, target)
	if err != nil {
		return fmt.Errorf(
			"error unmounting publishTarget: %v", err)
	}
	return nil
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

	source := req.GetStagingTargetPath()

	target := req.GetTargetPath()

	err := osUtils.PreparePublishPath(ctx, target)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"Target path could not be prepared: %v", err)
	}
	mounter, err := GetMounter(ctx, osUtils)
	log.Infof("NodePublishVolume: mounting %s at %s", source, target)
	if err := mounter.Mount(source, target, "", nil); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not mount %q at %q: %v", source, target, err)
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

	return nil, logger.LogNewErrorCodef(log, codes.Internal,
		"PublishBlockVol Raw Block devices are currently not supported by windows : %v", req)
}

// PublishBlockVol mounts file volume to publish target
func (osUtils *OsUtils) PublishFileVol(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	params NodePublishParams) (
	*csi.NodePublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	return nil, logger.LogNewErrorCodef(log, codes.Internal,
		"PublishBlockVol File Volumes are currently not supported by windows : %v", req)
}

// GetMetrics helps get volume metrics using k8s fsInfo strategy.
func (osUtils *OsUtils) GetMetrics(ctx context.Context, path string) (*k8svol.Metrics, error) {
	if path == "" {
		return nil, fmt.Errorf("no path given")
	}
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return nil, err
	}
	available, capacity, usage, inodes, inodesFree, inodesUsed, err := mounter.StatFS(ctx, path)
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
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return -1, err
	}
	sizeInBytes, err := mounter.GetVolumeSizeInBytes(ctx, devicePath)

	if err != nil {
		return -1, err
	}

	return sizeInBytes, nil
}

// GetDevice returns a Device struct with info about the given device, or
// an error if it doesn't exist or is not a block device.
func (osUtils *OsUtils) GetDevice(ctx context.Context, path string) (*Device, error) {
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return nil, err
	}
	d, err := mounter.GetDeviceNameFromMount(ctx, path)
	if err != nil {
		return nil, err
	}
	return &Device{
		Name:     path,
		FullPath: path,
		RealDev:  d,
	}, nil
}

// RescanDevice rescans the device
func (osUtils *OsUtils) RescanDevice(ctx context.Context, dev *Device) error {

	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return err
	}
	return mounter.Rescan(ctx)
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
	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return false, err
	}

	isExists, err := mounter.ExistsPath(ctx, target)
	if err != nil {
		return false, err
	}
	if isExists == false {
		if targetShouldExist {
			// Target path does not exist but targetShouldExist is set to true.
			return false, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
				"target: %s not pre-created", target)
		}
		// Target path does not exist but targetShouldExist is set to false,
		// so no error.
		return false, nil
	}
	log.Debugf("Target path %s verification complete", target)
	return true, nil
}

// GetDevFromMount returns device info mounted on the target dir
func (osUtils *OsUtils) GetDevFromMount(ctx context.Context, target string) (*Device, error) {
	return osUtils.GetDevice(ctx, target)
}

// IsTargetInMounts checks if a path exists in the mounts
// this check is no op for windows
func (osUtils *OsUtils) IsTargetInMounts(ctx context.Context, path string) (bool, error) {
	return true, nil
}

// GetVolumeCapabilityFsType retrieves fstype from VolumeCapability.
// Defaults to nfs4 for file volume and ntfs for block volume when empty string
// is observed. This function also ignores default ext4 fstype supplied by
// external-provisioner when none is specified in the StorageClass
func (osUtils *OsUtils) GetVolumeCapabilityFsType(ctx context.Context,
	capability *csi.VolumeCapability) (string, error) {
	log := logger.GetLogger(ctx)
	fsType := strings.ToLower(capability.GetMount().GetFsType())
	log.Infof("FsType received from Volume Capability: %q", fsType)
	isFileVolume := common.IsFileVolumeRequest(ctx, []*csi.VolumeCapability{capability})
	if isFileVolume {
		// Volumes with RWM or ROM access modes are not supported on Windows
		return "", logger.LogNewErrorCode(log, codes.FailedPrecondition,
			"vSAN file service volume can not be mounted on windows node")
	}

	// On Windows we only support ntfs filesystem. External-provisioner sets default fstype as ext4
	// when none is specified in StorageClass, hence overwrite it to ntfs while mounting the volume.
	if fsType == common.NTFSFsType {
		return fsType, nil
	} else if fsType == "" || fsType == "ext4" {
		log.Infof("replacing fsType: %q received from volume "+
			"capability with %q", fsType, common.NTFSFsType)
		return common.NTFSFsType, nil
	} else {
		return "", logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"unsupported fsType %q observed", fsType)
	}
}

// ResizeVolume resizes the volume
func (osUtils *OsUtils) ResizeVolume(ctx context.Context, devicePath, volumePath string, reqVolSizeBytes int64) error {
	log := logger.GetLogger(ctx)
	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return err
	}
	log.Infof("resizing using csi proxy, devicePath %s", devicePath)

	// Check the block size.
	currentBlockSizeBytes, err := mounter.GetDiskTotalBytes(ctx, devicePath)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"error when getting size of block volume at path %s: %v", devicePath, err)
	}
	// For windows volume size is less than the disk size by some mb.
	if reqVolSizeBytes-currentBlockSizeBytes < 1024*1024 {
		log.Infof("minimum extent size is 1 MB. Skip resize for volume %s from currentBytes=%d to wantedBytes=%d ",
			devicePath, currentBlockSizeBytes, reqVolSizeBytes)
		return nil
	}
	err = mounter.ResizeVolume(ctx, devicePath, reqVolSizeBytes)
	if err != nil {
		return fmt.Errorf(
			"error when resizing filesystem on devicePath %s and volumePath %s, err: %v ", devicePath, volumePath, err)
	}
	return nil
}

// VerifyVolumeAttachedFillParams is a noop for windows
func (osUtils *OsUtils) VerifyVolumeAttachedAndFillParams(ctx context.Context,
	pubCtx map[string]string, params *NodePublishParams, dev **Device) error {
	log := logger.GetLogger(ctx)
	log.Debugf("VerifyVolumeAttachedAndFillParams Not Implemented for windows")
	return nil
}

// preparePublishPath - In case of windows, the publish code path creates a soft link
// from global stage path to the publish path. But kubelet creates the directory in advance.
// We work around this issue by deleting the publish path then recreating the link.
func (osUtils *OsUtils) PreparePublishPath(ctx context.Context, path string) error {
	log := logger.GetLogger(ctx)
	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return err
	}
	isExists, err := mounter.ExistsPath(ctx, path)
	if err != nil {
		return err
	}
	if isExists {
		log.Infof("Removing path: %s", path)
		if err = mounter.Rmdir(ctx, path); err != nil {
			return err
		}
	}
	// ensure parent dir is created
	parentDir := filepath.Dir(path)
	if err := mounter.MakeDir(ctx, parentDir); err != nil {
		return err
	}
	return nil
}

// GetSystemUUID returns the UUID used to identify node vm
func (osUtils *OsUtils) GetSystemUUID(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)
	// get the mounter
	mounter, err := GetMounter(ctx, osUtils)
	if err != nil {
		return "", err
	}
	sn, err := mounter.GetBIOSSerialNumber(ctx)
	if err != nil {
		return "", err
	}
	log.Infof("Bios serial number: %s", sn)
	// Here Bios Serial Number has this format - VMware-42 29 92 4b 83 35 d9 77-f3 82 cf 46 56 61 ac 60
	// We need to convert it to VM UUID - 4229924b-8335-d977-f382-cf465661ac60 which can be used to
	// look up Node VM in the vCenter inventory
	uuid, err := osUtils.ConvertUUID(sn)
	if err != nil {
		log.Errorf("failed to convert Bios serial number to VM UUID. err: %v", err)
		return "", err
	}
	log.Infof("Node VM UUID: %s", uuid)
	return uuid, nil
}

// ConvertUUID helps convert UUID to vSphere format, for example,
// Input uuid:    VMware-42 02 e9 7e 3d ad 2a 49-22 86 7f f9 89 c6 64 ef
// Returned uuid: 4202e97e-3dad-2a49-2286-7ff989c664ef
func (osUtils *OsUtils) ConvertUUID(uuid string) (string, error) {
	//strip leading and trailing white space and new line char
	uuid = strings.TrimSpace(uuid)
	// check the uuid starts with "VMware-"
	if !strings.HasPrefix(uuid, UUIDPrefix) {
		return "", fmt.Errorf("failed to match Prefix, UUID read from the file is %v", uuid)
	}
	// Strip the prefix and white spaces and -
	uuid = strings.Replace(uuid[len(UUIDPrefix):(len(uuid))], " ", "", -1)
	uuid = strings.Replace(uuid, "-", "", -1)
	if len(uuid) != 32 {
		return "", fmt.Errorf("length check failed, UUID read from the file is %v", uuid)
	}
	// need to add dashes, e.g. "564d395e-d807-e18a-cb25-b79f65eb2b9f"
	uuid = fmt.Sprintf("%s-%s-%s-%s-%s", uuid[0:8], uuid[8:12], uuid[12:16], uuid[16:20], uuid[20:32])
	return uuid, nil
}

// decides if node should continue
func (osUtils *OsUtils) ShouldContinue(ctx context.Context) {
	log := logger.GetLogger(ctx)
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIWindowsSupport) {
		log.Errorf("csi plugin started on windows node without enabling feature switch")
		os.Exit(1)
	}
	return
}

// Check if device at given path is block device or not
func (osUtils *OsUtils) IsBlockDevice(ctx context.Context, volumePath string) (bool, error) {
	return false, nil
}
