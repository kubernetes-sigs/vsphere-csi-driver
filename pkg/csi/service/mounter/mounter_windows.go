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

package mounter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	disk "github.com/kubernetes-csi/csi-proxy/client/api/disk/v1"
	diskclient "github.com/kubernetes-csi/csi-proxy/client/groups/disk/v1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	fs "github.com/kubernetes-csi/csi-proxy/client/api/filesystem/v1"
	fsclient "github.com/kubernetes-csi/csi-proxy/client/groups/filesystem/v1"

	volume "github.com/kubernetes-csi/csi-proxy/client/api/volume/v1"
	volumeclient "github.com/kubernetes-csi/csi-proxy/client/groups/volume/v1"

	systemApi "github.com/kubernetes-csi/csi-proxy/client/api/system/v1alpha1"
	systemClient "github.com/kubernetes-csi/csi-proxy/client/groups/system/v1alpha1"

	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

// black assignment is used to check if it can be cast
var _ CSIProxyMounter = &csiProxyMounter{}

type csiProxyMounter struct {
	Ctx          context.Context
	FsClient     *fsclient.Client
	DiskClient   *diskclient.Client
	VolumeClient *volumeclient.Client
	SystemClient *systemClient.Client
}

// CSIProxyMounter extends the mount.Interface interface with CSI Proxy methods.
// In future this functions are supposed to be implemented in mount.Interface for windows
type CSIProxyMounter interface {
	mount.Interface
	// ExistsPath - Checks if a path exists. This call does not perform follow link.
	ExistsPath(ctx context.Context, path string) (bool, error)
	// FormatAndMount - accepts the source disk number, target path to mount, the fstype to format with and options to be used.
	FormatAndMount(ctx context.Context, source, target, fstype string, options []string) error
	// Rmdir - delete the given directory
	Rmdir(ctx context.Context, path string) error
	// MakeDir - Creates a directory.
	MakeDir(ctx context.Context, pathname string) error
	// Rescan would trigger an update storage cache via the CSI proxy.
	Rescan(ctx context.Context) error
	// GetDeviceNameFromMount returns the volume ID for a mount path.
	GetDeviceNameFromMount(ctx context.Context, mountPath string) (string, error)
	// Get the size in bytes for Volume
	GetVolumeSizeInBytes(ctx context.Context, devicePath string) (int64, error)
	// ResizeVolume resizes the volume to the maximum available size.
	ResizeVolume(ctx context.Context, devicePath string, sizeInBytes int64) error
	// GetAPIVersions returns the versions of the client APIs this mounter is using.
	GetAPIVersions() string
	// Gets windows specific disk number from diskId
	GetDiskNumber(ctx context.Context, diskID string) (string, error)
	// Get the size of the disk in bytes
	GetDiskTotalBytes(ctx context.Context, devicePath string) (int64, error)
	// StatFS returns info about volume
	StatFS(ctx context.Context, path string) (available, capacity, used, inodesFree, inodes, inodesUsed int64, err error)
	// GetBIOSSerialNumber - Get bios serial number
	GetBIOSSerialNumber(ctx context.Context) (string, error)
}

// NewSafeMounter returns mounter with exec
func NewSafeMounter(ctx context.Context) (*mount.SafeFormatAndMount, error) {
	csiProxyMounter, err := newCSIProxyMounter(ctx)
	log := logger.GetLogger(ctx)
	if err == nil {
		log.Infof("using CSIProxyMounterV1, %s", csiProxyMounter.GetAPIVersions())
		return &mount.SafeFormatAndMount{
			Interface: csiProxyMounter,
			Exec:      utilexec.New(),
		}, nil
	}
	return nil, err
}

// newCSIProxyMounter - creates a new CSI Proxy mounter struct which encompassed all the
// clients to the CSI proxy - filesystem, disk and volume clients.
func newCSIProxyMounter(ctx context.Context) (*csiProxyMounter, error) {
	fsClient, err := fsclient.NewClient()
	if err != nil {
		return nil, err
	}
	diskClient, err := diskclient.NewClient()
	if err != nil {
		return nil, err
	}
	volumeClient, err := volumeclient.NewClient()
	if err != nil {
		return nil, err
	}
	systemClient, err := systemClient.NewClient()
	if err != nil {
		return nil, err
	}
	return &csiProxyMounter{
		FsClient:     fsClient,
		DiskClient:   diskClient,
		VolumeClient: volumeClient,
		SystemClient: systemClient,
		Ctx:          ctx,
	}, nil
}

// GetAPIVersions returns the versions of the client APIs this mounter is using.
func (mounter *csiProxyMounter) GetAPIVersions() string {
	return fmt.Sprintf(
		"API Versions filesystem: %s, disk: %s, volume: %s, system: %s",
		fsclient.Version,
		diskclient.Version,
		volumeclient.Version,
		systemClient.Version,
	)
}

// normalizeWindowsPath normalizes windows path
func normalizeWindowsPath(path string) string {
	normalizedPath := strings.Replace(path, "/", "\\", -1)
	if strings.HasPrefix(normalizedPath, "\\") {
		normalizedPath = "c:" + normalizedPath
	}
	return normalizedPath
}

// ExistsPath - Checks if a path exists. This call does not perform follow link.
func (mounter *csiProxyMounter) ExistsPath(ctx context.Context, path string) (bool, error) {
	log := logger.GetLogger(ctx)
	isExistsResponse, err := mounter.FsClient.PathExists(ctx,
		&fs.PathExistsRequest{
			Path: normalizeWindowsPath(path),
		})
	if err != nil {
		log.Errorf("Proxy returned error while checking if PathExists for path: %q, err: %v", path, err)
		return false, err
	}
	return isExistsResponse.Exists, err
}

// Rmdir - delete the given directory
func (mounter *csiProxyMounter) Rmdir(ctx context.Context, path string) error {
	log := logger.GetLogger(ctx)
	rmdirRequest := &fs.RmdirRequest{
		Path:  normalizeWindowsPath(path),
		Force: true,
	}
	_, err := mounter.FsClient.Rmdir(context.Background(), rmdirRequest)
	if err != nil {
		log.Errorf("failed to remove dir: %q with force. err: %v", rmdirRequest.Path, err)
		return err
	}
	return nil
}

// MakeDir - Creates a directory.
func (mounter *csiProxyMounter) MakeDir(ctx context.Context, pathname string) error {
	log := logger.GetLogger(ctx)
	mkdirReq := &fs.MkdirRequest{
		Path: normalizeWindowsPath(pathname),
	}
	_, err := mounter.FsClient.Mkdir(ctx, mkdirReq)
	if err != nil {
		log.Errorf("failed to make directory: %q. Error: %v", mkdirReq.Path, err)
		return err
	}
	return nil
}

// Gets windows specific disk number from diskId
func (mounter *csiProxyMounter) GetDiskNumber(ctx context.Context, diskID string) (string, error) {
	log := logger.GetLogger(ctx)
	// Check device is attached
	log.Debug("GetDiskNumber called with diskID: %q", diskID)

	listRequest := &disk.ListDiskIDsRequest{}
	diskIDsResponse, err := mounter.DiskClient.ListDiskIDs(ctx, listRequest)
	if err != nil {
		log.Errorf("Could not get diskids. error: %v", err)
		return "", err
	}
	spew.Dump("disIDs: ", diskIDsResponse)
	for diskNum, diskInfo := range diskIDsResponse.GetDiskIDs() {
		log.Infof("found disk number %d, disk info %v", diskNum, diskInfo)
		ID := diskInfo.Page83
		if ID == "" {
			continue
		}
		if ID == diskID {
			log.Infof("Found disk number: %d with diskID: %s", diskNum, diskID)
			return strconv.FormatUint(uint64(diskNum), 10), nil
		}
	}
	return "", errors.New("no matching disks found")
}

// IsLikelyMountPoint - If the directory does not exists, the function will return os.ErrNotExist error.
// If the path exists, call to CSI proxy will check if its a link, if its a link then existence of target
// path is checked.
func (mounter *csiProxyMounter) IsLikelyNotMountPoint(path string) (bool, error) {
	isExists, err := mounter.ExistsPath(mounter.Ctx, path)
	if err != nil {
		return false, err
	}

	if !isExists {
		return true, os.ErrNotExist
	}

	response, err := mounter.FsClient.IsSymlink(mounter.Ctx,
		&fs.IsSymlinkRequest{
			Path: normalizeWindowsPath(path),
		})
	if err != nil {
		return false, err
	}
	return !response.IsSymlink, nil
	//TODO check if formatted else error out
}

// FormatAndMount - accepts the source disk number, target path to mount, the fstype to format with and options to be used.
func (mounter *csiProxyMounter) FormatAndMount(ctx context.Context, source string, target string, fstype string, options []string) error {
	log := logger.GetLogger(ctx)
	diskNum, err := strconv.Atoi(source)
	if err != nil {
		return fmt.Errorf("parse %s failed with error: %v", source, err)
	}
	log.Infof("Disk Number: %d", diskNum)
	// Call PartitionDisk CSI proxy call to partition the disk and return the volume id
	partionDiskRequest := &disk.PartitionDiskRequest{
		DiskNumber: uint32(diskNum),
	}
	if _, err = mounter.DiskClient.PartitionDisk(ctx, partionDiskRequest); err != nil {
		log.Errorf("partition disk failed for disk number: %d, err: %v", partionDiskRequest.DiskNumber, err)
		return err
	}

	// ensure disk is online
	log.Infof("setting disk %d to online", diskNum)
	attachRequest := &disk.SetDiskStateRequest{
		DiskNumber: uint32(diskNum),
		IsOnline:   true,
	}
	_, err = mounter.DiskClient.SetDiskState(ctx, attachRequest)
	if err != nil {
		log.Errorf("failed to set disk state as online for disk: %d, err: %v", attachRequest.DiskNumber, err)
		return err
	}

	// List the volumes on the given disk.
	volumeIDsRequest := &volume.ListVolumesOnDiskRequest{
		DiskNumber: uint32(diskNum),
	}
	volumeIdResponse, err := mounter.VolumeClient.ListVolumesOnDisk(ctx, volumeIDsRequest)
	if err != nil {
		return err
	}

	// TODO: consider partitions and choose the right partition.
	// For now just choose the first volume.
	volumeID := volumeIdResponse.VolumeIds[0]
	log.Infof("volumeIdResponse : %v", volumeIdResponse)
	log.Infof("volumeID : %s", volumeID)
	// Check if the volume is formatted.
	isVolumeFormattedRequest := &volume.IsVolumeFormattedRequest{
		VolumeId: volumeID,
	}
	isVolumeFormattedResponse, err := mounter.VolumeClient.IsVolumeFormatted(ctx, isVolumeFormattedRequest)
	if err != nil {
		return err
	}

	// If the volume is not formatted, then format it, else proceed to mount.
	if !isVolumeFormattedResponse.Formatted {
		log.Infof("volumeID is not formatted : %s", volumeID)
		formatVolumeRequest := &volume.FormatVolumeRequest{
			VolumeId: volumeID,
			// TODO: Accept the filesystem and other options
		}
		_, err = mounter.VolumeClient.FormatVolume(ctx, formatVolumeRequest)
		if err != nil {
			return err
		}
	}

	// Mount the volume by calling the CSI proxy call.
	mountVolumeRequest := &volume.MountVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: normalizeWindowsPath(target),
	}
	_, err = mounter.VolumeClient.MountVolume(ctx, mountVolumeRequest)
	log.Debug("Volume mounted")
	if err != nil {
		return err
	}
	return nil
}

// Unmount - Removes the directory - equivalent to unmount on Linux.
func (mounter *csiProxyMounter) Unmount(target string) error {
	// unmount internally calls WriteVolumeCache so no need to WriteVolumeCache
	// normalize target path
	target = normalizeWindowsPath(target)
	if exists, err := mounter.ExistsPath(mounter.Ctx, target); !exists {
		return err
	}
	// get volume id
	idRequest := &volume.GetVolumeIDFromTargetPathRequest{
		TargetPath: target,
	}
	idResponse, err := mounter.VolumeClient.GetVolumeIDFromTargetPath(mounter.Ctx, idRequest)
	if err != nil {
		return err
	}
	volumeId := idResponse.GetVolumeId()

	// unmount volume
	unmountRequest := &volume.UnmountVolumeRequest{
		TargetPath: target,
		VolumeId:   volumeId,
	}
	_, err = mounter.VolumeClient.UnmountVolume(mounter.Ctx, unmountRequest)
	if err != nil {
		return err
	}

	// remove the target directory
	err = mounter.Rmdir(mounter.Ctx, target)
	if err != nil {
		return err
	}

	// Set disk to offline mode to have a clean state
	getDiskNumberRequest := &volume.GetDiskNumberFromVolumeIDRequest{
		VolumeId: volumeId,
	}
	getDiskNumberResponse, err := mounter.VolumeClient.GetDiskNumberFromVolumeID(mounter.Ctx, getDiskNumberRequest)
	if err != nil {
		return err
	}
	diskNumber := getDiskNumberResponse.GetDiskNumber()
	setDiskStateRequest := &disk.SetDiskStateRequest{
		DiskNumber: diskNumber,
		IsOnline:   false,
	}
	if _, err = mounter.DiskClient.SetDiskState(mounter.Ctx, setDiskStateRequest); err != nil {
		return err
	}
	return nil
}

// Mount just creates a soft link at target pointing to source.
func (mounter *csiProxyMounter) Mount(source string, target string, fstype string, options []string) error {
	// Mount is called after the format is done.
	// TODO: Confirm that fstype is empty.
	linkRequest := &fs.CreateSymlinkRequest{
		SourcePath: normalizeWindowsPath(source),
		TargetPath: normalizeWindowsPath(target),
	}
	_, err := mounter.FsClient.CreateSymlink(mounter.Ctx, linkRequest)
	if err != nil {
		return err
	}
	return nil
}

// GetDeviceNameFromMount returns the volume ID for a mount path.
func (mounter *csiProxyMounter) GetDeviceNameFromMount(ctx context.Context, mountPath string) (string, error) {
	log := logger.GetLogger(ctx)
	req := &volume.GetVolumeIDFromTargetPathRequest{TargetPath: normalizeWindowsPath(mountPath)}
	resp, err := mounter.VolumeClient.GetVolumeIDFromTargetPath(ctx, req)
	if err != nil {
		log.Errorf("failed to get volume id from target path: %q, err: %v", req.TargetPath, err)
		return "", err
	}
	log.Infof("Device path for mount Path: %s: %s", mountPath, resp.VolumeId)
	return resp.VolumeId, nil
}

// ResizeVolume resizes the volume to the maximum available size.
// sizeInBytes is ignored in this function as windows is not resizing to full capacity
func (mounter *csiProxyMounter) ResizeVolume(ctx context.Context, devicePath string, sizeInBytes int64) error {
	log := logger.GetLogger(ctx)
	// Set disk to online mode before resize
	getDiskNumberRequest := &volume.GetDiskNumberFromVolumeIDRequest{
		VolumeId: devicePath, // here devicePath is Device.RealDev which is Volume ID for Windows
	}
	getDiskNumberResponse, err := mounter.VolumeClient.GetDiskNumberFromVolumeID(ctx, getDiskNumberRequest)
	if err != nil {
		log.Errorf("failed to get disk number for volume id: %q, err: %v", getDiskNumberRequest.VolumeId, err)
		return err
	}
	diskNumber := getDiskNumberResponse.GetDiskNumber()
	setDiskStateRequest := &disk.SetDiskStateRequest{
		DiskNumber: diskNumber,
		IsOnline:   true,
	}
	if _, err = mounter.DiskClient.SetDiskState(ctx, setDiskStateRequest); err != nil {
		log.Errorf("failed to set disk state as Online for disk: %d, err: %v", setDiskStateRequest.DiskNumber, err)
		return err
	}

	req := &volume.ResizeVolumeRequest{VolumeId: devicePath, SizeBytes: 0}
	_, err = mounter.VolumeClient.ResizeVolume(ctx, req)
	return err
}

// Get the size in bytes for Volume
func (mounter *csiProxyMounter) GetVolumeSizeInBytes(ctx context.Context, volumeId string) (int64, error) {
	log := logger.GetLogger(ctx)
	req := &volume.GetVolumeStatsRequest{VolumeId: volumeId}
	resp, err := mounter.VolumeClient.GetVolumeStats(ctx, req)
	if err != nil {
		log.Errorf("failed to get volume stats for volume id: %q err: %v", volumeId, err)
		return -1, err
	}
	return resp.TotalBytes, nil
}

// Rescan would trigger an update storage cache via the CSI proxy.
func (mounter *csiProxyMounter) Rescan(ctx context.Context) error {
	// Call Rescan from disk APIs of CSI Proxy.
	log := logger.GetLogger(ctx)
	log.Infof("Calling CSI Proxy's rescan API")
	if _, err := mounter.DiskClient.Rescan(ctx, &disk.RescanRequest{}); err != nil {
		log.Errorf("failed to rescan stroage cache. err: %v", err)
		return err
	}
	return nil
}

// Get the size of the disk in bytes
func (mounter *csiProxyMounter) GetDiskTotalBytes(ctx context.Context, volumeId string) (int64, error) {
	log := logger.GetLogger(ctx)
	getDiskNumberRequest := &volume.GetDiskNumberFromVolumeIDRequest{
		VolumeId: volumeId,
	}
	getDiskNumberResponse, err := mounter.VolumeClient.GetDiskNumberFromVolumeID(ctx, getDiskNumberRequest)
	if err != nil {
		log.Errorf("failed to get disk number from volumeID: %q, err: %v", volumeId, err)
		return -1, err
	}
	diskNumber := getDiskNumberResponse.GetDiskNumber()

	DiskStatsResponse, err := mounter.DiskClient.GetDiskStats(ctx,
		&disk.GetDiskStatsRequest{
			DiskNumber: diskNumber,
		})
	if err != nil {
		log.Errorf("failed to get disk stats for disk number: %d, err: %v", diskNumber, err)
	}
	return DiskStatsResponse.TotalBytes, err
}

// StatFS returns info about volume
func (mounter *csiProxyMounter) StatFS(ctx context.Context, path string) (available, capacity, used, inodesFree, inodes, inodesUsed int64, err error) {
	log := logger.GetLogger(ctx)
	zero := int64(0)

	idRequest := &volume.GetVolumeIDFromTargetPathRequest{
		TargetPath: path,
	}
	idResponse, err := mounter.VolumeClient.GetVolumeIDFromTargetPath(ctx, idRequest)
	if err != nil {
		log.Errorf("failed to get volume id from target path: %q, err: %v", idRequest.TargetPath, err)
		return zero, zero, zero, zero, zero, zero, err
	}
	volumeID := idResponse.GetVolumeId()

	request := &volume.GetVolumeStatsRequest{
		VolumeId: volumeID,
	}
	response, err := mounter.VolumeClient.GetVolumeStats(ctx, request)
	if err != nil {
		log.Errorf("failed to get volume stats for volume id: %q, err: %v", request.VolumeId, err)
		return zero, zero, zero, zero, zero, zero, err
	}
	capacity = response.GetTotalBytes()
	used = response.GetUsedBytes()
	available = capacity - used
	return available, capacity, used, zero, zero, zero, nil
}

// umimplemented methods of mount.Interface
func (mounter *csiProxyMounter) GetMountRefs(pathname string) ([]string, error) {
	return []string{}, fmt.Errorf("GetMountRefs is not implemented for csiProxyMounter")
}

func (mounter *csiProxyMounter) GetFSGroup(pathname string) (int64, error) {
	return -1, fmt.Errorf("GetFSGroup is not implemented for csiProxyMounter")
}

func (mounter *csiProxyMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return fmt.Errorf("MountSensitive is not implemented for csiProxyMounter")
}

func (mounter *csiProxyMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return fmt.Errorf("MountSensitiveWithoutSystemd is not implemented for csiProxyMounter")
}

func (mounter *csiProxyMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return mounter.MountSensitive(source, target, fstype, options, sensitiveOptions /* sensitiveOptions */)
}

func (mounter *csiProxyMounter) List() ([]mount.MountPoint, error) {
	return []mount.MountPoint{}, fmt.Errorf("List not implemented for csiProxyMounter")
}

// GetBIOSSerialNumber - Get bios serial number
func (mounter *csiProxyMounter) GetBIOSSerialNumber(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)

	serialNoResponse, err := mounter.SystemClient.GetBIOSSerialNumber(ctx,
		&systemApi.GetBIOSSerialNumberRequest{},
	)
	if err != nil {
		log.Errorf("Proxy returned error while checking serialNoResponse: %v", err)
		return "", err
	}
	return serialNoResponse.GetSerialNumber(), err
}
