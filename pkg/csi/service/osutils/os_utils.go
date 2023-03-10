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

// Package osutils provides methods to perform os specific operations
package osutils

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"k8s.io/mount-utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

type OsUtils struct {
	Mounter *mount.SafeFormatAndMount
}

// struct to hold params required for NodeStage operation
type NodeStageParams struct {
	// volID is the identifier for the underlying volume.
	VolID string
	// fsType is the file system type - ext3, ext4, nfs, nfs4.
	FsType string
	// Staging Target path is used to mount the volume to the node.
	StagingTarget string
	// Mount flags/options intended to be used while running the mount command.
	MntFlags []string
	// Read-only flag.
	Ro bool
}

// struct to hold params required for NodePublish operation
type NodePublishParams struct {
	// volID is the identifier for the underlying volume.
	VolID string
	// Target path is used to bind-mount a staged volume to the pod.
	Target string
	// Staging Target path is used to mount the volume to the node.
	StagingTarget string
	// diskID is the identifier for the disk.
	DiskID string
	// volumePath represents the sym-linked block volume full path.
	VolumePath string
	// device represents the actual path of the block volume.
	Device string
	// Read-only flag.
	Ro bool
}

// Device is a struct for holding details about a block device.
type Device struct {
	FullPath string // full path where device is mounted
	Name     string // name of device
	RealDev  string // in windows it represents volumeID and in linux it represents device path
}

// GetDiskID returns the diskID of the disk attached
func (osUtils *OsUtils) GetDiskID(pubCtx map[string]string, log *zap.SugaredLogger) (string, error) {
	var diskID string
	var ok bool
	if diskID, ok = pubCtx[common.AttributeFirstClassDiskUUID]; !ok {
		return "", logger.LogNewErrorCodef(log, codes.InvalidArgument,
			"attribute: %s required in publish context",
			common.AttributeFirstClassDiskUUID)
	}
	return diskID, nil
}

// EnsureMountVol ensures that VolumeCapability has mount option
// and returns fstype, mount flags
func (osUtils *OsUtils) EnsureMountVol(ctx context.Context, volCap *csi.VolumeCapability) (string, []string, error) {
	log := logger.GetLogger(ctx)
	mountVol := volCap.GetMount()
	if mountVol == nil {
		return "", nil, logger.LogNewErrorCode(log, codes.InvalidArgument, "access type missing")
	}
	fs, err := osUtils.GetVolumeCapabilityFsType(ctx, volCap)
	if err != nil {
		log.Errorf("GetVolumeCapabilityFsType failed with err: %v", err)
		return "", nil, err
	}

	mntFlags := mountVol.GetMountFlags()
	// By default, xfs does not allow mounting of two volumes with the same filesystem uuid.
	// Force ignore this uuid to be able to mount volume + its clone / restored snapshot on the same node.
	if fs == common.XFSType {
		mntFlags = append(mntFlags, "nouuid")
	}

	return fs, mntFlags, nil
}
