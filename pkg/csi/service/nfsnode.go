/*
Copyright 2026 The Kubernetes Authors.

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
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/wcpguest/nfsdriver"
)

// A guest NFS volume (see wcpguest.IsGuestNFSVolumeID) is an NFS export that the node mounts
// directly onto the pod's target path. It has no block device, no CNS volume and no Supervisor
// PVC, so none of the device, staging or Supervisor handling in node.go applies to it: these
// helpers are called from node.go ahead of that handling and either hand the request to the
// vendored nfsdriver package or acknowledge it where NFS has nothing to do.

// isGuestNFSVolume reports whether volumeID names a guest NFS volume.
func isGuestNFSVolume(volumeID string) bool {
	return wcpguest.IsGuestNFSVolumeID(volumeID)
}

// nodeStageGuestNFSVolume acknowledges NodeStageVolume: NFS has no staging step, the export is
// mounted by NodePublishVolume. Staging is advertised for the other volume types, so kubelet
// calls it and it must succeed.
func nodeStageGuestNFSVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {
	logger.GetLogger(ctx).Infof("NodeStageVolume: %q is a guest NFS volume, there is nothing to stage",
		req.GetVolumeId())
	return &csi.NodeStageVolumeResponse{}, nil
}

// nodeUnstageGuestNFSVolume acknowledges NodeUnstageVolume, for the same reason.
func nodeUnstageGuestNFSVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	logger.GetLogger(ctx).Infof("NodeUnstageVolume: %q is a guest NFS volume, there is nothing to unstage",
		req.GetVolumeId())
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// nodePublishGuestNFSVolume mounts the export onto the target path.
func nodePublishGuestNFSVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {
	log := logger.GetLogger(ctx)
	if !wcpguest.IsGuestNFSEnabled(ctx) {
		return nil, logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"cannot publish guest NFS volume %q: the %q feature state is disabled",
			req.GetVolumeId(), common.GuestNFSVolume)
	}
	return nfsdriver.GetOrCreateDriver().NodeServer().NodePublishVolume(ctx, req)
}

// nodeUnpublishGuestNFSVolume unmounts the export. It is not gated on the feature state, so a
// pod can always be torn down.
func nodeUnpublishGuestNFSVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (
	*csi.NodeUnpublishVolumeResponse, error) {
	return nfsdriver.GetOrCreateDriver().NodeServer().NodeUnpublishVolume(ctx, req)
}
