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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/node"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
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
