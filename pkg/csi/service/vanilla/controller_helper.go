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

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/node"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// validateVanillaDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for Vanilla CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateVanillaDeleteVolumeRequest(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	return common.ValidateDeleteVolumeRequest(ctx, req)

}

// validateControllerPublishVolumeRequest is the helper function to validate
// ControllerPublishVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateVanillaControllerPublishVolumeRequest(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	return common.ValidateControllerPublishVolumeRequest(ctx, req)
}

// validateControllerUnpublishVolumeRequest is the helper function to validate
// ControllerUnpublishVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateVanillaControllerUnpublishVolumeRequest(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) error {
	return common.ValidateControllerUnpublishVolumeRequest(ctx, req)
}

// validateVanillaControllerExpandVolumeRequest is the helper function to validate
// ExpandVolumeRequest for Vanilla CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateVanillaControllerExpandVolumeRequest(ctx context.Context, req *csi.ControllerExpandVolumeRequest) error {
	log := logger.GetLogger(ctx)
	if err := common.ValidateControllerExpandVolumeRequest(ctx, req); err != nil {
		return err
	}

	nodeManager := node.GetManager(ctx)
	nodes, err := nodeManager.GetAllNodes(ctx)
	if err != nil {
		msg := fmt.Sprintf("failed to find VirtualMachines for all registered nodes. Error: %v", err)
		log.Error(msg)
		return status.Error(codes.Internal, msg)
	}
	return common.IsOnlineExpansion(ctx, req.GetVolumeId(), nodes)
}
