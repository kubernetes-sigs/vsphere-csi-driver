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

package common

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// ValidateCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateCreateVolumeRequest(ctx context.Context, req *csi.CreateVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Volume Name
	volName := req.GetName()
	if len(volName) == 0 {
		msg := "Volume name is a required parameter."
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	}
	// Validate Volume Capabilities
	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}
	if !IsValidVolumeCapabilities(ctx, volCaps) {
		return status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}
	return nil
}

// ValidateDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateDeleteVolumeRequest(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	log := logger.GetLogger(ctx)
	//check for required parameters
	if len(req.VolumeId) == 0 {
		msg := "Volume ID is a required parameter."
		log.Error(msg)
		return status.Errorf(codes.InvalidArgument, msg)
	}
	return nil
}

// ValidateControllerPublishVolumeRequest is the helper function to validate
// ControllerPublishVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateControllerPublishVolumeRequest(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	log := logger.GetLogger(ctx)
	//check for required parameters
	if len(req.VolumeId) == 0 {
		msg := "Volume ID is a required parameter."
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	} else if len(req.NodeId) == 0 {
		msg := "Node ID is a required parameter."
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	}
	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return status.Error(codes.InvalidArgument, "Volume capability not provided")
	}
	caps := []*csi.VolumeCapability{volCap}
	if !IsValidVolumeCapabilities(ctx, caps) {
		return status.Error(codes.InvalidArgument, "Volume capability not supported")
	}
	return nil
}

// ValidateControllerUnpublishVolumeRequest is the helper function to validate
// ControllerUnpublishVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateControllerUnpublishVolumeRequest(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) error {
	log := logger.GetLogger(ctx)
	//check for required parameters
	if len(req.VolumeId) == 0 {
		msg := "Volume ID is a required parameter."
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	} else if len(req.NodeId) == 0 {
		msg := "Node ID is a required parameter."
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	}
	return nil
}

// CheckAPI checks if specified version is 6.7.3 or higher
func CheckAPI(version string) error {
	items := strings.Split(version, ".")
	if len(items) < 2 || len(items) > 4 {
		return fmt.Errorf("Invalid API Version format")
	}
	major, err := strconv.Atoi(items[0])
	if err != nil {
		return fmt.Errorf("Invalid Major Version value")
	}
	minor, err := strconv.Atoi(items[1])
	if err != nil {
		return fmt.Errorf("Invalid Minor Version value")
	}

	if major < MinSupportedVCenterMajor || (major == MinSupportedVCenterMajor && minor < MinSupportedVCenterMinor) {
		return fmt.Errorf("The minimum supported vCenter is 6.7.3")
	}

	if major == MinSupportedVCenterMajor && minor == MinSupportedVCenterMinor {
		if len(items) >= 3 {
			patch, err := strconv.Atoi(items[2])
			if err != nil || patch < MinSupportedVCenterPatch {
				return fmt.Errorf("Invalid patch version value")
			}
		}
	}
	return nil
}

// ValidateControllerExpandVolumeRequest is the helper function to validate
// ControllerExpandVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateControllerExpandVolumeRequest(ctx context.Context, req *csi.ControllerExpandVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// check for required parameters
	if len(req.GetVolumeId()) == 0 {
		msg := "volume id is a required parameter"
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	} else if req.GetCapacityRange() == nil {
		msg := "capacity range is a required parameter"
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	} else if req.GetCapacityRange().GetRequiredBytes() < 0 || req.GetCapacityRange().GetLimitBytes() < 0 {
		msg := "capacity ranges values cannot be negative"
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	}
	// Validate Volume Capabilities
	volCaps := req.GetVolumeCapability()
	if volCaps == nil {
		msg := "volume capabilities is a required parameter"
		log.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	}

	// TODO: Remove this restriction when volume expansion is supported for File Volumes
	if IsFileVolumeRequest(ctx, []*csi.VolumeCapability{volCaps}) {
		msg := "volume expansion is only supported for block volume type"
		log.Error(msg)
		return status.Error(codes.Unimplemented, msg)
	}

	return nil
}

// IsOnlineExpansion verifies if the input volume is attached to any of the given VirutalMachines, to prevent
// online expansion of volumes.
// Returns an error if the volume is attached.
func IsOnlineExpansion(ctx context.Context, volumeID string, nodes []*cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)
	diskUUID, err := cnsvolume.IsDiskAttachedToVMs(ctx, volumeID, nodes)
	if err != nil {
		msg := fmt.Sprintf("failed to check if volume %q is attached to any node with error: %+v", volumeID, err)
		log.Error(msg)
		return status.Errorf(codes.Internal, msg)
	} else if diskUUID != "" {
		msg := fmt.Sprintf("failed to expand volume: %q. Volume is attached to node. Only offline volume expansion is supported", volumeID)
		log.Error(msg)
		return status.Errorf(codes.FailedPrecondition, msg)
	}

	return nil
}
