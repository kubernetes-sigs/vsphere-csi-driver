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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vim25types "github.com/vmware/govmomi/vim25/types"
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
	if err := IsValidVolumeCapabilities(ctx, volCaps); err != nil {
		return status.Errorf(codes.InvalidArgument, "Volume capability not supported. Err: %+v", err)
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
	if err := IsValidVolumeCapabilities(ctx, caps); err != nil {
		return status.Errorf(codes.InvalidArgument, "Volume capability not supported. Err: %+v", err)
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
		return fmt.Errorf("invalid API Version format")
	}
	major, err := strconv.Atoi(items[0])
	if err != nil {
		return fmt.Errorf("invalid Major Version value")
	}
	minor, err := strconv.Atoi(items[1])
	if err != nil {
		return fmt.Errorf("invalid Minor Version value")
	}

	if major < MinSupportedVCenterMajor || (major == MinSupportedVCenterMajor && minor < MinSupportedVCenterMinor) {
		return fmt.Errorf("the minimum supported vCenter is 6.7.3")
	}

	if major == MinSupportedVCenterMajor && minor == MinSupportedVCenterMinor {
		if len(items) >= 3 {
			patch, err := strconv.Atoi(items[2])
			if err != nil || patch < MinSupportedVCenterPatch {
				return fmt.Errorf("invalid patch version value")
			}
		}
	}
	return nil
}

// UseVslmAPIs checks if specified version is between 6.7 Update 3l and 7.0
// The method takes aboutInfo{} as input which contains details about
// VC version, build number and so on.
// If the version is between the upper and lower bounds, the method returns true, else returns false
// and appropriate errors during failure cases
func UseVslmAPIs(ctx context.Context, aboutInfo vim25types.AboutInfo) (bool, error) {
	log := logger.GetLogger(ctx)
	items := strings.Split(aboutInfo.ApiVersion, ".")
	apiVersion := strings.Join(items[:], "")
	// Convert version string to string, Ex: "6.7.3" becomes 673, "7.0.0.0" becomes 700
	vSphereVersionInt, err := strconv.Atoi(apiVersion[0:3])
	if err != nil {
		msg := fmt.Sprintf("Error while converting ApiVersion %q to integer, err %+v", apiVersion, err)
		log.Errorf(msg)
		return false, errors.New(msg)
	}
	vSphere67u3VersionStr := strings.Join(strings.Split(VSphere67u3Version, "."), "")
	vSphere67u3VersionInt, err := strconv.Atoi(vSphere67u3VersionStr[0:3])
	if err != nil {
		msg := fmt.Sprintf("Error while converting VSphere67u3Version %q to integer, err %+v", VSphere67u3Version, err)
		log.Errorf(msg)
		return false, errors.New(msg)
	}
	vSphere7VersionStr := strings.Join(strings.Split(VSphere7Version, "."), "")
	vSphere7VersionInt, err := strconv.Atoi(vSphere7VersionStr[0:3])
	if err != nil {
		msg := fmt.Sprintf("Error while converting VSphere7Version %q to integer, err %+v", VSphere7Version, err)
		log.Errorf(msg)
		return false, errors.New(msg)
	}
	// Check if the current vSphere version is between 6.7.3 and 7.0.0
	if vSphereVersionInt > vSphere67u3VersionInt && vSphereVersionInt <= vSphere7VersionInt {
		return true, nil
	}
	// Check if version is 6.7.3
	if vSphereVersionInt == vSphere67u3VersionInt {
		// CSI migration feature will be supported only from 6.7 Update 3l vSphere version onwards
		// For all older 6.7 Update 3 such as 3a, 3b and so on, we do not support the CSI migration feature.
		// Because there is no patch version number in aboutInfo{}, we will rely on the build number to check for 6.7 Update 3l
		// VC builds are always incremental and hence we can use the build info to check if VC version is 6.7 Update 3l
		// Here is a snippet of the build info for the GA bits mentioned here: https://docs.vmware.com/en/VMware-vSphere/6.7/rn/vsphere-vcenter-server-67u3l-release-notes.html
		// Name: "VMware vCenter Server",
		// FullName: "VMware vCenter Server 6.7.0 build-17137327",
		// Version: "6.7.0",
		// Build:  "17137327",
		// ApiVersion: "6.7.3",
		if vcBuild, err := strconv.Atoi(aboutInfo.Build); err == nil {
			if vcBuild >= VSphere67u3lBuildInfo {
				return true, nil
			}
			msg := fmt.Sprintf("Found vCenter version :%q. The minimum supported vCenter version for CSI migration feature is vCenter Server 6.7 Update 3l.", aboutInfo.ApiVersion)
			log.Errorf(msg)
			return false, errors.New(msg)
		}
		if err != nil {
			msg := fmt.Sprintf("Error while converting VC Build info %q to integer, err %+v", aboutInfo.Build, err)
			log.Errorf(msg)
			return false, errors.New(msg)
		}
	}
	// For all other versions
	return false, nil
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
	diskUUID, err := cnsvolume.IsDiskAttachedToVMs(ctx, volumeID, nodes, false)
	if err != nil {
		msg := fmt.Sprintf("failed to check if volume %q is attached to any node with error: %+v", volumeID, err)
		log.Error(msg)
		return status.Errorf(codes.Internal, msg)
	} else if diskUUID != "" {
		msg := fmt.Sprintf("failed to expand volume: %q. Volume is attached to node. Online volume expansion is not supported in this version", volumeID)
		log.Error(msg)
		return status.Errorf(codes.FailedPrecondition, msg)
	}

	return nil
}
