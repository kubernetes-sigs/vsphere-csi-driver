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
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/block"
)

const (
	// MinSupportedVCenterMajor is the minimum, major version of vCenter
	// on which FCD is supported.
	MinSupportedVCenterMajor int = 6

	// MinSupportedVCenterMinor is the minimum, minor version of vCenter
	// on which FCD is supported.
	MinSupportedVCenterMinor int = 7
)

// checkAPI checks if specified version is 6.7.1 or higher
func checkAPI(version string) error {
	items := strings.Split(version, ".")
	if len(items) <= 1 || len(items) > 3 {
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
		return fmt.Errorf("The minimum supported vCenter is 6.7.1")
	}

	if major == MinSupportedVCenterMajor && minor == MinSupportedVCenterMinor {
		if len(items) == 2 {
			patch, err := strconv.Atoi(items[2])
			if err != nil || patch < 1 {
				return fmt.Errorf("Invalid patch version value")
			}
		}
	}
	return nil
}

// validateCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateCreateVolumeRequest(req *csi.CreateVolumeRequest) error {
	// Get create params
	params := req.GetParameters()
	if params != nil {
		for paramName := range params {
			if !(paramName == block.AttributeDatastoreName || paramName == block.AttributeStoragePolicyName) {
				msg := fmt.Sprintf("volume parameter %s is not a valid parameter.", paramName)
				return status.Error(codes.InvalidArgument, msg)
			}
		}
	}
	// Volume Name
	volName := req.GetName()
	if len(volName) == 0 {
		msg := "Volume name is a required parameter."
		klog.Error(msg)
		return status.Error(codes.InvalidArgument, msg)
	}
	// Validate Volume Capabilities
	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}
	if !block.IsValidVolumeCapabilities(volCaps) {
		return status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}
	return nil
}

// validateDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateDeleteVolumeRequest(req *csi.DeleteVolumeRequest) error {
	//check for required parameters
	if len(req.VolumeId) == 0 {
		msg := "Volume ID is a required parameter."
		klog.Error(msg)
		return status.Errorf(codes.Internal, msg)
	}
	return nil
}

// validateControllerPublishVolumeRequest is the helper function to validate
// ControllerPublishVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateControllerPublishVolumeRequest(req *csi.ControllerPublishVolumeRequest) error {
	//check for required parameters
	if len(req.VolumeId) == 0 {
		msg := "Volume ID is a required parameter."
		klog.Error(msg)
		return status.Errorf(codes.InvalidArgument, msg)
	} else if len(req.NodeId) == 0 {
		msg := "Node ID is a required parameter."
		klog.Error(msg)
		return status.Errorf(codes.InvalidArgument, msg)
	}
	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return status.Error(codes.InvalidArgument, "Volume capability not provided")
	}
	caps := []*csi.VolumeCapability{volCap}
	if !block.IsValidVolumeCapabilities(caps) {
		return status.Error(codes.InvalidArgument, "Volume capability not supported")
	}
	return nil
}

// validateControllerUnpublishVolumeRequest is the helper function to validate
// ControllerUnpublishVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateControllerUnpublishVolumeRequest(req *csi.ControllerUnpublishVolumeRequest) error {
	//check for required parameters
	if len(req.VolumeId) == 0 {
		msg := "Volume ID is a required parameter."
		klog.Error(msg)
		return status.Errorf(codes.InvalidArgument, msg)
	} else if len(req.NodeId) == 0 {
		msg := "Node ID is a required parameter."
		klog.Error(msg)
		return status.Errorf(codes.InvalidArgument, msg)
	}
	return nil
}
