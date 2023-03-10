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
	"time"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// InvalidTopologyProvisioningError is raised when volume provisioning
// fails on a topology aware setup due to an invalid setting.
type InvalidTopologyProvisioningError struct {
	ErrMsg string
}

func (e *InvalidTopologyProvisioningError) Error() string {
	return e.ErrMsg
}

// ValidateCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateCreateVolumeRequest(ctx context.Context, req *csi.CreateVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Volume Name.
	volName := req.GetName()
	if len(volName) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume name is a required parameter")
	}
	// Validate Volume Capabilities.
	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume capabilities not provided")
	}
	if err := IsValidVolumeCapabilities(ctx, volCaps); err != nil {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument, "volume capability not supported. Err: %+v", err)
	}
	return nil
}

// ValidateDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateDeleteVolumeRequest(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Check for required parameters.
	if len(req.VolumeId) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume ID is a required parameter")
	}
	return nil
}

// ValidateControllerPublishVolumeRequest is the helper function to validate
// ControllerPublishVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateControllerPublishVolumeRequest(ctx context.Context, req *csi.ControllerPublishVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Check for required parameters.
	if len(req.VolumeId) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume ID is a required parameter")
	} else if len(req.NodeId) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "node ID is a required parameter")
	}
	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume capability not provided")
	}
	caps := []*csi.VolumeCapability{volCap}
	if err := IsValidVolumeCapabilities(ctx, caps); err != nil {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument, "volume capability not supported. Err: %+v", err)
	}
	return nil
}

// ValidateControllerUnpublishVolumeRequest is the helper function to validate
// ControllerUnpublishVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateControllerUnpublishVolumeRequest(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Check for required parameters.
	if len(req.VolumeId) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume ID is a required parameter")
	} else if len(req.NodeId) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "node ID is a required parameter")
	}
	return nil
}

// CheckAPI checks if specified version against the specified minimum support version.
func CheckAPI(ctx context.Context,
	versionToCheck string,
	minSupportedVCenterMajor int,
	minSupportedVCenterMinor int,
	minSupportedVCenterPatch int) error {
	log := logger.GetLogger(ctx)
	items := strings.Split(versionToCheck, ".")
	if len(items) < 2 {
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

	if major < minSupportedVCenterMajor || (major == minSupportedVCenterMajor && minor < minSupportedVCenterMinor) {
		return fmt.Errorf("the minimum supported vCenter is %d.%d.%d",
			minSupportedVCenterMajor, minSupportedVCenterMinor, minSupportedVCenterPatch)
	}

	if major == minSupportedVCenterMajor && minor == minSupportedVCenterMinor {
		if len(items) >= 3 {
			patch, err := strconv.Atoi(items[2])
			if err != nil {
				return fmt.Errorf("invalid patch version value")
			}
			if patch < minSupportedVCenterPatch {
				return fmt.Errorf("the minimum supported vCenter is %d.%d.%d",
					minSupportedVCenterMajor, minSupportedVCenterMinor, minSupportedVCenterPatch)
			}
		}
	}
	log.Infof("VC version detected as %q satisfies minimum supported vcenter version %d.%d.%d.",
		versionToCheck, minSupportedVCenterMajor, minSupportedVCenterMinor, minSupportedVCenterPatch)
	return nil
}

// UseVslmAPIs checks if specified version is between 6.7 Update 3l and 7.0.
// The method takes aboutInfo{} as input which contains details about
// VC version, build number and so on.
// If the version is between the upper and lower bounds, the method returns
// true, else returns false and appropriate errors during failure cases.
func UseVslmAPIs(ctx context.Context, aboutInfo vim25types.AboutInfo) (bool, error) {
	log := logger.GetLogger(ctx)
	items := strings.Split(aboutInfo.ApiVersion, ".")
	apiVersion := strings.Join(items[:], "")
	// Convert version string to int, e.g. "6.7.3" to 673, "7.0.0.0" to 700.
	vSphereVersionInt, err := strconv.Atoi(apiVersion[0:3])
	if err != nil {
		return false, logger.LogNewErrorf(log,
			"Error while converting ApiVersion %q to integer, err %+v", apiVersion, err)
	}
	vSphere67u3VersionStr := strings.Join(strings.Split(VSphere67u3Version, "."), "")
	vSphere67u3VersionInt, err := strconv.Atoi(vSphere67u3VersionStr[0:3])
	if err != nil {
		return false, logger.LogNewErrorf(log,
			"Error while converting VSphere67u3Version %q to integer, err %+v", VSphere67u3Version, err)
	}
	vSphere7VersionStr := strings.Join(strings.Split(VSphere7Version, "."), "")
	vSphere7VersionInt, err := strconv.Atoi(vSphere7VersionStr[0:3])
	if err != nil {
		return false, logger.LogNewErrorf(log,
			"Error while converting VSphere7Version %q to integer, err %+v", VSphere7Version, err)
	}
	// Check if the current vSphere version is between 6.7.3 and 7.0.0.
	if vSphereVersionInt > vSphere67u3VersionInt && vSphereVersionInt <= vSphere7VersionInt {
		return true, nil
	}
	// Check if version is 6.7.3.
	if vSphereVersionInt == vSphere67u3VersionInt {
		// CSI migration feature will be supported only from 6.7 Update 3l vSphere
		// version onwards. For all older 6.7 Update 3 such as 3a, 3b and so on,
		// we do not support the CSI migration feature. Because there is no patch
		// version number in aboutInfo{}, we will rely on the build number to
		// check for 6.7 Update 3l. VC builds are always incremental and hence we
		// can use the build info to check if VC version is 6.7 Update 3l.
		//
		// Here is a snippet of the build info for the GA bits mentioned:
		// https://docs.vmware.com/en/VMware-vSphere/6.7/rn/vsphere-vcenter-server-67u3l-release-notes.html
		// Name: "VMware vCenter Server",
		// FullName: "VMware vCenter Server 6.7.0 build-17137327",
		// Version: "6.7.0",
		// Build:  "17137327",
		// ApiVersion: "6.7.3",
		if vcBuild, err := strconv.Atoi(aboutInfo.Build); err == nil {
			if vcBuild >= VSphere67u3lBuildInfo {
				return true, nil
			}
			return false, logger.LogNewErrorf(log,
				"Found vCenter version :%q. The minimum version for CSI migration is vCenter Server 6.7 Update 3l",
				aboutInfo.ApiVersion)
		}
		if err != nil {
			return false, logger.LogNewErrorf(log,
				"Error while converting VC Build info %q to integer, err %+v", aboutInfo.Build, err)
		}
	}
	// For all other versions.
	return false, nil
}

// ValidateControllerExpandVolumeRequest is the helper function to validate
// ControllerExpandVolumeRequest for all block controllers.
// Function returns error if validation fails otherwise returns nil.
func ValidateControllerExpandVolumeRequest(ctx context.Context, req *csi.ControllerExpandVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Check for required parameters.
	if len(req.GetVolumeId()) == 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume id is a required parameter")
	} else if req.GetCapacityRange() == nil {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "capacity range is a required parameter")
	} else if req.GetCapacityRange().GetRequiredBytes() < 0 || req.GetCapacityRange().GetLimitBytes() < 0 {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "capacity ranges values cannot be negative")
	}
	// Validate Volume Capabilities.
	volCaps := req.GetVolumeCapability()
	if volCaps == nil {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "volume capabilities is a required parameter")
	}

	// TODO: Remove this restriction when volume expansion is supported for
	// File Volumes.
	if IsFileVolumeRequest(ctx, []*csi.VolumeCapability{volCaps}) {
		return logger.LogNewErrorCode(log, codes.Unimplemented,
			"volume expansion is only supported for block volume type")
	}

	return nil
}

// IsOnlineExpansion verifies if the input volume is attached to any of the
// given VirutalMachines, to prevent online expansion of volumes.
// Returns an error if the volume is attached.
func IsOnlineExpansion(ctx context.Context, volumeID string, nodes []*cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)
	diskUUID, err := cnsvolume.IsDiskAttachedToVMs(ctx, volumeID, nodes, false)
	if err != nil {
		return logger.LogNewErrorCodef(log, codes.Internal,
			"failed to check if volume %q is attached to any node with error: %+v", volumeID, err)
	} else if diskUUID != "" {
		return logger.LogNewErrorCodef(log, codes.FailedPrecondition,
			"failed to expand volume: %q. Volume is attached to node. Online volume expansion is not supported",
			volumeID)
	}

	return nil
}

// IsvSphere8AndAbove returns true if vSphere version if 8.0 and above
func IsvSphere8AndAbove(ctx context.Context, aboutInfo vim25types.AboutInfo) (bool, error) {
	log := logger.GetLogger(ctx)
	items := strings.Split(aboutInfo.ApiVersion, ".")
	apiVersion := strings.Join(items[:], "")
	// Convert version string to int, e.g. 8.0.0.0" to 800.
	vSphereMajorVersionInt, err := strconv.Atoi(string(apiVersion[0]))
	if err != nil {
		return false, logger.LogNewErrorf(log,
			"Error while converting ApiVersion %q to integer, err %+v", apiVersion, err)
	}

	// Check if the current vSphere version is greater 8
	if vSphereMajorVersionInt >= VSphere8VersionMajorInt {
		return true, nil
	}
	// For all other versions.
	return false, nil
}

// CheckPVtoBackingDiskObjectIdSupport internally checks if the vCenter version is 7.0.2.
// Support both vsan and vvol.
func CheckPVtoBackingDiskObjectIdSupport(ctx context.Context, vc *cnsvsphere.VirtualCenter) bool {
	log := logger.GetLogger(ctx)
	currentVcVersion := vc.Client.ServiceContent.About.ApiVersion
	err := CheckAPI(ctx, currentVcVersion, PVtoBackingDiskObjectIdSupportedVCenterMajor,
		PVtoBackingDiskObjectIdSupportedVCenterMinor, PVtoBackingDiskObjectIdSupportedVCenterPatch)
	if err != nil {
		log.Errorf("checkAPI failed for PV to BackingDiskObjectId mapping support on vCenter API version: %s, err=%v",
			currentVcVersion, err)
		return false
	}
	// vCenter version supported.
	log.Infof("vCenter API version: %s supports CNS PV to BackingDiskObjectId mapping.", currentVcVersion)
	return true
}

// IsVolumeSnapshotReady return true if the VolumeSnapshot is ReadyToUse
func IsVolumeSnapshotReady(ctx context.Context, client snapshotterClientSet.Interface,
	supervisorVolumeSnapshotName string, namespace string, timeout time.Duration) (bool, *snap.VolumeSnapshot, error) {
	log := logger.GetLogger(ctx)
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Waiting up to %d seconds for VolumeSnapshot %v in namespace %s to be ReadyToUse",
		timeoutSeconds, supervisorVolumeSnapshotName, namespace)
	startTime := time.Now()
	isReadyToUse := false
	var svs *snap.VolumeSnapshot

	waitErr := wait.PollImmediate(5*time.Second, timeout, func() (done bool, err error) {
		svs, err := client.SnapshotV1().VolumeSnapshots(namespace).
			Get(ctx, supervisorVolumeSnapshotName, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Sprintf("unable to fetch volumesnapshot %q/%q "+
				"from supervisor cluster with err: %+v",
				namespace, supervisorVolumeSnapshotName, err)
			log.Warnf(msg)
			return false, logger.LogNewErrorf(log, msg)
		}
		isSnapshotReadyToUse := *svs.Status.ReadyToUse
		if isSnapshotReadyToUse {
			log.Infof("VolumeSnapshot %s/%s is in ReadyToUse state", namespace, supervisorVolumeSnapshotName)
			isReadyToUse = true
			return true, nil
		} else {
			log.Warnf("Waiting for VolumeSnapshot %s/%s to be ready since %+vs", namespace,
				supervisorVolumeSnapshotName, time.Since(startTime).Seconds())
		}
		return false, nil
	})

	if !isReadyToUse {
		msg := fmt.Sprintf("volumesnapshot %s in namespace %s not in ReadyToUse "+
			"within %d seconds", supervisorVolumeSnapshotName, namespace, timeoutSeconds)
		if waitErr != nil {
			msg += fmt.Sprintf(": message: %v", waitErr.Error())
		}
		return false, nil, fmt.Errorf(msg)
	}

	return true, svs, nil
}
