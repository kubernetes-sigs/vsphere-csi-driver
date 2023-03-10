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

package wcpguest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snap "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// Default timeout for provision, used unless overridden by user in
	// csi-controller YAML.
	defaultProvisionTimeoutInMin = 4

	// Timeout for attach and detach operation for watching on VirtualMachines
	// instances, used unless overridden by user in csi-controller YAML.
	defaultAttacherTimeoutInMin = 4

	// Default timeout for resize, used unless overridden by user in
	// csi-controller YAML.
	defaultResizeTimeoutInMin = 4
)

// validateGuestClusterCreateVolumeRequest is the helper function to validate
// CreateVolumeRequest for Guest Cluster CSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateGuestClusterCreateVolumeRequest(ctx context.Context, req *csi.CreateVolumeRequest) error {
	log := logger.GetLogger(ctx)
	// Validate Name length of volumeName is > 4, eg: pvc-xxxxx
	if len(req.Name) <= 4 {
		return logger.LogNewErrorCodef(log, codes.InvalidArgument, "Volume name %s is not valid", req.Name)
	}
	tkgsHAEnabled := commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.TKGsHA)
	// Get create params
	for param, val := range req.GetParameters() {
		switch strings.ToLower(param) {
		case common.AttributeSupervisorStorageClass:
			// Validate if the req contains non-empty common.AttributeSupervisorStorageClass
			if val == "" {
				return logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"volume parameter %s is not set in the CreateVolume request",
					common.AttributeSupervisorStorageClass)
			}
		case common.AttributeStorageTopologyType:
			if tkgsHAEnabled {
				storageTopologyTypeVal := strings.ToLower(val)
				if storageTopologyTypeVal != "" && storageTopologyTypeVal != "zonal" {
					return logger.LogNewErrorCodef(log, codes.InvalidArgument,
						"invalid value %q received for %q parameter.", val, param)
				}
			} else {
				return logger.LogNewErrorCodef(log, codes.InvalidArgument,
					"Volume parameter %s is not a valid GC CSI parameter", param)
			}
		default:
			return logger.LogNewErrorCodef(log, codes.InvalidArgument,
				"Volume parameter %s is not a valid GC CSI parameter", param)
		}
	}

	// Fail file volume creation if file volume feature gate is disabled
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.FileVolume) &&
		common.IsFileVolumeRequest(ctx, req.GetVolumeCapabilities()) {
		return logger.LogNewErrorCode(log, codes.InvalidArgument, "File volume provisioning is not supported.")
	}
	return common.ValidateCreateVolumeRequest(ctx, req)
}

// validateGuestClusterDeleteVolumeRequest is the helper function to validate
// DeleteVolumeRequest for pvCSI driver.
// Function returns error if validation fails otherwise returns nil.
func validateGuestClusterDeleteVolumeRequest(ctx context.Context, req *csi.DeleteVolumeRequest) error {
	return common.ValidateDeleteVolumeRequest(ctx, req)
}

// validateGuestClusterControllerPublishVolumeRequest is the helper function to validate
// pvcsi ControllerPublishVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateGuestClusterControllerPublishVolumeRequest(ctx context.Context,
	req *csi.ControllerPublishVolumeRequest) error {
	return common.ValidateControllerPublishVolumeRequest(ctx, req)
}

// validateGuestClusterControllerUnpublishVolumeRequest is the helper function to validate
// pvcsi ControllerUnpublishVolumeRequest. Function returns error if validation fails otherwise returns nil.
func validateGuestClusterControllerUnpublishVolumeRequest(ctx context.Context,
	req *csi.ControllerUnpublishVolumeRequest) error {
	return common.ValidateControllerUnpublishVolumeRequest(ctx, req)
}

func validateGuestClusterControllerExpandVolumeRequest(ctx context.Context,
	req *csi.ControllerExpandVolumeRequest) error {
	return common.ValidateControllerExpandVolumeRequest(ctx, req)
}

// checkForSupervisorPVCCondition returns nil if the PVC condition is set as
// required in the supervisor cluster before timeout, otherwise returns error.
func checkForSupervisorPVCCondition(ctx context.Context, client clientset.Interface,
	claim *v1.PersistentVolumeClaim, reqCondition v1.PersistentVolumeClaimConditionType, timeout time.Duration) error {
	log := logger.GetLogger(ctx)
	pvcName := claim.Name
	ns := claim.Namespace
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Waiting up to %d seconds for supervisor PersistentVolumeClaim %s in namespace %s to have %s condition",
		timeoutSeconds, pvcName, ns, reqCondition)
	watchClaim, err := client.CoreV1().PersistentVolumeClaims(ns).Watch(
		ctx,
		metav1.ListOptions{
			FieldSelector:  fields.OneTermEqualSelector("metadata.name", pvcName).String(),
			TimeoutSeconds: &timeoutSeconds,
			Watch:          true,
		})
	if err != nil {
		errMsg := fmt.Errorf("failed to watch supervisor PersistentVolumeClaim %s in namespace %s with Error: %+v",
			pvcName, ns, err)
		log.Error(errMsg)
		return errMsg
	}
	defer watchClaim.Stop()

	for event := range watchClaim.ResultChan() {
		pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		if checkPVCCondition(ctx, pvc, reqCondition) {
			return nil
		}
	}
	return fmt.Errorf("supervisor persistentVolumeClaim %s in namespace %s not in %q condition within %d seconds",
		pvcName, ns, reqCondition, timeoutSeconds)
}

func checkPVCCondition(ctx context.Context, pvc *v1.PersistentVolumeClaim,
	reqCondition v1.PersistentVolumeClaimConditionType) bool {
	log := logger.GetLogger(ctx)
	for _, condition := range pvc.Status.Conditions {
		log.Debugf("PersistentVolumeClaim %s in namespace %s is in %s condition", pvc.Name, pvc.Namespace, condition.Type)
		if condition.Type == reqCondition {
			log.Infof("PersistentVolumeClaim %s in namespace %s is in %s condition",
				pvc.Name, pvc.Namespace, condition.Type)
			return true
		}
	}
	return false
}

// getAccessMode returns the PersistentVolumeAccessMode for the PVC Spec given VolumeCapability_AccessMode
func getAccessMode(accessMode csi.VolumeCapability_AccessMode_Mode) v1.PersistentVolumeAccessMode {
	switch accessMode {
	case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:
		return v1.ReadWriteOnce
	case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return v1.ReadWriteMany
	case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:
		return v1.ReadOnlyMany
	default:
		return v1.ReadWriteOnce
	}
}

// getPersistentVolumeClaimSpecWithStorageClass return the PersistentVolumeClaim spec with specified storage class
func getPersistentVolumeClaimSpecWithStorageClass(pvcName string, namespace string, diskSize string,
	storageClassName string, pvcAccessMode v1.PersistentVolumeAccessMode, annotations map[string]string,
	volumeSnapshotName string) *v1.PersistentVolumeClaim {
	claim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvcName,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				pvcAccessMode,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): resource.MustParse(diskSize),
				},
			},
			StorageClassName: &storageClassName,
		},
	}
	snapshotApiGroup := common.VolumeSnapshotApiGroup
	volumeSnapshotKind := common.VolumeSnapshotKind
	if volumeSnapshotName != "" {
		localObjectReference := &v1.TypedLocalObjectReference{
			APIGroup: &snapshotApiGroup,
			Kind:     volumeSnapshotKind,
			Name:     volumeSnapshotName,
		}
		claim.Spec.DataSource = localObjectReference
	}
	return claim
}

func constructVolumeSnapshotWithVolumeSnapshotClass(volumeSnapshotName string, namespace string,
	volumeSnapshotClassName string, pvcName string) *snap.VolumeSnapshot {
	volumeSnapshot := &snap.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volumeSnapshotName,
			Namespace: namespace,
		},
		Spec: snap.VolumeSnapshotSpec{
			Source: snap.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
			VolumeSnapshotClassName: &volumeSnapshotClassName,
		},
		Status: nil,
	}
	return volumeSnapshot
}

// generateGuestClusterRequestedTopologyJSON translates the topology into a json string to be set on the supervisor
// PVC
func generateGuestClusterRequestedTopologyJSON(topologies []*csi.Topology) (string, error) {
	segmentsArray := make([]string, 0)
	for _, topology := range topologies {
		jsonSegment, err := json.Marshal(topology.Segments)
		if err != nil {
			return "", fmt.Errorf("failed to marshal topology segment: %v to json. Err: %v", topology.Segments, err)
		}
		segmentsArray = append(segmentsArray, string(jsonSegment))
	}
	return "[" + strings.Join(segmentsArray, ",") + "]", nil
}

// generateVolumeAccessibleTopologyFromPVCAnnotation returns accessible topologies generated using
// PVC annotation "csi.vsphere.volume-accessible-topology".
func generateVolumeAccessibleTopologyFromPVCAnnotation(claim *v1.PersistentVolumeClaim) (
	[]map[string]string, error) {
	volumeAccessibleTopology := claim.Annotations[common.AnnVolumeAccessibleTopology]
	if volumeAccessibleTopology == "" {
		return nil, fmt.Errorf("annotation %q is not set for the claim: %q, namespace: %q",
			common.AnnVolumeAccessibleTopology, claim.Name, claim.Namespace)
	}
	volumeAccessibleTopologyArray := make([]map[string]string, 0)
	err := json.Unmarshal([]byte(volumeAccessibleTopology), &volumeAccessibleTopologyArray)
	if err != nil {
		return nil, fmt.Errorf("failed to parse annotation: %q value %v from the claim: %q, namespace: %q. "+
			"err: %v", common.AnnVolumeAccessibleTopology, volumeAccessibleTopology,
			claim.Name, claim.Namespace, err)
	}
	return volumeAccessibleTopologyArray, nil
}

// isPVCInSupervisorClusterBound return true if the PVC is bound in the
// supervisor cluster before timeout, otherwise return false.
func isPVCInSupervisorClusterBound(ctx context.Context, client clientset.Interface,
	claim *v1.PersistentVolumeClaim, timeout time.Duration) (bool, error) {
	log := logger.GetLogger(ctx)
	pvcName := claim.Name
	ns := claim.Namespace
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Waiting up to %d seconds for PersistentVolumeClaim %v in namespace %s to have phase %s",
		timeoutSeconds, pvcName, ns, v1.ClaimBound)
	watchClaim, err := client.CoreV1().PersistentVolumeClaims(ns).Watch(
		ctx,
		metav1.ListOptions{
			FieldSelector:  fields.OneTermEqualSelector("metadata.name", pvcName).String(),
			TimeoutSeconds: &timeoutSeconds,
			Watch:          true,
		})
	if err != nil {
		errMsg := fmt.Errorf("failed to watch PersistentVolumeClaim %s with Error: %v", pvcName, err)
		log.Error(errMsg)
		return false, errMsg
	}
	defer watchClaim.Stop()

	for event := range watchClaim.ResultChan() {
		pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		log.Debugf("PersistentVolumeClaim %s in namespace %s is in state %s. Received event %v",
			pvcName, ns, pvc.Status.Phase, event)
		if pvc.Status.Phase == v1.ClaimBound && pvc.Name == pvcName {
			log.Infof("PersistentVolumeClaim %s in namespace %s is in state %s", pvcName, ns, pvc.Status.Phase)
			return true, nil
		}
	}
	return false, fmt.Errorf("persistentVolumeClaim %s in namespace %s not in phase %s within %d seconds",
		pvcName, ns, v1.ClaimBound, timeoutSeconds)
}

// getProvisionTimeoutInMin() return the timeout for volume provision.
// If environment variable PROVISION_TIMEOUT_MINUTES is set and valid,
// return the interval value read from environment variable
// otherwise, use the default timeout 4 mins
func getProvisionTimeoutInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	provisionTimeoutInMin := defaultProvisionTimeoutInMin
	if v := os.Getenv("PROVISION_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf(" provisionTimeout set in env variable PROVISION_TIMEOUT_MINUTES %s "+
					"is equal or less than 0, will use the default timeout", v)
			} else {
				provisionTimeoutInMin = value
				log.Infof("provisionTimeout is set to %d minutes", provisionTimeoutInMin)
			}
		} else {
			log.Warnf("provisionTimeout set in env variable PROVISION_TIMEOUT_MINUTES %s is invalid, "+
				"will use the default timeout", v)
		}
	}
	return provisionTimeoutInMin
}

// getResizeTimeoutInMin returns the timeout for volume resize.
// If environment variable RESIZE_TIMEOUT_MINUTES is set and valid,
// return the interval value read from environment variable
// otherwise, use the default timeout 4 mins
func getResizeTimeoutInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	resizeTimeoutInMin := defaultResizeTimeoutInMin
	if v := os.Getenv("RESIZE_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("resizeTimeout set in env variable RESIZE_TIMEOUT_MINUTES %s is equal or less than 0, "+
					"will use the default timeout of %d minutes", v, resizeTimeoutInMin)
			} else {
				resizeTimeoutInMin = value
				log.Infof("resizeTimeout is set to %d minutes", resizeTimeoutInMin)
			}
		} else {
			log.Warnf("resizeTimeout set in env variable RESIZE_TIMEOUT_MINUTES %s is invalid, "+
				"will use the default timeout of %d minutes", v, resizeTimeoutInMin)
		}
	}
	return resizeTimeoutInMin
}

// getAttacherTimeoutInMin() return the timeout for volume attach and detach.
// If environment variable ATTACHER_TIMEOUT_MINUTES is set and valid,
// return the interval value read from environment variable
// otherwise, use the default timeout 4 mins
func getAttacherTimeoutInMin(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	attacherTimeoutInMin := defaultAttacherTimeoutInMin
	if v := os.Getenv("ATTACHER_TIMEOUT_MINUTES"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("attacherTimeout set in env variable ATTACHER_TIMEOUT_MINUTES %s is equal or less than 0, "+
					"will use the default timeout", v)
			} else {
				attacherTimeoutInMin = value
				log.Infof("attacherTimeout is set to %d minutes", attacherTimeoutInMin)
			}
		} else {
			log.Warnf("attacherTimeout set in env variable ATTACHER_TIMEOUT_MINUTES %s is invalid, "+
				"will use the default timeout", v)
		}
	}
	return attacherTimeoutInMin
}

func constructListSnapshotEntry(vs snap.VolumeSnapshot) *csi.ListSnapshotsResponse_Entry {
	snapshotCreateTimeInProto := timestamppb.New(vs.Status.CreationTime.Time)
	snapshotSize := vs.Status.RestoreSize.Value()
	volumeID := *vs.Spec.Source.PersistentVolumeClaimName
	csiSnapshotInfo := &csi.Snapshot{
		SnapshotId:     vs.Name,
		SourceVolumeId: volumeID,
		CreationTime:   snapshotCreateTimeInProto,
		SizeBytes:      snapshotSize,
		ReadyToUse:     true,
	}
	entry := &csi.ListSnapshotsResponse_Entry{
		Snapshot: csiSnapshotInfo,
	}
	return entry
}
