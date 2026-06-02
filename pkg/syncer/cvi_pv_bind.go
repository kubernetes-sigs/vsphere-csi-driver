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

package syncer

import (
	"context"
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// reconcileCVIOnPVBind is called when a PersistentVolume transitions to Bound.
// It ensures the corresponding CsiVolumeInfo CR exists with the PV ownerReference
// set, creating or patching it as needed. It also handles same-namespace and
// cross-namespace rebind under the Retain reclaim policy.
func reconcileCVIOnPVBind(ctx context.Context, oldPV, newPV *v1.PersistentVolume,
	volumeManager volumes.Manager) {
	log := logger.GetLogger(ctx)

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		return
	}
	if newPV.Spec.CSI == nil || newPV.Spec.CSI.Driver != csitypes.Name {
		return
	}
	if newPV.Spec.ClaimRef == nil {
		return
	}
	if newPV.Status.Phase != v1.VolumeBound {
		return
	}

	volumeID := newPV.Spec.CSI.VolumeHandle
	pvName := newPV.Name
	pvUID := string(newPV.UID)
	newNS := newPV.Spec.ClaimRef.Namespace
	newPVCName := newPV.Spec.ClaimRef.Name

	cviSvc, err := csivolumeinfo.InitCsiVolumeInfoService(ctx)
	if err != nil {
		log.Errorf("reconcileCVIOnPVBind: failed to get CsiVolumeInfo service: %v", err)
		return
	}

	// Detect namespace change for cross-namespace Retain rebind.
	oldNS := ""
	if oldPV != nil && oldPV.Spec.ClaimRef != nil {
		oldNS = oldPV.Spec.ClaimRef.Namespace
	}
	if oldNS != "" && oldNS != newNS {
		// Cross-namespace rebind: remove the stale CVI from the old namespace.
		if delErr := cviSvc.DeleteCsiVolumeInfo(ctx, oldNS, volumeID); delErr != nil {
			log.Warnf("reconcileCVIOnPVBind: failed to delete stale CsiVolumeInfo in "+
				"old namespace %q for volume %q: %v", oldNS, volumeID, delErr)
		}
	}

	existing, err := cviSvc.GetCsiVolumeInfo(ctx, newNS, volumeID)
	if err != nil {
		log.Errorf("reconcileCVIOnPVBind: failed to look up CsiVolumeInfo for volume %q "+
			"in namespace %q: %v", volumeID, newNS, err)
		return
	}

	ownerRef := metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "PersistentVolume",
		Name:               pvName,
		UID:                k8stypes.UID(pvUID),
		Controller:         boolPtr(true),
		BlockOwnerDeletion: boolPtr(true),
	}

	if existing == nil {
		// CVI was not created at provisioning time (e.g., crash recovery).
		// Create it now with the ownerReference already included.
		diskUUID, diskPath, fcdErr := common.QueryFCDBackingInfo(ctx, volumeManager, volumeID)
		if fcdErr != nil {
			log.Errorf("reconcileCVIOnPVBind: failed to query FCD backing info for volume %q: %v",
				volumeID, fcdErr)
			return
		}
		cvi := csivolumeinfo.BuildCsiVolumeInfo(
			volumeID, newPVCName, newNS, pvName, pvUID, diskUUID, diskPath)
		if createErr := cviSvc.CreateCsiVolumeInfo(ctx, cvi); createErr != nil {
			log.Errorf("reconcileCVIOnPVBind: failed to create CsiVolumeInfo for volume %q: %v",
				volumeID, createErr)
		}
		return
	}

	// CVI exists — check whether ownerReference, pvcName, or status fields need
	// updating. The status.diskUUID may be empty if QueryFCDBackingInfo raced
	// at provisioning time or if the status subresource write was skipped.
	needsOwnerRef := !hasPVOwnerReference(existing, pvUID)
	needsPVCName := existing.Spec.PVCName != newPVCName
	needsStatus := existing.Status.DiskUUID == ""

	// Fetch FCD backing info only when the status needs to be filled in.
	var diskUUID, diskPath string
	if needsStatus {
		var fcdErr error
		diskUUID, diskPath, fcdErr = common.QueryFCDBackingInfo(ctx, volumeManager, volumeID)
		if fcdErr != nil {
			log.Warnf("reconcileCVIOnPVBind: failed to query FCD backing info for volume %q "+
				"(status will remain incomplete): %v", volumeID, fcdErr)
			needsStatus = false
		}
	}

	if !needsOwnerRef && !needsPVCName && !needsStatus {
		return
	}

	// Apply spec/metadata patch first (ownerRef, pvcName).
	if needsOwnerRef || needsPVCName {
		patch := map[string]interface{}{}
		if needsOwnerRef {
			patch["metadata"] = map[string]interface{}{
				"ownerReferences": []metav1.OwnerReference{ownerRef},
			}
		}
		if needsPVCName {
			patch["spec"] = map[string]interface{}{
				"pvcName": newPVCName,
			}
		}
		patchBytes, marshalErr := json.Marshal(patch)
		if marshalErr != nil {
			log.Errorf("reconcileCVIOnPVBind: failed to marshal patch for CsiVolumeInfo %q/%q: %v",
				newNS, existing.Name, marshalErr)
			return
		}
		if patchErr := cviSvc.PatchCsiVolumeInfo(ctx, newNS, volumeID, patchBytes); patchErr != nil {
			log.Errorf("reconcileCVIOnPVBind: failed to patch CsiVolumeInfo for volume %q: %v",
				volumeID, patchErr)
		}
	}

	// Backfill status when diskUUID was empty at provisioning time.
	// Use a status patch (not Update) to avoid resourceVersion conflicts caused
	// by the spec/metadata patch that may have just incremented the version above.
	if needsStatus {
		ownershipState := existing.Status.OwnershipState
		if ownershipState == "" {
			ownershipState = csivolumeinfov1alpha1.OwnershipStateCSIManaged
		}
		statusPatch := map[string]interface{}{
			"status": map[string]interface{}{
				"diskUUID":       diskUUID,
				"diskPath":       diskPath,
				"ownershipState": string(ownershipState),
			},
		}
		statusBytes, marshalErr := json.Marshal(statusPatch)
		if marshalErr != nil {
			log.Warnf("reconcileCVIOnPVBind: failed to marshal status patch for CsiVolumeInfo %q/%q: %v",
				newNS, existing.Name, marshalErr)
		} else if statusErr := cviSvc.PatchCsiVolumeInfoStatus(ctx, newNS, volumeID, statusBytes); statusErr != nil {
			log.Warnf("reconcileCVIOnPVBind: failed to backfill status for CsiVolumeInfo %q/%q: %v",
				newNS, existing.Name, statusErr)
		}
		// Patch the disk-uuid label in metadata to keep the API-server index consistent.
		labelPatch := map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": map[string]string{
					csivolumeinfov1alpha1.LabelDiskUUID: diskUUID,
				},
			},
		}
		labelBytes, marshalErr := json.Marshal(labelPatch)
		if marshalErr == nil {
			if patchErr := cviSvc.PatchCsiVolumeInfo(ctx, newNS, volumeID, labelBytes); patchErr != nil {
				log.Warnf("reconcileCVIOnPVBind: failed to patch disk-uuid label for CsiVolumeInfo %q/%q: %v",
					newNS, existing.Name, patchErr)
			}
		}
	}
}

// hasPVOwnerReference reports whether the given CVI already carries an
// ownerReference for a PersistentVolume with the supplied UID.
func hasPVOwnerReference(cvi *csivolumeinfov1alpha1.CsiVolumeInfo, pvUID string) bool {
	for _, ref := range cvi.OwnerReferences {
		if ref.Kind == "PersistentVolume" && string(ref.UID) == pvUID {
			return true
		}
	}
	return false
}

func boolPtr(b bool) *bool { return &b }
