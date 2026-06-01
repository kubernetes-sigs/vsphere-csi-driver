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

	// CVI exists — check whether ownerReference and pvcName are up to date.
	needsOwnerRef := !hasPVOwnerReference(existing, pvUID)
	needsPVCName := existing.Spec.PVCName != newPVCName

	if !needsOwnerRef && !needsPVCName {
		return
	}

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
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		log.Errorf("reconcileCVIOnPVBind: failed to marshal patch for CsiVolumeInfo %q/%q: %v",
			newNS, existing.Name, err)
		return
	}
	if patchErr := cviSvc.PatchCsiVolumeInfo(ctx, newNS, volumeID, patchBytes); patchErr != nil {
		log.Errorf("reconcileCVIOnPVBind: failed to patch CsiVolumeInfo for volume %q: %v",
			volumeID, patchErr)
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
