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

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// ReconcileCsiVolumeInfoOnPVBind is called when a PV transitions from
// Available to Bound. When the VMOwnedVolumes capability is active, this
// function ensures the CsiVolumeInfo CR for the volume has up-to-date
// spec.pvName, spec.pvcName, and spec.pvcNamespace fields.
//
// The function is idempotent: if no CsiVolumeInfo CR exists for the volume
// (e.g., pre-existing PVs created before the feature was enabled), it logs
// a debug message and returns nil. Errors from the patch are returned so
// the caller can decide whether to retry.
func ReconcileCsiVolumeInfoOnPVBind(
	ctx context.Context,
	oldPV, newPV *v1.PersistentVolume,
	svc csivolumeinfosvc.CsiVolumeInfoService,
) error {
	log := logger.GetLogger(ctx)

	// Only act on the Available → Bound transition.
	if oldPV.Status.Phase == newPV.Status.Phase {
		return nil
	}
	if newPV.Status.Phase != v1.VolumeBound {
		return nil
	}

	// Only handle vSphere CSI volumes.
	if newPV.Spec.CSI == nil || newPV.Spec.CSI.Driver != csitypes.Name {
		return nil
	}

	volumeID := newPV.Spec.CSI.VolumeHandle
	if volumeID == "" {
		log.Warnf("ReconcileCsiVolumeInfoOnPVBind: PV %q has empty volumeHandle; skipping", newPV.Name)
		return nil
	}

	// Retrieve the CsiVolumeInfo CR. A nil result means the CR does not exist.
	cvi, err := svc.GetCsiVolumeInfo(ctx, volumeID)
	if err != nil {
		log.Errorf("ReconcileCsiVolumeInfoOnPVBind: failed to get CsiVolumeInfo for volumeID=%q pv=%q: %v",
			volumeID, newPV.Name, err)
		return err
	}
	if cvi == nil {
		log.Debugf("ReconcileCsiVolumeInfoOnPVBind: no CsiVolumeInfo found for volumeID=%q pv=%q; skipping",
			volumeID, newPV.Name)
		return nil
	}

	// Derive desired PVC name/namespace from the PV's ClaimRef.
	var desiredPVCName, desiredPVCNamespace string
	if newPV.Spec.ClaimRef != nil {
		desiredPVCName = newPV.Spec.ClaimRef.Name
		desiredPVCNamespace = newPV.Spec.ClaimRef.Namespace
	}

	// Skip the patch if nothing has changed.
	if cvi.Spec.PVName == newPV.Name &&
		(desiredPVCName == "" || cvi.Spec.PVCName == desiredPVCName) &&
		(desiredPVCNamespace == "" || cvi.Spec.PVCNamespace == desiredPVCNamespace) {
		log.Debugf("ReconcileCsiVolumeInfoOnPVBind: CsiVolumeInfo for volumeID=%q already up-to-date; skipping",
			volumeID)
		return nil
	}

	// Build a merge-patch with only the fields that need updating.
	type specPatch struct {
		PVName       string `json:"pvName,omitempty"`
		PVCName      string `json:"pvcName,omitempty"`
		PVCNamespace string `json:"pvcNamespace,omitempty"`
	}
	type patch struct {
		Spec specPatch `json:"spec"`
	}
	p := patch{
		Spec: specPatch{
			PVName:       newPV.Name,
			PVCName:      desiredPVCName,
			PVCNamespace: desiredPVCNamespace,
		},
	}
	patchBytes, err := json.Marshal(p)
	if err != nil {
		log.Errorf("ReconcileCsiVolumeInfoOnPVBind: failed to marshal patch for volumeID=%q pv=%q: %v",
			volumeID, newPV.Name, err)
		return err
	}

	log.Infof("ReconcileCsiVolumeInfoOnPVBind: patching CsiVolumeInfo for volumeID=%q pv=%q pvcName=%q pvcNamespace=%q",
		volumeID, newPV.Name, desiredPVCName, desiredPVCNamespace)
	if err := svc.PatchCsiVolumeInfo(ctx, volumeID, patchBytes); err != nil {
		log.Errorf("ReconcileCsiVolumeInfoOnPVBind: failed to patch CsiVolumeInfo for volumeID=%q pv=%q: %v",
			volumeID, newPV.Name, err)
		return err
	}

	log.Infof("ReconcileCsiVolumeInfoOnPVBind: successfully patched CsiVolumeInfo for volumeID=%q pv=%q",
		volumeID, newPV.Name)
	return nil
}
