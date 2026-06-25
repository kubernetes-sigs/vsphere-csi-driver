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

package csivolumeinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// conditionTypeReady is the standard Ready condition type on CsiVolumeInfo.
	// It mirrors the condition vocabulary written by the reconciler so recovery
	// produces an identical status shape.
	conditionTypeReady = "Ready"
	// reasonUnregisterSucceeded is the Ready-condition reason after a completed unregister.
	reasonUnregisterSucceeded = "UnregisterSucceeded"
)

// RecoverPendingUnregisters is called once at CSI syncer startup to complete
// any two-phase FCD unregister operations that were interrupted by a crash.
//
// Each PENDING_UNREGISTER record from CNS represents an FCD that was removed
// from the catalog but whose acknowledgement was not yet sent. This function
// writes the disk coordinates to the CsiVolumeInfo spec, advances its status
// to VMManaged, then sends the acknowledgement to clean up the CNS row.
//
// Per-record errors are logged and do not abort recovery for subsequent
// records. The function is idempotent: re-running on an already-recovered
// volume is a safe no-op.
func RecoverPendingUnregisters(ctx context.Context,
	volumeManager volumes.Manager,
	cviSvc CsiVolumeInfoService,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("RecoverPendingUnregisters: scanning for incomplete two-phase unregister operations")

	records, err := volumeManager.QueryPendingUnregisters(ctx)
	if err != nil {
		return fmt.Errorf("RecoverPendingUnregisters: QueryPendingUnregisters failed: %w", err)
	}
	if len(records) == 0 {
		log.Infof("RecoverPendingUnregisters: no pending unregister records found")
		return nil
	}
	log.Infof("RecoverPendingUnregisters: found %d pending record(s)", len(records))

	recovered := 0
	for _, rec := range records {
		if recErr := recoverOneRecord(ctx, volumeManager, cviSvc, rec); recErr != nil {
			log.Errorf("RecoverPendingUnregisters: failed to recover volume %q: %v",
				rec.VolumeID, recErr)
			continue
		}
		recovered++
	}
	log.Infof("RecoverPendingUnregisters: completed — recovered %d of %d record(s)",
		recovered, len(records))
	return nil
}

// recoverOneRecord handles crash-recovery for a single PENDING_UNREGISTER record.
func recoverOneRecord(
	ctx context.Context,
	volumeManager volumes.Manager,
	cviSvc CsiVolumeInfoService,
	rec volumes.PendingUnregisterRecord,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("recoverOneRecord: recovering volume %q (diskPath=%q, diskUUID=%q)",
		rec.VolumeID, rec.BackingDiskPath, rec.DiskUUID)

	cvi, err := cviSvc.GetCsiVolumeInfo(ctx, rec.VolumeID)
	if err != nil {
		return fmt.Errorf("recoverOneRecord: failed to get CsiVolumeInfo for volume %q: %w",
			rec.VolumeID, err)
	}

	if cvi == nil {
		// No CsiVolumeInfo found: the volume was deleted while CSI was down.
		// Acknowledge the CNS row to clean up the stale PENDING_UNREGISTER entry.
		log.Warnf("recoverOneRecord: no CsiVolumeInfo found for volume %q; "+
			"acknowledging CNS row to clean up orphan record", rec.VolumeID)
		if ackErr := volumeManager.AckUnregister(ctx, rec.VolumeID); ackErr != nil {
			return fmt.Errorf("recoverOneRecord: AckUnregister failed for orphan volume %q: %w",
				rec.VolumeID, ackErr)
		}
		return nil
	}

	// If the CVI is already VMManaged, the crash occurred after the status
	// was written but before the acknowledgement. The double-ACK is safe.
	if cvi.Status.Ownership == csivolumeinfov1alpha1.OwnershipStateVMManaged {
		log.Infof("recoverOneRecord: CsiVolumeInfo for volume %q is already VMManaged; "+
			"sending acknowledgement (double-ACK is safe)", rec.VolumeID)
		if ackErr := volumeManager.AckUnregister(ctx, rec.VolumeID); ackErr != nil {
			return fmt.Errorf("recoverOneRecord: AckUnregister (double) failed for volume %q: %w",
				rec.VolumeID, ackErr)
		}
		return nil
	}

	// The crash happened before or during the spec/status write. Persist disk
	// coordinates in spec, then advance status to VMManaged, then ACK.
	//
	// A spec patch increments metadata.generation, so observedGeneration must be
	// taken from the post-patch generation (returned by PatchCsiVolumeInfo) rather
	// than the value read before the patch; otherwise vm-operator's green-signal
	// check would never be satisfied.
	gen := cvi.Generation
	if rec.BackingDiskPath != "" || rec.DiskUUID != "" {
		specPatch := map[string]interface{}{
			"spec": map[string]interface{}{
				"diskPath": rec.BackingDiskPath,
				"diskUUID": rec.DiskUUID,
			},
		}
		patchBytes, marshalErr := json.Marshal(specPatch)
		if marshalErr != nil {
			return fmt.Errorf("recoverOneRecord: failed to marshal spec patch for volume %q: %w",
				rec.VolumeID, marshalErr)
		}
		patchedGen, patchErr := cviSvc.PatchCsiVolumeInfo(ctx, rec.VolumeID, patchBytes)
		if patchErr != nil {
			return fmt.Errorf("recoverOneRecord: failed to patch spec for volume %q: %w",
				rec.VolumeID, patchErr)
		}
		gen = patchedGen
		log.Infof("recoverOneRecord: persisted diskPath and diskUUID in spec for volume %q (generation=%d)",
			rec.VolumeID, gen)
	}

	// Advance status to VMManaged, mirroring the steady-state unregister status
	// shape (ownership, phase, observedGeneration, cleared error, Ready condition).
	statusPatch := map[string]interface{}{
		"status": map[string]interface{}{
			"ownership":          string(csivolumeinfov1alpha1.OwnershipStateVMManaged),
			"phase":              string(csivolumeinfov1alpha1.PhaseSucceeded),
			"observedGeneration": gen,
			"error":              "",
			"conditions": []interface{}{
				map[string]interface{}{
					"type":               conditionTypeReady,
					"status":             string(metav1.ConditionTrue),
					"reason":             reasonUnregisterSucceeded,
					"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
				},
			},
		},
	}
	statusBytes, marshalErr := json.Marshal(statusPatch)
	if marshalErr != nil {
		return fmt.Errorf("recoverOneRecord: failed to marshal status patch for volume %q: %w",
			rec.VolumeID, marshalErr)
	}
	if patchErr := cviSvc.PatchCsiVolumeInfoStatus(ctx, rec.VolumeID, statusBytes); patchErr != nil {
		return fmt.Errorf("recoverOneRecord: failed to patch status for volume %q: %w",
			rec.VolumeID, patchErr)
	}
	log.Infof("recoverOneRecord: advanced CsiVolumeInfo for volume %q to VMManaged", rec.VolumeID)

	// Send the acknowledgement to delete the PENDING_UNREGISTER row.
	if ackErr := volumeManager.AckUnregister(ctx, rec.VolumeID); ackErr != nil {
		return fmt.Errorf("recoverOneRecord: AckUnregister failed for volume %q: %w",
			rec.VolumeID, ackErr)
	}
	log.Infof("recoverOneRecord: completed recovery for volume %q", rec.VolumeID)
	return nil
}
