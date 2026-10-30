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

package cnsvolumeinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsvolumeinfoconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/config"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// maxRestoreWriteAttempts bounds the conditional writes of a restored CnsVolumeInfo. Every attempt
// re-reads the CnsVolumeInfo, so only concurrent writers use attempts up.
const maxRestoreWriteAttempts = 5

// RestoredVolumeInfo describes the CnsVolumeInfo of a volume restored by SnapService.
// All fields except Capacity are required.
type RestoredVolumeInfo struct {
	// VolumeID is the FCD ID of the restored volume. Restore always reuses the deleted volume's ID.
	VolumeID string
	// Namespace is the namespace of the restored PVC. The CnsDeletedVolumeInfo must charge it.
	Namespace        string
	StoragePolicyID  string
	StorageClassName string
	VCenter          string
	Capacity         *resource.Quantity
	// RestoredBy is the UID of the CnsRegisterVolume that restored the volume.
	RestoredBy string
}

// RestoreResult reports the SnapshotSizeRestored condition that the restore left on the CnsVolumeInfo.
type RestoreResult struct {
	// Seeded is true if this call copied the snapshot size from the CnsDeletedVolumeInfo.
	Seeded bool
	// Condition is the SnapshotSizeRestored condition on the CnsVolumeInfo after the call.
	Condition metav1.Condition
}

// SnapshotSizeRestored returns true if the CnsVolumeInfo got its snapshot size from the restore.
func (r RestoreResult) SnapshotSizeRestored() bool {
	return r.Condition.Status == metav1.ConditionTrue
}

// deletedVolumeInfoLookup is the outcome of finding the CnsDeletedVolumeInfo of a restored volume.
// dvi is set only when the record exists and belongs to the restore's namespace; otherwise reason
// and message say why it does not match.
type deletedVolumeInfoLookup struct {
	dvi     *cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo
	reason  string
	message string
}

// InstallDeletedVolumeInfoCRD creates or updates the CnsDeletedVolumeInfo CRD and waits until it is
// established. It is safe to call on every start. It never deletes the CRD, even if the CRD does not
// become established in time, because that would delete every CnsDeletedVolumeInfo with it.
func InstallDeletedVolumeInfoCRD(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	log.Infof("Creating or updating the %s CRD", cnsvolumeinfov1alpha1.CnsDeletedVolumeInfoKind)
	err := k8s.CreateOrUpdateCustomResourceDefinitionFromManifest(ctx,
		cnsvolumeinfoconfig.EmbedCnsDeletedVolumeInfoFile, cnsvolumeinfoconfig.EmbedCnsDeletedVolumeInfoFileName)
	if err != nil {
		return fmt.Errorf("failed to create or update the %s CRD: %w",
			cnsvolumeinfov1alpha1.CnsDeletedVolumeInfoKind, err)
	}
	return nil
}

// PrecheckRestore checks, before anything is created for a restore, that volumeID is a deleted
// volume with a CnsDeletedVolumeInfo charged to namespace. A live CnsVolumeInfo passes only if it
// carries restoredBy, because then an earlier attempt of the same restore created it, and the
// CnsDeletedVolumeInfo may already be gone. The error message is meant for the CnsRegisterVolume status.
func (volumeInfo *volumeInfo) PrecheckRestore(ctx context.Context, volumeID, namespace, restoredBy string) error {
	log := logger.GetLogger(ctx)
	if volumeID == "" {
		return errors.New("restore requires spec.volumeID")
	}
	if restoredBy == "" {
		return errors.New("restore requires the UID of the CnsRegisterVolume")
	}
	log.Infof("Restore precheck for volume %q in namespace %q, restored by CnsRegisterVolume %q",
		volumeID, namespace, restoredBy)
	key := types.NamespacedName{Namespace: csiNamespace, Name: getCnsVolumeInfoCrName(ctx, volumeID)}
	current := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	err := volumeInfo.k8sClient.Get(ctx, key, current)
	switch {
	case apierrors.IsNotFound(err):
		log.Infof("Restore precheck: no CnsVolumeInfo %s for volume %q", key, volumeID)
	case err != nil:
		return fmt.Errorf("failed to read CnsVolumeInfo %s: %w", key, err)
	case current.DeletionTimestamp != nil:
		log.Infof("Restore precheck: CnsVolumeInfo %s is terminating; the deletion of volume %q is "+
			"still in progress", key, volumeID)
	case current.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume] == restoredBy:
		log.Infof("Restore precheck passed for volume %q: CnsVolumeInfo %s was created by an earlier "+
			"attempt of this restore", volumeID, key)
		return nil
	default:
		return fmt.Errorf("restore requires a deleted volume: CnsVolumeInfo %s is live", key)
	}
	lookup, err := volumeInfo.findDeletedVolumeInfo(ctx, volumeID, namespace)
	if err != nil {
		return err
	}
	if lookup.dvi == nil {
		return fmt.Errorf("restore prerequisite not met: %s", lookup.message)
	}
	log.Infof("Restore precheck passed for volume %q: found CnsDeletedVolumeInfo %s/%s with %d bytes",
		volumeID, lookup.dvi.Namespace, lookup.dvi.Name, lookup.dvi.Spec.SizeBytes)
	return nil
}

// GetRestoredBy returns the restore stamp of the live CnsVolumeInfo of volumeID, read from the API
// server rather than the informer so that a just-written stamp is seen. It returns "" if there is no
// CnsVolumeInfo, if it is terminating, because it then belongs to the previous deletion of the
// volume, or if it has no stamp.
func (volumeInfo *volumeInfo) GetRestoredBy(ctx context.Context, volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	key := types.NamespacedName{Namespace: csiNamespace, Name: getCnsVolumeInfoCrName(ctx, volumeID)}
	current := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
	err := volumeInfo.k8sClient.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		log.Infof("No CnsVolumeInfo %s for volume %q", key, volumeID)
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to read CnsVolumeInfo %s: %w", key, err)
	}
	if current.DeletionTimestamp != nil {
		log.Infof("CnsVolumeInfo %s of volume %q is terminating; ignoring its restore stamp", key, volumeID)
		return "", nil
	}
	restoredBy := current.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume]
	log.Infof("CnsVolumeInfo %s of volume %q has restore stamp %q", key, volumeID, restoredBy)
	return restoredBy, nil
}

// CreateOrUpdateRestoredVolumeInfo moves the snapshot charge of a restored volume from its
// CnsDeletedVolumeInfo back to its CnsVolumeInfo. It reads the CnsDeletedVolumeInfo, creates the
// CnsVolumeInfo or patches the live one, and then records the outcome in the SnapshotSizeRestored
// condition. The CnsVolumeInfo always gets the SnapService finalizer and the restore stamp, and it
// gets the snapshot size of a matching CnsDeletedVolumeInfo only if it has none yet.
//
// It is safe to call again after any failure or crash. It never writes the CnsDeletedVolumeInfo.
// An error means the condition may not be written yet, and the caller must retry.
func (volumeInfo *volumeInfo) CreateOrUpdateRestoredVolumeInfo(ctx context.Context,
	info RestoredVolumeInfo) (RestoreResult, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Restore handover for volume %q in namespace %q, restored by CnsRegisterVolume %q",
		info.VolumeID, info.Namespace, info.RestoredBy)
	if info.VolumeID == "" || info.RestoredBy == "" {
		return RestoreResult{}, logger.LogNewErrorf(log, "restore handover requires a volume ID and a "+
			"restore stamp, got volume %q and stamp %q", info.VolumeID, info.RestoredBy)
	}
	lookup, err := volumeInfo.findDeletedVolumeInfo(ctx, info.VolumeID, info.Namespace)
	if err != nil {
		return RestoreResult{}, err
	}
	if lookup.dvi == nil {
		log.Errorf("Restore handover for volume %q found no matching CnsDeletedVolumeInfo: %s. The "+
			"CnsVolumeInfo is created without a snapshot size and is undercounted until CNS reports one",
			info.VolumeID, lookup.message)
	}
	seeded, err := volumeInfo.writeRestoredVolumeInfo(ctx, info, lookup.dvi)
	if err != nil {
		return RestoreResult{}, err
	}
	condition, err := volumeInfo.writeRestoreCondition(ctx, info.VolumeID, seeded, lookup)
	if err != nil {
		return RestoreResult{}, err
	}
	log.Infof("Restore handover done for volume %q: seeded in this attempt %t, condition %s=%s (%s)",
		info.VolumeID, seeded, condition.Type, condition.Status, condition.Reason)
	return RestoreResult{Seeded: seeded, Condition: condition}, nil
}

// MarkRestoredSnapshotSizeMeasured turns a SnapshotSizeRestored condition that is False into True
// once the CnsVolumeInfo of volumeID holds a valid snapshot size, from whichever writer. It does
// nothing if the condition is missing, because a seeded and a measured size cannot be told apart.
func (volumeInfo *volumeInfo) MarkRestoredSnapshotSizeMeasured(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	key := types.NamespacedName{Namespace: csiNamespace, Name: getCnsVolumeInfoCrName(ctx, volumeID)}
	for attempt := 1; attempt <= maxRestoreWriteAttempts; attempt++ {
		current := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		if err := volumeInfo.k8sClient.Get(ctx, key, current); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to read CnsVolumeInfo %s: %w", key, err)
		}
		previous := apimeta.FindStatusCondition(current.Status.Conditions,
			cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored)
		if previous == nil || previous.Status != metav1.ConditionFalse {
			return nil
		}
		if current.Spec.AggregatedSnapshotSize == nil || !current.Spec.ValidAggregatedSnapshotSize {
			log.Infof("CnsVolumeInfo %s still has no valid snapshot size; %s stays False",
				key, cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored)
			return nil
		}
		condition := metav1.Condition{
			Type:   cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored,
			Status: metav1.ConditionTrue,
			Reason: cnsvolumeinfov1alpha1.ReasonMeasuredByCNS,
			Message: fmt.Sprintf("snapshot size reported by CNS; the restore could not copy it: %s",
				previous.Message),
			ObservedGeneration: current.Generation,
		}
		err := volumeInfo.patchRestoreCondition(ctx, current, condition)
		if err == nil {
			log.Infof("Set %s=True (%s) on CnsVolumeInfo %s", condition.Type, condition.Reason, key)
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		log.Infof("attempt %d: CnsVolumeInfo %s changed while setting %s; re-reading", attempt, key,
			condition.Type)
	}
	return logger.LogNewErrorf(log, "failed to set %s on CnsVolumeInfo %s after %d attempts",
		cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored, key, maxRestoreWriteAttempts)
}

// findDeletedVolumeInfo reads the CnsDeletedVolumeInfo of volumeID from the API server, not from a
// cache, because a stale cache could miss a record that DPO just created. An error means the read
// itself failed; a missing or non-matching record is reported in the lookup.
func (volumeInfo *volumeInfo) findDeletedVolumeInfo(ctx context.Context, volumeID,
	namespace string) (deletedVolumeInfoLookup, error) {
	log := logger.GetLogger(ctx)
	key := types.NamespacedName{Namespace: csiNamespace, Name: GetCnsVolumeInfoCrName(volumeID)}
	log.Infof("Reading CnsDeletedVolumeInfo %s", key)
	dvi := &cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo{}
	err := volumeInfo.k8sClient.Get(ctx, key, dvi)
	if apierrors.IsNotFound(err) {
		log.Warnf("CnsDeletedVolumeInfo %s not found", key)
		return deletedVolumeInfoLookup{
			reason:  cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound,
			message: fmt.Sprintf("CnsDeletedVolumeInfo %s not found", key),
		}, nil
	}
	if err != nil {
		return deletedVolumeInfoLookup{}, logger.LogNewErrorf(log, "failed to read CnsDeletedVolumeInfo %s: %v",
			key, err)
	}
	if dvi.Spec.VolumeID != volumeID {
		// The record under this name describes a different volume, so this volume has none.
		log.Warnf("CnsDeletedVolumeInfo %s has spec.volumeID %q, not %q", key, dvi.Spec.VolumeID, volumeID)
		return deletedVolumeInfoLookup{
			reason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNotFound,
			message: fmt.Sprintf("CnsDeletedVolumeInfo %s has spec.volumeID %s, not %s", key,
				dvi.Spec.VolumeID, volumeID),
		}, nil
	}
	if dvi.Spec.Namespace != namespace {
		log.Warnf("CnsDeletedVolumeInfo %s belongs to namespace %q, not %q", key, dvi.Spec.Namespace, namespace)
		return deletedVolumeInfoLookup{
			reason: cnsvolumeinfov1alpha1.ReasonDeletedVolumeInfoNamespaceMismatch,
			message: fmt.Sprintf("CnsDeletedVolumeInfo %s belongs to namespace %s, not %s", key,
				dvi.Spec.Namespace, namespace),
		}, nil
	}
	log.Infof("Found CnsDeletedVolumeInfo %s for volume %q with %d bytes", key, volumeID, dvi.Spec.SizeBytes)
	return deletedVolumeInfoLookup{dvi: dvi}, nil
}

// writeRestoredVolumeInfo creates the CnsVolumeInfo of a restored volume, or patches the live one.
// It returns true if it wrote the snapshot size of dvi. A nil dvi writes no size. A CnsVolumeInfo
// that is still terminating is an error: it belongs to the previous deletion of the volume, and
// treating it as present would leave the restore without a CnsVolumeInfo once DPO releases it.
func (volumeInfo *volumeInfo) writeRestoredVolumeInfo(ctx context.Context, info RestoredVolumeInfo,
	dvi *cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo) (bool, error) {
	log := logger.GetLogger(ctx)
	key := types.NamespacedName{Namespace: csiNamespace, Name: getCnsVolumeInfoCrName(ctx, info.VolumeID)}
	for attempt := 1; attempt <= maxRestoreWriteAttempts; attempt++ {
		current := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		err := volumeInfo.k8sClient.Get(ctx, key, current)
		if apierrors.IsNotFound(err) {
			created := newRestoredVolumeInfo(key, info, dvi)
			log.Infof("Creating restored CnsVolumeInfo %s with snapshot size %v", key,
				created.Spec.AggregatedSnapshotSize)
			err = volumeInfo.k8sClient.Create(ctx, created)
			if err == nil {
				log.Infof("Created restored CnsVolumeInfo %s", key)
				return dvi != nil, nil
			}
			if !apierrors.IsAlreadyExists(err) {
				return false, logger.LogNewErrorf(log, "failed to create CnsVolumeInfo %s: %v", key, err)
			}
			log.Infof("attempt %d: CnsVolumeInfo %s was created concurrently; re-reading", attempt, key)
			continue
		}
		if err != nil {
			return false, logger.LogNewErrorf(log, "failed to read CnsVolumeInfo %s: %v", key, err)
		}
		if current.DeletionTimestamp != nil {
			return false, logger.LogNewErrorf(log, "previous CnsVolumeInfo %s of volume %q is still "+
				"terminating; retrying once it is gone", key, info.VolumeID)
		}
		if owner := current.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume]; owner != "" &&
			owner != info.RestoredBy {
			return false, logger.LogNewErrorf(log, "CnsVolumeInfo %s of volume %q was restored by "+
				"CnsRegisterVolume %q, not %q; not changing it", key, info.VolumeID, owner, info.RestoredBy)
		}
		patch, seeds := restoredVolumeInfoPatch(current, info.RestoredBy, dvi)
		if patch == nil {
			log.Infof("CnsVolumeInfo %s already has the finalizer, the restore stamp and a snapshot size", key)
			return false, nil
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			return false, fmt.Errorf("failed to marshal the patch of CnsVolumeInfo %s: %w", key, err)
		}
		log.Infof("Patching restored CnsVolumeInfo %s (resourceVersion %q), writing snapshot size %t",
			key, current.ResourceVersion, seeds)
		err = volumeInfo.k8sClient.Patch(ctx, current, client.RawPatch(types.MergePatchType, patchBytes))
		if err == nil {
			log.Infof("Patched restored CnsVolumeInfo %s", key)
			return seeds, nil
		}
		if !apierrors.IsConflict(err) {
			return false, logger.LogNewErrorf(log, "failed to patch CnsVolumeInfo %s: %v", key, err)
		}
		log.Infof("attempt %d: CnsVolumeInfo %s changed after it was read; re-reading", attempt, key)
	}
	return false, logger.LogNewErrorf(log, "failed to write restored CnsVolumeInfo %s after %d attempts",
		key, maxRestoreWriteAttempts)
}

// newRestoredVolumeInfo returns the CnsVolumeInfo to create for a restored volume.
func newRestoredVolumeInfo(key types.NamespacedName, info RestoredVolumeInfo,
	dvi *cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo) *cnsvolumeinfov1alpha1.CNSVolumeInfo {
	created := &cnsvolumeinfov1alpha1.CNSVolumeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:        key.Name,
			Namespace:   key.Namespace,
			Finalizers:  []string{cnsvolumeinfov1alpha1.SnapServiceFinalizer},
			Annotations: map[string]string{cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: info.RestoredBy},
		},
		Spec: cnsvolumeinfov1alpha1.CNSVolumeInfoSpec{
			VolumeID:         info.VolumeID,
			Namespace:        info.Namespace,
			VCenterServer:    info.VCenter,
			StoragePolicyID:  info.StoragePolicyID,
			StorageClassName: info.StorageClassName,
			Capacity:         info.Capacity,
		},
	}
	if dvi != nil {
		created.Spec.AggregatedSnapshotSize = resource.NewQuantity(dvi.Spec.SizeBytes, resource.BinarySI)
		created.Spec.ValidAggregatedSnapshotSize = true
		created.Spec.SnapshotLatestOperationCompleteTime = metav1.NewTime(time.Now())
	}
	return created
}

// restoredVolumeInfoPatch returns the merge patch that adds whatever the restore still has to write
// to current, and whether the patch writes the snapshot size of dvi. It returns nil if nothing is
// missing. The size is written only if current has none, so an earlier seed or a CNS value is
// never replaced, and the stamp only if current has none. The caller rejects a current stamped by
// another CnsRegisterVolume. The patch carries the resourceVersion of current.
func restoredVolumeInfoPatch(current *cnsvolumeinfov1alpha1.CNSVolumeInfo, restoredBy string,
	dvi *cnsvolumeinfov1alpha1.CnsDeletedVolumeInfo) (map[string]interface{}, bool) {
	metadata := map[string]interface{}{}
	if !slices.Contains(current.Finalizers, cnsvolumeinfov1alpha1.SnapServiceFinalizer) {
		// A merge patch replaces the whole list; the resourceVersion keeps the other entries intact.
		metadata["finalizers"] = append(slices.Clone(current.Finalizers), cnsvolumeinfov1alpha1.SnapServiceFinalizer)
	}
	if current.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume] == "" {
		metadata["annotations"] = map[string]interface{}{
			cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: restoredBy,
		}
	}
	seeds := dvi != nil && current.Spec.AggregatedSnapshotSize == nil
	if len(metadata) == 0 && !seeds {
		return nil, false
	}
	metadata["resourceVersion"] = current.ResourceVersion
	patch := map[string]interface{}{"metadata": metadata}
	if seeds {
		patch["spec"] = map[string]interface{}{
			"aggregatedsnapshotsize":              resource.NewQuantity(dvi.Spec.SizeBytes, resource.BinarySI),
			"validaggregatedsnapshotsize":         true,
			"snapshotlatestoperationcompletetime": metav1.NewTime(time.Now()),
		}
	}
	return patch, seeds
}

// writeRestoreCondition sets the SnapshotSizeRestored condition after the CnsVolumeInfo was written.
// A retry often runs after DPO deleted the CnsDeletedVolumeInfo, so a lookup that found nothing must
// not undo a True written by an earlier attempt: a CnsVolumeInfo that already has a snapshot size
// keeps its condition, and gets True if it has none yet.
func (volumeInfo *volumeInfo) writeRestoreCondition(ctx context.Context, volumeID string, seeded bool,
	lookup deletedVolumeInfoLookup) (metav1.Condition, error) {
	log := logger.GetLogger(ctx)
	key := types.NamespacedName{Namespace: csiNamespace, Name: getCnsVolumeInfoCrName(ctx, volumeID)}
	for attempt := 1; attempt <= maxRestoreWriteAttempts; attempt++ {
		current := &cnsvolumeinfov1alpha1.CNSVolumeInfo{}
		if err := volumeInfo.k8sClient.Get(ctx, key, current); err != nil {
			return metav1.Condition{}, logger.LogNewErrorf(log, "failed to read CnsVolumeInfo %s: %v", key, err)
		}
		existing := apimeta.FindStatusCondition(current.Status.Conditions,
			cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored)
		desired := restoreCondition(seeded, current, lookup)
		if !seeded && current.Spec.AggregatedSnapshotSize != nil && existing != nil {
			log.Infof("CnsVolumeInfo %s already has a snapshot size; keeping %s=%s (%s)", key,
				existing.Type, existing.Status, existing.Reason)
			return *existing, nil
		}
		if existing != nil && existing.Status == desired.Status && existing.Reason == desired.Reason {
			log.Infof("CnsVolumeInfo %s already has %s=%s (%s)", key, existing.Type, existing.Status,
				existing.Reason)
			return *existing, nil
		}
		err := volumeInfo.patchRestoreCondition(ctx, current, desired)
		if err == nil {
			log.Infof("Set %s=%s (%s) on CnsVolumeInfo %s", desired.Type, desired.Status, desired.Reason, key)
			return desired, nil
		}
		if !apierrors.IsConflict(err) {
			return metav1.Condition{}, err
		}
		log.Infof("attempt %d: CnsVolumeInfo %s changed while setting %s; re-reading", attempt, key,
			desired.Type)
	}
	return metav1.Condition{}, logger.LogNewErrorf(log, "failed to set %s on CnsVolumeInfo %s after %d "+
		"attempts", cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored, key, maxRestoreWriteAttempts)
}

// restoreCondition returns the SnapshotSizeRestored condition for a CnsVolumeInfo that the restore
// has just written. seeded tells whether that write copied the snapshot size.
func restoreCondition(seeded bool, current *cnsvolumeinfov1alpha1.CNSVolumeInfo,
	lookup deletedVolumeInfoLookup) metav1.Condition {
	condition := metav1.Condition{
		Type:               cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored,
		Status:             metav1.ConditionTrue,
		Reason:             cnsvolumeinfov1alpha1.ReasonSeededFromDeletedVolumeInfo,
		ObservedGeneration: current.Generation,
	}
	switch {
	case seeded:
		condition.Message = fmt.Sprintf("seeded %d bytes from CnsDeletedVolumeInfo %s/%s",
			lookup.dvi.Spec.SizeBytes, lookup.dvi.Namespace, lookup.dvi.Name)
	case current.Spec.AggregatedSnapshotSize != nil:
		// An earlier attempt seeded the CnsVolumeInfo and stopped before it set the condition.
		condition.Message = "snapshot size was seeded by an earlier attempt of the restore"
	default:
		condition.Status = metav1.ConditionFalse
		condition.Reason = lookup.reason
		condition.Message = lookup.message
	}
	return condition
}

// patchRestoreCondition writes condition into the status of current, guarded by its resourceVersion.
// The patch carries only the conditions list; a merge patch replaces it as a whole.
func (volumeInfo *volumeInfo) patchRestoreCondition(ctx context.Context,
	current *cnsvolumeinfov1alpha1.CNSVolumeInfo, condition metav1.Condition) error {
	log := logger.GetLogger(ctx)
	conditions := slices.Clone(current.Status.Conditions)
	apimeta.SetStatusCondition(&conditions, condition)
	patchBytes, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{"resourceVersion": current.ResourceVersion},
		"status":   map[string]interface{}{"conditions": conditions},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal the status patch of CnsVolumeInfo %s/%s: %w",
			current.Namespace, current.Name, err)
	}
	log.Infof("Patching status of CnsVolumeInfo %s/%s (resourceVersion %q): %s=%s", current.Namespace,
		current.Name, current.ResourceVersion, condition.Type, condition.Status)
	err = volumeInfo.k8sClient.Status().Patch(ctx, current, client.RawPatch(types.MergePatchType, patchBytes))
	if err != nil && !apierrors.IsConflict(err) {
		return logger.LogNewErrorf(log, "failed to patch status of CnsVolumeInfo %s/%s: %v", current.Namespace,
			current.Name, err)
	}
	return err
}
