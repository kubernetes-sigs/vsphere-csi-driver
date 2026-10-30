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

package cnsregistervolume

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

var (
	// restoreLocks holds the volume IDs that a SnapService restore is reconciling right now, each with
	// the UID of the CnsRegisterVolume holding it. Restores run only in the leader syncer, so a
	// process-local lock serializes the restores of a volume. Which registration owns a restore is
	// recorded in the restore stamps, not here, so losing the locks on a restart is harmless. An entry
	// exists only while its reconcile runs.
	restoreLocks      = make(map[string]apitypes.UID)
	restoreLocksMutex = &sync.Mutex{}
)

// tryLockRestore takes the restore lock of volumeID for the CnsRegisterVolume uid. If another
// CnsRegisterVolume holds it, it returns false and the UID of the holder.
func tryLockRestore(volumeID string, uid apitypes.UID) (bool, apitypes.UID) {
	restoreLocksMutex.Lock()
	defer restoreLocksMutex.Unlock()
	if holder, held := restoreLocks[volumeID]; held && holder != uid {
		return false, holder
	}
	restoreLocks[volumeID] = uid
	return true, uid
}

// unlockRestore releases the restore lock of volumeID if uid holds it.
func unlockRestore(volumeID string, uid apitypes.UID) {
	restoreLocksMutex.Lock()
	defer restoreLocksMutex.Unlock()
	if restoreLocks[volumeID] == uid {
		delete(restoreLocks, volumeID)
	}
}

// startRestore decides whether the restore of instance may proceed, before anything is written for
// it. The restore stamp makes the first CnsRegisterVolume to create the PV, or the CnsVolumeInfo, of
// the volume its owner; every other CnsRegisterVolume of the volume is marked as not processed and
// is never retried. If proceed is true, the caller holds the restore lock of the volume until it
// calls unlock, and stamped tells whether instance already stamped the PV or the CnsVolumeInfo of the
// volume in an earlier attempt. Otherwise the reconcile returns result.
func (r *ReconcileCnsRegisterVolume) startRestore(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, request reconcile.Request,
	timeout time.Duration) (unlock func(), stamped bool, result reconcile.Result, proceed bool) {
	log := logger.GetLogger(ctx)
	volumeID := instance.Spec.VolumeID
	if owner := instance.Annotations[cnsvolumeinfov1alpha1.AnnRestoreNotProcessed]; owner != "" {
		log.Infof("CnsRegisterVolume %s/%s is not processed: volume %q was restored by CnsRegisterVolume %q",
			instance.Namespace, instance.Name, volumeID, owner)
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return nil, false, reconcile.Result{}, false
	}
	if volumeID == "" {
		// The restore precheck rejects the instance; there is no volume to lock.
		return func() {}, false, reconcile.Result{}, true
	}
	if locked, holder := tryLockRestore(volumeID, instance.UID); !locked {
		log.Infof("Volume %q is being restored by CnsRegisterVolume %q; retrying CnsRegisterVolume %s/%s "+
			"in %s", volumeID, holder, instance.Namespace, instance.Name, timeout)
		return nil, false, reconcile.Result{RequeueAfter: timeout}, false
	}
	unlock = func() { unlockRestore(volumeID, instance.UID) }
	log.Infof("CnsRegisterVolume %s/%s holds the restore lock of volume %q", instance.Namespace,
		instance.Name, volumeID)
	owner, err := r.restoreOwner(ctx, volumeID)
	if err != nil {
		unlock()
		log.Errorf("Failed to find the owner of the restore of volume %q, will retry: %v", volumeID, err)
		setInstanceError(ctx, r, instance, err.Error())
		return nil, false, reconcile.Result{RequeueAfter: timeout}, false
	}
	if owner == "" || owner == string(instance.UID) {
		log.Infof("Restore of volume %q by CnsRegisterVolume %s/%s proceeds (current stamp %q)", volumeID,
			instance.Namespace, instance.Name, owner)
		return unlock, owner != "", reconcile.Result{}, true
	}
	unlock()
	return nil, false, r.markRestoreNotProcessed(ctx, instance, owner, request, timeout), false
}

// restoreOwner returns the restore stamp of volumeID: the stamp on its PV if the PV exists, is not
// being deleted and is stamped, otherwise the stamp on its live CnsVolumeInfo. It returns "" if
// neither carries one. Both are read from the API server, because a cache could miss a stamp that
// another CnsRegisterVolume has just written.
func (r *ReconcileCnsRegisterVolume) restoreOwner(ctx context.Context, volumeID string) (string, error) {
	log := logger.GetLogger(ctx)
	pvName := staticPvNamePrefix + volumeID
	pv, err := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		log.Infof("No PV %s for volume %q", pvName, volumeID)
	case err != nil:
		return "", fmt.Errorf("failed to read PV %s: %w", pvName, err)
	case pv.DeletionTimestamp != nil:
		log.Infof("PV %s of volume %q is being deleted; ignoring its restore stamp", pvName, volumeID)
	default:
		if owner := pv.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume]; owner != "" {
			log.Infof("PV %s of volume %q has restore stamp %q", pvName, volumeID, owner)
			return owner, nil
		}
	}
	return r.volumeInfoService.GetRestoredBy(ctx, volumeID)
}

// markRestoreNotProcessed records on instance that another CnsRegisterVolume, owner, restored its
// volume, so the restore of instance is never processed. The annotation keeps the decision across
// restarts, when the volume may have been deleted again and the stamps are gone. The finalizer stays:
// deleting instance later does not clean up the owner's PV, because that PV carries the owner's stamp.
func (r *ReconcileCnsRegisterVolume) markRestoreNotProcessed(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, owner string, request reconcile.Request,
	timeout time.Duration) reconcile.Result {
	log := logger.GetLogger(ctx)
	log.Infof("Volume %q was restored by CnsRegisterVolume %q; marking CnsRegisterVolume %s/%s as not processed",
		instance.Spec.VolumeID, owner, instance.Namespace, instance.Name)
	original := instance.DeepCopy()
	metav1.SetMetaDataAnnotation(&instance.ObjectMeta, cnsvolumeinfov1alpha1.AnnRestoreNotProcessed, owner)
	if err := r.client.Patch(ctx, instance, client.MergeFrom(original)); err != nil {
		log.Errorf("Failed to mark CnsRegisterVolume %s/%s as not processed, will retry: %v", instance.Namespace,
			instance.Name, err)
		return reconcile.Result{RequeueAfter: timeout}
	}
	setInstanceError(ctx, r, instance, fmt.Sprintf("volume %s was restored by CnsRegisterVolume %s; this "+
		"CnsRegisterVolume is not processed", instance.Spec.VolumeID, owner))
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}
}

// isPVRestoredByOther returns true if pv carries the restore stamp of a CnsRegisterVolume other than
// instance. Such a PV belongs to that registration's restore, so instance must not clean it up.
func isPVRestoredByOther(ctx context.Context, pv *v1.PersistentVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) bool {
	if !isSnapServiceLocalProtectionEnabled {
		return false
	}
	owner := pv.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume]
	if owner == "" || owner == string(instance.UID) {
		return false
	}
	logger.GetLogger(ctx).Infof("PV %s was restored by CnsRegisterVolume %q, not by %s/%s", pv.Name, owner,
		instance.Namespace, instance.Name)
	return true
}

// isSnapServiceRestore returns true if instance is a SnapService restore that this controller
// handles as one: the instance carries the restore annotation, the SnapService local protection
// capability is enabled, and the CnsVolumeInfo service is running. Otherwise the instance is
// registered like any other, and no CnsDeletedVolumeInfo is read.
func (r *ReconcileCnsRegisterVolume) isSnapServiceRestore(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) bool {
	log := logger.GetLogger(ctx)
	if instance.Annotations[cnsvolumeinfov1alpha1.AnnSnapServiceRestore] != "true" {
		return false
	}
	if !isSnapServiceLocalProtectionEnabled {
		log.Infof("CnsRegisterVolume %s/%s is marked as a SnapService restore, but the %s capability is "+
			"disabled; registering it as an ordinary volume", instance.Namespace, instance.Name,
			common.SupportsSnapServiceLocalProtection)
		return false
	}
	if r.volumeInfoService == nil {
		log.Warnf("CnsRegisterVolume %s/%s is marked as a SnapService restore, but the CnsVolumeInfo service "+
			"is not running; registering it as an ordinary volume", instance.Namespace, instance.Name)
		return false
	}
	log.Infof("CnsRegisterVolume %s/%s is a SnapService restore of volume %q", instance.Namespace,
		instance.Name, instance.Spec.VolumeID)
	return true
}

// restoreAnnotations returns the annotations that mark the PV of a restore. Copying the restore
// annotation and the stamp to the PV lets the full sync recognise the restore, and stamp the
// CnsVolumeInfo, after the CnsRegisterVolume is gone.
func restoreAnnotations(instance *cnsregistervolumev1alpha1.CnsRegisterVolume) map[string]string {
	return map[string]string{
		cnsvolumeinfov1alpha1.AnnSnapServiceRestore:          "true",
		cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume: string(instance.UID),
	}
}

// setRestoreAnnotations adds the restore annotations to a PV that is about to be created.
func setRestoreAnnotations(pv *v1.PersistentVolume, instance *cnsregistervolumev1alpha1.CnsRegisterVolume) {
	if pv.Annotations == nil {
		pv.Annotations = map[string]string{}
	}
	for key, value := range restoreAnnotations(instance) {
		pv.Annotations[key] = value
	}
}

// ensureRestoreAnnotationsOnPV adds the restore annotations that an existing PV is missing and returns
// the PV as stored. It never replaces the stamp of another CnsRegisterVolume: such a PV belongs to that
// registration's restore, and an error is returned instead. The patch carries the resourceVersion of
// pv, so a concurrent change fails it and the reconcile retries with a fresh read.
func ensureRestoreAnnotationsOnPV(ctx context.Context, k8sclient clientset.Interface, pv *v1.PersistentVolume,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume) (*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	stamp := pv.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume]
	if stamp != "" && stamp != string(instance.UID) {
		return nil, fmt.Errorf("PV %s was restored by CnsRegisterVolume %s; not changing it", pv.Name, stamp)
	}
	missing := map[string]string{}
	if pv.Annotations[cnsvolumeinfov1alpha1.AnnSnapServiceRestore] != "true" {
		missing[cnsvolumeinfov1alpha1.AnnSnapServiceRestore] = "true"
	}
	if stamp == "" {
		missing[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume] = string(instance.UID)
	}
	if len(missing) == 0 {
		return pv, nil
	}
	patchBytes, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{"annotations": missing, "resourceVersion": pv.ResourceVersion},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the restore annotations of PV %s: %w", pv.Name, err)
	}
	log.Infof("Adding restore annotations %v to PV %s (resourceVersion %q)", missing, pv.Name, pv.ResourceVersion)
	patched, err := k8sclient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, apitypes.MergePatchType,
		patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to add the restore annotations to PV %s: %w", pv.Name, err)
	}
	return patched, nil
}

// handoverRestoredVolume creates or updates the CnsVolumeInfo of a restored volume once its PVC is
// bound, seeding its snapshot size from the CnsDeletedVolumeInfo. It first reads the PV from the
// API server: a PV that is gone or being deleted means the restore is no longer in progress, and
// a CnsVolumeInfo created now would hide the CnsDeletedVolumeInfo that DPO is about to record the
// deletion into. An error means the handover must be retried; it is safe to repeat.
func (r *ReconcileCnsRegisterVolume) handoverRestoredVolume(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, pvName string,
	info cnsvolumeinfo.RestoredVolumeInfo) error {
	log := logger.GetLogger(ctx)
	log.Infof("Restore handover for CnsRegisterVolume %s/%s, PV %s, volume %q", instance.Namespace,
		instance.Name, pvName, info.VolumeID)
	pv, err := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return logger.LogNewErrorf(log, "PV %s of restored volume %q is gone; not creating its CnsVolumeInfo",
			pvName, info.VolumeID)
	}
	if err != nil {
		return logger.LogNewErrorf(log, "failed to read PV %s: %v", pvName, err)
	}
	if pv.DeletionTimestamp != nil {
		return logger.LogNewErrorf(log, "PV %s of restored volume %q is being deleted; not creating its "+
			"CnsVolumeInfo", pvName, info.VolumeID)
	}
	result, err := r.volumeInfoService.CreateOrUpdateRestoredVolumeInfo(ctx, info)
	if err != nil {
		return err
	}
	if !result.SnapshotSizeRestored() {
		msg := fmt.Sprintf("snapshot size of restored volume %s was not copied (%s): %s. The volume is "+
			"undercounted until CNS reports its snapshot size", info.VolumeID, result.Condition.Reason,
			result.Condition.Message)
		log.Error(msg)
		r.recorder.Event(instance, v1.EventTypeWarning, "SnapshotSizeNotRestored", msg)
	}
	log.Infof("Restore handover done for CnsRegisterVolume %s/%s, volume %q", instance.Namespace,
		instance.Name, info.VolumeID)
	return nil
}
