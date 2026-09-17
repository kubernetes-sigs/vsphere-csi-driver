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

// Package cnsnfsvolumeinformation implements the Supervisor-side relay controller for
// CnsNfsVolumeInformation CRs: it watches the CR (one per VKS cluster, written by pvCSI
// running inside that guest cluster - see pkg/common/cnsnfsvolumeinformation), and
// relays each Spec.Volumes entry to CNS via volumes.Manager.RegisterNfsVolumeInfo /
// UpdateNfsVolumeInfo / DeleteNfsVolumeInfo, recording success in
// Status.SyncedVolumeIDs/SyncedVolumeHashes and failure in Status.Conditions. This is
// the only path by which CNS learns about guest-local-NFS-backed volumes - pvCSI itself
// has no vCenter/CNS credentials or network path.
package cnsnfsvolumeinformation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnfsvolumeinformation/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// relaySucceededCondition tracks whether the most recent relay attempt (for any
	// entry currently unsynced) succeeded. See the "Relay failure" swimlane: this is
	// the only signal pvCSI (or an observer) has that a sync is failing, since pvCSI
	// never talks to CNS/vCenter directly and cannot otherwise tell "still catching up"
	// from "stuck."
	relaySucceededCondition = "RelaySucceeded"

	// finalizerName blocks actual deletion of a CnsNfsVolumeInformation CR until every
	// volumeID it ever synced has been confirmed deleted from CNS (steps 12-16). Without
	// it, pvCSI deleting the CR (RemoveVolumeEntry, once it has no volumes left) could
	// remove the only record of which volumeIDs need cleaning up from CNS before this
	// controller ever gets to react - e.g. if it's down or behind at the moment of
	// deletion.
	finalizerName = "cns.vmware.com/cnsnfsvolumeinformation"
)

// Add creates a new CnsNfsVolumeInformation Controller and adds it to the Manager. Only
// relevant on the Supervisor - this CR only ever lives in a Supervisor Namespace.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	_ *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		return nil
	}
	return add(mgr, &Reconciler{client: mgr.GetClient(), volumeManager: volumeManager})
}

func add(mgr manager.Manager, r reconcile.Reconciler) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("cnsnfsvolumeinformation-controller").
		For(&v1a1.CnsNfsVolumeInformation{}).
		// No GenerationChangedPredicate here deliberately: marking the CR for deletion
		// (setting DeletionTimestamp, step 12's "delete whole CR" case) does not bump
		// generation, so filtering on it would make reconcileDelete's finalizer cleanup
		// unreachable. Reconcile is cheap to no-op on our own Status-only updates (an
		// unchanged content hash short-circuits before any write), so the extra
		// self-triggered reconcile per real change is an acceptable trade for that.
		WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
		Complete(r)
}

var _ reconcile.Reconciler = &Reconciler{}

// Reconciler relays CnsNfsVolumeInformation Spec.Volumes entries to CNS.
type Reconciler struct {
	client        client.Client
	volumeManager volumes.Manager
}

// Reconcile implements steps 2/8/13 (this reconcile is itself triggered by the informer
// watch set up in add(), above - the "Volume created", "Health / pod attach changes",
// and "Volume deleted" swimlanes all drive the same watch, since all three write to the
// CR), 3/9/14 (RegisterNfsVolumeInfo / UpdateNfsVolumeInfo / DeleteNfsVolumeInfo), and
// 6/19 (patch Status.SyncedVolumeIDs / Status.Conditions) of the CnsNfsVolumeInformation
// design. Step 4/5/10/11/15/16 (the CNS DB table upsert/update/delete and their acks)
// happen inside vCenter, behind those three calls, and are not implemented in this repo.
//
// Register vs. Update is decided by comparing each entry's content hash against
// Status.SyncedVolumeHashes: a volumeID with no recorded hash has never been relayed
// (Register); a volumeID whose current hash differs from the recorded one changed since
// the last successful relay - e.g. a pod attached/detached (Update); a volumeID whose
// hash is unchanged needs no call at all. A previously-synced volumeID no longer present
// in Spec.Volumes at all - the "Volume deleted" case, step 12 - gets DeleteNfsVolumeInfo
// instead.
//
// Full-CR deletion (step 12's "delete whole CR if it was the last entry") is handled
// separately by reconcileDelete: by the time RemoveVolumeEntry calls Delete on the CR,
// Spec.Volumes is already empty, so Status.SyncedVolumeHashes is the only remaining
// record of which volumeIDs need deleting from CNS. The finalizer added below exists
// solely to keep that Status readable until reconcileDelete has confirmed each of them
// is gone.
func (r *Reconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	instance := &v1a1.CnsNfsVolumeInformation{}
	if err := r.client.Get(ctx, request.NamespacedName, instance); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	if instance.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, instance)
	}

	if err := k8s.AddFinalizer(ctx, r.client, instance, finalizerName); err != nil {
		log.Errorf("Reconcile: failed to add finalizer on %s/%s: %v", instance.Namespace, instance.Name, err)
		return reconcile.Result{}, err
	}

	newHashes := make(map[string]string, len(instance.Status.SyncedVolumeHashes))
	for k, v := range instance.Status.SyncedVolumeHashes {
		newHashes[k] = v
	}

	var syncedIDs []string
	seen := make(map[string]bool, len(instance.Spec.Volumes))
	var relayErr error
	changed := false
	for pvcUID, entry := range instance.Spec.Volumes {
		volumeID := instance.Spec.VKSClusterName + "-" + instance.Spec.VKSClusterID + "-" + pvcUID
		seen[volumeID] = true

		hash, err := hashVolumeEntry(entry)
		if err != nil {
			log.Errorf("Reconcile: failed to hash entry for %q on %s/%s: %v", volumeID, instance.Namespace,
				instance.Name, err)
			relayErr = err
			continue
		}

		existingHash, alreadySynced := newHashes[volumeID]
		if alreadySynced && existingHash == hash {
			syncedIDs = append(syncedIDs, volumeID) // unchanged since the last successful sync
			continue
		}

		var callErr error
		if alreadySynced {
			callErr = r.volumeManager.UpdateNfsVolumeInfo(ctx, volumeID, entry)
		} else {
			callErr = r.volumeManager.RegisterNfsVolumeInfo(ctx, volumeID, entry)
		}
		if callErr != nil {
			log.Errorf("Reconcile: failed to relay %q (update=%v) on %s/%s: %v", volumeID, alreadySynced,
				instance.Namespace, instance.Name, callErr)
			relayErr = callErr
			continue
		}
		newHashes[volumeID] = hash
		syncedIDs = append(syncedIDs, volumeID)
		changed = true
		log.Infof("Reconcile: relayed %q (update=%v) on %s/%s", volumeID, alreadySynced, instance.Namespace,
			instance.Name)
	}

	// Steps 12-14: a volumeID that was synced before but is no longer in Spec.Volumes
	// (an individual key removed via merge patch, CR itself still alive) must be deleted
	// from CNS, not just quietly dropped from our own bookkeeping.
	for volumeID := range newHashes {
		if seen[volumeID] {
			continue
		}
		if err := r.volumeManager.DeleteNfsVolumeInfo(ctx, volumeID); err != nil {
			log.Errorf("Reconcile: failed to delete NFS volume info for %q on %s/%s: %v", volumeID,
				instance.Namespace, instance.Name, err)
			relayErr = err
			continue // keep the hash - not confirmed deleted from CNS yet
		}
		delete(newHashes, volumeID)
		changed = true
		log.Infof("Reconcile: deleted NFS volume info for %q on %s/%s", volumeID, instance.Namespace, instance.Name)
	}

	if !changed && relayErr == nil {
		return reconcile.Result{}, nil
	}

	if err := r.patchStatus(ctx, instance, syncedIDs, newHashes, relayErr); err != nil {
		log.Errorf("Reconcile: failed to patch status on %s/%s: %v", instance.Namespace, instance.Name, err)
		return reconcile.Result{}, err
	}

	if relayErr != nil {
		// Non-nil error requeues with the controller-runtime default exponential backoff.
		return reconcile.Result{}, relayErr
	}
	return reconcile.Result{}, nil
}

// reconcileDelete implements steps 12-16 for the "delete whole CR" case: it deletes
// every volumeID recorded in Status.SyncedVolumeHashes from CNS, then removes
// finalizerName so the CR's actual deletion (already requested - DeletionTimestamp is
// set) can proceed. Partial progress is persisted to Status after each attempt, so a
// retry (whether from a transient error here or a controller restart) never redelivers
// DeleteNfsVolumeInfo for a volumeID already confirmed gone from CNS.
func (r *Reconciler) reconcileDelete(ctx context.Context, instance *v1a1.CnsNfsVolumeInformation) (
	reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	if !controllerutil.ContainsFinalizer(instance, finalizerName) {
		// Never got as far as adding the finalizer (e.g. created and deleted before this
		// controller's watch ever saw it) - nothing recorded to clean up.
		return reconcile.Result{}, nil
	}

	remaining := make(map[string]string, len(instance.Status.SyncedVolumeHashes))
	for k, v := range instance.Status.SyncedVolumeHashes {
		remaining[k] = v
	}

	var deleteErr error
	for volumeID := range instance.Status.SyncedVolumeHashes {
		if err := r.volumeManager.DeleteNfsVolumeInfo(ctx, volumeID); err != nil {
			log.Errorf("reconcileDelete: failed to delete NFS volume info for %q on %s/%s: %v", volumeID,
				instance.Namespace, instance.Name, err)
			deleteErr = err
			continue
		}
		delete(remaining, volumeID)
		log.Infof("reconcileDelete: deleted NFS volume info for %q on %s/%s", volumeID, instance.Namespace,
			instance.Name)
	}

	if len(remaining) != len(instance.Status.SyncedVolumeHashes) {
		instance.Status.SyncedVolumeHashes = remaining
		if err := r.client.Status().Update(ctx, instance); err != nil && !apierrors.IsConflict(err) {
			log.Errorf("reconcileDelete: failed to patch status on %s/%s: %v", instance.Namespace, instance.Name, err)
			return reconcile.Result{}, err
		}
	}

	if deleteErr != nil {
		// Non-nil error requeues with the controller-runtime default exponential backoff;
		// the finalizer stays in place until every volumeID is confirmed deleted.
		return reconcile.Result{}, deleteErr
	}

	if err := k8s.RemoveFinalizer(ctx, r.client, instance, finalizerName); err != nil {
		log.Errorf("reconcileDelete: failed to remove finalizer on %s/%s: %v", instance.Namespace, instance.Name, err)
		return reconcile.Result{}, err
	}
	log.Infof("reconcileDelete: removed finalizer on %s/%s, deletion can proceed", instance.Namespace, instance.Name)
	return reconcile.Result{}, nil
}

// hashVolumeEntry returns a stable content hash of entry, used to detect changes
// between reconciles without diffing every field by hand.
func hashVolumeEntry(entry v1a1.NfsVolumeEntry) (string, error) {
	b, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("failed to marshal entry for hashing: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// patchStatus replaces Status.SyncedVolumeIDs/SyncedVolumeHashes with the freshly
// computed syncedIDs/syncedHashes (steps 6/19) and records whether this reconcile's
// relay attempts succeeded via a RelaySucceeded condition (step 19 on failure). Status
// is a separate subresource from Spec (this CRD has +kubebuilder:subresource:status),
// so this never conflicts with pvCSI's own concurrent Spec.Volumes merge-patches.
func (r *Reconciler) patchStatus(ctx context.Context, instance *v1a1.CnsNfsVolumeInformation,
	syncedIDs []string, syncedHashes map[string]string, relayErr error) error {
	instance.Status.SyncedVolumeIDs = syncedIDs
	instance.Status.SyncedVolumeHashes = syncedHashes

	condition := metav1.Condition{
		Type:    relaySucceededCondition,
		Status:  metav1.ConditionTrue,
		Reason:  "Relayed",
		Message: "All Spec.Volumes entries have been registered/updated/deleted with CNS.",
	}
	if relayErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Reason = "RelayFailed"
		condition.Message = relayErr.Error()
	}
	apimeta.SetStatusCondition(&instance.Status.Conditions, condition)

	if err := r.client.Status().Update(ctx, instance); err != nil {
		if apierrors.IsConflict(err) {
			// A concurrent reconcile (or an external status writer, if one is ever
			// added) raced us. The controller-runtime workqueue will requeue this key
			// again on its own; nothing more to do here.
			return nil
		}
		return err
	}
	return nil
}
