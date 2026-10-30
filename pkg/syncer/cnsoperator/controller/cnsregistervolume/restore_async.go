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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	api "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	infoapi "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

// A Supervisor has one vCenter. The volume UUID is therefore the reservation
// key, across all namespaces. The CR UID identifies the attempt. This label is
// persisted before submission and remains through PV recreation and handover;
// it does not expire while a task may still run. Completed/not-processed CRs
// are ignored; deletion removes the reservation together with the finalized CR.
const labelRestoreVolumeID = "cns.vmware.com/restore-volume-id"

func (r *ReconcileCnsRegisterVolume) executionReader() client.Reader {
	if r.apiReader != nil {
		return r.apiReader
	}
	return r.client // unit-test clients have no informer cache
}

// restoreReservationOwner must be called with the per-volume restore lock held.
// A server-side label selector avoids listing every registration. Reading the
// API server on each admission recovers reservations after restart without an
// in-memory startup barrier or relying on informer delivery order.
func (r *ReconcileCnsRegisterVolume) restoreReservationOwner(ctx context.Context,
	instance *api.CnsRegisterVolume) (string, error) {
	list := &api.CnsRegisterVolumeList{}
	if err := r.executionReader().List(ctx, list,
		client.MatchingLabels{labelRestoreVolumeID: instance.Spec.VolumeID}); err != nil {
		return "", fmt.Errorf("failed to read restore reservations: %w", err)
	}
	for _, other := range list.Items {
		if other.UID == instance.UID || other.Status.Registered ||
			other.Annotations[infoapi.AnnRestoreNotProcessed] != "" {
			continue
		}
		// Include terminating CRs: their finalizers must settle tasks/cleanup
		// before a new attempt may touch the same backing volume.
		return string(other.UID), nil
	}
	return "", nil
}

func (r *ReconcileCnsRegisterVolume) reserveRestore(ctx context.Context,
	instance *api.CnsRegisterVolume) error {
	if instance.Labels[labelRestoreVolumeID] == instance.Spec.VolumeID {
		return nil
	}
	original := instance.DeepCopy()
	if instance.Labels == nil {
		instance.Labels = map[string]string{}
	}
	instance.Labels[labelRestoreVolumeID] = instance.Spec.VolumeID
	if err := r.client.Patch(ctx, instance,
		client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil {
		instance.Labels = original.Labels
		return fmt.Errorf("failed to reserve restore volume: %w", err)
	}
	return nil
}

// persistCnsExecution atomically replaces the task with its successful result.
// A conflict is retried by a fresh reconcile, never by replaying a stale patch.
// Conditions remain best-effort diagnostics and cannot overwrite these fields.
func (r *ReconcileCnsRegisterVolume) persistCnsExecution(ctx context.Context,
	instance *api.CnsRegisterVolume, taskID, volumeID string) error {
	original := instance.DeepCopy()
	instance.Status.CnsTaskID, instance.Status.CnsVolumeID = taskID, volumeID
	if err := r.client.Status().Patch(ctx, instance,
		client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil {
		instance.Status = original.Status
		return err
	}
	return nil
}

func (r *ReconcileCnsRegisterVolume) reconcileRestoreMetadata(ctx context.Context,
	instance *api.CnsRegisterVolume, pvName string, info cnsvolumeinfo.RestoredVolumeInfo) error {
	if conditions.Get(instance, api.ConditionRestoreMetadataConfigured) == nil {
		setInstanceConditions(ctx, r, instance, falseCondition(api.ConditionRestoreMetadataConfigured,
			api.ReasonConfiguringRestoreMetadata, "Configuring restored volume metadata"))
	}
	if err := r.handoverRestoredVolume(ctx, instance, pvName, info); err != nil {
		setInstanceConditions(ctx, r, instance, falseCondition(api.ConditionRestoreMetadataConfigured,
			api.ReasonRestoreMetadataFailed, "Waiting to retry restored volume metadata handover"))
		return err
	}
	setInstanceConditions(ctx, r, instance, trueCondition(api.ConditionRestoreMetadataConfigured,
		api.ReasonRestoreMetadataConfigured, "Restored volume metadata handover completed"),
		falseCondition(api.ConditionReady, api.ReasonFinalizing, "Applying storage quota accounting"))
	return nil
}

// A definitively failed/cleaned-up attempt has no outstanding CNS work. Let a
// competing registration try; only the first PV/CVI stamp establishes ownership.
func (r *ReconcileCnsRegisterVolume) releaseRestoreReservation(ctx context.Context,
	instance *api.CnsRegisterVolume) error {
	if instance.Labels[labelRestoreVolumeID] == "" {
		return nil
	}
	original := instance.DeepCopy()
	delete(instance.Labels, labelRestoreVolumeID)
	if err := r.client.Patch(ctx, instance,
		client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil {
		instance.Labels = original.Labels
		return err
	}
	return nil
}
