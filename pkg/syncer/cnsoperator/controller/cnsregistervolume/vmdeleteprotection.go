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
	"errors"
	"time"

	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	api "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	vmDeleteProtectionClearTimeout  = 30 * time.Second
	vmDeleteProtectionRetryInterval = 5 * time.Second
)

var errPersistVMDeleteProtection = errors.New("failed to persist VM-delete-protection result")

// reconcileVMDeleteProtection runs after binding but before quota accounting and
// registration success. A non-nil result means the caller must requeue. The PVC
// annotation, not the informational condition, records a completed clear across
// retries and restarts. Only keepAfterDeleteVm is cleared; other flags are preserved.
func (r *ReconcileCnsRegisterVolume) reconcileVMDeleteProtection(ctx context.Context,
	instance *api.CnsRegisterVolume, pvc *v1.PersistentVolumeClaim, volumeID string,
) (*v1.PersistentVolumeClaim, *reconcile.Result) {
	log := logger.GetLogger(ctx)
	// Publish the initial attempt once. Resetting this on every retry would
	// destroy the failure condition's lastTransitionTime.
	if conditions.Get(instance, api.ConditionVMDeleteProtectionConfigured) == nil {
		setInstanceConditions(ctx, r, instance, falseCondition(api.ConditionVMDeleteProtectionConfigured,
			api.ReasonClearingKeepAfterDeleteVm, "Configuring VM-delete protection"))
	}
	clearCtx, cancel := context.WithTimeout(ctx, vmDeleteProtectionClearTimeout)
	updatedPVC, err := clearKeepAfterDeleteVmIfNonRemovable(clearCtx, r.client,
		r.volumeManager, pvc, instance.Namespace, volumeID)
	timedOut := errors.Is(clearCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded)
	cancel()
	if err != nil {
		reason := api.ReasonClearKeepAfterDeleteVmFailed
		message := "Waiting to retry VM-delete-protection configuration"
		switch {
		case errors.Is(err, errPersistVMDeleteProtection):
			reason = api.ReasonPersistClearResultFailed
			message = "Waiting to persist the completed keepAfterDeleteVm clear"
		case timedOut:
			reason = api.ReasonClearKeepAfterDeleteVmTimedOut
			message = "The VM-delete-protection attempt exceeded its deadline"
		}
		// Use the parent context for reporting, since the attempt's context may
		// have expired. Do not use recordEvent: it doubles the shared backoff,
		// whereas this step has its own fixed retry interval.
		original := instance.DeepCopy()
		instance.Status.Registered = false
		instance.Status.Error = err.Error()
		applyConditions(instance, falseCondition(api.ConditionVMDeleteProtectionConfigured, reason, message))
		if patchErr := patchCnsRegisterVolumeStatus(ctx, r.client, original, instance); patchErr != nil {
			log.Errorf("Failed to report VM-delete-protection failure: %v", patchErr)
		}
		log.Errorf("VM-delete-protection attempt for volume %s failed; retrying in %s: %v",
			volumeID, vmDeleteProtectionRetryInterval, err)
		r.recorder.Event(instance, v1.EventTypeWarning, reason, err.Error())
		return pvc, &reconcile.Result{RequeueAfter: vmDeleteProtectionRetryInterval}
	}
	reason, message := api.ReasonNotRequired, "This PVC does not require keepAfterDeleteVm clearing"
	if updatedPVC != nil && updatedPVC.Annotations[common.AnnVMDeleteProtectionCleared] == "true" {
		reason, message = api.ReasonKeepAfterDeleteVmCleared, "keepAfterDeleteVm clearing is recorded on the PVC"
	}
	setInstanceConditions(ctx, r, instance,
		trueCondition(api.ConditionVMDeleteProtectionConfigured, reason, message))
	return updatedPVC, nil
}
