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
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// cnsTaskSafetyRequeue bounds how long a CR can sit in CnsRegistering if
	// its completion event is lost. Completion normally arrives as a push from
	// the listview, so this is a backstop, not the mechanism; it is deliberately
	// long enough that it does not become a poll.
	cnsTaskSafetyRequeue = 10 * time.Second
	// cnsSlotRetryInterval is how long a CR waits before trying again for an
	// in-flight CNS slot.
	cnsSlotRetryInterval = 5 * time.Second
	// pvcBindPollInterval is how often a CR in WaitingForBind re-checks its
	// PVC. This is a single Get, replacing a 60s per-CR watch against the API
	// server.
	pvcBindPollInterval = 2 * time.Second
)

// submitCreateVolumeTaskFn indirects the CNS submit so tests can drive the
// async path without a vCenter, matching the pattern already used in this
// package for checkDatastorePolicyCompatibilityFn and friends.
//
// Note this path does not go through Manager.CreateVolume, so the Prometheus
// CnsCreateVolume histogram that method records is not emitted here. Restoring
// that is a follow-up: the metric should move to wrap submit+completion rather
// than the blocking call it currently times.
var submitCreateVolumeTaskFn = volumes.SubmitCreateVolumeTask

// submitOrPollCreateVolume replaces the blocking Manager.CreateVolume call.
//
// Instead of parking the reconciler worker until CNS responds, it either
// submits the task and returns, or reports on a task submitted by an earlier
// reconcile. The worker is held only for the submit itself.
//
// The contract: when the returned *reconcile.Result is non-nil the caller must
// return it immediately and do no further work this pass. A non-nil
// CnsVolumeInfo means the volume exists and the caller should carry on exactly
// as it did after the old blocking call returned.
func (r *ReconcileCnsRegisterVolume) submitOrPollCreateVolume(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, vc *cnsvsphere.VirtualCenter,
	createSpec *cnstypes.CnsVolumeCreateSpec, timeout time.Duration) (
	*volumes.CnsVolumeInfo, *reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	if err := r.taskTracker.ensureInit(ctx, r.volumeManager, vc); err != nil {
		msg := fmt.Sprintf("failed to initialize CNS task tracking: %v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil
	}

	if instance.Status.CnsTaskID == "" {
		return r.submitCreateVolume(ctx, instance, vc, createSpec, timeout)
	}
	return r.pollCreateVolume(ctx, instance, vc, createSpec, timeout)
}

// submitCreateVolume admits the CR against the in-flight cap and dispatches the
// CNS task without waiting for it.
func (r *ReconcileCnsRegisterVolume) submitCreateVolume(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, vc *cnsvsphere.VirtualCenter,
	createSpec *cnstypes.CnsVolumeCreateSpec, timeout time.Duration) (
	*volumes.CnsVolumeInfo, *reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	cr := instanceKey(instance)

	// The 40 reconciler workers used to be an implicit ceiling on concurrent
	// CreateVolume calls. Now that the workers are released, that ceiling has
	// to be explicit or a burst would hit CNS far harder than it does today.
	if !r.taskTracker.tryAcquireSlot(cr) {
		log.Infof("CnsRegisterVolume %s/%s is waiting for a CNS task slot (%d in flight)",
			instance.Namespace, instance.Name, r.taskTracker.inFlight())
		setInstanceConditions(ctx, r, instance, falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonWaitingForTaskSlot, "Waiting for a CNS task slot"))
		return nil, &reconcile.Result{RequeueAfter: cnsSlotRetryInterval}, nil
	}

	taskMoRef, _, err := submitCreateVolumeTaskFn(ctx, vc, createSpec)
	if err != nil {
		r.taskTracker.releaseSlot(cr)
		msg := fmt.Sprintf("failed to submit CNS CreateVolume task. Error: %v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Persist the reference before anything else. If this write fails the task
	// is running on CNS with nothing pointing at it; the CR falls back to
	// submitting again next pass, which is safe because CNS answers a repeat
	// registration of the same disk with CnsAlreadyRegisteredFault, which the
	// shared decoding helpers map to success.
	if err := setInstanceCnsTaskID(ctx, r, instance, taskMoRef.Value); err != nil {
		r.taskTracker.releaseSlot(cr)
		log.Errorf("submitted CNS task %q but failed to record it on CnsRegisterVolume %s/%s: %v",
			taskMoRef.Value, instance.Namespace, instance.Name, err)
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil
	}

	setInstanceConditions(ctx, r, instance, falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
		cnsregistervolumev1alpha1.ReasonCnsTaskRunning, "CNS CreateVolume task is running"))

	if err := r.taskTracker.watch(ctx, cr, taskMoRef, createSpec.Name); err != nil {
		// The task is submitted and recorded; only the push subscription
		// failed. The safety requeue will retry the subscription, so this is
		// not fatal and must not fail the registration.
		log.Errorf("failed to subscribe to CNS task %q updates, will retry: %v", taskMoRef.Value, err)
	}

	// Worker released here. This is the change: previously this call sat in
	// waitOnTask for as long as CNS took.
	return nil, &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}, nil
}

// pollCreateVolume reports on a task submitted by an earlier reconcile. In the
// normal case this is a map lookup against a result the listview already
// pushed, so it costs no vCenter call.
func (r *ReconcileCnsRegisterVolume) pollCreateVolume(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, vc *cnsvsphere.VirtualCenter,
	createSpec *cnstypes.CnsVolumeCreateSpec, timeout time.Duration) (
	*volumes.CnsVolumeInfo, *reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	cr := instanceKey(instance)
	taskID := instance.Status.CnsTaskID
	taskMoRef := vim25types.ManagedObjectReference{Type: "Task", Value: taskID}

	// A CR resuming after a syncer restart holds a task ID but has no
	// subscription, because the listview is in-process and starts empty. Keep
	// the slot it is implicitly occupying accounted for.
	r.taskTracker.tryAcquireSlot(cr)

	outcome, done := r.taskTracker.outcome(taskID)
	if !done {
		if !r.taskTracker.isWatching(taskID) {
			if err := r.taskTracker.watch(ctx, cr, taskMoRef, createSpec.Name); err != nil {
				// Most often ManagedObjectNotFound: the task has aged out of
				// vCenter or the VC restarted. Read it directly instead.
				log.Infof("could not subscribe to CNS task %q (%v); reading its state directly",
					taskID, err)
				return r.resolveTaskDirectly(ctx, instance, vc, createSpec, timeout)
			}
		}
		return nil, &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}, nil
	}

	switch outcome.state {
	case volumes.CreateVolumeTaskSucceeded:
		r.taskTracker.forget(cr, taskID)
		if err := setInstanceCnsTaskID(ctx, r, instance, ""); err != nil {
			log.Errorf("failed to clear cnsTaskID after success on %s/%s: %v",
				instance.Namespace, instance.Name, err)
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, "CNS has registered the disk"))
		return outcome.volInfo, nil, nil

	case volumes.CreateVolumeTaskFailed:
		// Today's behaviour is preserved exactly: a CreateVolume failure is
		// retried indefinitely rather than becoming terminal. Deciding which
		// CNS faults are permanent needs agreement with VM Operator first,
		// because a misclassification wedges a VM that currently self-heals.
		r.taskTracker.forget(cr, taskID)
		if err := setInstanceCnsTaskID(ctx, r, instance, ""); err != nil {
			log.Errorf("failed to clear cnsTaskID after failure on %s/%s: %v",
				instance.Namespace, instance.Name, err)
		}
		msg := fmt.Sprintf("failed to create CNS volume. Error: %v", outcome.err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg,
			falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
				cnsregistervolumev1alpha1.ReasonCnsTaskFailed, "CNS reported a fault for the CreateVolume task"))
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil

	default:
		// Unknown. The task may well still be running on CNS. Keep the task ID
		// and the slot, drop the inconclusive result so the next pass
		// re-establishes the real state, and above all do not submit a second
		// CreateVolume for the same disk.
		log.Infof("state of CNS task %q for %s/%s is unknown (%v); keeping the task and retrying",
			taskID, instance.Namespace, instance.Name, outcome.err)
		setInstanceConditions(ctx, r, instance, falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonCnsTaskStateUnknown,
			"The state of the CNS CreateVolume task could not be determined"))
		r.taskTracker.forgetResult(taskID)
		return nil, &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}, nil
	}
}

// resolveTaskDirectly reads a task's state straight from vCenter, for the cases
// where it cannot be subscribed to: it aged out, or vCenter restarted. This is
// the fallback path only; steady state goes through the listview.
func (r *ReconcileCnsRegisterVolume) resolveTaskDirectly(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, vc *cnsvsphere.VirtualCenter,
	createSpec *cnstypes.CnsVolumeCreateSpec, timeout time.Duration) (
	*volumes.CnsVolumeInfo, *reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	cr := instanceKey(instance)
	taskID := instance.Status.CnsTaskID
	taskMoRef := vim25types.ManagedObjectReference{Type: "Task", Value: taskID}

	state, volInfo, _, err := volumes.GetCreateVolumeTaskResult(ctx, vc, taskMoRef, createSpec.Name)
	switch state {
	case volumes.CreateVolumeTaskSucceeded:
		r.taskTracker.forget(cr, taskID)
		if setErr := setInstanceCnsTaskID(ctx, r, instance, ""); setErr != nil {
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, "CNS has registered the disk"))
		return volInfo, nil, nil

	case volumes.CreateVolumeTaskRunning:
		return nil, &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}, nil

	case volumes.CreateVolumeTaskFailed:
		r.taskTracker.forget(cr, taskID)
		if setErr := setInstanceCnsTaskID(ctx, r, instance, ""); setErr != nil {
			log.Errorf("failed to clear cnsTaskID on %s/%s: %v", instance.Namespace, instance.Name, setErr)
		}
		msg := fmt.Sprintf("failed to create CNS volume. Error: %v", err)
		setInstanceError(ctx, r, instance, msg,
			falseCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
				cnsregistervolumev1alpha1.ReasonCnsTaskFailed, "CNS reported a fault for the CreateVolume task"))
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil

	default:
		// Still unknown, e.g. vCenter unreachable. Hold everything and retry.
		log.Infof("CNS task %q for %s/%s remains unresolved (%v); retrying",
			taskID, instance.Namespace, instance.Name, err)
		return nil, &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}, nil
	}
}

// isPVCBoundNow reports whether the PVC is Bound right now, with a single Get.
//
// It replaces isPVCBound, which opened a per-CR watch against the API server
// and blocked the reconciler worker for up to 60s. During a burst that was up
// to MaxConcurrentReconciles simultaneous single-object watches, all of which
// this removes along with the blocking.
func isPVCBoundNow(ctx context.Context, client clientset.Interface,
	claim *v1.PersistentVolumeClaim) (bool, error) {
	pvc, err := client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(
		ctx, claim.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return pvc.Status.Phase == v1.ClaimBound, nil
}

// resolveInFlightTaskForDelete settles a CNS task that is still running when
// the CR is deleted.
//
// Returning a non-nil result means the caller must return it and wait: the task
// has not finished, so neither the finalizer nor any cleanup decision can be
// made yet. Returning nil means the task has settled, its volume (if any) has
// been cleaned up, and the normal delete path can proceed.
func (r *ReconcileCnsRegisterVolume) resolveInFlightTaskForDelete(ctx context.Context,
	instance *cnsregistervolumev1alpha1.CnsRegisterVolume, request reconcile.Request,
	timeout time.Duration) *reconcile.Result {
	log := logger.GetLogger(ctx)
	cr := request.NamespacedName
	taskID := instance.Status.CnsTaskID

	outcome, done := r.taskTracker.outcome(taskID)
	if !done {
		log.Infof("CnsRegisterVolume %s/%s is being deleted while CNS task %q is still in flight; "+
			"holding the finalizer until it settles", instance.Namespace, instance.Name, taskID)
		return &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}
	}

	// The task finished. If it produced a volume, that volume exists in CNS
	// with no PV pointing at it, so clean it up before letting the CR go.
	if outcome.state == volumes.CreateVolumeTaskSucceeded && outcome.volInfo != nil {
		volumeID := outcome.volInfo.VolumeID.Id
		log.Infof("Cleaning up CNS volume %q created by a task that completed during deletion of %s/%s",
			volumeID, instance.Namespace, instance.Name)
		if err := r.cleanupCNSVolume(ctx, instance, volumeID); err != nil {
			log.Errorf("failed to clean up CNS volume %q during deletion: %v", volumeID, err)
			return &reconcile.Result{RequeueAfter: timeout}
		}
	}

	r.taskTracker.forget(cr, taskID)
	if err := setInstanceCnsTaskID(ctx, r, instance, ""); err != nil {
		log.Errorf("failed to clear cnsTaskID during deletion of %s/%s: %v",
			instance.Namespace, instance.Name, err)
		return &reconcile.Result{RequeueAfter: timeout}
	}
	return nil
}
