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
var getCreateVolumeTaskResultFn = volumes.GetCreateVolumeTaskResult
var taskVirtualCenterFn = (*ReconcileCnsRegisterVolume).taskVirtualCenter

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

	if instance.Status.CnsVolumeID != "" {
		// The caller queries this persisted result before Kubernetes work.
		// No second task or extra query is needed for bind/finalization retries.
		return &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: instance.Status.CnsVolumeID}}, nil, nil
	}
	if err := r.taskTracker.ensureInit(ctx, r.volumeManager, vc); err != nil {
		msg := fmt.Sprintf("failed to initialize CNS task tracking: %v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil
	}

	if instance.Status.CnsTaskID == "" {
		r.taskTracker.mu.Lock()
		pending := r.taskTracker.slots[instanceKey(instance)]
		r.taskTracker.mu.Unlock()
		if pending != "" {
			if err := setInstanceCnsTaskID(ctx, r, instance, pending); err != nil {
				return nil, &reconcile.Result{RequeueAfter: timeout}, nil
			}
			return r.pollCreateVolume(ctx, instance, vc, createSpec, timeout)
		}
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

	if r.isSnapServiceRestore(ctx, instance) {
		if err := r.reserveRestore(ctx, instance); err != nil {
			r.taskTracker.releaseSlot(cr)
			setInstanceError(ctx, r, instance, err.Error(), falseCondition(
				cnsregistervolumev1alpha1.ConditionRestoreValidated,
				cnsregistervolumev1alpha1.ReasonRestorePrecheckFailed, "Waiting to persist restore admission"))
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
	}
	taskMoRef, _, err := submitCreateVolumeTaskFn(ctx, vc, createSpec)
	if err != nil {
		r.taskTracker.releaseSlot(cr)
		msg := fmt.Sprintf("failed to submit CNS CreateVolume task. Error: %v", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Persist the reference before subscribing. Retain it in memory if the
	// write fails so the next pass retries persistence rather than submission.
	// A process crash in the submit/persist window can still lose the reference.
	r.taskTracker.mu.Lock()
	r.taskTracker.slots[cr] = taskMoRef.Value
	r.taskTracker.mu.Unlock()
	if err := setInstanceCnsTaskID(ctx, r, instance, taskMoRef.Value); err != nil {
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
		if err := r.persistCnsExecution(ctx, instance, "", outcome.volInfo.VolumeID.Id); err != nil {
			log.Errorf("failed to persist CNS result on %s/%s: %v", instance.Namespace, instance.Name, err)
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		r.taskTracker.forget(cr, taskID)
		setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, "CNS has registered the disk"))
		return outcome.volInfo, nil, nil

	case volumes.CreateVolumeTaskFailed:
		// Today's behaviour is preserved exactly: a CreateVolume failure is
		// retried indefinitely rather than becoming terminal. Deciding which
		// CNS faults are permanent needs agreement with VM Operator first,
		// because a misclassification wedges a VM that currently self-heals.
		if err := setInstanceCnsTaskID(ctx, r, instance, ""); err != nil {
			log.Errorf("failed to clear cnsTaskID after failure on %s/%s: %v",
				instance.Namespace, instance.Name, err)
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		r.taskTracker.forget(cr, taskID)
		if instance.Status.CnsTaskID == "" {
			if err := r.releaseRestoreReservation(ctx, instance); err != nil {
				return nil, &reconcile.Result{RequeueAfter: timeout}, nil
			}
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
		return r.resolveTaskDirectly(ctx, instance, vc, createSpec, timeout)
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

	state, volInfo, _, err := getCreateVolumeTaskResultFn(ctx, vc, taskMoRef, createSpec.Name)
	if state == volumes.CreateVolumeTaskUnknown && cnsvsphere.IsManagedObjectNotFound(err, taskMoRef) {
		// Recover a successful registration before clearing the task. This
		// also protects deletion in the interval before a replacement task
		// would otherwise be submitted.
		filter := cnstypes.CnsQueryFilter{}
		if instance.Spec.VolumeID != "" {
			filter.VolumeIds = []cnstypes.CnsVolumeId{{Id: instance.Spec.VolumeID}}
		} else {
			filter.Names = []string{createSpec.Name}
		}
		if r.configInfo == nil || r.volumeManager == nil {
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		cfg := r.configInfo.GetCfg()
		if cfg == nil {
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		clusterID := cfg.Global.ClusterID
		if isTKGSHAEnabled {
			clusterID = cfg.Global.SupervisorID
		}
		if clusterID == "" {
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		filter.ContainerClusterIds = []string{clusterID}
		result, queryErr := r.volumeManager.QueryAllVolume(ctx, filter, cnstypes.CnsQuerySelection{})
		if queryErr != nil || result == nil || len(result.Volumes) > 1 ||
			result.Cursor.TotalRecords > int64(len(result.Volumes)) {
			log.Errorf("cannot resolve missing task %q: query result %v, error %v", taskID, result, queryErr)
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		volumeID := ""
		if len(result.Volumes) == 1 {
			volumeID = result.Volumes[0].VolumeId.Id
			if volumeID == "" || (instance.Spec.VolumeID != "" && instance.Spec.VolumeID != volumeID) {
				return nil, &reconcile.Result{RequeueAfter: timeout}, nil
			}
		}
		if err := r.persistCnsExecution(ctx, instance, "", volumeID); err != nil {
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		r.taskTracker.forget(cr, taskID)
		if volumeID != "" {
			setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
				cnsregistervolumev1alpha1.ReasonRegistered, "CNS has registered the disk"))
			return &volumes.CnsVolumeInfo{VolumeID: cnstypes.CnsVolumeId{Id: volumeID}}, nil, nil
		}
		if instance.DeletionTimestamp != nil {
			return nil, nil, nil
		}
		// No matching registration was found; retry admission and submit a
		// replacement task while retaining any restore reservation.
		return nil, &reconcile.Result{RequeueAfter: timeout}, nil
	}
	switch state {
	case volumes.CreateVolumeTaskSucceeded:
		if setErr := r.persistCnsExecution(ctx, instance, "", volInfo.VolumeID.Id); setErr != nil {
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		r.taskTracker.forget(cr, taskID)
		setInstanceConditions(ctx, r, instance, trueCondition(cnsregistervolumev1alpha1.ConditionCnsVolumeRegistered,
			cnsregistervolumev1alpha1.ReasonRegistered, "CNS has registered the disk"))
		return volInfo, nil, nil

	case volumes.CreateVolumeTaskRunning:
		return nil, &reconcile.Result{RequeueAfter: cnsTaskSafetyRequeue}, nil

	case volumes.CreateVolumeTaskFailed:
		if setErr := setInstanceCnsTaskID(ctx, r, instance, ""); setErr != nil {
			log.Errorf("failed to clear cnsTaskID on %s/%s: %v", instance.Namespace, instance.Name, setErr)
			return nil, &reconcile.Result{RequeueAfter: timeout}, nil
		}
		r.taskTracker.forget(cr, taskID)
		if instance.Status.CnsTaskID == "" {
			if releaseErr := r.releaseRestoreReservation(ctx, instance); releaseErr != nil {
				return nil, &reconcile.Result{RequeueAfter: timeout}, nil
			}
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
	if instance.Status.CnsTaskID != "" {
		taskID := instance.Status.CnsTaskID
		outcome, done := r.taskTracker.outcome(taskID)
		if !done || outcome.state == volumes.CreateVolumeTaskUnknown {
			if done {
				r.taskTracker.forgetResult(taskID)
			}
			vc, err := taskVirtualCenterFn(r, ctx)
			if err != nil {
				log.Errorf("Cannot recover task during deletion: %v", err)
				return &reconcile.Result{RequeueAfter: timeout}
			}
			if err := r.taskTracker.ensureInit(ctx, r.volumeManager, vc); err != nil {
				return &reconcile.Result{RequeueAfter: timeout}
			}
			// Monitoring/deletion must not depend on a StorageClass that may
			// have disappeared since submission. Only the stable name is needed.
			name := staticPvNamePrefix + instance.Spec.VolumeID
			if instance.Spec.VolumeID == "" {
				name = staticPvNamePrefix + string(instance.UID)
			}
			spec := &cnstypes.CnsVolumeCreateSpec{Name: name}
			var retry *reconcile.Result
			if done {
				_, retry, err = r.resolveTaskDirectly(ctx, instance, vc, spec, timeout)
			} else {
				_, retry, err = r.pollCreateVolume(ctx, instance, vc, spec, timeout)
			}
			if err != nil {
				return &reconcile.Result{RequeueAfter: timeout}
			}
			if retry != nil {
				return retry
			}
		} else if outcome.state == volumes.CreateVolumeTaskSucceeded {
			if err := r.persistCnsExecution(ctx, instance, "", outcome.volInfo.VolumeID.Id); err != nil {
				return &reconcile.Result{RequeueAfter: timeout}
			}
			r.taskTracker.forget(request.NamespacedName, taskID)
		} else {
			if err := setInstanceCnsTaskID(ctx, r, instance, ""); err != nil {
				return &reconcile.Result{RequeueAfter: timeout}
			}
			r.taskTracker.forget(request.NamespacedName, taskID)
		}
	}
	if volumeID := instance.Status.CnsVolumeID; volumeID != "" {
		pv, err := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, staticPvNamePrefix+volumeID, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return &reconcile.Result{RequeueAfter: timeout}
			}
			pv = nil
		}
		// Existing PVs go through the normal bound/ownership checks first.
		if pv == nil {
			if err := r.cleanupCNSVolume(ctx, instance, volumeID); err != nil {
				return &reconcile.Result{RequeueAfter: timeout}
			}
		}
	}
	return nil
}

func (r *ReconcileCnsRegisterVolume) taskVirtualCenter(ctx context.Context) (*cnsvsphere.VirtualCenter, error) {
	if r.configInfo == nil {
		return nil, fmt.Errorf("vCenter configuration is unavailable")
	}
	cfg, err := cnsvsphere.GetVirtualCenterConfig(ctx, r.configInfo.GetCfg())
	if err != nil {
		return nil, err
	}
	return cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, cfg, false)
}
