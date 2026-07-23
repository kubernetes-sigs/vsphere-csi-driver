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

// Package vksregistervolume implements the VKSRegisterVolume controller, which runs in the
// guest (VKS) cluster vsphere-syncer and bridges a pre-created guest PVC to a Supervisor-registered
// volume without exposing any vCenter-relative disk identity in the guest cluster.
package vksregistervolume

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_VKS_REGISTER_VOLUME"
	defaultMaxWorkerThreads = 10

	// vksRegisterVolumeFinalizer is added to every VKSRegisterVolume CR before any work begins.
	// On deletion the controller only removes this finalizer; it never deletes the guest PV/PVC
	// or the Supervisor CnsRegisterVolume (both owned by the caller that drives this CR).
	vksRegisterVolumeFinalizer = "cns.vmware.com/vks-register-volume"

	// TODO(T7): add labels stamped on the guest PV created by this controller
	// (cns.vmware.com/created-by, .../vksregistervolume-namespace, .../vksregistervolume-name)
	// alongside the guest PV spec builder in pvspec.go.
)

var (
	// backOffDuration maps a CR's NamespacedName to the current requeue delay.
	// Initialized to 1s on first reconcile; doubled on each failure up to MaxBackOffDurationForReconciler.
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// ReconcileVKSRegisterVolume reconciles VKSRegisterVolume objects in the guest cluster.
type ReconcileVKSRegisterVolume struct {
	// client is the controller-runtime split client for the guest cluster (reads from cache, writes to apiserver).
	client client.Client
	scheme *runtime.Scheme

	// recorder emits Kubernetes events on VKSRegisterVolume instances.
	recorder record.EventRecorder

	// k8sclient is the typed Kubernetes client for the guest cluster (StorageClasses, etc.).
	k8sclient clientset.Interface

	// supervisorNamespace is the Supervisor namespace in which the VKS cluster is deployed.
	supervisorNamespace string

	// supervisorClient is a CoreV1 client for the Supervisor cluster (Supervisor PVCs/PVs).
	supervisorClient clientset.Interface

	// supervisorCnsOperatorClient is a typed client for the Supervisor cluster restricted to
	// read-only GET of CnsRegisterVolume CRs (get, list, watch; no create or delete).
	supervisorCnsOperatorClient client.Client
}

// blank compile-time check.
var _ reconcile.Reconciler = &ReconcileVKSRegisterVolume{}

// Add creates a new VKSRegisterVolume Controller and registers it with the manager.
//
// Guard: only active when clusterFlavor == CnsClusterFlavorGuest AND the VKSRegisterVolume FSS
// is enabled in both the guest PVCSI ConfigMap and the Supervisor csi-feature-states ConfigMap.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()

	if clusterFlavor != cnstypes.CnsClusterFlavorGuest {
		log.Debug("Not initializing the VKSRegisterVolume Controller as it is not a Guest CSI deployment")
		return nil
	}
	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VKSRegisterVolume) {
		log.Infof("Not initializing the VKSRegisterVolume Controller as %s FSS is disabled",
			common.VKSRegisterVolume)
		return nil
	}
	_ = volumeManager

	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	supervisorNamespace, err := commonconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("Failed to get supervisor namespace. Err: %v", err)
		return err
	}

	restClientConfig := k8s.GetRestClientConfigForSupervisor(ctx,
		configInfo.Cfg.GC.Endpoint, configInfo.Cfg.GC.Port)

	supervisorClient, err := k8s.NewSupervisorClient(ctx, restClientConfig)
	if err != nil {
		log.Errorf("Failed to create supervisorClient. Err: %v", err)
		return err
	}

	supervisorCnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, apis.GroupName)
	if err != nil {
		log.Errorf("Failed to create supervisorCnsOperatorClient. Err: %v", err)
		return err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{Interface: k8sclient.CoreV1().Events("")},
	)
	recorder := eventBroadcaster.NewRecorder(k8sscheme.Scheme, corev1.EventSource{Component: apis.GroupName})

	reconciler := newReconciler(mgr, recorder, k8sclient, supervisorNamespace,
		supervisorClient, supervisorCnsOperatorClient)
	return add(mgr, reconciler)
}

// add wires the reconciler into the controller-runtime manager.
// Called from Add() once all dependencies are initialised.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadsEnvVar, defaultMaxWorkerThreads)
	err := ctrl.NewControllerManagedBy(mgr).
		Named("vksregistervolume-controller").
		For(&vksregistervolumev1alpha1.VKSRegisterVolume{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("Failed to build vksregistervolume controller. Err: %v", err)
		return err
	}
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	return nil
}

// newReconciler constructs the reconciler once all Supervisor clients are available.
// Kept separate from Add() so tests can construct ReconcileVKSRegisterVolume directly.
func newReconciler(
	mgr manager.Manager,
	recorder record.EventRecorder,
	k8sclient clientset.Interface,
	supervisorNamespace string,
	supervisorClient clientset.Interface,
	supervisorCnsOperatorClient client.Client,
) *ReconcileVKSRegisterVolume {
	return &ReconcileVKSRegisterVolume{
		client:                      mgr.GetClient(),
		scheme:                      mgr.GetScheme(),
		recorder:                    recorder,
		k8sclient:                   k8sclient,
		supervisorNamespace:         supervisorNamespace,
		supervisorClient:            supervisorClient,
		supervisorCnsOperatorClient: supervisorCnsOperatorClient,
	}
}

// Reconcile implements the reconcile.Reconciler interface.
//
// Phase state machine (T5 wires steps 1–3; T6 wires steps 4–5; T7 wires steps 6–8):
//  1. Finalizer — add vksRegisterVolumeFinalizer; requeue if absent.
//  2. Validate spec fields (terminal Failed on any missing field).
//  3. Resolve guest PVC: GET + validate (Pending, volumeName set, accessModes, storage>0,
//     storageClass w/ svstorageclass); transient requeue if PVC not yet created (bounded window),
//     terminal Failed otherwise.
//     3b. VolumeMode default — Filesystem if nil/empty.
//  4. [TODO T6] WaitingForSupervisorRegistration — GET Supervisor CnsRegisterVolume; wait Registered==true.
//  5. [TODO T6] WaitingForSupervisorBinding — GET Supervisor PVC; wait Bound; read topology.
//  6. [TODO T7] CreatingGuestPV — GET-before-CREATE guest PV with volumeHandle, claimRef, nodeAffinity.
//  7. [TODO T7] WaitingForGuestPVCBound — poll until guest PVC↔PV are Bound.
//  8. [TODO T7] Registered — set Phase=Registered, Registered=true.
func (r *ReconcileVKSRegisterVolume) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Fetch the VKSRegisterVolume instance.
	instance := &vksregistervolumev1alpha1.VKSRegisterVolume{}
	if err := r.client.Get(ctx, request.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			log.With("name", request.Name).With("namespace", request.Namespace).
				Info("VKSRegisterVolume resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading VKSRegisterVolume %s/%s: %+v",
			request.Namespace, request.Name, err)
		return reconcile.Result{}, err
	}

	// Initialise the per-CR exponential backoff duration on first reconcile.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	// Handle CR deletion — the controller only removes the finalizer; it never deletes
	// the guest PV/PVC or the Supervisor CnsRegisterVolume (both owned by the caller that
	// drives this CR).
	if instance.DeletionTimestamp != nil {
		log.With("name", instance.Name).With("namespace", instance.Namespace).
			Info("VKSRegisterVolume instance is marked for deletion")
		return r.reconcileDelete(ctx, instance, request)
	}

	// If already registered, remove from the work queue — nothing left to do.
	if instance.Status.Registered {
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	log.Infof("Reconciling VKSRegisterVolume %s/%s (backoff %s)",
		request.Namespace, request.Name, timeout)

	// ── Step 1: Finalizer ──────────────────────────────────────────────────────────────────────
	if err := protectInstance(ctx, r.client, instance); err != nil {
		log.Errorf("Failed to add finalizer to VKSRegisterVolume %s/%s: %+v",
			instance.Namespace, instance.Name, err)
		r.setStatusError(ctx, instance, "Failed to add finalizer")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// ── Step 2: Validate spec (terminal failure on bad spec) ───────────────────────────────────
	if err := validateSpec(ctx, &instance.Spec); err != nil {
		log.Errorf("VKSRegisterVolume %s/%s spec validation failed: %v",
			instance.Namespace, instance.Name, err)
		r.setStatusFailed(ctx, instance, err.Error())
		return reconcile.Result{}, nil // no requeue — terminal
	}

	// ── Step 3: Resolve and validate the referenced guest PVC ─────────────────────────────────
	pvc, terminal, err := resolveGuestPVC(ctx, r.client, r.k8sclient, instance)
	if err != nil {
		log.Errorf("VKSRegisterVolume %s/%s guest PVC resolution failed (terminal=%v): %v",
			instance.Namespace, instance.Name, terminal, err)
		if terminal {
			r.setStatusFailed(ctx, instance, err.Error())
			return reconcile.Result{}, nil // no requeue — terminal
		}
		r.setStatusError(ctx, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// ── Step 3b: Default VolumeMode to Filesystem if unset ────────────────────────────────────
	volumeMode := defaultVolumeMode(pvc.Spec.VolumeMode)
	// volumeMode is forwarded to buildGuestPV in T7.  Suppress unused-var until then.
	_ = volumeMode

	// ── Advance phase to WaitingForSupervisorRegistration ────────────────────────────────────
	if err := r.setStatusPhase(ctx, instance,
		vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorRegistration); err != nil {
		log.Errorf("VKSRegisterVolume %s/%s: failed to advance phase: %v",
			instance.Namespace, instance.Name, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// ── Step 4: WaitingForSupervisorRegistration ──────────────────────────────────────────────
	supervisorPVCName, terminal, err := waitForSupervisorRegistration(ctx,
		r.supervisorCnsOperatorClient, r.supervisorNamespace, instance)
	if err != nil {
		log.Errorf("VKSRegisterVolume %s/%s waiting for supervisor registration failed (terminal=%v): %v",
			instance.Namespace, instance.Name, terminal, err)
		if terminal {
			r.setStatusFailed(ctx, instance, err.Error())
			return reconcile.Result{}, nil // no requeue — terminal
		}
		r.setStatusError(ctx, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// ── Advance phase to WaitingForSupervisorBinding ──────────────────────────────────────────
	if err := r.setStatusPhase(ctx, instance,
		vksregistervolumev1alpha1.VKSRegisterVolumePhaseWaitingForSupervisorBinding); err != nil {
		log.Errorf("VKSRegisterVolume %s/%s: failed to advance phase: %v",
			instance.Namespace, instance.Name, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// ── Step 5: WaitingForSupervisorBinding ───────────────────────────────────────────────────
	accessibleTopology, terminal, err := waitForSupervisorBinding(ctx,
		r.supervisorClient, r.supervisorNamespace, supervisorPVCName)
	if err != nil {
		log.Errorf("VKSRegisterVolume %s/%s waiting for supervisor binding failed (terminal=%v): %v",
			instance.Namespace, instance.Name, terminal, err)
		if terminal {
			r.setStatusFailed(ctx, instance, err.Error())
			return reconcile.Result{}, nil // no requeue — terminal
		}
		r.setStatusError(ctx, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// supervisorPVCName, accessibleTopology, pvc, and volumeMode are consumed by T7
	// (CreatingGuestPV: buildGuestPV + create). Suppress unused-var until then.
	_ = supervisorPVCName
	_ = accessibleTopology
	_ = pvc

	// ── TODO(T7): CreatingGuestPV ─────────────────────────────────────────────────────────────
	// guestPVName = pvc.Spec.VolumeName
	// GET-before-CREATE guest PV via r.client (idempotent).
	// If not found: call buildGuestPV(pvc, supervisorPVCName, volumeMode, accessibleTopology, instance)
	//   then r.client.Create.
	// If found: validate volumeHandle == supervisorPVCName AND claimRef → {instance.Namespace, pvcName};
	//   if wrong claimRef (bound to a different PVC) → terminal Failed.

	// ── TODO(T7): WaitingForGuestPVCBound ────────────────────────────────────────────────────
	// GET guest PVC; if Bound to guestPVName → advance.
	// If Bound to a different PV → terminal Failed.
	// If still Pending → requeue.

	// ── TODO(T7): Registered ─────────────────────────────────────────────────────────────────
	// setStatusRegistered(ctx, instance)

	return reconcile.Result{RequeueAfter: timeout}, nil
}

// reconcileDelete handles VKSRegisterVolume CR deletion.
//
// The controller removes only the vksRegisterVolumeFinalizer. It does NOT:
//   - delete the guest PV or PVC (owned by the workload / the caller that drives this CR)
//   - delete the Supervisor CnsRegisterVolume (owned by the caller that drives this CR)
//
// This is safe: deleting the CnsRegisterVolume CR does not delete the Supervisor PVC/PV
// it registered, so the guest PV's csi.volumeHandle keeps resolving after CR removal.
func (r *ReconcileVKSRegisterVolume) reconcileDelete(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx).With("name", instance.Name).With("namespace", instance.Namespace)
	log.Info("Removing finalizer from VKSRegisterVolume (guest PV/PVC and Supervisor CR retained)")

	if err := removeFinalizer(ctx, r.client, instance); err != nil {
		log.Warnf("Failed to remove finalizer: %v", err)
		backOffDurationMapMutex.Lock()
		timeout := backOffDuration[request.NamespacedName]
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}, nil
}

// ── Status helpers ────────────────────────────────────────────────────────────────────────────

// setStatusFailed sets Phase=Failed and Error, records a Warning event, and increments the
// backoff duration.  The caller must return reconcile.Result{} (no requeue) after this call.
func (r *ReconcileVKSRegisterVolume) setStatusFailed(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume, errMsg string) {
	log := logger.GetLogger(ctx)
	orig := instance.DeepCopy()
	instance.Status.Phase = vksregistervolumev1alpha1.VKSRegisterVolumePhaseFailed
	instance.Status.Registered = false
	instance.Status.Error = errMsg
	if err := patchVKSRegisterVolumeStatus(ctx, r.client, orig, instance); err != nil {
		log.Errorf("patchVKSRegisterVolumeStatus failed while setting Failed: %v", err)
	}
	recordEvent(ctx, r, instance, corev1.EventTypeWarning, errMsg)
}

// setStatusError sets Status.Error (preserving the current Phase) and records a Warning event.
// Use for transient errors where the phase should not regress to Failed.
// The caller must return reconcile.Result{RequeueAfter: timeout} after this call.
func (r *ReconcileVKSRegisterVolume) setStatusError(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume, errMsg string) {
	log := logger.GetLogger(ctx)
	orig := instance.DeepCopy()
	instance.Status.Error = errMsg
	if err := patchVKSRegisterVolumeStatus(ctx, r.client, orig, instance); err != nil {
		log.Errorf("patchVKSRegisterVolumeStatus failed while setting error: %v", err)
	}
	recordEvent(ctx, r, instance, corev1.EventTypeWarning, errMsg)
}

// setStatusPhase advances Phase, clears Error, and patches status.
func (r *ReconcileVKSRegisterVolume) setStatusPhase(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	phase vksregistervolumev1alpha1.VKSRegisterVolumePhase) error {
	if instance.Status.Phase == phase {
		return nil // already at this phase — idempotent
	}
	orig := instance.DeepCopy()
	instance.Status.Phase = phase
	instance.Status.Error = ""
	return patchVKSRegisterVolumeStatus(ctx, r.client, orig, instance)
}

// TODO(T7): add setStatusRegistered(ctx, instance) — sets Phase=Registered, Registered=true,
// clears Error, and records a Normal event — and wire it into the Registered step of Reconcile.

// patchVKSRegisterVolumeStatus sends a merge-patch for the status subresource.
// Mirrors patchCnsRegisterVolumeStatus: always includes "registered" (required field),
// includes "phase" when set, and includes "error" only when changing.
// Retries up to 3 times with a short sleep between attempts.
func patchVKSRegisterVolumeStatus(ctx context.Context, c client.Client,
	oldObj *vksregistervolumev1alpha1.VKSRegisterVolume,
	newObj *vksregistervolumev1alpha1.VKSRegisterVolume) error {
	log := logger.GetLogger(ctx)

	statusMap := map[string]interface{}{
		"registered": newObj.Status.Registered,
	}
	if newObj.Status.Phase != "" {
		statusMap["phase"] = string(newObj.Status.Phase)
	}
	// Include error when setting a new message or explicitly clearing a previous one.
	if newObj.Status.Error != "" || (oldObj.Status.Error != "" && newObj.Status.Error == "") {
		statusMap["error"] = newObj.Status.Error
	}

	patchBytes, err := json.Marshal(map[string]interface{}{"status": statusMap})
	if err != nil {
		return fmt.Errorf("failed to marshal status patch: %w", err)
	}

	rawPatch := client.RawPatch(apitypes.MergePatchType, patchBytes)
	const maxRetries = 3
	for attempt := 1; ; attempt++ {
		patchErr := c.Status().Patch(ctx, oldObj, rawPatch)
		if patchErr == nil {
			log.Debugf("Patched VKSRegisterVolume status %s/%s", oldObj.Namespace, oldObj.Name)
			return nil
		}
		if attempt >= maxRetries {
			log.Errorf("failed to patch VKSRegisterVolume status %s/%s after %d attempts: %v",
				oldObj.Namespace, oldObj.Name, maxRetries, patchErr)
			return patchErr
		}
		log.Warnf("attempt %d: patch VKSRegisterVolume status %s/%s failed: %v; retrying...",
			attempt, oldObj.Namespace, oldObj.Name, patchErr)
		time.Sleep(100 * time.Millisecond)
	}
}

// recordEvent emits a Kubernetes event and updates the backoff duration.
// Warning events double the backoff (up to MaxBackOffDurationForReconciler).
// Normal events reset the backoff to 1s.
func recordEvent(ctx context.Context, r *ReconcileVKSRegisterVolume,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume, eventtype string, msg string) {
	if r.recorder == nil {
		return
	}
	nn := apitypes.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}
	switch eventtype {
	case corev1.EventTypeWarning:
		backOffDurationMapMutex.Lock()
		backOffDuration[nn] = min(backOffDuration[nn]*2, cnsoperatortypes.MaxBackOffDurationForReconciler)
		r.recorder.Event(instance, corev1.EventTypeWarning, "VKSRegisterVolumeFailed", msg)
		backOffDurationMapMutex.Unlock()
	case corev1.EventTypeNormal:
		backOffDurationMapMutex.Lock()
		backOffDuration[nn] = time.Second
		r.recorder.Event(instance, corev1.EventTypeNormal, "VKSRegisterVolumeSucceeded", msg)
		backOffDurationMapMutex.Unlock()
	}
}

// ── Finalizer helpers ─────────────────────────────────────────────────────────────────────────

// protectInstance adds vksRegisterVolumeFinalizer to the CR if not already present.
func protectInstance(ctx context.Context, c client.Client,
	obj *vksregistervolumev1alpha1.VKSRegisterVolume) error {
	log := logger.GetLogger(ctx).With("name", obj.Name).With("namespace", obj.Namespace)
	if controllerutil.ContainsFinalizer(obj, vksRegisterVolumeFinalizer) {
		log.With("finalizer", vksRegisterVolumeFinalizer).Debug("Finalizer already present")
		return nil
	}
	log.With("finalizer", vksRegisterVolumeFinalizer).Info("Adding finalizer")
	controllerutil.AddFinalizer(obj, vksRegisterVolumeFinalizer)
	return c.Update(ctx, obj)
}

// removeFinalizer removes vksRegisterVolumeFinalizer from the CR.
func removeFinalizer(ctx context.Context, c client.Client,
	obj *vksregistervolumev1alpha1.VKSRegisterVolume) error {
	log := logger.GetLogger(ctx).With("name", obj.Name).With("namespace", obj.Namespace)
	if !controllerutil.ContainsFinalizer(obj, vksRegisterVolumeFinalizer) {
		log.With("finalizer", vksRegisterVolumeFinalizer).Debug("Finalizer not present")
		return nil
	}
	log.With("finalizer", vksRegisterVolumeFinalizer).Info("Removing finalizer")
	controllerutil.RemoveFinalizer(obj, vksRegisterVolumeFinalizer)
	return c.Update(ctx, obj)
}
