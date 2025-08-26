/*
Copyright 2025 The Kubernetes Authors.

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

package cnsnodevmbatchattachment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"

	vmoperatorv1alpha4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	v1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// backOffDuration is a map of cnsnodevmbatchattachment name's to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[types.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

const (
	defaultMaxWorkerThreads = 10
)

func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsNodeVmBatchAttachment Controller as its a non-WCP CSI deployment")
		return nil
	}

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SharedDiskFss) {
		log.Debug("Not initializing the CnsNodeVmBatchAttachment Controller as SharedDisk FSS is not enabled")
		return nil
	}

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsnodevmbatchattachment instances to
	// the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)

	restClientConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize rest clientconfig. Error: %+v", err)
		log.Error(msg)
		return err
	}

	vmOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, vmoperatorv1alpha4.GroupName)
	if err != nil {
		msg := fmt.Sprintf("Failed to initialize vmOperatorClient. Error: %+v", err)
		log.Error(msg)
		return err
	}

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: cnsoperatorapis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, vmOperatorClient, recorder))
}

func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	volumeManager volumes.Manager, vmOperatorClient client.Client,
	recorder record.EventRecorder) reconcile.Reconciler {
	return &Reconciler{client: mgr.GetClient(),
		scheme:     mgr.GetScheme(),
		configInfo: *configInfo, volumeManager: volumeManager,
		vmOperatorClient: vmOperatorClient,
		recorder:         recorder, instanceLock: sync.Map{}}
}

// add adds this package's controller to the provided manager.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := getMaxWorkerThreads(ctx)

	backOffDuration = make(map[types.NamespacedName]time.Duration)

	// Create a new controller.
	err := ctrl.NewControllerManagedBy(mgr).
		Named("cnsnodevmbatchattachment-controller").
		For(&v1alpha1.CnsNodeVmBatchAttachment{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxWorkerThreads,
		}).
		Complete(r)

	if err != nil {
		log.Errorf("failed to watch for changes to CnsNodeVmBatchAttachment resource with error: %+v", err)
		return err
	}
	return nil
}

// Reconciler reconciles a CnsNodeVmBatchAttachment object.
type Reconciler struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client           client.Client
	scheme           *runtime.Scheme
	configInfo       config.ConfigurationInfo
	volumeManager    volumes.Manager
	vmOperatorClient client.Client
	recorder         record.EventRecorder
	// instanceLock to ensure that for an instance we have only
	// one reconciliation at a time.
	instanceLock sync.Map
}

// getMaxWorkerThreads returns the maximum
// number of worker threads which can be run to reconcile CnsNodeVmBatchAttachment
// instances. If environment variable WORKER_THREADS_NODEVM_BATCH_ATTACH is set and
// valid, return the value read from environment variable otherwise, use the
// default value.
func getMaxWorkerThreads(ctx context.Context) int {
	log := logger.GetLogger(ctx)

	workerThreads := defaultMaxWorkerThreads
	envVal := os.Getenv("WORKER_THREADS_NODEVM_BATCH_ATTACH")
	if envVal == "" {
		log.Debugf("WORKER_THREADS_NODEVM_BATCH_ATTACH is not set. Picking the default value %d",
			defaultMaxWorkerThreads)
		return workerThreads
	}

	value, err := strconv.Atoi(envVal)
	if err != nil {
		log.Warnf("Invalid value for WORKER_THREADS_NODEVM_BATCH_ATTACH: %s. Using default value %d",
			envVal, defaultMaxWorkerThreads)
		return workerThreads
	}

	switch {
	case value <= 0 || value > defaultMaxWorkerThreads:
		log.Warnf("Value %s for WORKER_THREADS_NODEVM_BATCH_ATTACH is invalid. Using default value %d",
			envVal, defaultMaxWorkerThreads)
	default:
		workerThreads = value
		log.Debugf("Maximum number of worker threads to reconcile CnsNodeVmBatchAttachment is set to %d",
			workerThreads)

	}
	return workerThreads
}

// Reconcile over CnsNodeVmBatchAttachment CR.
// Reconcile stops when all volumes have been attached or detached successfully.
func (r *Reconciler) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for CnsNodeVmBatchAttachment request: %q", request.NamespacedName)

	// Creating new context as kubernetes supplied context can get canceled.
	// This is required to ensure CNS operations won't get prematurely canceled by the controller runtime’s
	// internal reconcile logic.
	batchAttachCtx, cancel := context.WithTimeout(context.Background(),
		volumes.VolumeOperationTimeoutInSeconds*time.Second)
	defer cancel()

	batchAttachCtx = logger.NewContextWithLogger(batchAttachCtx)
	log := logger.GetLogger(batchAttachCtx)

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	// Acquire lock for the instance.
	actual, _ := r.instanceLock.LoadOrStore(request.NamespacedName, &sync.Mutex{})
	lock, ok := actual.(*sync.Mutex)
	if !ok {
		log.Errorf("failed to cast lock for instance %s", request.NamespacedName.String())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	lock.Lock()
	log.Infof("Acquired lock for instance %s", request.NamespacedName.String())
	defer func() {
		lock.Unlock()
		log.Infof("Released lock for instance %s", request.NamespacedName.String())
	}()

	instance := &v1alpha1.CnsNodeVmBatchAttachment{}
	err := r.client.Get(batchAttachCtx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CnsNodeVmBatchAttachment resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsNodeVmBatchAttachment with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - return with err.
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	log.Debugf("Reconciling CnsNodeVmBatchAttachment with Request.Name: %q instance %q timeout %q seconds",
		request.Name, instance.Name, timeout)

	// Initialise volumeStatus if it is set to nil
	if instance.Status.VolumeStatus == nil {
		instance.Status.VolumeStatus = make([]v1alpha1.VolumeStatus, 0)
	}

	// Get the VM object for the given UUID.
	vm, err := getVmObject(batchAttachCtx, r.client, r.configInfo, instance)
	if err != nil {
		return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
	}

	volumesToDetach := make(map[string]string)
	if vm == nil {
		// If VM is nil, it means it is deleted from the vCenter.
		if instance.DeletionTimestamp == nil {
			// If VM is deleted from the VC but CnsNodeVmBatchAttachment is not being deleted, it is an error.
			err := fmt.Errorf("virtual Machine with UUID %s on vCenter does not exist. "+
				"Vm is CR is deleted or is being deleted but"+
				"CnsNodeVmBatchAttachmentInstance %s is not being deleted", instance.Spec.NodeUUID, instance.Name)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
		// If CnsNodeVmBatchAttachment is also being deleted, then all volumes on the instance can be considered detached.
		log.Infof("VM is deleted from vCenter and instance %s deletion timestamp. Considering all volumes as detached.",
			request.NamespacedName)
	} else {
		// If VM was found on vCenter, find the volumes to be detached from it.
		volumesToDetach, err = getVolumesToDetach(batchAttachCtx, instance, vm, r.client)
		if err != nil {
			log.Errorf("failed to find volumes to detach for instance %s. Err: %s",
				request.NamespacedName.String(), err)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
	}

	// If instance is being deleted and VM object is also nil,
	// It means VM is deleted from the VC, VM CR is either already deleted or is being deleted.
	// This means all volumes can be considered detached. So remove finalizer from CR instance.
	if instance.DeletionTimestamp != nil && vm == nil {
		log.Infof("Instance %s is being deleted and VM object is also deleted from VC", request.NamespacedName.String())
		// TODO: remove PVC finalizer

		patchErr := removeFinalizerFromCRDInstance(batchAttachCtx, instance, r.client)
		if patchErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, patchErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, patchErr)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Infof("Successfully removed finalizer %s from instance %s",
			cnsoperatortypes.CNSFinalizer, request.NamespacedName.String())

		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()

		msg := fmt.Sprintf("ReconcileCnsNodeVmBatchAttachment: Successfully processed instance %s",
			request.NamespacedName.String())
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeNormal, msg)
		return reconcile.Result{}, nil
	}

	// The CR is not being deleted, so call attach and detach for volumes.
	if instance.DeletionTimestamp == nil {
		log.Infof("Starting reconciliation for instance %s", request.NamespacedName.String())

		// Add finalizer to CR if it does not already exist.
		if !controllerutil.ContainsFinalizer(instance, cnsoperatortypes.CNSFinalizer) {
			log.Debugf("Finalizer %s not found on instance %s. Adding it now.",
				cnsoperatortypes.CNSFinalizer, request.NamespacedName.String())
			err := k8s.PatchFinalizers(batchAttachCtx, r.client, instance,
				append(instance.Finalizers, cnsoperatortypes.CNSFinalizer))
			if err != nil {
				log.Errorf("failed to add finalizer on CRD instance %s, Err: %s", request.NamespacedName.String(), err)
				return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
			}
			log.Infof("Successfully added finalizer %s on instance %s",
				cnsoperatortypes.CNSFinalizer, request.NamespacedName.String())
		}

		// Call reconcile when deletion timestamp is not set on the instance.
		err := r.reconcileInstanceWithoutDeletionTimestamp(batchAttachCtx, instance, volumesToDetach, vm)
		if err != nil {
			log.Errorf("failed to reconile instance %s. Err: %s", request.NamespacedName.String(), err)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
		return r.completeReconciliationWithSuccess(batchAttachCtx, instance, request.NamespacedName, timeout)
	}

	// If the CR itself is being deleted, then first detach all volumes in it.
	if instance.DeletionTimestamp != nil {
		log.Infof("Deletion timestamp observed on instance %s. Detaching all volumes.", request.NamespacedName.String())

		// Call reconcile when deletion timestamp is set on the instance.
		err := r.reconcileInstanceWithDeletionTimestamp(batchAttachCtx, instance, volumesToDetach, vm)
		if err != nil {
			log.Errorf("failed to reconcile instance %s. Err: %s", request.NamespacedName.String(), err)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
		return r.completeReconciliationWithSuccess(batchAttachCtx, instance, request.NamespacedName, timeout)
	}

	reconcileLog.Infof("Reconcile for CnsNodeVmBatchAttachment request: %q completed.", request.NamespacedName)
	return reconcile.Result{}, nil
}

// reconcileInstanceWithDeletionTimestamp calls detach volume for all volumes present in volumesToDetach.
// As the instance is being deleted, we do not need to attach anything.
func (r *Reconciler) reconcileInstanceWithDeletionTimestamp(ctx context.Context,
	instance *v1alpha1.CnsNodeVmBatchAttachment,
	volumesToDetach map[string]string,
	vm *cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	err := r.processDetach(ctx, vm, instance, volumesToDetach)
	if err != nil {
		log.Errorf("failed to detach all volumes. Err: %s", err)
		return err
	}

	// CR is being deleted and all volumes were detached successfully.
	return removeFinalizerFromCRDInstance(ctx, instance, r.client)
}

// reconcileInstanceWithoutDeletionTimestamp calls CNS batch attach for all volumes in instance spec
// and CNS detach for the volumes volumesToDetach.
func (r *Reconciler) reconcileInstanceWithoutDeletionTimestamp(ctx context.Context,
	instance *v1alpha1.CnsNodeVmBatchAttachment,
	volumesToDetach map[string]string,
	vm *cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	// Call batch attach for volumes.
	err := r.processBatchAttach(ctx, vm, instance)
	if err != nil {
		log.Errorf("failed to attach all volumes. Err: %+v", err)
		return err
	}

	// Call detach if there are some volumes which need to be detached.
	if len(volumesToDetach) != 0 {
		err := r.processDetach(ctx, vm, instance, volumesToDetach)
		if err != nil {
			log.Errorf("failed to detach all volumes. Err: +v", err)
			return err
		}
		log.Infof("Successfully detached all volumes %+v", volumesToDetach)
	}
	return nil
}

// processDetach detaches each of the volumes in volumesToDetach by calling CNS DetachVolume API.
func (r *Reconciler) processDetach(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
	instance *v1alpha1.CnsNodeVmBatchAttachment, volumesToDetach map[string]string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("Calling detach volume for PVC %+v", volumesToDetach)

	volumesThatFailedToDetach := r.detachVolumes(ctx, vm, volumesToDetach, instance)

	var overallErr error
	if len(volumesThatFailedToDetach) != 0 {
		msg := "failed to detach volumes: "
		failedVolumes := strings.Join(volumesThatFailedToDetach, ",")
		msg += failedVolumes
		overallErr = errors.New(msg)
		log.Error(overallErr)
	}

	return overallErr
}

// detachVolumes calls Cns DetachVolume for every PVC in volumesToDetach.
func (r *Reconciler) detachVolumes(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, volumesToDetach map[string]string,
	instance *v1alpha1.CnsNodeVmBatchAttachment) []string {
	log := logger.GetLogger(ctx)

	volumesThatFailedToDetach := make([]string, 0)

	for pvc, volumeId := range volumesToDetach {
		log.Infof("Detach call started for PVC %s with volumeID %s in namespace %s for instance %s",
			pvc, volumeId, instance.Namespace, instance.Name)

		// Call CNS DetachVolume
		faulttype, detachErr := r.volumeManager.DetachVolume(ctx, vm, volumeId)
		if detachErr != nil {
			// If VM was not found, can assume that the detach is successful.
			if cnsvsphere.IsManagedObjectNotFound(detachErr, vm.VirtualMachine.Reference()) {
				log.Infof("Found a managed object not found fault for vm: %+v", vm)
				// TODO: remove PVC finalizer

				// Remove entry of this volume from the instance's status.
				deleteVolumeFromStatus(pvc, instance)
				log.Infof("Successfully detached volume %s from VM %s", pvc, instance.Spec.NodeUUID)
			} else {
				log.Errorf("failed to detach volume %s from VM %s. Fault: %s Err: %s",
					pvc, instance.Spec.NodeUUID, faulttype, detachErr)
				// Update the instance with error for this PVC.
				updateInstanceWithErrorForPvc(instance, pvc, detachErr.Error())
				volumesThatFailedToDetach = append(volumesThatFailedToDetach, pvc)
			}
		} else {
			// TODO: remove PVC finalizer
			// Remove entry of this volume from the instance's status.
			deleteVolumeFromStatus(pvc, instance)
			log.Infof("Successfully detached volume %s from VM %s", pvc, instance.Spec.NodeUUID)
		}
		log.Infof("Detach call ended for PVC %s in namespace %s for instance %s",
			pvc, instance.Namespace, instance.Name)
	}

	// Send back list of volumes which failed to attach.
	return volumesThatFailedToDetach
}

// processBatchAttach first constructs the batch attach volume request for all volumes in instance spec
// and then calls CNS batch attach for them.
func (r *Reconciler) processBatchAttach(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	instance *v1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)

	// Construct batch attach request
	pvcsInSpec, volumeIdsInSpec, batchAttachRequest, err := constructBatchAttachRequest(ctx, instance)
	if err != nil {
		log.Errorf("failed to construct batch attach request. Err: %s", err)
		return err
	}

	// Call CNS AttachVolume
	batchAttachResult, faultType, attachErr := r.volumeManager.BatchAttachVolumes(ctx, vm, batchAttachRequest)
	if attachErr != nil {
		log.Errorf("failed to batch attach all volumes. Fault: %s Err: %s", faultType, attachErr)
	} else {
		log.Infof("Successfully batch attached all volumes")
	}

	// Update instance based on the result of BatchAttach
	for _, result := range batchAttachResult {
		pvcName, ok := volumeIdsInSpec[result.VolumeID]
		if !ok {
			log.Errorf("failed to get pvcName for volumeID %s", result.VolumeID)
			return fmt.Errorf("failed to get pvcName for volumeID %s", result.VolumeID)
		}
		volumeName, ok := pvcsInSpec[pvcName]
		if !ok {
			log.Errorf("failed to get volumeName for pvc %s", pvcName)
			return fmt.Errorf("failed to get volumeName for pvc %s", pvcName)

		}
		// Update instance with attach result
		updateInstanceWithAttachVolumeResult(instance, volumeName, pvcName, result)
	}
	return attachErr
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *Reconciler,
	instance *v1alpha1.CnsNodeVmBatchAttachment, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	namespacedName := types.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
			cnsoperatortypes.MaxBackOffDurationForReconciler)
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "NodeVmBatchAttachFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "NodeVmBatchAttachSucceeded", msg)
		log.Info(msg)
	}
}

// completeReconciliationWithSuccess updates the instance with success and records successful event as well.
func (r *Reconciler) completeReconciliationWithSuccess(ctx context.Context, instance *v1alpha1.CnsNodeVmBatchAttachment,
	namespaceName types.NamespacedName, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	instance.Status.Error = ""
	updateErr := updateInstanceStatus(ctx, r.client, instance)
	if updateErr != nil {
		recordEvent(ctx, r, instance, v1.EventTypeWarning, updateErr.Error())
		log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", namespaceName, updateErr)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, namespaceName)
	backOffDurationMapMutex.Unlock()

	msg := fmt.Sprintf("ReconcileCnsNodeVmBatchAttachment: Successfully processed instance %s "+
		"in namespace %q.", namespaceName, namespaceName)
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return reconcile.Result{}, nil
}

// completeReconciliationWithError updates the instance with failure and records failure event as well.
func (r *Reconciler) completeReconciliationWithError(ctx context.Context, instance *v1alpha1.CnsNodeVmBatchAttachment,
	namespaceName types.NamespacedName, timeout time.Duration, err error) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	instance.Status.Error = err.Error()
	updateErr := updateInstanceStatus(ctx, r.client, instance)
	if updateErr != nil {
		recordEvent(ctx, r, instance, v1.EventTypeWarning, updateErr.Error())
		log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", namespaceName, updateErr)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, err.Error())
	return reconcile.Result{RequeueAfter: timeout}, nil

}
