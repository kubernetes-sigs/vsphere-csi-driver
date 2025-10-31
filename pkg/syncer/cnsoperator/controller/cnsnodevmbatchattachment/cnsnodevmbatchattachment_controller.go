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
	"strings"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/conditions"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
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
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
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
	// Per volume lock for concurrent access to PVCs.
	// Keys are strings representing namespace + PVC name.
	// Values are individual sync.Mutex locks that need to be held
	// to make updates to the PVC on the API server.
	VolumeLock *sync.Map
)

const (
	workerThreadsEnvVar     = "WORKER_THREADS_NODEVM_BATCH_ATTACH"
	defaultMaxWorkerThreads = 20
)

var newClientFunc = func(ctx context.Context) (kubernetes.Interface, error) {
	log := logger.GetLogger(ctx)

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return nil, err
	}

	return k8sclient, nil
}

func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsNodeVMBatchAttachment Controller as its a non-WCP CSI deployment")
		return nil
	}

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SharedDiskFss) {
		log.Debug("Not initializing the CnsNodeVMBatchAttachment Controller as SharedDisk FSS is not enabled")
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

	vmOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, vmoperatortypes.GroupName)
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

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx,
		workerThreadsEnvVar, defaultMaxWorkerThreads)
	// Create a new controller.
	err := ctrl.NewControllerManagedBy(mgr).
		Named("cnsnodevmbatchattachment-controller").
		For(&v1alpha1.CnsNodeVMBatchAttachment{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxWorkerThreads,
		}).
		Complete(r)
	if err != nil {
		log.Errorf("Failed to build application controller. Err: %v", err)
		return err
	}

	VolumeLock = &sync.Map{}
	backOffDuration = make(map[types.NamespacedName]time.Duration)
	return nil
}

// Reconciler reconciles a CnsNodeVMBatchAttachment object.
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

// Reconcile over CnsNodeVMBatchAttachment CR.
// Reconcile stops when all volumes have been attached or detached successfully.
func (r *Reconciler) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	reconcileLog := logger.GetLogger(ctx)
	reconcileLog.Infof("Received Reconcile for CnsNodeVMBatchAttachment request: %q", request.NamespacedName)

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

	instance := &v1alpha1.CnsNodeVMBatchAttachment{}
	err := r.client.Get(batchAttachCtx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CnsNodeVMBatchAttachment resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsNodeVMBatchAttachment with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - return with err.
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	log.Debugf("Reconciling CnsNodeVMBatchAttachment with Request.Name: %q instance %q timeout %q seconds",
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

	// Initializes kubernetes client.
	k8sClient, err := newClientFunc(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
	}

	volumesToDetach := make(map[string]string)
	if vm == nil {
		// If VM is nil, it means it is deleted from the vCenter.
		if instance.DeletionTimestamp == nil {
			// If VM is deleted from the VC but CnsNodeVMBatchAttachment is not being deleted, it is an error.
			err := fmt.Errorf("virtual Machine with UUID %s on vCenter does not exist. "+
				"Vm is CR is deleted or is being deleted but"+
				"CnsNodeVMBatchAttachmentInstance %s is not being deleted", instance.Spec.InstanceUUID, instance.Name)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
		// If CnsNodeVMBatchAttachment is also being deleted, then all volumes on the instance can be considered detached.
		log.Infof("VM is deleted from vCenter and instance %s deletion timestamp. Considering all volumes as detached.",
			request.NamespacedName)
	} else {
		// If VM was found on vCenter, find the volumes to be detached from it.
		volumesToDetach, err = getVolumesToDetach(batchAttachCtx, instance, vm, r.client, k8sClient)
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

		// For every PVC mentioned in instance.Spec, remove finalizer from its PVC.
		for _, volume := range instance.Spec.Volumes {
			err := removePvcFinalizer(ctx, r.client, k8sClient, volume.PersistentVolumeClaim.ClaimName, instance.Namespace,
				instance.Spec.InstanceUUID)
			if err != nil {
				updateInstanceVolumeStatus(instance, volume.Name, volume.PersistentVolumeClaim.ClaimName, "", "", err,
					v1alpha1.ConditionDetached, v1alpha1.ReasonDetachFailed)
				log.Errorf("failed to remove finalizer from PVC %s. Err: %s", volume.PersistentVolumeClaim.ClaimName,
					err)
				return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
			} else {
				updateInstanceVolumeStatus(instance, volume.Name, volume.PersistentVolumeClaim.ClaimName, "", "", err,
					v1alpha1.ConditionDetached, "")
			}
		}

		patchErr := removeFinalizerFromCRDInstance(batchAttachCtx, instance, r.client)
		if patchErr != nil {
			log.Errorf("failed to update CnsNodeVMBatchAttachment %s. Err: %s", instance.Name, patchErr)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
		log.Infof("Successfully removed finalizer %s from instance %s",
			cnsoperatortypes.CNSFinalizer, request.NamespacedName.String())

		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return r.completeReconciliationWithSuccess(batchAttachCtx, instance, request.NamespacedName, timeout)
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
		err := r.reconcileInstanceWithoutDeletionTimestamp(batchAttachCtx, k8sClient, instance, volumesToDetach, vm)
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
		err := r.reconcileInstanceWithDeletionTimestamp(batchAttachCtx, k8sClient, instance, volumesToDetach, vm)
		if err != nil {
			log.Errorf("failed to reconcile instance %s. Err: %s", request.NamespacedName.String(), err)
			return r.completeReconciliationWithError(batchAttachCtx, instance, request.NamespacedName, timeout, err)
		}
		return r.completeReconciliationWithSuccess(batchAttachCtx, instance, request.NamespacedName, timeout)
	}

	reconcileLog.Infof("Reconcile for CnsNodeVMBatchAttachment request: %q completed.", request.NamespacedName)
	return reconcile.Result{}, nil
}

// reconcileInstanceWithDeletionTimestamp calls detach volume for all volumes present in volumesToDetach.
// As the instance is being deleted, we do not need to attach anything.
func (r *Reconciler) reconcileInstanceWithDeletionTimestamp(ctx context.Context,
	k8sClient kubernetes.Interface,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	volumesToDetach map[string]string,
	vm *cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	err := r.processDetach(ctx, k8sClient, vm, instance, volumesToDetach)
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
	k8sClient kubernetes.Interface,
	instance *v1alpha1.CnsNodeVMBatchAttachment,
	volumesToDetach map[string]string,
	vm *cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	var detachErr error
	// Call detach if there are some volumes which need to be detached.
	if len(volumesToDetach) != 0 {
		detachErr = r.processDetach(ctx, k8sClient, vm, instance, volumesToDetach)
		if detachErr != nil {
			log.Errorf("failed to detach all volumes. Err: %s", detachErr)
		} else {
			log.Infof("Successfully detached all volumes %+v", volumesToDetach)
		}
	}

	// Call batch attach for volumes.
	attachErr := r.processBatchAttach(ctx, k8sClient, vm, instance)
	if attachErr != nil {
		log.Errorf("failed to attach all volumes. Err: %+v", attachErr)
	}

	return errors.Join(attachErr, detachErr)
}

// processDetach detaches each of the volumes in volumesToDetach by calling CNS DetachVolume API.
func (r *Reconciler) processDetach(ctx context.Context,
	k8sClient kubernetes.Interface,
	vm *cnsvsphere.VirtualMachine,
	instance *v1alpha1.CnsNodeVMBatchAttachment, volumesToDetach map[string]string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("Calling detach volume for PVC %+v", volumesToDetach)

	volumesThatFailedToDetach := r.detachVolumes(ctx, k8sClient, vm, volumesToDetach, instance)

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
	k8sClient kubernetes.Interface,
	vm *cnsvsphere.VirtualMachine, volumesToDetach map[string]string,
	instance *v1alpha1.CnsNodeVMBatchAttachment) []string {
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
				// VM not found, so marking detach as Success and removing finalizer from PVC
				volumesThatFailedToDetach = removeFinalizerAndStatusEntry(ctx, r.client, k8sClient,
					instance, pvc, volumesThatFailedToDetach)
			} else {
				log.Errorf("failed to detach volume %s from VM %s. Fault: %s Err: %s",
					pvc, instance.Spec.InstanceUUID, faulttype, detachErr)
				// Update the instance with error for this PVC.

				// First find a volumeName to fall back on. If entry for the given PVC is not found in the status,
				// then this volumeName will be used to add a new entry.
				allVolumeNamesInStatus := getVolumeNamesInStatus(instance)
				fallbackVolumeName := getUniqueVolumeName(pvc, allVolumeNamesInStatus)
				updateInstanceVolumeStatus(instance, fallbackVolumeName, pvc, "", "", detachErr,
					v1alpha1.ConditionDetached, v1alpha1.ReasonDetachFailed)

				volumesThatFailedToDetach = append(volumesThatFailedToDetach, pvc)
			}
		} else {
			// Remove finalizer from the PVC as the detach was successful.
			volumesThatFailedToDetach = removeFinalizerAndStatusEntry(ctx, r.client, k8sClient,
				instance, pvc, volumesThatFailedToDetach)
		}
		log.Infof("Detach call ended for PVC %s in namespace %s for instance %s",
			pvc, instance.Namespace, instance.Name)
	}

	// Send back list of volumes which failed to attach.
	return volumesThatFailedToDetach
}

// removeFinalizerAndStatusEntry removes finalizer from the given PVC and
// removes its entry from the instance status if it is successful.
// If removing the finalizer fails, it adds the volume to volumesThatFailedToDetach list.
func removeFinalizerAndStatusEntry(ctx context.Context, client client.Client, k8sClient kubernetes.Interface,
	instance *v1alpha1.CnsNodeVMBatchAttachment, pvc string,
	volumesThatFailedToDetach []string) []string {
	log := logger.GetLogger(ctx)

	err := removePvcFinalizer(ctx, client, k8sClient, pvc, instance.Namespace, instance.Spec.InstanceUUID)
	if err != nil {
		log.Errorf("failed to remove finalizer from PVC %s. Err: %s", pvc, err)
		// First find a volumeName to fall back on. If entry for the given PVC is not found in the status,
		// then this volumeName will be used to add a new entry.
		allVolumeNamesInStatus := getVolumeNamesInStatus(instance)
		fallbackVolumeName := getUniqueVolumeName(pvc, allVolumeNamesInStatus)
		updateInstanceVolumeStatus(instance, fallbackVolumeName, pvc, "", "", err,
			v1alpha1.ConditionDetached, v1alpha1.ReasonDetachFailed)

		volumesThatFailedToDetach = append(volumesThatFailedToDetach, pvc)
	} else {
		// Remove entry of this volume from the instance's status.
		deleteVolumeFromStatus(pvc, instance)
		log.Infof("Successfully detached volume %s from VM %s", pvc, instance.Spec.InstanceUUID)
	}
	return volumesThatFailedToDetach
}

// processBatchAttach first constructs the batch attach volume request for all volumes in instance spec
// and then calls CNS batch attach for them.
func (r *Reconciler) processBatchAttach(ctx context.Context, k8sClient kubernetes.Interface,
	vm *cnsvsphere.VirtualMachine,
	instance *v1alpha1.CnsNodeVMBatchAttachment) error {
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

		reason := v1alpha1.ReasonAttachFailed
		// If attach was successful, add finalizer to the PVC.
		if result.Error == nil {
			reason = ""
			// Add finalizer on PVC as attach was successful.
			err = addPvcFinalizer(ctx, r.client, k8sClient, pvcName, instance.Namespace, instance.Spec.InstanceUUID)
			if err != nil {
				log.Errorf("failed to add finalizer %s on PVC %s", cnsoperatortypes.CNSPvcFinalizer, pvcName)
				result.Error = err
				attachErr = errors.Join(attachErr,
					fmt.Errorf("failure during attach of PVC %s", pvcName))
			}
		}
		// Update instance with attach result.
		updateInstanceVolumeStatus(instance, volumeName, pvcName, result.VolumeID, result.DiskUUID, result.Error,
			v1alpha1.ConditionAttached, reason)

	}
	return attachErr
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *Reconciler,
	instance *v1alpha1.CnsNodeVMBatchAttachment, eventtype string, msg string) {
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
func (r *Reconciler) completeReconciliationWithSuccess(ctx context.Context, instance *v1alpha1.CnsNodeVMBatchAttachment,
	namespaceName types.NamespacedName, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	conditions.MarkTrue(instance, v1alpha1.ConditionReady)

	updateErr := updateInstanceStatus(ctx, r.client, instance)
	if updateErr != nil {
		recordEvent(ctx, r, instance, v1.EventTypeWarning, updateErr.Error())
		log.Errorf("failed to update CnsNodeVMBatchAttachment %s. Err: %s", namespaceName, updateErr)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, namespaceName)
	backOffDurationMapMutex.Unlock()

	msg := fmt.Sprintf("ReconcileCnsNodeVMBatchAttachment: Successfully processed instance %s "+
		"in namespace %q.", namespaceName, namespaceName)
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return reconcile.Result{}, nil
}

// completeReconciliationWithError updates the instance with failure and records failure event as well.
func (r *Reconciler) completeReconciliationWithError(ctx context.Context, instance *v1alpha1.CnsNodeVMBatchAttachment,
	namespaceName types.NamespacedName, timeout time.Duration, err error) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	trimmedError := trimMessage(err)
	conditions.MarkError(instance, v1alpha1.ConditionReady, v1alpha1.ReasonFailed, trimmedError)

	updateErr := updateInstanceStatus(ctx, r.client, instance)
	if updateErr != nil {
		recordEvent(ctx, r, instance, v1.EventTypeWarning, updateErr.Error())
		log.Errorf("failed to update CnsNodeVMBatchAttachment %s. Err: %s", namespaceName, updateErr)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, err.Error())
	return reconcile.Result{RequeueAfter: timeout}, nil

}
