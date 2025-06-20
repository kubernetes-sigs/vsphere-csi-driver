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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsnodevmbatchattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	cnsnode "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
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
	defaultMaxWorkerThreads         = 10
	maxParrallelAttachOrDetachCalls = 10
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
	ctx, _ := logger.GetNewContextWithLogger()
	return &ReconcileCnsNodeVmBatchAttachment{client: mgr.GetClient(),
		scheme:     mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager,
		vmOperatorClient: vmOperatorClient, nodeManager: cnsnode.GetManager(ctx),
		recorder: recorder, instanceLock: &sync.Map{}}
}

// add adds this package's controller to the provided manager.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := getMaxWorkerThreads(ctx)
	// Create a new controller.
	c, err := controller.New("cnsnodevmbatchattachment-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new CnsNodeVmBatchAttachment controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[types.NamespacedName]time.Duration)

	// Watch for changes to primary resource CnsNodeVmBatchAttachment.
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment{},
		&handler.TypedEnqueueRequestForObject[*cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment]{},
	))
	if err != nil {
		log.Errorf("failed to watch for changes to CnsNodeVmBatchAttachment resource with error: %+v", err)
		return err
	}
	return nil
}

// ReconcileCnsNodeVmBatchAttachment reconciles a CnsNodeVmBatchAttachment object.
type ReconcileCnsNodeVmBatchAttachment struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client           client.Client
	scheme           *runtime.Scheme
	configInfo       *config.ConfigurationInfo
	volumeManager    volumes.Manager
	vmOperatorClient client.Client
	nodeManager      cnsnode.Manager
	recorder         record.EventRecorder
	// instanceLock is required during parallel attach/detach oeprations
	// to ensure that only one goroutine is updating an instance's status at a time.
	instanceLock *sync.Map
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
	case value <= 0:
		log.Warnf("Value %s for WORKER_THREADS_NODEVM_BATCH_ATTACH is less than 1. Using default value %d",
			envVal, defaultMaxWorkerThreads)
	case value > defaultMaxWorkerThreads:
		log.Warnf("Value %s for WORKER_THREADS_NODEVM_BATCH_ATTACH is greater than %d. Using default value %d",
			envVal, defaultMaxWorkerThreads, defaultMaxWorkerThreads)
	default:
		workerThreads = value
		log.Debugf("Maximum number of worker threads to reconcile CnsNodeVmBatchAttachment is set to %d",
			workerThreads)

	}
	return workerThreads
}

// Reconcile over CnsNodeVmBatchAttchment CR.
// Reconcile stops when all volumes have been attached or detached successfully.
func (r *ReconcileCnsNodeVmBatchAttachment) Reconcile(ctx context.Context,
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

	instance := &cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment{}
	err := r.client.Get(batchAttachCtx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CnsNodeVmBatchAttachment resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsNodeVmBatchAttachment with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - return with err.
		return reconcile.Result{}, err
	}

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout = backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()
	log.Debugf("Reconciling CnsNodeVmBatchAttachment with Request.Name: %q instance %q timeout %q seconds",
		request.Name, instance.Name, timeout)

	// Instance is considered processed if all volumes have been attached or detached
	// successfully.
	isInstanceProcessed, volumesToAttach,
		volumesToDetach, vm, err := r.isInstanceProcessed(ctx, r.client, instance)
	if err != nil {
		instance.Status.Error = err.Error()
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
		updateErr := updateInstance(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, err
	}

	// If instance is not processed, it is being deleted and VM object is also nil,
	// It means VM is deleted from the VC, VM CR is either already deleted or is being deleted.
	// This means all volumes can be considered detached. So remove finalizer from CR instance.
	if !isInstanceProcessed && instance.DeletionTimestamp != nil && vm == nil {
		// TODO: remove PVC finalizer

		removeFinalizerFromCRDInstance(batchAttachCtx, instance)
		updateErr := updateInstance(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()

		msg := fmt.Sprintf("ReconcileCnsNodeVmBatchAttachment: Successfully processed instance %s "+
			"in namespace %q.", request.Name, request.Namespace)
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeNormal, msg)
		return reconcile.Result{}, nil
	}

	// If the CnsNodeVmBatchAttachment instance is already processed and
	// not deleted by the user, remove the instance from the queue.
	if isInstanceProcessed && instance.DeletionTimestamp == nil {
		// TODO: add PVC finalizer

		log.Infof("CnsNodeVmbatchAttachment instance %q status is already processed "+
			"and is not being deleted. Removing from the queue.", instance.Name)
		// Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// Initialise volumeStatus if it is set to nil
	if instance.Status.VolumeStatus == nil {
		instance.Status.VolumeStatus = make([]cnsnodevmbatchattachmentv1alpha1.VolumeStatus, 0)
	}

	// All volumes are not processed yet and the CR is not being deleted either.
	if !isInstanceProcessed && instance.DeletionTimestamp == nil {

		// Add finalizer to CR if it does not already exist.
		if !controllerutil.ContainsFinalizer(instance, cnsoperatortypes.CNSFinalizer) {
			log.Infof("Finalizer not found on instance %s. Adding it now.", instance.Name)
			err := addCnsFinalizerOnCRDInstance(ctx, instance, r.client)
			if err != nil {
				instance.Status.Error = err.Error()
				recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
				log.Errorf("failed to add finalizer on CRD instance %s, Err: %s", instance.Name, err)
				return reconcile.Result{RequeueAfter: timeout}, err
			}
		}

		// Call reconcile when deletion timestamp is not set on the instance.
		err := r.reconcileInstanceWithoutDeletionTimestamp(batchAttachCtx, instance,
			volumesToAttach, volumesToDetach, vm)
		if err != nil {
			updateErr := updateInstance(batchAttachCtx, r.client, instance)
			if updateErr != nil {
				recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
				log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
				return reconcile.Result{RequeueAfter: timeout}, updateErr
			}
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
			return reconcile.Result{RequeueAfter: timeout}, err
		}

		instance.Status.Error = ""
		updateErr := updateInstance(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}

		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()

		msg := fmt.Sprintf("ReconcileCnsNodeVmBatchAttachment: Successfully processed instance %s "+
			"in namespace %q.", request.Name, request.Namespace)
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeNormal, msg)
		return reconcile.Result{}, nil
	}

	// If the CR itself is being deleted, then first detach all volumes in it.
	if instance.DeletionTimestamp != nil {
		log.Infof("Deletion timestamp observed on instance %s. Detaching all volumes.", instance.Name)

		// Call reconcile when deletion timestamp is set on the instance.
		err := r.reconcileInstanceWithDeletionTimestamp(ctx, instance, volumesToDetach, vm)
		if err != nil {
			updateErr := updateInstance(batchAttachCtx, r.client, instance)
			if updateErr != nil {
				recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
				log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
				return reconcile.Result{RequeueAfter: timeout}, updateErr
			}
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
			return reconcile.Result{RequeueAfter: timeout}, err
		}

		instance.Status.Error = ""
		updateErr := updateInstance(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()

		msg := fmt.Sprintf("ReconcileCnsNodeVmBatchAttachment: Successfully processed instance %s "+
			"in namespace %q.", request.Name, request.Namespace)
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeNormal, msg)
		return reconcile.Result{}, nil
	}

	reconcileLog.Infof("Reconcile for CnsNodeVmBatchAttachment request: %q completed.", request.NamespacedName)
	return reconcile.Result{}, nil
}

// reconcileInstanceWithDeletionTimestamp calls detach volume for all volumes present in volumesToDetach.
// As the instance is being deleted, we do not need to attach anything.
func (r *ReconcileCnsNodeVmBatchAttachment) reconcileInstanceWithDeletionTimestamp(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToDetach map[string]string,
	vm *cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	err := r.detachVolumes(ctx, vm, instance, volumesToDetach)
	if err != nil {
		instance.Status.Error = err.Error()
		log.Errorf("failed to detach all volumes. Err: %s", err)
		return err
	}

	// CR is being deleted and all volumes were detached successfully.
	removeFinalizerFromCRDInstance(ctx, instance)
	return nil
}

// reconcileInstanceWithoutDeletionTimestamp calls attach a well as detach calls
// for volumes present in volumesToAttach and volumesToDetach lists.
func (r *ReconcileCnsNodeVmBatchAttachment) reconcileInstanceWithoutDeletionTimestamp(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToAttach map[string]string, volumesToDetach map[string]string,
	vm *cnsvsphere.VirtualMachine) error {
	log := logger.GetLogger(ctx)

	// Call batch attach for volumes which need to be attached.
	if len(volumesToAttach) != 0 {
		err := r.attachVolumes(ctx, vm, instance, volumesToAttach)
		if err != nil {
			instance.Status.Error = err.Error()
			log.Errorf("failed to attach all volumes. Err: %+v", err)
			return err
		}
	}

	// Call detach if there are some volumes which need to be detached.
	if len(volumesToDetach) != 0 {
		err := r.detachVolumes(ctx, vm, instance, volumesToDetach)
		if err != nil {
			instance.Status.Error = err.Error()
			log.Errorf("failed to detach all volumes. Err: +v", err)
			return err
		}
		log.Infof("Successfully detached all volumes %+v", volumesToDetach)
	}

	return nil
}

// isInstanceProcessed finds the VM on vCenter to find the list of FCDs attached to it.
// It then takes a diff of those FCDs and the ones in spec to find out
// which volumes need to be attached and which ones need to be detached.
//
// isInstanceProcessed returns true if no attach or detach is required.
// It also returns list of volumes to attach, list of volumes to detach,
// VM object and error.
func (r *ReconcileCnsNodeVmBatchAttachment) isInstanceProcessed(ctx context.Context, client client.Client,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (instanceProcessed bool,
	volumesToAttach map[string]string, volumesToDetach map[string]string, vm *cnsvsphere.VirtualMachine, err error) {
	log := logger.GetLogger(ctx)

	// Get vm from vCenter.
	vm, err = cnsoperatorutil.GetVMFromVcenter(ctx, instance.Spec.NodeUUID, r.configInfo)
	if err != nil {
		if err != cnsvsphere.ErrVMNotFound {
			return false, map[string]string{}, map[string]string{}, nil, err
		}

		// If VM is deleted on vCenter, check if CnsNodeVmBatchAttachment instance also has deletion timestamp.
		// In case deletion timestamp is present, we can assume all volumes on the instance spec are already detached.
		// CSI should remove finalzier from CR and let the CR get deleted.
		// If instance is not being deleted, something is wrong. CSI should send back an error.
		if instance.DeletionTimestamp == nil {
			err := fmt.Errorf("virtual Machine with UUID %s on vCenter does not exist. "+
				"Vm is CR is deleted or is being deleted but"+
				"CnsNodeVmBatchAttachmentInstance %s is not being deleted", instance.Spec.NodeUUID, instance.Name)
			return false, map[string]string{}, map[string]string{}, nil, err
		}
		return false, map[string]string{}, map[string]string{}, nil, nil
	}

	// If VM was found on vCenter but instance has deletion timestamp,
	// add all volumes to volumesToDetach list.
	if instance.DeletionTimestamp != nil {
		volumesToDetach, err := getPvcsInSpec(instance)
		if err != nil {
			return false, map[string]string{}, volumesToDetach, vm, err
		}
		return false, map[string]string{}, volumesToDetach, vm, nil
	}

	// Query vCenter to find the list of FCDs which are attached to the VM.
	attachedFcdList, err := getListOfAttachedVolumesForVM(ctx, vm)
	if err != nil {
		return false, map[string]string{}, map[string]string{}, nil, err
	}
	log.Infof("List of attached FCDs %+v to VM %s", attachedFcdList, instance.Spec.NodeUUID)

	// Find volumes to be attached and volumes to be detached.
	volumesToAttach, volumesToDetach, err = getVolumesToAttachAndDetach(ctx, instance, client, attachedFcdList)
	if err != nil {
		return false, map[string]string{}, map[string]string{}, nil, err
	}

	// If there no volumes to be attached or detached, no action is required.
	// The instance can be considered processed.
	if len(volumesToAttach) == 0 && len(volumesToDetach) == 0 {
		return true, volumesToAttach, volumesToDetach, vm, nil
	}
	log.Infof("Volumes to be attached %+v, volumes to be detached %+v", volumesToAttach, volumesToDetach)

	return false, volumesToAttach, volumesToDetach, vm, nil
}

// detachVolumes detaches each of the volumes in volumesToDetach by calling CNS DetachVolume API.
func (r *ReconcileCnsNodeVmBatchAttachment) detachVolumes(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, volumesToDetach map[string]string) error {
	log := logger.GetLogger(ctx)

	volumesThatFailedToDetach := r.callSingleDetachVolume(ctx, vm, volumesToDetach, instance)

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

// callSingleAttachVolume call CnsDetach volume for every PVC.
// A maximum of maxParrallelAttachOrDetachCalls goroutines are created for simultaneous detach calls.
func (r *ReconcileCnsNodeVmBatchAttachment) callSingleDetachVolume(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, volumesToDetach map[string]string,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) []string {
	log := logger.GetLogger(ctx)

	// Initiallize semaphore with maxParrallelAttachOrDetachCalls.
	semaphore := make(chan struct{}, maxParrallelAttachOrDetachCalls)
	var wg sync.WaitGroup

	volumesThatFailedToDetach := make([]string, 0)

	for pvc, volumeId := range volumesToDetach {
		semaphore <- struct{}{} // acquire semaphore
		wg.Add(1)

		// Start goroutine for the current PVC.
		go func(currPvc string, currVolumeID string) {

			log.Infof("Detach call started for PVC %s in namespace %s for instance %s",
				currPvc, instance.Namespace, instance.Name)

			defer wg.Done()
			defer func() { <-semaphore }() // release semaphore

			// Call CNS DetachVolume
			faulttype, detachErr := r.volumeManager.DetachVolume(ctx, vm, currVolumeID)
			if detachErr != nil {
				// If VM was not found, can assume that the detach is successful.
				if cnsvsphere.IsManagedObjectNotFound(detachErr, vm.VirtualMachine.Reference()) {
					log.Infof("Found a managed object not found fault for vm: %+v", vm)
					// TODO: remove PVC finalizer

					// Acquire lock for the instance.
					actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
					lock, ok := actual.(*sync.Mutex)
					if !ok {
						log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
						volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
						return
					}
					lock.Lock()
					// Remove entry of this volume from the instance's status and then release the lock.
					deleteVolumeFromStatus(currPvc, instance)
					lock.Unlock()

					log.Infof("Successfully detached volume %s from VM %s", currPvc, instance.Spec.NodeUUID)
				} else {
					log.Errorf("failed to detach volume %s from VM %s. Fault: %s Err: %s",
						currPvc, instance.Spec.NodeUUID, faulttype, detachErr)

					// Acquire lock for the instance.
					actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
					lock, ok := actual.(*sync.Mutex)
					if !ok {
						log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
						volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
						return
					}
					lock.Lock()
					// Update the instance with error for this PVC and then relase the lock.
					updateInstanceWithErrorPvc(instance, currPvc, detachErr.Error())
					lock.Unlock()

					volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
				}
			} else {
				// TODO: remove PVC finalizer

				// Acquire lock for the instance.
				actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
				lock, ok := actual.(*sync.Mutex)
				if !ok {
					log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
					volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
					return
				}
				lock.Lock()
				// Remove entry of this volume from the instance's status and then release the lock.
				deleteVolumeFromStatus(currPvc, instance)
				lock.Unlock()

				log.Infof("Successfully detached volume %s from VM %s", currPvc, instance.Spec.NodeUUID)
			}

			log.Infof("Detach call ended for PVC %s in namespace %s for instance %s",
				currPvc, instance.Namespace, instance.Name)

		}(pvc, volumeId)
	}
	// Wait for all goroutines to complete
	wg.Wait()

	// Send back list of volumes which failed to attach.
	return volumesThatFailedToDetach
}

// callSingleAttachVolume call CnsAttach volume for every PVC.
// A maximum of maxParrallelAttachOrDetachCalls goroutines are created for simultaneous attach calls.
func (r *ReconcileCnsNodeVmBatchAttachment) callSingleAttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToAttach map[string]string) []string {

	log := logger.GetLogger(ctx)

	// Initiallize semaphore with maxParrallelAttachOrDetachCalls.
	semaphore := make(chan struct{}, maxParrallelAttachOrDetachCalls)
	var wg sync.WaitGroup

	volumesThatFailedToAttach := make([]string, 0)

	for pvc, volumeName := range volumesToAttach {
		semaphore <- struct{}{} // acquire semaphore
		wg.Add(1)

		// Start goroutine for the current PVC.
		go func(currPvc string, volumeName string) {

			log.Infof("Attach call started for PVC %s in namespace %s for instance %s",
				currPvc, instance.Namespace, instance.Name)

			defer wg.Done()
			defer func() { <-semaphore }() // release semaphore

			// TODO: Add PVC finalizer
			namespacedPvcName := getNamespacedPvcName(instance.Namespace, pvc)
			attachVolumeId, ok := commonco.ContainerOrchestratorUtility.GetVolumeIDFromPVCName(namespacedPvcName)
			if !ok {
				err := fmt.Errorf("failed to find volumeID for PVC %s", pvc)
				updateInstanceWithErrorVolumeName(instance, volumeName, pvc, err.Error())
				volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
			}

			log.Infof("vSphere CSI driver is attaching volume: %q to vm: %+v for "+
				"CnsNodeVmBatchAttachment request with name: %q on namespace: %q",
				attachVolumeId, vm, instance.Name, instance.Namespace)

			// Call CNS AttachVolume
			diskUUID, faultType, attachErr := r.volumeManager.AttachVolume(ctx, vm, attachVolumeId, false)
			if attachErr != nil {
				log.Errorf("failed to attach disk: %q to vm: %+v for CnsNodeVmBatchAttachment "+
					"request with name: %q on namespace: %q. Fault %s Err: %+v",
					pvc, vm, instance.Name, instance.Namespace, faultType, attachErr)

				// Acquire lock for the instance.
				actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
				lock, ok := actual.(*sync.Mutex)
				if !ok {
					log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
					volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
					return
				}
				lock.Lock()
				// Update instance with error on this volume and then release the lock.
				updateInstanceWithErrorVolumeName(instance, volumeName, currPvc, attachErr.Error())
				lock.Unlock()

				volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
			} else {

				// Acquire lock for the instance.
				actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
				lock, ok := actual.(*sync.Mutex)
				if !ok {
					log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
					volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
					return
				}

				lock.Lock()
				// Update instance with successful attach and then release the lock.
				found := false
				for i, volumeStatus := range instance.Status.VolumeStatus {
					if volumeStatus.Name == volumeName {
						found = true
						instance.Status.VolumeStatus[i].PersistentVolumeClaim.ClaimName = pvc
						instance.Status.VolumeStatus[i].PersistentVolumeClaim.Attached = true
						instance.Status.VolumeStatus[i].PersistentVolumeClaim.Error = ""
						instance.Status.VolumeStatus[i].PersistentVolumeClaim.CnsVolumeID = attachVolumeId
						instance.Status.VolumeStatus[i].PersistentVolumeClaim.Diskuuid = diskUUID
						break
					}
				}

				if !found {
					newVolStatus := cnsnodevmbatchattachmentv1alpha1.VolumeStatus{
						Name: volumeName,
						PersistentVolumeClaim: cnsnodevmbatchattachmentv1alpha1.PersistentVolumeClaimStatus{
							ClaimName:   pvc,
							Attached:    true,
							Error:       "",
							CnsVolumeID: attachVolumeId,
							Diskuuid:    diskUUID,
						},
					}
					instance.Status.VolumeStatus = append(instance.Status.VolumeStatus, newVolStatus)
				}
				lock.Unlock()
			}
			log.Infof("Attach call ended for PVC %s in namespace %s for instance %s",
				currPvc, instance.Namespace, instance.Name)

		}(pvc, volumeName)
	}
	// Wait for all goroutines to complete
	wg.Wait()

	// Send back list of volumes which failed to attach.
	return volumesThatFailedToAttach
}

// attachVolumes calls CNS batch attach for PVCs for which controllerNumber and controllerKey are provided by the user.
// It calls individual attach calls for PVCs without controllerNumber and controllerKey.
func (r *ReconcileCnsNodeVmBatchAttachment) attachVolumes(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToAttach map[string]string) error {
	log := logger.GetLogger(ctx)

	// TODO: Call batch attach over here once CNS API is ready.
	volumesThatFailedToAttached := r.callSingleAttachVolume(ctx, vm, instance, volumesToAttach)

	var overallErr error
	if len(volumesThatFailedToAttached) != 0 {
		msg := "failed to attach volumes: "
		failedVolumes := strings.Join(volumesThatFailedToAttached, ",")
		msg += failedVolumes
		overallErr = errors.New(msg)
		log.Error(overallErr)
	}
	return overallErr
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsNodeVmBatchAttachment,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	namespacedName := types.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = backOffDuration[namespacedName] * 2
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
