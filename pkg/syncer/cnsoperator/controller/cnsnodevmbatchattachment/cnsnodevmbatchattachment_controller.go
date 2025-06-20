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
	defaultMaxWorkerThreadsForNodeVmBatchAttach = 10
	maxParrallelAttachOrDetachCalls             = 10
)

func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsNodeVmBatchAttachment Controller as its a non-WCP CSI deployment")
		return nil
	}

	if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SharedDiskFss) {
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
	return &ReconcileCnsNodeVmBatchAttachment{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager,
		vmOperatorClient: vmOperatorClient, nodeManager: cnsnode.GetManager(ctx),
		recorder: recorder, instanceLock: &sync.Map{}}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsNodeVmBatchAttachment(ctx)
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
	instanceLock     *sync.Map
}

// getMaxWorkerThreadsToReconcileCnsNodeVmBatchAttachment returns the maximum
// number of worker threads which can be run to reconcile CnsNodeVmBatchAttachment
// instances. If environment variable WORKER_THREADS_NODEVM_BATCH_ATTACH is set and
// valid, return the value read from environment variable otherwise, use the
// default value.
func getMaxWorkerThreadsToReconcileCnsNodeVmBatchAttachment(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForNodeVmBatchAttach
	if v := os.Getenv("WORKER_THREADS_NODEVM_BATCH_ATTACH"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_NODEVM_BATCH_ATTACH %s is less than 1, will use the default value %d",
					v, defaultMaxWorkerThreadsForNodeVmBatchAttach)
			} else if value > defaultMaxWorkerThreadsForNodeVmBatchAttach {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_NODEVM_BATCH_ATTACH %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForNodeVmBatchAttach, defaultMaxWorkerThreadsForNodeVmBatchAttach)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsNodeVmBatchAttachment "+
					"instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable "+
				"WORKER_THREADS_NODEVM_BATCH_ATTACH %s is invalid, will use the default value %d",
				v, defaultMaxWorkerThreadsForNodeVmBatchAttach)
		}
	} else {
		log.Debugf("WORKER_THREADS_NODEVM_BATCH_ATTACH is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForNodeVmBatchAttach)
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
	// Start a goroutine to listen for context cancellation
	go func() {
		<-ctx.Done()
		reconcileLog.Infof("context canceled for reconcile for CnsNodeVmBatchAttachment request: %q, error: %v",
			request.NamespacedName, ctx.Err())
	}()

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

	// Get a map of volumeID to PVC map in the current namespace in K8s cluster.
	volumeIdToPvc, pvcToVolumeId, err := cnsoperatorutil.GetVolumeIDPvcMappingInNamespace(batchAttachCtx,
		instance.Namespace)
	if err != nil {
		instance.Status.Error = error.Error(err)
		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, err
	}

	// Instance is considered processed if all volumes have been attached or detached
	// successfully.
	isInstanceProcessed, volumesToAttach,
		volumesToDetach, nodeVM, err := r.isInstanceProcessed(ctx, volumeIdToPvc, pvcToVolumeId, instance)
	if err != nil {
		instance.Status.Error = err.Error()
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, err
	}

	// If instance is not processed, it is being deleted and nodeVM is also nil,
	// It means nodeVM is deleted from the VC, VM Cr is either already deleted or is being deleted.
	// This means all volumes can be considered detached. So remove finalizer from CR instance.
	if !isInstanceProcessed && instance.DeletionTimestamp != nil && nodeVM == nil {
		// TODO: remove PVC finalizer

		removeFinalizerFromCRDInstance(batchAttachCtx, instance)
		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
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
		if !cnsFinalizerOnCRDInstanceExists(instance) {
			log.Infof("Finalizer not found on instance %s. Adding it now.", instance.Name)
			err := addCnsFinalizerOnCRDInstance(ctx, instance, r.client)
			if err != nil {
				instance.Status.Error = err.Error()
				recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
				log.Errorf("failed to add finalizer on CRD instance %s, Err: %s", instance.Name, err)
				return reconcile.Result{RequeueAfter: timeout}, err
			}
		}

		err := r.reconcileInstanceWithoutDeletionTimestamp(batchAttachCtx, instance,
			volumesToAttach, volumesToDetach, nodeVM, pvcToVolumeId)
		if err != nil {
			updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
			if updateErr != nil {
				recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
				log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
				return reconcile.Result{RequeueAfter: timeout}, updateErr
			}
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
			return reconcile.Result{RequeueAfter: timeout}, err
		}

		instance.Status.Error = ""
		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
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

		err := r.reconcileInstanceWithDeletionTimestamp(ctx, instance, volumesToDetach,
			nodeVM, pvcToVolumeId)
		if err != nil {
			updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
			if updateErr != nil {
				recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, updateErr.Error())
				log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
				return reconcile.Result{RequeueAfter: timeout}, updateErr
			}
			recordEvent(batchAttachCtx, r, instance, v1.EventTypeWarning, err.Error())
			return reconcile.Result{RequeueAfter: timeout}, err
		}

		instance.Status.Error = ""
		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
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

func (r *ReconcileCnsNodeVmBatchAttachment) reconcileInstanceWithDeletionTimestamp(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToDetach map[string]string,
	nodeVM *cnsvsphere.VirtualMachine,
	pvcToVolumeId map[string]string) error {
	log := logger.GetLogger(ctx)

	err := r.detachVolumes(ctx, nodeVM, instance, volumesToDetach)
	if err != nil {
		instance.Status.Error = err.Error()
		log.Errorf("failed to detach all volumes. Err: +v", err)
		return err
	}

	// CR is being deleted and all volumes were detached successfully
	removeFinalizerFromCRDInstance(ctx, instance)
	return nil
}

// reconcileInstanceWithoutDeletionTimestamp calls batch attach a well as individual detach calls
// for volumes present in volumesToAttach and volumesToDetach lists.
func (r *ReconcileCnsNodeVmBatchAttachment) reconcileInstanceWithoutDeletionTimestamp(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToAttach map[string]string, volumesToDetach map[string]string,
	nodeVM *cnsvsphere.VirtualMachine, pvcToVolumeId map[string]string) error {
	log := logger.GetLogger(ctx)

	// Call batch attach for volumes which need to be attached.
	if len(volumesToAttach) != 0 {
		err := r.attachVolumes(ctx, nodeVM, pvcToVolumeId, instance, volumesToAttach)
		if err != nil {
			instance.Status.Error = err.Error()
			log.Errorf("failed to attach all volumes. Err: %+v", err)
			return err
		}
	}

	// Call detach if there are some volumes which need to be detached.
	if len(volumesToDetach) != 0 {
		err := r.detachVolumes(ctx, nodeVM, instance, volumesToDetach)
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
// If VM retrieval from the VC fails,
// then fail this operation as it is invalid.
//
// isInstanceProcessed returns true if no attach or detach is required.
// It also returns list of volumes to attach, list of volumes to detach,
// VM object and error.
func (r *ReconcileCnsNodeVmBatchAttachment) isInstanceProcessed(ctx context.Context,
	volumeIdToPvc map[string]string, pvcToVolumeId map[string]string,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (bool,
	map[string]string, map[string]string, *cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	// Get nodeVM from vCenter.
	nodeVM, err := getNodeVm(ctx, instance, r.configInfo)
	if err != nil {
		if err != cnsvsphere.ErrVMNotFound {
			return false, map[string]string{}, map[string]string{}, nil, err
		}

		// Check if VM CR is also being deleted or is already deleted.
		// In case the above condition is true, we can assume all volumes
		// on the instance spec are already detached.
		// This means the instance must have deletion timestamp.
		// CSI should remove finalzier from CR and let the CR get deleted.
		err := validateVmCrWhenNodeVmIsDeleted(ctx, r.client, instance.Spec.NodeUUID,
			instance.Namespace, instance.Name)
		if err != nil {
			return false, map[string]string{}, map[string]string{}, nil, err
		}
		if instance.DeletionTimestamp == nil {
			err := fmt.Errorf("NodeVM with UUID %s on vCenter does not exist. "+
				"Vm is CR is deleted or is being deleted but"+
				"CnsNodeVmBatchAttachmentInstance %s is not being deleted", instance.Spec.NodeUUID, instance.Name)
			return false, map[string]string{}, map[string]string{}, nil, err
		}
		return false, map[string]string{}, map[string]string{}, nil, nil
	}

	if instance.DeletionTimestamp != nil {
		volumesToDetach, err := getVolumeInInstanceSpec(instance, pvcToVolumeId)
		if err != nil {
			return false, map[string]string{}, volumesToDetach, nodeVM, err
		}
		return false, map[string]string{}, volumesToDetach, nodeVM, nil
	}

	// Query vCenter to find the list of FCDs which are attached to the VM.
	attachedFcdList, err := volumes.GetListOfAttachedVolumesForVM(ctx, volumeIdToPvc, nodeVM)
	if err != nil {
		return false, map[string]string{}, map[string]string{}, nil, err
	}
	log.Infof("List of attached FCDs %+v to VM %s", attachedFcdList, instance.Spec.NodeUUID)

	// Find volumes to be attached and volumes to be detached.
	volumesToAttach, volumesToDetach, err := volumesToAttachAndDetach(ctx, instance, pvcToVolumeId,
		volumeIdToPvc, attachedFcdList)
	if err != nil {
		return false, map[string]string{}, map[string]string{}, nil, err
	}

	// If there no volumes to be attached or detached, no action is required.
	// The instance can be considered processed.
	if len(volumesToAttach) == 0 && len(volumesToDetach) == 0 {
		return true, volumesToAttach, volumesToDetach, nodeVM, nil
	}
	log.Infof("Volumes to be attached %+v, volumes to be detached %+v", volumesToAttach, volumesToDetach)

	return false, volumesToAttach, volumesToDetach, nodeVM, nil
}

func (r *ReconcileCnsNodeVmBatchAttachment) detachVolumes(ctx context.Context,
	nodeVM *cnsvsphere.VirtualMachine,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, volumesToDetach map[string]string) error {
	log := logger.GetLogger(ctx)

	// Start detach for every volume
	volumesThatFailedToDetach := r.callDetachForAllVolumes(ctx, nodeVM, volumesToDetach, instance)

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

// callDetachForAllVolumes calls CNS detach API for every volume.
func (r *ReconcileCnsNodeVmBatchAttachment) callDetachForAllVolumes(ctx context.Context,
	nodeVM *cnsvsphere.VirtualMachine, volumesToDetach map[string]string,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) []string {
	log := logger.GetLogger(ctx)

	semaphore := make(chan struct{}, maxParrallelAttachOrDetachCalls)
	var wg sync.WaitGroup

	volumesThatFailedToDetach := make([]string, 0)

	for pvc, volumeId := range volumesToDetach {
		semaphore <- struct{}{} // acquire semaphore
		wg.Add(1)

		go func(currPvc string, currVolumeID string) {

			log.Infof("Detach call started for PVC %s in namespace %s for instance %s",
				currPvc, instance.Namespace, instance.Name)

			defer wg.Done()
			defer func() { <-semaphore }() // release semaphore

			_, detachErr := r.volumeManager.DetachVolume(ctx, nodeVM, currVolumeID)
			if detachErr != nil {
				if cnsvsphere.IsManagedObjectNotFound(detachErr, nodeVM.VirtualMachine.Reference()) {
					log.Infof("Found a managed object not found fault for vm: %+v", nodeVM)
					// TODO: remove PVC finalizer

					actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
					lock, ok := actual.(*sync.Mutex)
					if !ok {
						log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
						volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
						return
					}
					lock.Lock()
					deleteVolumeFromStatus(currPvc, instance)
					lock.Unlock()

					log.Infof("Successfully detached volume %s from VM %s", currPvc, instance.Spec.NodeUUID)
				} else {
					log.Errorf("failed to detach volume %s from VM %s. Err: %s",
						currPvc, instance.Spec.NodeUUID, detachErr)

					actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
					lock, ok := actual.(*sync.Mutex)
					if !ok {
						log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
						volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
						return
					}
					lock.Lock()
					updateInstanceWithErrorPvc(instance, currPvc, detachErr.Error())
					lock.Unlock()

					volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
				}
			} else {
				// TODO: remove PVC finalizer
				actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
				lock, ok := actual.(*sync.Mutex)
				if !ok {
					log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
					volumesThatFailedToDetach = append(volumesThatFailedToDetach, currPvc)
					return
				}
				lock.Lock()
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

	return volumesThatFailedToDetach
}

func (r *ReconcileCnsNodeVmBatchAttachment) callAttachVolume(ctx context.Context, nodeVM *cnsvsphere.VirtualMachine,
	pvcToVolumeId map[string]string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToAttach map[string]string) []string {

	log := logger.GetLogger(ctx)

	semaphore := make(chan struct{}, maxParrallelAttachOrDetachCalls)
	var wg sync.WaitGroup

	volumesThatFailedToAttach := make([]string, 0)

	for pvc, volumeName := range volumesToAttach {
		semaphore <- struct{}{} // acquire semaphore
		wg.Add(1)

		go func(currPvc string, volumeName string) {

			log.Infof("Attach call started for PVC %s in namespace %s for instance %s",
				currPvc, instance.Namespace, instance.Name)

			defer wg.Done()
			defer func() { <-semaphore }() // release semaphore

			// TODO: Add PVC finalizer
			attachVolumeId, ok := pvcToVolumeId[pvc]
			// If even 1 PVC is not found in pvcToVolumeId map, then error out.
			if !ok {
				err := fmt.Errorf("failed to find volumeID for PVC %s", pvc)
				updateInstanceWithErrorVolumeName(instance, volumeName, pvc, err.Error())
				volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
			}

			log.Infof("vSphere CSI driver is attaching volume: %q to nodevm: %+v for "+
				"CnsNodeVmBatchAttachment request with name: %q on namespace: %q",
				attachVolumeId, nodeVM, instance.Name, instance.Namespace)

			diskUUID, _, attachErr := r.volumeManager.AttachVolume(ctx, nodeVM, attachVolumeId, false)
			if attachErr != nil {
				log.Errorf("failed to attach disk: %q to nodevm: %+v for CnsNodeVmBatchAttachment "+
					"request with name: %q on namespace: %q. Err: %+v",
					pvc, nodeVM, instance.Name, instance.Namespace, attachErr)

				actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
				lock, ok := actual.(*sync.Mutex)
				if !ok {
					log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
					volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
					return
				}
				lock.Lock()
				updateInstanceWithErrorVolumeName(instance, volumeName, currPvc, attachErr.Error())
				lock.Unlock()

				volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
			} else {

				actual, _ := r.instanceLock.LoadOrStore(instance.Name, &sync.Mutex{})
				lock, ok := actual.(*sync.Mutex)
				if !ok {
					log.Errorf("failed to cast lock for instance instance: %s", instance.Name)
					volumesThatFailedToAttach = append(volumesThatFailedToAttach, currPvc)
					return
				}

				lock.Lock()
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

	return volumesThatFailedToAttach
}

func (r *ReconcileCnsNodeVmBatchAttachment) attachVolumes(ctx context.Context, nodeVM *cnsvsphere.VirtualMachine,
	pvcToVolumeId map[string]string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volumesToAttach map[string]string) error {
	log := logger.GetLogger(ctx)

	// TODO: Call single attach only for RWO volumes.
	volumesThatFailedToAttached := r.callAttachVolume(ctx, nodeVM, pvcToVolumeId, instance, volumesToAttach)

	var overallErr error
	if len(volumesThatFailedToAttached) != 0 {
		msg := "failed to attach volumes: "
		failedVolumes := strings.Join(volumesThatFailedToAttached, ",")
		msg += failedVolumes
		overallErr = errors.New(msg)
		log.Error(overallErr)
	}

	// TODO: Call batch attach over here

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
