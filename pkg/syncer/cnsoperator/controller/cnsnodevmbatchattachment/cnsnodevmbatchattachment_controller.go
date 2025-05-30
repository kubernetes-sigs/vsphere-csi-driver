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
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsnodevmbatchattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	cnsnode "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

// backOffDuration is a map of cnsnodevmbatchattachment name's to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

const (
	defaultMaxWorkerThreadsForNodeVMAttach = 10
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
	return &ReconcileCnsNodeVmBatchAttachment{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager,
		vmOperatorClient: vmOperatorClient, nodeManager: cnsnode.GetManager(ctx),
		recorder: recorder}
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

	backOffDuration = make(map[string]time.Duration)

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
}

// getMaxWorkerThreadsToReconcileCnsNodeVmBatchAttachment returns the maximum
// number of worker threads which can be run to reconcile CnsNodeVmBatchAttachment
// instances. If environment variable WORKER_THREADS_NODEVM_ATTACH is set and
// valid, return the value read from environment variable otherwise, use the
// default value.
func getMaxWorkerThreadsToReconcileCnsNodeVmBatchAttachment(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForNodeVMAttach
	if v := os.Getenv("WORKER_THREADS_NODEVM_ATTACH"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_NODEVM_ATTACH %s is less than 1, will use the default value %d",
					v, defaultMaxWorkerThreadsForNodeVMAttach)
			} else if value > defaultMaxWorkerThreadsForNodeVMAttach {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_NODEVM_ATTACH %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForNodeVMAttach, defaultMaxWorkerThreadsForNodeVMAttach)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsNodeVmBatchAttachment "+
					"instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable "+
				"WORKER_THREADS_NODEVM_ATTACH %s is invalid, will use the default value %d",
				v, defaultMaxWorkerThreadsForNodeVMAttach)
		}
	} else {
		log.Debugf("WORKER_THREADS_NODEVM_ATTACH is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForNodeVMAttach)
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
	batchAttachCtx, cancel := context.WithTimeout(context.Background(), volumes.VolumeOperationTimeoutInSeconds*time.Second)
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
	if _, exists := backOffDuration[instance.Namespace+"/"+instance.Name]; !exists {
		backOffDuration[instance.Namespace+"/"+instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Namespace+"/"+instance.Name]
	backOffDurationMapMutex.Unlock()
	log.Debugf("Reconciling CnsNodeVmBatchAttachment with Request.Name: %q instance %q timeout %q seconds",
		request.Name, instance.Name, timeout)

	// Instance is considered processed if all volumes have been attached or detached
	// successfully.
	isInstanceProcessed := isInstanceProcessed(instance)

	// If the CnsNodeVmBatchAttachment instance is already processed and
	// not deleted by the user, remove the instance from the queue.
	if isInstanceProcessed && instance.DeletionTimestamp == nil {
		// TODO: add PVC finalizer

		log.Infof("CnsNodeVmbatchAttachment instance %q status is already procesed and is not being deleted. Removing from the queue.", instance.Name)
		// Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Namespace+"/"+instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// All volumes are not processed yet and the CR is not being deleted either.
	if !isInstanceProcessed && instance.DeletionTimestamp == nil {

		if !cnsFinalizerOnCrExists(instance) {
			log.Infof("Finalizer not found on instance %s. Adding it now.", instance.Name)
			addCnsFinalizerOnCr(batchAttachCtx, r.client, instance)
			if err != nil {
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			// Read the CnsNodeVmBatchAttachment instance again because the instance
			// is already modified.
			err = r.client.Get(batchAttachCtx, request.NamespacedName, instance)
			if err != nil {
				log.Errorf("failed to get updated CnsNodeVmBatchAttachment instance %s in namespace %s. Err: %+v",
					instance.Name, instance.Namespace, err)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}

		// Get the list of volumes which need to be attached and the ones which need to be detached.
		volumesToAttach, volumesToDetach := getVolumesToAttachAndDetach(batchAttachCtx, instance)

		// Call batch attach for volumes which need to be attached.
		if len(volumesToAttach) != 0 {
			err := r.batchAttach(batchAttachCtx, instance, volumesToAttach)
			if err != nil {
				log.Errorf("failed to detach all volumes. Err: +v", err)
				updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
				if updateErr != nil {
					log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
					return reconcile.Result{RequeueAfter: timeout}, updateErr
				}
				log.Errorf("failed to batch attach PVCs. Err: %+v", err)
				return reconcile.Result{RequeueAfter: timeout}, err
			}
		}

		// If there are some volumes which need to be detached
		if len(volumesToDetach) != 0 {
			err := r.detachVolumes(batchAttachCtx, instance, volumesToDetach)
			if err != nil {
				log.Errorf("failed to detach all volumes. Err: +v", err)
				updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
				if updateErr != nil {
					log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
					return reconcile.Result{RequeueAfter: timeout}, updateErr
				}
				return reconcile.Result{RequeueAfter: timeout}, err
			}
			log.Infof("Successfully detached all volumes %+v", volumesToDetach)
		}

		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Namespace+"/"+instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// If the CR itself is being deleted, then first detach all volumes in it.
	if instance.DeletionTimestamp != nil {
		log.Infof("Deletion timestamp observed on instance %s. Detaching all volumes.", instance.Name)
		// Detach all volumes in spec
		volumesToDetach := make([]string, 0)
		for _, volume := range instance.Spec.Volumes {
			volumesToDetach = append(volumesToDetach, volume.PvcName)
		}

		err = r.detachVolumes(batchAttachCtx, instance, volumesToDetach)
		if err != nil {
			log.Errorf("failed to detach all volumes. Err: +v", err)
			updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
			if updateErr != nil {
				log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
				return reconcile.Result{RequeueAfter: timeout}, updateErr
			}
			log.Errorf("failed to detach all volumes. Err: %+v", err)
			return reconcile.Result{RequeueAfter: timeout}, err
		}

		// CR is being deleted and all volumes were detached successfully
		removeFinalizerFromCRDInstance(batchAttachCtx, instance)

		updateErr := updateCnsNodeVmBatchAttachment(batchAttachCtx, r.client, instance)
		if updateErr != nil {
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return reconcile.Result{RequeueAfter: timeout}, updateErr
		}
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Namespace+"/"+instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	reconcileLog.Infof("Reconcile for CnsNodeVmBatchAttachment request: %q completed.", request.NamespacedName)
	return reconcile.Result{}, nil
}

func addCnsFinalizerOnCr(ctx context.Context, client client.Client, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)
	// Update finalizer and attachmentMetadata together in CnsNodeVMAttachment.
	// Add finalizer.
	instance.Finalizers = append(instance.Finalizers, cnsoperatortypes.CNSFinalizer)

	err := updateCnsNodeVmBatchAttachment(ctx, client, instance)
	if err != nil {
		log.Errorf("failed to update CnsNodeVmBatchAttachment %s with CNS finalizer. Err: +%v", instance.Name, err)
		return err
	}
	return nil
}

func cnsFinalizerOnCrExists(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) bool {
	// Check if finalizer already exists.
	for _, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			return true
		}
	}

	return false
}

func validateCnsVolumeIdBeforeDetach(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, volumeToDetach string) (string, string, error) {
	log := logger.GetLogger(ctx)

	// Verify cnsVolumeId is present
	volumeStatus, ok := instance.Status.VolumeStatus[volumeToDetach]
	if !ok {
		return "", csifault.CSIInternalFault, fmt.Errorf("failed to find status for volume %s", volumeToDetach)
	}
	if volumeStatus.CnsVolumeID == "" {
		log.Errorf("CnsNodeVmBatchAttachment does not have CNS volume ID. Volume Status: %+v",
			volumeStatus)
		return "", csifault.CSIInternalFault, fmt.Errorf("CnsNodeVmBatchAttachment %s does not have CNS volume ID", instance.Name)
	}
	return volumeStatus.CnsVolumeID, "", nil
}

// isInstanceProcessed returns false if number of volumes in spec and status do not match or
// if there are volumes whose status is not ATTACH_COMPLETED
func isInstanceProcessed(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) bool {
	if len(instance.Spec.Volumes) != len(instance.Status.VolumeStatus) {
		return false
	}

	for _, volume := range instance.Status.VolumeStatus {
		if volume.AttachState != cnsnodevmbatchattachmentv1alpha1.AttachCompleted {
			return false
		}
	}

	return true
}

// validateVmBeforeDetach finds the VM on the instance and validates if it exists on the VC and K8s.
func (r *ReconcileCnsNodeVmBatchAttachment) validateVmBeforeDetach(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (*cnsvsphere.VirtualMachine, bool, error) {
	log := logger.GetLogger(ctx)

	nodeUUID := instance.Spec.NodeUUID
	detachCompleted := false
	nodeVM, err := r.getNodeVm(ctx, instance)
	if err != nil {
		if err != cnsvsphere.ErrVMNotFound {
			log.Errorf("failed to find node VM with UUID %s", nodeUUID)
			return nodeVM, false, err
		}

		// Now that VM on VC is not found, check VirtualMachine CRD instance exists.
		// This check is needed in scenarios where VC inventory is stale due
		// to upgrade or back-up and restore.
		vmInstance, err := cnsoperatorutil.IsVmCrPresent(ctx, r.vmOperatorClient, nodeUUID,
			instance.Namespace)
		if err != nil {
			log.Errorf("failed to find VM CR for node with UUID", nodeUUID)
			return nodeVM, false, err
		}
		if vmInstance == nil {
			// This is the case where VirtualMachine is not present on the VC and VM CR
			// is also not found in the API server. The detach will be marked as
			// successful in CnsNodeVmBatchAttachment.
			log.Infof("VM CR is not present with UUID: %s in namespace: %s. "+
				"Removing finalizer on CnsNodeVmBatchAttachment: %s instance.",
				nodeUUID, instance.Namespace, instance.Name)
			detachCompleted = true
		} else {
			if vmInstance.DeletionTimestamp != nil {
				// This is the case where VirtualMachine is not present on the VC and VM CR
				// has the deletionTimestamp set. The CnsNodeVmBatchAttachment
				// can be marked as a success since the VM CR has deletionTimestamp set
				log.Infof("VM on VC not found but VM CR with UUID: %s "+
					"is still present in namespace: %s and is being deleted. "+
					"Hence returning success.", nodeUUID, instance.Namespace)
				detachCompleted = true
			} else {
				// This is a case where VirtualMachine is not present on the VC and VM CR
				// does not have the deletionTimestamp set.
				// This is an error and will need to be retried.
				return nodeVM, false, fmt.Errorf("VM with nodeUUID %s not present on VM but is present in K8s cluster. unexpected failure", nodeUUID)
			}
		}

	}

	return nodeVM, detachCompleted, nil
}

// removePvcFinalizer removed finalizer from the CNS finalizer
func (r *ReconcileCnsNodeVmBatchAttachment) removePvcFinalizer(ctx context.Context, volumes []string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)

	for _, volume := range volumes {
		pvc := &v1.PersistentVolumeClaim{}
		pvcDeleted := false
		err := r.client.Get(ctx, k8stypes.NamespacedName{Name: volume, Namespace: instance.Namespace}, pvc)
		if err != nil {
			if apierrors.IsNotFound(err) {
				pvcDeleted = true
			} else {
				return fmt.Errorf("failed to get PVC with volumename: %q on namespace: %q. Err: %+v",
					volume, instance.Namespace, err)
			}
			return err
		}

		// TODO: How to find out if volume is attached to any other VMs?
		// Cannot remove finalizer otherwise!!

		if !pvcDeleted {
			cnsoperatorutil.RemoveFinalizerFromPVC(ctx, r.client, pvc)
			if err != nil {
				fmt.Errorf("failed to remove %q finalizer on the PVC with volumename: %q on namespace: %q. Err: %+v",
					cnsoperatortypes.CNSPvcFinalizer, volume, instance.Namespace, err)
				return fmt.Errorf("failed to remove %q finalizer on the PVC with volumename: %q on namespace: %q. Err: %+v",
					cnsoperatortypes.CNSPvcFinalizer, volume, instance.Namespace, err)

			}
		}
		removeFinalizerFromCRDInstance(ctx, instance)
		err = updateCnsNodeVmBatchAttachment(ctx, r.client, instance)
		if err != nil {
			log.Errorf("updateCnsNodeVMAttachment failed. err: %v", err)
			return err
		}
	}
	return nil
}

// addPvcFinalizer adds CNS finalizer to PVC if it is not already added.
func (r *ReconcileCnsNodeVmBatchAttachment) addPvcFinalizer(ctx context.Context, volumes []string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)

	for _, volume := range volumes {
		pvc := &v1.PersistentVolumeClaim{}
		err := r.client.Get(ctx, k8stypes.NamespacedName{Name: volume, Namespace: instance.Namespace}, pvc)
		if err != nil {
			log.Errorf("failed to get PVC with volumename: %q on namespace: %q. Err: %+v",
				volume, instance.Namespace, err)
			updateInstanceWithError(instance, volume, err.Error())
			return err
		}
		cnsPvcFinalizerExists := false
		// Check if cnsPvcFinalizerExists already exists.
		for _, finalizer := range pvc.Finalizers {
			if finalizer == cnsoperatortypes.CNSPvcFinalizer {
				cnsPvcFinalizerExists = true
				log.Infof("Finalizer: %q already exists in the PVC with name: %q on namespace: %q.",
					cnsoperatortypes.CNSPvcFinalizer, volume, instance.Namespace)
				break
			}
		}
		if !cnsPvcFinalizerExists {
			_, err := cnsoperatorutil.AddFinalizerToPVC(ctx, r.client, pvc)
			if err != nil {
				log.Errorf("failed to add %q finalizer on the PVC with volumename: %q on namespace: %q. Err: %+v",
					cnsoperatortypes.CNSPvcFinalizer, volume, instance.Namespace, err)
				updateInstanceWithError(instance, volume, err.Error())
				return err
			}
		}
	}
	return nil
}

func updateCnsNodeVmBatchAttachment(ctx context.Context, client client.Client,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		if apierrors.IsConflict(err) {
			log.Infof("Observed conflict while updating CnsNodeVmBatchAttachment instance %q in namespace %q."+
				"Reapplying changes to the latest instance.", instance.Name, instance.Namespace)

			// Fetch the latest instance version from the API server and apply changes on top of it.
			latestInstance := &cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment{}
			err = client.Get(ctx, k8stypes.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, latestInstance)
			if err != nil {
				log.Errorf("Error reading the CnsNodeVmBatchAttachment with name: %q on namespace: %q. Err: %+v",
					instance.Name, instance.Namespace, err)
				// Error reading the object - return error
				return err
			}

			// The callers of updateCnsNodeVMBatchAttachment are either updating the instance finalizers or
			// one of the fields in instance status.
			// Hence we copy only finalizers and Status from the instance passed for update
			// on the latest instance from API server.
			latestInstance.Finalizers = instance.Finalizers
			latestInstance.Status = *instance.Status.DeepCopy()

			err := client.Update(ctx, latestInstance)
			if err != nil {
				log.Errorf("failed to update CnsNodeVmBatchAttachment instance: %q on namespace: %q. Error: %+v",
					instance.Name, instance.Namespace, err)
				return err
			}
			return nil
		} else {
			log.Errorf("failed to update CnsNodeVmBatchAttachment instance: %q on namespace: %q. Error: %+v",
				instance.Name, instance.Namespace, err)
		}
	}
	return err
}

// getDatacenterObject returns the datacenter object for the vCenter.
func (r *ReconcileCnsNodeVmBatchAttachment) getDatacenterObject(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (*cnsvsphere.Datacenter, error) {
	log := logger.GetLogger(ctx)

	vcdcMap, err := cnsoperatorutil.GetVCDatacentersFromConfig(r.configInfo.Cfg)
	if err != nil {
		log.Errorf("failed to find datacenter moref from config for CnsNodeVmBatchAttachment "+
			"request with name: %q on namespace: %q. Err: %+v", instance.Name, instance.Namespace, err)
		return nil, err
	}

	var host, dcMoref string
	for key, value := range vcdcMap {
		host = key
		dcMoref = value[0]
	}

	// Get datacenter object
	var dc *cnsvsphere.Datacenter
	vcenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, r.configInfo, false)
	if err != nil {
		log.Errorf("failed to get virtual center instance with error: %v", err)
		return nil, err
	}
	err = vcenter.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to VC with error: %v", err)
		return nil, err
	}
	dc = &cnsvsphere.Datacenter{
		Datacenter: object.NewDatacenter(vcenter.Client.Client,
			vimtypes.ManagedObjectReference{
				Type:  "Datacenter",
				Value: dcMoref,
			}),
		VirtualCenterHost: host,
	}
	return dc, nil
}

// getNodeVm returns the nodeVM for the given nodeUUID.
func (r *ReconcileCnsNodeVmBatchAttachment) getNodeVm(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) (*cnsvsphere.VirtualMachine, error) {
	log := logger.GetLogger(ctx)

	nodeUUID := instance.Spec.NodeUUID

	dc, err := r.getDatacenterObject(ctx, instance)
	if err != nil {
		log.Errorf("failed to get datacenter for node: %s. Err: %+q", nodeUUID, err)
		return nil, err
	}

	nodeVM, err := dc.GetVirtualMachineByUUID(ctx, nodeUUID, false)
	if err != nil {
		log.Errorf("failed to find the VM with UUID: %q for CnsNodeVmbatchAttachment "+
			"request with name: %q on namespace: %q. Err: %+v",
			nodeUUID, instance.Name, instance.Namespace, err)
		return nil, err
	}

	return nodeVM, nil
}

func (r *ReconcileCnsNodeVmBatchAttachment) detachVolumes(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, volumesToDetach []string) error {
	log := logger.GetLogger(ctx)

	// Check if nodeVM exists or not.
	nodeVM, detachCompleted, err := r.validateVmBeforeDetach(ctx, instance)
	if err != nil {
		log.Errorf("failed to validate node VM with nodeUUID %s", instance.Spec.NodeUUID)
		return err
	}
	// Detach may already be completed if:
	// 1. The node itself is deleted.
	// 2. VM has deletion timestamp.
	if detachCompleted {
		// TODO: remove PVC finalizer

		// Remove all PVCs from VM status.
		for _, volume := range volumesToDetach {
			delete(instance.Status.VolumeStatus, volume)
		}

		updateErr := updateCnsNodeVmBatchAttachment(ctx, r.client, instance)
		if updateErr != nil {
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s. Err: +%v", instance.Name, updateErr)
			return updateErr
		}
		log.Debugf("Detach completed for VM %s for volume %+v", instance.Spec.NodeUUID, volumesToDetach)
		return nil
	}

	// Start detach for every volume
	volumesThatFailedToDetach := r.callDetachForAllVolumes(ctx, nodeVM, volumesToDetach, instance)

	var overallErr error
	if len(volumesThatFailedToDetach) != 0 {
		msg := "failed to detach volumes: "
		failedVolumes := strings.Join(volumesThatFailedToDetach, ",")
		msg += failedVolumes
		overallErr = fmt.Errorf(msg)
		log.Error(overallErr)
	}

	return overallErr
}

// callDetachForAllVolumes calls CNS detach API for every volume.
func (r *ReconcileCnsNodeVmBatchAttachment) callDetachForAllVolumes(ctx context.Context, nodeVM *cnsvsphere.VirtualMachine,
	volumesToDetach []string, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) []string {
	log := logger.GetLogger(ctx)

	volumesThatFailedToDetach := make([]string, 0)

	for _, volume := range volumesToDetach {
		cnsVolumeID, _, err := validateCnsVolumeIdBeforeDetach(ctx, instance, volume)
		if err != nil {
			updateInstanceWithError(instance, volume, err.Error())
			volumesThatFailedToDetach = append(volumesThatFailedToDetach, volume)
			continue
		}

		updateInstanceWithAttachState(instance, volume, cnsnodevmbatchattachmentv1alpha1.DetachInProgress)
		updateErr := updateCnsNodeVmBatchAttachment(ctx, r.client, instance)
		if updateErr != nil {
			updateInstanceWithError(instance, volume, err.Error())
			log.Errorf("failed to update CnsNodeVmBatchAttachment %s wit DetachInProgress. Err: +%v", instance.Name, updateErr)
			volumesThatFailedToDetach = append(volumesThatFailedToDetach, volume)
			continue
		}

		_, detachErr := r.volumeManager.DetachVolume(ctx, nodeVM, cnsVolumeID)
		if detachErr != nil {
			if cnsvsphere.IsManagedObjectNotFound(detachErr, nodeVM.VirtualMachine.Reference()) {
				log.Infof("Found a managed object not found fault for vm: %+v", nodeVM)
				// TODO: remove PVC finalizer
				delete(instance.Status.VolumeStatus, volume)
				log.Infof("Successfully detached volume %s from VM %s", volume, instance.Spec.NodeUUID)
				continue
			} else {
				log.Errorf("failed to detach volume %s from VM %s", volume, instance.Spec.NodeUUID)
				updateInstanceWithError(instance, volume, detachErr.Error())
				updateInstanceWithAttachState(instance, volume, cnsnodevmbatchattachmentv1alpha1.DetachFailed)
				volumesThatFailedToDetach = append(volumesThatFailedToDetach, volume)
				continue
			}
		} else {
			// TODO: remove PVC finalizer
			delete(instance.Status.VolumeStatus, volume)
			log.Infof("Successfully detached volume %s from VM %s", volume, instance.Spec.NodeUUID)
		}
	}
	return volumesThatFailedToDetach
}

func (r *ReconcileCnsNodeVmBatchAttachment) batchAttach(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, volumesToAttach []string) error {
	log := logger.GetLogger(ctx)

	// First verify if VM even exists or not
	nodeVM, err := r.getNodeVm(ctx, instance)
	if err != nil {
		log.Errorf("failed to find the VM with UUID: %q for CnsNodeVmBatchAttachment "+
			"request with name: %q on namespace: %q. Err: %+v",
			instance.Spec.NodeUUID, instance.Name, instance.Namespace, err)
		// Update error for all volumes
		for _, volume := range volumesToAttach {
			updateInstanceWithError(instance, volume, err.Error())
		}
		updateErr := updateCnsNodeVmBatchAttachment(ctx, r.client, instance)
		if updateErr != nil {
			log.Errorf("updateCnsNodeVmBatchAttachment failed. err: %v", err)
			return updateErr
		}
		return err
	}

	pvcToVolumeId := make(map[string]string)
	for _, volume := range volumesToAttach {
		volumeID, err := cnsoperatorutil.GetVolumeID(ctx, r.client, volume, instance.Namespace)
		if err != nil {
			log.Errorf("failed to get volumeID from volumeName: %q for CnsNodeVmBatchAttachment "+
				"request with name: %q on namespace: %q. Error: %+v",
				volume, instance.Name, instance.Namespace, err)
			for _, volume := range volumesToAttach {
				volumeStatus := instance.Status.VolumeStatus[volume]
				volumeStatus.AttachState = cnsnodevmbatchattachmentv1alpha1.AttachFailed
				instance.Status.VolumeStatus[volume] = volumeStatus
				updateInstanceWithError(instance, volume, err.Error())
			}
			updateErr := updateCnsNodeVmBatchAttachment(ctx, r.client, instance)
			if updateErr != nil {
				log.Errorf("updateCnsNodeVmBatchAttachment failed. err: %v", updateErr)
				return updateErr
			}
			return err
		}
		pvcToVolumeId[volume] = volumeID
	}

	// TODO:  Add finalizer to PVC if it is not already added

	// TODO: This is placeholder code. It needs to be replaced with the new CNS API.
	attachVolumeId := ""
	for _, vol := range pvcToVolumeId {
		attachVolumeId = vol
		break
	}

	log.Infof("vSphere CSI driver is attaching volume: %q to nodevm: %+v for "+
		"CnsNodeVmBatchAttachment request with name: %q on namespace: %q",
		attachVolumeId, nodeVM, instance.Name, instance.Namespace)

	diskUUID, _, attachErr := r.volumeManager.AttachVolume(ctx, nodeVM, attachVolumeId, false)
	if attachErr != nil {
		log.Errorf("failed to attach disk: %q to nodevm: %+v for CnsNodeVmBatchAttachment "+
			"request with name: %q on namespace: %q. Err: %+v",
			pvcToVolumeId["a"], nodeVM, instance.Name, instance.Namespace, attachErr)
		for _, volume := range volumesToAttach {
			volumeStatus := instance.Status.VolumeStatus[volume]
			volumeStatus.AttachState = cnsnodevmbatchattachmentv1alpha1.AttachFailed
			instance.Status.VolumeStatus[volume] = volumeStatus
			updateInstanceWithError(instance, volume, err.Error())
		}
	} else {
		if instance.Status.VolumeStatus == nil {
			instance.Status.VolumeStatus = make(map[string]cnsnodevmbatchattachmentv1alpha1.VolumeStatus)
		}

		for _, volume := range volumesToAttach {
			instance.Status.VolumeStatus[volume] = cnsnodevmbatchattachmentv1alpha1.VolumeStatus{
				AttachState: cnsnodevmbatchattachmentv1alpha1.AttachCompleted,
				Error:       "",
				CnsVolumeID: attachVolumeId,
				Diskuuid:    diskUUID,
			}
		}
	}

	updateErr := updateCnsNodeVmBatchAttachment(ctx, r.client, instance)
	if updateErr != nil {
		log.Errorf("failed to update attach status on CnsNodeVmBatchAttachment "+
			"instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, updateErr)
		return updateErr
	}

	return attachErr
}

func updateInstanceWithError(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment,
	volume string, errMsg string) {
	volumeStatus := cnsnodevmbatchattachmentv1alpha1.VolumeStatus{}
	if val, exists := instance.Status.VolumeStatus[volume]; exists {
		volumeStatus = val
	} else {
		instance.Status.VolumeStatus[volume] = volumeStatus
	}
	volumeStatus.Error = errMsg
	instance.Status.VolumeStatus[volume] = volumeStatus
}

func updateInstanceWithAttachState(instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, volume string, attachState cnsnodevmbatchattachmentv1alpha1.AttachState) {
	volumeStatus := cnsnodevmbatchattachmentv1alpha1.VolumeStatus{}
	if val, exists := instance.Status.VolumeStatus[volume]; exists {
		volumeStatus = val
	} else {
		instance.Status.VolumeStatus[volume] = volumeStatus
	}
	volumeStatus.AttachState = attachState
	instance.Status.VolumeStatus[volume] = volumeStatus
}

func getVolumesToAttachAndDetach(ctx context.Context, instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) ([]string, []string) {
	log := logger.GetLogger(ctx)

	volumesToAttach := make([]string, 0)
	volumesToDetach := make([]string, 0)

	// All volumes need to be attached every single time
	for _, volume := range instance.Spec.Volumes {
		volumesToAttach = append(volumesToAttach, volume.PvcName)
	}

	volumesInSpec := make(map[string]bool)
	for _, volume := range instance.Spec.Volumes {
		volumesInSpec[volume.PvcName] = true
	}

	for volumeName, _ := range instance.Status.VolumeStatus {
		if _, ok := volumesInSpec[volumeName]; !ok {
			volumesToDetach = append(volumesToDetach, volumeName)
		}
	}

	log.Infof("Volumes to attach: %+v, volumes to detach %+v", volumesToAttach, volumesToDetach)

	return volumesToAttach, volumesToDetach
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer, cns.vmware.com,
// from a given nodevmattachment instance.
func removeFinalizerFromCRDInstance(ctx context.Context,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment) {
	log := logger.GetLogger(ctx)
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Debugf("Removing %q finalizer from CnsNodeVmBatchAttachment instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, instance.Name, instance.Namespace)
			instance.Finalizers = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
		}
	}
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsNodeVmBatchAttachment,
	instance *cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Namespace+"/"+instance.Name] = backOffDuration[instance.Namespace+"/"+instance.Name] * 2
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "NodeVmBatchAttachFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Namespace+"/"+instance.Name] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "NodeVmBatchAttachSucceeded", msg)
		log.Info(msg)
	}
}
