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

package cnsunregistervolume

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
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
	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	cnsoptypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadEnvVar      = "WORKER_THREADS_UNREGISTER_VOLUME"
	defaultMaxWorkerThreads = 10
)

var (
	// backOffDuration is a map of cnsunregistervolume name's to the time after which
	// a request for this instance will be requeued.
	// Initialized to 1 second for new instances and for instances whose latest
	// reconcile operation succeeded.
	// If the reconcile fails, backoff is incremented exponentially.
	backOffDuration         map[types.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new CnsUnregisterVolume Controller and adds it to the Manager,
// ConfigurationInfo and VirtualCenterTypes. The Manager will set fields on
// the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsUnregisterVolume Controller as its a non-WCP CSI deployment")
		return nil
	}

	coCommonInterface, err := commonco.GetContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}

	if !coCommonInterface.IsFSSEnabled(ctx, common.WCPMobilityNonDisruptiveImport) {
		log.Infof("Not initializing the CnsUnregisterVolume Controller as this feature is disabled on the cluster")
		return nil
	}

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on CNSUnregisterVolume instances to the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, configInfo *commonconfig.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	return &Reconciler{
		client:        mgr.GetClient(),
		scheme:        mgr.GetScheme(),
		configInfo:    configInfo,
		volumeManager: volumeManager,
		recorder:      recorder,
	}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx,
		workerThreadEnvVar, defaultMaxWorkerThreads)
	// Create a new controller.
	err := ctrl.NewControllerManagedBy(mgr).Named("cnsunregistervolume-controller").
		For(&v1a1.CnsUnregisterVolume{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("Failed to build application controller. Err: %v", err)
		return err
	}

	backOffDuration = make(map[types.NamespacedName]time.Duration)
	return nil
}

// blank assignment to verify that Reconciler implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &Reconciler{}

// Reconciler reconciles a CnsUnregisterVolume object.
type Reconciler struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *commonconfig.ConfigurationInfo
	volumeManager volumes.Manager
	recorder      record.EventRecorder
}

var (
	newK8sClient           = k8s.NewClient
	protectPVC             = k8s.AddFinalizerOnPVC
	retainPV               = k8s.RetainPersistentVolume
	deletePVC              = k8s.DeletePersistentVolumeClaim
	deletePV               = k8s.DeletePersistentVolume
	removeFinalizerFromPVC = k8s.RemoveFinalizerFromPVC
)

// Reconcile reads that state of the cluster for a Reconciler object
// and makes changes based on the state read and what is in the
// Reconciler.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the
// returned error is non-nil or Result.Requeue is true. Otherwise, upon
// completion it will remove the work from the queue.
func (r *Reconciler) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	// Fetch the CnsUnregisterVolume instance.
	instance := &v1a1.CnsUnregisterVolume{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("instance not found. Ignoring since it must be deleted.")
			return reconcile.Result{}, nil
		}

		log.Error("Error reading the instance. ", err)
		return reconcile.Result{}, err
	}

	log.Info("reconciling instance")
	defer func() {
		log.Info("finished reconciling instance")
	}()

	backoff := getBackoffDuration(ctx, request.NamespacedName)
	log.Info("backoff duration is ", backoff)

	// Handle deletion of the instance.
	// The finalizer will only be removed in the following cases:
	// 1. The volume is already unregistered.
	// 2. The volume is in use and cannot be unregistered.
	// 3. The volume is successfully unregistered.
	// 4. The volume does not exist or the operation is not supported.
	// In all other cases, the instance will be re-queued for reconciliation.
	// This ensures that the system remains in a consistent state.
	// See reconcileDelete for more details.
	if instance.DeletionTimestamp != nil {
		log.Info("instance is marked for deletion")
		err = r.reconcileDelete(ctx, instance, request)
		if err != nil {
			log.Error("failed to reconcile with error ", err)
			setInstanceError(ctx, r, instance, err.Error())
			return reconcile.Result{RequeueAfter: backoff}, nil
		}

		log.Info("removing finalizer and allowing deletion of the instance")
		err := removeFinalizer(ctx, r.client, instance)
		if err != nil {
			log.Error("failed to remove finalizer from the instance with error ", err)
			setInstanceError(ctx, r, instance, "failed to remove finalizer from the instance")
			return reconcile.Result{RequeueAfter: backoff}, nil
		}

		deleteBackoffEntry(ctx, request.NamespacedName)
		return reconcile.Result{}, nil
	}

	// If the volume is already unregistered, remove the instance from the queue.
	if instance.Status.Unregistered {
		log.Debug("instance is already unregistered")
		deleteBackoffEntry(ctx, request.NamespacedName)
		return reconcile.Result{}, nil
	}

	err = r.reconcile(ctx, instance, request)
	if err != nil {
		log.Error("failed to reconcile with error ", err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: backoff}, nil
	}

	msg := "successfully unregistered the volume"
	err = setInstanceSuccess(ctx, r, instance, msg)
	if err != nil {
		log.Warn("failed to update status to success with error ", err)
		setInstanceError(ctx, r, instance, "failed to update status to success")
		return reconcile.Result{RequeueAfter: backoff}, nil
	}

	deleteBackoffEntry(ctx, request.NamespacedName)
	log.Info(msg)
	return reconcile.Result{}, nil
}

// reconcile handles creation/update of a CnsUnregisterVolume instance.
func (r *Reconciler) reconcile(ctx context.Context,
	instance *v1a1.CnsUnregisterVolume, request reconcile.Request) error {
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	params := getParams(ctx, *instance)

	// protect the instance before performing any operation.
	err := protectInstance(ctx, r.client, instance)
	if err != nil {
		log.Error("failed to protect instance with error ", err)
		return err
	}

	usageInfo, err := getVolumeUsageInfo(ctx, params.pvcName, params.namespace, params.force)
	if err != nil {
		log.Error("failed to get volume usage info with error ", err)
		return err
	}

	if usageInfo.isInUse {
		msg := fmt.Sprintf("volume %s cannot be unregistered because %s", params.volumeID, usageInfo)
		log.Error(msg)
		return errors.New(msg)
	}

	faultType, err := unregisterVolume(ctx, r.volumeManager, request, params)
	if err != nil {
		log.Error("failed to unregister volume with error ", err)
		if faultType == fault.VimFaultNotFound {
			// By design, NotFound error is ignored and the instance is marked
			// as successfully unregistered.
			log.With("fault", faultType).With("error", err).
				Warn("ignoring error and marking instance as successfully unregistered")
			return nil
		}

		return err
	}

	log.Info("successfully unregistered volume")
	return nil
}

// reconcileDelete handles deletion of a CnsUnregisterVolume instance.
// The reconciler tries to keep the system consistent by carefully deciding
// when to continue with volume unregistration or when to allow deletion of the instance
// without unregistering the volume.
// See the comments below and in unregisterVolume for more details.
func (r *Reconciler) reconcileDelete(ctx context.Context,
	instance *v1a1.CnsUnregisterVolume, request reconcile.Request) error {
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	if instance.Status.Unregistered {
		// If the volume is already unregistered, the instance can be deleted.
		log.Info("volume is already unregistered")
		return nil
	}

	params := getParams(ctx, *instance)
	usageInfo, err := getVolumeUsageInfo(ctx, params.pvcName, params.namespace, params.force)
	if err != nil {
		log.Error("failed to get volume usage info with error ", err)
		return err
	}

	if usageInfo.isInUse {
		// If the volume is in use, the instance can be deleted
		// since the volume cannot be unregistered.
		log.Info(usageInfo)
		return nil
	}

	// Try to unregister the volume. This ensures that the system remains
	// consistent and the volume is not left in an unusable state.
	// If unregistration fails, the instance will be re-queued for
	// reconciliation.
	faultType, err := unregisterVolume(ctx, r.volumeManager, request, params)
	if err != nil {
		if faultType == fault.VimFaultNotFound ||
			faultType == fault.VimFaultNotSupported {
			// Since the state of the system won't change by retrying,
			// the instance can be deleted.
			log.With("fault", faultType).With("error", err).
				Warn("ignoring error and proceeding with deletion of the instance")
			return nil
		}

		// For all other errors, return the error and requeue the instance
		// as it's likely that the error is transient.
		log.With("fault", faultType).With("error", err).
			Error("failed to unregister volume with error")
		return err
	}

	log.Info("successfully unregistered volume")
	return nil
}

var unregisterVolume = _unregisterVolume

// _unregisterVolume unregisters the volume and deletes associated PV/PVC.
// If retainFCD is false, the FCD is also deleted.
// The associated PVC is protected with a finalizer to prevent deletion
// before the volume is unregistered.
// The finalizer is removed from the PVC after successful unregistration
// ensuring correct garbage collection of the PVC and PV.
// If the PVC or PV are not found, the volume is still unregistered.
// `ignoreNonTransientError` is used to ignore non-transient errors from CNS.
// This is EXPLICITLY used by the reconciler to handle deletion of the instance.
//
// Behavior Matrix:
//
//	+------------+-----------+-------------+-----------------------------------------------+
//	| PVC Exists | PV Exists | FCD Exists  | Action                                        |
//	+------------+-----------+-------------+-----------------------------------------------+
//	| true       | true      | true        | Delete PVC/PV, Unregister FCD                 |
//	| true       | true      | false       | Delete PVC/PV                                 |
//	| true       | false     | true        | Delete PVC, Delete volume                     |
//	| true       | false     | false       | Delete PVC                                    |
//	| false      | true      | true        | Delete PV, Unregister FCD                     |
//	| false      | true      | false       | Delete PV                                     |
//	| false      | false     | true        | Unregister FCD                                |
//	| false      | false     | false       | Mark success (no-op)                          |
//	+------------+-----------+-------------+-----------------------------------------------+
func _unregisterVolume(ctx context.Context, volMgr volumes.Manager, request reconcile.Request,
	params params) (string, error) {
	log := logger.GetLogger(ctx).With("name", request.NamespacedName)

	k8sClient, err := newK8sClient(ctx)
	if err != nil {
		log.Error("failed to init K8s client for volume unregistration with error ", err)
		return "", errors.New("failed to init K8s client for volume unregistration")
	}

	if params.pvcName != "" {
		err = protectPVC(ctx, k8sClient, params.pvcName, params.namespace,
			cnsoptypes.CNSUnregisterProtectionFinalizer)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error("failed to protect associated PVC with error ", err)
				return "", fmt.Errorf("failed to protect associated PVC %s/%s", params.namespace, params.pvcName)
			}

			log.Info("associated PVC not found. Continuing with volume unregistration")
		}
	}

	if params.pvName != "" {
		err = retainPV(ctx, k8sClient, params.pvName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error("failed to set reclaim policy to Retain on associated PV with error ", err)
				return "", fmt.Errorf("failed to set reclaim policy to Retain on associated PV %s", params.pvName)
			}

			log.Info("associated PV not found. Continuing with volume unregistration")
		}
	}

	if params.pvcName != "" {
		err = deletePVC(ctx, k8sClient, params.pvcName, params.namespace)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error("failed to delete associated PVC with error ", err)
				return "", fmt.Errorf("failed to delete associated PVC %s/%s", params.namespace, params.pvcName)
			}

			log.Info("associated PVC not found. Continuing with volume unregistration")
		}
	}

	if params.pvName != "" {
		err = deletePV(ctx, k8sClient, params.pvName)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error("failed to delete associated PV with error ", err)
				return "", fmt.Errorf("failed to delete associated PV %s", params.pvName)
			}

			log.Info("associated PV not found. Continuing with volume unregistration")
		}
	}

	if params.volumeID != "" {
		unregDisk := !params.retainFCD // If retainFCD is false, unregister the FCD too.
		faultType, err := volMgr.UnregisterVolume(ctx, params.volumeID, unregDisk)
		if err != nil {
			log.Error("failed to unregister CNS volume with error ", err)
			return faultType, fmt.Errorf("failed to unregister associated volume %s", params.volumeID)
		}
	}

	if params.pvcName != "" {
		err = removeFinalizerFromPVC(ctx, k8sClient, params.pvcName, params.namespace,
			cnsoptypes.CNSUnregisterProtectionFinalizer)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error("failed to remove finalizer from associated PVC with error ", err)
				return "", fmt.Errorf("failed to remove finalizer from associated PVC %s/%s",
					params.namespace, params.pvcName)
			}

			log.Info("associated PVC not found. Continuing with volume unregistration")
		}
	}

	log.Info("successfully unregistered CNS volume ", params.volumeID)
	return "", nil
}

// setInstanceError sets error and records an event on the CnsUnregisterVolume
// instance.
func setInstanceError(ctx context.Context, r *Reconciler,
	instance *v1a1.CnsUnregisterVolume, errMsg string) {
	instance.Status.Error = errMsg
	_ = k8s.UpdateStatus(ctx, r.client, instance)
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

// setInstanceSuccess sets instance to success and records an event on the
// CnsUnregisterVolume instance.
func setInstanceSuccess(ctx context.Context, r *Reconciler,
	instance *v1a1.CnsUnregisterVolume, msg string) error {
	instance.Status.Unregistered = true
	instance.Status.Error = ""
	err := k8s.UpdateStatus(ctx, r.client, instance)
	if err != nil {
		return err
	}

	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return nil
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure
// until it reaches a maximum of 5 minutes.
func recordEvent(ctx context.Context, r *Reconciler,
	instance *v1a1.CnsUnregisterVolume, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	namespacedName := types.NamespacedName{
		Name:      instance.Name,
		Namespace: instance.Namespace,
	}
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		doubleBackoffDuration(ctx, namespacedName)
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsUnregisterVolumeFailed", msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		updateBackoffEntry(ctx, namespacedName, time.Second)
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsUnregisterVolumeSucceeded", msg)
	}
}

type params struct {
	retainFCD bool
	force     bool
	namespace string
	volumeID  string
	pvcName   string
	pvName    string
}

func (p params) String() string {
	parts := []string{}
	if p.retainFCD {
		parts = append(parts, fmt.Sprintf("retainFCD: %t", p.retainFCD))
	}
	if p.force {
		parts = append(parts, fmt.Sprintf("force: %t", p.force))
	}
	if p.namespace != "" {
		parts = append(parts, fmt.Sprintf("namespace: %s", p.namespace))
	}
	if p.volumeID != "" {
		parts = append(parts, fmt.Sprintf("volumeID: %s", p.volumeID))
	}
	if p.pvcName != "" {
		parts = append(parts, fmt.Sprintf("pvcName: %s", p.pvcName))
	}
	if p.pvName != "" {
		parts = append(parts, fmt.Sprintf("pvName: %s", p.pvName))
	}
	return strings.Join(parts, ", ")
}

var getParams = _getParams

// _getParams returns the parameters required for volume unregistration.
// If VolumeID is specified, PVCName and PVName are derived.
// If PVCName is specified, VolumeID and PVName are derived.
func _getParams(ctx context.Context, instance v1a1.CnsUnregisterVolume) params {
	log := logger.GetLogger(ctx).With("name", instance.Namespace+"/"+instance.Name)
	var err error
	p := params{
		retainFCD: instance.Spec.RetainFCD,
		force:     instance.Spec.ForceUnregister,
		namespace: instance.Namespace,
	}

	if instance.Spec.VolumeID != "" {
		p.volumeID = instance.Spec.VolumeID

		p.pvcName, _, err = getPVCName(ctx, instance.Spec.VolumeID)
		if err != nil {
			log.With("volumeID", instance.Spec.VolumeID).Info("no PVC found for the Volume ID")
		}
	} else {
		p.pvcName = instance.Spec.PVCName
		p.volumeID, err = getVolumeID(ctx, p.pvcName, p.namespace)
		if err != nil {
			log.With("pvcName", p.pvcName, "namespace", p.namespace).Info("no VolumeID found for the PVC")
		}
	}

	p.pvName, err = getPVName(ctx, p.volumeID)
	if err != nil {
		log.With("volumeID", p.volumeID).Info("no PV found for the volumeID")
	}

	log.With("params", p).Info("derived parameters for volume unregistration")
	return p
}

// getBackoffDuration returns the backoff duration for the instance.
// If the instance is not present in the map, it is added with
// a backoff duration of 1 second.
func getBackoffDuration(ctx context.Context, name types.NamespacedName) time.Duration {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	if _, exists := backOffDuration[name]; !exists {
		backOffDuration[name] = time.Second
	}

	return backOffDuration[name]
}

// doubleBackoffDuration doubles the backoff duration for the instance
// until it reaches a maximum of 5 minutes.
func doubleBackoffDuration(ctx context.Context, name types.NamespacedName) {
	d := getBackoffDuration(ctx, name)
	d = min(d*2, cnsoptypes.MaxBackOffDurationForReconciler)
	updateBackoffEntry(ctx, name, d)
}

// updateBackoffEntry updates the backoff duration for the instance.
func updateBackoffEntry(ctx context.Context, name types.NamespacedName, duration time.Duration) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	backOffDuration[name] = duration
}

// deleteBackoffEntry deletes the backoff entry for the instance.
func deleteBackoffEntry(ctx context.Context, name types.NamespacedName) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	delete(backOffDuration, name)
}

// protectInstance adds finalizer to the CnsUnregisterVolume instance to handle deletion.
func protectInstance(ctx context.Context, c client.Client, obj *v1a1.CnsUnregisterVolume) error {
	log := logger.GetLogger(ctx).
		With("name", obj.Namespace+"/"+obj.Name).
		With("finalizer", cnsoptypes.CNSUnregisterVolumeFinalizer)

	if !controllerutil.AddFinalizer(obj, cnsoptypes.CNSUnregisterVolumeFinalizer) {
		log.Info("finalizer already exists on instance")
		return nil
	}

	log.Info("adding finalizer to instance")
	return c.Update(ctx, obj)
}

// removeFinalizer removes finalizer from the CnsUnregisterVolume instance to allow deletion.
func removeFinalizer(ctx context.Context, c client.Client, obj *v1a1.CnsUnregisterVolume) error {
	log := logger.GetLogger(ctx).
		With("name", obj.Namespace+"/"+obj.Name).
		With("finalizer", cnsoptypes.CNSUnregisterVolumeFinalizer)

	if controllerutil.RemoveFinalizer(obj, cnsoptypes.CNSUnregisterVolumeFinalizer) {
		log.Info("removing finalizer from instance")
		return c.Update(ctx, obj)
	}

	log.Info("finalizer does not exist on instance")
	return nil
}
