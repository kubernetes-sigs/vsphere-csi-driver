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
	if !coCommonInterface.IsFSSEnabled(ctx, common.CnsUnregisterVolume) {
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
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxWorkerThreads},
		).
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

	if instance.DeletionTimestamp != nil {
		// TODO: Need to handle deletion if the instance is not fully processed.
		log.Info("instance is being deleted")
		err = removeFinalizer(ctx, r.client, instance)
		if err != nil {
			log.Error("failed to remove finalizer from the instance with error ", err)
			return reconcile.Result{}, err
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

	err = protectInstance(ctx, r.client, instance)
	if err != nil {
		log.Error("failed to protect instance with error ", err)
		return reconcile.Result{}, err
	}

	// Initialize backOffDuration for the instance, if required.
	duration := getBackoffDuration(ctx, request.NamespacedName)
	params, err := getValidatedParams(ctx, *instance)
	if err != nil {
		log.Error("failed to get input parameters with error ", err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	k8sClient, err := newK8sClient(ctx)
	if err != nil {
		log.Error("failed to init K8s client for volume unregistration with error ", err)
		setInstanceError(ctx, r, instance, "failed to init K8s client for volume unregistration")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	usageInfo, err := getVolumeUsageInfo(ctx, k8sClient, params.pvcName, params.namespace,
		instance.Spec.ForceUnregister)
	if err != nil {
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	if usageInfo.isInUse {
		msg := fmt.Sprintf("volume %s cannot be unregistered because %s", params.volumeID, usageInfo)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	err = protectPVC(ctx, k8sClient, params.pvcName, params.namespace,
		cnsoptypes.CNSUnregisterProtectionFinalizer)
	if err != nil {
		setInstanceError(ctx, r, instance, "failed to add finalizer on PVC")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	err = deletePVC(ctx, k8sClient, params.pvcName, params.namespace)
	if err != nil {
		setInstanceError(ctx, r, instance, "failed to delete PVC")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	err = deletePV(ctx, k8sClient, params.pvName)
	if err != nil {
		setInstanceError(ctx, r, instance, "failed to delete PV")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	unregDisk := !instance.Spec.RetainFCD
	err = r.volumeManager.UnregisterVolume(ctx, params.volumeID, unregDisk)
	if err != nil {
		setInstanceError(ctx, r, instance, "failed to unregister volume")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	err = removeFinalizerFromPVC(ctx, k8sClient, params.pvcName, params.namespace,
		cnsoptypes.CNSUnregisterProtectionFinalizer)
	if err != nil {
		setInstanceError(ctx, r, instance, "failed to remove finalizer from PVC")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	log.Infof("successfully unregistered CNS volume %s", params.volumeID)
	msg := "successfully unregistered the volume"
	err = setInstanceSuccess(ctx, r, instance, msg)
	if err != nil {
		log.Warn("failed to set instance to success with error ", err)
		setInstanceError(ctx, r, instance, "failed to set instance to success")
		return reconcile.Result{RequeueAfter: duration}, nil
	}

	deleteBackoffEntry(ctx, request.NamespacedName)
	log.Info(msg)
	return reconcile.Result{}, nil
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
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = min(backOffDuration[namespacedName]*2,
			cnsoptypes.MaxBackOffDurationForReconciler)
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsUnregisterVolumeFailed", msg)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[namespacedName] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsUnregisterVolumeSucceeded", msg)
		backOffDurationMapMutex.Unlock()
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

var getValidatedParams = _getValidatedParams

func _getValidatedParams(ctx context.Context, instance v1a1.CnsUnregisterVolume) (*params, error) {
	log := logger.GetLogger(ctx).With("name", instance.Namespace+"/"+instance.Name)
	var err error
	p := params{
		retainFCD: instance.Spec.RetainFCD,
		force:     instance.Spec.ForceUnregister,
		namespace: instance.Namespace,
	}

	if instance.Spec.VolumeID == "" && instance.Spec.PVCName == "" {
		return nil, errors.New("either VolumeID or PVCName must be specified")
	}

	if instance.Spec.VolumeID != "" && instance.Spec.PVCName != "" {
		return nil, errors.New("both VolumeID and PVCName cannot be specified")
	}

	if instance.Spec.VolumeID != "" {
		p.volumeID = instance.Spec.VolumeID

		p.pvcName, _, err = getPVCName(ctx, instance.Spec.VolumeID)
		if err != nil {
			log.Info("no PVC found for the Volume ID ", instance.Spec.VolumeID)
		}
	} else {
		p.pvcName = instance.Spec.PVCName
		p.volumeID, err = getVolumeID(ctx, p.pvcName, p.namespace)
		if err != nil {
			log.Info("no Volume found for the PVC ", p.pvcName)
		}
	}

	p.pvName, err = getPVName(ctx, p.volumeID)
	if err != nil {
		log.Info("no PV found for the Volume ID ", p.volumeID)
	}

	return &p, nil
}

func getBackoffDuration(ctx context.Context, name types.NamespacedName) time.Duration {
	log := logger.GetLogger(ctx).With("name", name)
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	if _, exists := backOffDuration[name]; !exists {
		log.Debug("initializing backoff duration to 1 second")
		backOffDuration[name] = time.Second
	}

	log.Infof("backoff duration is %s", backOffDuration[name])
	return backOffDuration[name]
}

func deleteBackoffEntry(ctx context.Context, name types.NamespacedName) {
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	delete(backOffDuration, name)
}

func protectInstance(ctx context.Context, c client.Client, obj *v1a1.CnsUnregisterVolume) error {
	log := logger.GetLogger(ctx).With("name", obj.Namespace+"/"+obj.Name)

	if !controllerutil.AddFinalizer(obj, cnsoptypes.CNSUnregisterVolumeFinalizer) {
		log.Debugf("finalizer %s already exists on instance", cnsoptypes.CNSUnregisterVolumeFinalizer)
		return nil
	}

	log.Infof("adding finalizer %s to instance", cnsoptypes.CNSUnregisterVolumeFinalizer)
	return c.Update(ctx, obj)
}

func removeFinalizer(ctx context.Context, c client.Client, obj *v1a1.CnsUnregisterVolume) error {
	log := logger.GetLogger(ctx).With("name", obj.Namespace+"/"+obj.Name)

	if controllerutil.RemoveFinalizer(obj, cnsoptypes.CNSUnregisterVolumeFinalizer) {
		log.Infof("removing finalizer %s from instance", cnsoptypes.CNSUnregisterVolumeFinalizer)
		return c.Update(ctx, obj)
	}

	log.Debugf("finalizer %s does not exist on instance", cnsoptypes.CNSUnregisterVolumeFinalizer)
	return nil
}
