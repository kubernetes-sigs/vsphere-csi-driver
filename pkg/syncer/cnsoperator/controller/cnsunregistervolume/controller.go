/*
Copyright 2024 The Kubernetes Authors.

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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	defaultMaxWorkerThreads = 40
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
	return &Reconciler{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	// Create a new controller.
	c, err := controller.New("cnsunregistervolume-controller", mgr,
		controller.Options{
			Reconciler:              r,
			MaxConcurrentReconciles: getMaxWorkerThreads(ctx),
		})
	if err != nil {
		log.Errorf("Failed to create new CnsUnregisterVolume controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[types.NamespacedName]time.Duration)

	// Watch for changes to primary resource CnsUnregisterVolume.
	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&v1a1.CnsUnregisterVolume{},
		&handler.TypedEnqueueRequestForObject[*v1a1.CnsUnregisterVolume]{},
	))
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsUnregisterVolume resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that Reconciler implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &Reconciler{}

// Reconciler reconciles a CnsŪnregisterVolume object.
type Reconciler struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *commonconfig.ConfigurationInfo
	volumeManager volumes.Manager
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a Reconciler object
// and makes changes based on the state read and what is in the
// Reconciler.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the
// returned error is non-nil or Result.Requeue is true. Otherwise, upon
// completion it will remove the work from the queue.
func (r *Reconciler) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	// Fetch the CnsUnregisterVolume instance.
	instance := &v1a1.CnsUnregisterVolume{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("CnsUnregisterVolume resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}

		log.Errorf("Error reading the CnsUnregisterVolume with name: %q on namespace: %q. Err: %+v",
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

	// If the volume is already unregistered, remove the instance from the queue.
	if instance.Status.Unregistered {
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	log.Infof("Reconciling CnsUnregisterVolume instance %q from namespace %q. timeout %q seconds",
		instance.Name, request.Namespace, timeout)
	pvName, pvExists := commonco.ContainerOrchestratorUtility.GetPVNameFromCSIVolumeID(instance.Spec.VolumeID)
	if pvExists {
		log.Infof("found PV: %q for the volume Id: %q", pvName, instance.Spec.VolumeID)
	} else {
		log.Infof("cound not find PV for the volume Id: %q", instance.Spec.VolumeID)
	}
	pvcName, pvcNamespace, pvcExists := commonco.ContainerOrchestratorUtility.
		GetPVCNameFromCSIVolumeID(instance.Spec.VolumeID)
	if !pvcExists {
		log.Infof("cound not find PVC for the volume Id: %q", instance.Spec.VolumeID)
	} else {
		log.Infof("found PVC: %q in the namespace: %q for the volume Id: %q", pvcName, pvcNamespace,
			instance.Spec.VolumeID)
	}

	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to initialize K8S client when reconciling CnsUnregisterVolume "+
			"instance: %s on namespace: %s. Error: %+v", instance.Name, instance.Namespace, err)
		setInstanceError(ctx, r, instance, "Failed to init K8S client for volume unregistration")
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// TODO: Fix this to use the complex error struct if implemented.
	usageInfo, err := getVolumeUsageInfo(ctx, k8sClient, pvcName, pvcNamespace,
		instance.Spec.ForceUnregister)
	if err != nil {
		log.Error(err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if usageInfo.isInUse {
		msg := fmt.Sprintf("Volume %q cannot be unregistered because %s", instance.Spec.VolumeID, usageInfo)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	err = k8s.RetainPersistentVolume(ctx, k8sClient, pvName)
	if err != nil {
		log.Error(err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	err = k8s.DeletePersistentVolumeClaim(ctx, k8sClient, pvcName, pvcNamespace)
	if err != nil {
		log.Error(err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	err = k8s.DeletePersistentVolume(ctx, k8sClient, pvName)
	if err != nil {
		log.Error(err)
		setInstanceError(ctx, r, instance, err.Error())
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// TODO: use CNS UnregisterVolume API when it's implemented. And create a util func
	// to handle this.
	// Also, rethink the order in which the delete operations are performed.
	// Maybe it's better to delete the CNS volume first and then delete the PVC and PV?
	log.Info("CNS UnregisterVolume API not implemented yet. Using CNS DeleteVolume API instead for now")
	_, err = r.volumeManager.DeleteVolume(ctx, instance.Spec.VolumeID, false)
	if err != nil {
		if cnsvsphere.IsNotFoundError(err) {
			log.Infof("VolumeID %q not found in CNS. It may have already been deleted."+
				"Marking the operation as success.", instance.Spec.VolumeID)
		} else {
			log.Errorf("Failed to delete volume %q in CNS with error %s.",
				instance.Spec.VolumeID, err)
			return reconcile.Result{}, err
		}
	} else {
		log.Infof("Deleted CNS volume %q with deleteDisk set to false", instance.Spec.VolumeID)
	}

	// Update the instance to indicate the volume unregistration is successful.
	msg := fmt.Sprintf("Successfully unregistered the volume on namespace: %s", instance.Namespace)
	err = setInstanceSuccess(ctx, r, instance, msg)
	if err != nil {
		msg := fmt.Sprintf("Failed to update CnsUnregisterVolume instance with error: %s", err)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()
	log.Info(msg)
	return reconcile.Result{}, nil
}

// setInstanceError sets error and records an event on the CnsUnregisterVolume
// instance.
func setInstanceError(ctx context.Context, r *Reconciler,
	instance *v1a1.CnsUnregisterVolume, errMsg string) {
	log := logger.GetLogger(ctx)
	instance.Status.Error = errMsg
	err := updateCnsUnregisterVolume(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateCnsUnregisterVolume failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

// setInstanceSuccess sets instance to success and records an event on the
// CnsUnregisterVolume instance.
func setInstanceSuccess(ctx context.Context, r *Reconciler,
	instance *v1a1.CnsUnregisterVolume, msg string) error {
	instance.Status.Unregistered = true
	instance.Status.Error = ""
	err := updateCnsUnregisterVolume(ctx, r.client, instance)
	if err != nil {
		return err
	}
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return nil
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
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
		backOffDuration[namespacedName] = backOffDuration[namespacedName] * 2
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

// updateCnsUnregisterVolume updates the CnsUnregisterVolume instance in K8S.
// TODO: use status subresource to update the status of the instance.
// This will avoid the need to update the entire instance and prevent extra reconcile calls.
// Ref: CNS Node VM Batch Attachment Controller.
func updateCnsUnregisterVolume(ctx context.Context, client client.Client,
	instance *v1a1.CnsUnregisterVolume) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("Failed to update CnsUnregisterVolume instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}
