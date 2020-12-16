/*
Copyright 2020 The Kubernetes Authors.

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

package cnsfileaccessconfig

import (
	"context"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	apis "sigs.k8s.io/vsphere-csi-driver/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/util"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

const (
	defaultMaxWorkerThreadsForFileAccessConfig = 10
)

// backOffDuration is a map of cnsnodevmattachment name's to the time after which a request
// for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest reconcile
// operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new CnsFileAccessConfig Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	// Initializes kubernetes client
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsnodevmattachment instances to the event sink
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileCnsFileAccessConfig{client: mgr.GetClient(), scheme: mgr.GetScheme(), configInfo: configInfo, volumeManager: volumeManager, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsFileAccessConfig(ctx)

	// Create a new controller
	c, err := controller.New("cnsfileaccessconfig-controller", mgr, controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new CnsFileAccessConfig controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsFileAccessConfig
	err = c.Watch(&source.Kind{Type: &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsFileAccessConfig resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsFileAccessConfig implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsFileAccessConfig{}

// ReconcileCnsFileAccessConfig reconciles a CnsFileAccessConfig object
type ReconcileCnsFileAccessConfig struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *types.ConfigInfo
	volumeManager volumes.Manager
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a CnsFileAccessConfig object and makes changes based on the state read
// and what is in the CnsFileAccessConfig.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsFileAccessConfig) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx, log := logger.GetNewContextWithLogger()
	// Fetch the CnsFileAccessConfig instance
	instance := &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Infof("CnsFileAccessConfig resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CnsFileAccessConfig with name: %q on namespace: %q. Err: %+v",
			request.Name, request.Namespace, err)
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Initialize backOffDuration for the instance, if required.
	backOffDurationMapMutex.Lock()
	var timeout time.Duration
	if _, exists := backOffDuration[instance.Name]; !exists {
		backOffDuration[instance.Name] = time.Second
	}
	timeout = backOffDuration[instance.Name]
	backOffDurationMapMutex.Unlock()

	// If the CnsFileAccessConfig instance is already successful,
	// and not deleted by the user, remove the instance from the queue.
	if instance.Status.Done && instance.DeletionTimestamp == nil {
		// Cleanup instance entry from backOffDuration map
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	log.Infof("Reconciling CnsFileAccessConfig with instance: %q from namespace: %q. timeout %q seconds", instance.Name, instance.Namespace, timeout)
	if !instance.Status.Done && instance.DeletionTimestamp == nil {
		// Get the virtualmachine instance
		vm, err := getVirtualMachine(ctx, instance.Spec.VMName, instance.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get virtualmachine instance for the VM with name: %q. Error: %+v", instance.Spec.VMName, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Debugf("Found virtualMachine instance for VM with name: %q: +%v", instance.Spec.VMName, vm)

		// Set ownerRef on CnsFileAccessConfig instance (in-memory) to VM instance
		setInstanceOwnerRef(instance, instance.Spec.VMName, vm.UID)

		cnsFinalizerExists := false
		// Check if finalizer already exists.
		for _, finalizer := range instance.Finalizers {
			if finalizer == cnsoperatortypes.CNSFinalizer {
				cnsFinalizerExists = true
				break
			}
		}
		// Update finalizer and ownerRef on CnsFileAccessConfig instance
		if !cnsFinalizerExists {
			// Add finalizer.
			instance.Finalizers = append(instance.Finalizers, cnsoperatortypes.CNSFinalizer)
			err = updateCnsFileAccessConfig(ctx, r.client, instance)
			if err != nil {
				msg := fmt.Sprintf("failed to update CnsFileAccessConfig instance: %q on namespace: %q. Error: %+v",
					instance.Name, instance.Namespace, err)
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
		}

		volumeID, err := util.GetVolumeID(ctx, r.client, instance.Spec.PvcName, instance.Namespace)
		if err != nil {
			msg := fmt.Sprintf("Failed to get volumeID from pvcName: %q. Error: %+v", instance.Spec.PvcName, err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Query volume
		log.Infof("Querying volume: %s for CnsFileAccessConfig request with name: %q on namespace: %q",
			volumeID, instance.Name, instance.Namespace)
		volume, err := common.QueryVolumeByID(ctx, r.volumeManager, volumeID)
		if err != nil {
			if err.Error() == common.ErrNotFound.Error() {
				msg := fmt.Sprintf("CNS Volume: %s not found", volumeID)
				log.Error(msg)
				setInstanceError(ctx, r, instance, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			log.Errorf("Failed to query CNS volume: %s with error: %+v", volumeID, err)
			setInstanceError(ctx, r, instance, "Unable to find the volume in CNS")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		vSANFileBackingDetails := volume.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
		accessPoints := make(map[string]string)
		for _, kv := range vSANFileBackingDetails.AccessPoints {
			accessPoints[kv.Key] = kv.Value
		}
		if len(accessPoints) == 0 {
			log.Errorf("No access points found for volume: %q", volumeID)
			setInstanceError(ctx, r, instance, "No access points found.")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Read the CnsFileAccessConfig instance again because the instance is already modified
		err = r.client.Get(ctx, request.NamespacedName, instance)
		if err != nil {
			msg := fmt.Sprintf("Error reading the CnsFileAccessConfig with name: %q on namespace: %q. Err: %+v",
				request.Name, request.Namespace, err)
			// Error reading the object - requeue the request.
			recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}

		// Update the instance to indicate the volume registration is successful
		msg := fmt.Sprintf("Successfully configured access points of VM: %q on the volume: %q", instance.Spec.VMName, instance.Spec.PvcName)
		instance.Status.AccessPoints = accessPoints
		err = setInstanceSuccess(ctx, r, instance, msg)
		if err != nil {
			msg := fmt.Sprintf("Failed to update CnsFileAccessConfig instance with error: %+v", err)
			log.Error(msg)
			setInstanceError(ctx, r, instance, msg)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Info(msg)
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}, nil
}

// setInstanceSuccess sets instance to success and records an event on the CnsRegisterVolume instance
func setInstanceSuccess(ctx context.Context, r *ReconcileCnsFileAccessConfig,
	instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, msg string) error {
	instance.Status.Done = true
	instance.Status.Error = ""
	err := updateCnsFileAccessConfig(ctx, r.client, instance)
	if err != nil {
		return err
	}
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
	return nil
}

// setInstanceError sets error and records an event on the CnsFileAccessConfig instance
func setInstanceError(ctx context.Context, r *ReconcileCnsFileAccessConfig,
	instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, errMsg string) {
	log := logger.GetLogger(ctx)
	instance.Status.Error = errMsg
	err := updateCnsFileAccessConfig(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateCnsFileAccessConfig failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

func updateCnsFileAccessConfig(ctx context.Context, client client.Client, instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("failed to update CnsFileAccessConfig instance: %q on namespace: %q. Error: %+v",
			instance.Name, instance.Namespace, err)
	}
	return err
}

// recordEvent records the event, sets the backOffDuration for the instance appropriately
// and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsFileAccessConfig, instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		r.recorder.Event(instance, v1.EventTypeWarning, "CnsFileAccessConfigFailed", msg)
		backOffDurationMapMutex.Unlock()
	case v1.EventTypeNormal:
		// Reset backOff duration to one second
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		r.recorder.Event(instance, v1.EventTypeNormal, "CnsFileAccessConfigSucceeded", msg)
		backOffDurationMapMutex.Unlock()
	}
}

// removeFinalizerFromCRDInstance will remove the CNS Finalizer = cns.vmware.com, from a given CnsFileAccessConfig instance
func removeFinalizerFromCRDInstance(ctx context.Context, instance *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig) {
	log := logger.GetLogger(ctx)
	for i, finalizer := range instance.Finalizers {
		if finalizer == cnsoperatortypes.CNSFinalizer {
			log.Debugf("Removing %q finalizer from CnsFileAccessConfig instance with name: %q on namespace: %q",
				cnsoperatortypes.CNSFinalizer, instance.Name, instance.Namespace)
			instance.Finalizers = append(instance.Finalizers[:i], instance.Finalizers[i+1:]...)
		}
	}
}
