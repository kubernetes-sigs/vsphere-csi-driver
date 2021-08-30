/*
Copyright 2021 The Kubernetes Authors.

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

package triggercsifullsync

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	apis "sigs.k8s.io/vsphere-csi-driver/v2/pkg/apis/cnsoperator"
	volumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v2/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer"
)

const (
	defaultMaxWorkerThreadsForTriggerCsiFullSync = 1
)

// backOffDuration is a map of triggercsifullsync name's to the time after which
// a request for this instance will be requeued. Initialized to 1 second for new
// instances and for instances whose latest reconcile operation succeeded. If
// the reconcile fails, backoff is incremented exponentially.
// This map will have only one {name: time} pair.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new TriggerCsiFullSync Controller and adds it to the Manager,
// ConfigurationInfo and VirtualCenterTypes. The Manager will set fields on the
// Controller and start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()

	var coCommonInterface commonco.COCommonInterface
	var err error
	coCommonInterface, err = commonco.GetContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}
	if !coCommonInterface.IsFSSEnabled(ctx, common.TriggerCsiFullSync) {
		log.Infof("Not initializing the TriggerCsiFullSync Controller as this feature is disabled on the cluster")
		return nil
	}
	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on triggercsifullsync instances to the
	// event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})
	return add(mgr, newReconciler(mgr, clusterFlavor, configInfo, recorder))
}

// newReconciler returns a new reconcile.Reconciler.
func newReconciler(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, recorder record.EventRecorder) reconcile.Reconciler {
	return &ReconcileTriggerCsiFullSync{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		clusterFlavor: clusterFlavor, configInfo: configInfo, recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := getMaxWorkerThreadsToReconcileTriggerCsiFullSync(ctx)
	// Create a new controller.
	c, err := controller.New("triggercsifullsync-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new TriggerCsiFullSync controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource TriggerCsiFullSync.
	err = c.Watch(&source.Kind{Type: &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}},
		&handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("Failed to watch for changes to TriggerCsiFullSync resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileTriggerCsiFullSync implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileTriggerCsiFullSync{}

// ReconcileTriggerCsiFullSync reconciles a TriggerCsiFullSync object.
type ReconcileTriggerCsiFullSync struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver.
	client        client.Client
	scheme        *runtime.Scheme
	clusterFlavor cnstypes.CnsClusterFlavor
	configInfo    *config.ConfigurationInfo
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a TriggerCsiFullSync object and
// makes changes based on the state read and what is in TriggerCsiFullSync.Spec.
// Note:
// The Controller will requeue the Request to be processed again if the returned
// error is non-nil or Result.Requeue is true. Otherwise, upon completion it
// will remove the work from the queue.
func (r *ReconcileTriggerCsiFullSync) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)
	// Fetch the TriggerCsiFullSync instance.
	instance := &triggercsifullsyncv1alpha1.TriggerCsiFullSync{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("TriggerCsiFullSync resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the TriggerCsiFullSync with name: %q. Err: %+v",
			request.Name, err)
		// Error reading the object - return with err.
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

	// Ignore TriggerCsiFullSync instances other than reserved
	// "csifullsync" TriggerCsiFullSync instance.
	if instance.Name != common.TriggerCsiFullSyncCRName {
		msg := fmt.Sprintf("Only %q should be used to trigger full sync and not %q",
			common.TriggerCsiFullSyncCRName, instance.Name)
		log.Error(msg)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{}, nil
	}

	// Ignore any updates on TriggerCsiFullSync instance with TriggerSyncID set
	// to 0 and TriggerSyncID same as LastTriggerSyncID.
	if instance.Spec.TriggerSyncID == 0 || instance.Spec.TriggerSyncID == instance.Status.LastTriggerSyncID {
		return reconcile.Result{}, nil
	}

	// If TriggerSyncID is not one greater than LastTriggerSyncID, raise an event
	// that the trigger sync will be ignored.
	if instance.Spec.TriggerSyncID != instance.Status.LastTriggerSyncID+1 {
		msg := fmt.Sprintf("TriggerSyncID: %d is invalid. TriggerSyncID should be one greater than LastTriggerSyncID.",
			instance.Spec.TriggerSyncID)
		log.Error(msg)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		return reconcile.Result{}, nil
	}

	// If the TriggerCsiFullSync instance is already in progress, update
	// LastTriggerSyncID and raise an event that full sync is already in progress.
	if instance.Status.InProgress && instance.Spec.TriggerSyncID == instance.Status.LastTriggerSyncID+1 {
		// LastTriggerSyncID saves the last TriggerSyncID attempted by the
		// user regardless of success or failure.
		instance.Status.LastTriggerSyncID = instance.Spec.TriggerSyncID
		err = updateTriggerCsiFullSync(ctx, r.client, instance)
		if err != nil {
			recordEvent(ctx, r, instance, v1.EventTypeWarning,
				fmt.Sprintf("Failed to increment LastTriggerSyncID with TriggerSyncID: %d", instance.Spec.TriggerSyncID))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		msg := fmt.Sprintf("A full sync is already in progress. Ignoring this sync with triggersync ID: %d",
			instance.Spec.TriggerSyncID)
		log.Warn(msg)
		recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	log.Infof("Reconciling trigger full sync with triggerSyncID: %d", instance.Spec.TriggerSyncID)
	instance.Status.LastTriggerSyncID = instance.Spec.TriggerSyncID
	instance.Status.InProgress = true
	err = updateTriggerCsiFullSync(ctx, r.client, instance)
	if err != nil {
		recordEvent(ctx, r, instance, v1.EventTypeWarning,
			fmt.Sprintf("Failed to update LastTriggerSyncID and Inprogress for TriggerSyncID: %d",
				instance.Spec.TriggerSyncID))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	startTime := time.Now()
	triggerSyncID := instance.Spec.TriggerSyncID
	var fullSyncErr error
	if r.clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		fullSyncErr = syncer.PvcsiFullSync(ctx, syncer.MetadataSyncer)
	} else {
		fullSyncErr = syncer.CsiFullSync(ctx, syncer.MetadataSyncer)
	}
	err = r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		return reconcile.Result{}, nil
	}
	if fullSyncErr != nil {
		msg := fmt.Sprintf("Full sync failed for triggerSyncID: %d with error: %+v", triggerSyncID, fullSyncErr)
		log.Error(msg)
		setInstanceError(ctx, r, instance, msg, startTime)
	} else {
		msg := fmt.Sprintf("Full sync successful with triggerSyncID: %d", triggerSyncID)
		log.Info(msg)
		setInstanceSuccess(ctx, r, instance, msg, startTime)
	}
	backOffDurationMapMutex.Lock()
	delete(backOffDuration, instance.Name)
	backOffDurationMapMutex.Unlock()
	return reconcile.Result{}, nil
}

// setInstanceError sets error and records an event on the TriggerCsiFullSync
// instance.
func setInstanceError(ctx context.Context, r *ReconcileTriggerCsiFullSync,
	instance *triggercsifullsyncv1alpha1.TriggerCsiFullSync, errMsg string, startTime time.Time) {
	log := logger.GetLogger(ctx)
	instance.Status.LastRunStartTimeStamp = &metav1.Time{Time: startTime}
	instance.Status.LastRunEndTimeStamp = &metav1.Time{Time: time.Now()}
	instance.Status.LastTriggerSyncID = instance.Spec.TriggerSyncID
	instance.Status.InProgress = false
	instance.Status.Error = errMsg
	err := updateTriggerCsiFullSync(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateTriggerCsiFullSync failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeWarning, errMsg)
}

// setInstanceSuccess sets instance to success and records an event on the
// TriggerCsiFullSync instance.
func setInstanceSuccess(ctx context.Context, r *ReconcileTriggerCsiFullSync,
	instance *triggercsifullsyncv1alpha1.TriggerCsiFullSync, msg string, startTime time.Time) {
	log := logger.GetLogger(ctx)
	instance.Status.LastSuccessfulStartTimeStamp = &metav1.Time{Time: startTime}
	instance.Status.LastSuccessfulEndTimeStamp = &metav1.Time{Time: time.Now()}
	instance.Status.LastRunStartTimeStamp = &metav1.Time{Time: startTime}
	instance.Status.LastRunEndTimeStamp = &metav1.Time{Time: time.Now()}
	instance.Status.InProgress = false
	instance.Status.Error = ""
	err := updateTriggerCsiFullSync(ctx, r.client, instance)
	if err != nil {
		log.Errorf("updateTriggerCsiFullSync failed. err: %v", err)
	}
	recordEvent(ctx, r, instance, v1.EventTypeNormal, msg)
}

// recordEvent records the event.
func recordEvent(ctx context.Context, r *ReconcileTriggerCsiFullSync,
	instance *triggercsifullsyncv1alpha1.TriggerCsiFullSync, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	log.Debugf("Event type is %s", eventtype)
	switch eventtype {
	case v1.EventTypeWarning:
		r.recorder.Event(instance, v1.EventTypeWarning, "TriggerCsiFullSyncFailed", msg)
	case v1.EventTypeNormal:
		r.recorder.Event(instance, v1.EventTypeNormal, "TriggerCsiFullSyncSucceeded", msg)
	}
}

// updateTriggerCsiFullSync updates the TriggerCsiFullSync instance in K8S.
func updateTriggerCsiFullSync(ctx context.Context, client client.Client,
	instance *triggercsifullsyncv1alpha1.TriggerCsiFullSync) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		log.Errorf("Failed to update TriggerCsiFullSync instance: %+v. Error: %+v",
			instance, err)
	}
	return err
}

// getMaxWorkerThreadsToReconcileTriggerCsiFullSync returns the maximum number
// of worker threads which can be run to reconcile TriggerCsiFullSync instances.
// If environment variable WORKER_THREADS_TRIGGER_CSI_FULLSYNC is set and valid,
// return the value read from environment variable. Otherwise, use the default
// value.
func getMaxWorkerThreadsToReconcileTriggerCsiFullSync(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForTriggerCsiFullSync
	// Maximum number of worker threads to run.
	if v := os.Getenv("WORKER_THREADS_TRIGGER_CSI_FULLSYNC"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Env variable WORKER_THREADS_TRIGGER_CSI_FULLSYNC %s is less than 1, use the default %d",
					v, defaultMaxWorkerThreadsForTriggerCsiFullSync)
			} else if value > defaultMaxWorkerThreadsForTriggerCsiFullSync {
				log.Warnf("Env variable WORKER_THREADS_TRIGGER_CSI_FULLSYNC %s is greater than %d, use the default %d",
					v, defaultMaxWorkerThreadsForTriggerCsiFullSync, defaultMaxWorkerThreadsForTriggerCsiFullSync)
			} else {
				workerThreads = value
				log.Debugf("Maximum #worker to reconcile TriggerCsiFullSync instances is set to %d",
					workerThreads)
			}
		} else {
			log.Warnf("Env variable WORKER_THREADS_TRIGGER_CSI_FULLSYNC %s is invalid, use the default %d",
				v, defaultMaxWorkerThreadsForTriggerCsiFullSync)
		}
	} else {
		log.Debugf("WORKER_THREADS_TRIGGER_CSI_FULLSYNC is not set. Use the default value %d",
			defaultMaxWorkerThreadsForTriggerCsiFullSync)
	}
	return workerThreads
}
