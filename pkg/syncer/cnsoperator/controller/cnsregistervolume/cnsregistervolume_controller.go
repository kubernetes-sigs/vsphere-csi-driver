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

package cnsregistervolume

import (
	"context"
	"os"
	"strconv"
	"time"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	v1 "k8s.io/api/core/v1"
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
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsregistervolume/v1alpha1"

	volumes "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

const (
	defaultMaxWorkerThreadsForRegisterVolume = 10
)

// backOffDuration is a map of cnsregistervolume name's to the time after which a request
// for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest reconcile
// operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var backOffDuration map[string]time.Duration

// Add creates a new CnsRegisterVolume Controller and adds it to the Manager, ConfigInfo
// and VirtualCenterTypes. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *types.ConfigInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
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
	return &ReconcileCnsRegisterVolume{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsRegisterVolume(ctx)
	// Create a new controller
	c, err := controller.New("cnsregistervolume-controller", mgr, controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("Failed to create new CnsRegisterVolume controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsRegisterVolume
	err = c.Watch(&source.Kind{Type: &cnsregistervolumev1alpha1.CnsRegisterVolume{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("Failed to watch for changes to CnsRegisterVolume resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsRegisterVolume implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsRegisterVolume{}

// ReconcileCnsRegisterVolume reconciles a CnsRegisterVolume object
type ReconcileCnsRegisterVolume struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client        client.Client
	scheme        *runtime.Scheme
	configInfo    *types.ConfigInfo
	volumeManager volumes.Manager
	recorder      record.EventRecorder
}

// Reconcile reads that state of the cluster for a CnsRegisterVolume object and makes changes based on the state read
// and what is in the CnsRegisterVolume.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsRegisterVolume) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	_, log := logger.GetNewContextWithLogger()
	log.Infof("Inside dummy CnsRegister controller")

	return reconcile.Result{}, nil
}

// getMaxWorkerThreadsToReconcileCnsRegisterVolume returns the maximum
// number of worker threads which can be run to reconcile CnsRegisterVolume instances.
// If environment variable WORKER_THREADS_REGISTER_VOLUME is set and valid,
// return the value read from enviroment variable otherwise, use the default value
func getMaxWorkerThreadsToReconcileCnsRegisterVolume(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForRegisterVolume
	if v := os.Getenv("WORKER_THREADS_REGISTER_VOLUME"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_REGISTER_VOLUME %s is less than 1, will use the default value %d", v, defaultMaxWorkerThreadsForRegisterVolume)
			} else if value > defaultMaxWorkerThreadsForRegisterVolume {
				log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_REGISTER_VOLUME %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForRegisterVolume, defaultMaxWorkerThreadsForRegisterVolume)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsRegisterVolume instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable WORKER_THREADS_REGISTER_VOLUME %s is invalid, will use the default value %d", v, defaultMaxWorkerThreadsForRegisterVolume)
		}
	} else {
		log.Debugf("WORKER_THREADS_REGISTER_VOLUME is not set. Picking the default value %d", defaultMaxWorkerThreadsForRegisterVolume)
	}
	return workerThreads
}
