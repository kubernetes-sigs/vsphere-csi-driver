/*
Copyright 2026 The Kubernetes Authors.

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

package cbtconfig

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	maxReconcileWorkerThreads  = 1
	reconcileErrorRequeueAfter = 5 * time.Minute
)

// cbtStatusState decodes the CBT intent from a CBTConfigStatus.
//
// active carries the resolved active/inactive intent; it is only meaningful
// when configured is true.
// configured is true when the operator has written status.state (non-nil);
// both Active and Inactive values are actionable. configured is false when the
// operator has not yet reconciled the field, and the caller should skip acting.
func cbtStatusState(st *cbtconfigv1alpha1.CBTConfigStatus) (active, configured bool) {
	if st == nil || st.State == nil {
		return false, false
	}
	return *st.State == cbtconfigv1alpha1.CBTStateActive, true
}

// Add creates a new CBTConfig controller and adds it to the Manager.
// pvLister, pvcLister and vaLister come from the singleton InformerManager shared with the
// metadata syncer and k8sorchestrator, so no second copy of those informers is started here.
// kubeClient is the shared clientset used by the CBT label-sync phase.
func Add(mgr manager.Manager, kubeClient clientset.Interface, volumeManager volume.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister) error {
	rec := newReconciler(mgr, kubeClient, volumeManager, pvLister, pvcLister, vaLister)
	return add(mgr, rec)
}

func newReconciler(mgr manager.Manager, kubeClient clientset.Interface, volumeManager volume.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister) *ReconcileCBTConfig {
	return &ReconcileCBTConfig{
		client:    mgr.GetClient(),
		scheme:    mgr.GetScheme(),
		cbtSyncer: syncer.NewCBTSyncer(kubeClient, volumeManager, pvLister, pvcLister, vaLister),
	}
}

func add(mgr manager.Manager, r *ReconcileCBTConfig) error {
	_, log := logger.GetNewContextWithLogger()
	c, err := controller.New("cbtconfig-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxReconcileWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new CBTConfig controller with error: %+v", err)
		return err
	}

	pred := predicate.TypedFuncs[*cbtconfigv1alpha1.CBTConfig]{
		// Reconcile whenever the operator has written status.state (non-nil).
		// An Inactive value is also actionable — it means CBT must be cleared.
		CreateFunc: func(e event.TypedCreateEvent[*cbtconfigv1alpha1.CBTConfig]) bool {
			_, configured := cbtStatusState(e.Object.Status)
			return configured
		},
		// Enqueue only when the effective active/inactive intent changes — either
		// status.state transitions from nil to set, or its value flips.
		// This avoids reconciling on every unrelated status or metadata update.
		UpdateFunc: func(e event.TypedUpdateEvent[*cbtconfigv1alpha1.CBTConfig]) bool {
			oldActive, oldConfigured := cbtStatusState(e.ObjectOld.Status)
			newActive, newConfigured := cbtStatusState(e.ObjectNew.Status)
			return oldConfigured != newConfigured || oldActive != newActive
		},
		DeleteFunc: func(e event.TypedDeleteEvent[*cbtconfigv1alpha1.CBTConfig]) bool {
			return false
		},
	}

	err = c.Watch(source.Kind(
		mgr.GetCache(),
		&cbtconfigv1alpha1.CBTConfig{},
		&handler.TypedEnqueueRequestForObject[*cbtconfigv1alpha1.CBTConfig]{}, pred))
	if err != nil {
		log.Errorf("failed to watch for changes to CBTConfig resource with error: %+v", err)
		return err
	}
	return nil
}

// ReconcileCBTConfig reconciles a CBTConfig object.
type ReconcileCBTConfig struct {
	client client.Client
	scheme *runtime.Scheme
	// cbtSyncer holds the Kubernetes and CNS dependencies for the CBT reconcile pipeline.
	cbtSyncer *syncer.CBTSyncer
}

var _ reconcile.Reconciler = &ReconcileCBTConfig{}

// Reconcile reads that state of the cluster for a CBTConfig object and makes changes.
func (r *ReconcileCBTConfig) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	log.Infof("Received Reconcile for CBTConfig request: %q", request.NamespacedName)

	instance := &cbtconfigv1alpha1.CBTConfig{}
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CBTConfig resource not found. Ignoring since object must be deleted.")
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading the CBTConfig with name: %q. Err: %+v", request.Name, err)
		return reconcile.Result{}, err
	}

	if instance.DeletionTimestamp != nil {
		log.Debugf("CBTConfig %q is being deleted; skipping reconcile", request.NamespacedName)
		return reconcile.Result{}, nil
	}

	active, configured := cbtStatusState(instance.Status)
	if !configured {
		log.Debugf("CBTConfig %q status.state not yet set; skipping reconcile", request.NamespacedName)
		return reconcile.Result{}, nil
	}
	if err := r.cbtSyncer.ReconcileCBTForNamespace(ctx, instance.Namespace, active); err != nil {
		log.Errorf("CBTConfig reconcile failed for namespace %q: %+v", instance.Namespace, err)
		return reconcile.Result{RequeueAfter: reconcileErrorRequeueAfter}, nil
	}
	log.Infof("Successfully reconciled CBTConfig %q (active=%t)", request.NamespacedName, active)
	return reconcile.Result{}, nil
}
