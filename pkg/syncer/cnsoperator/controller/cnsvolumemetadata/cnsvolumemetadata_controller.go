/*
Copyright 2019 The Kubernetes Authors.

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

package cnsvolumemetadata

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cnsv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
)

// Add creates a new CnsVolumeMetadata Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileCnsVolumeMetadata{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("cnsvolumemetadata-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource CnsVolumeMetadata
	err = c.Watch(&source.Kind{Type: &cnsv1alpha1.CnsVolumeMetadata{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileCnsVolumeMetadata implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsVolumeMetadata{}

// ReconcileCnsVolumeMetadata reconciles a CnsVolumeMetadata object
type ReconcileCnsVolumeMetadata struct {
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a CnsVolumeMetadata object and makes changes on CNS
// based on the state read in the CnsVolumeMetadata.Spec
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsVolumeMetadata) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	klog.V(2).Infof("Unimplemented Reconcile function for CnsVolumeMetadata CRD")
	// TODO: Implement the reconcile logic for CnsVolumeMetadata CRDs
	return reconcile.Result{}, nil
}

