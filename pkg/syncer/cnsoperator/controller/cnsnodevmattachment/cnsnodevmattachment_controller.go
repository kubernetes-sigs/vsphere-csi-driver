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

package cnsnodevmattachment

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsnodevmattachment/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

// Add creates a new CnsNodeVmAttachment Controller and adds it to the Manager, ConfigInfo
// and VirtualCenterTypes. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, configInfo *types.ConfigInfo, vcTypes *types.VirtualCenterTypes) error {
	return add(mgr, newReconciler(mgr, configInfo, vcTypes))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, configInfo *types.ConfigInfo, vcTypes *types.VirtualCenterTypes) reconcile.Reconciler {
	return &ReconcileCnsNodeVmAttachment{client: mgr.GetClient(), scheme: mgr.GetScheme(), configInfo: configInfo, vcTypes: vcTypes}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("cnsnodevmattachment-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		klog.Errorf("Failed to create new CnsNodeVmAttachment controller with error: %+v", err)
		return err
	}

	// Watch for changes to primary resource CnsNodeVmAttachment
	err = c.Watch(&source.Kind{Type: &cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		klog.Errorf("Failed to watch for changes to CnsNodeVmAttachment resource with error: %+v", err)
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileCnsNodeVmAttachment implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileCnsNodeVmAttachment{}

// ReconcileCnsNodeVmAttachment reconciles a CnsNodeVmAttachment object
type ReconcileCnsNodeVmAttachment struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client     client.Client
	scheme     *runtime.Scheme
	configInfo *types.ConfigInfo
	vcTypes    *types.VirtualCenterTypes
}

// Reconcile reads that state of the cluster for a CnsNodeVmAttachment object and makes changes based on the state read
// and what is in the CnsNodeVmAttachment.Spec
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileCnsNodeVmAttachment) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	klog.V(2).Infof("Reconciling CnsNodeVmAttachment with Request.Name: %s and Request.Namespace: %s", request.Namespace, request.Name)

	// TODO: Implement the controller logic for CnsNodeVmAttachment objects
	return reconcile.Result{}, nil
}
