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

package syncer

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	storagepolicyusagev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// ObjectToProcess defines object to be processd for addition/deletion event of StoragePolicyQuota
type ObjectToProcess struct {
	policyQuota *storagepolicyusagev1alpha1.StoragePolicyQuota
	isDeleted   bool
}

// StoragePolicyQuotaReconciler is the interface for StoragePolicyQuota reconciler.
type StoragePolicyQuotaReconciler interface {
	// Run starts the reconciler.
	Run(ctx context.Context, workers int)
}

type storagePolicyQuotaReconciler struct {
	// metadataSyncer informer to get FSS information.
	metadataSyncerInformer *metadataSyncInformer
	// StoragePolicyQuota queue to add/delete StoragePolicyUsage CRs.
	svcQuotaOpsQueue workqueue.RateLimitingInterface
	// StoragePolicyQuota Synced.
	svcQuotaSynced cache.InformerSynced
}

// newStoragePolicyQuotaReconciler returns a StoragePolicyQuotaReconciler.
func newStoragePolicyQuotaReconciler(
	ctx context.Context,
	metadataSyncerInformer *metadataSyncInformer,
	svcStoragePolicyQuotaRateLimiter workqueue.RateLimiter,
	stopCh <-chan struct{}) (*storagePolicyQuotaReconciler, error) {

	log := logger.GetLogger(ctx)
	// Create an informer for StoragePolicyQuota instances.
	k8sConfig, err := k8s.GetKubeConfig(ctx)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "newStoragePolicyQuotaReconciler: failed to get kubeconfig with error: %v", err)
	}
	dynamicInformerFactory, err := k8s.NewDynamicInformerFactory(ctx, k8sConfig, metav1.NamespaceAll, true)
	if err != nil {
		log.Errorf("newStoragePolicyQuotaReconciler: could not retrieve dynamic informer factory. Error: %+v", err)
		return nil, err
	}
	// Return informer from shared dynamic informer factory for input resource.
	gvr := schema.GroupVersionResource{Group: cnsoperatorv1alpha1.GroupName, Version: cnsoperatorv1alpha1.Version,
		Resource: cnsoperatorv1alpha1.CnsStoragePolicyQuotaPlural}
	storagePolicyQuotaInformer := dynamicInformerFactory.ForResource(gvr)
	svcQuotaOpsQueue := workqueue.NewRateLimitingQueueWithConfig(
		svcStoragePolicyQuotaRateLimiter, workqueue.RateLimitingQueueConfig{
			Name: "storage-policy-quota-ops",
		})

	rc := &storagePolicyQuotaReconciler{
		metadataSyncerInformer: metadataSyncerInformer,
		svcQuotaSynced:         storagePolicyQuotaInformer.Informer().HasSynced,
		svcQuotaOpsQueue:       svcQuotaOpsQueue,
	}

	_, err = storagePolicyQuotaInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    rc.addStoragePolicyQuota,
			DeleteFunc: rc.delStoragePolicyQuota,
		})
	if err != nil {
		return nil, logger.LogNewErrorf(log, "newStoragePolicyQuotaReconciler: failed to add event handler on "+
			"StoragePolicyQuota informer. Error: %v", err)
	}

	// Start StoragePolicyQuota Informer.
	dynamicInformerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.svcQuotaSynced) {
		return nil, fmt.Errorf("newStoragePolicyQuotaReconciler: cannot sync StoragePolicyQuota cache")
	}

	return rc, nil
}

func (rc *storagePolicyQuotaReconciler) addStoragePolicyQuota(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	policyQuotaObj := &storagepolicyusagev1alpha1.StoragePolicyQuota{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object,
		policyQuotaObj)
	if err != nil {
		log.Errorf("addStoragePolicyQuota: failed to cast object %+v to %s. Error: %v", obj,
			cnsoperatorv1alpha1.CnsStoragePolicyQuotaSingular, err)
		return
	}
	objToProcess := &ObjectToProcess{
		policyQuota: policyQuotaObj,
		isDeleted:   false,
	}
	rc.svcQuotaOpsQueue.Add(objToProcess)
}

func (rc *storagePolicyQuotaReconciler) delStoragePolicyQuota(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	policyQuotaObj := &storagepolicyusagev1alpha1.StoragePolicyQuota{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.(*unstructured.Unstructured).Object,
		policyQuotaObj)
	if err != nil {
		log.Errorf("delStoragePolicyQuota: failed to cast object %+v to %s. Error: %v", obj,
			cnsoperatorv1alpha1.CnsStoragePolicyQuotaSingular, err)
		return
	}
	objToProcess := &ObjectToProcess{
		policyQuota: policyQuotaObj,
		isDeleted:   true,
	}
	rc.svcQuotaOpsQueue.Add(objToProcess)
}

// Run starts the reconciler.
func (rc *storagePolicyQuotaReconciler) Run(
	ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer rc.svcQuotaOpsQueue.ShutDown()

	log.Infof("Starting StoragePolicyQuota reconciler")
	defer log.Infof("Shutting down StoragePolicyQuota reconciler after draining")

	stopCh := ctx.Done()

	for i := 0; i < workers; i++ {
		go wait.Until(func() { rc.syncStoragePolicyQuotas(ctx) }, 0, stopCh)
	}
	<-stopCh
}

// syncStoragePolicyQuotas is the main worker to add/delete StoragePolicyUsage for given StoragePolicyQuota.
func (rc *storagePolicyQuotaReconciler) syncStoragePolicyQuotas(ctx context.Context) {
	obj, quit := rc.svcQuotaOpsQueue.Get()
	if quit {
		return
	}
	defer rc.svcQuotaOpsQueue.Done(obj)
	objToProcess := obj.(*ObjectToProcess)
	policyQuotaObj := objToProcess.policyQuota
	// If object has been deleted, process deletion of corresponding StoragePolicyUsage(s).
	// Otherwise process the object for addition of corresponding StoragePolicyUsage(s).
	if !objToProcess.isDeleted {
		if err := rc.syncStoragePolicyQuotaAddCase(ctx, policyQuotaObj); err != nil {
			// Put StoragePolicyQuota back to the queue so that we can retry later.
			rc.svcQuotaOpsQueue.AddRateLimited(obj)
		} else {
			rc.svcQuotaOpsQueue.Forget(obj)
		}
	} else {
		if err := rc.syncStoragePolicyQuotaDelCase(ctx, policyQuotaObj); err != nil {
			// Put StoragePolicyQuota back to the queue so that we can retry later.
			rc.svcQuotaOpsQueue.AddRateLimited(obj)
		} else {
			rc.svcQuotaOpsQueue.Forget(obj)
		}
	}
}

// syncStoragePolicyQuotaAddCase processes one StoragePolicyQuota CR added to cluster
// and creates corresponding StoragePolicyUsage CR(s) for associated storage policy
// and namespace.
func (rc *storagePolicyQuotaReconciler) syncStoragePolicyQuotaAddCase(ctx context.Context,
	storagePolicyQuota *storagepolicyusagev1alpha1.StoragePolicyQuota) error {
	log := logger.GetLogger(ctx)
	log.Infof("syncStoragePolicyQuotaAddCase: Started StoragePolicyQuota processing %s/%s",
		storagePolicyQuota.Namespace, storagePolicyQuota.Name)

	_, err := getOrCreateStoragePolicyUsageCR(ctx, storagePolicyQuota.Spec.StoragePolicyId,
		storagePolicyQuota.Namespace, rc.metadataSyncerInformer)
	if err != nil {
		log.Errorf("syncStoragePolicyQuotaAddCase: Could not create StoragePolicyUsage CR for %s/%s err: %v",
			storagePolicyQuota.Namespace, storagePolicyQuota.Name, err)
		return err
	}

	return nil
}

// syncStoragePolicyQuotaAddCase processes one StoragePolicyQuota CR deleted from cluster
// and deletes corresponding StoragePolicyUsage CR(s) for associated storage policy
// and namespace.
func (rc *storagePolicyQuotaReconciler) syncStoragePolicyQuotaDelCase(ctx context.Context,
	storagePolicyQuota *storagepolicyusagev1alpha1.StoragePolicyQuota) error {
	log := logger.GetLogger(ctx)
	log.Debugf("syncStoragePolicyQuotaDelCase: Started StoragePolicyQuota processing %s/%s",
		storagePolicyQuota.Namespace, storagePolicyQuota.Name)

	err := deleteStoragePolicyUsageCR(ctx, storagePolicyQuota.Spec.StoragePolicyId,
		storagePolicyQuota.Namespace, rc.metadataSyncerInformer)
	if err != nil {
		log.Errorf("syncStoragePolicyQuotaDelCase: Could not delete StoragePolicyUsage CR for %s/%s err: %v",
			storagePolicyQuota.Namespace, storagePolicyQuota.Name, err)
		return err
	}

	return nil
}
