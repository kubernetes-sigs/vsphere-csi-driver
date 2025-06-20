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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// PVCObjectToProcess defines object to be processd on receiving event of PersistentVolumeclaim
type PVCObjectToProcess struct {
	pvc *corev1.PersistentVolumeClaim
}

// PersistentVolumeClaimReconciler is the interface for PersistentVolumeclaim reconciler.
type PersistentVolumeClaimReconciler interface {
	// Run starts the reconciler.
	Run(ctx context.Context, workers int)
}

type persistentVolumeClaimReconciler struct {
	// metadataSyncer informer to get FSS information.
	metadataSyncerInformer *metadataSyncInformer
	// PersistentVolumeclaim queue to add/delete PersistentVolumeclaim CRs.
	pvcOpsQueue workqueue.TypedRateLimitingInterface[any]
	// PersistentVolumeclaim Synced.
	pvcSynced cache.InformerSynced
}

// newPersistentVolumeClaimReconciler returns a PersistentVolumeclaimReconciler.
func newPersistentVolumeClaimReconciler(
	ctx context.Context,
	metadataSyncerInformer *metadataSyncInformer,
	informerFactory informers.SharedInformerFactory,
	pvcRateLimiter workqueue.TypedRateLimiter[any],
	stopCh <-chan struct{}) (*persistentVolumeClaimReconciler, error) {

	log := logger.GetLogger(ctx)

	// Return informer from shared dynamic informer factory for input resource.
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	pvcOpsQueue := workqueue.NewTypedRateLimitingQueueWithConfig(
		pvcRateLimiter, workqueue.TypedRateLimitingQueueConfig[any]{
			Name: "persistent-volume-claim-ops",
		})

	rc := &persistentVolumeClaimReconciler{
		metadataSyncerInformer: metadataSyncerInformer,
		pvcSynced:              pvcInformer.Informer().HasSynced,
		pvcOpsQueue:            pvcOpsQueue,
	}

	_, err := pvcInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			DeleteFunc: rc.delPVC,
		})
	if err != nil {
		return nil, logger.LogNewErrorf(log, "newPersistentVolumeClaimReconciler: failed to add event handler"+
			" on PersistentVolumeclaim informer. Error: %v", err)
	}

	// Start PersistentVolumeclaim Informer.
	informerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.pvcSynced) {
		return nil, fmt.Errorf("newPersistentVolumeClaimReconciler: cannot sync PersistentVolumeclaim cache")
	}

	return rc, nil
}

func (rc *persistentVolumeClaimReconciler) delPVC(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	pvcObj, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok || pvcObj == nil {
		log.Errorf("delPVC: failed to cast object %+v to %s", obj, ResourceKindPVC)
		return
	}
	objToProcess := &PVCObjectToProcess{
		pvc: pvcObj,
	}
	rc.pvcOpsQueue.Add(objToProcess)
}

// Run starts the reconciler.
func (rc *persistentVolumeClaimReconciler) Run(
	ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer rc.pvcOpsQueue.ShutDown()

	log.Infof("Starting PersistentVolumeclaim reconciler")
	defer log.Infof("Shutting down PersistentVolumeclaim reconciler after draining")

	stopCh := ctx.Done()

	for i := 0; i < workers; i++ {
		go wait.Until(func() { rc.syncPersistentVolumeClaims(ctx) }, 0, stopCh)
	}
	<-stopCh
}

// syncPersistentVolumeClaims is the main worker to cleanup CNS finalizer from given PersistentVolumeClaim,
// if associated namespace is being deleted and TKG cluster is already deleted.
func (rc *persistentVolumeClaimReconciler) syncPersistentVolumeClaims(ctx context.Context) {
	obj, quit := rc.pvcOpsQueue.Get()
	if quit {
		return
	}
	defer rc.pvcOpsQueue.Done(obj)
	objToProcess := obj.(*PVCObjectToProcess)
	pvcObj := objToProcess.pvc
	if err := rc.syncPersistentVolumeClaimDelCase(ctx, pvcObj); err != nil {
		// Put PersistentVolumeClaim back to the queue so that we can retry later.
		rc.pvcOpsQueue.AddRateLimited(obj)
	} else {
		rc.pvcOpsQueue.Forget(obj)
	}

}

// syncPersistentVolumeClaimDelCase processes one PersistentVolumeClaim CR deleted from cluster
// and removes CNS finalizer present on that PVC if associated namespace is being deleted
// and TKG cluster is already deleted.
func (rc *persistentVolumeClaimReconciler) syncPersistentVolumeClaimDelCase(ctx context.Context,
	pvc *corev1.PersistentVolumeClaim) error {
	log := logger.GetLogger(ctx)
	log.Infof("syncPersistentVolumeClaimDelCase: Started PersistentVolumeClaim processing %s/%s",
		pvc.Namespace, pvc.Name)
	// Create K8S client and process PersistentVolumeClaim
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("syncPersistentVolumeClaimDelCase: Failed to get kubernetes client. Err: %+v", err)
		return err
	}
	RemoveCNSFinalizerFromPVCIfTKGClusterDeleted(ctx, k8sClient, pvc, cnsoperatortypes.CNSVolumeFinalizer)
	return nil
}
