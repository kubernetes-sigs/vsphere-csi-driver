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

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapv1informer "github.com/kubernetes-csi/external-snapshotter/client/v8/informers/externalversions"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// SnapObjectToProcess defines object to be processd on receiving an event of VolumeSnapshot
type SnapObjectToProcess struct {
	snap *snapv1.VolumeSnapshot
}

// VolumeSnapshotReconciler is the interface for VolumeSnapshot reconciler.
type VolumeSnapshotReconciler interface {
	// Run starts the reconciler.
	Run(ctx context.Context, workers int)
}

type volumeSnapshotReconciler struct {
	// metadataSyncer informer to get FSS information.
	metadataSyncerInformer *metadataSyncInformer
	// VolumeSnapshot queue to add/delete VolumeSnapshot CRs.
	snapOpsQueue workqueue.TypedRateLimitingInterface[any]
	// VolumeSnapshot Synced.
	snapSynced cache.InformerSynced
}

// newVolumeSnapshotReconciler returns a VolumeSnapshotReconciler.
func newVolumeSnapshotReconciler(
	ctx context.Context,
	metadataSyncerInformer *metadataSyncInformer,
	informerFactory snapv1informer.SharedInformerFactory,
	snapshotRateLimiter workqueue.TypedRateLimiter[any],
	stopCh <-chan struct{}) (*volumeSnapshotReconciler, error) {

	log := logger.GetLogger(ctx)
	log.Infof("Entered newVolumeSnapshotReconciler")
	snapInformer := informerFactory.Snapshot().V1().VolumeSnapshots()
	snapOpsQueue := workqueue.NewTypedRateLimitingQueueWithConfig(
		snapshotRateLimiter, workqueue.TypedRateLimitingQueueConfig[any]{
			Name: "volume-snapshot-ops",
		})

	rc := &volumeSnapshotReconciler{
		metadataSyncerInformer: metadataSyncerInformer,
		snapSynced:             snapInformer.Informer().HasSynced,
		snapOpsQueue:           snapOpsQueue,
	}

	_, err := snapInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			DeleteFunc: rc.delVolumeSnapshot,
		})
	if err != nil {
		log.Infof("Entered error case for newVolumeSnapshotReconciler")
		return nil, logger.LogNewErrorf(log, "newVolumeSnapshotReconciler: failed to add event handler on "+
			"VolumeSnapshot informer. Error: %v", err)
	}

	// Start VolumeSnapshot Informer.
	informerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.snapSynced) {
		return nil, fmt.Errorf("newVolumeSnapshotReconciler: cannot sync VolumeSnapshot cache")
	}
	log.Infof("Leaving newVolumeSnapshotReconciler")
	return rc, nil
}

func (rc *volumeSnapshotReconciler) delVolumeSnapshot(obj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	log.Infof("Entered delVolumeSnapshot")
	snapObj, ok := obj.(*snapv1.VolumeSnapshot)
	if !ok || snapObj == nil {
		log.Errorf("delVolumeSnapshot: failed to cast object %+v to %s", obj, ResourceKindSnapshot)
		return
	}
	objToProcess := &SnapObjectToProcess{
		snap: snapObj,
	}
	rc.snapOpsQueue.Add(objToProcess)
}

// Run starts the reconciler.
func (rc *volumeSnapshotReconciler) Run(
	ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer rc.snapOpsQueue.ShutDown()

	log.Infof("Starting VolumeSnapshot reconciler")
	defer log.Infof("Shutting down VolumeSnapshot reconciler after draining")

	stopCh := ctx.Done()

	for i := 0; i < workers; i++ {
		go wait.Until(func() { rc.syncVolumeSnapshots(ctx) }, 0, stopCh)
	}
	<-stopCh
}

// syncVolumeSnapshots is the main worker to delete CNS finalizer from given VolumeSnapshot.
func (rc *volumeSnapshotReconciler) syncVolumeSnapshots(ctx context.Context) {
	obj, quit := rc.snapOpsQueue.Get()
	if quit {
		return
	}
	defer rc.snapOpsQueue.Done(obj)
	objToProcess := obj.(*SnapObjectToProcess)
	snapObj := objToProcess.snap
	// Process removal of CNS finalizer from given VolumeSnapshot, if associated namespace is getting deleted
	// and TKG cluster is already deleted.
	if err := rc.syncVolumeSnapshotDelCase(ctx, snapObj); err != nil {
		// Put VolumeSnapshot back to the queue so that we can retry later.
		rc.snapOpsQueue.AddRateLimited(obj)
	} else {
		rc.snapOpsQueue.Forget(obj)
	}

}

// syncVolumeSnapshotDelCase processes one VolumeSnapshot CR deleted from cluster
// and removes CNS finalizer from given VolumeSnapshot if associated namespace is getting deleted
// and TKG cluster is already deleted.
func (rc *volumeSnapshotReconciler) syncVolumeSnapshotDelCase(ctx context.Context,
	volumeSnapshot *snapv1.VolumeSnapshot) error {
	log := logger.GetLogger(ctx)
	log.Infof("syncVolumeSnapshotDelCase: Started VolumeSnapshot processing %s/%s",
		volumeSnapshot.Namespace, volumeSnapshot.Name)
	// Create snapshotter client and process VolumeSnapshot
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("syncVolumeSnapshotDelCase: failed to get snapshotterClient. Err: %v", err)
		return err
	}
	RemoveCNSFinalizerFromSnapIfTKGClusterDeleted(ctx, snapshotterClient, volumeSnapshot,
		cnsoperatortypes.CNSSnapshotFinalizer)

	return nil
}
