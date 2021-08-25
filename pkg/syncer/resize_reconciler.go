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

package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

// ResizeReconciler is the interface of resizeReconciler.
type ResizeReconciler interface {
	// Run starts the reconciler.
	Run(workers int, ctx context.Context)
}

type resizeReconciler struct {
	// Tanzu Kubernetes Grid KubeClient.
	tkgClient kubernetes.Interface
	// Supervisor Cluster KubeClient.
	supervisorClient kubernetes.Interface
	// Supervisor Cluster namespace.
	supervisorNamespace string
	// Tanzu Kubernetes Grid claim queue.
	claimQueue workqueue.RateLimitingInterface

	// Tanzu Kubernetes Grid PVC Lister.
	pvcLister corelisters.PersistentVolumeClaimLister
	// Tanzu Kubernetes Grid PVC Synced.
	pvcSynced cache.InformerSynced
	// Tanzu Kubernetes Grid PV Lister.
	pvLister corelisters.PersistentVolumeLister
	// Tanzu Kubernetes Grid PV Synced.
	pvSynced cache.InformerSynced
}

var (
	knownResizeConditions = map[v1.PersistentVolumeClaimConditionType]bool{
		v1.PersistentVolumeClaimFileSystemResizePending: true,
		v1.PersistentVolumeClaimResizing:                true,
	}
)

type resizeProcessStatus struct {
	// condition reprensents the current resize condition of PVC.
	condition v1.PersistentVolumeClaimCondition
	// processed represents whether this PVC is processed.
	processed bool
}

// newResizeReconciler returns a resizeReconciler.
func newResizeReconciler(
	// Tanzu Kubernetes Grid KubeClient.
	tkgClient kubernetes.Interface,
	// Supervisor Cluster KubeClient.
	supervisorClient kubernetes.Interface,
	// Supervisor Cluster Namespace.
	supervisorNamespace string,
	resyncPeriod time.Duration,
	informerFactory informers.SharedInformerFactory,
	pvcRateLimitter workqueue.RateLimiter,
	stopCh <-chan struct{}) (*resizeReconciler, error) {
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims()
	pvInformer := informerFactory.Core().V1().PersistentVolumes()
	claimQueue := workqueue.NewNamedRateLimitingQueue(pvcRateLimitter, "resize-pvc")

	rc := &resizeReconciler{
		tkgClient:           tkgClient,
		supervisorClient:    supervisorClient,
		supervisorNamespace: supervisorNamespace,
		pvcLister:           pvcInformer.Lister(),
		pvcSynced:           pvcInformer.Informer().HasSynced,
		pvLister:            pvInformer.Lister(),
		pvSynced:            pvInformer.Informer().HasSynced,
		claimQueue:          claimQueue,
	}
	// TODO: Need to figure out how to handle the scenario that
	// FileSystemResizePending is not removed from SV PVC  when syncer is down
	// and FileSystemResizePending was removed from a TKG PVC.
	// https://github.com/kubernetes-sigs/vsphere-csi-driver/issues/591
	pvcInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: rc.updatePVC,
	}, resyncPeriod)

	informerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.pvcSynced, rc.pvSynced) {
		return nil, fmt.Errorf("cannot sync pv/pvc caches")
	}
	return rc, nil

}

func (rc *resizeReconciler) updatePVC(oldObj, newObj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	oldPVC, ok := oldObj.(*v1.PersistentVolumeClaim)
	if !ok || oldPVC == nil {
		return
	}

	newPVC, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok || newPVC == nil {
		return
	}

	newPVCSize := newPVC.Status.Capacity[v1.ResourceStorage]
	oldPVCSize := oldPVC.Status.Capacity[v1.ResourceStorage]

	if newPVCSize.Value() == 0 || oldPVCSize.Value() == 0 {
		return
	}

	// Add tkgPVC to the claim queue only when the new size is bigger or oldPVC
	// has FileSystemResizePending condition, newPVC does not have.
	if newPVCSize.Cmp(oldPVCSize) > 0 || (checkFileSystemPendingOnPVC(oldPVC) && !checkFileSystemPendingOnPVC(newPVC)) {
		objKey, err := getPVCKey(ctx, newObj)
		if err != nil {
			return
		}
		log.Infof("Add new PVC %s to the claim queue", newPVC.Name)
		log.Debugf("Detect PVC size/conditions change, old PVC %+v, new PVC %+v", oldPVC, newPVC)
		rc.claimQueue.Add(objKey)
	}
}

// Run starts the reconciler.
func (rc *resizeReconciler) Run(ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer rc.claimQueue.ShutDown()

	log.Info("Resize reconciler: Start")
	defer log.Info("Resize reconciler: End")

	stopCh := ctx.Done()

	for i := 0; i < workers; i++ {
		go wait.Until(func() { rc.syncPVCs(ctx) }, 0, stopCh)
	}

	<-stopCh

}

// syncPVCs is the main worker.
func (rc *resizeReconciler) syncPVCs(ctx context.Context) {
	key, quit := rc.claimQueue.Get()
	if quit {
		return
	}
	defer rc.claimQueue.Done(key)

	if err := rc.syncPVC(ctx, key.(string)); err != nil {
		rc.claimQueue.AddRateLimited(key)
	} else {
		rc.claimQueue.Forget(key)
	}
}

func (rc *resizeReconciler) syncPVC(ctx context.Context, key string) error {
	_, log := logger.GetNewContextWithLogger()
	log.Infof("Started PVC processing %q in the Tanzu Kubernetes Grid ", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		log.Errorf("Split meta namespace key of pvc %s failed: %+v", key, err)
		return err
	}

	tkgPVC, err := rc.pvcLister.PersistentVolumeClaims(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Infof("PVC %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		log.Errorf("Get PVC %s/%s failed: %+v", namespace, name, err)
		return err
	}

	tkgPV, err := rc.pvLister.Get(tkgPVC.Spec.VolumeName)
	if err != nil {
		log.Errorf("Could not get the volume for pvc %s", tkgPVC.Name)
		return err
	}

	// Get corresponding PVC from the Supervisor Cluster given the pv in the
	// Tanzu Kubernetes Grid.
	svcPVC, err := rc.supervisorClient.CoreV1().PersistentVolumeClaims(rc.supervisorNamespace).Get(
		ctx, tkgPV.Spec.CSI.VolumeHandle, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Error get supervisor cluster pvc %s from api server in the namespace %s: %v",
			tkgPV.Spec.CSI.VolumeHandle, rc.supervisorNamespace, err)
		return err
	}

	tkgPvcSize := tkgPVC.Status.Capacity[v1.ResourceStorage]
	svcPvcSize := svcPVC.Status.Capacity[v1.ResourceStorage]

	// Update pvc size and conditions in supervisor cluster when the size is
	// different from pvc in tkg or pvc is in FileSystemResizePending condition
	// in svc but not in tkg.

	svcPvcClone := svcPVC.DeepCopy()
	updatePVC := false

	if tkgPvcSize.Cmp(svcPvcSize) > 0 {
		svcPvcClone.Status.Capacity[v1.ResourceStorage] = tkgPvcSize
		updatePVC = true
	}
	if !checkFileSystemPendingOnPVC(tkgPVC) && checkFileSystemPendingOnPVC(svcPVC) {
		svcPvcClone = mergeResizeConditionOnPVC(svcPvcClone, []v1.PersistentVolumeClaimCondition{})
		updatePVC = true
	}

	if updatePVC {
		svcUpdatedPVC, err := patchPVCStatus(ctx, svcPVC, svcPvcClone, rc.supervisorClient)
		if err != nil {
			log.Errorf("cannot update Supervisor Cluster PVC  [%s] in namespace [%s]: [%v]",
				svcUpdatedPVC.Name, rc.supervisorNamespace, err)
			return err
		}
		log.Infof("Updated Supervisor Cluster PVC %+v in namespace [%s]", svcUpdatedPVC, rc.supervisorNamespace)
	}
	return nil
}

// patchPVCStatus patch the old pvc using new pvc's status.
func patchPVCStatus(ctx context.Context,
	oldPVC *v1.PersistentVolumeClaim,
	newPVC *v1.PersistentVolumeClaim,
	supervisorClient kubernetes.Interface) (*v1.PersistentVolumeClaim, error) {
	patchBytes, err := createPVCPatch(oldPVC, newPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to patch supervisor cluster PVC %q in namespace %s: %v",
			oldPVC.Name, oldPVC.Namespace, err)
	}

	updatedClaim, updateErr := supervisorClient.CoreV1().PersistentVolumeClaims(oldPVC.Namespace).
		Patch(ctx, oldPVC.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if updateErr != nil {
		return nil, fmt.Errorf("failed to supervisor cluster patch PVC %q in namespace %s: %v",
			oldPVC.Name, oldPVC.Namespace, updateErr)
	}
	return updatedClaim, nil
}

func createPVCPatch(
	oldPVC *v1.PersistentVolumeClaim,
	newPVC *v1.PersistentVolumeClaim) ([]byte, error) {
	oldData, err := json.Marshal(oldPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal old data: %v", err)
	}

	newData, err := json.Marshal(newPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new data: %v", err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, oldPVC)
	if err != nil {
		return nil, fmt.Errorf("failed to create 2 way merge patch: %v", err)
	}

	patchBytes, err = addResourceVersion(patchBytes, oldPVC.ResourceVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to add resource version: %v", err)
	}

	return patchBytes, nil
}

func addResourceVersion(patchBytes []byte, resourceVersion string) ([]byte, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patchBytes, &patchMap)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling patch: %v", err)
	}
	u := unstructured.Unstructured{Object: patchMap}
	a, err := meta.Accessor(&u)
	if err != nil {
		return nil, fmt.Errorf("error creating accessor: %v", err)
	}
	a.SetResourceVersion(resourceVersion)
	versionBytes, err := json.Marshal(patchMap)
	if err != nil {
		return nil, fmt.Errorf("error marshalling json patch: %v", err)
	}
	return versionBytes, nil
}

// mergeResizeConditionOnPVC updates pvc with requested resize conditions
// leaving other conditions untouched.
func mergeResizeConditionOnPVC(
	pvc *v1.PersistentVolumeClaim,
	resizeConditions []v1.PersistentVolumeClaimCondition) *v1.PersistentVolumeClaim {
	resizeConditionMap := map[v1.PersistentVolumeClaimConditionType]*resizeProcessStatus{}

	for _, condition := range resizeConditions {
		resizeConditionMap[condition.Type] = &resizeProcessStatus{condition, false}
	}

	oldConditions := pvc.Status.Conditions
	newConditions := []v1.PersistentVolumeClaimCondition{}
	for _, condition := range oldConditions {
		// If Condition is of not resize type, we keep it.
		if _, ok := knownResizeConditions[condition.Type]; !ok {
			newConditions = append(newConditions, condition)
			continue
		}

		if newCondition, ok := resizeConditionMap[condition.Type]; ok {
			if newCondition.condition.Status != condition.Status {
				newConditions = append(newConditions, newCondition.condition)
			} else {
				newConditions = append(newConditions, condition)
			}
			newCondition.processed = true
		}
	}

	// Append all unprocessed conditions.
	for _, newCondition := range resizeConditionMap {
		if !newCondition.processed {
			newConditions = append(newConditions, newCondition.condition)
		}
	}
	pvc.Status.Conditions = newConditions
	return pvc
}

// checkFileSystemPendingOnPVC checks whether PVC has
// PersistentVolumeClaimFileSystemResizePending. Return true if have,
// false if not.
func checkFileSystemPendingOnPVC(
	pvc *v1.PersistentVolumeClaim) bool {
	conditions := pvc.Status.Conditions
	for _, condition := range conditions {
		if condition.Type == v1.PersistentVolumeClaimFileSystemResizePending {
			return true
		}
	}
	return false
}
