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
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
)

// VolumeHealthReconciler is the interface for volume health reconciler.
type VolumeHealthReconciler interface {
	// Run starts the reconciler.
	Run(ctx context.Context, workers int)
}

// Map of volume handles to the list of PV's that point to this volume handle.
// Keys are strings representing volume handles.
// Values are list of strings representing PV names that reference this volume.
type volumeHandleToPVs struct {
	*sync.RWMutex
	items map[string][]string
}

// Adds a pv to the list of pv's referenced by a volumeHandle.
// Creates an entry in the map if it doesn't exist.
func (m *volumeHandleToPVs) add(volumeHandle, pvName string) {
	_, log := logger.GetNewContextWithLogger()
	m.Lock()
	defer m.Unlock()
	pvList := m.items[volumeHandle]
	log.Debugf("Add PV %s to list %+v for volume %s", pvName, pvList, volumeHandle)
	pvList = append(pvList, pvName)
	m.items[volumeHandle] = pvList
}

// Removes a pv from the list of pv's referencing a volumeHandle.
// Deletes the entry if list of pv's is empty.
func (m *volumeHandleToPVs) remove(volumeHandle, pvToRemove string) {
	_, log := logger.GetNewContextWithLogger()
	m.Lock()
	defer m.Unlock()
	if pvList, ok := m.items[volumeHandle]; ok {
		for index, pvName := range pvList {
			if pvName == pvToRemove {
				log.Debugf("Remove PV %s from list %+v for volume %s", pvToRemove, pvList, volumeHandle)
				pvList = append(pvList[:index], pvList[index+1:]...)
			}
		}
		if len(pvList) == 0 {
			log.Debugf("Delete volume %s from volumeHandleToPVs mapping", volumeHandle)
			delete(m.items, volumeHandle)
		} else {
			log.Debugf("Set new list for volume %s to %+v", volumeHandle, pvList)
			m.items[volumeHandle] = pvList
		}
	}
}

// Returns the list of pv's referencing a volumeHandle.
// Returns an empty list if this entry doesn't exist.
func (m *volumeHandleToPVs) get(volumeHandle string) []string {
	_, log := logger.GetNewContextWithLogger()
	m.RLock()
	defer m.RUnlock()
	log.Debugf("PV list for volume %s: %+v", volumeHandle, m.items[volumeHandle])
	return m.items[volumeHandle]
}

type volumeHealthReconciler struct {
	// Tanzu Kubernetes Grid KubeClient.
	tkgKubeClient kubernetes.Interface
	// Supervisor Cluster KubeClient.
	svcKubeClient kubernetes.Interface
	// Supervisor Cluster claim queue.
	svcClaimQueue workqueue.RateLimitingInterface

	// Tanzu Kubernetes Grid PV Lister.
	tkgPVLister corelisters.PersistentVolumeLister
	// Tanzu Kubernetes Grid PV Synced.
	tkgPVSynced cache.InformerSynced
	// Supervisor Cluster PVC Lister.
	svcPVCLister corelisters.PersistentVolumeClaimLister
	// Supervisor Cluster PVC Synced.
	svcPVCSynced cache.InformerSynced
	// Supervisor Cluster namespace.
	supervisorNamespace string

	// Volume handle to PV list mapping.
	volumeHandleToPVs *volumeHandleToPVs
}

// NewVolumeHealthReconciler returns a VolumeHealthReconciler.
func NewVolumeHealthReconciler(
	// Tanzu Kubernetes Grid KubeClient.
	tkgKubeClient kubernetes.Interface,
	// Supervisor Cluster KubeClient.
	svcKubeClient kubernetes.Interface,
	resyncPeriod time.Duration,
	tkgInformerFactory informers.SharedInformerFactory,
	svcInformerFactory informers.SharedInformerFactory,
	svcPVCRateLimiter workqueue.RateLimiter,
	supervisorNamespace string, stopCh <-chan struct{}) (VolumeHealthReconciler, error) {
	svcPVCInformer := svcInformerFactory.Core().V1().PersistentVolumeClaims()
	tkgPVInformer := tkgInformerFactory.Core().V1().PersistentVolumes()

	svcClaimQueue := workqueue.NewNamedRateLimitingQueue(
		svcPVCRateLimiter, "volume-health-pvc")

	rc := &volumeHealthReconciler{
		tkgKubeClient: tkgKubeClient,
		svcKubeClient: svcKubeClient,
		// TODO: Discuss pros and cons of having a single controller
		// vs separate controllers to handle metadata sync, resize,
		// volume health, and any other logic that is triggered by
		// pv or pvc update in the future.
		tkgPVLister:         tkgPVInformer.Lister(),
		tkgPVSynced:         tkgPVInformer.Informer().HasSynced,
		svcPVCLister:        svcPVCInformer.Lister(),
		svcPVCSynced:        svcPVCInformer.Informer().HasSynced,
		svcClaimQueue:       svcClaimQueue,
		supervisorNamespace: supervisorNamespace,
		volumeHandleToPVs: &volumeHandleToPVs{
			RWMutex: &sync.RWMutex{},
			items:   make(map[string][]string),
		},
	}

	svcPVCInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.svcAddPVC,
		UpdateFunc: rc.svcUpdatePVC,
		DeleteFunc: rc.svcAddPVC,
	}, resyncPeriod)

	tkgPVInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    nil,
		UpdateFunc: rc.tkgUpdatePV,
		DeleteFunc: rc.tkgDeletePV,
	}, resyncPeriod)

	ctx, log := logger.GetNewContextWithLogger()

	// Start TKG Informers.
	tkgInformerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.tkgPVSynced) {
		return nil, fmt.Errorf("cannot sync tkg pv cache")
	}

	// Initialize volumeHandleToPVs map with existing PV's.
	// Since this mapping is lost across pvCSI restarts, it needs to be
	// initialized with existing values to prevent missing an SVC-PVC
	// event due to absence of an entry in the map. This is done after
	// TKG informers cache is synced to avoid missing a PV that
	// enters the Bound state after volumeHandleToPVs is initialized.
	pvs, err := rc.tkgKubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("failed to list TKG PV's with error: %+v", err)
		return nil, err
	}
	for _, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name && pv.Status.Phase == v1.VolumeBound {
			// Add to volumeHandleToPVs.
			rc.volumeHandleToPVs.add(pv.Spec.CSI.VolumeHandle, pv.Name)
		}
	}

	// Start SVC Informers. This is done after volumeHandleToPV mapping is
	// initialized to prevent incorrectly skipping a PV due to absence in
	// the map.
	svcInformerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.svcPVCSynced) {
		return nil, fmt.Errorf("cannot sync supervisor pvc cache")
	}
	return rc, nil
}

func (rc *volumeHealthReconciler) tkgUpdatePV(oldObj, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	oldPv, ok := oldObj.(*v1.PersistentVolume)
	if !ok {
		return
	}
	newPv, ok := newObj.(*v1.PersistentVolume)
	if !ok {
		return
	}
	if newPv.Spec.CSI != nil && newPv.Spec.CSI.Driver == csitypes.Name &&
		oldPv.Status.Phase != v1.VolumeBound && newPv.Status.Phase == v1.VolumeBound {
		// Add to volumeHandleToPVs.
		rc.volumeHandleToPVs.add(newPv.Spec.CSI.VolumeHandle, newPv.Name)

		// Add SVC PVC to work queue to add volume health annotation to statically
		// provisioned volumes.
		objKey := rc.supervisorNamespace + "/" + newPv.Spec.CSI.VolumeHandle
		log.Infof("tkgUpdatePV: add %s to claim queue", objKey)
		rc.svcClaimQueue.Add(objKey)
	}
}

func (rc *volumeHealthReconciler) tkgDeletePV(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		return
	}
	if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == csitypes.Name {
		// Remove from volumeHandleToPVs.
		rc.volumeHandleToPVs.remove(pv.Spec.CSI.VolumeHandle, pv.Name)
	}
}

func (rc *volumeHealthReconciler) svcAddPVC(obj interface{}) {
	ctx, log := logger.GetNewContextWithLogger()
	log.Debugf("addPVC: %+v", obj)
	objKey, err := getPVCKey(ctx, obj)
	if err != nil {
		return
	}
	log.Infof("addPVC: add %s to claim queue", objKey)
	rc.svcClaimQueue.Add(objKey)
}

func (rc *volumeHealthReconciler) svcUpdatePVC(oldObj, newObj interface{}) {
	_, log := logger.GetNewContextWithLogger()
	log.Debugf("updatePVC: old [%+v] new [%+v]", oldObj, newObj)
	oldPVC, ok := oldObj.(*v1.PersistentVolumeClaim)
	if !ok || oldPVC == nil {
		return
	}
	log.Debugf("updatePVC: old PVC %+v", oldPVC)

	newPVC, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok || newPVC == nil {
		return
	}
	log.Debugf("updatePVC: new PVC %+v", newPVC)

	newPVCAnnValue, newPVCFound := newPVC.ObjectMeta.Annotations[annVolumeHealth]
	oldPVCAnnValue, oldPVCFound := oldPVC.ObjectMeta.Annotations[annVolumeHealth]
	if !newPVCFound && !oldPVCFound {
		// both old and new PVC have no annotation, skip update.
		log.Infof("updatePVC: No volume health annotation. Skip updating PVC %s/%s", oldPVC.Namespace, oldPVC.Name)
		return
	}
	if !newPVCFound && oldPVCFound || newPVCFound && !oldPVCFound || newPVCAnnValue != oldPVCAnnValue {
		// volume health annotation changed.
		log.Infof("updatePVC: Detected volume health annotation change. Add PVC %s/%s", newPVC.Namespace, newPVC.Name)
		rc.svcAddPVC(newObj)
	}
}

// Run starts the reconciler.
func (rc *volumeHealthReconciler) Run(
	ctx context.Context, workers int) {
	log := logger.GetLogger(ctx)
	defer rc.svcClaimQueue.ShutDown()

	log.Infof("Starting volume health reconciler")
	defer log.Infof("Shutting down volume health reconciler")

	stopCh := ctx.Done()

	for i := 0; i < workers; i++ {
		go wait.Until(rc.syncPVCs, 0, stopCh)
	}

	<-stopCh
}

// syncPVCs is the main worker.
func (rc *volumeHealthReconciler) syncPVCs() {
	_, log := logger.GetNewContextWithLogger()
	log.Debugf("syncPVCs: Enter syncPVCs")

	key, quit := rc.svcClaimQueue.Get()
	if quit {
		return
	}
	defer rc.svcClaimQueue.Done(key)

	if err := rc.syncPVC(key.(string)); err != nil {
		// Put PVC back to the queue so that we can retry later.
		rc.svcClaimQueue.AddRateLimited(key)
	} else {
		rc.svcClaimQueue.Forget(key)
	}
}

// syncPVC processes one Supervisor Cluster PVC.
// It finds Tanzu Kubernetes Grid PV. It then finds Tanzu Kubernetes
// Grid PVC accordingly and update the volume health annotation based
// on Supervisor Cluster PVC.
func (rc *volumeHealthReconciler) syncPVC(key string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Debugf("syncPVC: Started PVC processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		log.Errorf("Split meta namespace key of pvc %s failed: %v", key, err)
		return err
	}

	svcPVC, err := rc.svcPVCLister.PersistentVolumeClaims(namespace).Get(name)
	switch {
	case err == nil:
		log.Infof("syncPVC: Found Supervisor Cluster PVC: %s/%s", svcPVC.Namespace, svcPVC.Name)
	case k8serrors.IsNotFound(err):
		log.Infof("syncPVC: Supervisor Cluster PVC %s/%s is deleted, process any left over TKG PVCs", namespace, name)
	default:
		log.Errorf("syncPVC: Get PVC %s/%s failed: %v", namespace, name, err)
		return err
	}

	// The input PVC is a Supervisor Cluster PVC.
	// Find list of Tanzu Kubernetes Grid PV's referencing this PVC.
	// Find corresponding Tanzu Kubernetes Grid PVCs and update them.
	tkgPVList, err := rc.findTKGPVforSupervisorPVC(ctx, name, namespace)
	if err != nil {
		return err
	}
	if tkgPVList == nil {
		// If no PV is found, the SV PVC may not be referenced within
		// this TKG. Do not requeue this request.
		log.Debugf("Tanzu Kubernetes Grid PV not found for Supervisor PVC %s/%s. Igonoring ...", namespace, name)
		return nil
	}

	log.Debugf("Found Tanzu Kubernetes Grid PV's: %v", tkgPVList)

	for _, tkgPV := range tkgPVList {
		// Update Tanzu Kubernetes Grid PVC volume health annotation.
		if tkgPV.Spec.ClaimRef == nil {
			log.Debugf("skipping tkgPV: %v with nil ClaimRef", tkgPV.Name)
			continue
		}
		err = rc.updateTKGPVC(ctx, svcPVC, tkgPV)
		if err != nil {
			log.Errorf("updating Tanzu Kubernetes Grid PVC for PV %s failed: %v", tkgPV.Name, err)
			return err
		}
		log.Infof("Updated Tanzu Kubernetes Grid PVC for PV %s", tkgPV.Name)
	}

	return nil
}

// Find list of PV's in TKG that matches PVC in supervisor cluster.
// Returns list of pointers to PV's in TKG.
func (rc *volumeHealthReconciler) findTKGPVforSupervisorPVC(ctx context.Context,
	pvcName, pvcNamespace string) ([]*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	// For the SV PVC for which this event was triggered, find the TKG PV's
	// that reference this PVC.
	var tkgPVList []*v1.PersistentVolume
	log.Debugf("findTKGPVforSupervisorPVC enter: Supervisor Cluster PVC %s/%s", pvcNamespace, pvcName)

	pvNames := rc.volumeHandleToPVs.get(pvcName)
	if len(pvNames) == 0 {
		return nil, nil
	}
	for _, name := range pvNames {
		tkgPV, err := rc.tkgPVLister.Get(name)
		if err != nil {
			log.Errorf("failed to get TKG PV %s with error: %+v", name, err)
			return nil, err
		}
		tkgPVList = append(tkgPVList, tkgPV)
	}
	return tkgPVList, nil
}

// Update PVC in TKG based on PVC in supervisor cluster.
// Returns updated PVC in TKG.
func (rc *volumeHealthReconciler) updateTKGPVC(ctx context.Context,
	svcPVC *v1.PersistentVolumeClaim, tkgPV *v1.PersistentVolume) error {
	log := logger.GetLogger(ctx)
	// If matching, find the corresponding PVC in Tanzu Kubernetes Grid from
	// pv.Spec.ClaimRef. Compare the volume health annotation on the PVC in
	// Tanzu Kubernetes Grid with the annotation on PVC in the Supervisor
	// Cluster. If annotation is different, update the volume health annotation
	// on the PVC in Tanzu Kubernetes Grid based on the one in Supervisor
	// Cluster. If same, do nothing. If the SVC PVC was deleted but TKG PVC
	// exists, change annotation to inaccessible. If update fails, the caller
	// will add PVC in Supervisor Cluster back to RateLimited queue to retry.
	log.Debugf("updateTKGPVC enter: Tanzu Kubernetes Grid PV %s", tkgPV.Name)
	tkgPVCObj, err := rc.tkgKubeClient.CoreV1().PersistentVolumeClaims(tkgPV.Spec.ClaimRef.Namespace).
		Get(ctx, tkgPV.Spec.ClaimRef.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error get pvc %s/%s from api server: %v",
			tkgPV.Spec.ClaimRef.Namespace, tkgPV.Spec.ClaimRef.Name, err)
	}
	log.Debugf("updateTKGPVC: Found Tanzu Kubernetes Grid PVC %s/%s", tkgPVCObj.Namespace, tkgPVCObj.Name)

	// Check if annotation is the same on PVC in Tanzu Kubernetes Grid and
	// Supervisor Cluster and copy from Supervisor Cluster if different.
	var tkgAnnValue, svcAnnValue string
	var tkgAnnFound, svcAnnFound bool
	tkgAnnValue, tkgAnnFound = tkgPVCObj.ObjectMeta.Annotations[annVolumeHealth]
	if svcPVC != nil {
		svcAnnValue, svcAnnFound = svcPVC.ObjectMeta.Annotations[annVolumeHealth]
	} else {
		svcAnnValue = common.VolHealthStatusInaccessible
	}

	if !tkgAnnFound && svcAnnFound || tkgAnnFound && svcAnnFound && tkgAnnValue != svcAnnValue || svcPVC == nil {
		log.Infof("updateTKGPVC: Detected volume health annotation change. "+
			"Need to update Tanzu Kubernetes Grid PVC %s/%s. Existing TKG PVC annotation: %s. New annotation: %s",
			tkgPVCObj.Namespace, tkgPVCObj.Name, tkgAnnValue, svcAnnValue)
		tkgPVCClone := tkgPVCObj.DeepCopy()
		metav1.SetMetaDataAnnotation(&tkgPVCClone.ObjectMeta, annVolumeHealth, svcAnnValue)
		metav1.SetMetaDataAnnotation(&tkgPVCClone.ObjectMeta, annVolumeHealthTS, time.Now().Format(time.UnixDate))
		_, err := rc.tkgKubeClient.CoreV1().PersistentVolumeClaims(tkgPVCClone.Namespace).
			Update(ctx, tkgPVCClone, metav1.UpdateOptions{})
		if err != nil {
			log.Errorf("cannot update claim [%s/%s]: [%v]", tkgPVCClone.Namespace, tkgPVCClone.Name, err)
			return err
		}
		log.Infof("updateTKGPVC: Updated Tanzu Kubernetes Grid PVC %s/%s, set annotation %s at time %s",
			tkgPVCObj.Namespace, tkgPVCObj.Name, svcAnnValue, time.Now().Format(time.UnixDate))
		return nil
	}

	log.Debugf("updateTKGPVC exit: Tanzu Kubernetes Grid PVC %s/%s", tkgPVCObj.Namespace, tkgPVCObj.Name)
	return nil
}
