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
	"time"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// VolumeHealthReconciler is the interface for volume health reconciler
type VolumeHealthReconciler interface {
	// Run starts the reconciler.
	Run(ctx context.Context, workers int)
}

type volumeHealthReconciler struct {
	// Tanzu Kubernetes Grid KubeClient
	tkgKubeClient kubernetes.Interface
	// Supervisor Cluster KubeClient
	svcKubeClient kubernetes.Interface
	// Supervisor Cluster claim queue
	svcClaimQueue workqueue.RateLimitingInterface

	// Tanzu Kubernetes Grid PV Lister
	tkgPVLister corelisters.PersistentVolumeLister
	// Tanzu Kubernetes Grid PV Synced
	tkgPVSynced cache.InformerSynced
	// Supervisor Cluster PVC Lister
	svcPVCLister corelisters.PersistentVolumeClaimLister
	// Supervisor Cluster PVC Synced
	svcPVCSynced cache.InformerSynced
	// Supervisor Cluster namespace
	supervisorNamespace string
}

// NewVolumeHealthReconciler returns a VolumeHealthReconciler.
func NewVolumeHealthReconciler(
	// Tanzu Kubernetes Grid KubeClient
	tkgKubeClient kubernetes.Interface,
	// Supervisor Cluster KubeClient
	svcKubeClient kubernetes.Interface,
	resyncPeriod time.Duration,
	tkgInformerFactory informers.SharedInformerFactory,
	svcInformerFactory informers.SharedInformerFactory,
	svcPVCRateLimiter workqueue.RateLimiter,
	supervisorNamespace string) VolumeHealthReconciler {
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
	}

	svcPVCInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    rc.svcAddPVC,
		UpdateFunc: rc.svcUpdatePVC,
	}, resyncPeriod)

	return rc
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
	log.Infof("updatePVC: old PVC %+v", oldPVC)

	newPVC, ok := newObj.(*v1.PersistentVolumeClaim)
	if !ok || newPVC == nil {
		return
	}
	log.Infof("updatePVC: new PVC %+v", newPVC)

	newPVCAnnValue, newPVCFound := newPVC.ObjectMeta.Annotations[annVolumeHealth]
	oldPVCAnnValue, oldPVCFound := oldPVC.ObjectMeta.Annotations[annVolumeHealth]
	if !newPVCFound && !oldPVCFound {
		// both old and new PVC have no annotation, skip update
		log.Infof("updatePVC: No volume health annotation. Skip updating PVC %s/%s", oldPVC.Namespace, oldPVC.Name)
		return
	}
	if !newPVCFound && oldPVCFound || newPVCFound && !oldPVCFound || newPVCAnnValue != oldPVCAnnValue {
		// volume health annotation changed
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

	if !cache.WaitForCacheSync(stopCh, rc.tkgPVSynced, rc.svcPVCSynced) {
		log.Errorf("Cannot sync supervisor and/or TKG pv/pvc caches")
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(rc.syncPVCs, 0, stopCh)
	}

	<-stopCh
}

// syncPVCs is the main worker.
func (rc *volumeHealthReconciler) syncPVCs() {
	_, log := logger.GetNewContextWithLogger()
	log.Infof("syncPVCs: Enter syncPVCs")

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

// syncPVC processes one Supervisor Cluster PVC
// It finds Tanzu Kubernetes Grid PV
// It then finds Tanzu Kubernetes Grid PVC accordingly and update
// the volume health annotation based on Supervisor Cluster PVC
func (rc *volumeHealthReconciler) syncPVC(key string) error {
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("syncPVC: Started PVC processing %s", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		log.Errorf("Split meta namespace key of pvc %s failed: %v", key, err)
		return err
	}

	svcPVC, err := rc.svcPVCLister.PersistentVolumeClaims(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Infof("PVC %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		log.Errorf("Get PVC %s/%s failed: %v", namespace, name, err)
		return err
	}
	log.Infof("syncPVC: Found Supervisor Cluster PVC: %s/%s", svcPVC.Namespace, svcPVC.Name)

	// The input PVC is a Supervisor Cluster PVC
	// Find Tanzu Kubernetes Grid PV
	// Find Tanzu Kubernetes Grid PVC accordingly and update it
	tkgPV, err := rc.findTKGPVforSupervisorPVC(ctx, svcPVC)
	if err != nil {
		log.Errorf("Find Tanzu Kubernetes Grid PV for Supervisor Cluster PVC %s/%s failed: %v", namespace, name, err)
		return err
	}
	if tkgPV == nil {
		log.Infof("Tanzu Kubernetes Grid PV not found for Supervisor PVC %s/%s", svcPVC.Namespace, svcPVC.Name)
		return fmt.Errorf("tanzu Kubernetes Grid PV not found for Supervisor PVC %s/%s", svcPVC.Namespace, svcPVC.Name)
	}

	log.Infof("Found Tanzu Kubernetes Grid PV %s", tkgPV.Name)

	// Update Tanzu Kubernetes Grid PVC volume health annotation
	err = rc.updateTKGPVC(ctx, svcPVC, tkgPV)
	if err != nil {
		log.Errorf("Update Tanzu Kubernetes Grid PVC for PV %s failed: %v", tkgPV.Name, err)
		return err
	}

	log.Infof("Updated Tanzu Kubernetes Grid PVC for PV %s", tkgPV.Name)
	return nil
}

// find PV in TKG that matches PVC in supervisor cluster
// Returns PV in TKG
func (rc *volumeHealthReconciler) findTKGPVforSupervisorPVC(ctx context.Context, svcPVC *v1.PersistentVolumeClaim) (*v1.PersistentVolume, error) {
	log := logger.GetLogger(ctx)
	// For each PV in Tanzu Kubernetes Grid, find corresponding PVC name in the Supervisor Cluster (svcPVCName := tkgPV.Spec.CSI.VolumeHandle)
	// Compare retrieved SC PVC Name with PVC name in Supervisor Cluster triggered by the Add/Update event and find a match.
	log.Infof("findTKGPVforSupervisorPVC enter: Supervisor Cluster PVC %s/%s", svcPVC.Namespace, svcPVC.Name)
	tkgPVs, err := rc.tkgPVLister.List(labels.Everything())
	if err != nil {
		log.Errorf("No persistent volumes found in Tanzu Kubernetes Grid")
		return nil, err
	}
	for _, tkgPV := range tkgPVs {
		log.Debugf("findTKGPVforSupervisorPVC: Supervisor Cluster PVC name: %s, Tanzu Kubernetes Grid PV CSI VolumeHandle: %s", svcPVC.Name, tkgPV.Spec.CSI.VolumeHandle)
		if svcPVC.Name == tkgPV.Spec.CSI.VolumeHandle {
			log.Infof("Found Tanzu Kubernetes Grid PV %s for Supervisor Cluster PVC %s/%s", tkgPV.Name, svcPVC.Namespace, svcPVC.Name)
			return tkgPV, nil
		}
	}

	log.Debugf("findTKGPVforSupervisorPVC exit: Supervisor Cluster PVC %s/%s", svcPVC.Namespace, svcPVC.Name)

	return nil, nil
}

// update PVC in TKG based on PVC in supervisor cluster
// Returns updated PVC in TKG
func (rc *volumeHealthReconciler) updateTKGPVC(ctx context.Context, svcPVC *v1.PersistentVolumeClaim, tkgPV *v1.PersistentVolume) error {
	log := logger.GetLogger(ctx)
	// If matching, find the corresponding PVC in Tanzu Kubernetes Grid from pv.Spec.ClaimRef.
	// compare the volume health annotation on the PVC in Tanzu Kubernetes Grid with the annotation on PVC in the Supervisor Cluster.
	// If annotation is different, update the volume health annotation on the PVC in Tanzu Kubernetes Grid based on the one in Supervisor Cluster.
	// If same, do nothing
	// If update fails, the caller will add PVC in Supervisor Cluster back to RateLimited queue to retry.
	log.Infof("updateTKGPVC enter: Supervisor Cluster PVC %s/%s, Tanzu Kubernetes Grid PV %s", svcPVC.Namespace, svcPVC.Name, tkgPV.Name)
	tkgPVCObj, err := rc.tkgKubeClient.CoreV1().PersistentVolumeClaims(tkgPV.Spec.ClaimRef.Namespace).Get(tkgPV.Spec.ClaimRef.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error get pvc %s/%s from api server: %v", tkgPV.Spec.ClaimRef.Namespace, tkgPV.Spec.ClaimRef.Name, err)
	}
	log.Infof("updateTKGPVC: Found Tanzu Kubernetes Grid PVC %s/%s", tkgPVCObj.Namespace, tkgPVCObj.Name)

	// Check if annotation is the same on PVC in Tanzu Kubernetes Grid and Supervisor Cluster and copy from Supervisor Cluster if different
	tkgAnnValue, tkgFound := tkgPVCObj.ObjectMeta.Annotations[annVolumeHealth]
	svcAnnValue, svcFound := svcPVC.ObjectMeta.Annotations[annVolumeHealth]
	if !tkgFound && svcFound || tkgFound && svcFound && tkgAnnValue != svcAnnValue {
		log.Infof("updateTKGPVC: Detected volume health annotation change. Need to update Tanzu Kubernetes Grid PVC %s/%s", tkgPVCObj.Namespace, tkgPVCObj.Name)
		tkgPVCClone := tkgPVCObj.DeepCopy()
		metav1.SetMetaDataAnnotation(&tkgPVCClone.ObjectMeta, annVolumeHealth, svcAnnValue)
		_, err := rc.tkgKubeClient.CoreV1().PersistentVolumeClaims(tkgPVCClone.Namespace).Update(tkgPVCClone)
		if err != nil {
			log.Errorf("cannot update claim [%s/%s]: [%v]", tkgPVCClone.Namespace, tkgPVCClone.Name, err)
			return err
		}
		log.Infof("updateTKGPVC: Updated Tanzu Kubernetes Grid PVC %s/%s", tkgPVCObj.Namespace, tkgPVCObj.Name)
		return nil
	}

	log.Infof("updateTKGPVC exit: Tanzu Kubernetes Grid PVC %s/%s", tkgPVCObj.Namespace, tkgPVCObj.Name)
	return nil
}
