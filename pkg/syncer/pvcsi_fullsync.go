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

package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"

	"slices"

	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var (
	cnsconfigGetSupervisorNamespace = cnsconfig.GetSupervisorNamespace
	k8sNewClient                    = k8s.NewClient
	timeoutAddNodeAffinityOnPVs     = 300 * time.Second
)

// PvcsiFullSync reconciles PV/PVC/Pod metadata on the guest cluster with
// cnsvolumemetadata objects on the supervisor cluster for the guest cluster.
func PvcsiFullSync(ctx context.Context, metadataSyncer *metadataSyncInformer) error {
	log := logger.GetLogger(ctx)
	log.Infof("FullSync: Start")
	var err error
	fullSyncStartTime := time.Now()
	defer func() {
		fullSyncStatus := prometheus.PrometheusPassStatus
		if err != nil {
			fullSyncStatus = prometheus.PrometheusFailStatus
		}
		prometheus.FullSyncOpsHistVec.WithLabelValues(fullSyncStatus).Observe(
			(time.Since(fullSyncStartTime)).Seconds())
	}()

	isWorkloadDomainIsolationEnabledInPVCSI := metadataSyncer.coCommonInterface.IsFSSEnabled(
		ctx, common.WorkloadDomainIsolationFSS)
	if isWorkloadDomainIsolationEnabledInPVCSI {
		go AddNodeAffinityRulesOnPV(ctx, metadataSyncer)
	}
	// guestCnsVolumeMetadataList is an in-memory list of cnsvolumemetadata
	// objects that represents PV/PVC/Pod objects in the guest cluster API server.
	// These objects are compared to actual objects on the supervisor
	// cluster to make reconciliation decisions.
	guestCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}

	// Get the supervisor namespace in which the guest cluster is deployed.
	supervisorNamespace, err := cnsconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("FullSync: could not get supervisor namespace in which guest cluster was deployed. Err: %v", err)
		return err
	}

	// Populate guestCnsVolumeMetadataList with cnsvolumemetadata objects created
	// from the guest cluster.
	err = createCnsVolumeMetadataList(ctx, metadataSyncer, supervisorNamespace, &guestCnsVolumeMetadataList)
	if err != nil {
		log.Errorf("FullSync: Failed to create CnsVolumeMetadataList from guest cluster. Err: %v", err)
		return err
	}

	// Get list of cnsvolumemetadata objects that exist in the given supervisor
	// cluster namespace.
	supervisorNamespaceList := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	err = metadataSyncer.cnsOperatorClient.List(ctx, supervisorNamespaceList, client.InNamespace(supervisorNamespace))
	if err != nil {
		log.Warnf("FullSync: Failed to get CnsVolumeMetadatas from supervisor cluster. Err: %v", err)
		return err
	}
	// cleanup remaining duplicate cnsvolumemetadata post migration from legacy to non-legacy clusters.
	cleanUpCnsVolumeMetadata(ctx, metadataSyncer, supervisorNamespaceList)
	supervisorCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	// Remove cnsvolumemetadata object from supervisorCnsVolumeMetadataList that
	// do not belong to this guest cluster.
	for _, object := range supervisorNamespaceList.Items {
		if object.Spec.GuestClusterID == metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID {
			supervisorCnsVolumeMetadataList.Items = append(supervisorCnsVolumeMetadataList.Items, object)
		}
	}

	// guestObjectsMap maintains a mapping of cnsvolumemetadata objects names to
	// their spec. Used by pvcsi full sync for quick look-up to check existence
	// in the guest cluster.
	guestObjectsMap := make(map[string]*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	for index, object := range guestCnsVolumeMetadataList.Items {
		guestObjectsMap[object.Name] = &guestCnsVolumeMetadataList.Items[index]
	}

	// supervisorObjectsMap maintains a mapping of cnsvolumemetadata objects names
	// to their spec. Used by pvcsi full sync for quick look-up to check existence
	// in the supervisor cluster.
	supervisorObjectsMap := make(map[string]*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	for index, object := range supervisorCnsVolumeMetadataList.Items {
		supervisorObjectsMap[object.Name] = &supervisorCnsVolumeMetadataList.Items[index]
	}

	// Identify cnsvolumemetadata objects that need to be updated or created
	// on the supervisor cluster API server.
	for _, guestObject := range guestCnsVolumeMetadataList.Items {
		if supervisorObject, exists := supervisorObjectsMap[guestObject.Name]; !exists {
			// Create objects that do not exist.
			log.Infof("FullSync: Creating CnsVolumeMetadata %v on the supervisor cluster for entity type %q",
				guestObject.Name, guestObject.Spec.EntityType)
			guestObject.Namespace = supervisorNamespace
			if err := metadataSyncer.cnsOperatorClient.Create(ctx, &guestObject); err != nil {
				log.Warnf("FullSync: Failed to create CnsVolumeMetadata %v. Err: %v", guestObject.Name, err)
			}
		} else {
			// Compare objects between the guest cluster and supervisor cluster.
			// Update the supervisor cluster API server if an object is stale.
			if guestObject.Spec.EntityType != cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD &&
				!compareCnsVolumeMetadatas(&guestObject.Spec, &supervisorObject.Spec) {
				log.Infof("FullSync: Updating CnsVolumeMetadata %v on the supervisor cluster", guestObject.Name)
				if err := metadataSyncer.cnsOperatorClient.Update(ctx, supervisorObject); err != nil {
					log.Warnf("FullSync: Failed to update CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
				}
			}
		}
	}

	// Delete outdated cnsvolumemetadata objects present in
	// the supervisor cluster API server that shouldn't exist.
	for _, supervisorObject := range supervisorCnsVolumeMetadataList.Items {
		if _, exists := guestObjectsMap[supervisorObject.Name]; !exists {
			log.Infof("FullSync: Deleting CnsVolumeMetadata %v on the supervisor cluster for entity type %q",
				supervisorObject.Name, supervisorObject.Spec.EntityType)
			if err := metadataSyncer.cnsOperatorClient.Delete(ctx, &supervisorObject); err != nil {
				log.Warnf("FullSync: Failed to delete CnsVolumeMetadata %v. Err: %v", supervisorObject.Name, err)
			}
		}
	}

	// Set csi.vsphere-volume labels and CNS finalizer(if SVPVCSnapshotProtectionFinalizer FSS enabled)
	// on the Supervisor PVC, which is requested from TKC Cluster
	err = setGuestClusterDetailsOnSupervisorPVC(ctx, metadataSyncer, supervisorNamespace)
	if err != nil {
		log.Errorf("FullSync: Failed to set Guest Cluster data on SupervisorPVC. Err: %v", err)
		return err
	}
	// Set csi.vsphere-volume labels and CNS finalizer on the Supervisor VolumeSnapshot which is
	// requested from TKC Cluster, if SVPVCSnapshotProtectionFinalizer FSS enabled
	if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.SVPVCSnapshotProtectionFinalizer) {
		err = setGuestClusterDetailsOnSupervisorSnapshot(ctx, metadataSyncer, supervisorNamespace)
		if err != nil {
			log.Errorf("FullSync: Failed to set Guest Cluster data on SupervisorSnapshot. Err: %v", err)
			return err
		}
	}
	log.Infof("FullSync: End")
	return nil
}

// cleanUpCnsVolumeMetadata deletes the cnsvolumemetadata created on legacy kubernetes releases,
// which are left unused as customer have migrated to non-legacy kubernetes releases
func cleanUpCnsVolumeMetadata(ctx context.Context, metadataSyncer *metadataSyncInformer,
	cnsVolumeMetadataList *cnsvolumemetadatav1alpha1.CnsVolumeMetadataList) {
	log := logger.GetLogger(ctx)
	log.Info("cleanUpCnsVolumeMetadata: deleting the CnsVolumeMetadata CRs " +
		"created on legacy vsphere kubernetes releases")
	toDeleteCnsVolumeMetadataList := cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	cnsVolMetadataMap := make(map[string]int)
	for _, object := range cnsVolumeMetadataList.Items {
		cnsVolMetadataMap[object.Spec.EntityName+object.Spec.Namespace]++
		if object.ObjectMeta.OwnerReferences != nil {
			for _, ownerRef := range object.ObjectMeta.OwnerReferences {
				if ownerRef.APIVersion != cnsconfig.ClusterVersionv1beta1 {
					log.Debugf("duplicate cnsvolumemetadata %s from namespace %s is marked for deletion",
						object.Name, object.Namespace)
					toDeleteCnsVolumeMetadataList.Items = append(toDeleteCnsVolumeMetadataList.Items, object)
					break
				}
			}
		}
	}
	for _, cvm := range toDeleteCnsVolumeMetadataList.Items {
		if cnsVolMetadataMap[cvm.Spec.EntityName+cvm.Spec.Namespace] > 1 {
			if err := metadataSyncer.cnsOperatorClient.Delete(ctx, &cvm); err != nil {
				log.Warnf("FullSync: Failed to delete CnsVolumeMetadata %v. Err: %v", cvm.Name, err)
			}
		}
	}
	log.Info("cleanUpCnsVolumeMetadata: cleaned up duplicate CnsVolumeMetadata CRs " +
		"created on legacy vsphere kubernetes releases")
}

// createCnsVolumeMetadataList creates cnsvolumemetadata objects from the API
// server using the input k8s client.
// All objects that can be created are added to returnList. This includes
// PERSISTENT_VOLUME, PERSISTENT_VOLUME_CLAIM and POD entity types.
func createCnsVolumeMetadataList(ctx context.Context, metadataSyncer *metadataSyncInformer,
	supervisorNamespace string, returnList *cnsvolumemetadatav1alpha1.CnsVolumeMetadataList) error {
	log := logger.GetLogger(ctx)
	log.Debugf("FullSync: Querying guest cluster API server for all PV objects.")
	pvList, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: Failed to get PVs from guest cluster. Err: %v", err)
		return err
	}

	// Structure to map PVC names to corresponding volume handles.
	pvcToVolumeName := make(map[string]string)

	// Create cnsvolumemetadata objects for PV and PVC entity types.
	for _, pv := range pvList {
		var volumeNames []string
		volumeNames = append(volumeNames, pv.Spec.CSI.VolumeHandle)

		// Get the cnsvolumemetadata object for this pv and add it to the return
		// list.
		entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
			pv.Spec.CSI.VolumeHandle, supervisorNamespace, cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, "")
		pvObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(
			volumeNames, metadataSyncer.configInfo.Cfg.GC, string(pv.UID), pv.Name,
			cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV, pv.Labels, "",
			[]cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
		returnList.Items = append(returnList.Items, *pvObject)

		// Get the cnsvolumemetadata object for pvc bound to this pv and add it
		// to the return list.
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			pvc, err := metadataSyncer.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(
				pv.Spec.ClaimRef.Name)
			if err != nil {
				log.Errorf("FullSync: Failed to get PVC %q from guest cluster. Err: %v", pvc.Name, err)
				return err
			}
			entityReference := cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
				pvc.Spec.VolumeName, "", cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
				metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID)
			pvcObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(
				volumeNames, metadataSyncer.configInfo.Cfg.GC, string(pvc.UID), pvc.Name,
				cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC, pvc.GetLabels(), pvc.Namespace,
				[]cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{entityReference})
			returnList.Items = append(returnList.Items, *pvcObject)
			pvcToVolumeName[pvc.Name] = pv.Spec.CSI.VolumeHandle
		}
	}

	log.Debugf("FullSync: Querying guest cluster API server for all POD objects.")
	pods, err := metadataSyncer.podLister.Pods(v1.NamespaceAll).List(labels.Everything())
	if err != nil {
		log.Errorf("FullSync: Failed to get all PODs from the guest cluster. Err: %v", err)
		return err
	}
	// Create cnsvolumemetadata objects for POD entity types.
	for _, pod := range pods {
		var entityReferences []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference
		var volumeNames []string
		for _, volume := range pod.Spec.Volumes {
			if volume.VolumeSource.PersistentVolumeClaim == nil {
				continue
			}
			volumeName, ok := pvcToVolumeName[volume.VolumeSource.PersistentVolumeClaim.ClaimName]
			if !ok {
				log.Debugf("FullSync: PVC %q claimed by Pod %q is not a CSI vSphere Volume",
					volume.VolumeSource.PersistentVolumeClaim.ClaimName, pod.Name)
				continue
			}
			entityReferences = append(entityReferences,
				cnsvolumemetadatav1alpha1.GetCnsOperatorEntityReference(
					volume.VolumeSource.PersistentVolumeClaim.ClaimName, pod.Namespace,
					cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
					metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID))
			volumeNames = append(volumeNames, volumeName)
		}
		if len(volumeNames) > 0 {
			log.Debugf("Pod %q claims vsphere volumes %v", pod.Name, volumeNames)
			podObject := cnsvolumemetadatav1alpha1.CreateCnsVolumeMetadataSpec(
				volumeNames, metadataSyncer.configInfo.Cfg.GC, string(pod.UID), pod.Name,
				cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD, nil, pod.Namespace, entityReferences)
			returnList.Items = append(returnList.Items, *podObject)
		}
	}
	return nil
}

// AddNodeAffinityRulesOnPV update TKG PVs with node affinity rules set on associated supervisor PVC if
// node affinity rule is not set on the TKG PV
func AddNodeAffinityRulesOnPV(ctx context.Context, metadataSyncer *metadataSyncInformer) {
	log := logger.GetLogger(ctx)
	log.Info("AddNodeAffinityRulesOnPV Start.")
	pvList, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: Failed to get PVs from guest cluster. Err: %v", err)
		return
	}
	// Get the supervisor namespace in which the guest cluster is deployed.
	supervisorNamespace, err := cnsconfigGetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("FullSync: could not get supervisor namespace in which guest cluster was deployed. Err: %v", err)
		return
	}

	// Create the kubernetes client from config.
	k8sClient, err := k8sNewClient(ctx)
	if err != nil {
		log.Errorf("creating Kubernetes client failed. Err: %v", err)
		return
	}

	pvsWithoutSupervisorPvcTopologyAnnotation := make(map[string]*v1.PersistentVolume)
	addNodeAffinityOnPVInternal := func(pv *v1.PersistentVolume) error {
		if pv.Spec.NodeAffinity == nil {
			supervisorPVClaim, err := metadataSyncer.supervisorClient.CoreV1().
				PersistentVolumeClaims(supervisorNamespace).Get(ctx, pv.Spec.CSI.VolumeHandle, metav1.GetOptions{})
			if err != nil {
				log.Errorf("AddNodeAffinityRulesOnPV: failed to get supervisor PVC: %v "+
					"for the TKG PV %v in the supervisor namespace: %v. Err: %v",
					pv.Spec.CSI.VolumeHandle, pv.Name, supervisorNamespace, err)
				return err
			}
			// If volume accessible topology annotation is not yet available on the supervisor PVC, then add
			// this PV to the pvsWithoutSupervisorPvcTopologyAnnotation map and we will check again after some
			// time if annotation gets added.
			if supervisorPVClaim.Annotations[common.AnnVolumeAccessibleTopology] == "" {
				errstr := fmt.Sprintf("Annotation %q is not set on the PVC: %q, namespace: %q",
					common.AnnVolumeAccessibleTopology, supervisorPVClaim.Name, supervisorPVClaim.Namespace)
				log.Errorf(errstr)
				pvsWithoutSupervisorPvcTopologyAnnotation[pv.Name] = pv
				return errors.New(errstr)
			}
			accessibleTopologies, err := generateVolumeAccessibleTopologyFromPVCAnnotation(supervisorPVClaim)
			if err != nil {
				log.Errorf("failed to generate volume accessibleTopologies "+
					"from supervisor PVC: %v for csi.vsphere.volume-accessible-topology annoation: %v, "+
					"Err: %v", supervisorPVClaim.Name,
					supervisorPVClaim.Annotations[common.AnnVolumeAccessibleTopology], err)
				return err
			}
			var csiAccessibleTopology []*csi.Topology
			for _, topoSegments := range accessibleTopologies {
				volumeTopology := &csi.Topology{
					Segments: topoSegments,
				}
				csiAccessibleTopology = append(csiAccessibleTopology, volumeTopology)
			}
			oldData, err := json.Marshal(pv)
			if err != nil {
				log.Errorf("failed to marshal pv: %v, Error: %v", pv, err)
				return err
			}
			newPV := pv.DeepCopy()
			newPV.Spec.NodeAffinity = GenerateVolumeNodeAffinity(csiAccessibleTopology)
			newData, err := json.Marshal(newPV)
			if err != nil {
				log.Errorf("failed to marshal updated PV with node affinity rules: %v, Error: %v", newPV, err)
				return err
			}

			patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, pv)
			if err != nil {
				log.Errorf("error Creating two way merge patch for PV %q with error : %v", pv.Name, err)
				return err
			}
			_, err = k8sClient.CoreV1().PersistentVolumes().Patch(
				context.TODO(), pv.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
			if err != nil {
				log.Errorf("error patching PV %q error : %v", pv.Name, err)
				return err
			}
			log.Infof("patched PV: %v with node affinity %v", pv.Name, newPV.Spec.NodeAffinity)
		}
		return nil
	}

	for _, pv := range pvList {
		_ = addNodeAffinityOnPVInternal(pv)
	}

	// Check if there are any PVs on which we didn't add node affinity rules yet, as topology annotation
	// was missing from associated supervisor PVC. We will iterate over all such PVs again and will check if
	// PVC annotation is added now. We will retry this until all PVs get node affinity rules added or until
	// timeout of 5 minutes is reached.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeoutAddNodeAffinityOnPVs)
	defer cancel()
	for len(pvsWithoutSupervisorPvcTopologyAnnotation) != 0 {
		select {
		case <-timeoutCtx.Done():
			log.Infof("Timeout exceeded for adding node affinity rules on PVs. Waited 5 minutes to get " +
				"volume accessibility topology annotation added on supervisor PVCs.")
			// Make pvsWithoutSupervisorPvcTopologyAnnotation nil once timeout is hit
			// to exit from the outer for loop
			pvsWithoutSupervisorPvcTopologyAnnotation = nil
		default:
			for pvName, pv := range pvsWithoutSupervisorPvcTopologyAnnotation {
				err := addNodeAffinityOnPVInternal(pv)
				if err == nil {
					delete(pvsWithoutSupervisorPvcTopologyAnnotation, pvName)
				}
			}
			if len(pvsWithoutSupervisorPvcTopologyAnnotation) != 0 {
				// Sleep for some time before retrying
				time.Sleep(10 * time.Second)
			}
		}
	}
	log.Info("AddNodeAffinityRulesOnPV End.")
}

func GenerateVolumeNodeAffinity(accessibleTopology []*csi.Topology) *v1.VolumeNodeAffinity {
	if len(accessibleTopology) == 0 {
		return nil
	}

	var terms []v1.NodeSelectorTerm
	for _, topology := range accessibleTopology {
		if len(topology.Segments) == 0 {
			continue
		}

		var expressions []v1.NodeSelectorRequirement
		for k, v := range topology.Segments {
			expressions = append(expressions, v1.NodeSelectorRequirement{
				Key:      k,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{v},
			})
		}
		terms = append(terms, v1.NodeSelectorTerm{
			MatchExpressions: expressions,
		})
	}

	return &v1.VolumeNodeAffinity{
		Required: &v1.NodeSelector{
			NodeSelectorTerms: terms,
		},
	}
}

func setGuestClusterDetailsOnSupervisorPVC(ctx context.Context, metadataSyncer *metadataSyncInformer,
	supervisorNamespace string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("FullSync: Querying guest cluster API server for all PV objects.")
	pvList, err := getPVsInBoundAvailableOrReleased(ctx, metadataSyncer)
	if err != nil {
		log.Errorf("FullSync: Failed to get PVs from guest cluster. Err: %v", err)
		return err
	}
	for _, pv := range pvList {
		if pv.Spec.ClaimRef != nil && pv.Status.Phase == v1.VolumeBound {
			svPVC, err := metadataSyncer.supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).
				Get(ctx, pv.Spec.CSI.VolumeHandle, metav1.GetOptions{})
			if err != nil {
				msg := fmt.Sprintf("failed to retrieve supervisor PVC %q in %q namespace. Error: %+v",
					pv.Spec.CSI.VolumeHandle, supervisorNamespace, err)
				log.Error(msg)
				continue
			}
			// Add label to Supervisor PVC that contains guest cluster details, if not present already.
			key := fmt.Sprintf("%s/%s", metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterName,
				metadataSyncer.configInfo.Cfg.GC.ClusterDistribution)
			val, ok := svPVC.Labels[key]
			if !ok || val != metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID {
				if svPVC.Labels == nil {
					svPVC.Labels = make(map[string]string)
				}
				svPVC.Labels[key] = metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID
				_, err = metadataSyncer.supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Update(
					ctx, svPVC, metav1.UpdateOptions{})
				if err != nil {
					msg := fmt.Sprintf("failed to update supervisor PVC: %q with guest cluster labels in %q namespace."+
						"Error: %+v", pv.Spec.CSI.VolumeHandle, supervisorNamespace, err)
					log.Error(msg)
					continue
				}
			}
			// If SVPVCSnapshotProtectionFinalizer FSS is enabled, add finalizer "cns.vmware.com/pvc-protection"
			// to Supervisor PVC, if not present already, to prevent accidental deletion from supervisor cluster.
			// This is to handle upgrade case, where finalizer gets applied to existing PVCs from earlier TKR release.
			if metadataSyncer.coCommonInterface.IsFSSEnabled(ctx, common.SVPVCSnapshotProtectionFinalizer) {
				cnsFinalizerPresent := slices.Contains(svPVC.ObjectMeta.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
				if !cnsFinalizerPresent {
					svPVC.ObjectMeta.Finalizers = append(svPVC.ObjectMeta.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
				}
				if !cnsFinalizerPresent {
					_, err = metadataSyncer.supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Update(
						ctx, svPVC, metav1.UpdateOptions{})
					if err != nil {
						msg := fmt.Sprintf("failed to update supervisor PVC: %q with guest cluster labels in %q namespace."+
							" Error: %+v", pv.Spec.CSI.VolumeHandle, supervisorNamespace, err)
						log.Error(msg)
						continue
					}
				}
			}
		}
	}
	return nil
}

// compareCnsVolumeMetadatas compares input cnsvolumemetadata objects
// and returns false if their labels are not deeply equal.
func compareCnsVolumeMetadatas(guestObject *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec,
	supervisorObject *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec) bool {
	if !reflect.DeepEqual(guestObject.Labels, supervisorObject.Labels) ||
		!reflect.DeepEqual(guestObject.ClusterDistribution, supervisorObject.ClusterDistribution) {
		supervisorObject.Labels = guestObject.Labels
		supervisorObject.ClusterDistribution = guestObject.ClusterDistribution
		return false
	}
	return true
}

func setGuestClusterDetailsOnSupervisorSnapshot(ctx context.Context, metadataSyncer *metadataSyncInformer,
	supervisorNamespace string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("Fullsync: Querying guest cluster API server for all VSC objects.")
	// Create snaphotter client for guest cluster
	snapshotterClient, err := k8s.NewSnapshotterClient(ctx)
	if err != nil {
		log.Errorf("setGuestClusterDetailsOnSupervisorSnapshot: failed to get snapshotterClient with error: %v", err)
		return err
	}
	vscList, err := snapshotterClient.SnapshotV1().VolumeSnapshotContents().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Errorf("setGuestClusterDetailsOnSupervisorSnapshot: failed to list VolumeSnapshot with"+
			" error: %v in namespace", err)
		return err
	}
	// Create snaphotter client for supervisor cluster
	restClientConfig := k8s.GetRestClientConfigForSupervisor(ctx,
		metadataSyncer.configInfo.Cfg.GC.Endpoint, metadataSyncer.configInfo.Cfg.GC.Port)
	supervisorSnapshotterClient, err := k8s.NewSupervisorSnapshotClient(ctx, restClientConfig)
	if err != nil {
		log.Errorf("setGuestClusterDetailsOnSupervisorSnapshot: failed to create supervisorSnapshotterClient. "+
			"Error: %+v", err)
		return err
	}
	for _, vsc := range vscList.Items {
		if (vsc.Spec.VolumeSnapshotRef.Name != "") &&
			(vsc.Status != nil && vsc.Status.ReadyToUse != nil && *vsc.Status.ReadyToUse) {
			svVS, err := supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).
				Get(ctx, *vsc.Status.SnapshotHandle, metav1.GetOptions{})
			if err != nil {
				msg := fmt.Sprintf("setGuestClusterDetailsOnSupervisorSnapshot: failed to retrieve supervisor"+
					" Snapshot %q in %q namespace. Error: %+v", *vsc.Status.SnapshotHandle, supervisorNamespace,
					err)
				log.Error(msg)
				continue
			}
			// Add label to Supervisor Snapshot that contains guest cluster details, if not present already.
			tkcLabelPresent := true
			key := fmt.Sprintf("%s/%s", metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterName,
				metadataSyncer.configInfo.Cfg.GC.ClusterDistribution)
			if val, ok := svVS.Labels[key]; !ok || val != metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID {
				if svVS.Labels == nil {
					svVS.Labels = make(map[string]string)
				}
				svVS.Labels[key] = metadataSyncer.configInfo.Cfg.GC.TanzuKubernetesClusterUID
				tkcLabelPresent = false
			}
			// Add finalizer "cns.vmware.com/volumesnapshot-protection" to Supervisor snapshot, if not present already,
			// to prevent accidental deletion from supervisor cluster
			cnsFinalizerPresent := slices.Contains(svVS.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
			if !cnsFinalizerPresent {
				svVS.ObjectMeta.Finalizers = append(svVS.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
			}
			if !cnsFinalizerPresent || !tkcLabelPresent {
				_, err = supervisorSnapshotterClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Update(
					ctx, svVS, metav1.UpdateOptions{})
				if err != nil {
					msg := fmt.Sprintf("failed to update supervisor Snapshot: %q with guest cluster labels "+
						"in %q namespace. Error: %+v", *vsc.Status.SnapshotHandle, supervisorNamespace, err)
					log.Error(msg)
					continue
				}
			}
		}
	}
	return nil
}
