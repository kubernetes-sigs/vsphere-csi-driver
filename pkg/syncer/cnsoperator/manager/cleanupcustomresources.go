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

package manager

import (
	"context"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	nodeattachv1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	batchattachv1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsunregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

// cleanUpCnsRegisterVolumeInstances cleans up successful CnsRegisterVolume instances
// whose creation time is past time specified in timeInMin
func cleanUpCnsRegisterVolumeInstances(ctx context.Context, restClientConfig *rest.Config, timeInMin int) {
	log := logger.GetLogger(ctx)
	log.Infof("cleanUpCnsRegisterVolumeInstances: start")
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
		return
	}

	// Get list of CnsRegisterVolume instances from all namespaces
	cnsRegisterVolumesList := &cnsregistervolumev1alpha1.CnsRegisterVolumeList{}
	err = cnsOperatorClient.List(ctx, cnsRegisterVolumesList)
	if err != nil {
		log.Warnf("Failed to get CnsRegisterVolumes from supervisor cluster. Err: %+v", err)
		return
	}

	currentTime := time.Now()
	for _, cnsRegisterVolume := range cnsRegisterVolumesList.Items {
		var elapsedMinutes float64 = currentTime.Sub(cnsRegisterVolume.CreationTimestamp.Time).Minutes()
		if cnsRegisterVolume.Status.Registered && int(elapsedMinutes)-timeInMin >= 0 {
			err = cnsOperatorClient.Delete(ctx, &cnsregistervolumev1alpha1.CnsRegisterVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cnsRegisterVolume.Name,
					Namespace: cnsRegisterVolume.Namespace,
				},
			})
			if err != nil {
				log.Warnf("Failed to delete CnsRegisterVolume: %s on namespace: %s. Error: %v",
					cnsRegisterVolume.Name, cnsRegisterVolume.Namespace, err)
				continue
			}
			log.Infof("Successfully deleted CnsRegisterVolume: %s on namespace: %s",
				cnsRegisterVolume.Name, cnsRegisterVolume.Namespace)
		}
	}
}

// cleanUpCnsUnregisterVolumeInstances cleans up successful CnsUnregisterVolume instances
// whose creation time is past time specified in timeInMin
func cleanUpCnsUnregisterVolumeInstances(ctx context.Context, restClientConfig *rest.Config, timeInMin int) {
	log := logger.GetLogger(ctx)
	log.Infof("cleanUpCnsUnregisterVolumeInstances: start")
	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restClientConfig, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Errorf("Failed to create CnsOperator client. Err: %+v", err)
		return
	}

	// Get list of CnsUnregisterVolume instances from all namespaces
	cnsUnregisterVolumesList := &cnsunregistervolumev1alpha1.CnsUnregisterVolumeList{}
	err = cnsOperatorClient.List(ctx, cnsUnregisterVolumesList)
	if err != nil {
		log.Warnf("Failed to get CnsUnregisterVolumes from supervisor cluster. Err: %+v", err)
		return
	}

	currentTime := time.Now()
	for _, cnsUnregisterVolume := range cnsUnregisterVolumesList.Items {
		var elapsedMinutes float64 = currentTime.Sub(cnsUnregisterVolume.CreationTimestamp.Time).Minutes()
		if cnsUnregisterVolume.Status.Unregistered && int(elapsedMinutes)-timeInMin >= 0 {
			err = cnsOperatorClient.Delete(ctx, &cnsunregistervolumev1alpha1.CnsUnregisterVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cnsUnregisterVolume.Name,
					Namespace: cnsUnregisterVolume.Namespace,
				},
			})
			if err != nil {
				log.Warnf("Failed to delete CnsUnregisterVolume: %s on namespace: %s. Error: %v",
					cnsUnregisterVolume.Name, cnsUnregisterVolume.Namespace, err)
				continue
			}
			log.Infof("Successfully deleted CnsUnregisterVolume: %s on namespace: %s",
				cnsUnregisterVolume.Name, cnsUnregisterVolume.Namespace)
		}
	}
}

var (
	newClientForGroup = k8s.NewClientForGroup
	newForConfig      = func(config *rest.Config) (kubernetes.Interface, error) {
		return kubernetes.NewForConfig(config)
	}
)

// cleanupOrphanedBatchAttachPVCs removes the `CNSPvcFinalizer` from the PVCs in cases
// where the CnsNodeVmBatchAttachment CR gets deleted before the PVCs.
// This is EXTREMELY UNLIKELY to happen but in case of concurrent updates and deletes of the CR,
// it was observed that the CRs could sometime lose track of the PVCs that could lead to this.
// Even though #3784(https://github.com/kubernetes-sigs/vsphere-csi-driver/pull/3784) addressed the root cause,
// this routine is a SAFETY GUARDRAIL to avoid situations where namespaced could remain stuck in terminating state.
// This routine usually runs every 10 minutes.
func cleanupOrphanedBatchAttachPVCs(ctx context.Context, config rest.Config) {
	log := logger.GetLogger(ctx)

	pvcList := commonco.ContainerOrchestratorUtility.ListPVCs(ctx, "")
	if len(pvcList) == 0 {
		log.Debug("no PVCs found in cluster, skipping cleanup cycle")
		return
	}

	// map to hold all the PVCs that are orphaned by CnsNodeVmBatchAttachment reconciler.
	// structure of the map follows the logical grouping kubernetes uses.
	// namespace -> pvc -> struct{}{}
	orphanPVCs := make(map[string]map[string]struct{})
	// prefix for the annotation that indicates the PVC is used by a VM.
	usedByVmAnnotationPrefix := "cns.vmware.com/usedby-vm-"
	for _, pvc := range pvcList {
		if pvc.DeletionTimestamp == nil {
			// PVC is not being deleted. Can be ignored.
			continue
		}

		if !controllerutil.ContainsFinalizer(pvc, cnsoperatortypes.CNSPvcFinalizer) {
			// not a PVC that is attached to or being attached to a VM. Can be ignored.
			continue
		}

		isBatchAttachPVC := false
		for key := range pvc.GetAnnotations() {
			if !strings.HasPrefix(key, usedByVmAnnotationPrefix) {
				continue
			}

			// only PVCs that are attached to VMs by CnsNodeVmBatchAttachment CR have the annotation set.
			// Example: `cns.vmware.com/usedby-vm-ae6c8201-b485-462c-a93b-d4342b16cd68: ""`
			isBatchAttachPVC = true
			break
		}

		if !isBatchAttachPVC {
			// not a PVC that was processed by CnsNodeVmBatchAttachment. Can be ignored.
			continue
		}

		if _, ok := orphanPVCs[pvc.Namespace]; !ok {
			orphanPVCs[pvc.Namespace] = map[string]struct{}{}
		}
		orphanPVCs[pvc.Namespace][pvc.Name] = struct{}{}
	}

	if len(orphanPVCs) == 0 {
		log.Debug("no orphan PVCs found in cluster, skipping cleanup cycle")
		return
	}

	cnsClient, err := newClientForGroup(ctx, &config, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Error("failed to create cns operator client")
		return
	}

	// Iterate over namespaces that have candidate orphan PVCs
	for namespace := range orphanPVCs {
		batchAttachList := batchattachv1a1.CnsNodeVMBatchAttachmentList{}
		err = cnsClient.List(ctx, &batchAttachList, ctrlclient.InNamespace(namespace))
		if err != nil {
			log.With("kind", batchAttachList.Kind).With("namespace", namespace).Error("listing failed")
			continue
		}

		for _, cr := range batchAttachList.Items {
			for _, vol := range cr.Spec.Volumes {
				// Any PVCs that are still in the spec can be safely ignored by this routine
				// to be processed by the reconciler.
				delete(orphanPVCs[cr.Namespace], vol.PersistentVolumeClaim.ClaimName)
			}

			for _, vol := range cr.Status.VolumeStatus {
				// Any PVCs that are still in the status can be safely ignored by this routine
				// to be processed by the reconciler.
				delete(orphanPVCs[cr.Namespace], vol.PersistentVolumeClaim.ClaimName)
			}
		}
	}

	c, err := newForConfig(&config)
	if err != nil {
		log.Error("failed to create core API client")
		return
	}

	// All the PVCs that are remaining in the map are eligible orphans whose finalizer can be removed.
	for namespace, pvcs := range orphanPVCs {
		for name := range pvcs {
			err := k8s.RemoveFinalizerFromPVC(ctx, c, name, namespace, cnsoperatortypes.CNSPvcFinalizer)
			if err != nil {
				log.With("name", name).With("namespace", namespace).
					Error("failed to remove the finalizer")
				// safe to continue as failure to remove finalizer of a pvc doesn't influence others
				continue
			}
		}
	}
}

// cleanupOrphanedNodeAttachPVCs removes the `CNSPvcFinalizer` from the PVCs in cases
// where the CnsNodeVmAttachment CR gets deleted before the PVCs.
// This is EXTREMELY UNLIKELY to happen but in case of concurrent updates and deletes of the CR,
// it was observed that the CRs could sometime lose track of the PVCs that could lead to this.
// This routine is a SAFETY GUARDRAIL to avoid situations where namespaces could remain stuck in terminating state.
// This routine usually runs every 10 minutes.
func cleanupOrphanedNodeAttachPVCs(ctx context.Context, config rest.Config) {
	log := logger.GetLogger(ctx)

	pvcList := commonco.ContainerOrchestratorUtility.ListPVCs(ctx, "")
	if len(pvcList) == 0 {
		log.Debug("no PVCs found in cluster, skipping node attach cleanup cycle")
		return
	}

	// map to hold all the PVCs that are orphaned by CnsNodeVmAttachment reconciler.
	// structure of the map follows the logical grouping kubernetes uses.
	// namespace -> pvc -> struct{}{}
	orphanPVCs := make(map[string]map[string]struct{})

	for _, pvc := range pvcList {
		if pvc.DeletionTimestamp == nil {
			// PVC is not being deleted. Can be ignored.
			continue
		}

		if !controllerutil.ContainsFinalizer(pvc, cnsoperatortypes.CNSPvcFinalizer) {
			// not a PVC that is attached to or being attached to a VM. Can be ignored.
			continue
		}

		// For CnsNodeVmAttachment, we need to check if there's a corresponding CR
		// that references this PVC. Unlike batch attach, node attach doesn't use
		// specific annotations, so we'll identify orphaned PVCs by checking if
		// any CnsNodeVmAttachment CR exists that could be managing this PVC.

		if _, ok := orphanPVCs[pvc.Namespace]; !ok {
			orphanPVCs[pvc.Namespace] = map[string]struct{}{}
		}
		orphanPVCs[pvc.Namespace][pvc.Name] = struct{}{}
	}

	if len(orphanPVCs) == 0 {
		log.Debug("no orphan PVCs found in cluster, skipping node attach cleanup cycle")
		return
	}

	cnsClient, err := newClientForGroup(ctx, &config, cnsoperatorv1alpha1.GroupName)
	if err != nil {
		log.Error("failed to create cns operator client")
		return
	}

	// Iterate over namespaces that have candidate orphan PVCs
	for namespace := range orphanPVCs {
		nodeAttachList := nodeattachv1a1.CnsNodeVmAttachmentList{}
		err = cnsClient.List(ctx, &nodeAttachList, ctrlclient.InNamespace(namespace))
		if err != nil {
			log.With("kind", nodeAttachList.Kind).With("namespace", namespace).Error("listing failed")
			continue
		}

		// If there are any CnsNodeVmAttachment CRs in the namespace, we need to be careful
		// about which PVCs to clean up. For now, we'll take a conservative approach:
		// Only clean up PVCs if there are NO CnsNodeVmAttachment CRs in the namespace.
		// This ensures we don't accidentally remove finalizers from PVCs that might
		// still be managed by existing CRs.
		if len(nodeAttachList.Items) > 0 {
			log.With("namespace", namespace).With("nodeAttachCRCount", len(nodeAttachList.Items)).
				Debug("skipping PVC cleanup in namespace with existing CnsNodeVmAttachment CRs")
			delete(orphanPVCs, namespace)
		}
	}

	c, err := newForConfig(&config)
	if err != nil {
		log.Error("failed to create core API client")
		return
	}

	// All the PVCs that are remaining in the map are eligible orphans whose finalizer can be removed.
	for namespace, pvcs := range orphanPVCs {
		for name := range pvcs {
			err := k8s.RemoveFinalizerFromPVC(ctx, c, name, namespace, cnsoperatortypes.CNSPvcFinalizer)
			if err != nil {
				log.With("name", name).With("namespace", namespace).
					Error("failed to remove the finalizer")
				// safe to continue as failure to remove finalizer of a pvc doesn't influence others
				continue
			}
			log.With("name", name).With("namespace", namespace).
				Info("successfully removed CNS PVC finalizer from orphaned PVC")
		}
	}
}
