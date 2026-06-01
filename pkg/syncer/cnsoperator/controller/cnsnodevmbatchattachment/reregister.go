/*
Copyright 2026 The Kubernetes Authors.

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

package cnsnodevmbatchattachment

import (
	"context"
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// ReregisterVolumeAsFCD re-registers a formerly VM-managed VMDK as a
// first-class disk (FCD) with full Kubernetes metadata, then transitions
// the corresponding CsiVolumeInfo to CSI_MANAGED, removes the cvi-protection
// finalizer, and labels the PVC csi-owned.
//
// This is shared between Workflow C (detach without snapshots) and Workflow D
// (snapshot deletion where no remaining snapshot retains the disk).
//
// The function is idempotent: a CnsAlreadyRegisteredFault / CnsVolumeAlreadyExistsFault
// from CNS is treated as success.
//
// Precondition: cvi.Status.DiskPath is set to the current datastore path.
// Precondition: cvi.Status.OwnershipState is TRANSFERRING_TO_CSI or VM_MANAGED.
func ReregisterVolumeAsFCD(ctx context.Context,
	volumeManager volumes.Manager,
	cviSvc csivolumeinfo.CsiVolumeInfoService,
	k8sClient client.Client,
	cvi *csivolumeinfov1alpha1.CsiVolumeInfo,
	configInfo *config.ConfigurationInfo,
) error {
	log := logger.GetLogger(ctx)
	log.Infof("ReregisterVolumeAsFCD: starting for volume %q (cvi %s/%s, diskPath=%q)",
		cvi.Spec.VolumeID, cvi.Namespace, cvi.Name, cvi.Status.DiskPath)

	// Build full PVC entity metadata by reading the live PVC.
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: cvi.Namespace,
		Name:      cvi.Spec.PVCName,
	}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			// A missing PVC at this stage is unexpected (the admission webhook
			// should have blocked deletion). Log and requeue.
			return fmt.Errorf("ReregisterVolumeAsFCD: PVC %s/%s not found; cannot reconstruct metadata: %w",
				cvi.Namespace, cvi.Spec.PVCName, err)
		}
		return fmt.Errorf("ReregisterVolumeAsFCD: failed to get PVC %s/%s: %w",
			cvi.Namespace, cvi.Spec.PVCName, err)
	}

	// Build PV entity reference.
	pv := &corev1.PersistentVolume{}
	if err := k8sClient.Get(ctx, k8stypes.NamespacedName{Name: cvi.Spec.PVName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("ReregisterVolumeAsFCD: PV %q not found; cannot reconstruct metadata: %w",
				cvi.Spec.PVName, err)
		}
		return fmt.Errorf("ReregisterVolumeAsFCD: failed to get PV %q: %w", cvi.Spec.PVName, err)
	}

	clusterID := configInfo.Cfg.Global.ClusterID
	if clusterID == "" {
		clusterID = configInfo.Cfg.Global.SupervisorID
	}

	// Find the vCenter config for this cluster to extract the vSphere user.
	var vcUser, clusterDist string
	for _, vcCfg := range configInfo.Cfg.VirtualCenter {
		vcUser = vcCfg.User
		break
	}
	clusterDist = configInfo.Cfg.Global.ClusterDistribution

	containerCluster := cnsvsphere.GetContainerCluster(
		clusterID, vcUser, cnstypes.CnsClusterFlavorWorkload, clusterDist)

	// Reconstruct entity metadata from the live PVC and PV state.
	pvRef := cnsvsphere.CreateCnsKuberenetesEntityReference(
		string(cnstypes.CnsKubernetesEntityTypePV),
		pv.Name, "", clusterID)

	pvcMeta := cnsvsphere.GetCnsKubernetesEntityMetaData(
		pvc.Name, pvc.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePVC),
		pvc.Namespace, clusterID,
		[]cnstypes.CnsKubernetesEntityReference{pvRef})

	pvMeta := cnsvsphere.GetCnsKubernetesEntityMetaData(
		pv.Name, pv.Labels, false,
		string(cnstypes.CnsKubernetesEntityTypePV),
		"", clusterID, nil)

	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       pv.Name,
		VolumeType: string(cnstypes.CnsVolumeTypeBlock),
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: []cnstypes.CnsContainerCluster{containerCluster},
			EntityMetadata:        []cnstypes.BaseCnsEntityMetadata{pvcMeta, pvMeta},
		},
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			BackingDiskId: cvi.Spec.VolumeID,
		},
	}

	log.Infof("ReregisterVolumeAsFCD: calling CreateVolume for volume %q (backing diskID=%q, diskPath=%q)",
		cvi.Spec.VolumeID, cvi.Spec.VolumeID, cvi.Status.DiskPath)

	_, faultType, err := volumeManager.CreateVolume(ctx, createSpec, nil)
	if err != nil {
		// CnsVolumeAlreadyExistsFault means the volume is already registered as
		// an FCD — treat as success (idempotent).
		// Note: CnsAlreadyRegisteredFault is handled internally by CreateVolume
		// and returns the existing volume ID without surfacing an error.
		if volumes.IsCnsVolumeAlreadyExistsFault(ctx, faultType) {
			log.Infof("ReregisterVolumeAsFCD: volume %q is already registered as FCD (idempotent); continuing",
				cvi.Spec.VolumeID)
		} else {
			return fmt.Errorf("ReregisterVolumeAsFCD: CreateVolume failed for volume %q (fault=%q): %w",
				cvi.Spec.VolumeID, faultType, err)
		}
	} else {
		log.Infof("ReregisterVolumeAsFCD: successfully re-registered volume %q as FCD", cvi.Spec.VolumeID)
	}

	// Transition CVI to CSI_MANAGED.
	cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateCSIManaged
	cvi.Status.VMName = ""
	cvi.Status.VMInstanceUUID = ""
	if err := cviSvc.UpdateCsiVolumeInfoStatus(ctx, cvi); err != nil {
		return fmt.Errorf("ReregisterVolumeAsFCD: failed to update CVI status for %q: %w",
			cvi.Spec.VolumeID, err)
	}
	log.Infof("ReregisterVolumeAsFCD: CVI %s/%s transitioned to CSI_MANAGED", cvi.Namespace, cvi.Name)

	// Remove the cvi-protection finalizer now that the volume is CSI-managed.
	if err := cviSvc.RemoveCVIProtectionFinalizer(ctx, cvi.Namespace, cvi.Spec.VolumeID); err != nil {
		return fmt.Errorf("ReregisterVolumeAsFCD: failed to remove finalizer from CVI %s/%s: %w",
			cvi.Namespace, cvi.Name, err)
	}
	log.Infof("ReregisterVolumeAsFCD: removed cvi-protection finalizer from CVI %s/%s",
		cvi.Namespace, cvi.Name)

	// Label the PVC csi-owned to reflect the new steady state.
	if err := PatchPVCOwnershipLabel(ctx, k8sClient,
		cvi.Namespace, cvi.Spec.PVCName,
		csivolumeinfov1alpha1.OwnershipLabelCSIOwned); err != nil {
		return fmt.Errorf("ReregisterVolumeAsFCD: failed to label PVC %s/%s: %w",
			cvi.Namespace, cvi.Spec.PVCName, err)
	}
	log.Infof("ReregisterVolumeAsFCD: labeled PVC %s/%s as %q",
		cvi.Namespace, cvi.Spec.PVCName, csivolumeinfov1alpha1.OwnershipLabelCSIOwned)

	// Defense-in-depth: if the PVC entered Terminating while snapshot-retained
	// (admission webhook was bypassed), release the PVC protection finalizer so
	// the standard CSI delete path can now proceed. The volume is a registered
	// FCD again and the DeleteVolume guard will allow deletion.
	if pvc.DeletionTimestamp != nil {
		log.Infof("ReregisterVolumeAsFCD: PVC %s/%s is Terminating; "+
			"releasing PVC protection finalizer", pvc.Namespace, pvc.Name)
		pvcPatch := client.MergeFrom(pvc.DeepCopy())
		controllerutil.RemoveFinalizer(pvc, cnsoperatortypes.CNSPvcFinalizer)
		if patchErr := k8sClient.Patch(ctx, pvc, pvcPatch); patchErr != nil {
			return fmt.Errorf("ReregisterVolumeAsFCD: failed to remove PVC protection "+
				"finalizer from %s/%s: %w", pvc.Namespace, pvc.Name, patchErr)
		}
		log.Infof("ReregisterVolumeAsFCD: released PVC protection finalizer from %s/%s",
			pvc.Namespace, pvc.Name)
	}

	return nil
}
