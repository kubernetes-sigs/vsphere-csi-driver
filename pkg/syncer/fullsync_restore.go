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

package syncer

import (
	"context"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	cnsvolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo/v1alpha1"
)

// isSnapServiceRestoreHandoverEnabled returns true if the full sync runs the restore handover and
// maintains the SnapshotSizeRestored condition.
func isSnapServiceRestoreHandoverEnabled(ctx context.Context) bool {
	return commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SupportsSnapServiceLocalProtection)
}

// isRestoredPV returns true if pv was created by a SnapService restore.
func isRestoredPV(pv *v1.PersistentVolume) bool {
	return pv.Annotations[cnsvolumeinfov1alpha1.AnnSnapServiceRestore] == "true"
}

// handoverRestoredVolumeInFullSync creates the CnsVolumeInfo of a restored volume whose CnsRegisterVolume
// stopped, or was deleted, after the PVC was bound and before its CnsVolumeInfo was created. It runs the
// same handover as the register controller, taking the restore stamp from the PV. Failures are logged
// and retried by the next full sync; until then the CnsDeletedVolumeInfo still holds the charge.
func handoverRestoredVolumeInFullSync(ctx context.Context, k8sClient clientset.Interface, vc commontypes.FQDN,
	pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim, storagePolicyID string,
	capacity *resource.Quantity) {
	log := logger.GetLogger(ctx)
	volumeID := pv.Spec.CSI.VolumeHandle
	log.Infof("FullSync for VC %s: PV %s of volume %q is a SnapService restore without a CnsVolumeInfo; "+
		"running the restore handover", vc, pv.Name, volumeID)
	live, err := k8sClient.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		log.Infof("FullSync for VC %s: PV %s is gone; not creating the CnsVolumeInfo of volume %q",
			vc, pv.Name, volumeID)
		return
	}
	if err != nil {
		log.Errorf("FullSync for VC %s: failed to read PV %s, will retry in the next full sync. Error: %v",
			vc, pv.Name, err)
		return
	}
	if live.DeletionTimestamp != nil {
		log.Infof("FullSync for VC %s: PV %s is being deleted; not creating the CnsVolumeInfo of volume %q",
			vc, pv.Name, volumeID)
		return
	}
	restoredBy := live.Annotations[cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume]
	if restoredBy == "" {
		// Without the stamp, a later retry of the register controller could not recognise the
		// CnsVolumeInfo as its own. The CnsDeletedVolumeInfo keeps counting meanwhile.
		log.Errorf("FullSync for VC %s: restored PV %s has no %s annotation; not creating the CnsVolumeInfo "+
			"of volume %q", vc, pv.Name, cnsvolumeinfov1alpha1.AnnRestoredByCnsRegisterVolume, volumeID)
		return
	}
	if pvc.Spec.StorageClassName == nil {
		log.Warnf("FullSync for VC %s: failed to create the CnsVolumeInfo of restored volume %q. "+
			"StorageClassName not found in the PVC spec %v.", vc, volumeID, pvc.Spec)
		return
	}
	result, err := volumeInfoService.CreateOrUpdateRestoredVolumeInfo(ctx, cnsvolumeinfo.RestoredVolumeInfo{
		VolumeID:         volumeID,
		Namespace:        pvc.Namespace,
		StoragePolicyID:  storagePolicyID,
		StorageClassName: *pvc.Spec.StorageClassName,
		VCenter:          vc.String(),
		Capacity:         capacity,
		RestoredBy:       restoredBy,
	})
	if err != nil {
		log.Errorf("FullSync for VC %s: restore handover failed for volume %q, will retry in the next full "+
			"sync. Error: %v", vc, volumeID, err)
		return
	}
	if !result.SnapshotSizeRestored() {
		log.Errorf("FullSync for VC %s: snapshot size of restored volume %q was not copied (%s): %s. The "+
			"volume is undercounted until CNS reports its snapshot size", vc, volumeID,
			result.Condition.Reason, result.Condition.Message)
	}
}

// markRestoredSnapshotSizesMeasured sets SnapshotSizeRestored to True on every restored CnsVolumeInfo
// in volumeInfos whose condition is False, once it holds a valid snapshot size. volumeInfos is only
// used to pick the candidates; each one is read again from the API server, so a value written by
// this full sync, or by any other writer, counts.
func markRestoredSnapshotSizesMeasured(ctx context.Context,
	volumeInfos map[string]*cnsvolumeinfov1alpha1.CNSVolumeInfo) {
	log := logger.GetLogger(ctx)
	for volumeID, info := range volumeInfos {
		condition := apimeta.FindStatusCondition(info.Status.Conditions,
			cnsvolumeinfov1alpha1.ConditionSnapshotSizeRestored)
		if condition == nil || condition.Status != metav1.ConditionFalse {
			continue
		}
		log.Infof("FullSync: CnsVolumeInfo of restored volume %q has %s=False (%s); checking for a valid "+
			"snapshot size", volumeID, condition.Type, condition.Reason)
		if err := volumeInfoService.MarkRestoredSnapshotSizeMeasured(ctx, volumeID); err != nil {
			log.Errorf("FullSync: failed to update %s of volume %q, will retry in the next full sync. "+
				"Error: %v", condition.Type, volumeID, err)
		}
	}
}
