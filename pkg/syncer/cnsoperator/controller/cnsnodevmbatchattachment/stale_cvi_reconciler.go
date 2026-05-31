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

	"sigs.k8s.io/controller-runtime/pkg/client"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// reconcileStaleCVIs checks for CsiVolumeInfo CRs that are stuck in a transient
// state and heals them. It only processes CVIs reachable from this BA's status
// (bounded by the number of volumes on this VM, max 64), so it never performs
// an unbounded cluster-wide list.
//
// Stale conditions detected and handled:
//  1. CVI is TRANSFERRING_TO_VM or TRANSFERRING_TO_CSI, but the volume is NOT in
//     BA.spec.volumes. This means the attach/detach flow was interrupted and the
//     detach recovery path should handle it (reconcileVMOwnedVolumesDetach).
//  2. CVI is VM_MANAGED with vmName set, but the VM no longer exists. The volume
//     is orphaned; it is handled by running the detach path (snapshot-retention
//     check then re-register).
func reconcileStaleCVIs(ctx context.Context,
	cviSvc csivolumeinfo.CsiVolumeInfoService,
	volumeManager volumes.Manager,
	vmOperatorClient client.Client,
	k8sClient client.Client,
	instance *bav1alpha1.CnsNodeVMBatchAttachment,
) error {
	log := logger.GetLogger(ctx)

	// Build a set of PVC names that are in the BA spec for quick lookup.
	pvcsInSpec := make(map[string]struct{}, len(instance.Spec.Volumes))
	for _, vol := range instance.Spec.Volumes {
		pvcsInSpec[vol.PersistentVolumeClaim.ClaimName] = struct{}{}
	}

	var errs []error
	for _, vs := range instance.Status.VolumeStatus {
		pvcName := vs.PersistentVolumeClaim.ClaimName
		if _, inSpec := pvcsInSpec[pvcName]; inSpec {
			// Volume is still in spec; attach/detach reconcile handles it normally.
			continue
		}

		// Volume is in BA status but NOT in BA spec.
		volumeID := vs.PersistentVolumeClaim.CnsVolumeID
		if volumeID == "" {
			continue
		}

		cvi, err := cviSvc.GetCsiVolumeInfo(ctx, instance.Namespace, volumeID)
		if err != nil {
			log.Warnf("reconcileStaleCVIs: failed to get CVI for volume %q: %v", volumeID, err)
			continue
		}
		if cvi == nil {
			continue
		}

		switch cvi.Status.OwnershipState {
		case csivolumeinfov1alpha1.OwnershipStateCSIManaged:
			// Already returned to CSI. No action needed.
			continue

		case csivolumeinfov1alpha1.OwnershipStateVMManaged:
			if cvi.Status.VMName == "" {
				// Snapshot-retained state -- waiting for Workflow D. No action needed.
				continue
			}
			// Check whether the VM still exists. If it does, the volume is
			// legitimately on the VM (e.g., BA status not yet cleaned up after
			// a detach that is still in progress).
			if VMExistsInK8s(ctx, vmOperatorClient, instance.Namespace, cvi.Status.VMName) {
				continue
			}
			// VM no longer exists. Advance CVI to TRANSFERRING_TO_CSI so the
			// next detach reconcile can complete the re-registration. The actual
			// snapshot-retention check and re-registration is done by the standard
			// detach path when it is triggered.
			log.Warnf("reconcileStaleCVIs: VM %s/%s no longer exists; "+
				"advancing CVI for volume %q to TRANSFERRING_TO_CSI",
				instance.Namespace, cvi.Status.VMName, volumeID)
			cvi.Status.OwnershipState = csivolumeinfov1alpha1.OwnershipStateTransferringToCSI
			if updateErr := cviSvc.UpdateCsiVolumeInfoStatus(ctx, cvi); updateErr != nil {
				log.Errorf("reconcileStaleCVIs: failed to advance CVI for volume %q: %v",
					volumeID, updateErr)
				errs = append(errs, updateErr)
			}

		case csivolumeinfov1alpha1.OwnershipStateTransferringToVM,
			csivolumeinfov1alpha1.OwnershipStateTransferringToCSI:
			// Transient state + volume not in spec = potentially stuck. Log a warning.
			// The Layer-2 crash recovery check in processBatchAttach handles
			// TRANSFERRING_TO_VM; the standard detach path handles TRANSFERRING_TO_CSI.
			// Logging here provides visibility without taking any action that
			// could race with an in-progress reconcile.
			log.Warnf("reconcileStaleCVIs: volume %q is in transient state %q "+
				"and not in BA spec; Layer-2 recovery or the next reconcile will handle it",
				volumeID, cvi.Status.OwnershipState)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("reconcileStaleCVIs: %d error(s) encountered: %v", len(errs), errs)
	}
	return nil
}
