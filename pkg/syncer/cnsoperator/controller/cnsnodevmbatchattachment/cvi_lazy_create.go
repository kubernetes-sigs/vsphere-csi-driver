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

	"github.com/vmware/govmomi/vim25/mo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfo "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// VMHasVCenterSnapshots queries vCenter for the given VM and reports whether
// its snapshot tree is non-empty. This uses the VirtualMachine managed object
// property rather than listing K8s VirtualMachineSnapshot CRs.
func VMHasVCenterSnapshots(ctx context.Context,
	vm *cnsvsphere.VirtualMachine) (bool, error) {
	var vmMo mo.VirtualMachine
	err := vm.VirtualMachine.Properties(ctx,
		vm.VirtualMachine.Reference(), []string{"snapshot"}, &vmMo)
	if err != nil {
		return false, err
	}
	return vmMo.Snapshot != nil, nil
}

// IsSnapshotRevertInducedDetach reports whether the per-volume status
// conditions indicate that the disk was dropped by a snapshot revert rather
// than by an explicit user request. It looks for a VolumeDetached=True
// condition with the DroppedBySnapshotRevert reason.
func IsSnapshotRevertInducedDetach(conditions []metav1.Condition) bool {
	for _, c := range conditions {
		if c.Type == bav1alpha1.ConditionDetached &&
			c.Status == "True" &&
			c.Reason == bav1alpha1.ReasonDroppedBySnapshotRevert {
			return true
		}
	}
	return false
}

// MaybeLazilyCreateCVI creates a CsiVolumeInfo CR for the volume after a
// successful legacy detach — but only when both preconditions are met:
//  1. The detach was user-initiated, not caused by a snapshot revert.
//  2. The VM's vCenter snapshot tree is empty (no vSphere snapshots exist
//     that could trigger a future revert requiring legacy semantics).
//
// A failure to create the CVI is logged but not propagated; the CVI can
// be created later during PV bind reconciliation.
func MaybeLazilyCreateCVI(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
	volumeManager volumes.Manager,
	namespace string,
	volumeID, pvcName, pvName, pvUID string,
	detachConditions []metav1.Condition,
) {
	log := logger.GetLogger(ctx)

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		return
	}
	if IsSnapshotRevertInducedDetach(detachConditions) {
		log.Debugf("MaybeLazilyCreateCVI: skipping volume %q — detach was revert-induced",
			volumeID)
		return
	}

	hasSnaps, err := VMHasVCenterSnapshots(ctx, vm)
	if err != nil {
		log.Warnf("MaybeLazilyCreateCVI: failed to check vCenter snapshot tree "+
			"for VM %v: %v", vm.VirtualMachine.Reference(), err)
		return
	}
	if hasSnaps {
		log.Debugf("MaybeLazilyCreateCVI: skipping volume %q — VM %v still has "+
			"vCenter snapshots", volumeID, vm.VirtualMachine.Reference())
		return
	}

	cviSvc, err := csivolumeinfo.InitCsiVolumeInfoService(ctx)
	if err != nil {
		log.Errorf("MaybeLazilyCreateCVI: failed to get CsiVolumeInfo service: %v", err)
		return
	}

	exists, err := cviSvc.CsiVolumeInfoExists(ctx, namespace, volumeID)
	if err != nil {
		log.Warnf("MaybeLazilyCreateCVI: failed to check CsiVolumeInfo for %q: %v",
			volumeID, err)
		return
	}
	if exists {
		return
	}

	diskUUID, diskPath, fcdErr := common.QueryFCDBackingInfo(ctx, volumeManager, volumeID)
	if fcdErr != nil {
		log.Errorf("MaybeLazilyCreateCVI: failed to query FCD backing for %q: %v",
			volumeID, fcdErr)
		return
	}

	cvi := csivolumeinfo.BuildCsiVolumeInfo(
		volumeID, pvcName, namespace, pvName, pvUID, diskUUID, diskPath)
	if createErr := cviSvc.CreateCsiVolumeInfo(ctx, cvi); createErr != nil {
		log.Errorf("MaybeLazilyCreateCVI: failed to create CsiVolumeInfo for %q: %v",
			volumeID, createErr)
	}
}
