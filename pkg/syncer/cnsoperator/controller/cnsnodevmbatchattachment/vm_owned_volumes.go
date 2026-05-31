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
	"encoding/json"
	"fmt"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	// VMOwnedVolumesAnnotation is the annotation key set on VirtualMachine CRs
	// when the VMOwnedVolumes feature is enabled. Its presence signals that
	// all volume attach/detach operations on the VM use the ownership-transfer
	// path rather than the legacy CNS attach path.
	VMOwnedVolumesAnnotation = "vmoperator.vmware.com/vm-owned-volumes"
)

// IsVMOwnedVolumesVM reports whether the VirtualMachine CR identified by
// vmName in the given namespace carries the VMOwnedVolumes annotation set to
// "true".
//
// The function performs a direct O(1) Get (no list) to stay within the
// performance budget at scale. A NotFound result (VM deleted before the check)
// is treated as false rather than an error, because a missing VM means the
// attach cannot proceed regardless.
func IsVMOwnedVolumesVM(ctx context.Context, vmOperatorClient client.Client,
	namespace, vmName string) (bool, error) {
	log := logger.GetLogger(ctx)

	vm := &vmoperatortypes.VirtualMachine{}
	err := vmOperatorClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: namespace,
		Name:      vmName,
	}, vm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("IsVMOwnedVolumesVM: VirtualMachine %s/%s not found",
				namespace, vmName)
			return false, nil
		}
		return false, fmt.Errorf("IsVMOwnedVolumesVM: failed to get VirtualMachine %s/%s: %w",
			namespace, vmName, err)
	}

	val := vm.Annotations[VMOwnedVolumesAnnotation]
	result := val == "true"
	log.Infof("IsVMOwnedVolumesVM: VirtualMachine %s/%s annotation %q=%q result=%v",
		namespace, vmName, VMOwnedVolumesAnnotation, val, result)
	return result, nil
}

// VMExistsInK8s returns true if a VirtualMachine CR with the given name exists
// in the given namespace. Returns false on NotFound; returns true (conservative)
// on any other error to avoid false-positive stale-CVI recovery.
func VMExistsInK8s(ctx context.Context, vmOperatorClient client.Client,
	namespace, vmName string) bool {
	log := logger.GetLogger(ctx)
	vm := &vmoperatortypes.VirtualMachine{}
	err := vmOperatorClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: namespace,
		Name:      vmName,
	}, vm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		log.Warnf("VMExistsInK8s: error checking VM %s/%s: %v; treating as existing",
			namespace, vmName, err)
		return true
	}
	return true
}

// PatchPVCOwnershipLabel applies the cns.vmware.com/volume-ownership label to
// the named PVC in the given namespace. This is idempotent — setting a label
// to the value it already has is a no-op.
//
// Callers use the label values defined in the csivolumeinfov1alpha1 package
// (OwnershipLabelVMOwned, OwnershipLabelCSIOwned, OwnershipLabelRetainedBySnapshot).
func PatchPVCOwnershipLabel(ctx context.Context, k8sClient client.Client,
	namespace, pvcName, labelValue string) error {
	log := logger.GetLogger(ctx)

	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, k8stypes.NamespacedName{
		Namespace: namespace,
		Name:      pvcName,
	}, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			// PVC may have been deleted concurrently; log and skip rather than
			// fail. The label is a derived projection — its absence is recoverable.
			log.Warnf("PatchPVCOwnershipLabel: PVC %s/%s not found; skipping label patch",
				namespace, pvcName)
			return nil
		}
		return fmt.Errorf("PatchPVCOwnershipLabel: failed to get PVC %s/%s: %w",
			namespace, pvcName, err)
	}

	// Skip the patch if the label is already at the target value.
	if pvc.Labels[csivolumeinfov1alpha1.LabelVolumeOwnership] == labelValue {
		log.Infof("PatchPVCOwnershipLabel: PVC %s/%s label already %q=%q; no-op",
			namespace, pvcName, csivolumeinfov1alpha1.LabelVolumeOwnership, labelValue)
		return nil
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]string{
				csivolumeinfov1alpha1.LabelVolumeOwnership: labelValue,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("PatchPVCOwnershipLabel: failed to marshal patch for PVC %s/%s: %w",
			namespace, pvcName, err)
	}

	if err := k8sClient.Patch(ctx, pvc, client.RawPatch(k8stypes.MergePatchType, patchBytes)); err != nil {
		return fmt.Errorf("PatchPVCOwnershipLabel: failed to patch PVC %s/%s: %w",
			namespace, pvcName, err)
	}
	log.Infof("PatchPVCOwnershipLabel: set label %q=%q on PVC %s/%s",
		csivolumeinfov1alpha1.LabelVolumeOwnership, labelValue, namespace, pvcName)
	return nil
}

// isIndependentDiskMode reports whether the given DiskMode is an independent
// mode (independent_persistent or independent_nonpersistent). Independent-mode
// disks are not captured in snapshots and must use the legacy CnsAttachVolume
// path; they must not go through the ownership-transfer path.
func isIndependentDiskMode(mode bav1alpha1.DiskMode) bool {
	return mode == bav1alpha1.IndependentPersistent ||
		mode == bav1alpha1.DiskMode(bav1alpha1.IndependentNonPersistent)
}

// GetRetainedDiskUUIDs queries the vCenter snapshot tree of the given VM once
// and returns the set of all diskUUIDs still referenced by any remaining
// vCenter snapshot. Delegates to the shared utility implementation.
func GetRetainedDiskUUIDs(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
) (map[string]struct{}, error) {
	return cnsoperatorutil.GetRetainedDiskUUIDs(ctx, vm)
}

// IsVolumeRetainedByVCenterSnapshot reports whether any vCenter snapshot of
// the given VM still references the given diskUUID. Delegates to the shared
// utility implementation.
func IsVolumeRetainedByVCenterSnapshot(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, diskUUID string,
) (bool, error) {
	return cnsoperatorutil.IsVolumeRetainedByVCenterSnapshot(ctx, vm, diskUUID)
}
