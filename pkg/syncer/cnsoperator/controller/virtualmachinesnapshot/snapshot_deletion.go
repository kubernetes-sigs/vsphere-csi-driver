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

package virtualmachinesnapshot

import (
	"context"
	"encoding/json"
	"fmt"

	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apitypes "k8s.io/apimachinery/pkg/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"

	bactrl "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/controller/cnsnodevmbatchattachment"
)

// vmSnapshotDiskEntry mirrors the per-disk entry in VMSnap.status.disks that
// is populated by vm-operator. Because the field does not yet exist in the
// typed v1alpha5 API we unmarshal it via a local struct from the raw JSON
// status.
type vmSnapshotDiskEntry struct {
	// ID is the VirtualDisk.Backing.Uuid (diskUUID).
	ID string `json:"id"`
	// Key is the VirtualDisk device key on the VM.
	Key int32 `json:"key"`
}

// reconcileSnapshotDeletion is called when a VirtualMachineSnapshot is being
// deleted after vm-operator has already removed its finalizer, meaning the
// vCenter snapshot deletion is complete. CSI re-evaluates each disk that was
// part of the snapshot:
//
//   - If another snapshot still references the disk, leave it as
//     snapshot-retained (no-op; CVI is already VM_MANAGED with vmName="").
//   - If no snapshot references the disk and the volume is not re-adopted
//     by a VM, re-register the VMDK as an FCD (CVI → CSI_MANAGED).
func (r *ReconcileVirtualMachineSnapshot) reconcileSnapshotDeletion(
	ctx context.Context, log *zap.SugaredLogger,
	vmsnapshot *vmoperatortypes.VirtualMachineSnapshot,
) error {
	log.Infof("reconcileSnapshotDeletion: starting for vmsnapshot %s/%s",
		vmsnapshot.Namespace, vmsnapshot.Name)

	// Extract per-disk list from status. The disks field is not in the
	// typed API yet, so we unmarshal from the raw status JSON.
	disks, err := extractSnapshotDisks(vmsnapshot)
	if err != nil {
		return fmt.Errorf("reconcileSnapshotDeletion: failed to extract disk list from "+
			"vmsnapshot %s/%s: %w", vmsnapshot.Namespace, vmsnapshot.Name, err)
	}

	if len(disks) == 0 {
		log.Infof("reconcileSnapshotDeletion: vmsnapshot %s/%s has no disks in status; "+
			"nothing to re-evaluate", vmsnapshot.Namespace, vmsnapshot.Name)
		return nil
	}
	log.Infof("reconcileSnapshotDeletion: vmsnapshot %s/%s has %d disk(s) to re-evaluate",
		vmsnapshot.Namespace, vmsnapshot.Name, len(disks))

	// Fetch the VM's vCenter object to query remaining snapshots.
	// If the VM is gone, treat all disks as "no snapshot retains" (the VM
	// and its snapshots are gone).
	var retainedDiskUUIDs map[string]struct{}

	vmKey := apitypes.NamespacedName{
		Namespace: vmsnapshot.Namespace,
		Name:      vmsnapshot.Spec.VMName,
	}

	vm, vmErr := r.getVirtualMachineV1alpha5(ctx, vmKey)
	if vmErr != nil && !apierrors.IsNotFound(vmErr) {
		return fmt.Errorf("reconcileSnapshotDeletion: failed to get VM %s/%s: %w",
			vmsnapshot.Namespace, vmsnapshot.Spec.VMName, vmErr)
	}

	if vm != nil && r.vcenter != nil {
		// Look up the govmomi VM handle by instance UUID for the snapshot tree query.
		instanceUUID := vm.Status.InstanceUUID
		if instanceUUID == "" {
			log.Warnf("reconcileSnapshotDeletion: VM %s/%s has no instance UUID; "+
				"treating all disks as not retained", vmsnapshot.Namespace, vmsnapshot.Spec.VMName)
		} else {
			vcVM, lookupErr := cnsvsphere.GetVirtualMachineByUUID(ctx, instanceUUID, true)
			if lookupErr != nil {
				log.Warnf("reconcileSnapshotDeletion: could not look up VM by instance UUID %q; "+
					"treating all disks as not retained. Err: %v", instanceUUID, lookupErr)
			} else {
				var snapErr error
				retainedDiskUUIDs, snapErr = cnsoperatorutil.GetRetainedDiskUUIDs(ctx, vcVM)
				if snapErr != nil {
					return fmt.Errorf("reconcileSnapshotDeletion: failed to query snapshot tree "+
						"for VM %s/%s: %w", vmsnapshot.Namespace, vmsnapshot.Spec.VMName, snapErr)
				}
			}
		}
	} else {
		// VM not found or vcenter not initialised — assume no retaining snapshots.
		log.Infof("reconcileSnapshotDeletion: VM %s/%s not found or vCenter unavailable; "+
			"treating all disks as not retained",
			vmsnapshot.Namespace, vmsnapshot.Spec.VMName)
		retainedDiskUUIDs = map[string]struct{}{}
	}

	// Process each disk.
	var errs []error
	for _, disk := range disks {
		if err := r.processDiskAfterSnapshotDeletion(ctx, log, vmsnapshot.Namespace,
			disk.ID, retainedDiskUUIDs); err != nil {
			log.Errorf("reconcileSnapshotDeletion: failed to process disk %q in vmsnapshot %s/%s: %v",
				disk.ID, vmsnapshot.Namespace, vmsnapshot.Name, err)
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("reconcileSnapshotDeletion: %d disk(s) failed to process for "+
			"vmsnapshot %s/%s (first error: %w)",
			len(errs), vmsnapshot.Namespace, vmsnapshot.Name, errs[0])
	}

	log.Infof("reconcileSnapshotDeletion: completed processing for vmsnapshot %s/%s",
		vmsnapshot.Namespace, vmsnapshot.Name)
	return nil
}

// processDiskAfterSnapshotDeletion evaluates a single disk after the parent
// VMSnap has been deleted from vCenter. It re-registers the disk as an FCD
// if no other snapshot still references it and it is not re-adopted by a VM.
func (r *ReconcileVirtualMachineSnapshot) processDiskAfterSnapshotDeletion(
	ctx context.Context, log *zap.SugaredLogger,
	namespace, diskUUID string,
	retainedDiskUUIDs map[string]struct{},
) error {
	// Look up CVI by the disk-uuid label (O(1) indexed label selector).
	cvi, err := r.cviService.GetCsiVolumeInfoByDiskUUID(ctx, namespace, diskUUID)
	if err != nil {
		return fmt.Errorf("processDiskAfterSnapshotDeletion: failed to get CVI for diskUUID %q "+
			"in namespace %q: %w", diskUUID, namespace, err)
	}
	if cvi == nil {
		// No CVI means the volume was provisioned before the VMOwnedVolumes
		// feature was enabled, or it has already been processed. Either way
		// there is nothing to do.
		log.Infof("processDiskAfterSnapshotDeletion: no CVI found for diskUUID %q in namespace %q; "+
			"skipping", diskUUID, namespace)
		return nil
	}

	// If another snapshot still references this disk, leave it as
	// snapshot-retained. The CVI stays in VM_MANAGED with vmName="".
	if _, retained := retainedDiskUUIDs[diskUUID]; retained {
		log.Infof("processDiskAfterSnapshotDeletion: diskUUID %q is still retained by "+
			"another snapshot in namespace %q; no-op", diskUUID, namespace)
		return nil
	}

	// If the volume has been re-adopted by a VM (Workflow E ran concurrently),
	// vmName will be set again. Skip re-registration in that case.
	if cvi.Status.VMName != "" {
		log.Infof("processDiskAfterSnapshotDeletion: diskUUID %q (CVI %s/%s) has vmName=%q; "+
			"re-adopted by VM — no-op", diskUUID, namespace, cvi.Name, cvi.Status.VMName)
		return nil
	}

	// No snapshot retains the disk and it is not re-adopted. Re-register the
	// VMDK as an FCD to return the volume to full CSI management.
	log.Infof("processDiskAfterSnapshotDeletion: re-registering diskUUID %q (CVI %s/%s) as FCD",
		diskUUID, namespace, cvi.Name)
	if err := bactrl.ReregisterVolumeAsFCD(ctx,
		r.volumeManager,
		r.cviService,
		r.client,
		cvi,
		r.configInfo,
	); err != nil {
		return fmt.Errorf("processDiskAfterSnapshotDeletion: ReregisterVolumeAsFCD failed "+
			"for diskUUID %q (CVI %s/%s): %w", diskUUID, namespace, cvi.Name, err)
	}
	log.Infof("processDiskAfterSnapshotDeletion: successfully re-registered diskUUID %q "+
		"(CVI %s/%s)", diskUUID, namespace, cvi.Name)
	return nil
}

// extractSnapshotDisks parses the per-disk list from the VMSnap status.
// The disks field is not in the typed v1alpha5 API, so we use the raw JSON
// annotation approach via the unstructured status map.
func extractSnapshotDisks(
	vmsnapshot *vmoperatortypes.VirtualMachineSnapshot,
) ([]vmSnapshotDiskEntry, error) {
	rawStatus, err := json.Marshal(vmsnapshot.Status)
	if err != nil {
		return nil, fmt.Errorf("extractSnapshotDisks: failed to marshal status: %w", err)
	}

	var statusMap map[string]json.RawMessage
	if err := json.Unmarshal(rawStatus, &statusMap); err != nil {
		return nil, fmt.Errorf("extractSnapshotDisks: failed to unmarshal status map: %w", err)
	}

	rawDisks, ok := statusMap["disks"]
	if !ok || string(rawDisks) == "null" {
		return nil, nil
	}

	var disks []vmSnapshotDiskEntry
	if err := json.Unmarshal(rawDisks, &disks); err != nil {
		return nil, fmt.Errorf("extractSnapshotDisks: failed to unmarshal disk list: %w", err)
	}
	return disks, nil
}
