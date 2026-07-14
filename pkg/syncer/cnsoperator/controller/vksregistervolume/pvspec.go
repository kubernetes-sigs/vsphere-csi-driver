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

package vksregistervolume

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// Labels stamped on the guest PV created by this controller.
const (
	labelVKSRegVolCreatedBy      = "cns.vmware.com/created-by"
	labelVKSRegVolCreatedByValue = "vksregistervolume"
	labelVKSRegVolCRNamespace    = "cns.vmware.com/vksregistervolume-namespace"
	labelVKSRegVolCRName         = "cns.vmware.com/vksregistervolume-name"
)

// buildGuestPV constructs the guest PersistentVolume for a VKSRegisterVolume CR (Plan Part 4e).
//
// Per the task breakdown (T7 can be implemented without T6), supervisorPVCName and
// accessibleTopology are taken as plain arguments rather than fetched here, so this builder is
// independently unit-testable against literals. Once T6 lands, Reconcile() supplies the real
// values:
//   - supervisorPVCName: TODO(T6) — CnsRegisterVolume.Spec.PvcName, read after the referenced
//     Supervisor CnsRegisterVolume reports status.Registered == true (Plan Part 4d step 4).
//   - accessibleTopology: TODO(T6) — parsed from the Supervisor PVC's
//     common.AnnVolumeAccessibleTopology annotation once that PVC reaches Bound, falling back to
//     the Supervisor PV's spec.nodeAffinity when the annotation is absent (Plan Part 4d step 5).
//     nil is a valid input (single-zone, or topology genuinely unavailable) and yields a guest PV
//     with no nodeAffinity — full-sync's AddNodeAffinityRulesOnPV backfills it later.
//
// All other fields — name, capacity, accessModes, volumeMode, storageClassName — are copied from
// the pre-created guest PVC (pvc), never recomputed, per the plan's "all guest-PV specs are
// sourced from this PVC" invariant.
func buildGuestPV(
	pvc *v1.PersistentVolumeClaim,
	guestPVName string,
	supervisorPVCName string,
	volumeMode v1.PersistentVolumeMode,
	accessibleTopology []map[string]string,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
) *v1.PersistentVolume {
	storageClassName := ""
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: guestPVName,
			Labels: map[string]string{
				labelVKSRegVolCreatedBy:   labelVKSRegVolCreatedByValue,
				labelVKSRegVolCRNamespace: instance.Namespace,
				labelVKSRegVolCRName:      instance.Name,
			},
			Annotations: map[string]string{
				"pv.kubernetes.io/provisioned-by": cnsoperatortypes.VSphereCSIDriverName,
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: pvc.Spec.Resources.Requests[v1.ResourceStorage],
			},
			AccessModes:                   pvc.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			StorageClassName:              storageClassName,
			VolumeMode:                    &volumeMode,
			ClaimRef: &v1.ObjectReference{
				Kind:       "PersistentVolumeClaim",
				APIVersion: "v1",
				Namespace:  instance.Namespace,
				Name:       instance.Spec.PVCName,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       cnsoperatortypes.VSphereCSIDriverName,
					VolumeHandle: supervisorPVCName,
					ReadOnly:     false,
				},
			},
			NodeAffinity: syncer.GenerateVolumeNodeAffinity(toCSITopology(accessibleTopology)),
		},
	}

	// FSType applies only to Filesystem volumes (mirrors getPersistentVolumeSpec in cnsregistervolume).
	if volumeMode == v1.PersistentVolumeFilesystem {
		pv.Spec.PersistentVolumeSource.CSI.FSType = "ext4"
	}

	return pv
}

// toCSITopology converts the []map[string]string accessible-topology shape (as parsed off the
// Supervisor PVC's AnnVolumeAccessibleTopology annotation) into the []*csi.Topology shape consumed
// by syncer.GenerateVolumeNodeAffinity. Returns nil for a nil/empty input.
func toCSITopology(accessibleTopology []map[string]string) []*csi.Topology {
	if len(accessibleTopology) == 0 {
		return nil
	}
	topologies := make([]*csi.Topology, 0, len(accessibleTopology))
	for _, segment := range accessibleTopology {
		topologies = append(topologies, &csi.Topology{Segments: segment})
	}
	return topologies
}

// guestPVMatchesExpected reports whether an already-existing guest PV (found on the
// GET-before-CREATE check in the CreatingGuestPV phase) still correctly points at supervisorPVCName
// and is claimed by the CR's referenced PVC. Used by Reconcile (T7 wiring, TODO below) to decide
// between "idempotent no-op" and terminal Failed when a guest PV with the expected name already
// exists but disagrees with the current CR/Supervisor state.
//
// TODO(T7 wiring / T6 dependency): call this from the CreatingGuestPV phase once
// Reconcile() has a real supervisorPVCName from T6. Today nothing calls this helper outside tests.
func guestPVMatchesExpected(pv *v1.PersistentVolume, supervisorPVCName string,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume) bool {
	if pv == nil || pv.Spec.CSI == nil {
		return false
	}
	if pv.Spec.CSI.VolumeHandle != supervisorPVCName {
		return false
	}
	if pv.Spec.ClaimRef == nil {
		return false
	}
	return pv.Spec.ClaimRef.Namespace == instance.Namespace && pv.Spec.ClaimRef.Name == instance.Spec.PVCName
}
