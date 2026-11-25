/*
Copyright 2024 The Kubernetes Authors.

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

package crypto

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto/internal"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

// GetEncryptionClassNameForPVC extracts the name of the encryption class associated
// with the provided PersistentVolumeClaim (PVC).
func GetEncryptionClassNameForPVC(pvc *corev1.PersistentVolumeClaim) string {
	annotations := pvc.GetAnnotations()
	if annotations == nil {
		return ""
	}
	return annotations[PVCEncryptionClassAnnotationName]
}

// SetEncryptionClassNameForPVC associates an encryption class with the specified
// PersistentVolumeClaim (PVC).
func SetEncryptionClassNameForPVC(pvc *corev1.PersistentVolumeClaim, encClassName string) {
	annotations := pvc.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	if encClassName != "" {
		annotations[PVCEncryptionClassAnnotationName] = encClassName
	} else {
		delete(annotations, PVCEncryptionClassAnnotationName)
	}

	pvc.SetAnnotations(annotations)

}

// GetStoragePolicyID retrieves the storage policy ID associated with the
// provided StorageClass.
func GetStoragePolicyID(sc *storagev1.StorageClass) string {
	if sc.Provisioner != types.Name {
		return ""
	}
	// vSphere CSI supports case insensitive parameters.
	for key, value := range sc.Parameters {
		if strings.ToLower(key) == internal.StorageClassAttributeStoragePolicyID {
			return value
		}
	}
	return ""
}

// GetStoragePolicyIDFromVAC retrieves the storage policy ID associated with the
// provided VolumeAttributesClass.
func GetStoragePolicyIDFromVAC(vac *storagev1.VolumeAttributesClass) string {
	// vSphere CSI supports case insensitive parameters.
	for key, value := range vac.Parameters {
		if strings.ToLower(key) == internal.StorageClassAttributeStoragePolicyID {
			return value
		}
	}
	return ""
}

// IsVACPolicyEffective returns true if the PVC's VolumeAttributesClassName is set and
// its policy has actually taken effect on the underlying volume. A non-nil
// pvc.Status.ModifyVolumeStatus means a ModifyVolume operation is still pending or in
// progress, so the volume may still be backed by its previous (StorageClass) policy —
// callers should keep using the StorageClass's policy until this returns true.
//
// ModifyVolumeStatus == nil alone is not sufficient: it is also the initial PVC state
// before any modify has ever been attempted. Only treat the VAC as effective once
// CurrentVolumeAttributesClassName has actually caught up to the target VAC.
func IsVACPolicyEffective(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Spec.VolumeAttributesClassName == nil || pvc.Status.ModifyVolumeStatus != nil {
		return false
	}
	return pvc.Status.CurrentVolumeAttributesClassName != nil &&
		*pvc.Status.CurrentVolumeAttributesClassName == *pvc.Spec.VolumeAttributesClassName
}

// StoragePolicySource identifies where the storage policy backing a PVC's volume comes from.
type StoragePolicySource string

const (
	// StoragePolicySourceStorageClass means the policy comes from the PVC's StorageClass.
	StoragePolicySourceStorageClass StoragePolicySource = "StorageClass"
	// StoragePolicySourceVAC means the policy comes from the PVC's VolumeAttributesClass.
	StoragePolicySourceVAC StoragePolicySource = "VolumeAttributesClass"
)

// IsEncryptedVAC returns true if the storage policy of the named VolumeAttributesClass
// supports encryption, along with the policy ID. The VolumeAttributesClass is read using
// reader, and errors from reading it (including NotFound, and NoKindMatch on clusters that
// don't serve the VolumeAttributesClass v1 API) are returned as-is.
func IsEncryptedVAC(
	ctx context.Context,
	cryptoClient Client,
	reader ctrlclient.Reader,
	name string,
) (bool, string, error) {
	vac := &storagev1.VolumeAttributesClass{}
	if err := reader.Get(ctx, ctrlclient.ObjectKey{Name: name}, vac); err != nil {
		return false, "", err
	}

	profileID := GetStoragePolicyIDFromVAC(vac)
	if profileID == "" {
		return false, "", nil
	}

	encrypted, err := cryptoClient.IsEncryptedStorageProfile(ctx, profileID)
	return encrypted, profileID, err
}

// IsEncryptedStoragePolicyForPVC determines whether the storage policy currently backing
// the PVC's volume supports encryption, and returns the policy ID and where the policy came
// from. If vacPolicyMutabilityEnabled is true and the PVC's VolumeAttributesClass has taken
// effect (IsVACPolicyEffective), its policy takes precedence over the StorageClass's,
// consistent with CSI ModifyVolume semantics. Otherwise the StorageClass's policy is used,
// since a ModifyVolume switching the PVC to the VAC's policy may still be pending or in
// progress. The VolumeAttributesClass is read using reader.
//
// If the effective VolumeAttributesClass does not exist, its NotFound error is returned so
// callers can report it. If the cluster doesn't serve the VolumeAttributesClass v1 API, the
// StorageClass's policy is used instead. A PVC without a StorageClass is reported as not
// encrypted.
func IsEncryptedStoragePolicyForPVC(
	ctx context.Context,
	cryptoClient Client,
	reader ctrlclient.Reader,
	pvc *corev1.PersistentVolumeClaim,
	vacPolicyMutabilityEnabled bool,
) (bool, string, StoragePolicySource, error) {
	if vacPolicyMutabilityEnabled && IsVACPolicyEffective(pvc) {
		encrypted, profileID, err := IsEncryptedVAC(ctx, cryptoClient, reader, *pvc.Spec.VolumeAttributesClassName)
		if !meta.IsNoMatchError(err) {
			return encrypted, profileID, StoragePolicySourceVAC, err
		}
		// Clusters older than 1.34 don't serve the VolumeAttributesClass v1 API; keep the
		// StorageClass-only behavior there.
	}

	if pvc.Spec.StorageClassName == nil {
		return false, "", StoragePolicySourceStorageClass, nil
	}

	encrypted, profileID, err := cryptoClient.IsEncryptedStorageClass(ctx, *pvc.Spec.StorageClassName)
	return encrypted, profileID, StoragePolicySourceStorageClass, err
}
