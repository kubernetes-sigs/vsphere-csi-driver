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
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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
