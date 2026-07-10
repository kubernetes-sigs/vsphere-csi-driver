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

package internal

import (
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// VolumeAttributesClassKind is the kind for a VolumeAttributesClass resource.
	VolumeAttributesClassKind = "VolumeAttributesClass"

	// VolumeAttributesClassGroupVersion is the API group and version for a
	// VolumeAttributesClass resource.
	VolumeAttributesClassGroupVersion = StorageClassGroup + "/v1"
)

// GetOwnerRefForVAC returns an OwnerRef for the provided VolumeAttributesClass. It is
// recorded on the same ConfigMap as GetOwnerRefForStorageClass, distinguished by Kind, so
// existing consumers that filter owner refs by StorageClassKind are unaffected.
func GetOwnerRefForVAC(vac *storagev1.VolumeAttributesClass) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: VolumeAttributesClassGroupVersion,
		Kind:       VolumeAttributesClassKind,
		Name:       vac.Name,
		UID:        vac.UID,
	}
}
