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
	"testing"

	"github.com/stretchr/testify/assert"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestGetOwnerRefForVAC(t *testing.T) {
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-vac",
			UID:  types.UID("11111111-2222-3333-4444-555555555555"),
		},
	}

	ref := GetOwnerRefForVAC(vac)

	assert.Equal(t, VolumeAttributesClassKind, ref.Kind)
	assert.Equal(t, VolumeAttributesClassGroupVersion, ref.APIVersion)
	assert.Equal(t, "my-vac", ref.Name)
	assert.Equal(t, types.UID("11111111-2222-3333-4444-555555555555"), ref.UID)

	// The VAC owner ref must be distinguishable by Kind from a StorageClass owner ref on the
	// same shared ConfigMap, so external consumers filtering by Kind=="StorageClass" (e.g.
	// vm-operator's GetEncryptedStorageClassRefs) transparently ignore VAC entries.
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-vac", UID: types.UID("11111111-2222-3333-4444-555555555555")},
	}
	scRef := GetOwnerRefForStorageClass(sc)
	assert.NotEqual(t, scRef.Kind, ref.Kind)
}
