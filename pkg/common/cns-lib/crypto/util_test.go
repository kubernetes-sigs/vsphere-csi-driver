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

package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
)

func TestGetStoragePolicyIDFromVAC(t *testing.T) {
	t.Run("CamelCaseKey", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{
			Parameters: map[string]string{"storagePolicyID": "policy-123"},
		}
		assert.Equal(t, "policy-123", GetStoragePolicyIDFromVAC(vac))
	})

	t.Run("CaseInsensitiveKey", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{
			Parameters: map[string]string{"STORAGEPOLICYID": "policy-456"},
		}
		assert.Equal(t, "policy-456", GetStoragePolicyIDFromVAC(vac))
	})

	t.Run("NoParameters", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{}
		assert.Empty(t, GetStoragePolicyIDFromVAC(vac))
	})

	t.Run("UnrelatedParameters", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{
			Parameters: map[string]string{"otherParam": "value"},
		}
		assert.Empty(t, GetStoragePolicyIDFromVAC(vac))
	})
}

func TestIsVACPolicyEffective(t *testing.T) {
	vacName := "encrypted-vac"
	otherVACName := "other-vac"

	t.Run("NoVACSet", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{}
		assert.False(t, IsVACPolicyEffective(pvc))
	})

	t.Run("ModifyVolumeInProgress", func(t *testing.T) {
		pvc := &corev1.PersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeAttributesClassName: &vacName,
			},
			Status: corev1.PersistentVolumeClaimStatus{
				ModifyVolumeStatus: &corev1.ModifyVolumeStatus{
					TargetVolumeAttributesClassName: vacName,
					Status:                          corev1.PersistentVolumeClaimModifyVolumeInProgress,
				},
			},
		}
		assert.False(t, IsVACPolicyEffective(pvc), "VAC must not be trusted while a modify is in flight")
	})

	t.Run("InitialStateNeverModified", func(t *testing.T) {
		// ModifyVolumeStatus == nil is also the initial PVC state before any modify has
		// ever been attempted (e.g. a PVC created with a VAC set at creation time before
		// the provisioner/resizer has populated CurrentVolumeAttributesClassName). This is
		// the exact regression the fix addresses: nil ModifyVolumeStatus alone is not proof
		// the VAC is in effect.
		pvc := &corev1.PersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeAttributesClassName: &vacName,
			},
		}
		assert.False(t, IsVACPolicyEffective(pvc))
	})

	t.Run("CurrentVACDoesNotMatchTarget", func(t *testing.T) {
		current := otherVACName
		pvc := &corev1.PersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeAttributesClassName: &vacName,
			},
			Status: corev1.PersistentVolumeClaimStatus{
				CurrentVolumeAttributesClassName: &current,
			},
		}
		assert.False(t, IsVACPolicyEffective(pvc))
	})

	t.Run("ModifyCompletedAndCurrentMatchesTarget", func(t *testing.T) {
		current := vacName
		pvc := &corev1.PersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeAttributesClassName: &vacName,
			},
			Status: corev1.PersistentVolumeClaimStatus{
				CurrentVolumeAttributesClassName: &current,
			},
		}
		assert.True(t, IsVACPolicyEffective(pvc))
	})
}
