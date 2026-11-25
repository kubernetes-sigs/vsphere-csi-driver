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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto/internal"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
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

func TestIsEncryptedStoragePolicyForPVC(t *testing.T) {
	ctx := context.Background()
	scheme := testCryptoScheme(t)

	encryptedSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "encrypted-sc", UID: types.UID("sc-uid-1")},
		Provisioner: csitypes.Name,
		Parameters:  map[string]string{"storagepolicyid": "profile-encrypted-sc"},
	}
	encryptedVAC := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "encrypted-vac", UID: types.UID("vac-uid-1")},
		Parameters: map[string]string{"storagePolicyID": "profile-vac-only"},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      internal.EncryptedStorageClassNamesConfigMapName,
			Namespace: testCSINamespace,
			OwnerReferences: []metav1.OwnerReference{
				internal.GetOwnerRefForStorageClass(encryptedSC),
				internal.GetOwnerRefForVAC(encryptedVAC),
			},
		},
	}

	newPVC := func(scName, vacName *string) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName:          scName,
				VolumeAttributesClassName: vacName,
			},
		}
		// Mark the VAC as having taken effect.
		pvc.Status.CurrentVolumeAttributesClassName = vacName
		return pvc
	}
	newClient := func(objs ...ctrlclient.Object) *defaultClient {
		return &defaultClient{
			Client:       fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
			csiNamespace: testCSINamespace,
		}
	}

	t.Run("NilStorageClassName", func(t *testing.T) {
		c := newClient(encryptedSC, cm)

		ok, profileID, source, err := IsEncryptedStoragePolicyForPVC(ctx, c, c, newPVC(nil, nil), true)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Empty(t, profileID)
		assert.Equal(t, StoragePolicySourceStorageClass, source)
	})

	t.Run("StorageClassWhenNoVAC", func(t *testing.T) {
		c := newClient(encryptedSC, cm)

		ok, profileID, source, err := IsEncryptedStoragePolicyForPVC(
			ctx, c, c, newPVC(&encryptedSC.Name, nil), true)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "profile-encrypted-sc", profileID)
		assert.Equal(t, StoragePolicySourceStorageClass, source)
	})

	t.Run("EffectiveVAC", func(t *testing.T) {
		c := newClient(encryptedSC, encryptedVAC, cm)

		ok, profileID, source, err := IsEncryptedStoragePolicyForPVC(
			ctx, c, c, newPVC(&encryptedSC.Name, &encryptedVAC.Name), true)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "profile-vac-only", profileID)
		assert.Equal(t, StoragePolicySourceVAC, source)
	})

	t.Run("StorageClassWhenVACPolicyMutabilityDisabled", func(t *testing.T) {
		c := newClient(encryptedSC, encryptedVAC, cm)

		ok, profileID, source, err := IsEncryptedStoragePolicyForPVC(
			ctx, c, c, newPVC(&encryptedSC.Name, &encryptedVAC.Name), false)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "profile-encrypted-sc", profileID)
		assert.Equal(t, StoragePolicySourceStorageClass, source)
	})

	t.Run("MissingEffectiveVACReturnsNotFound", func(t *testing.T) {
		c := newClient(encryptedSC, cm)

		ok, _, source, err := IsEncryptedStoragePolicyForPVC(
			ctx, c, c, newPVC(&encryptedSC.Name, &encryptedVAC.Name), true)
		assert.True(t, apierrors.IsNotFound(err), "expected NotFound, got %v", err)
		assert.False(t, ok)
		assert.Equal(t, StoragePolicySourceVAC, source)
	})

	t.Run("FallsBackToStorageClassWhenVACAPINotServed", func(t *testing.T) {
		c := newClient(encryptedSC, cm)
		reader := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, ctrlclient.WithWatch, ctrlclient.ObjectKey,
				ctrlclient.Object, ...ctrlclient.GetOption) error {
				return &meta.NoKindMatchError{
					GroupKind:        schema.GroupKind{Group: "storage.k8s.io", Kind: "VolumeAttributesClass"},
					SearchedVersions: []string{"v1"},
				}
			},
		}).Build()

		ok, profileID, source, err := IsEncryptedStoragePolicyForPVC(
			ctx, c, reader, newPVC(&encryptedSC.Name, &encryptedVAC.Name), true)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "profile-encrypted-sc", profileID)
		assert.Equal(t, StoragePolicySourceStorageClass, source)
	})
}
