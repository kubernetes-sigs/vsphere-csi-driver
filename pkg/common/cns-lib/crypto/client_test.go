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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto/internal"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const testCSINamespace = "vmware-system-csi"

func testCryptoScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme, err := NewK8sScheme()
	require.NoError(t, err)
	return scheme
}

// listErrorClient wraps a ctrlclient.Client and forces List calls for a given object list type
// to fail with a fixed error, to simulate the target API not being served by the API server
// (e.g. VolumeAttributesClass on a pre-1.34 cluster).
type listErrorClient struct {
	ctrlclient.Client
	errsByType map[reflect.Type]error
}

func (c *listErrorClient) List(ctx context.Context, list ctrlclient.ObjectList, opts ...ctrlclient.ListOption) error {
	if err, ok := c.errsByType[reflect.TypeOf(list)]; ok {
		return err
	}
	return c.Client.List(ctx, list, opts...)
}

func noMatchErrorForVACList() error {
	return &meta.NoResourceMatchError{
		PartialResource: schema.GroupVersionResource{
			Group: "storage.k8s.io", Version: "v1", Resource: "volumeattributesclasses",
		},
	}
}

func TestIsEncryptedStorageProfile(t *testing.T) {
	scheme := testCryptoScheme(t)

	encryptedSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "encrypted-sc", UID: types.UID("sc-uid-1")},
		Provisioner: csitypes.Name,
		Parameters:  map[string]string{"storagepolicyid": "profile-encrypted-sc"},
	}
	plainSC := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "plain-sc", UID: types.UID("sc-uid-2")},
		Provisioner: csitypes.Name,
		Parameters:  map[string]string{"storagepolicyid": "profile-plain-sc"},
	}
	encryptedVAC := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "encrypted-vac", UID: types.UID("vac-uid-1")},
		Parameters: map[string]string{"storagePolicyID": "profile-vac-only"},
	}

	newConfigMapMarkingEncrypted := func(refs ...metav1.OwnerReference) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:            internal.EncryptedStorageClassNamesConfigMapName,
				Namespace:       testCSINamespace,
				OwnerReferences: refs,
			},
		}
	}

	t.Run("MatchesEncryptedStorageClass", func(t *testing.T) {
		cm := newConfigMapMarkingEncrypted(internal.GetOwnerRefForStorageClass(encryptedSC))
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(encryptedSC, plainSC, cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, err := c.IsEncryptedStorageProfile(context.Background(), "profile-encrypted-sc")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("MatchesNonEncryptedStorageClass", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(encryptedSC, plainSC).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, err := c.IsEncryptedStorageProfile(context.Background(), "profile-plain-sc")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("FallsBackToVACWhenNoStorageClassMatches", func(t *testing.T) {
		cm := newConfigMapMarkingEncrypted(internal.GetOwnerRefForVAC(encryptedVAC))
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(encryptedSC, plainSC, encryptedVAC, cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, err := c.IsEncryptedStorageProfile(context.Background(), "profile-vac-only")
		require.NoError(t, err)
		assert.True(t, ok, "profile referenced only by a VAC must still be reported as encrypted")
	})

	t.Run("NoMatchAnywhere", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(encryptedSC, plainSC, encryptedVAC).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, err := c.IsEncryptedStorageProfile(context.Background(), "profile-does-not-exist")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("VACAPINotServedIsTreatedAsNoMatch", func(t *testing.T) {
		// Simulate a pre-1.34 cluster where the VolumeAttributesClass API isn't served:
		// listing it fails with a RESTMapper "no match" error. This must be treated the
		// same as "no VAC references this policy", not propagated as a hard error.
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(encryptedSC, plainSC).Build()
		wrapped := &listErrorClient{
			Client: fakeClient,
			errsByType: map[reflect.Type]error{
				reflect.TypeOf(&storagev1.VolumeAttributesClassList{}): noMatchErrorForVACList(),
			},
		}
		c := &defaultClient{Client: wrapped, csiNamespace: testCSINamespace}

		ok, err := c.IsEncryptedStorageProfile(context.Background(), "profile-vac-only")
		require.NoError(t, err, "VAC API unavailability must not surface as an error")
		assert.False(t, ok)
	})

	t.Run("OtherVACListErrorsStillPropagate", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(encryptedSC, plainSC).Build()
		wrapped := &listErrorClient{
			Client: fakeClient,
			errsByType: map[reflect.Type]error{
				reflect.TypeOf(&storagev1.VolumeAttributesClassList{}): assertErr,
			},
		}
		c := &defaultClient{Client: wrapped, csiNamespace: testCSINamespace}

		_, err := c.IsEncryptedStorageProfile(context.Background(), "profile-vac-only")
		assert.ErrorIs(t, err, assertErr)
	})
}

var assertErr = &genericTestError{"boom"}

type genericTestError struct{ msg string }

func (e *genericTestError) Error() string { return e.msg }

func TestMarkEncryptedVAC(t *testing.T) {
	scheme := testCryptoScheme(t)
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-vac", UID: types.UID("vac-uid")},
	}
	ownerRef := internal.GetOwnerRefForVAC(vac)

	t.Run("ConfigMapMissingMarkEncryptedCreatesIt", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		require.NoError(t, c.MarkEncryptedVAC(context.Background(), vac, true))

		var cm corev1.ConfigMap
		require.NoError(t, fakeClient.Get(context.Background(),
			ctrlclient.ObjectKey{Namespace: testCSINamespace, Name: internal.EncryptedStorageClassNamesConfigMapName}, &cm))
		assert.Contains(t, cm.OwnerReferences, ownerRef)
	})

	t.Run("ConfigMapMissingMarkNotEncryptedIsNoOp", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		require.NoError(t, c.MarkEncryptedVAC(context.Background(), vac, false))

		var cm corev1.ConfigMap
		err := fakeClient.Get(context.Background(),
			ctrlclient.ObjectKey{Namespace: testCSINamespace, Name: internal.EncryptedStorageClassNamesConfigMapName}, &cm)
		assert.True(t, apierrors.IsNotFound(err),
			"ConfigMap should not be created just to mark something as not-encrypted")
	})

	t.Run("ConfigMapExistsAddsOwnerRef", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		require.NoError(t, c.MarkEncryptedVAC(context.Background(), vac, true))

		var got corev1.ConfigMap
		require.NoError(t, fakeClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(cm), &got))
		assert.Contains(t, got.OwnerReferences, ownerRef)
	})

	t.Run("ConfigMapExistsAlreadyOwnerMarkEncryptedIsNoOp", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		require.NoError(t, c.MarkEncryptedVAC(context.Background(), vac, true))

		var got corev1.ConfigMap
		require.NoError(t, fakeClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(cm), &got))
		assert.Len(t, got.OwnerReferences, 1)
	})

	t.Run("ConfigMapExistsRemovesOwnerRefWhenNotEncrypted", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace,
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		require.NoError(t, c.MarkEncryptedVAC(context.Background(), vac, false))

		var got corev1.ConfigMap
		require.NoError(t, fakeClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(cm), &got))
		assert.NotContains(t, got.OwnerReferences, ownerRef)
	})

	t.Run("ConfigMapExistsNotOwnerMarkNotEncryptedIsNoOp", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		require.NoError(t, c.MarkEncryptedVAC(context.Background(), vac, false))

		var got corev1.ConfigMap
		require.NoError(t, fakeClient.Get(context.Background(), ctrlclient.ObjectKeyFromObject(cm), &got))
		assert.Empty(t, got.OwnerReferences)
	})
}

func TestIsEncryptedVAC(t *testing.T) {
	scheme := testCryptoScheme(t)
	vacWithPolicy := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "my-vac", UID: types.UID("vac-uid")},
		Parameters: map[string]string{"storagePolicyID": "profile-1"},
	}
	vacWithoutPolicy := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "no-policy-vac", UID: types.UID("vac-uid-2")},
	}

	t.Run("ConfigMapMissing", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, profileID, err := c.isEncryptedVAC(context.Background(), vacWithPolicy)
		require.NoError(t, err)
		assert.False(t, ok)
		assert.Empty(t, profileID)
	})

	t.Run("VACIsOwnerWithPolicyID", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace,
				OwnerReferences: []metav1.OwnerReference{internal.GetOwnerRefForVAC(vacWithPolicy)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, profileID, err := c.isEncryptedVAC(context.Background(), vacWithPolicy)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, "profile-1", profileID)
	})

	t.Run("VACIsOwnerButHasNoPolicyIDParam", func(t *testing.T) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace,
				OwnerReferences: []metav1.OwnerReference{internal.GetOwnerRefForVAC(vacWithoutPolicy)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, _, err := c.isEncryptedVAC(context.Background(), vacWithoutPolicy)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("VACNotOwner", func(t *testing.T) {
		other := &storagev1.VolumeAttributesClass{ObjectMeta: metav1.ObjectMeta{Name: "other", UID: types.UID("other-uid")}}
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: internal.EncryptedStorageClassNamesConfigMapName, Namespace: testCSINamespace,
				OwnerReferences: []metav1.OwnerReference{internal.GetOwnerRefForVAC(other)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		c := &defaultClient{Client: fakeClient, csiNamespace: testCSINamespace}

		ok, _, err := c.isEncryptedVAC(context.Background(), vacWithPolicy)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
