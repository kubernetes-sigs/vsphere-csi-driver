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

package clusterstoragepolicyinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, apis.SchemeBuilder.AddToScheme(s))
	require.NoError(t, storagev1.AddToScheme(s))
	return s
}

func volumeBindingPtr(m storagev1.VolumeBindingMode) *storagev1.VolumeBindingMode {
	return &m
}

// TestVolumeAttributesClassAPIAvailable exercises the discovery path used by
// volumeAttributesClassAPIAvailable (via volumeAttributesClassAPIAvailableFromRESTConfig)
// against a stub API server.
func TestVolumeAttributesClassAPIAvailable(t *testing.T) {
	t.Run("volumeattributesclasses present", func(t *testing.T) {
		srv := newDiscoveryTestServer(t, true)
		t.Cleanup(srv.Close)

		cfg := &rest.Config{Host: srv.URL}
		ok, err := volumeAttributesClassAPIAvailableFromRESTConfig(cfg)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("volumeattributesclasses absent", func(t *testing.T) {
		srv := newDiscoveryTestServer(t, false)
		t.Cleanup(srv.Close)

		cfg := &rest.Config{Host: srv.URL}
		ok, err := volumeAttributesClassAPIAvailableFromRESTConfig(cfg)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func newDiscoveryTestServer(t *testing.T, includeVAC bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"APIVersions","apiVersion":"v1","versions":["v1"]}`))
	})
	mux.HandleFunc("/apis", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "kind": "APIGroupList",
  "apiVersion": "v1",
  "groups": [
    {
      "name": "storage.k8s.io",
      "versions": [
        {"groupVersion": "storage.k8s.io/v1", "version": "v1"}
      ],
      "preferredVersion": {"groupVersion": "storage.k8s.io/v1", "version": "v1"}
    }
  ]
}`))
	})
	mux.HandleFunc("/apis/storage.k8s.io/v1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		verbs := []string{"create", "delete", "get", "list", "patch", "update", "watch"}
		list := &metav1.APIResourceList{
			GroupVersion: storagev1.SchemeGroupVersion.String(),
			APIResources: []metav1.APIResource{
				{Name: "storageclasses", Namespaced: false, Kind: "StorageClass", Verbs: verbs},
			},
		}
		if includeVAC {
			list.APIResources = append(list.APIResources, metav1.APIResource{
				Name: "volumeattributesclasses", Namespaced: false, Kind: "VolumeAttributesClass", Verbs: verbs,
			})
		}
		payload, err := json.Marshal(list)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	})
	return httptest.NewServer(mux)
}

func TestStorageClassIsWaitForFirstConsumer(t *testing.T) {
	wffc := storagev1.VolumeBindingWaitForFirstConsumer
	immediate := storagev1.VolumeBindingImmediate
	cases := []struct {
		name string
		sc   *storagev1.StorageClass
		want bool
	}{
		{"nil binding mode", &storagev1.StorageClass{}, false},
		{"immediate", &storagev1.StorageClass{VolumeBindingMode: volumeBindingPtr(immediate)}, false},
		{"wait for first consumer", &storagev1.StorageClass{VolumeBindingMode: volumeBindingPtr(wffc)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, storageClassIsWaitForFirstConsumer(tc.sc))
		})
	}
}

func TestMergeOwnerReference(t *testing.T) {
	controllerFalse := false
	blockFalse := false
	base := metav1.OwnerReference{
		APIVersion: "storage.k8s.io/v1", Kind: "StorageClass", Name: "sc1",
		UID:        types.UID("11111111-1111-1111-1111-111111111111"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	other := metav1.OwnerReference{
		APIVersion: "v1", Kind: "ConfigMap", Name: "cm1",
		UID:        types.UID("22222222-2222-2222-2222-222222222222"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	sameKeyNewUID := metav1.OwnerReference{
		APIVersion: base.APIVersion, Kind: base.Kind, Name: base.Name,
		UID:        types.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}

	t.Run("append when new", func(t *testing.T) {
		out := mergeOwnerReference([]metav1.OwnerReference{base}, other)
		require.Len(t, out, 2)
		assert.Equal(t, base, out[0])
		assert.Equal(t, other, out[1])
	})

	t.Run("unchanged when same key and UID", func(t *testing.T) {
		refs := []metav1.OwnerReference{base}
		out := mergeOwnerReference(refs, base)
		assert.Equal(t, refs, out)
	})

	t.Run("replace when same key different UID", func(t *testing.T) {
		out := mergeOwnerReference([]metav1.OwnerReference{base}, sameKeyNewUID)
		require.Len(t, out, 1)
		assert.Equal(t, sameKeyNewUID, out[0])
	})

	t.Run("empty adds one", func(t *testing.T) {
		out := mergeOwnerReference(nil, base)
		require.Len(t, out, 1)
		assert.Equal(t, base, out[0])
	})
}

func TestOwnerReferenceFor(t *testing.T) {
	t.Run("storage class", func(t *testing.T) {
		scheme := testScheme(t)
		r := &ReconcileClusterStoragePolicyInfo{scheme: scheme}
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")},
		}
		ref, err := r.generateOwnerReference(sc)
		require.NoError(t, err)
		assert.Equal(t, "storage.k8s.io/v1", ref.APIVersion)
		assert.Equal(t, "StorageClass", ref.Kind)
		assert.Equal(t, "gold", ref.Name)
		assert.Equal(t, sc.UID, ref.UID)
		require.NotNil(t, ref.Controller)
		assert.False(t, *ref.Controller)
		require.NotNil(t, ref.BlockOwnerDeletion)
		assert.False(t, *ref.BlockOwnerDeletion)
	})

	t.Run("unknown type in scheme", func(t *testing.T) {
		s := runtime.NewScheme()
		r := &ReconcileClusterStoragePolicyInfo{scheme: s}
		_, err := r.generateOwnerReference(&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "x"}})
		require.Error(t, err)
	})
}

func TestCreateClusterSPIWithOwner(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)
	controllerFalse := false
	blockFalse := false
	scOwner := metav1.OwnerReference{
		APIVersion: "storage.k8s.io/v1", Kind: "StorageClass", Name: "gold",
		UID:        types.UID("11111111-1111-1111-1111-111111111111"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	namespacedName := types.NamespacedName{Name: "gold"}

	t.Run("creates ClusterStoragePolicyInfo", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.createClusterSPIWithOwner(ctx, "gold", "StorageClass", scOwner, namespacedName)
		require.Len(t, reqs, 1)
		assert.Equal(t, namespacedName, reqs[0].NamespacedName)

		got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, got))
		assert.Equal(t, "gold", got.Spec.K8sCompliantName)
		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, scOwner, got.OwnerReferences[0])
	})

	t.Run("already exists merges owner", func(t *testing.T) {
		existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "gold"},
			Spec:       clusterspiv1alpha1.ClusterStoragePolicyInfoSpec{K8sCompliantName: "gold"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.createClusterSPIWithOwner(ctx, "gold", "StorageClass", scOwner, namespacedName)
		require.Len(t, reqs, 1)

		updated := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, updated))
		require.Len(t, updated.OwnerReferences, 1)
		assert.Equal(t, scOwner, updated.OwnerReferences[0])
	})

	t.Run("create error returns nil", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*clusterspiv1alpha1.ClusterStoragePolicyInfo); ok {
					return fmt.Errorf("injected create failure")
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		assert.Nil(t, r.createClusterSPIWithOwner(ctx, "gold", "StorageClass", scOwner, namespacedName))
	})

	t.Run("already exists get failure returns nil", func(t *testing.T) {
		existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "gold"},
			Spec:       clusterspiv1alpha1.ClusterStoragePolicyInfoSpec{K8sCompliantName: "gold"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object,
				opts ...client.GetOption) error {
				if _, ok := obj.(*clusterspiv1alpha1.ClusterStoragePolicyInfo); ok {
					return fmt.Errorf("injected get after AlreadyExists")
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		assert.Nil(t, r.createClusterSPIWithOwner(ctx, "gold", "StorageClass", scOwner, namespacedName))
	})
}

func TestEnsureOwnerReferenceOnClusterSPI(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	controllerFalse := false
	blockFalse := false
	owner := metav1.OwnerReference{
		APIVersion: "storage.k8s.io/v1", Kind: "StorageClass", Name: "gold",
		UID:        types.UID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}

	t.Run("no-op when already merged", func(t *testing.T) {
		cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "gold",
				OwnerReferences: []metav1.OwnerReference{owner},
			},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cspi.DeepCopy()).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		require.NoError(t, r.ensureOwnerReferenceOnClusterSPI(ctx, cspi, owner, "StorageClass"))

		updated := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, updated))
		require.Len(t, updated.OwnerReferences, 1)
		assert.Equal(t, owner, updated.OwnerReferences[0])
	})

	t.Run("patch adds owner", func(t *testing.T) {
		cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "gold"},
			Spec:       clusterspiv1alpha1.ClusterStoragePolicyInfoSpec{K8sCompliantName: "gold"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cspi.DeepCopy()).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		require.NoError(t, r.ensureOwnerReferenceOnClusterSPI(ctx, cspi, owner, "StorageClass"))

		updated := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, updated))
		require.Len(t, updated.OwnerReferences, 1)
		assert.Equal(t, owner, updated.OwnerReferences[0])
	})

	t.Run("patch error", func(t *testing.T) {
		cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "gold"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cspi.DeepCopy()).WithInterceptorFuncs(
			interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object,
					patch client.Patch, opts ...client.PatchOption) error {
					return fmt.Errorf("injected patch failure")
				},
			}).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		err := r.ensureOwnerReferenceOnClusterSPI(ctx, cspi, owner, "StorageClass")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "injected patch failure")
	})
}

func TestMapStorageClassToClusterSPI(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	t.Run("wrong type returns nil", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.mapStorageClassToClusterSPI(ctx, &storagev1.VolumeAttributesClass{})
		assert.Nil(t, reqs)
	})

	t.Run("wait for first consumer skips", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		sc := &storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: "gold"},
			VolumeBindingMode: volumeBindingPtr(storagev1.VolumeBindingWaitForFirstConsumer),
		}
		assert.Nil(t, r.mapStorageClassToClusterSPI(ctx, sc))
	})

	t.Run("creates ClusterStoragePolicyInfo and returns request", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		sc := &storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "gold", UID: types.UID("dddddddd-dddd-dddd-dddd-dddddddddddd")},
			Provisioner: "p",
		}
		reqs := r.mapStorageClassToClusterSPI(ctx, sc)
		require.Len(t, reqs, 1)
		assert.Equal(t, types.NamespacedName{Name: "gold"}, reqs[0].NamespacedName)

		got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, got))
		assert.Equal(t, "gold", got.Spec.K8sCompliantName)
		require.NotEmpty(t, got.OwnerReferences)
	})
}

func TestMapVolumeAttributesClassToClusterSPI(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)

	t.Run("wrong type returns nil", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.mapVolumeAttributesClassToClusterSPI(ctx, &storagev1.StorageClass{})
		assert.Nil(t, reqs)
	})

	t.Run("creates ClusterStoragePolicyInfo from VAC", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan-vac", UID: types.UID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")},
			DriverName: "csi.test",
			Parameters: map[string]string{"k": "v"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.mapVolumeAttributesClassToClusterSPI(ctx, vac)
		require.Len(t, reqs, 1)
		assert.Equal(t, types.NamespacedName{Name: "orphan-vac"}, reqs[0].NamespacedName)
	})

	t.Run("second VAC name creates separate CSPI", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "fast", UID: types.UID("12121212-1212-1212-1212-121212121212")},
			DriverName: "csi.test",
			Parameters: map[string]string{"tier": "fast"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.mapVolumeAttributesClassToClusterSPI(ctx, vac)
		require.Len(t, reqs, 1)
		assert.Equal(t, types.NamespacedName{Name: "fast"}, reqs[0].NamespacedName)
	})
}

func TestMapVolumeAttributesClassToClusterSPI_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold"},
		Spec:       clusterspiv1alpha1.ClusterStoragePolicyInfoSpec{K8sCompliantName: "gold"},
	}
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: types.UID("abababab-abab-abab-abab-abababababab")},
		DriverName: "csi.test",
		Parameters: map[string]string{"k": "v"},
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
	reqs := r.mapVolumeAttributesClassToClusterSPI(ctx, vac)
	require.Len(t, reqs, 1)

	updated := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, updated))
	found := false
	for _, ref := range updated.OwnerReferences {
		if ref.Kind == "VolumeAttributesClass" && ref.Name == "gold" && ref.UID == vac.UID {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ownerReference for VAC")
}

func TestMapStorageClassToClusterSPI_PatchOwnerOnExisting(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold"},
		Spec:       clusterspiv1alpha1.ClusterStoragePolicyInfoSpec{K8sCompliantName: "gold"},
	}
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "gold", UID: types.UID("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd")},
		Provisioner: "p",
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
	reqs := r.mapStorageClassToClusterSPI(ctx, sc)
	require.Len(t, reqs, 1)

	updated := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
	require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, updated))
	found := false
	for _, ref := range updated.OwnerReferences {
		if ref.Kind == "StorageClass" && ref.Name == "gold" && ref.UID == sc.UID {
			found = true
			break
		}
	}
	assert.True(t, found, "expected ownerReference for StorageClass")
}

func TestMapStorageClassToClusterSPI_GetCSPIError(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "gold", UID: types.UID("efefefef-efef-efef-efef-efefefefefef")},
		Provisioner: "p",
	}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object,
			opts ...client.GetOption) error {
			if _, ok := obj.(*clusterspiv1alpha1.ClusterStoragePolicyInfo); ok {
				return fmt.Errorf("injected get CSPI failure")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}).Build()
	r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
	assert.Nil(t, r.mapStorageClassToClusterSPI(ctx, sc))
}
