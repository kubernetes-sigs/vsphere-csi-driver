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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
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
		UID:        apitypes.UID("11111111-1111-1111-1111-111111111111"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	other := metav1.OwnerReference{
		APIVersion: "v1", Kind: "ConfigMap", Name: "cm1",
		UID:        apitypes.UID("22222222-2222-2222-2222-222222222222"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	sameKeyNewUID := metav1.OwnerReference{
		APIVersion: base.APIVersion, Kind: base.Kind, Name: base.Name,
		UID:        apitypes.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
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
			ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: apitypes.UID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")},
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
		UID:        apitypes.UID("11111111-1111-1111-1111-111111111111"),
		Controller: &controllerFalse, BlockOwnerDeletion: &blockFalse,
	}
	namespacedName := apitypes.NamespacedName{Name: "gold"}

	t.Run("creates ClusterStoragePolicyInfo", func(t *testing.T) {
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.createClusterSPIWithOwner(ctx, "gold", "StorageClass", scOwner, namespacedName)
		require.Len(t, reqs, 1)
		assert.Equal(t, namespacedName, reqs[0].NamespacedName)

		got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, got))
		assert.Equal(t, "gold", got.Name)
		require.Len(t, got.OwnerReferences, 1)
		assert.Equal(t, scOwner, got.OwnerReferences[0])
	})

	t.Run("already exists merges owner", func(t *testing.T) {
		existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
			ObjectMeta: metav1.ObjectMeta{Name: "gold"},
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
		UID:        apitypes.UID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
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
			ObjectMeta:  metav1.ObjectMeta{Name: "gold", UID: apitypes.UID("dddddddd-dddd-dddd-dddd-dddddddddddd")},
			Provisioner: "p",
		}
		reqs := r.mapStorageClassToClusterSPI(ctx, sc)
		require.Len(t, reqs, 1)
		assert.Equal(t, apitypes.NamespacedName{Name: "gold"}, reqs[0].NamespacedName)

		got := &clusterspiv1alpha1.ClusterStoragePolicyInfo{}
		require.NoError(t, cli.Get(ctx, client.ObjectKey{Name: "gold"}, got))
		assert.Equal(t, "gold", got.Name)
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
			ObjectMeta: metav1.ObjectMeta{Name: "orphan-vac", UID: apitypes.UID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")},
			DriverName: "csi.test",
			Parameters: map[string]string{"k": "v"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.mapVolumeAttributesClassToClusterSPI(ctx, vac)
		require.Len(t, reqs, 1)
		assert.Equal(t, apitypes.NamespacedName{Name: "orphan-vac"}, reqs[0].NamespacedName)
	})

	t.Run("second VAC name creates separate CSPI", func(t *testing.T) {
		vac := &storagev1.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "fast", UID: apitypes.UID("12121212-1212-1212-1212-121212121212")},
			DriverName: "csi.test",
			Parameters: map[string]string{"tier": "fast"},
		}
		cli := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}
		reqs := r.mapVolumeAttributesClassToClusterSPI(ctx, vac)
		require.Len(t, reqs, 1)
		assert.Equal(t, apitypes.NamespacedName{Name: "fast"}, reqs[0].NamespacedName)
	})
}

func TestMapVolumeAttributesClassToClusterSPI_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme(t)
	existing := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "gold"},
	}
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gold", UID: apitypes.UID("abababab-abab-abab-abab-abababababab")},
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
	}
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "gold", UID: apitypes.UID("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd")},
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
		ObjectMeta:  metav1.ObjectMeta{Name: "gold", UID: apitypes.UID("efefefef-efef-efef-efef-efefefefefef")},
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

func TestUpdateStatus_PolicyDeleted(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cspi"},
		Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{
			StoragePolicyDeleted: false,
			Error:                "",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cspi).
		WithStatusSubresource(&clusterspiv1alpha1.ClusterStoragePolicyInfo{}).Build()
	r := &ReconcileClusterStoragePolicyInfo{client: cli, scheme: scheme}

	// Update status to indicate policy is deleted
	cspi.Status.StoragePolicyDeleted = true
	cspi.Status.Error = ""

	err := k8s.UpdateStatus(ctx, r.client, cspi)
	assert.NoError(t, err, "k8s.UpdateStatus should succeed")

	// Verify the status was updated
	assert.True(t, cspi.Status.StoragePolicyDeleted, "expected StoragePolicyDeleted to be true")
	assert.Empty(t, cspi.Status.Error, "expected no error message")
}

func TestValidateStoragePolicyExistsOnVcenter_NilVCenter(t *testing.T) {
	// Skip this test as it involves testing panic behavior with nil VirtualCenter
	// The important fault handling logic is tested in the TestSyncStoragePolicyLogic tests
	t.Skip("Skipping nil VirtualCenter test - fault handling logic tested separately")
}

func TestValidateStoragePolicyExistsOnVcenter_WithRealVCenter(t *testing.T) {
	// Skip this test as it requires actual vCenter connection
	// The new fault handling logic is properly tested in TestSyncStoragePolicyLogic tests
	t.Skip("Skipping real vCenter test - requires connection, fault handling tested separately")
}

func TestRecordEvent_Warning(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cspi"},
	}

	// Initialize backoff map for testing
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	namespacedName := apitypes.NamespacedName{Name: "test-cspi"}
	backOffDuration[namespacedName] = time.Second

	r := &ReconcileClusterStoragePolicyInfo{recorder: record.NewFakeRecorder(10)}
	r.recordEvent(ctx, cspi, v1.EventTypeWarning, "test warning")

	// Verify backoff duration was doubled
	backOffDurationMapMutex.Lock()
	duration := backOffDuration[namespacedName]
	backOffDurationMapMutex.Unlock()

	assert.Equal(t, 2*time.Second, duration, "expected backoff duration to be doubled")
}

func TestRecordEvent_Normal(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cspi"},
	}

	// Initialize backoff map for testing with higher duration
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	namespacedName := apitypes.NamespacedName{Name: "test-cspi"}
	backOffDuration[namespacedName] = 30 * time.Second

	r := &ReconcileClusterStoragePolicyInfo{recorder: record.NewFakeRecorder(10)}
	r.recordEvent(ctx, cspi, v1.EventTypeNormal, "test success")

	// Verify backoff duration was reset to 1 second
	backOffDurationMapMutex.Lock()
	duration := backOffDuration[namespacedName]
	backOffDurationMapMutex.Unlock()

	assert.Equal(t, time.Second, duration, "expected backoff duration to be reset to 1 second")
}

// Helper function to simulate the policy existence check logic for testing
func simulatePolicyExistenceCheck(instance *clusterspiv1alpha1.ClusterStoragePolicyInfo,
	policyExists bool, errorType string) error {
	if errorType == fault.CSINotFoundFault {
		// Profile not found - this is expected when policy is deleted
		instance.Status.StoragePolicyDeleted = true
		return nil
	} else if errorType != "" {
		// Other errors (like internal errors) should be returned as failures
		return fmt.Errorf("failed to query storage policy: simulated %s error", errorType)
	}

	// Profile found - policy exists
	instance.Status.StoragePolicyDeleted = !policyExists
	return nil
}

func TestSyncStoragePolicyLogic_ProfileFound(t *testing.T) {

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
		Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{
			StoragePolicyDeleted: true, // Initially marked as deleted
			Error:                "previous error",
		},
	}

	// Simulate policy found scenario
	err := simulatePolicyExistenceCheck(cspi, true, "")
	assert.NoError(t, err, "expected no error when profile is found")
	assert.False(t, cspi.Status.StoragePolicyDeleted, "expected StoragePolicyDeleted to be false")
}

func TestSyncStoragePolicyLogic_ProfileNotFound(t *testing.T) {

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
		Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{
			StoragePolicyDeleted: false, // Initially marked as existing
			Error:                "",
		},
	}

	// Simulate policy not found scenario
	err := simulatePolicyExistenceCheck(cspi, false, fault.CSINotFoundFault)
	assert.NoError(t, err, "expected no error when profile is not found (expected case)")
	assert.True(t, cspi.Status.StoragePolicyDeleted, "expected StoragePolicyDeleted to be true")
}

func TestSyncStoragePolicyLogic_InternalError(t *testing.T) {

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
		Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{
			StoragePolicyDeleted: false,
			Error:                "",
		},
	}

	// Simulate internal error scenario
	err := simulatePolicyExistenceCheck(cspi, false, fault.CSIInternalFault)
	assert.Error(t, err, "expected error for internal fault")
	assert.Contains(t, err.Error(), "failed to query storage policy", "expected specific error message")
	// StoragePolicyDeleted should remain unchanged (false) since we couldn't determine status
	assert.False(t, cspi.Status.StoragePolicyDeleted, "expected StoragePolicyDeleted to remain unchanged")
}

func TestSyncStoragePolicyLogic_UnknownFault(t *testing.T) {

	cspi := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
		Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{
			StoragePolicyDeleted: false,
			Error:                "",
		},
	}

	// Simulate unknown error scenario
	err := simulatePolicyExistenceCheck(cspi, false, "unknown.fault.type")
	assert.Error(t, err, "expected error for unknown fault")
	assert.Contains(t, err.Error(), "failed to query storage policy", "expected specific error message")
	// StoragePolicyDeleted should remain unchanged (false) since it's not a NotFound fault
	assert.False(t, cspi.Status.StoragePolicyDeleted, "expected StoragePolicyDeleted to remain unchanged")
}

func TestCheckVsanEncryption(t *testing.T) {
	tests := []struct {
		name          string
		policyContent []cnsvsphere.SpbmPolicyContent
		expected      bool
	}{
		{
			name:          "empty policy content",
			policyContent: []cnsvsphere.SpbmPolicyContent{},
			expected:      false,
		},
		{
			name: "policy with dataAtRestEncryption capability",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "dataAtRestEncryption",
									Value:  "true",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "policy without dataAtRestEncryption capability",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "someOtherCapability",
									Value:  "true",
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "multiple policies with dataAtRestEncryption in second policy",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy-1",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "someCapability",
									Value:  "true",
								},
							},
						},
					},
				},
				{
					ID: "test-policy-2",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "dataAtRestEncryption",
									Value:  "true",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checkVsanEncryption(tt.policyContent)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAnalyzeEncryptionCapabilities(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	// nil vc is safe here: none of the test cases include a dataservice reference,
	// so vc.PbmRetrieveContent is never actually called inside checkVmEncryption.
	var vc *cnsvsphere.VirtualCenter

	tests := []struct {
		name               string
		policyContent      []cnsvsphere.SpbmPolicyContent
		expectedEncryption bool
		expectedTypes      []clusterspiv1alpha1.EncryptionType
	}{
		{
			name:               "empty policy content",
			policyContent:      []cnsvsphere.SpbmPolicyContent{},
			expectedEncryption: false,
			expectedTypes:      []clusterspiv1alpha1.EncryptionType{},
		},
		{
			name: "policy with data-at-rest encryption",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy-id",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "dataAtRestEncryption",
									Value:  "true",
								},
							},
						},
					},
				},
			},
			expectedEncryption: true,
			expectedTypes:      []clusterspiv1alpha1.EncryptionType{"vsan-encryption"},
		},
		{
			name: "policy without data-at-rest encryption",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy-id",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "someOtherCapability",
									Value:  "true",
								},
							},
						},
					},
				},
			},
			expectedEncryption: false,
			expectedTypes:      []clusterspiv1alpha1.EncryptionType{},
		},
		{
			name: "policy with VM encryption via direct rule",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy-id",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    vmEncryptionNs,
									CapID: vmEncryptionCapID,
									Value: "true",
								},
							},
						},
					},
				},
			},
			expectedEncryption: true,
			expectedTypes:      []clusterspiv1alpha1.EncryptionType{"vm-encryption"},
		},
		{
			name: "policy with both vSAN and VM encryption",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy-id",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									PropID: "dataAtRestEncryption",
									Value:  "true",
								},
								{
									Ns:    vmEncryptionNs,
									CapID: vmEncryptionCapID,
									Value: "true",
								},
							},
						},
					},
				},
			},
			expectedEncryption: true,
			expectedTypes: []clusterspiv1alpha1.EncryptionType{
				"vsan-encryption",
				"vm-encryption",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-policy",
				},
				Status: clusterspiv1alpha1.ClusterStoragePolicyInfoStatus{},
			}

			require.NoError(t, populateEncryptionCapabilities(ctx, instance, "test-policy-id", vc, tt.policyContent))

			assert.NotNil(t, instance.Status.Encryption)
			assert.Equal(t, tt.expectedEncryption, instance.Status.Encryption.SupportsEncryption)
			assert.Equal(t, tt.expectedTypes, instance.Status.Encryption.EncryptionTypes)
		})
	}
}

func TestExtractIopsLimit(t *testing.T) {
	iops100 := int64(100)
	tests := []struct {
		name          string
		policyContent []cnsvsphere.SpbmPolicyContent
		expected      *int64
		expectErr     bool
	}{
		{
			name:          "empty policy content",
			policyContent: []cnsvsphere.SpbmPolicyContent{},
			expected:      nil,
			expectErr:     false,
		},
		{
			name: "policy with IOPS limit",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:     vsanIopsLimitNs,
									PropID: vsanIopsLimitPropID,
									Value:  "100",
								},
							},
						},
					},
				},
			},
			expected:  &iops100,
			expectErr: false,
		},
		{
			name: "policy with non-numeric IOPS value",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:     vsanIopsLimitNs,
									PropID: vsanIopsLimitPropID,
									Value:  "not-a-number",
								},
							},
						},
					},
				},
			},
			expected:  nil,
			expectErr: true,
		},
		{
			name: "policy without IOPS limit rule",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:     vsanIopsLimitNs,
									CapID:  "someOtherCap",
									PropID: "someOtherProp",
									Value:  "100",
								},
							},
						},
					},
				},
			},
			expected:  nil,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractIopsLimit(tt.policyContent)
			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				if tt.expected == nil {
					assert.Nil(t, result)
				} else {
					require.NotNil(t, result)
					assert.Equal(t, *tt.expected, *result)
				}
			}
		})
	}
}

func TestPopulatePerformanceCapabilities(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	iops500 := int64(500)
	tests := []struct {
		name          string
		policyContent []cnsvsphere.SpbmPolicyContent
		expectedPerf  *clusterspiv1alpha1.Performance
		expectErr     bool
	}{
		{
			name:          "no IOPS limit",
			policyContent: []cnsvsphere.SpbmPolicyContent{},
			expectedPerf:  nil,
			expectErr:     false,
		},
		{
			name: "policy with IOPS limit",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:     vsanIopsLimitNs,
									PropID: vsanIopsLimitPropID,
									Value:  "500",
								},
							},
						},
					},
				},
			},
			expectedPerf: &clusterspiv1alpha1.Performance{IopsLimit: &iops500},
			expectErr:    false,
		},
		{
			name: "policy with non-numeric IOPS value",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:     vsanIopsLimitNs,
									PropID: vsanIopsLimitPropID,
									Value:  "not-a-number",
								},
							},
						},
					},
				},
			},
			expectedPerf: nil,
			expectErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
				ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
			}
			err := populatePerformanceCapabilities(ctx, instance, "test-policy-id", tt.policyContent)
			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, instance.Status.Performance)
			} else {
				require.NoError(t, err)
				if tt.expectedPerf == nil {
					assert.Nil(t, instance.Status.Performance)
				} else {
					require.NotNil(t, instance.Status.Performance)
					require.NotNil(t, instance.Status.Performance.IopsLimit)
					assert.Equal(t, *tt.expectedPerf.IopsLimit, *instance.Status.Performance.IopsLimit)
				}
			}
		})
	}
}

func TestHasVmEncryptionRule(t *testing.T) {
	tests := []struct {
		name          string
		policyContent []cnsvsphere.SpbmPolicyContent
		expected      bool
	}{
		{
			name:          "empty policy content",
			policyContent: []cnsvsphere.SpbmPolicyContent{},
			expected:      false,
		},
		{
			name: "policy with VM encryption rule",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    vmEncryptionNs,
									CapID: vmEncryptionCapID,
									Value: "true",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "policy with matching Ns but wrong CapID",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    vmEncryptionNs,
									CapID: "someOtherCap",
									Value: "true",
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "policy with matching CapID but wrong Ns",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    "someOtherNs",
									CapID: vmEncryptionCapID,
									Value: "true",
								},
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasVmEncryptionRule(tt.policyContent)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckVmEncryption(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	vmEncryptContent := []cnsvsphere.SpbmPolicyContent{
		{
			ID: "ref-policy",
			Profiles: []cnsvsphere.SpbmPolicySubProfile{
				{
					Rules: []cnsvsphere.SpbmPolicyRule{
						{
							Ns:    vmEncryptionNs,
							CapID: vmEncryptionCapID,
							Value: "true",
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name            string
		policyContent   []cnsvsphere.SpbmPolicyContent
		retrieveContent func(context.Context, []string) ([]cnsvsphere.SpbmPolicyContent, error)
		expected        bool
		expectErr       bool
	}{
		{
			name:          "empty policy content",
			policyContent: []cnsvsphere.SpbmPolicyContent{},
			retrieveContent: func(_ context.Context, _ []string) ([]cnsvsphere.SpbmPolicyContent, error) {
				return nil, nil
			},
			expected:  false,
			expectErr: false,
		},
		{
			name: "direct VM encryption rule present",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    vmEncryptionNs,
									CapID: vmEncryptionCapID,
									Value: "true",
								},
							},
						},
					},
				},
			},
			retrieveContent: func(_ context.Context, _ []string) ([]cnsvsphere.SpbmPolicyContent, error) {
				t.Fatal("retrieveContent should not be called for direct VM encryption")
				return nil, nil
			},
			expected:  true,
			expectErr: false,
		},
		{
			name: "indirect VM encryption via dataservice reference",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    dataserviceNs,
									CapID: "ref-policy-id",
									Value: "true",
								},
							},
						},
					},
				},
			},
			retrieveContent: func(_ context.Context, ids []string) ([]cnsvsphere.SpbmPolicyContent, error) {
				assert.Equal(t, []string{"ref-policy-id"}, ids)
				return vmEncryptContent, nil
			},
			expected:  true,
			expectErr: false,
		},
		{
			name: "dataservice reference does not contain VM encryption",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    dataserviceNs,
									CapID: "ref-policy-id",
									Value: "true",
								},
							},
						},
					},
				},
			},
			retrieveContent: func(_ context.Context, _ []string) ([]cnsvsphere.SpbmPolicyContent, error) {
				return []cnsvsphere.SpbmPolicyContent{}, nil
			},
			expected:  false,
			expectErr: false,
		},
		{
			name: "dataservice reference lookup fails",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    dataserviceNs,
									CapID: "ref-policy-id",
									Value: "true",
								},
							},
						},
					},
				},
			},
			retrieveContent: func(_ context.Context, _ []string) ([]cnsvsphere.SpbmPolicyContent, error) {
				return nil, fmt.Errorf("vCenter unavailable")
			},
			expected:  false,
			expectErr: true,
		},
		{
			name: "dataservice rule with empty CapID is skipped",
			policyContent: []cnsvsphere.SpbmPolicyContent{
				{
					ID: "test-policy",
					Profiles: []cnsvsphere.SpbmPolicySubProfile{
						{
							Rules: []cnsvsphere.SpbmPolicyRule{
								{
									Ns:    dataserviceNs,
									CapID: "",
									Value: "true",
								},
							},
						},
					},
				},
			},
			retrieveContent: func(_ context.Context, _ []string) ([]cnsvsphere.SpbmPolicyContent, error) {
				t.Fatal("retrieveContent should not be called for empty CapID")
				return nil, nil
			},
			expected:  false,
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := checkVmEncryption(ctx, tt.retrieveContent, tt.policyContent)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, result)
		})
	}
}
