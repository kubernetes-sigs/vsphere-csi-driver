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

package snapshotmetadataservice

import (
	"context"
	"errors"
	"testing"

	snapshotmetadatav1beta1 "github.com/kubernetes-csi/external-snapshot-metadata/client/apis/snapshotmetadataservice/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	wcpcapapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/wcpcapabilities"
	wcpcapv1alph1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/wcpcapabilities/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// newExportTestScheme builds a scheme with the types the CR export path needs. The
// ConfigExport CRD has no vendored Go types, so its GVK is registered against
// unstructured so the fake client can serve it.
func newExportTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	require.NoError(t, wcpcapapis.AddToScheme(s))
	s.AddKnownTypeWithName(configExportGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(
		configExportGVK.GroupVersion().WithKind(configExportGVK.Kind+"List"),
		&unstructured.UnstructuredList{})
	return s
}

// capabilitiesCR returns the supervisor-capabilities CR with core_addon_management set
// to activated.
func capabilitiesCR(activated bool) *wcpcapv1alph1.Capabilities {
	return &wcpcapv1alph1.Capabilities{
		ObjectMeta: metav1.ObjectMeta{Name: common.WCPCapabilitiesCRName},
		Status: wcpcapv1alph1.CapabilitiesStatus{
			Services: map[wcpcapv1alph1.ServiceID]map[wcpcapv1alph1.CapabilityName]wcpcapv1alph1.CapabilityStatus{
				tkgServiceID: {
					coreAddonManagementCapability: {Activated: activated},
				},
			},
		},
	}
}

// getConfigExport fetches the export ConfigExport from c, or nil if absent.
func getConfigExport(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	configExport := &unstructured.Unstructured{}
	configExport.SetGroupVersionKind(configExportGVK)
	err := c.Get(context.Background(), k8stypes.NamespacedName{
		Name:      exportConfigExportName,
		Namespace: targetNamespace,
	}, configExport)
	if err != nil {
		return nil
	}
	return configExport
}

// assertExportResourcesExist asserts all four export resources are present in c.
func assertExportResourcesExist(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()

	sa := &v1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, k8stypes.NamespacedName{
		Name: exportServiceAccountName, Namespace: targetNamespace}, sa))

	cr := &rbacv1.ClusterRole{}
	require.NoError(t, c.Get(ctx, k8stypes.NamespacedName{Name: exportReaderClusterRoleName}, cr))

	crb := &rbacv1.ClusterRoleBinding{}
	require.NoError(t, c.Get(ctx, k8stypes.NamespacedName{Name: exportRoleBindingName}, crb))

	assert.NotNil(t, getConfigExport(t, c))
}

func TestIsCoreAddonManagementActivated(t *testing.T) {
	tests := []struct {
		name string
		caps *wcpcapv1alph1.Capabilities
		want bool
	}{
		{
			name: "activated",
			caps: capabilitiesCR(true),
			want: true,
		},
		{
			name: "not activated",
			caps: capabilitiesCR(false),
			want: false,
		},
		{
			name: "capability absent from service",
			caps: &wcpcapv1alph1.Capabilities{
				Status: wcpcapv1alph1.CapabilitiesStatus{
					Services: map[wcpcapv1alph1.ServiceID]map[wcpcapv1alph1.CapabilityName]wcpcapv1alph1.CapabilityStatus{
						tkgServiceID: {},
					},
				},
			},
			want: false,
		},
		{
			name: "service absent",
			caps: &wcpcapv1alph1.Capabilities{},
			want: false,
		},
		{
			name: "capability set on a different service",
			caps: &wcpcapv1alph1.Capabilities{
				Status: wcpcapv1alph1.CapabilitiesStatus{
					Services: map[wcpcapv1alph1.ServiceID]map[wcpcapv1alph1.CapabilityName]wcpcapv1alph1.CapabilityStatus{
						"some.other.service": {
							coreAddonManagementCapability: {Activated: true},
						},
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isCoreAddonManagementActivated(tt.caps))
		})
	}
}

// namedCapabilities returns a Capabilities CR with the given name and
// core_addon_management activation state.
func namedCapabilities(name string, activated bool) *wcpcapv1alph1.Capabilities {
	caps := capabilitiesCR(activated)
	caps.Name = name
	return caps
}

// capabilitiesWithServiceCapability returns a Capabilities CR carrying a single
// capability under the given service ID, so tests can assert that only
// core_addon_management on the TKG service is considered.
func capabilitiesWithServiceCapability(serviceID wcpcapv1alph1.ServiceID,
	capability wcpcapv1alph1.CapabilityName, activated bool) *wcpcapv1alph1.Capabilities {
	return &wcpcapv1alph1.Capabilities{
		ObjectMeta: metav1.ObjectMeta{Name: common.WCPCapabilitiesCRName},
		Status: wcpcapv1alph1.CapabilitiesStatus{
			Services: map[wcpcapv1alph1.ServiceID]map[wcpcapv1alph1.CapabilityName]wcpcapv1alph1.CapabilityStatus{
				serviceID: {capability: {Activated: activated}},
			},
		},
	}
}

// TestCapabilitiesPredicateCreate covers the startup path: an already-activated
// capability must be picked up when the informer first lists the CR.
func TestCapabilitiesPredicateCreate(t *testing.T) {
	pred := capabilitiesPredicate()

	tests := []struct {
		name   string
		object *wcpcapv1alph1.Capabilities
		want   bool
	}{
		{
			name:   "target CR, activated",
			object: namedCapabilities(common.WCPCapabilitiesCRName, true),
			want:   true,
		},
		{
			// Enqueued regardless of state: Reconcile decides what to do, and a
			// not-activated CR must still be observed so a later flip is diffable.
			name:   "target CR, not activated",
			object: namedCapabilities(common.WCPCapabilitiesCRName, false),
			want:   true,
		},
		{
			name:   "unrelated CR name",
			object: namedCapabilities("some-other-capabilities", true),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Create(event.TypedCreateEvent[*wcpcapv1alph1.Capabilities]{Object: tt.object})
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCapabilitiesPredicateUpdate is the core of the late-activation path: the
// disabled->enabled flip must enqueue a reconcile, and unrelated churn on the same CR
// must not. This is the behaviour a supervisor testbed cannot easily exercise, because
// WCP recomputes status.services from spec.
func TestCapabilitiesPredicateUpdate(t *testing.T) {
	pred := capabilitiesPredicate()

	tests := []struct {
		name string
		old  *wcpcapv1alph1.Capabilities
		new  *wcpcapv1alph1.Capabilities
		want bool
	}{
		{
			name: "activation flips on",
			old:  namedCapabilities(common.WCPCapabilitiesCRName, false),
			new:  namedCapabilities(common.WCPCapabilitiesCRName, true),
			want: true,
		},
		{
			name: "activation flips off",
			old:  namedCapabilities(common.WCPCapabilitiesCRName, true),
			new:  namedCapabilities(common.WCPCapabilitiesCRName, false),
			want: true,
		},
		{
			name: "capability appears and is activated",
			old:  &wcpcapv1alph1.Capabilities{ObjectMeta: metav1.ObjectMeta{Name: common.WCPCapabilitiesCRName}},
			new:  namedCapabilities(common.WCPCapabilitiesCRName, true),
			want: true,
		},
		{
			name: "capability appears but is not activated",
			old:  &wcpcapv1alph1.Capabilities{ObjectMeta: metav1.ObjectMeta{Name: common.WCPCapabilitiesCRName}},
			new:  namedCapabilities(common.WCPCapabilitiesCRName, false),
			want: false,
		},
		{
			name: "no change, still activated",
			old:  namedCapabilities(common.WCPCapabilitiesCRName, true),
			new:  namedCapabilities(common.WCPCapabilitiesCRName, true),
			want: false,
		},
		{
			name: "no change, still not activated",
			old:  namedCapabilities(common.WCPCapabilitiesCRName, false),
			new:  namedCapabilities(common.WCPCapabilitiesCRName, false),
			want: false,
		},
		{
			// Every other supervisor capability writes to this same CR; none of that
			// churn should wake this controller.
			name: "unrelated capability on the TKG service flips",
			old:  capabilitiesWithServiceCapability(tkgServiceID, "kube_proxy_disabled", false),
			new:  capabilitiesWithServiceCapability(tkgServiceID, "kube_proxy_disabled", true),
			want: false,
		},
		{
			name: "core_addon_management flips on a different service",
			old: capabilitiesWithServiceCapability("some.other.service",
				coreAddonManagementCapability, false),
			new: capabilitiesWithServiceCapability("some.other.service",
				coreAddonManagementCapability, true),
			want: false,
		},
		{
			name: "unrelated CR name, activation flips on",
			old:  namedCapabilities("some-other-capabilities", false),
			new:  namedCapabilities("some-other-capabilities", true),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Update(event.TypedUpdateEvent[*wcpcapv1alph1.Capabilities]{
				ObjectOld: tt.old,
				ObjectNew: tt.new,
			})
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCapabilitiesPredicateDelete asserts deletion is ignored. Removing the
// Capabilities CR is not a signal to tear down the export resources.
func TestCapabilitiesPredicateDelete(t *testing.T) {
	pred := capabilitiesPredicate()
	got := pred.Delete(event.TypedDeleteEvent[*wcpcapv1alph1.Capabilities]{
		Object: namedCapabilities(common.WCPCapabilitiesCRName, true),
	})
	assert.False(t, got)
}

func TestEnsureCRExportResourcesCreatesAll(t *testing.T) {
	scheme := newExportTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	require.NoError(t, ensureCRExportResources(context.Background(), fakeClient))
	assertExportResourcesExist(t, fakeClient)
}

func TestEnsureCRExportResourcesConfigExportSpec(t *testing.T) {
	scheme := newExportTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	require.NoError(t, ensureCRExportResources(context.Background(), fakeClient))

	configExport := getConfigExport(t, fakeClient)
	require.NotNil(t, configExport)
	assert.Equal(t, configExportGVK.GroupVersion().String(), configExport.GetAPIVersion())
	assert.Equal(t, configExportGVK.Kind, configExport.GetKind())

	source, found, err := unstructured.NestedStringMap(configExport.Object, "spec", "source")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, map[string]string{
		"apiGroup": snapshotmetadatav1beta1.GroupName,
		"kind":     "SnapshotMetadataService",
		"name":     targetSMSName,
	}, source)

	toNamespace, found, err := unstructured.NestedString(configExport.Object, "spec", "toNamespace")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, vksPublicNamespace, toNamespace)

	sa, found, err := unstructured.NestedString(configExport.Object, "spec", "serviceAccountName")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, exportServiceAccountName, sa)

	extract, found, err := unstructured.NestedSlice(configExport.Object, "spec", "extract")
	require.NoError(t, err)
	require.True(t, found)
	var keys []string
	for _, entry := range extract {
		keys = append(keys, entry.(map[string]interface{})["key"].(string))
	}
	assert.Equal(t, []string{"address", "audience", "caCert"}, keys)
}

func TestEnsureCRExportResourcesIsIdempotent(t *testing.T) {
	scheme := newExportTestScheme(t)
	var creates int
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				creates++
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()

	require.NoError(t, ensureCRExportResources(context.Background(), fakeClient))
	assert.Equal(t, 4, creates)

	// Second pass must find everything present and create nothing further.
	require.NoError(t, ensureCRExportResources(context.Background(), fakeClient))
	assert.Equal(t, 4, creates)
	assertExportResourcesExist(t, fakeClient)
}

func TestEnsureCRExportResourcesToleratesAlreadyExists(t *testing.T) {
	scheme := newExportTestScheme(t)
	// Get reports the object as absent while Create reports it as already present,
	// which is what a concurrent create by another replica looks like.
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				if err := c.Create(ctx, obj, opts...); err != nil {
					return err
				}
				gvk := obj.GetObjectKind().GroupVersionKind()
				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, obj.GetName())
			},
		}).
		Build()

	require.NoError(t, ensureCRExportResources(context.Background(), fakeClient))
	assertExportResourcesExist(t, fakeClient)
}

func TestEnsureCRExportResourcesPropagatesCreateFailure(t *testing.T) {
	scheme := newExportTestScheme(t)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				return errors.New("create boom")
			},
		}).
		Build()

	err := ensureCRExportResources(context.Background(), fakeClient)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create boom")
}

func TestSyncCRExportResourcesCreatesWhenActivated(t *testing.T) {
	scheme := newExportTestScheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(capabilitiesCR(true)).Build()
	apiClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileSnapshotMetadataService{client: cachedClient, scheme: scheme, apiClient: apiClient}
	requeue := r.syncCRExportResources(context.Background())

	assert.False(t, requeue, "a successful export must not requeue")
	assertExportResourcesExist(t, apiClient)
}

func TestSyncCRExportResourcesSkipsWhenNotActivated(t *testing.T) {
	scheme := newExportTestScheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(capabilitiesCR(false)).Build()
	apiClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileSnapshotMetadataService{client: cachedClient, scheme: scheme, apiClient: apiClient}
	requeue := r.syncCRExportResources(context.Background())

	// Steady state on a supervisor without VKS addon management: nothing to do, and
	// no polling — the capability watch reports an activation if one happens.
	assert.False(t, requeue)
	assert.Nil(t, getConfigExport(t, apiClient))
	sa := &v1.ServiceAccount{}
	err := apiClient.Get(context.Background(), k8stypes.NamespacedName{
		Name: exportServiceAccountName, Namespace: targetNamespace}, sa)
	assert.Error(t, err)
}

// TestSyncCRExportResourcesCapabilitiesNotFound covers an absent Capabilities CR. That
// is a definitive "not activated" rather than a failure, so it must not start polling —
// the capability watch reports a Create if the CR ever appears.
func TestSyncCRExportResourcesCapabilitiesNotFound(t *testing.T) {
	scheme := newExportTestScheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	apiClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileSnapshotMetadataService{client: cachedClient, scheme: scheme, apiClient: apiClient}
	requeue := r.syncCRExportResources(context.Background())

	assert.False(t, requeue, "a missing Capabilities CR must not trigger polling")
	assert.Nil(t, getConfigExport(t, apiClient))
}

// TestSyncCRExportResourcesCapabilitiesReadErrorRequeues covers a transient read
// failure, which no event will follow up on and so must be retried.
func TestSyncCRExportResourcesCapabilitiesReadErrorRequeues(t *testing.T) {
	scheme := newExportTestScheme(t)
	cachedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(capabilitiesCR(true)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				return errors.New("etcdserver: request timed out")
			},
		}).
		Build()
	apiClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &ReconcileSnapshotMetadataService{client: cachedClient, scheme: scheme, apiClient: apiClient}
	requeue := r.syncCRExportResources(context.Background())

	assert.True(t, requeue, "a transient capability read failure must retry")
	assert.Nil(t, getConfigExport(t, apiClient))
}

func TestSyncCRExportResourcesMissingConfigExportCRDIsNonFatal(t *testing.T) {
	scheme := newExportTestScheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(capabilitiesCR(true)).Build()
	// A supervisor without the addon framework answers ConfigExport writes with a
	// no-kind-match error.
	apiClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if obj.GetObjectKind().GroupVersionKind() == configExportGVK {
					return &meta.NoKindMatchError{
						GroupKind:        configExportGVK.GroupKind(),
						SearchedVersions: []string{configExportGVK.Version},
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{client: cachedClient, scheme: scheme, apiClient: apiClient}
	// Must not panic and must not propagate: the SMS caCert/address sync depends on it.
	requeue := r.syncCRExportResources(context.Background())

	// A VKS service upgrade activates the capability and installs the ConfigExport CRD
	// in no guaranteed order, so a missing CRD right after activation is transient. The
	// activation event is already spent, so without a requeue nothing would retry until
	// a cert rotation or a syncer restart.
	assert.True(t, requeue, "a missing ConfigExport CRD must requeue, not be given up on")

	// The RBAC objects preceding the ConfigExport were still created.
	sa := &v1.ServiceAccount{}
	require.NoError(t, apiClient.Get(context.Background(), k8stypes.NamespacedName{
		Name: exportServiceAccountName, Namespace: targetNamespace}, sa))
}

// TestSyncCRExportResourcesRetriesUntilCRDAppears simulates the VKS upgrade ordering
// race end to end: the capability activates before the addon framework registers the
// ConfigExport CRD, and the export must succeed on a later pass once it exists.
func TestSyncCRExportResourcesRetriesUntilCRDAppears(t *testing.T) {
	scheme := newExportTestScheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(capabilitiesCR(true)).Build()

	crdInstalled := false
	apiClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if !crdInstalled && obj.GetObjectKind().GroupVersionKind() == configExportGVK {
					return &meta.NoKindMatchError{
						GroupKind:        configExportGVK.GroupKind(),
						SearchedVersions: []string{configExportGVK.Version},
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	r := &ReconcileSnapshotMetadataService{client: cachedClient, scheme: scheme, apiClient: apiClient}

	// First pass: CRD absent, so requeue and no ConfigExport.
	assert.True(t, r.syncCRExportResources(context.Background()))
	assert.Nil(t, getConfigExport(t, apiClient))

	// The addon framework finishes installing, then the requeued reconcile runs.
	crdInstalled = true
	assert.False(t, r.syncCRExportResources(context.Background()))
	assertExportResourcesExist(t, apiClient)
}

func TestGetWcpCapabilities(t *testing.T) {
	scheme := newExportTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(capabilitiesCR(true)).Build()

	r := &ReconcileSnapshotMetadataService{client: fakeClient, scheme: scheme}
	caps, err := r.getWcpCapabilities(context.Background())
	require.NoError(t, err)
	assert.Equal(t, common.WCPCapabilitiesCRName, caps.GetName())
	assert.True(t, isCoreAddonManagementActivated(caps))
}
