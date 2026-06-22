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

package admissionhandler

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	ccV1beta2 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// setupSchemeForTest creates a runtime scheme with Cluster API types registered
func setupSchemeForTest() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = ccV1beta2.AddToScheme(scheme)
	return scheme
}

// createTestVSphereClusters creates realistic VSphereCluster objects for different test scenarios
func createTestVSphereClusters() []*unstructured.Unstructured {
	clusters := []*unstructured.Unstructured{}

	clusterData := []struct {
		name      string
		namespace string
	}{
		{"test-cluster", "default"},
		{"guest-cluster", "vmware-system-csi"},
		{"test-cluster-e2e-script-95jxs", "test-gc-e2e-demo-ns"},
		{"prod-cluster", "production"},
	}

	for _, data := range clusterData {
		cluster := &unstructured.Unstructured{}
		cluster.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   vsphereClusterGroup,
			Version: vsphereClusterDefaultVersion,
			Kind:    "VSphereCluster",
		})
		cluster.SetName(data.name)
		cluster.SetNamespace(data.namespace)

		clusters = append(clusters, cluster)
	}

	return clusters
}

// convertVSphereClustersToObjects converts VSphereCluster objects to runtime.Object for fake client
func convertVSphereClustersToObjects(clusters []*unstructured.Unstructured) []client.Object {
	objects := make([]client.Object, len(clusters))
	for i := range clusters {
		objects[i] = clusters[i]
	}
	return objects
}

// setupClusterAPIMocking sets up gomonkey patches for VSphere cluster validation with realistic test data
// Returns patches that must be reset with defer patches.Reset()
func setupClusterAPIMocking(t *testing.T) client.Client {

	// Setup fake client with VSphere cluster test data
	scheme := setupSchemeForTest()
	testVSphereClusters := createTestVSphereClusters()

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(convertVSphereClustersToObjects(testVSphereClusters)...).
		Build()

	// Return the fake client directly for use in tests
	return fakeClient
}

func TestValidatePvCSIServiceAccount(t *testing.T) {
	// Setup cluster API mocking with realistic test data
	fakeClient := setupClusterAPIMocking(t)
	testCases := []struct {
		name           string
		username       string
		expected       bool
		expectError    bool
		errorSubstring string // Optional: partial error message to match
	}{
		{
			name:        "CSI controller service account (not PvCSI)",
			username:    "system:serviceaccount:vmware-system-csi:vsphere-csi-controller",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Valid vsphere-csi-node in valid namespace",
			username:    "system:serviceaccount:vmware-system-csi:vsphere-csi-node",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Valid legacy pvcsi service account",
			username:    "system:serviceaccount:kube-system:pvcsi",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid namespace for test-cluster VSphereCluster validation",
			username:    "system:serviceaccount:vmware-system-csi:test-cluster-pvcsi",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Valid VSphereCluster validation for test-cluster-e2e-script-95jxs",
			username:    "system:serviceaccount:test-gc-e2e-demo-ns:test-cluster-e2e-script-95jxs-pvcsi",
			expected:    true, // Matches "test-cluster-e2e-script-95jxs" VSphereCluster in our fake cluster list
			expectError: false,
		},
		{
			name:        "Valid VSphereCluster validation for guest-cluster",
			username:    "system:serviceaccount:vmware-system-csi:guest-cluster-pvcsi",
			expected:    true, // Matches "guest-cluster" VSphereCluster in our fake cluster list
			expectError: false,
		},
		{
			name:        "SECURITY FIX: malicious service account in wrong namespace blocked",
			username:    "system:serviceaccount:malicious-ns:evil-pvcsi",
			expected:    false,
			expectError: false,
		},
		{
			name:        "SECURITY FIX: attempt to bypass with multiple colons blocked",
			username:    "system:serviceaccount:namespace:extra:vsphere-csi-controller",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - wrong service account name in valid namespace",
			username:    "system:serviceaccount:vmware-system-csi:invalid-service-account",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - missing namespace",
			username:    "system:serviceaccount::vsphere-csi-controller",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - empty username",
			username:    "",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - not a service account",
			username:    "regular-user",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - kubernetes admin user",
			username:    "kubernetes-admin",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - missing colon separators",
			username:    "system:serviceaccountnamespacevsphere-csi-controller",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - partial match",
			username:    "system:serviceaccount:namespace:vsphere-csi-contro",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - extra text after valid pattern",
			username:    "system:serviceaccount:namespace:vsphere-csi-controller-extra",
			expected:    false,
			expectError: false,
		},
		{
			name:        "Invalid - valid service account in unauthorized namespace",
			username:    "system:serviceaccount:my-namespace-with-dashes:vsphere-csi-node",
			expected:    false, // Wrong namespace - only vmware-system-csi allowed for CSI service accounts
			expectError: false,
		},
		{
			name:        "Invalid - valid service account in unauthorized namespace",
			username:    "system:serviceaccount:namespace123:vsphere-csi-controller",
			expected:    false, // Wrong namespace - only vmware-system-csi allowed for CSI service accounts
			expectError: false,
		},
		{
			name:        "Invalid - pvcsi service account in wrong namespace",
			username:    "system:serviceaccount:some-namespace:pvcsi",
			expected:    false, // pvcsi only allowed in kube-system namespace
			expectError: false,
		},
		{
			name:        "SECURITY FIX: multiple colon bypass attempt blocked",
			username:    "system:serviceaccount:malicious:namespace:attacker-pvcsi",
			expected:    false,
			expectError: false,
		},
		{
			name:        "SECURITY FIX: service account with colon in name blocked",
			username:    "system:serviceaccount:vmware-system-csi:service:account-pvcsi",
			expected:    false, // Colon in cluster name blocked by fallback validation
			expectError: false,
		},
		{
			name:        "Invalid - empty cluster name with -pvcsi suffix",
			username:    "system:serviceaccount:vmware-system-csi:-pvcsi",
			expected:    false, // Empty cluster name blocked by fallback validation
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validatePvCSIServiceAccount(context.TODO(), tc.username, fakeClient,
				vsphereClusterCache{version: vsphereClusterDefaultVersion})

			// Check error expectation
			if tc.expectError {
				if err == nil {
					t.Errorf("validatePvCSIServiceAccount() expected error but got none")
					return
				}
				// Check if error contains expected substring if specified
				if tc.errorSubstring != "" && !strings.Contains(err.Error(), tc.errorSubstring) {
					t.Errorf("validatePvCSIServiceAccount() error = %v, want error containing %q", err, tc.errorSubstring)
					return
				}
			} else {
				if err != nil {
					t.Errorf("validatePvCSIServiceAccount() returned unexpected error: %v", err)
					return
				}
			}

			// For error cases, we might not need to check the boolean result as it's less meaningful
			// But we can still check it for completeness
			if result != tc.expected {
				t.Errorf("validatePvCSIServiceAccount(%q) = %v, want %v", tc.username, result, tc.expected)
			}
		})
	}
}

func TestIsUserAllowedForDeletion(t *testing.T) {
	// Setup cluster API mocking with realistic test data
	// This is needed because isUserAllowedForDeletion() calls validatePvCSIServiceAccount()
	// which requires cluster API access
	fakeClient := setupClusterAPIMocking(t)

	testCases := []struct {
		name     string
		username string
		expected bool
	}{
		{
			name:     "CSI controller service account (not PvCSI)",
			username: "system:serviceaccount:vmware-system-csi:vsphere-csi-controller",
			expected: false,
		},
		{
			name:     "kube-system invalid SA",
			username: "system:serviceaccount:kube-system:i-am-invalid",
			expected: false,
		},
		{
			name:     "kube-system default SA",
			username: "system:serviceaccount:kube-system:default",
			expected: false,
		},
		{
			name:     "kube-system generic-garbage-collector",
			username: "system:serviceaccount:kube-system:generic-garbage-collector",
			expected: true,
		},
		{
			name:     "kube-system namespace-controller",
			username: "system:serviceaccount:kube-system:namespace-controller",
			expected: true,
		},
		{
			name:     "kube-system supervisor-authz-service-controller-pkg-sa",
			username: "system:serviceaccount:kube-system:supervisor-authz-service-controller-pkg-sa",
			expected: true,
		},
		{
			name:     "kube-system supervisor-authz-service-controller-sa",
			username: "system:serviceaccount:kube-system:supervisor-authz-service-controller-sa",
			expected: true,
		},
		{
			name:     "Bare kube-system prefix is not allowed",
			username: "system:serviceaccount:kube-system",
			expected: false,
		},
		{
			name:     "Valid Kubernetes admin",
			username: "kubernetes-admin",
			expected: true,
		},
		{
			name:     "Invalid user",
			username: "regular-user",
			expected: false,
		},
		{
			name:     "Malicious service account that should not be allowed",
			username: "system:serviceaccount:malicious:evil-service",
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := isUserAllowedForDeletion(context.TODO(), tc.username, fakeClient, vsphereClusterCache{})
			if err != nil {
				t.Errorf("isUserAllowedForDeletion() returned error: %v", err)
				return
			}
			if result != tc.expected {
				t.Errorf("isUserAllowedForDeletion(%q) = %v, want %v", tc.username, result, tc.expected)
			}
		})
	}
}

// newFakeStore creates a cache.Store populated with the given VSphereCluster objects.
// The store uses MetaNamespaceKeyFunc so keys are "namespace/name", matching the
// lookup in validateVSphereClusterResource.
func newFakeStore(clusters ...*unstructured.Unstructured) cache.Store {
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	for _, c := range clusters {
		_ = store.Add(c)
	}
	return store
}

// makeVSphereCluster is a test helper that returns an unstructured VSphereCluster.
func makeVSphereCluster(name, namespace string) *unstructured.Unstructured {
	c := &unstructured.Unstructured{}
	c.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   vsphereClusterGroup,
		Version: vsphereClusterDefaultVersion,
		Kind:    "VSphereCluster",
	})
	c.SetName(name)
	c.SetNamespace(namespace)
	return c
}

func TestValidateVSphereClusterResource(t *testing.T) {
	synced := func() bool { return true }
	notSynced := func() bool { return false }

	scheme := setupSchemeForTest()
	existingCluster := makeVSphereCluster("guest-cluster", "vmware-system-csi")

	clientWithCluster := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCluster).
		Build()
	emptyClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	testCases := []struct {
		name        string
		clusterName string
		namespace   string
		store       cache.Store
		hasSynced   cache.InformerSynced
		k8sClient   client.Client
		expected    bool
	}{
		{
			name:        "store synced, cluster present — returns true without hitting API",
			clusterName: "guest-cluster",
			namespace:   "vmware-system-csi",
			store:       newFakeStore(existingCluster),
			hasSynced:   synced,
			k8sClient:   emptyClient, // API has nothing — proves the store path is used
			expected:    true,
		},
		{
			name:        "store synced, cluster absent — returns false even when API has it",
			clusterName: "guest-cluster",
			namespace:   "vmware-system-csi",
			store:       newFakeStore(), // empty store
			hasSynced:   synced,
			k8sClient:   clientWithCluster, // API has the cluster — proves store wins
			expected:    false,
		},
		{
			name:        "store not yet synced, cluster in API — fallback returns true",
			clusterName: "guest-cluster",
			namespace:   "vmware-system-csi",
			store:       newFakeStore(),
			hasSynced:   notSynced,
			k8sClient:   clientWithCluster,
			expected:    true,
		},
		{
			name:        "store not yet synced, cluster missing from API — fallback returns false",
			clusterName: "nonexistent-cluster",
			namespace:   "vmware-system-csi",
			store:       newFakeStore(),
			hasSynced:   notSynced,
			k8sClient:   emptyClient,
			expected:    false,
		},
		{
			name:        "nil store — always falls back to API, cluster present",
			clusterName: "guest-cluster",
			namespace:   "vmware-system-csi",
			store:       nil,
			hasSynced:   nil, // never called when store is nil
			k8sClient:   clientWithCluster,
			expected:    true,
		},
		{
			name:        "store synced, cluster deleted — empty store reflects deletion",
			clusterName: "deleted-cluster",
			namespace:   "vmware-system-csi",
			store:       newFakeStore(), // cluster removed from store on deletion
			hasSynced:   synced,
			k8sClient:   emptyClient,
			expected:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validateVSphereClusterResource(context.TODO(), tc.clusterName, tc.namespace,
				tc.k8sClient, vsphereClusterCache{
					store:     tc.store,
					hasSynced: tc.hasSynced,
					version:   vsphereClusterDefaultVersion,
				})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tc.expected {
				t.Errorf("validateVSphereClusterResource(%q, %q) = %v, want %v",
					tc.namespace, tc.clusterName, result, tc.expected)
			}
		})
	}
}

func TestValidatePvCSIServiceAccountWithStore(t *testing.T) {
	synced := func() bool { return true }
	notSynced := func() bool { return false }

	scheme := setupSchemeForTest()
	// emptyClient: no API objects — proves the store path is taken, not the API
	emptyClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	guestCluster := makeVSphereCluster("guest-cluster", "vmware-system-csi")
	e2eCluster := makeVSphereCluster("test-cluster-e2e-script-95jxs", "test-gc-e2e-demo-ns")

	populatedStore := newFakeStore(guestCluster, e2eCluster)
	emptyStore := newFakeStore()

	testCases := []struct {
		name      string
		username  string
		store     cache.Store
		hasSynced cache.InformerSynced
		k8sClient client.Client
		expected  bool
	}{
		{
			name:      "store synced, cluster present — SA allowed",
			username:  "system:serviceaccount:vmware-system-csi:guest-cluster-pvcsi",
			store:     populatedStore,
			hasSynced: synced,
			k8sClient: emptyClient,
			expected:  true,
		},
		{
			name:      "store synced, cluster absent — SA rejected",
			username:  "system:serviceaccount:vmware-system-csi:nonexistent-cluster-pvcsi",
			store:     emptyStore,
			hasSynced: synced,
			k8sClient: emptyClient,
			expected:  false,
		},
		{
			name:      "store synced, e2e cluster SA — cluster found",
			username:  "system:serviceaccount:test-gc-e2e-demo-ns:test-cluster-e2e-script-95jxs-pvcsi",
			store:     populatedStore,
			hasSynced: synced,
			k8sClient: emptyClient,
			expected:  true,
		},
		{
			name:      "store synced, cluster exists but SA uses wrong namespace",
			username:  "system:serviceaccount:wrong-ns:guest-cluster-pvcsi",
			store:     populatedStore, // has guest-cluster in vmware-system-csi, not wrong-ns
			hasSynced: synced,
			k8sClient: emptyClient,
			expected:  false,
		},
		{
			name:      "store not synced, cluster found via API fallback",
			username:  "system:serviceaccount:vmware-system-csi:guest-cluster-pvcsi",
			store:     emptyStore, // empty — would return false if consulted
			hasSynced: notSynced,  // forces the API fallback path
			k8sClient: fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(guestCluster).
				Build(),
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validatePvCSIServiceAccount(context.TODO(), tc.username, tc.k8sClient,
				vsphereClusterCache{
					store:     tc.store,
					hasSynced: tc.hasSynced,
					version:   vsphereClusterDefaultVersion,
				})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tc.expected {
				t.Errorf("validatePvCSIServiceAccount(%q) = %v, want %v", tc.username, result, tc.expected)
			}
		})
	}
}
