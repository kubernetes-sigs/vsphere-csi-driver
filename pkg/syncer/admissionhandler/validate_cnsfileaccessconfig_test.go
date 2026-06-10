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
	goruntime "runtime"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
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
			Group:   "vmware.infrastructure.cluster.x-k8s.io",
			Version: "v1beta2",
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
func setupClusterAPIMocking(t *testing.T) *gomonkey.Patches {
	// Skip test on ARM64 due to gomonkey limitations
	if goruntime.GOARCH == "arm64" {
		t.Skip("Skipping test on ARM64 due to gomonkey function patching limitations")
	}

	// Setup fake client with VSphere cluster test data
	scheme := setupSchemeForTest()
	testVSphereClusters := createTestVSphereClusters()

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(convertVSphereClustersToObjects(testVSphereClusters)...).
		Build()

	// Mock getVSphereClusterClient to return our fake client with VSphere cluster test data
	patches := gomonkey.ApplyFunc(getVSphereClusterClient,
		func(ctx context.Context) (client.Client, error) {
			return fakeClient, nil
		})

	return patches
}

func TestValidatePvCSIServiceAccount(t *testing.T) {
	// Setup cluster API mocking with realistic test data
	patches := setupClusterAPIMocking(t)
	defer patches.Reset()
	testCases := []struct {
		name           string
		username       string
		expected       bool
		expectError    bool
		errorSubstring string // Optional: partial error message to match
	}{
		{
			name:        "Valid vsphere-csi-controller in valid namespace",
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
			name:           "Invalid namespace for test-cluster VSphereCluster validation",
			username:       "system:serviceaccount:vmware-system-csi:test-cluster-pvcsi",
			expected:       false,
			expectError:    true,
			errorSubstring: "failed to check VSphereCluster resource", // Namespace mismatch expected
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
			name:           "SECURITY FIX: malicious service account in wrong namespace blocked",
			username:       "system:serviceaccount:malicious-ns:evil-pvcsi",
			expected:       false,
			expectError:    true,
			errorSubstring: "failed to check VSphereCluster resource", // Now properly blocked - wrong namespace
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
		// Note: VSphereCluster validation tests are omitted as they require a real cluster
		// The dynamic client approach will work with actual deployed API groups
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validatePvCSIServiceAccount(context.TODO(), tc.username)

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
	patches := setupClusterAPIMocking(t)
	defer patches.Reset()

	testCases := []struct {
		name     string
		username string
		expected bool
	}{
		{
			name:     "Valid PvCSI service account",
			username: "system:serviceaccount:vmware-system-csi:vsphere-csi-controller",
			expected: false,
		},
		{
			name:     "Valid Kubernetes service account",
			username: "system:serviceaccount:kube-system:namespace-controller",
			expected: true,
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
			result, err := isUserAllowedForDeletion(context.TODO(), tc.username)
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
