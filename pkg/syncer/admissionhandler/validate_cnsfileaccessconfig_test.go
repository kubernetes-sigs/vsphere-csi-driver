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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

// createTestClusters creates realistic cluster objects for different test scenarios
func createTestClusters() []ccV1beta2.Cluster {
	return []ccV1beta2.Cluster{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "guest-cluster",
				Namespace: "vmware-system-csi",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "prod-cluster",
				Namespace: "production",
			},
		},
	}
}

// convertClustersToObjects converts cluster objects to runtime.Object for fake client
func convertClustersToObjects(clusters []ccV1beta2.Cluster) []client.Object {
	objects := make([]client.Object, len(clusters))
	for i := range clusters {
		objects[i] = &clusters[i]
	}
	return objects
}

// setupClusterAPIMocking sets up gomonkey patches for cluster API client with realistic test data
// Returns patches that must be reset with defer patches.Reset()
func setupClusterAPIMocking(t *testing.T) *gomonkey.Patches {
	// Skip test on ARM64 due to gomonkey limitations
	if goruntime.GOARCH == "arm64" {
		t.Skip("Skipping test on ARM64 due to gomonkey function patching limitations")
	}

	// Setup fake cluster API client with realistic test data
	scheme := setupSchemeForTest()
	testClusters := createTestClusters()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(convertClustersToObjects(testClusters)...).
		Build()

	// Mock getClusterAPIClient to return our fake client with test cluster data
	patches := gomonkey.ApplyFunc(getClusterAPIClient,
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
		name     string
		username string
		expected bool
	}{
		{
			name:     "Valid vsphere-csi-controller in valid namespace",
			username: "system:serviceaccount:vmware-system-csi:vsphere-csi-controller",
			expected: false,
		},
		{
			name:     "Valid vsphere-csi-node in valid namespace",
			username: "system:serviceaccount:vmware-system-csi:vsphere-csi-node",
			expected: false,
		},
		{
			name:     "Valid legacy pvcsi service account",
			username: "system:serviceaccount:kube-system:pvcsi",
			expected: false,
		},
		{
			name:     "Valid test-cluster-pvcsi pattern with real cluster validation",
			username: "system:serviceaccount:vmware-system-csi:test-cluster-pvcsi",
			expected: true, // Matches "test-cluster" in our fake cluster list
		},
		{
			name:     "SECURITY FIX: malicious service account in wrong namespace blocked",
			username: "system:serviceaccount:malicious-ns:evil-pvcsi",
			expected: false, // Now properly blocked - wrong namespace
		},
		{
			name:     "SECURITY FIX: attempt to bypass with multiple colons blocked",
			username: "system:serviceaccount:namespace:extra:vsphere-csi-controller",
			expected: false,
		},
		{
			name:     "Invalid - wrong service account name in valid namespace",
			username: "system:serviceaccount:vmware-system-csi:invalid-service-account",
			expected: false,
		},
		{
			name:     "Invalid - missing namespace",
			username: "system:serviceaccount::vsphere-csi-controller",
			expected: false,
		},
		{
			name:     "Invalid - empty username",
			username: "",
			expected: false,
		},
		{
			name:     "Invalid - not a service account",
			username: "regular-user",
			expected: false,
		},
		{
			name:     "Invalid - kubernetes admin user",
			username: "kubernetes-admin",
			expected: false,
		},
		{
			name:     "Invalid - missing colon separators",
			username: "system:serviceaccountnamespacevsphere-csi-controller",
			expected: false,
		},
		{
			name:     "Invalid - partial match",
			username: "system:serviceaccount:namespace:vsphere-csi-contro",
			expected: false,
		},
		{
			name:     "Invalid - extra text after valid pattern",
			username: "system:serviceaccount:namespace:vsphere-csi-controller-extra",
			expected: false,
		},
		{
			name:     "Invalid - valid service account in unauthorized namespace",
			username: "system:serviceaccount:my-namespace-with-dashes:vsphere-csi-node",
			expected: false, // Wrong namespace - only vmware-system-csi allowed for CSI service accounts
		},
		{
			name:     "Invalid - valid service account in unauthorized namespace",
			username: "system:serviceaccount:namespace123:vsphere-csi-controller",
			expected: false, // Wrong namespace - only vmware-system-csi allowed for CSI service accounts
		},
		{
			name:     "Invalid - pvcsi service account in wrong namespace",
			username: "system:serviceaccount:some-namespace:pvcsi",
			expected: false, // pvcsi only allowed in kube-system namespace
		},
		{
			name:     "SECURITY FIX: multiple colon bypass attempt blocked",
			username: "system:serviceaccount:malicious:namespace:attacker-pvcsi",
			expected: false,
		},
		{
			name:     "SECURITY FIX: service account with colon in name blocked",
			username: "system:serviceaccount:vmware-system-csi:service:account-pvcsi",
			expected: false, // Colon in cluster name blocked by fallback validation
		},
		{
			name:     "Invalid - empty cluster name with -pvcsi suffix",
			username: "system:serviceaccount:vmware-system-csi:-pvcsi",
			expected: false, // Empty cluster name blocked by fallback validation
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validatePvCSIServiceAccount(tc.username)
			if err != nil {
				t.Errorf("validatePvCSIServiceAccount() returned error: %v", err)
				return
			}
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
			result, err := isUserAllowedForDeletion(tc.username)
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
