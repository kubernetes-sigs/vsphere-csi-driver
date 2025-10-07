/*
Copyright 2025 The Kubernetes Authors.

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

package util

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

func TestGetSnatIpFromNamespaceNetworkInfo(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{
		Group:    "nsx.vmware.com",
		Version:  "v1alpha1",
		Resource: "namespacenetworkinfos",
	}

	namespace := "test-namespace"
	vmName := "test-vm"

	tests := []struct {
		name           string
		initialObjects []runtime.Object
		expectedIP     string
		expectError    bool
	}{
		{
			name: "Happy path - SNAT IP present",
			initialObjects: []runtime.Object{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "nsx.vmware.com/v1alpha1",
						"kind":       "NamespaceNetworkInfo",
						"metadata": map[string]interface{}{
							"name":      namespace,
							"namespace": namespace,
						},
						"topology": map[string]interface{}{
							"defaultEgressIP": "10.10.10.10",
						},
					},
				},
			},
			expectedIP:  "10.10.10.10",
			expectError: false,
		},
		{
			name:           "Error - resource not found",
			initialObjects: []runtime.Object{}, // Nothing in fake client
			expectedIP:     "",
			expectError:    true,
		},
		{
			name: "Error - defaultEgressIP missing",
			initialObjects: []runtime.Object{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "nsx.vmware.com/v1alpha1",
						"kind":       "NamespaceNetworkInfo",
						"metadata": map[string]interface{}{
							"name":      namespace,
							"namespace": namespace,
						},
						"spec": map[string]interface{}{
							"topology": map[string]interface{}{
								// defaultEgressIP intentionally missing
							},
						},
					},
				},
			},
			expectedIP:  "",
			expectError: true,
		},
		{
			name: "Error - defaultEgressIP is empty",
			initialObjects: []runtime.Object{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "nsx.vmware.com/v1alpha1",
						"kind":       "NamespaceNetworkInfo",
						"metadata": map[string]interface{}{
							"name":      namespace,
							"namespace": namespace,
						},
						"spec": map[string]interface{}{
							"topology": map[string]interface{}{
								"defaultEgressIP": "",
							},
						},
					},
				},
			},
			expectedIP:  "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			scheme.AddKnownTypes(gvr.GroupVersion())
			fakeClient := fake.NewSimpleDynamicClient(scheme, tt.initialObjects...)
			ip, err := getSnatIpFromNamespaceNetworkInfo(ctx, fakeClient, namespace, vmName)

			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, ip)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedIP, ip)
			}
		})
	}
}

func TestGetMaxWorkerThreads(t *testing.T) {
	envVar := "MAX_WORKER_THREADS"

	t.Run("WhenEnvVarNotSet", func(t *testing.T) {
		// Setup
		err := os.Unsetenv(envVar)
		if err != nil {
			t.Fatalf("Failed to unset env var: %v", err)
		}

		defVal, expVal := 10, 10

		// Execute
		val := GetMaxWorkerThreads(context.Background(), envVar, defVal)

		// Assert
		assert.Equal(t, expVal, val)
	})

	t.Run("WhenEnvNotInteger", func(t *testing.T) {
		// Setup
		err := os.Setenv(envVar, "non-integer")
		if err != nil {
			t.Fatalf("Failed to set env var: %v", err)
		}

		defVal, expVal := 10, 10

		// Execute
		val := GetMaxWorkerThreads(context.Background(), envVar, defVal)

		// Assert
		assert.Equal(t, expVal, val)
	})

	t.Run("WhenEnvNotInExpRange", func(t *testing.T) {
		// Setup
		err := os.Setenv(envVar, "-10000")
		if err != nil {
			t.Fatalf("Failed to set env var: %v", err)
		}

		defVal, expVal := 10, 10

		// Execute
		val := GetMaxWorkerThreads(context.Background(), envVar, defVal)

		// Assert
		assert.Equal(t, expVal, val)
	})

	t.Run("WhenEnvValid", func(t *testing.T) {
		// Setup
		err := os.Setenv(envVar, "10")
		if err != nil {
			t.Fatalf("Failed to set env var: %v", err)
		}

		defVal, expVal := 50, 10

		// Execute
		val := GetMaxWorkerThreads(context.Background(), envVar, defVal)

		// Assert
		assert.Equal(t, expVal, val)
	})
}
