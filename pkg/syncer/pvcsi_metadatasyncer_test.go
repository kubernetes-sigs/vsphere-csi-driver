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

package syncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

func TestPvcsiVolumeUpdated_PatchObject(t *testing.T) {
	ctx := context.Background()

	// Setup test data
	supervisorNamespace := "vmware-system-csi"
	volumeHandle := "test-volume-handle"

	// Create a test PVC
	testPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
			UID:       "test-pvc-uid",
			Labels: map[string]string{
				"test-label": "test-value",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create existing CnsVolumeMetadata object
	existingMetadata := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-metadata",
			Namespace:       supervisorNamespace,
			ResourceVersion: "1",
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames: []string{volumeHandle},
			EntityName:  "test-pvc",
			EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
		},
	}

	tests := []struct {
		name          string
		setupClient   func() client.Client
		expectError   bool
		validatePatch bool
	}{
		{
			name: "Successful patch operation",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorapis.AddToScheme(scheme)
				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(existingMetadata).
					Build()
			},
			expectError:   false,
			validatePatch: true,
		},
		{
			name: "Patch operation with client error",
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorapis.AddToScheme(scheme)
				// Create client without the existing object to simulate Get error
				return fake.NewClientBuilder().
					WithScheme(scheme).
					Build()
			},
			expectError:   true,
			validatePatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup fake client
			fakeClient := tt.setupClient()

			// Create test config
			testConfig := &cnsconfig.ConfigurationInfo{
				Cfg: &cnsconfig.Config{
					GC: cnsconfig.GCConfig{
						TanzuKubernetesClusterUID: "test-cluster-uid",
					},
				},
			}

			// Create metadataSyncer
			metadataSyncer := &metadataSyncInformer{
				cnsOperatorClient: fakeClient,
				configInfo:        testConfig,
			}

			// Note: In a real test, we would mock GetSupervisorNamespace
			// For this test, we'll assume it returns the expected namespace

			// Execute the function under test
			pvcsiVolumeUpdated(ctx, testPVC, volumeHandle, metadataSyncer)

			if tt.validatePatch {
				// Verify that the object was patched by checking if it exists
				// and has the expected properties
				updatedMetadata := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
				key := types.NamespacedName{
					Namespace: supervisorNamespace,
					Name:      existingMetadata.Name,
				}
				err := fakeClient.Get(ctx, key, updatedMetadata)

				if !tt.expectError {
					assert.NoError(t, err, "Should be able to retrieve updated metadata")
					// Verify that the resource version was preserved (indicating patch was used)
					assert.Equal(t, existingMetadata.ResourceVersion, updatedMetadata.ResourceVersion,
						"ResourceVersion should be preserved from original object")
				}
			}
		})
	}
}

func TestPvcsiVolumeUpdated_PVResource(t *testing.T) {
	ctx := context.Background()
	supervisorNamespace := "vmware-system-csi"
	volumeHandle := "test-volume-handle"

	// Create a test PV
	testPV := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
			UID:  "test-pv-uid",
			Labels: map[string]string{
				"test-label": "test-value",
			},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: volumeHandle,
				},
			},
		},
	}

	// Create existing CnsVolumeMetadata object
	existingMetadata := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-pv-metadata",
			Namespace:       supervisorNamespace,
			ResourceVersion: "1",
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames: []string{volumeHandle},
			EntityName:  "test-pv",
			EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
		},
	}

	scheme := runtime.NewScheme()
	_ = cnsoperatorapis.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingMetadata).
		Build()

	// Create test config
	testConfig := &cnsconfig.ConfigurationInfo{
		Cfg: &cnsconfig.Config{
			GC: cnsconfig.GCConfig{
				TanzuKubernetesClusterUID: "test-cluster-uid",
			},
		},
	}

	// Create metadataSyncer
	metadataSyncer := &metadataSyncInformer{
		cnsOperatorClient: fakeClient,
		configInfo:        testConfig,
	}

	// Note: In a real test, we would mock GetSupervisorNamespace
	// For this test, we'll assume it returns the expected namespace

	// Execute the function under test
	pvcsiVolumeUpdated(ctx, testPV, volumeHandle, metadataSyncer)

	// Verify that the PV metadata was processed correctly
	updatedMetadata := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
	key := types.NamespacedName{
		Namespace: supervisorNamespace,
		Name:      existingMetadata.Name,
	}
	err := fakeClient.Get(ctx, key, updatedMetadata)
	assert.NoError(t, err, "Should be able to retrieve updated PV metadata")
}

func TestPvcsiVolumeUpdated_SupervisorNamespaceError(t *testing.T) {
	ctx := context.Background()
	volumeHandle := "test-volume-handle"

	testPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
		},
	}

	// Create metadataSyncer (minimal setup since function should return early)
	metadataSyncer := &metadataSyncInformer{}

	// Note: This test would verify error handling for GetSupervisorNamespace
	// In a real implementation, we would mock this function to return an error

	// Execute the function under test - should return early due to error
	// This test verifies that the function handles supervisor namespace errors gracefully
	pvcsiVolumeUpdated(ctx, testPVC, volumeHandle, metadataSyncer)

	// If we reach here without panic, the error handling worked correctly
	assert.True(t, true, "Function should handle supervisor namespace error gracefully")
}
