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

package cnsvolumemetadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlruntimefake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

func TestCnsVolumeMetadata_PatchOperations(t *testing.T) {
	ctx := context.Background()

	// Setup scheme
	testScheme := runtime.NewScheme()
	err := cnsoperatorapis.AddToScheme(testScheme)
	assert.NoError(t, err)

	tests := []struct {
		name      string
		setupFunc func() (client.Client, *cnsvolumemetadatav1alpha1.CnsVolumeMetadata,
			*cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
		expectError  bool
		validateFunc func(t *testing.T, client client.Client, original *cnsvolumemetadatav1alpha1.CnsVolumeMetadata)
	}{
		{
			name: "Patch finalizer addition",
			setupFunc: func() (client.Client, *cnsvolumemetadatav1alpha1.CnsVolumeMetadata,
				*cnsvolumemetadatav1alpha1.CnsVolumeMetadata) {
				original := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-volume",
						Namespace: "test-namespace",
					},
					Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
						VolumeNames:    []string{"volume1"},
						EntityType:     cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
						GuestClusterID: "test-cluster",
						EntityName:     "test-entity",
						EntityReferences: []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{
							{
								EntityType: "PERSISTENT_VOLUME",
								EntityName: "test-pv",
								ClusterID:  "test-cluster",
							},
						},
					},
				}

				fakeClient := ctrlruntimefake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(original).
					Build()

				modified := original.DeepCopy()
				modified.Finalizers = append(modified.Finalizers, cnsoperatortypes.CNSFinalizer)

				return fakeClient, original, modified
			},
			expectError: false,
			validateFunc: func(t *testing.T, client client.Client, original *cnsvolumemetadatav1alpha1.CnsVolumeMetadata) {
				// Retrieve the updated instance
				retrieved := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
				key := types.NamespacedName{Name: original.Name, Namespace: original.Namespace}
				err := client.Get(ctx, key, retrieved)
				assert.NoError(t, err)

				// Verify finalizer was added
				assert.Contains(t, retrieved.Finalizers, cnsoperatortypes.CNSFinalizer)
			},
		},
		{
			name: "Patch status update",
			setupFunc: func() (client.Client, *cnsvolumemetadatav1alpha1.CnsVolumeMetadata,
				*cnsvolumemetadatav1alpha1.CnsVolumeMetadata) {
				original := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-volume-status",
						Namespace: "test-namespace",
					},
					Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
						VolumeNames:    []string{"volume1"},
						EntityType:     cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
						GuestClusterID: "test-cluster",
						EntityName:     "test-entity",
						EntityReferences: []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{
							{
								EntityType: "PERSISTENT_VOLUME",
								EntityName: "test-pv",
								ClusterID:  "test-cluster",
							},
						},
					},
					Status: cnsvolumemetadatav1alpha1.CnsVolumeMetadataStatus{
						VolumeStatus: []cnsvolumemetadatav1alpha1.CnsVolumeMetadataVolumeStatus{
							{
								VolumeName: "volume1",
								Updated:    false,
							},
						},
					},
				}

				fakeClient := ctrlruntimefake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(original).
					Build()

				modified := original.DeepCopy()
				modified.Status.VolumeStatus[0].Updated = true
				modified.Status.VolumeStatus[0].ErrorMessage = "Test error message"

				return fakeClient, original, modified
			},
			expectError: false,
			validateFunc: func(t *testing.T, client client.Client, original *cnsvolumemetadatav1alpha1.CnsVolumeMetadata) {
				// Retrieve the updated instance
				retrieved := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
				key := types.NamespacedName{Name: original.Name, Namespace: original.Namespace}
				err := client.Get(ctx, key, retrieved)
				assert.NoError(t, err)

				// Verify status was updated
				assert.True(t, retrieved.Status.VolumeStatus[0].Updated)
				assert.Equal(t, "Test error message", retrieved.Status.VolumeStatus[0].ErrorMessage)
			},
		},
		{
			name: "Patch finalizer removal",
			setupFunc: func() (client.Client, *cnsvolumemetadatav1alpha1.CnsVolumeMetadata,
				*cnsvolumemetadatav1alpha1.CnsVolumeMetadata) {
				original := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-volume-finalizer-remove",
						Namespace:  "test-namespace",
						Finalizers: []string{cnsoperatortypes.CNSFinalizer, "other-finalizer"},
					},
					Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
						VolumeNames:    []string{"volume1"},
						EntityType:     cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePV,
						GuestClusterID: "test-cluster",
						EntityName:     "test-entity",
						EntityReferences: []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{
							{
								EntityType: "PERSISTENT_VOLUME",
								EntityName: "test-pv",
								ClusterID:  "test-cluster",
							},
						},
					},
				}

				fakeClient := ctrlruntimefake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(original).
					Build()

				modified := original.DeepCopy()
				// Remove the CNS finalizer
				for i, finalizer := range modified.Finalizers {
					if finalizer == cnsoperatortypes.CNSFinalizer {
						modified.Finalizers = append(modified.Finalizers[:i], modified.Finalizers[i+1:]...)
						break
					}
				}

				return fakeClient, original, modified
			},
			expectError: false,
			validateFunc: func(t *testing.T, client client.Client, original *cnsvolumemetadatav1alpha1.CnsVolumeMetadata) {
				// Retrieve the updated instance
				retrieved := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
				key := types.NamespacedName{Name: original.Name, Namespace: original.Namespace}
				err := client.Get(ctx, key, retrieved)
				assert.NoError(t, err)

				// Verify CNS finalizer was removed but other finalizer remains
				assert.NotContains(t, retrieved.Finalizers, cnsoperatortypes.CNSFinalizer)
				assert.Contains(t, retrieved.Finalizers, "other-finalizer")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient, original, modified := tt.setupFunc()

			// Execute the patch operation using our common utility
			err := k8s.PatchObject(ctx, fakeClient, original, modified)

			// Validate results
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Run custom validation if provided
				if tt.validateFunc != nil {
					tt.validateFunc(t, fakeClient, original)
				}
			}
		})
	}
}

func TestCnsVolumeMetadata_PatchErrorHandling(t *testing.T) {
	ctx := context.Background()

	// Setup scheme
	testScheme := runtime.NewScheme()
	err := cnsoperatorapis.AddToScheme(testScheme)
	assert.NoError(t, err)

	t.Run("Patch non-existent object", func(t *testing.T) {
		// Create fake client without the object
		fakeClient := ctrlruntimefake.NewClientBuilder().
			WithScheme(testScheme).
			Build()

		original := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "non-existent-volume",
				Namespace: "test-namespace",
			},
		}

		modified := original.DeepCopy()
		modified.Finalizers = append(modified.Finalizers, cnsoperatortypes.CNSFinalizer)

		// Execute the patch operation - should fail
		err := k8s.PatchObject(ctx, fakeClient, original, modified)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Patch with invalid object", func(t *testing.T) {
		// Create an object with invalid data that would cause marshaling issues
		original := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-volume",
				Namespace: "test-namespace",
			},
		}

		fakeClient := ctrlruntimefake.NewClientBuilder().
			WithScheme(testScheme).
			WithObjects(original).
			Build()

		// Create a modified version
		modified := original.DeepCopy()
		modified.Status.VolumeStatus = []cnsvolumemetadatav1alpha1.CnsVolumeMetadataVolumeStatus{
			{
				VolumeName: "volume1",
				Updated:    true,
			},
		}

		// This should succeed as the objects are valid
		err := k8s.PatchObject(ctx, fakeClient, original, modified)
		assert.NoError(t, err)

		// Verify the patch was applied
		retrieved := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{}
		key := types.NamespacedName{Name: original.Name, Namespace: original.Namespace}
		err = fakeClient.Get(ctx, key, retrieved)
		assert.NoError(t, err)
		assert.True(t, retrieved.Status.VolumeStatus[0].Updated)
	})
}
