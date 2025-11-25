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
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	testclient "k8s.io/client-go/kubernetes/fake"
)

func TestUpdateVolumeHealthStatus(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name               string
		pvc                *v1.PersistentVolumeClaim
		volumeHealthStatus string
		expectedAnnotation string
		shouldUpdatePVC    bool
	}{
		{
			name: "Update healthy volume status",
			pvc: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "default",
					UID:       types.UID("test-uid"),
				},
				Spec: v1.PersistentVolumeClaimSpec{
					VolumeName: "test-pv",
				},
			},
			volumeHealthStatus: "accessible",
			expectedAnnotation: "accessible",
			shouldUpdatePVC:    true,
		},
		{
			name: "Update inaccessible volume status",
			pvc: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-2",
					Namespace: "default",
					UID:       types.UID("test-uid-2"),
				},
				Spec: v1.PersistentVolumeClaimSpec{
					VolumeName: "test-pv-2",
				},
			},
			volumeHealthStatus: "inaccessible",
			expectedAnnotation: "inaccessible",
			shouldUpdatePVC:    true,
		},
		{
			name: "No update needed - same status",
			pvc: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc-3",
					Namespace: "default",
					UID:       types.UID("test-uid-3"),
					Annotations: map[string]string{
						annVolumeHealth: "accessible",
					},
				},
				Spec: v1.PersistentVolumeClaimSpec{
					VolumeName: "test-pv-3",
				},
			},
			volumeHealthStatus: "accessible",
			expectedAnnotation: "accessible",
			shouldUpdatePVC:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client with the PVC
			k8sclient := testclient.NewSimpleClientset(tt.pvc)

			// Call updateVolumeHealthStatus with correct parameter order
			updateVolumeHealthStatus(ctx, k8sclient, tt.pvc, tt.volumeHealthStatus)

			// Verify the PVC was updated correctly
			updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims(tt.pvc.Namespace).Get(
				ctx, tt.pvc.Name, metav1.GetOptions{})
			assert.NoError(t, err)

			if tt.shouldUpdatePVC {
				assert.Equal(t, tt.expectedAnnotation, updatedPVC.Annotations[annVolumeHealth])
			}

			// Verify timestamp annotation is set
			if tt.shouldUpdatePVC {
				assert.NotEmpty(t, updatedPVC.Annotations[annVolumeHealthTS])
			}
		})
	}
}

func TestVolumeHealthAnnotationConstants(t *testing.T) {
	// Test that the volume health annotation constants are properly defined
	assert.Equal(t, "volumehealth.storage.kubernetes.io/health", annVolumeHealth)
	assert.Equal(t, "volumehealth.storage.kubernetes.io/health-timestamp", annVolumeHealthTS)
}

func TestVolumeHealthTimestamp(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc-timestamp",
			Namespace: "default",
			UID:       types.UID("test-uid-timestamp"),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv-timestamp",
		},
	}

	k8sclient := testclient.NewSimpleClientset(pvc)

	// Record time before update
	beforeUpdate := time.Now()

	// Call updateVolumeHealthStatus
	updateVolumeHealthStatus(ctx, k8sclient, pvc, "accessible")

	// Record time after update
	afterUpdate := time.Now()

	// Verify the PVC was updated with timestamp
	updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
		ctx, pvc.Name, metav1.GetOptions{})
	assert.NoError(t, err)

	// Verify timestamp annotation exists and is reasonable
	timestampStr := updatedPVC.Annotations[annVolumeHealthTS]
	assert.NotEmpty(t, timestampStr)

	// Parse timestamp and verify it's within reasonable bounds
	timestamp, err := time.Parse(time.UnixDate, timestampStr)
	assert.NoError(t, err)
	assert.True(t, timestamp.After(beforeUpdate.Add(-time.Second)))
	assert.True(t, timestamp.Before(afterUpdate.Add(time.Second)))
}

func TestVolumeHealthStatusUpdate_ExistingAnnotations(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc-existing",
			Namespace: "default",
			UID:       types.UID("test-uid-existing"),
			Annotations: map[string]string{
				"existing-annotation": "existing-value",
				annVolumeHealth:       "inaccessible",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv-existing",
		},
	}

	k8sclient := testclient.NewSimpleClientset(pvc)

	// Update to accessible status
	updateVolumeHealthStatus(ctx, k8sclient, pvc, "accessible")

	// Verify the PVC was updated correctly
	updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
		ctx, pvc.Name, metav1.GetOptions{})
	assert.NoError(t, err)

	// Verify health status was updated
	assert.Equal(t, "accessible", updatedPVC.Annotations[annVolumeHealth])

	// Verify existing annotation is preserved
	assert.Equal(t, "existing-value", updatedPVC.Annotations["existing-annotation"])

	// Verify timestamp annotation is set
	assert.NotEmpty(t, updatedPVC.Annotations[annVolumeHealthTS])
}

func TestVolumeHealthStatusUpdate_EmptyStatus(t *testing.T) {
	ctx := context.Background()

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc-empty",
			Namespace: "default",
			UID:       types.UID("test-uid-empty"),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv-empty",
		},
	}

	k8sclient := testclient.NewSimpleClientset(pvc)

	// Update with empty status
	updateVolumeHealthStatus(ctx, k8sclient, pvc, "")

	// Verify the PVC was updated with empty status
	updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(
		ctx, pvc.Name, metav1.GetOptions{})
	assert.NoError(t, err)

	// Verify empty health status was set
	assert.Equal(t, "", updatedPVC.Annotations[annVolumeHealth])

	// Verify timestamp annotation is still set
	assert.NotEmpty(t, updatedPVC.Annotations[annVolumeHealthTS])
}
