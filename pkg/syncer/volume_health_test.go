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
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	testclient "k8s.io/client-go/kubernetes/fake"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	fvsapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume"
	fvv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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
			k8sclient := testclient.NewClientset(tt.pvc)

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

	k8sclient := testclient.NewClientset(pvc)

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

	k8sclient := testclient.NewClientset(pvc)

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

	k8sclient := testclient.NewClientset(pvc)

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

// newFVSScheme returns a runtime.Scheme with FileVolume types registered.
func newFVSScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = fvsapis.AddToScheme(s)
	return s
}

// newTestFileVolume creates a FileVolume CR for testing with the given conditions.
func newTestFileVolume(namespace, name string, conditions []metav1.Condition) *fvv1alpha1.FileVolume {
	return &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size:   resource.MustParse("10Gi"),
			PvcUID: name,
		},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:      fvv1alpha1.FileVolumePhaseReady,
			Conditions: conditions,
		},
	}
}

func TestDeriveHealthFromFileVolumeConditions(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		want       string
	}{
		{
			name: "Scenario 1: BackendReady=True AND ExportReady=True -> accessible",
			conditions: []metav1.Condition{
				{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue},
				{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionTrue},
			},
			want: common.VolHealthStatusAccessible,
		},
		{
			name: "Scenario 2: BackendReady=True, ExportReady=False -> inaccessible",
			conditions: []metav1.Condition{
				{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue},
				{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionFalse,
					Reason: "ExportDrift", Message: "ShowExports found no export for this volume"},
			},
			want: common.VolHealthStatusInaccessible,
		},
		{
			name: "Scenario 3: BackendReady=False, ExportReady absent -> inaccessible",
			conditions: []metav1.Condition{
				{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionFalse,
					Reason: "VdfsInsufficientSpace"},
			},
			want: common.VolHealthStatusInaccessible,
		},
		{
			name: "Scenario 4: BackendReady=True, ExportReady absent (provisioning) -> inaccessible",
			conditions: []metav1.Condition{
				{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue,
					Reason: "Created"},
			},
			want: common.VolHealthStatusInaccessible,
		},
		{
			name:       "No conditions at all -> inaccessible",
			conditions: nil,
			want:       common.VolHealthStatusInaccessible,
		},
		{
			name: "Both conditions present but both False -> inaccessible",
			conditions: []metav1.Condition{
				{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionFalse},
				{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionFalse},
			},
			want: common.VolHealthStatusInaccessible,
		},
		{
			name: "Ready condition True but BackendReady/ExportReady absent -> inaccessible",
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue},
			},
			want: common.VolHealthStatusInaccessible,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveHealthFromFileVolumeConditions(tt.conditions)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetFileVolumeHealthStatus(t *testing.T) {
	ctx := context.Background()
	fvsScheme := newFVSScheme()

	t.Run("Scenario 1: both conditions true -> accessible", func(t *testing.T) {
		fv := newTestFileVolume("nsfvs-1234", "pvc-abc123", []metav1.Condition{
			{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue},
			{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionTrue},
		})
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).WithObjects(fv).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pvc", Namespace: "default", UID: "uid-1"},
		}
		k8sclient := testclient.NewClientset(pvc)
		fvsVolumes := volumeHandlePVCMap{
			"fv:nsfvs-1234:pvc-abc123": pvc,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 1, accessible)
		assert.Equal(t, 0, inaccessible)

		updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "my-pvc", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusAccessible, updatedPVC.Annotations[annVolumeHealth])
	})

	t.Run("Scenario 2: ExportReady=False -> inaccessible", func(t *testing.T) {
		fv := newTestFileVolume("nsfvs-1234", "pvc-def456", []metav1.Condition{
			{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue},
			{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionFalse},
		})
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).WithObjects(fv).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pvc-2", Namespace: "default", UID: "uid-2"},
		}
		k8sclient := testclient.NewClientset(pvc)
		fvsVolumes := volumeHandlePVCMap{
			"fv:nsfvs-1234:pvc-def456": pvc,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 0, accessible)
		assert.Equal(t, 1, inaccessible)

		updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "my-pvc-2", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusInaccessible, updatedPVC.Annotations[annVolumeHealth])
	})

	t.Run("Scenario 3: BackendReady=False, ExportReady absent -> inaccessible", func(t *testing.T) {
		fv := newTestFileVolume("nsfvs-5678", "pvc-ghi789", []metav1.Condition{
			{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionFalse,
				Reason: "VdfsInsufficientSpace"},
		})
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).WithObjects(fv).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pvc-3", Namespace: "default", UID: "uid-3"},
		}
		k8sclient := testclient.NewClientset(pvc)
		fvsVolumes := volumeHandlePVCMap{
			"fv:nsfvs-5678:pvc-ghi789": pvc,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 0, accessible)
		assert.Equal(t, 1, inaccessible)

		updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "my-pvc-3", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusInaccessible, updatedPVC.Annotations[annVolumeHealth])
	})

	t.Run("Scenario 4: provisioning - BackendReady=True, ExportReady absent -> inaccessible", func(t *testing.T) {
		fv := newTestFileVolume("nsfvs-5678", "pvc-jkl012", []metav1.Condition{
			{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue,
				Reason: "Created"},
		})
		fv.Status.Phase = fvv1alpha1.FileVolumePhaseProvisioning
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).WithObjects(fv).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pvc-4", Namespace: "default", UID: "uid-4"},
		}
		k8sclient := testclient.NewClientset(pvc)
		fvsVolumes := volumeHandlePVCMap{
			"fv:nsfvs-5678:pvc-jkl012": pvc,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 0, accessible)
		assert.Equal(t, 1, inaccessible)

		updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "my-pvc-4", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusInaccessible, updatedPVC.Annotations[annVolumeHealth])
	})

	t.Run("FileVolume CR not found -> inaccessible", func(t *testing.T) {
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pvc-5", Namespace: "default", UID: "uid-5"},
		}
		k8sclient := testclient.NewClientset(pvc)
		fvsVolumes := volumeHandlePVCMap{
			"fv:nsfvs-gone:pvc-missing": pvc,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 0, accessible)
		assert.Equal(t, 1, inaccessible)

		updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "my-pvc-5", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusInaccessible, updatedPVC.Annotations[annVolumeHealth])
	})

	t.Run("Malformed volume handle -> inaccessible", func(t *testing.T) {
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).Build()

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "my-pvc-6", Namespace: "default", UID: "uid-6"},
		}
		k8sclient := testclient.NewClientset(pvc)
		fvsVolumes := volumeHandlePVCMap{
			"not-fvs-handle": pvc,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 0, accessible)
		assert.Equal(t, 1, inaccessible)

		updatedPVC, err := k8sclient.CoreV1().PersistentVolumeClaims("default").Get(ctx, "my-pvc-6", metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusInaccessible, updatedPVC.Annotations[annVolumeHealth])
	})

	t.Run("Multiple FVS volumes - mixed health", func(t *testing.T) {
		fvHealthy := newTestFileVolume("nsfvs-a", "pvc-healthy", []metav1.Condition{
			{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue},
			{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionTrue},
		})
		fvUnhealthy := newTestFileVolume("nsfvs-a", "pvc-unhealthy", []metav1.Condition{
			{Type: common.FileVolumeConditionBackendReady, Status: metav1.ConditionTrue},
			{Type: common.FileVolumeConditionExportReady, Status: metav1.ConditionFalse},
		})
		fvClient := ctrlclientfake.NewClientBuilder().WithScheme(fvsScheme).
			WithObjects(fvHealthy, fvUnhealthy).Build()

		pvcH := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-h", Namespace: "default", UID: "uid-h"},
		}
		pvcU := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-u", Namespace: "default", UID: "uid-u"},
		}
		k8sclient := testclient.NewClientset(pvcH, pvcU)
		fvsVolumes := volumeHandlePVCMap{
			"fv:nsfvs-a:pvc-healthy":   pvcH,
			"fv:nsfvs-a:pvc-unhealthy": pvcU,
		}

		accessible, inaccessible := getFileVolumeHealthStatus(ctx, k8sclient, fvClient, fvsVolumes)
		assert.Equal(t, 1, accessible)
		assert.Equal(t, 1, inaccessible)
	})
}
