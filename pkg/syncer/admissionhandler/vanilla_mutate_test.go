package admissionhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

func TestMutateNewPVC(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                      string
		pvc                       *corev1.PersistentVolumeClaim
		linkedCloneSupportEnabled bool
		expectedMutated           bool
		expectedLabels            map[string]string
		expectedError             bool
	}{
		{
			name: "LinkedClone FSS disabled - no mutation",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			linkedCloneSupportEnabled: false,
			expectedMutated:           false,
			expectedLabels:            nil,
		},
		{
			name: "LinkedClone FSS enabled but no annotation - no mutation",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			linkedCloneSupportEnabled: true,
			expectedMutated:           false,
			expectedLabels:            nil,
		},
		{
			name: "LinkedClone FSS enabled with annotation false - no mutation",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "false",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			linkedCloneSupportEnabled: true,
			expectedMutated:           false,
			expectedLabels:            nil,
		},
		{
			name: "LinkedClone FSS enabled with annotation true - mutation expected",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			linkedCloneSupportEnabled: true,
			expectedMutated:           true,
			expectedLabels: map[string]string{
				common.LinkedClonePVCLabel: "true",
			},
		},
		{
			name: "LinkedClone FSS enabled with existing labels - mutation expected",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
					Labels: map[string]string{
						"existing-label": "existing-value",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			linkedCloneSupportEnabled: true,
			expectedMutated:           true,
			expectedLabels: map[string]string{
				"existing-label":           "existing-value",
				common.LinkedClonePVCLabel: "true",
			},
		},
		{
			name: "LinkedClone FSS enabled with label already present - no mutation",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
					Labels: map[string]string{
						common.LinkedClonePVCLabel: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			linkedCloneSupportEnabled: true,
			expectedMutated:           false,
			expectedLabels: map[string]string{
				common.LinkedClonePVCLabel: "true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the global feature flag
			originalFeatureFlag := featureIsLinkedCloneSupportEnabled
			featureIsLinkedCloneSupportEnabled = tt.linkedCloneSupportEnabled
			defer func() {
				featureIsLinkedCloneSupportEnabled = originalFeatureFlag
			}()

			// Marshal PVC to raw bytes
			pvcBytes, err := json.Marshal(tt.pvc)
			require.NoError(t, err)

			// Create admission request
			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Create,
					Kind: metav1.GroupVersionKind{
						Group:   "",
						Version: "v1",
						Kind:    "PersistentVolumeClaim",
					},
					Object: runtime.RawExtension{
						Raw: pvcBytes,
					},
				},
			}

			// Call the function
			resp := mutateNewPVC(ctx, req)

			// Verify response
			if tt.expectedError {
				assert.False(t, resp.Allowed)
				assert.NotNil(t, resp.Result)
				return
			}

			assert.True(t, resp.Allowed)
			// admission.Allowed("") always creates a Result with code 200
			if resp.Result != nil {
				assert.Equal(t, int32(200), resp.Result.Code)
			}

			if !tt.expectedMutated {
				// No mutation expected - should have no patches
				assert.Empty(t, resp.Patches)
			} else {
				// Mutation expected - should have patches
				assert.NotEmpty(t, resp.Patches)

				// Debug: print all patches (can be removed in production)
				t.Logf("Number of patches: %d", len(resp.Patches))
				for i, patch := range resp.Patches {
					t.Logf("Patch %d: Op=%s, Path=%s, Value=%v", i, patch.Operation, patch.Path, patch.Value)
				}

				// Verify the patches contain the expected label
				found := false
				for _, patch := range resp.Patches {
					if patch.Operation == "add" {
						if patch.Path == "/metadata/labels/"+common.LinkedClonePVCLabel {
							// Individual label patch
							assert.Equal(t, "true", patch.Value)
							found = true
							break
						} else if patch.Path == "/metadata/labels" {
							// Entire labels object patch
							if labelsMap, ok := patch.Value.(map[string]interface{}); ok {
								if val, exists := labelsMap[common.LinkedClonePVCLabel]; exists {
									assert.Equal(t, "true", val)
									found = true
									break
								}
							}
						}
					}
				}
				assert.True(t, found, "Expected linked clone label patch not found")
			}
		})
	}
}

func TestMutateNewPVC_InvalidJSON(t *testing.T) {
	ctx := context.Background()

	// Set the global feature flag
	originalFeatureFlag := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureFlag
	}()

	// Create admission request with invalid JSON
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Kind: metav1.GroupVersionKind{
				Group:   "",
				Version: "v1",
				Kind:    "PersistentVolumeClaim",
			},
			Object: runtime.RawExtension{
				Raw: []byte("invalid json"),
			},
		},
	}

	// Call the function
	resp := mutateNewPVC(ctx, req)

	// Verify error response
	assert.False(t, resp.Allowed)
	assert.NotNil(t, resp.Result)
	assert.Equal(t, http.StatusInternalServerError, int(resp.Result.Code))
}

func TestMutatePVC_Wrapper(t *testing.T) {
	ctx := context.Background()

	// Set the global feature flag
	originalFeatureFlag := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = true
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureFlag
	}()

	// Create test PVC with linked clone annotation
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-ns",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}

	// Marshal PVC to raw bytes
	pvcBytes, err := json.Marshal(pvc)
	require.NoError(t, err)

	// Create admission request
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Kind: metav1.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "PersistentVolumeClaim",
		},
		Object: runtime.RawExtension{
			Raw: pvcBytes,
		},
	}

	// Call the wrapper function
	resp := mutatePVC(ctx, req)

	// Verify response
	assert.True(t, resp.Allowed)
	// admission.Allowed("") always creates a Result with code 200
	if resp.Result != nil {
		assert.Equal(t, int32(200), resp.Result.Code)
	}
	assert.NotNil(t, resp.Patch)
	assert.NotNil(t, resp.PatchType)
	assert.Equal(t, admissionv1.PatchTypeJSONPatch, *resp.PatchType)

	// Verify the patch contains the expected operations
	var patches []map[string]interface{}
	err = json.Unmarshal(resp.Patch, &patches)
	require.NoError(t, err)
	assert.NotEmpty(t, patches)

	// Find the linked clone label patch
	found := false
	for _, patch := range patches {
		if patch["op"] == "add" {
			if patch["path"] == "/metadata/labels/"+common.LinkedClonePVCLabel {
				// Individual label patch
				assert.Equal(t, "true", patch["value"])
				found = true
				break
			} else if patch["path"] == "/metadata/labels" {
				// Entire labels object patch
				if labelsMap, ok := patch["value"].(map[string]interface{}); ok {
					if val, exists := labelsMap[common.LinkedClonePVCLabel]; exists {
						assert.Equal(t, "true", val)
						found = true
						break
					}
				}
			}
		}
	}
	assert.True(t, found, "Expected linked clone label patch not found")
}

func TestMutatePVC_Wrapper_NoMutation(t *testing.T) {
	ctx := context.Background()

	// Set the global feature flag to disabled
	originalFeatureFlag := featureIsLinkedCloneSupportEnabled
	featureIsLinkedCloneSupportEnabled = false
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureFlag
	}()

	// Create test PVC with linked clone annotation
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-ns",
			Annotations: map[string]string{
				common.AnnKeyLinkedClone: "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}

	// Marshal PVC to raw bytes
	pvcBytes, err := json.Marshal(pvc)
	require.NoError(t, err)

	// Create admission request
	req := &admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Kind: metav1.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "PersistentVolumeClaim",
		},
		Object: runtime.RawExtension{
			Raw: pvcBytes,
		},
	}

	// Call the wrapper function
	resp := mutatePVC(ctx, req)

	// Verify response - should be allowed with no patches
	assert.True(t, resp.Allowed)
	// admission.Allowed("") always creates a Result with code 200
	if resp.Result != nil {
		assert.Equal(t, int32(200), resp.Result.Code)
	}
	assert.Nil(t, resp.Patch)
	assert.Nil(t, resp.PatchType)
}
