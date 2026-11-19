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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func TestValidationHandlerRejectsOversizedBody(t *testing.T) {
	body := bytes.Repeat([]byte("a"), maxAdmissionReviewBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	validationHandler(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestValidationHandlerRejectsEmptyBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/validate", http.NoBody)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	validationHandler(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestValidationHandlerAcceptsLargePVCUpdate verifies that a valid PVC UPDATE
// review is not rejected by the body size limit. The review carries both the
// new and the old PVC, each with an annotation that uses the full 256 KiB
// annotation budget and is made of control characters, which JSON-encode to
// six bytes each (\u00XX). Together they serialize to more than 3 MiB.
func TestValidationHandlerAcceptsLargePVCUpdate(t *testing.T) {
	origBlockVolumeSnapshot := featureGateBlockVolumeSnapshotEnabled
	origVACPolicyMutability := featureIsVACPolicyMutabilityEnabled
	featureGateBlockVolumeSnapshotEnabled = false
	featureIsVACPolicyMutabilityEnabled = false
	defer func() {
		featureGateBlockVolumeSnapshotEnabled = origBlockVolumeSnapshot
		featureIsVACPolicyMutabilityEnabled = origVACPolicyMutability
	}()

	const annotationKey = "example.com/large"
	newPVC := func(value string) []byte {
		pvc := corev1.PersistentVolumeClaim{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pvc",
				Namespace:   "default",
				Annotations: map[string]string{annotationKey: value},
			},
		}
		raw, err := json.Marshal(pvc)
		assert.NoError(t, err)
		return raw
	}
	// Leave one byte for the "\x02" appended to the new PVC's annotation.
	annotationValue := strings.Repeat("\x01",
		apimachineryvalidation.TotalAnnotationSizeLimitB-len(annotationKey)-1)
	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request: &admissionv1.AdmissionRequest{
			UID:       types.UID("test-uid"),
			Kind:      metav1.GroupVersionKind{Version: "v1", Kind: "PersistentVolumeClaim"},
			Name:      "test-pvc",
			Namespace: "default",
			Operation: admissionv1.Update,
			Object:    runtime.RawExtension{Raw: newPVC(annotationValue + "\x02")},
			OldObject: runtime.RawExtension{Raw: newPVC(annotationValue)},
		},
	}
	body, err := json.Marshal(review)
	assert.NoError(t, err)
	assert.Greater(t, len(body), 3<<20)
	assert.LessOrEqual(t, len(body), maxAdmissionReviewBodyBytes)

	req := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	validationHandler(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	resp := admissionv1.AdmissionReview{}
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	if assert.NotNil(t, resp.Response) {
		assert.True(t, resp.Response.Allowed)
		assert.Equal(t, types.UID("test-uid"), resp.Response.UID)
	}
}
