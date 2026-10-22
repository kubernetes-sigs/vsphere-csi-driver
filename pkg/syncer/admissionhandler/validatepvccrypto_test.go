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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
)

func newCryptoTestPVC(t *testing.T, encClassName string, labels map[string]string) []byte {
	storageClassName := "test-sc"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
		},
	}
	if encClassName != "" {
		crypto.SetEncryptionClassNameForPVC(pvc, encClassName)
	}
	raw, err := json.Marshal(pvc)
	assert.NoError(t, err)
	return raw
}

func TestValidatePVCRequestForCrypto(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		operation      admissionv1.Operation
		oldObject      []byte
		newObject      []byte
		encryptedSC    *bool // nil means IsEncryptedStorageClass must not be called
		expectedResult bool
	}{
		{
			name:           "Create with encryption class and encrypted storage class is allowed",
			operation:      admissionv1.Create,
			newObject:      newCryptoTestPVC(t, "enc-class", nil),
			encryptedSC:    ptr.To(true),
			expectedResult: true,
		},
		{
			name:           "Create with encryption class and non-encrypted storage class is denied",
			operation:      admissionv1.Create,
			newObject:      newCryptoTestPVC(t, "enc-class", nil),
			encryptedSC:    ptr.To(false),
			expectedResult: false,
		},
		{
			name:           "Update adding encryption class with non-encrypted storage class is denied",
			operation:      admissionv1.Update,
			oldObject:      newCryptoTestPVC(t, "", nil),
			newObject:      newCryptoTestPVC(t, "enc-class", nil),
			encryptedSC:    ptr.To(false),
			expectedResult: false,
		},
		{
			name:           "Update changing encryption class with non-encrypted storage class is denied",
			operation:      admissionv1.Update,
			oldObject:      newCryptoTestPVC(t, "enc-class-1", nil),
			newObject:      newCryptoTestPVC(t, "enc-class-2", nil),
			encryptedSC:    ptr.To(false),
			expectedResult: false,
		},
		{
			name:           "Update changing encryption class with encrypted storage class is allowed",
			operation:      admissionv1.Update,
			oldObject:      newCryptoTestPVC(t, "enc-class-1", nil),
			newObject:      newCryptoTestPVC(t, "enc-class-2", nil),
			encryptedSC:    ptr.To(true),
			expectedResult: true,
		},
		{
			name:           "Update not touching encryption class is allowed without validation",
			operation:      admissionv1.Update,
			oldObject:      newCryptoTestPVC(t, "enc-class", nil),
			newObject:      newCryptoTestPVC(t, "enc-class", map[string]string{"foo": "bar"}),
			expectedResult: true,
		},
		{
			name:           "Update removing encryption class is allowed without validation",
			operation:      admissionv1.Update,
			oldObject:      newCryptoTestPVC(t, "enc-class", nil),
			newObject:      newCryptoTestPVC(t, "", nil),
			expectedResult: true,
		},
		{
			name:           "Delete is allowed without validation",
			operation:      admissionv1.Delete,
			oldObject:      newCryptoTestPVC(t, "enc-class", nil),
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCryptoClient := &MockCryptoClient{}
			if tt.encryptedSC != nil {
				mockCryptoClient.On("IsEncryptedStorageClass", mock.Anything, "test-sc").
					Return(*tt.encryptedSC, "", nil)
			}

			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: tt.operation,
					Object:    runtime.RawExtension{Raw: tt.newObject},
					OldObject: runtime.RawExtension{Raw: tt.oldObject},
				},
			}

			resp := validatePVCRequestForCrypto(ctx, mockCryptoClient, req)
			assert.Equal(t, tt.expectedResult, resp.Allowed)
			mockCryptoClient.AssertExpectations(t)
			if tt.encryptedSC == nil {
				mockCryptoClient.AssertNotCalled(t, "IsEncryptedStorageClass", mock.Anything, mock.Anything)
			}
		})
	}
}

// newCryptoTestPVCWithVAC returns a PVC with the given encryption class that references the
// "test-vac" VolumeAttributesClass. If effective is true, the PVC status shows the VAC has
// taken effect; otherwise a ModifyVolume to the VAC is still in progress.
func newCryptoTestPVCWithVAC(t *testing.T, encClassName string, effective bool) []byte {
	vacName := "test-vac"
	pvc := &corev1.PersistentVolumeClaim{}
	assert.NoError(t, json.Unmarshal(newCryptoTestPVC(t, encClassName, nil), pvc))
	pvc.Spec.VolumeAttributesClassName = &vacName
	if effective {
		pvc.Status.CurrentVolumeAttributesClassName = &vacName
	} else {
		pvc.Status.ModifyVolumeStatus = &corev1.ModifyVolumeStatus{
			TargetVolumeAttributesClassName: vacName,
			Status:                          corev1.PersistentVolumeClaimModifyVolumeInProgress,
		}
	}
	raw, err := json.Marshal(pvc)
	assert.NoError(t, err)
	return raw
}

func TestValidatePVCRequestForCryptoWithVAC(t *testing.T) {
	ctx := context.Background()

	origVACPolicyMutabilityEnabled := featureIsVACPolicyMutabilityEnabled
	t.Cleanup(func() {
		featureIsVACPolicyMutabilityEnabled = origVACPolicyMutabilityEnabled
	})

	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vac"},
		DriverName: "csi.vsphere.vmware.com",
		Parameters: map[string]string{"storagePolicyID": "vac-profile-id"},
	}

	tests := []struct {
		name               string
		vacFeatureEnabled  bool
		newObject          []byte
		encryptedSC        *bool // nil means IsEncryptedStorageClass must not be called
		encryptedVACPolicy *bool // nil means IsEncryptedStorageProfile must not be called
		expectedResult     bool
	}{
		{
			name:               "Effective encrypted VAC with non-encrypted storage class is allowed",
			vacFeatureEnabled:  true,
			newObject:          newCryptoTestPVCWithVAC(t, "enc-class-2", true),
			encryptedVACPolicy: ptr.To(true),
			expectedResult:     true,
		},
		{
			name:               "Effective non-encrypted VAC with encrypted storage class is denied",
			vacFeatureEnabled:  true,
			newObject:          newCryptoTestPVCWithVAC(t, "enc-class-2", true),
			encryptedVACPolicy: ptr.To(false),
			expectedResult:     false,
		},
		{
			name:              "VAC not yet effective falls back to non-encrypted storage class and is denied",
			vacFeatureEnabled: true,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", false),
			encryptedSC:       ptr.To(false),
			expectedResult:    false,
		},
		{
			name:              "VAC policy mutability disabled falls back to non-encrypted storage class and is denied",
			vacFeatureEnabled: false,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", true),
			encryptedSC:       ptr.To(false),
			expectedResult:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featureIsVACPolicyMutabilityEnabled = tt.vacFeatureEnabled

			mockCryptoClient := &MockCryptoClient{
				Client: fake.NewClientBuilder().WithObjects(vac).Build(),
			}
			if tt.encryptedSC != nil {
				mockCryptoClient.On("IsEncryptedStorageClass", mock.Anything, "test-sc").
					Return(*tt.encryptedSC, "", nil)
			}
			if tt.encryptedVACPolicy != nil {
				mockCryptoClient.On("IsEncryptedStorageProfile", mock.Anything, "vac-profile-id").
					Return(*tt.encryptedVACPolicy, nil)
			}

			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					Object:    runtime.RawExtension{Raw: tt.newObject},
					OldObject: runtime.RawExtension{Raw: newCryptoTestPVCWithVAC(t, "enc-class-1", true)},
				},
			}

			resp := validatePVCRequestForCrypto(ctx, mockCryptoClient, req)
			assert.Equal(t, tt.expectedResult, resp.Allowed)
			mockCryptoClient.AssertExpectations(t)
			if tt.encryptedSC == nil {
				mockCryptoClient.AssertNotCalled(t, "IsEncryptedStorageClass", mock.Anything, mock.Anything)
			}
			if tt.encryptedVACPolicy == nil {
				mockCryptoClient.AssertNotCalled(t, "IsEncryptedStorageProfile", mock.Anything, mock.Anything)
			}
		})
	}
}
