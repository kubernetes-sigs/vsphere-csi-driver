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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
// named VolumeAttributesClass. If effective is true, the PVC status shows the VAC has taken
// effect; otherwise a ModifyVolume to the VAC is still in progress.
func newCryptoTestPVCWithVAC(t *testing.T, encClassName, vacName string, effective bool) []byte {
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

func newCryptoTestVAC(name, profileID string) *storagev1.VolumeAttributesClass {
	return &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		DriverName: "csi.vsphere.vmware.com",
		Parameters: map[string]string{"storagePolicyID": profileID},
	}
}

// newCryptoTestMockClient returns a MockCryptoClient backed by a fake client serving the given
// VolumeAttributesClasses. If vacAPINotServed is true, reading any VolumeAttributesClass fails
// with a NoKindMatch error, as on clusters that don't serve the VolumeAttributesClass v1 API.
func newCryptoTestMockClient(vacAPINotServed bool, vacs ...client.Object) *MockCryptoClient {
	builder := fake.NewClientBuilder().WithObjects(vacs...)
	if vacAPINotServed {
		builder = builder.WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*storagev1.VolumeAttributesClass); ok {
					return &meta.NoKindMatchError{
						GroupKind:        schema.GroupKind{Group: "storage.k8s.io", Kind: "VolumeAttributesClass"},
						SearchedVersions: []string{"v1"},
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}
	return &MockCryptoClient{Client: builder.Build()}
}

func TestValidatePVCRequestForCryptoWithVAC(t *testing.T) {
	ctx := context.Background()

	origVACPolicyMutabilityEnabled := featureIsVACPolicyMutabilityEnabled
	t.Cleanup(func() {
		featureIsVACPolicyMutabilityEnabled = origVACPolicyMutabilityEnabled
	})

	tests := []struct {
		name               string
		vacFeatureEnabled  bool
		vacMissing         bool
		vacAPINotServed    bool
		newObject          []byte
		encryptedSC        *bool // nil means IsEncryptedStorageClass must not be called
		encryptedVACPolicy *bool // nil means IsEncryptedStorageProfile must not be called
		expectedResult     bool
		expectedReason     string // substring of the denial reason
	}{
		{
			name:               "Effective encrypted VAC with non-encrypted storage class is allowed",
			vacFeatureEnabled:  true,
			newObject:          newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", true),
			encryptedVACPolicy: ptr.To(true),
			expectedResult:     true,
		},
		{
			name:               "Effective non-encrypted VAC with encrypted storage class is denied",
			vacFeatureEnabled:  true,
			newObject:          newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", true),
			encryptedVACPolicy: ptr.To(false),
			expectedResult:     false,
			expectedReason:     `effective spec.volumeAttributesClassName "test-vac"`,
		},
		{
			name:              "Missing effective VAC is denied as not found",
			vacFeatureEnabled: true,
			vacMissing:        true,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", true),
			expectedResult:    false,
			expectedReason:    `spec.volumeAttributesClassName: Not found: "test-vac"`,
		},
		{
			name:              "VAC API not served falls back to non-encrypted storage class and is denied",
			vacFeatureEnabled: true,
			vacAPINotServed:   true,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", true),
			encryptedSC:       ptr.To(false),
			expectedResult:    false,
			expectedReason:    "requires spec.storageClassName specify an encryption storage class",
		},
		{
			name:              "VAC API not served falls back to encrypted storage class and is allowed",
			vacFeatureEnabled: true,
			vacAPINotServed:   true,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", true),
			encryptedSC:       ptr.To(true),
			expectedResult:    true,
		},
		{
			name:              "VAC not yet effective falls back to non-encrypted storage class and is denied",
			vacFeatureEnabled: true,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", false),
			encryptedSC:       ptr.To(false),
			expectedResult:    false,
			expectedReason:    "requires spec.storageClassName specify an encryption storage class",
		},
		{
			name:              "VAC policy mutability disabled falls back to non-encrypted storage class and is denied",
			vacFeatureEnabled: false,
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class-2", "test-vac", true),
			encryptedSC:       ptr.To(false),
			expectedResult:    false,
			expectedReason:    "requires spec.storageClassName specify an encryption storage class",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featureIsVACPolicyMutabilityEnabled = tt.vacFeatureEnabled

			var vacs []client.Object
			if !tt.vacMissing {
				vacs = append(vacs, newCryptoTestVAC("test-vac", "vac-profile-id"))
			}
			mockCryptoClient := newCryptoTestMockClient(tt.vacAPINotServed, vacs...)
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
					OldObject: runtime.RawExtension{Raw: newCryptoTestPVCWithVAC(t, "enc-class-1", "test-vac", true)},
				},
			}

			resp := validatePVCRequestForCrypto(ctx, mockCryptoClient, req)
			assert.Equal(t, tt.expectedResult, resp.Allowed)
			if tt.expectedReason != "" {
				assert.Contains(t, string(resp.Result.Reason), tt.expectedReason)
			}
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

func TestValidatePVCRequestForCryptoVACChange(t *testing.T) {
	ctx := context.Background()

	origVACPolicyMutabilityEnabled := featureIsVACPolicyMutabilityEnabled
	t.Cleanup(func() {
		featureIsVACPolicyMutabilityEnabled = origVACPolicyMutabilityEnabled
	})

	tests := []struct {
		name                string
		vacFeatureEnabled   bool
		vacAPINotServed     bool
		oldObject           []byte
		newObject           []byte
		encryptedSC         *bool // nil means IsEncryptedStorageClass must not be called
		encryptedTargetVAC  *bool // nil means IsEncryptedStorageProfile must not be called
		expectedResult      bool
		expectedReasonMatch string // substring of the denial reason
	}{
		{
			name:                "Annotated PVC switching to non-encrypted VAC is denied",
			vacFeatureEnabled:   true,
			oldObject:           newCryptoTestPVC(t, "enc-class", nil),
			newObject:           newCryptoTestPVCWithVAC(t, "enc-class", "target-vac", false),
			encryptedTargetVAC:  ptr.To(false),
			expectedResult:      false,
			expectedReasonMatch: `spec.volumeAttributesClassName: Invalid value: "target-vac"`,
		},
		{
			name:                "Annotated PVC switching between VACs to non-encrypted VAC is denied",
			vacFeatureEnabled:   true,
			oldObject:           newCryptoTestPVCWithVAC(t, "enc-class", "test-vac", true),
			newObject:           newCryptoTestPVCWithVAC(t, "enc-class", "target-vac", true),
			encryptedTargetVAC:  ptr.To(false),
			expectedResult:      false,
			expectedReasonMatch: `spec.volumeAttributesClassName: Invalid value: "target-vac"`,
		},
		{
			name:               "Annotated PVC switching to encrypted VAC is allowed",
			vacFeatureEnabled:  true,
			oldObject:          newCryptoTestPVC(t, "enc-class", nil),
			newObject:          newCryptoTestPVCWithVAC(t, "enc-class", "target-vac", false),
			encryptedTargetVAC: ptr.To(true),
			expectedResult:     true,
		},
		{
			name:              "Annotated PVC switching to a VAC that doesn't exist yet is allowed",
			vacFeatureEnabled: true,
			oldObject:         newCryptoTestPVC(t, "enc-class", nil),
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class", "missing-vac", false),
			expectedResult:    true,
		},
		{
			name:              "Annotated PVC switching VAC when VAC API not served is allowed",
			vacFeatureEnabled: true,
			vacAPINotServed:   true,
			oldObject:         newCryptoTestPVC(t, "enc-class", nil),
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class", "target-vac", false),
			expectedResult:    true,
		},
		{
			name:              "Unannotated PVC switching to non-encrypted VAC is allowed",
			vacFeatureEnabled: true,
			oldObject:         newCryptoTestPVC(t, "", nil),
			newObject:         newCryptoTestPVCWithVAC(t, "", "target-vac", false),
			expectedResult:    true,
		},
		{
			name:              "VAC change is not validated when VAC policy mutability is disabled",
			vacFeatureEnabled: false,
			oldObject:         newCryptoTestPVC(t, "enc-class", nil),
			newObject:         newCryptoTestPVCWithVAC(t, "enc-class", "target-vac", false),
			expectedResult:    true,
		},
		{
			name:                "Adding annotation and switching to non-encrypted VAC is denied",
			vacFeatureEnabled:   true,
			oldObject:           newCryptoTestPVC(t, "", nil),
			newObject:           newCryptoTestPVCWithVAC(t, "enc-class", "target-vac", false),
			encryptedSC:         ptr.To(true),
			encryptedTargetVAC:  ptr.To(false),
			expectedResult:      false,
			expectedReasonMatch: `spec.volumeAttributesClassName: Invalid value: "target-vac"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featureIsVACPolicyMutabilityEnabled = tt.vacFeatureEnabled

			mockCryptoClient := newCryptoTestMockClient(tt.vacAPINotServed,
				newCryptoTestVAC("test-vac", "vac-profile-id"),
				newCryptoTestVAC("target-vac", "target-profile-id"))
			if tt.encryptedSC != nil {
				mockCryptoClient.On("IsEncryptedStorageClass", mock.Anything, "test-sc").
					Return(*tt.encryptedSC, "", nil)
			}
			if tt.encryptedTargetVAC != nil {
				mockCryptoClient.On("IsEncryptedStorageProfile", mock.Anything, "target-profile-id").
					Return(*tt.encryptedTargetVAC, nil)
			}

			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					Object:    runtime.RawExtension{Raw: tt.newObject},
					OldObject: runtime.RawExtension{Raw: tt.oldObject},
				},
			}

			resp := validatePVCRequestForCrypto(ctx, mockCryptoClient, req)
			assert.Equal(t, tt.expectedResult, resp.Allowed)
			if tt.expectedReasonMatch != "" {
				assert.Contains(t, string(resp.Result.Reason), tt.expectedReasonMatch)
			}
			mockCryptoClient.AssertExpectations(t)
			if tt.encryptedSC == nil {
				mockCryptoClient.AssertNotCalled(t, "IsEncryptedStorageClass", mock.Anything, mock.Anything)
			}
			if tt.encryptedTargetVAC == nil {
				mockCryptoClient.AssertNotCalled(t, "IsEncryptedStorageProfile", mock.Anything, mock.Anything)
			}
		})
	}
}
