package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

const (
	nonCSIServiceAccountExample = "nonCSIServiceAccount"
	csiServiceAccountExample    = "system:serviceaccount:vmware-system-csi:vsphere-csi-controller"
)

var (
	pvcHealthAnnotationAdmissionTestInstance *pvcHealthAnnotationAdmissionTest
	onceForPVCHealthAnnotationAdmissionTest  sync.Once
)

type pvcHealthAnnotationAdmissionTest struct {
	pvcWithNonExistentAnnVolumeHealth  []byte
	pvcWithEmptyAnnVolumeHealthValue   []byte
	pvcWithAccessibleAnnVolumeHealth   []byte
	pvcWithInaccessibleAnnVolumeHealth []byte
	pvcWithRandomAnnVolumeHealth       []byte
}

func TestValidatePVCAnnotationForVolumeHealth(t *testing.T) {

	testValidatePVCHealthAnnotationInstance := getPVCHealthAnnotationTest(t)

	tests := []struct {
		name             string
		admissionReview  admission.Request
		expectedResponse admission.Response
	}{
		{
			name: "TestCreatePVCWithNonEmptyAnnVolumeHealthByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Denied(fmt.Sprintf(NonCreatablePVCAnnotation,
				common.AnnVolumeHealth, nonCSIServiceAccountExample)),
		},
		{
			name: "TestCreatePVCWithAbsentAnnVolumeHealthByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithNonExistentAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestCreatePVCWithNonEmptyAnnVolumeHealthByCSIServiceAccount",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: csiServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestUpdatePVCAnnVolumeHealthToInaccessibleByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithInaccessibleAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Denied(fmt.Sprintf(NonUpdatablePVCAnnotation,
				common.AnnVolumeHealth, nonCSIServiceAccountExample)),
		},
		{
			name: "TestUpdatePVCAnnVolumeHealthToRandomByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithRandomAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Denied(fmt.Sprintf(NonUpdatablePVCAnnotation,
				common.AnnVolumeHealth, nonCSIServiceAccountExample)),
		},
		{
			name: "TestUpdatePVCAnnVolumeHealthToInaccessibleByCSIServiceAccount",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: csiServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithInaccessibleAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestUpdatePVCAnnVolumeHealthToAccessibleByCSIServiceAccount",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: csiServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithInaccessibleAnnVolumeHealth,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestUpdatePVCRemoveAnnVolumeHealthByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithNonExistentAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Denied(fmt.Sprintf(NonUpdatablePVCAnnotation,
				common.AnnVolumeHealth, nonCSIServiceAccountExample)),
		},
		{
			name: "TestUpdatePVCRemoveAnnVolumeHealthByCSIServiceAccount",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo: v1.UserInfo{Username: csiServiceAccountExample},
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithAccessibleAnnVolumeHealth,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCHealthAnnotationInstance.pvcWithNonExistentAnnVolumeHealth,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validatePVCAnnotationForVolumeHealth(context.TODO(), tt.admissionReview)
			if !reflect.DeepEqual(got, tt.expectedResponse) {
				t.Errorf("validatePVCAnnotationForVolumeHealth() = %v, want %v", got, tt.expectedResponse)
			}
		})
	}
}

func getPVCHealthAnnotationTest(t *testing.T) *pvcHealthAnnotationAdmissionTest {

	onceForPVCHealthAnnotationAdmissionTest.Do(func() {
		// cases for pvc volume health annotation validation

		pvcWithNonExistentAnnVolumeHealth := pvcWithoutAnnotation.DeepCopy()
		pvcWithNonExistentAnnVolumeHealthRaw, err := json.Marshal(pvcWithNonExistentAnnVolumeHealth)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithNonExistentAnnVolumeHealth, %v: %v",
				pvcWithNonExistentAnnVolumeHealth, err)
		}

		pvcWithEmptyAnnVolumeHealthValue := pvcWithoutAnnotation.DeepCopy()
		pvcWithEmptyAnnVolumeHealthValue.Annotations = make(map[string]string)
		pvcWithEmptyAnnVolumeHealthValue.Annotations[common.AnnVolumeHealth] = ""

		pvcWithEmptyAnnVolumeHealthValueRaw, err := json.Marshal(pvcWithEmptyAnnVolumeHealthValue)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithEmptyAnnVolumeHealthValue, %v: %v",
				pvcWithEmptyAnnVolumeHealthValue, err)
		}

		pvcWithAccessibleAnnVolumeHealth := pvcWithoutAnnotation.DeepCopy()
		pvcWithAccessibleAnnVolumeHealth.Annotations = make(map[string]string)
		pvcWithAccessibleAnnVolumeHealth.Annotations[common.AnnVolumeHealth] = common.VolHealthStatusAccessible

		pvcWithAccessibleAnnVolumeHealthRaw, err := json.Marshal(pvcWithAccessibleAnnVolumeHealth)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAccessibleAnnVolumeHealth, %v: %v",
				pvcWithAccessibleAnnVolumeHealth, err)
		}

		pvcWithInaccessibleAnnVolumeHealth := pvcWithoutAnnotation.DeepCopy()
		pvcWithInaccessibleAnnVolumeHealth.Annotations = make(map[string]string)
		pvcWithInaccessibleAnnVolumeHealth.Annotations[common.AnnVolumeHealth] = common.VolHealthStatusInaccessible

		pvcWithInaccessibleAnnVolumeHealthRaw, err := json.Marshal(pvcWithInaccessibleAnnVolumeHealth)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithInaccessibleAnnVolumeHealth, %v: %v",
				pvcWithInaccessibleAnnVolumeHealth, err)
		}

		pvcWithRandomAnnVolumeHealth := pvcWithoutAnnotation.DeepCopy()
		pvcWithRandomAnnVolumeHealth.Annotations = make(map[string]string)
		pvcWithRandomAnnVolumeHealth.Annotations[common.AnnVolumeHealth] = "RandomValue"

		pvcWithRandomAnnVolumeHealthRaw, err := json.Marshal(pvcWithRandomAnnVolumeHealth)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithRandomAnnVolumeHealth, %v: %v",
				pvcWithRandomAnnVolumeHealth, err)
		}

		pvcHealthAnnotationAdmissionTestInstance = &pvcHealthAnnotationAdmissionTest{
			pvcWithNonExistentAnnVolumeHealth:  pvcWithNonExistentAnnVolumeHealthRaw,
			pvcWithEmptyAnnVolumeHealthValue:   pvcWithEmptyAnnVolumeHealthValueRaw,
			pvcWithAccessibleAnnVolumeHealth:   pvcWithAccessibleAnnVolumeHealthRaw,
			pvcWithInaccessibleAnnVolumeHealth: pvcWithInaccessibleAnnVolumeHealthRaw,
			pvcWithRandomAnnVolumeHealth:       pvcWithRandomAnnVolumeHealthRaw,
		}
	})
	return pvcHealthAnnotationAdmissionTestInstance

}
