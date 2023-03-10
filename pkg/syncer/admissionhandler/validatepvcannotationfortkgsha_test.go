package admissionhandler

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	pvcWithoutAnnotation = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testFirstPVCName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &testStorageClassName,
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
			VolumeName: testPVName,
		},
	}

	pvcAnnotationAdmissionTestInstance *pvcAnnotationAdmissionTest
	onceForPVCAnnotationAdmissionTest  sync.Once
)

type pvcAnnotationAdmissionTest struct {
	pvcWithoutAnnotationRaw                                 []byte
	pvcWithAnnGuestClusterRequestedTopologyButEmptyValueRaw []byte
	pvcWithAnnGuestClusterRequestedTopologyRaw              []byte
	pvcWithAnotherAnnGuestClusterRequestedTopologyRaw       []byte
	pvcWithAnnVolumeAccessibleTopologyButEmptyValueRaw      []byte
	pvcWithAnnVolumeAccessibleTopologyRaw                   []byte
	pvcWithAnotherAnnVolumeAccessibleTopologyRaw            []byte
}

func getPVCAnnotationAdmissionTest(t *testing.T) *pvcAnnotationAdmissionTest {
	onceForPVCAnnotationAdmissionTest.Do(func() {
		pvcWithoutAnnotationRaw, err := json.Marshal(pvcWithoutAnnotation)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithoutAnnotation, %v: %v", pvcWithoutAnnotation, err)
		}

		// AnnGuestClusterRequestedTopology, case 1: pvc with annotation key and empty value
		pvcWithAnnGuestClusterRequestedTopologyButEmptyValue := pvcWithoutAnnotation.DeepCopy()
		pvcWithAnnGuestClusterRequestedTopologyButEmptyValue.Annotations = make(map[string]string)
		pvcWithAnnGuestClusterRequestedTopologyButEmptyValue.Annotations[common.AnnGuestClusterRequestedTopology] = ""

		pvcWithAnnGuestClusterRequestedTopologyButEmptyValueRaw, err :=
			json.Marshal(pvcWithAnnGuestClusterRequestedTopologyButEmptyValue)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAnnGuestClusterRequestedTopologyButEmptyValue, %v: %v",
				pvcWithAnnGuestClusterRequestedTopologyButEmptyValue, err)
		}

		// AnnGuestClusterRequestedTopology, case 2: pvc with annotation key and nonempty value
		pvcWithAnnGuestClusterRequestedTopology := pvcWithoutAnnotation.DeepCopy()
		pvcWithAnnGuestClusterRequestedTopology.Annotations = make(map[string]string)
		pvcWithAnnGuestClusterRequestedTopology.Annotations[common.AnnGuestClusterRequestedTopology] =
			`[{"topology.kubernetes.io/zone":"zone1"},{"topology.kubernetes.io/zone":"zone2"}]`

		pvcWithAnnGuestClusterRequestedTopologyRaw, err := json.Marshal(pvcWithAnnGuestClusterRequestedTopology)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAnnGuestClusterRequestedTopology, %v: %v",
				pvcWithAnnGuestClusterRequestedTopology, err)
		}

		// AnnGuestClusterRequestedTopology, case 3: pvc with annotation key and another nonempty value
		pvcWithAnotherAnnGuestClusterRequestedTopology := pvcWithoutAnnotation.DeepCopy()
		pvcWithAnotherAnnGuestClusterRequestedTopology.Annotations = make(map[string]string)
		pvcWithAnotherAnnGuestClusterRequestedTopology.Annotations[common.AnnGuestClusterRequestedTopology] =
			`[{"topology.kubernetes.io/zone":"zone3"}]`

		pvcWithAnotherAnnGuestClusterRequestedTopologyRaw, err :=
			json.Marshal(pvcWithAnotherAnnGuestClusterRequestedTopology)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAnotherAnnGuestClusterRequestedTopology, %v: %v",
				pvcWithAnotherAnnGuestClusterRequestedTopology, err)
		}

		// AnnVolumeAccessibleTopology, case 1: pvc with AnnVolumeAccessibleTopology key and empty value
		pvcWithAnnVolumeAccessibleTopologyButEmptyValue := pvcWithoutAnnotation.DeepCopy()
		pvcWithAnnVolumeAccessibleTopologyButEmptyValue.Annotations = make(map[string]string)
		pvcWithAnnVolumeAccessibleTopologyButEmptyValue.Annotations[common.AnnVolumeAccessibleTopology] = ""

		pvcWithAnnVolumeAccessibleTopologyButEmptyValueRaw, err :=
			json.Marshal(pvcWithAnnVolumeAccessibleTopologyButEmptyValue)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAnnVolumeAccessibleTopologyButEmptyValue, %v: %v",
				pvcWithAnnVolumeAccessibleTopologyButEmptyValue, err)
		}

		// AnnVolumeAccessibleTopology, case 2: pvc with AnnVolumeAccessibleTopology key and nonempty value
		pvcWithAnnVolumeAccessibleTopology := pvcWithoutAnnotation.DeepCopy()
		pvcWithAnnVolumeAccessibleTopology.Annotations = make(map[string]string)
		pvcWithAnnVolumeAccessibleTopology.Annotations[common.AnnVolumeAccessibleTopology] =
			`[{"topology.kubernetes.io/zone":"zone1"}]`

		pvcWithAnnVolumeAccessibleTopologyRaw, err := json.Marshal(pvcWithAnnVolumeAccessibleTopology)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAnnVolumeAccessibleTopology, %v: %v",
				pvcWithAnnVolumeAccessibleTopology, err)
		}

		// AnnVolumeAccessibleTopology, case 3: pvc with AnnVolumeAccessibleTopology key and another nonempty value
		pvcWithAnotherAnnVolumeAccessibleTopology := pvcWithoutAnnotation.DeepCopy()
		pvcWithAnotherAnnVolumeAccessibleTopology.Annotations = make(map[string]string)
		pvcWithAnotherAnnVolumeAccessibleTopology.Annotations[common.AnnVolumeAccessibleTopology] =
			`[{"topology.kubernetes.io/zone":"zone2"}]`

		pvcWithAnotherAnnVolumeAccessibleTopologyRaw, err := json.Marshal(pvcWithAnotherAnnVolumeAccessibleTopology)
		if err != nil {
			t.Fatalf("Failed to marshall the pvcWithAnotherAnnVolumeAccessibleTopology, %v: %v",
				pvcWithAnotherAnnVolumeAccessibleTopology, err)
		}

		pvcAnnotationAdmissionTestInstance = &pvcAnnotationAdmissionTest{
			pvcWithoutAnnotationRaw:                                 pvcWithoutAnnotationRaw,
			pvcWithAnnGuestClusterRequestedTopologyButEmptyValueRaw: pvcWithAnnGuestClusterRequestedTopologyButEmptyValueRaw,
			pvcWithAnnGuestClusterRequestedTopologyRaw:              pvcWithAnnGuestClusterRequestedTopologyRaw,
			pvcWithAnotherAnnGuestClusterRequestedTopologyRaw:       pvcWithAnotherAnnGuestClusterRequestedTopologyRaw,
			pvcWithAnnVolumeAccessibleTopologyButEmptyValueRaw:      pvcWithAnnVolumeAccessibleTopologyButEmptyValueRaw,
			pvcWithAnnVolumeAccessibleTopologyRaw:                   pvcWithAnnVolumeAccessibleTopologyRaw,
			pvcWithAnotherAnnVolumeAccessibleTopologyRaw:            pvcWithAnotherAnnVolumeAccessibleTopologyRaw,
		}
	})
	return pvcAnnotationAdmissionTestInstance
}

func TestValidatePVCAnnotationForTKGSHA(t *testing.T) {
	testValidatePVCAnnotationInstance := getPVCAnnotationAdmissionTest(t)

	tests := []struct {
		name             string
		admissionReview  admission.Request
		expectedResponse admission.Response
	}{
		{
			name: "TestCreatePVCwithAnnGuestClusterRequestedTopologyAndNonEmptyValue",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnGuestClusterRequestedTopologyRaw,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestCreatePVCwithAnnGuestClusterRequestedTopologyButEmptyValue",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnGuestClusterRequestedTopologyButEmptyValueRaw,
					},
				},
			},
			expectedResponse: admission.Denied(CreatePVCWithInvalidAnnotation),
		},
		{
			name: "TestUpdatePVCToAddAnnGuestClusterRequestedTopology",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithoutAnnotationRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnGuestClusterRequestedTopologyRaw,
					},
				},
			},
			expectedResponse: admission.Denied(AddPVCAnnotation + ", " + common.AnnGuestClusterRequestedTopology),
		},
		{
			name: "TestUpdatePVCToRemoveAnnGuestClusterRequestedTopology",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnGuestClusterRequestedTopologyRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithoutAnnotationRaw,
					},
				},
			},
			expectedResponse: admission.Denied(RemovePVCAnnotation + ", " + common.AnnGuestClusterRequestedTopology),
		},
		{
			name: "TestUpdatePVCToUpdateAnnGuestClusterRequestedTopology",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnGuestClusterRequestedTopologyRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnotherAnnGuestClusterRequestedTopologyRaw,
					},
				},
			},
			expectedResponse: admission.Denied(UpdatePVCAnnotation + ", " + common.AnnGuestClusterRequestedTopology),
		},
		{
			name: "TestUpdatePVCToAddAnnVolumeAccessibleTopology",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithoutAnnotationRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyRaw,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestUpdatePVCToRemoveAnnVolumeAccessibleTopology",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithoutAnnotationRaw,
					},
				},
			},
			expectedResponse: admission.Denied(RemovePVCAnnotation + ", " + common.AnnVolumeAccessibleTopology),
		},
		{
			name: "TestUpdatePVCToUpdateAnnVolumeAccessibleTopologyToAnotherValue",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnotherAnnVolumeAccessibleTopologyRaw,
					},
				},
			},
			expectedResponse: admission.Denied(UpdatePVCAnnotation + ", " + common.AnnVolumeAccessibleTopology),
		},
		{
			name: "TestUpdatePVCToUpdateAnnVolumeAccessibleTopologyFromEmtpyToNonempty",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyButEmptyValueRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyRaw,
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestUpdatePVCToUpdateAnnVolumeAccessibleTopologyFromNonemtpyToEmpty",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyRaw,
					},
					Object: runtime.RawExtension{
						Raw: testValidatePVCAnnotationInstance.pvcWithAnnVolumeAccessibleTopologyButEmptyValueRaw,
					},
				},
			},
			expectedResponse: admission.Denied(UpdatePVCAnnotation + ", " + common.AnnVolumeAccessibleTopology),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validatePVCAnnotationForTKGSHA(context.TODO(), tt.admissionReview)
			if !reflect.DeepEqual(got, tt.expectedResponse) {
				t.Errorf("validatePVCAnnotationForTKGSHA() = %v, want %v", got, tt.expectedResponse)
			}
		})
	}
}
