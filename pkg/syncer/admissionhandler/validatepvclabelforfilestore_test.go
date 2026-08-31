package admissionhandler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

const testFileStoreName = "my-file-store"

// fileStorePVCOption customises the PVC built by fileStorePVCRaw.
type fileStorePVCOption func(*corev1.PersistentVolumeClaim)

// withFileStoreLabel sets the reserved file store label to the given value.
func withFileStoreLabel(value string) fileStorePVCOption {
	return func(pvc *corev1.PersistentVolumeClaim) {
		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}
		pvc.Labels[common.FileStoreLabelKey] = value
	}
}

// withVolumeName pre-binds the PVC to an existing PV, making it statically provisioned.
func withVolumeName(name string) fileStorePVCOption {
	return func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.VolumeName = name }
}

// withoutStorageClass clears the storage class, so the PVC is not dynamically provisioned.
func withoutStorageClass() fileStorePVCOption {
	return func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.StorageClassName = nil }
}

// withUserLabel adds an unrelated user label, to confirm other labels are never considered.
func withUserLabel(key, value string) fileStorePVCOption {
	return func(pvc *corev1.PersistentVolumeClaim) {
		if pvc.Labels == nil {
			pvc.Labels = map[string]string{}
		}
		pvc.Labels[key] = value
	}
}

// fileStorePVCRaw builds a dynamically provisioned PVC (storage class set, no volumeName) with the
// given modifications applied, and returns its JSON encoding for the admission request payload.
func fileStorePVCRaw(t *testing.T, opts ...fileStorePVCOption) []byte {
	t.Helper()
	sc := common.StorageClassVsanFileServicePolicy
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pvc", Namespace: "pvc-ns"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &sc},
	}
	for _, opt := range opts {
		opt(pvc)
	}
	raw, err := json.Marshal(pvc)
	if err != nil {
		t.Fatalf("failed to marshal PVC %v: %v", pvc, err)
	}
	return raw
}

func TestValidatePVCLabelForFileStore(t *testing.T) {
	deniedOnCreate := admission.Denied(fmt.Sprintf(NonCreatablePVCLabel,
		common.FileStoreLabelKey, nonCSIServiceAccountExample))
	deniedOnUpdate := admission.Denied(fmt.Sprintf(NonUpdatablePVCLabel,
		common.FileStoreLabelKey, nonCSIServiceAccountExample))

	tests := []struct {
		name             string
		admissionReview  admission.Request
		expectedResponse admission.Response
	}{
		{
			name: "TestCreateDynamicPVCWithFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
				},
			},
			expectedResponse: deniedOnCreate,
		},
		{
			// Even an empty value is a squat on the reserved key.
			name: "TestCreateDynamicPVCWithEmptyFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: fileStorePVCRaw(t, withFileStoreLabel(""))},
				},
			},
			expectedResponse: deniedOnCreate,
		},
		{
			name: "TestCreateDynamicPVCWithoutFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: fileStorePVCRaw(t, withUserLabel("app", "web"))},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			// A statically provisioned PVC names an existing PV, so its labels are the user's to set.
			name: "TestCreateStaticPVCWithFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName), withVolumeName("pv-1")),
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestCreatePVCWithoutStorageClassAndWithFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName), withoutStorageClass()),
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestCreateDynamicPVCWithFileStoreLabelByCSIServiceAccount",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: csiServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestAddFileStoreLabelOnUpdateByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{Raw: fileStorePVCRaw(t)},
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
				},
			},
			expectedResponse: deniedOnUpdate,
		},
		{
			name: "TestChangeFileStoreLabelOnUpdateByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel("other-file-store")),
					},
				},
			},
			expectedResponse: deniedOnUpdate,
		},
		{
			name: "TestRemoveFileStoreLabelOnUpdateByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
					Object: runtime.RawExtension{Raw: fileStorePVCRaw(t)},
				},
			},
			expectedResponse: deniedOnUpdate,
		},
		{
			// Editing anything else on a PVC that carries the label must still go through.
			name: "TestUpdateOtherLabelsKeepingFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName), withUserLabel("app", "web")),
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			// This is external-provisioner publishing the label after CreateVolume returns.
			name: "TestAddFileStoreLabelOnUpdateByCSIServiceAccount",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: csiServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{Raw: fileStorePVCRaw(t)},
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestDeletePVCWithFileStoreLabelByDevopsUser",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Delete,
					OldObject: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
				},
			},
			expectedResponse: admission.Allowed(""),
		},
		{
			name: "TestCreatePVCWithMalformedObject",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: []byte("not-json")},
				},
			},
			expectedResponse: admission.Allowed(
				"skipped validation when failed to deserialize PVC from new request object"),
		},
		{
			name: "TestUpdatePVCWithMalformedOldObject",
			admissionReview: admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
					Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{Raw: []byte("not-json")},
					Object: runtime.RawExtension{
						Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName)),
					},
				},
			},
			expectedResponse: admission.Allowed(
				"skipped validation when failed to deserialize PVC from old request object"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validatePVCLabelForFileStore(context.Background(), tt.admissionReview)
			if !reflect.DeepEqual(got, tt.expectedResponse) {
				t.Errorf("validatePVCLabelForFileStore() = %v, want %v", got, tt.expectedResponse)
			}
		})
	}
}

// TestCSISupervisorWebhookFileStoreLabelGatedByFSS verifies the file store label validation only runs
// when the vSAN file service capability is enabled, so clusters on the legacy vSAN file service are
// unaffected.
func TestCSISupervisorWebhookFileStoreLabelGatedByFSS(t *testing.T) {
	origVsanFileService := featureIsVsanFileVolumeServiceEnabled
	origSnapshot := featureGateBlockVolumeSnapshotEnabled
	origTKGSHa := featureGateTKGSHaEnabled
	origByok := featureGateByokEnabled
	defer func() {
		featureIsVsanFileVolumeServiceEnabled = origVsanFileService
		featureGateBlockVolumeSnapshotEnabled = origSnapshot
		featureGateTKGSHaEnabled = origTKGSHa
		featureGateByokEnabled = origByok
	}()
	// Keep every other PVC validator out of the way so the result is attributable to this one.
	featureGateBlockVolumeSnapshotEnabled = false
	featureGateTKGSHaEnabled = false
	featureGateByokEnabled = false

	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			UserInfo:  authv1.UserInfo{Username: nonCSIServiceAccountExample},
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: fileStorePVCRaw(t, withFileStoreLabel(testFileStoreName))},
		},
	}
	webhook := &CSISupervisorWebhook{}

	featureIsVsanFileVolumeServiceEnabled = false
	if resp := webhook.Handle(context.Background(), req); !resp.Allowed {
		t.Errorf("with the vSAN file service capability disabled the request must be admitted, got %v", resp)
	}

	featureIsVsanFileVolumeServiceEnabled = true
	resp := webhook.Handle(context.Background(), req)
	if resp.Allowed {
		t.Errorf("with the vSAN file service capability enabled the request must be denied, got %v", resp)
	}
	want := fmt.Sprintf(NonCreatablePVCLabel, common.FileStoreLabelKey, nonCSIServiceAccountExample)
	if resp.Result == nil || resp.Result.Message != want {
		t.Errorf("unexpected denial message: got %v, want %q", resp.Result, want)
	}
}
