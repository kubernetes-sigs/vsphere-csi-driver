package admissionhandler

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/agiledragon/gomonkey/v2"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotclientfake "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var (
	testStorageClassName        = "test-sc"
	testNamespace               = "test"
	testFirstPVCName            = "test-vanilla-block-pvc-1"
	testSecondPVCName           = "test-vanilla-block-pvc-2"
	testVolumeSnapshotName      = "test-volume-snapshot"
	testVolumeSnapshotClassName = "test-volume-snapshot-class"
	testPVName                  = "test-pv"
	testPV                      = &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: testPVName,
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		},
	}
	oldPVC = &corev1.PersistentVolumeClaim{
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
	newPVC = &corev1.PersistentVolumeClaim{
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
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
			VolumeName: testPVName,
		},
	}
	pvcAdmissionTestInstance *pvcAdmissionTest
	onceForPVCAdmissionTest  sync.Once
)

type pvcAdmissionTest struct {
	oldPVCRaw []byte
	newPVCRaw []byte
}

func getPVCAdmissionTest(t *testing.T) *pvcAdmissionTest {
	onceForPVCAdmissionTest.Do(func() {
		oldPVCRaw, err := json.Marshal(oldPVC)
		if err != nil {
			t.Fatalf("Failed to marshall the old PVC, %v: %v", oldPVC, err)
		}

		newPVCRaw, err := json.Marshal(newPVC)
		if err != nil {
			t.Fatalf("Failed to marshall the new PVC, %v: %v", newPVC, err)
		}

		pvcAdmissionTestInstance = &pvcAdmissionTest{
			oldPVCRaw: oldPVCRaw,
			newPVCRaw: newPVCRaw,
		}
	})
	return pvcAdmissionTestInstance
}

func TestValidatePVC(t *testing.T) {
	testInstance := getPVCAdmissionTest(t)
	featureGateBlockVolumeSnapshotEnabled = true
	tests := []struct {
		name             string
		kubeObjs         []runtime.Object
		snapshotObjs     []runtime.Object
		admissionReview  *admissionv1.AdmissionReview
		expectedResponse *admissionv1.AdmissionResponse
	}{
		{
			name: "TestDeletePVCwithSnapshotShouldFail",
			kubeObjs: []runtime.Object{
				testPV,
			},
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testFirstPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Delete,
					OldObject: runtime.RawExtension{
						Raw: testInstance.oldPVCRaw,
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Reason: DeleteVolumeWithSnapshotErrorMessage,
				},
			},
		},
		{
			name: "TestDeletePVCwithoutSnapshotShouldPass",
			kubeObjs: []runtime.Object{
				testPV,
			},
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testSecondPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Delete,
					OldObject: runtime.RawExtension{
						Raw: testInstance.oldPVCRaw,
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
		{
			name: "TestExpandPVCwithSnapshotShouldFail",
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testFirstPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testInstance.oldPVCRaw,
					},
					Object: runtime.RawExtension{
						Raw: testInstance.newPVCRaw,
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Reason: ExpandVolumeWithSnapshotErrorMessage,
				},
			},
		},
		{
			name: "TestExpandPVCwithoutSnapshotShouldPass",
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testSecondPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: testInstance.oldPVCRaw,
					},
					Object: runtime.RawExtension{
						Raw: testInstance.newPVCRaw,
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
		{
			name: "TestCreatePVCShouldPass",
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testSecondPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Create,
					Object: runtime.RawExtension{
						Raw: testInstance.newPVCRaw,
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
		{
			name: "TestDeleteNonRwoPVCwithSnapshotShouldPass",
			kubeObjs: []runtime.Object{
				testPV,
			},
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testFirstPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Delete,
					OldObject: runtime.RawExtension{
						Raw: func() []byte {
							pvc := oldPVC.DeepCopy()
							pvc.Spec.AccessModes[0] = corev1.ReadWriteMany
							pvcRaw, _ := json.Marshal(pvc)
							return pvcRaw
						}(),
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
		{
			name: "TestExpandNonRwoPVCwithSnapshotShouldPass",
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testFirstPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Update,
					OldObject: runtime.RawExtension{
						Raw: func() []byte {
							pvc := oldPVC.DeepCopy()
							pvc.Spec.AccessModes[0] = corev1.ReadWriteMany
							pvcRaw, _ := json.Marshal(pvc)
							return pvcRaw
						}(),
					},
					Object: runtime.RawExtension{
						Raw: func() []byte {
							pvc := newPVC.DeepCopy()
							pvc.Spec.AccessModes[0] = corev1.ReadWriteMany
							pvcRaw, _ := json.Marshal(pvc)
							return pvcRaw
						}(),
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
		{
			name: "TestDeletePVCwithSnapshotwithRetainPolicyShouldPass",
			kubeObjs: []runtime.Object{
				func() *corev1.PersistentVolume {
					pv := testPV.DeepCopy()
					pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
					return pv
				}(),
			},
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: &testFirstPVCName,
						},
						VolumeSnapshotClassName: &testVolumeSnapshotClassName,
					},
				},
			},
			admissionReview: &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					Kind: metav1.GroupVersionKind{
						Kind: "PersistentVolumeClaim",
					},
					Operation: admissionv1.Delete,
					OldObject: runtime.RawExtension{
						Raw: testInstance.oldPVCRaw,
					},
				},
			},
			expectedResponse: &admissionv1.AdmissionResponse{
				Allowed: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshotClient := snapshotclientfake.NewSimpleClientset(test.snapshotObjs...)
			kubeClient := fake.NewSimpleClientset(test.kubeObjs...)

			var patches *gomonkey.Patches
			patches = gomonkey.ApplyFunc(
				k8s.NewSnapshotterClient, func(ctx context.Context) (snapshotterClientSet.Interface, error) {
					return snapshotClient, nil
				})
			defer patches.Reset()

			patches = gomonkey.ApplyFunc(
				k8s.NewClient, func(ctx context.Context) (clientset.Interface, error) {
					return kubeClient, nil
				})
			defer patches.Reset()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			actualResponse := validatePVC(ctx, test.admissionReview)
			assert.Equal(t, actualResponse, test.expectedResponse)
		})
	}
}
