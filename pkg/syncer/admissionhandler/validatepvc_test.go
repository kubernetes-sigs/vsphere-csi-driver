package admissionhandler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/stretchr/testify/assert"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	snapshotclientfake "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

var (
	testStorageClassName        = "test-sc"
	testNamespace               = "test"
	testFirstPVCName            = "test-vanilla-block-pvc-1"
	testSecondPVCName           = "test-vanilla-block-pvc-2"
	testVolumeSnapshotName      = "test-volume-snapshot"
	testVolumeSnapshotClassName = "test-volume-snapshot-class"
	testPVName                  = "test-pv"
	volumeMode                  = corev1.PersistentVolumeFilesystem
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
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
			VolumeMode: &volumeMode,
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
			Resources: corev1.VolumeResourceRequirements{
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
				&corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: testNamespace,
						// No DeletionTimestamp - normal namespace
					},
				},
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
				&corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: testNamespace,
						// No DeletionTimestamp - normal namespace
					},
				},
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
		{
			name: "TestDeletePVCwithSnapshotShouldPassWhenNamespaceIsBeingDeleted",
			kubeObjs: []runtime.Object{
				testPV,
				&corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:              testNamespace,
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
				},
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
				Result: &metav1.Status{
					Reason: "Namespace is being deleted",
				},
			},
		},
		{
			name: "TestDeletePVCwithSnapshotShouldPassWhenNamespaceIsAlreadyDeleted",
			kubeObjs: []runtime.Object{
				testPV,
				// No namespace object - simulating already deleted namespace
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
				Result: &metav1.Status{
					Reason: "Namespace is being deleted",
				},
			},
		},
		{
			name: "TestDeletePVCwithVolumeSnapshotContentSourceShouldPassWhenNamespaceIsBeingDeleted",
			kubeObjs: []runtime.Object{
				testPV,
				&corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:              testNamespace,
						DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					},
				},
			},
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							// Using VolumeSnapshotContentName instead of PersistentVolumeClaimName
							// This represents the DSM use case
							VolumeSnapshotContentName: func() *string {
								name := "test-volume-snapshot-content"
								return &name
							}(),
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
				Result: &metav1.Status{
					Reason: "Namespace is being deleted",
				},
			},
		},
		{
			name: "TestDeletePVCwithVolumeSnapshotContentSourceShouldPassWhenNamespaceIsNormal",
			kubeObjs: []runtime.Object{
				testPV,
				&corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: testNamespace,
						// No DeletionTimestamp - normal namespace
					},
				},
			},
			snapshotObjs: []runtime.Object{
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: testNamespace,
						Name:      testVolumeSnapshotName,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							// Using VolumeSnapshotContentName instead of PersistentVolumeClaimName
							// This represents the DSM use case
							VolumeSnapshotContentName: func() *string {
								name := "test-volume-snapshot-content"
								return &name
							}(),
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
			snapshotClient := snapshotclientfake.NewClientset(test.snapshotObjs...)
			kubeClient := fake.NewClientset(test.kubeObjs...)

			origK8sClient := newK8sClient
			origSnapshotterClient := newSnapshotterClient
			defer func() {
				newK8sClient = origK8sClient
				newSnapshotterClient = origSnapshotterClient
			}()
			newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
				return kubeClient, nil
			}
			newSnapshotterClient = func(ctx context.Context) (snapshotterClientSet.Interface, error) {
				return snapshotClient, nil
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			actualResponse := validatePVC(ctx, test.admissionReview.Request)
			assert.Equal(t, test.expectedResponse, actualResponse)
		})
	}
}

func TestValidateGuestPVCOperation_LinkedClone_StorageClass(t *testing.T) {
	// Store the original feature gate value and restore it after the test
	originalFeatureGate := featureIsLinkedCloneSupportEnabled
	defer func() {
		featureIsLinkedCloneSupportEnabled = originalFeatureGate
	}()
	featureIsLinkedCloneSupportEnabled = true

	const (
		testLinkedCloneNamespace     = "test-ns"
		testLinkedClonePVCName       = "test-lc-pvc"
		testSourcePVCName            = "test-source-pvc"
		testSnapshotName             = "test-snapshot"
		testStorageClassA            = "storage-class-a"
		testStorageClassB            = "storage-class-b"
		testSvStorageClass1          = "wcpglobal-storage-profile"
		testSvStorageClass2          = "wcpglobal-storage-profile-2"
		testLinkedCloneSnapshotClass = "test-snapshot-class"
	)

	stringPtr := func(s string) *string {
		return &s
	}

	boolPtr := func(b bool) *bool {
		return &b
	}

	tests := []struct {
		name                   string
		kubeObjs               []runtime.Object
		snapshotObjs           []runtime.Object
		pvc                    *corev1.PersistentVolumeClaim
		expectedAllowed        bool
		expectedMessageContain string
	}{
		{
			name: "LinkedClone with matching svStorageClass should succeed",
			kubeObjs: []runtime.Object{
				// Source PVC StorageClass with svStorageClass1
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassA,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass1,
					},
				},
				// Source PVC
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSourcePVCName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr(testStorageClassA),
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("5Gi"),
							},
						},
					},
				},
			},
			snapshotObjs: []runtime.Object{
				// VolumeSnapshot
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSnapshotName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: stringPtr(testSourcePVCName),
						},
						VolumeSnapshotClassName: stringPtr(testLinkedCloneSnapshotClass),
					},
					Status: &snapshotv1.VolumeSnapshotStatus{
						ReadyToUse: boolPtr(true),
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testLinkedClonePVCName,
					Namespace: testLinkedCloneNamespace,
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr(testStorageClassA), // Same storage class
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
					DataSource: &corev1.TypedLocalObjectReference{
						APIGroup: stringPtr("snapshot.storage.k8s.io"),
						Kind:     "VolumeSnapshot",
						Name:     testSnapshotName,
					},
				},
			},
			expectedAllowed:        true,
			expectedMessageContain: "",
		},
		{
			name: "LinkedClone with different StorageClass but same svStorageClass should succeed",
			kubeObjs: []runtime.Object{
				// Source PVC StorageClass with svStorageClass1
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassA,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass1,
					},
				},
				// LinkedClone PVC StorageClass with same svStorageClass1 but different name
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassB,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass1, // Same supervisor storage class
					},
				},
				// Source PVC
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSourcePVCName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr(testStorageClassA),
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("5Gi"),
							},
						},
					},
				},
			},
			snapshotObjs: []runtime.Object{
				// VolumeSnapshot
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSnapshotName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: stringPtr(testSourcePVCName),
						},
						VolumeSnapshotClassName: stringPtr(testLinkedCloneSnapshotClass),
					},
					Status: &snapshotv1.VolumeSnapshotStatus{
						ReadyToUse: boolPtr(true),
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testLinkedClonePVCName,
					Namespace: testLinkedCloneNamespace,
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr(testStorageClassB), // Different storage class
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
					DataSource: &corev1.TypedLocalObjectReference{
						APIGroup: stringPtr("snapshot.storage.k8s.io"),
						Kind:     "VolumeSnapshot",
						Name:     testSnapshotName,
					},
				},
			},
			expectedAllowed:        true,
			expectedMessageContain: "",
		},
		{
			name: "LinkedClone with mismatched svStorageClass should fail",
			kubeObjs: []runtime.Object{
				// Source PVC StorageClass with svStorageClass1
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassA,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass1,
					},
				},
				// LinkedClone PVC StorageClass with different svStorageClass2
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassB,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass2, // Different supervisor storage class
					},
				},
				// Source PVC
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSourcePVCName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr(testStorageClassA),
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("5Gi"),
							},
						},
					},
				},
			},
			snapshotObjs: []runtime.Object{
				// VolumeSnapshot
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSnapshotName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: stringPtr(testSourcePVCName),
						},
						VolumeSnapshotClassName: stringPtr(testLinkedCloneSnapshotClass),
					},
					Status: &snapshotv1.VolumeSnapshotStatus{
						ReadyToUse: boolPtr(true),
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testLinkedClonePVCName,
					Namespace: testLinkedCloneNamespace,
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr(testStorageClassB), // Different storage class with different policy
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
					DataSource: &corev1.TypedLocalObjectReference{
						APIGroup: stringPtr("snapshot.storage.k8s.io"),
						Kind:     "VolumeSnapshot",
						Name:     testSnapshotName,
					},
				},
			},
			expectedAllowed:        false,
			expectedMessageContain: "svStorageClass mismatch",
		},
		{
			name: "LinkedClone with missing svStorageClass in source StorageClass should fail",
			kubeObjs: []runtime.Object{
				// Source PVC StorageClass without svStorageClass
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassA,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters:  map[string]string{
						// Missing common.AttributeSupervisorStorageClass
					},
				},
				// LinkedClone PVC StorageClass with svStorageClass
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassB,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass1,
					},
				},
				// Source PVC
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSourcePVCName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr(testStorageClassA),
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("5Gi"),
							},
						},
					},
				},
			},
			snapshotObjs: []runtime.Object{
				// VolumeSnapshot
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSnapshotName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: stringPtr(testSourcePVCName),
						},
						VolumeSnapshotClassName: stringPtr(testLinkedCloneSnapshotClass),
					},
					Status: &snapshotv1.VolumeSnapshotStatus{
						ReadyToUse: boolPtr(true),
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testLinkedClonePVCName,
					Namespace: testLinkedCloneNamespace,
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr(testStorageClassB),
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
					DataSource: &corev1.TypedLocalObjectReference{
						APIGroup: stringPtr("snapshot.storage.k8s.io"),
						Kind:     "VolumeSnapshot",
						Name:     testSnapshotName,
					},
				},
			},
			expectedAllowed:        false,
			expectedMessageContain: "does not have svstorageclass parameter",
		},
		{
			name: "LinkedClone with missing svStorageClass in LinkedClone StorageClass should fail",
			kubeObjs: []runtime.Object{
				// Source PVC StorageClass with svStorageClass
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassA,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters: map[string]string{
						common.AttributeSupervisorStorageClass: testSvStorageClass1,
					},
				},
				// LinkedClone PVC StorageClass without svStorageClass
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: testStorageClassB,
					},
					Provisioner: "csi.vsphere.vmware.com",
					Parameters:  map[string]string{
						// Missing common.AttributeSupervisorStorageClass
					},
				},
				// Source PVC
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSourcePVCName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: stringPtr(testStorageClassA),
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("5Gi"),
							},
						},
					},
				},
			},
			snapshotObjs: []runtime.Object{
				// VolumeSnapshot
				&snapshotv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSnapshotName,
						Namespace: testLinkedCloneNamespace,
					},
					Spec: snapshotv1.VolumeSnapshotSpec{
						Source: snapshotv1.VolumeSnapshotSource{
							PersistentVolumeClaimName: stringPtr(testSourcePVCName),
						},
						VolumeSnapshotClassName: stringPtr(testLinkedCloneSnapshotClass),
					},
					Status: &snapshotv1.VolumeSnapshotStatus{
						ReadyToUse: boolPtr(true),
					},
				},
			},
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testLinkedClonePVCName,
					Namespace: testLinkedCloneNamespace,
					Annotations: map[string]string{
						common.AnnKeyLinkedClone: "true",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr(testStorageClassB),
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
					DataSource: &corev1.TypedLocalObjectReference{
						APIGroup: stringPtr("snapshot.storage.k8s.io"),
						Kind:     "VolumeSnapshot",
						Name:     testSnapshotName,
					},
				},
			},
			expectedAllowed:        false,
			expectedMessageContain: "does not have svstorageclass parameter",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create fake clients
			kubeClient := fake.NewClientset(test.kubeObjs...)
			snapshotClient := snapshotclientfake.NewClientset(test.snapshotObjs...)

			origK8sClient := newK8sClient
			origSnapshotterClient := newSnapshotterClient
			defer func() {
				newK8sClient = origK8sClient
				newSnapshotterClient = origSnapshotterClient
			}()
			newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
				return kubeClient, nil
			}
			newSnapshotterClient = func(ctx context.Context) (snapshotterClientSet.Interface, error) {
				return snapshotClient, nil
			}

			// Marshal the PVC to raw JSON
			pvcBytes, err := json.Marshal(test.pvc)
			assert.NoError(t, err)

			// Create admission request
			admissionReq := &admissionv1.AdmissionRequest{
				Kind: metav1.GroupVersionKind{
					Kind: "PersistentVolumeClaim",
				},
				Operation: admissionv1.Create,
				Namespace: testLinkedCloneNamespace,
				Name:      testLinkedClonePVCName,
				Object: runtime.RawExtension{
					Raw: pvcBytes,
				},
			}

			// Call the validation function
			ctx := context.Background()
			response := validateGuestPVCOperation(ctx, admissionReq)

			// Verify the response
			assert.Equal(t, test.expectedAllowed, response.Allowed,
				"Expected allowed=%v but got allowed=%v. Message: %v",
				test.expectedAllowed, response.Allowed, response.Result)

			if !test.expectedAllowed && test.expectedMessageContain != "" {
				assert.Contains(t, response.Result.Message, test.expectedMessageContain,
					"Expected error message to contain '%s' but got: %s",
					test.expectedMessageContain, response.Result.Message)
			}
		})
	}
}

func TestValidatePVC_VACChange(t *testing.T) {
	oldVACName := "old-vac"
	newVACName := "new-vac"

	makeVACUpdateReq := func(oldVAC, newVAC *string) *admissionv1.AdmissionRequest {
		oldPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testFirstPVCName},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName:          &testStorageClassName,
				VolumeAttributesClassName: oldVAC,
				AccessModes:               []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeMode:                &volumeMode,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
				},
			},
		}
		newPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testFirstPVCName},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName:          &testStorageClassName,
				VolumeAttributesClassName: newVAC,
				AccessModes:               []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeMode:                &volumeMode,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
				},
			},
		}
		oldRaw, err := json.Marshal(oldPVC)
		assert.NoError(t, err)
		newRaw, err := json.Marshal(newPVC)
		assert.NoError(t, err)
		return &admissionv1.AdmissionRequest{
			Kind:      metav1.GroupVersionKind{Kind: "PersistentVolumeClaim"},
			Operation: admissionv1.Update,
			OldObject: runtime.RawExtension{Raw: oldRaw},
			Object:    runtime.RawExtension{Raw: newRaw},
		}
	}

	tests := []struct {
		name string
		// blockVolumeSnapshotEnabled is set explicitly per case (rather than left to whatever
		// other tests in this package leave the global at) so validatePVC's top-level gate -
		// "featureGateBlockVolumeSnapshotEnabled || featureIsVACPolicyMutabilityEnabled" - is
		// satisfied deterministically even for cases where vacPolicyMutabilityEnabled is false.
		blockVolumeSnapshotEnabled    bool
		vacPolicyMutabilityEnabled    bool
		isVolumeAttributesClassServed func(ctx context.Context) (bool, error)
		req                           *admissionv1.AdmissionRequest
		expectedAllowed               bool
		expectedReason                metav1.StatusReason
	}{
		{
			name:                       "no VAC change is unaffected regardless of feature state",
			blockVolumeSnapshotEnabled: true,
			vacPolicyMutabilityEnabled: false,
			req:                        makeVACUpdateReq(&oldVACName, &oldVACName),
			expectedAllowed:            true,
		},
		{
			name:                       "VAC change denied when VACPolicyMutability feature is disabled",
			blockVolumeSnapshotEnabled: true,
			vacPolicyMutabilityEnabled: false,
			req:                        makeVACUpdateReq(&oldVACName, &newVACName),
			expectedAllowed:            false,
			expectedReason:             VACChangeFeatureDisabledErrorMessage,
		},
		{
			name:                       "VAC change denied when VolumeAttributesClass API is not served",
			vacPolicyMutabilityEnabled: true,
			isVolumeAttributesClassServed: func(ctx context.Context) (bool, error) {
				return false, nil
			},
			req:             makeVACUpdateReq(&oldVACName, &newVACName),
			expectedAllowed: false,
			expectedReason:  VACChangeAPINotServedErrorMessage,
		},
		{
			name:                       "VAC change denied when API availability check errors",
			vacPolicyMutabilityEnabled: true,
			isVolumeAttributesClassServed: func(ctx context.Context) (bool, error) {
				return false, errors.New("discovery unavailable")
			},
			req:             makeVACUpdateReq(&oldVACName, &newVACName),
			expectedAllowed: false,
		},
		{
			name:                       "VAC change allowed when feature enabled and API served",
			vacPolicyMutabilityEnabled: true,
			isVolumeAttributesClassServed: func(ctx context.Context) (bool, error) {
				return true, nil
			},
			req:             makeVACUpdateReq(&oldVACName, &newVACName),
			expectedAllowed: true,
		},
	}

	origBlockVolumeSnapshotEnabled := featureGateBlockVolumeSnapshotEnabled
	origVACPolicyMutabilityEnabled := featureIsVACPolicyMutabilityEnabled
	origIsVolumeAttributesClassServed := isVolumeAttributesClassServed
	defer func() {
		featureGateBlockVolumeSnapshotEnabled = origBlockVolumeSnapshotEnabled
		featureIsVACPolicyMutabilityEnabled = origVACPolicyMutabilityEnabled
		isVolumeAttributesClassServed = origIsVolumeAttributesClassServed
	}()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featureGateBlockVolumeSnapshotEnabled = test.blockVolumeSnapshotEnabled
			featureIsVACPolicyMutabilityEnabled = test.vacPolicyMutabilityEnabled
			if test.isVolumeAttributesClassServed != nil {
				isVolumeAttributesClassServed = test.isVolumeAttributesClassServed
			} else {
				isVolumeAttributesClassServed = origIsVolumeAttributesClassServed
			}

			resp := validatePVC(context.Background(), test.req)
			assert.Equal(t, test.expectedAllowed, resp.Allowed)
			if test.expectedReason != "" {
				assert.Equal(t, test.expectedReason, resp.Result.Reason)
			}
		})
	}
}

func TestDetectVACChange(t *testing.T) {
	vacA := "vac-a"
	vacB := "vac-b"

	tests := []struct {
		name            string
		oldVAC          *string
		newVAC          *string
		expectedChanged bool
		expectedOldVAC  string
		expectedNewVAC  string
	}{
		{name: "both nil", oldVAC: nil, newVAC: nil, expectedChanged: false},
		{name: "unset to set", oldVAC: nil, newVAC: &vacA, expectedChanged: true, expectedNewVAC: vacA},
		{name: "set to unset", oldVAC: &vacA, newVAC: nil, expectedChanged: true, expectedOldVAC: vacA},
		{name: "same value", oldVAC: &vacA, newVAC: &vacA, expectedChanged: false,
			expectedOldVAC: vacA, expectedNewVAC: vacA},
		{name: "different values", oldVAC: &vacA, newVAC: &vacB, expectedChanged: true,
			expectedOldVAC: vacA, expectedNewVAC: vacB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldPVC := corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{VolumeAttributesClassName: test.oldVAC},
			}
			newPVC := corev1.PersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{VolumeAttributesClassName: test.newVAC},
			}
			changed, oldVAC, newVAC := detectVACChange(oldPVC, newPVC)
			assert.Equal(t, test.expectedChanged, changed)
			assert.Equal(t, test.expectedOldVAC, oldVAC)
			assert.Equal(t, test.expectedNewVAC, newVAC)
		})
	}
}

func TestIsVolumeAttributesClassServed(t *testing.T) {
	tests := []struct {
		name           string
		resources      []*metav1.APIResourceList
		clientErr      error
		expectedServed bool
		expectErr      bool
	}{
		{
			name: "GA v1 serves VolumeAttributesClass",
			resources: []*metav1.APIResourceList{
				{GroupVersion: "storage.k8s.io/v1", APIResources: []metav1.APIResource{
					{Name: common.VolumeAttributesClassResourceName},
				}},
			},
			expectedServed: true,
		},
		{
			name: "beta v1beta1 only is not sufficient (GA required)",
			resources: []*metav1.APIResourceList{
				{GroupVersion: "storage.k8s.io/v1beta1", APIResources: []metav1.APIResource{
					{Name: common.VolumeAttributesClassResourceName},
				}},
			},
			expectedServed: false,
		},
		{
			name: "GA served without the volumeattributesclasses resource",
			resources: []*metav1.APIResourceList{
				{GroupVersion: "storage.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "storageclasses"}}},
			},
			expectedServed: false,
		},
		{
			name:           "GA not served",
			resources:      []*metav1.APIResourceList{},
			expectedServed: false,
		},
		{
			name:      "client construction error propagates",
			clientErr: errors.New("failed to build client"),
			expectErr: true,
		},
	}

	origK8sClient := newK8sClient
	defer func() { newK8sClient = origK8sClient }()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.clientErr != nil {
				newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
					return nil, test.clientErr
				}
			} else {
				kubeClient := fake.NewClientset()
				kubeClient.Fake.Resources = test.resources
				newK8sClient = func(ctx context.Context) (clientset.Interface, error) {
					return kubeClient, nil
				}
			}

			served, err := isVolumeAttributesClassServed(context.Background())
			if test.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, test.expectedServed, served)
		})
	}
}
