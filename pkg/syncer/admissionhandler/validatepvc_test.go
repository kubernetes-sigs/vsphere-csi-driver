package admissionhandler

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/agiledragon/gomonkey/v2"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	snapshotclientfake "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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
			kubeClient := fake.NewSimpleClientset(test.kubeObjs...)
			snapshotClient := snapshotclientfake.NewSimpleClientset(test.snapshotObjs...)

			// Patch k8s client functions
			patches := gomonkey.ApplyFunc(
				k8s.NewClient, func(ctx context.Context) (clientset.Interface, error) {
					return kubeClient, nil
				})
			defer patches.Reset()

			patches = gomonkey.ApplyFunc(
				k8s.NewSnapshotterClient, func(ctx context.Context) (snapshotterClientSet.Interface, error) {
					return snapshotClient, nil
				})
			defer patches.Reset()

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
