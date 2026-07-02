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

package persistentvolumeclaim

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// createTestScheme creates a scheme with all required types for testing
func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = vmoperatortypes.AddToScheme(scheme)
	return scheme
}

// setVACSupportForTest points commonco.ContainerOrchestratorUtility at a fake orchestrator
// with VMPVCStoragePolicyMutability set to the given state, restoring the previous value (nil,
// in these tests) on cleanup so other tests aren't affected.
func setVACSupportForTest(t *testing.T, enabled bool) {
	t.Helper()
	previous := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = previous })

	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(csicommon.Kubernetes)
	require.NoError(t, err)
	if enabled {
		require.NoError(t, fakeCO.EnableFSS(context.Background(), csicommon.VMPVCStoragePolicyMutability))
	}
	commonco.ContainerOrchestratorUtility = fakeCO
}

// fakeCryptoClient is a minimal stand-in for crypto.Client that records the arguments it was
// called with and returns canned results, scoped to what isEncryptedStoragePolicyForPVC needs
// (IsEncryptedStorageClass / IsEncryptedStorageProfile). The remaining crypto.Client methods
// are not exercised by these tests and return zero values.
type fakeCryptoClient struct {
	client.Client

	isEncryptedStorageClassResult  bool
	isEncryptedStorageClassProfile string
	isEncryptedStorageClassErr     error
	storageClassCalledWith         string
	storageClassCallCount          int

	isEncryptedProfileResult bool
	isEncryptedProfileErr    error
	profileCalledWith        string
	profileCallCount         int
}

func (f *fakeCryptoClient) IsEncryptedStorageClass(_ context.Context, name string) (bool, string, error) {
	f.storageClassCalledWith = name
	f.storageClassCallCount++
	return f.isEncryptedStorageClassResult, f.isEncryptedStorageClassProfile, f.isEncryptedStorageClassErr
}

func (f *fakeCryptoClient) IsEncryptedStorageProfile(_ context.Context, profileID string) (bool, error) {
	f.profileCalledWith = profileID
	f.profileCallCount++
	return f.isEncryptedProfileResult, f.isEncryptedProfileErr
}

func (f *fakeCryptoClient) MarkEncryptedStorageClass(context.Context, *storagev1.StorageClass, bool) error {
	return nil
}

func (f *fakeCryptoClient) MarkEncryptedVAC(context.Context, *storagev1.VolumeAttributesClass, bool) error {
	return nil
}

func (f *fakeCryptoClient) GetEncryptionClass(context.Context, string, string) (*byokv1.EncryptionClass, error) {
	return nil, nil
}

func (f *fakeCryptoClient) GetDefaultEncryptionClass(context.Context, string) (*byokv1.EncryptionClass, error) {
	return nil, nil
}

func (f *fakeCryptoClient) GetEncryptionClassForPVC(context.Context, string, string) (*byokv1.EncryptionClass, error) {
	return nil, nil
}

// TestFindVolume tests the findVolume function
func TestFindVolume(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()
	volumeHandle := "test-volume-handle"
	pvName := "pv-test"

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}

	t.Run("Success", func(t *testing.T) {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvName,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       csicommon.VSphereCSIDriverName,
						VolumeHandle: volumeHandle,
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv).Build()
		r := &reconciler{
			Client: fakeClient,
			logger: logger.GetLoggerWithNoContext().Named("test"),
		}

		volID, err := r.findVolume(ctx, pvc)
		assert.NoError(t, err)
		assert.Equal(t, volumeHandle, volID)
	})

	t.Run("PVNotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
		r := &reconciler{
			Client: fakeClient,
			logger: logger.GetLoggerWithNoContext().Named("test"),
		}

		volID, err := r.findVolume(ctx, pvc)
		assert.Error(t, err)
		assert.Empty(t, volID)
	})

	t.Run("NotVSphereCSIVolume", func(t *testing.T) {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvName,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver: "other-driver",
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv).Build()
		r := &reconciler{
			Client: fakeClient,
			logger: logger.GetLoggerWithNoContext().Named("test"),
		}

		volID, err := r.findVolume(ctx, pvc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not a vSphere CSI volume")
		assert.Empty(t, volID)
	})

	t.Run("IsFileVolume", func(t *testing.T) {
		fileVolumeHandle := "file:test-volume-handle"
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: pvName,
			},
			Spec: corev1.PersistentVolumeSpec{
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{
						Driver:       csicommon.VSphereCSIDriverName,
						VolumeHandle: fileVolumeHandle,
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc, pv).Build()
		r := &reconciler{
			Client: fakeClient,
			logger: logger.GetLoggerWithNoContext().Named("test"),
		}

		volID, err := r.findVolume(ctx, pvc)
		assert.NoError(t, err)
		assert.Empty(t, volID)
	})
}

// newReconciler returns a reconciler suitable for isPVCAttachedToVM tests. The function
// only inspects PVC-local fields, so no Kubernetes client is required.
func newReconciler() *reconciler {
	return &reconciler{
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}
}

// TestIsPVCAttachedToVM_NotAttached verifies that a PVC carrying neither the usedby-vm
// annotation nor the CNSPvcFinalizer is reported as not attached (encryption proceeds).
func TestIsPVCAttachedToVM_NotAttached(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				"csi.vsphere.encryption-class": "my-encryption-class",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	isAttached, detail := newReconciler().isPVCAttachedToVM(pvc)

	assert.False(t, isAttached, "PVC should not be attached without usedby-vm annotation or CNS finalizer")
	assert.Empty(t, detail, "detail should be empty when not attached")
}

// TestIsPVCAttachedToVM_Annotation verifies that a PVC carrying the cns.vmware.com/usedby-vm-
// annotation (written by CnsNodeVMBatchAttachment) is reported as attached, and the VM
// instance UUID suffix is surfaced in the detail string for logging.
func TestIsPVCAttachedToVM_Annotation(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				"csi.vsphere.encryption-class":                          "my-encryption-class",
				cnsoperatortypes.UsedByVMAnnotationPrefix + "vm-uuid-1": "",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	isAttached, detail := newReconciler().isPVCAttachedToVM(pvc)

	assert.True(t, isAttached, "PVC should be attached when usedby-vm annotation is present")
	assert.Contains(t, detail, "vm-uuid-1", "detail should contain the VM instance UUID suffix")
}

// TestIsPVCAttachedToVM_Finalizer verifies that a PVC carrying the CNSPvcFinalizer but no
// usedby-vm annotation (e.g. attached via the legacy CnsNodeVMAttachment path) is reported
// as attached.
func TestIsPVCAttachedToVM_Finalizer(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pvc",
			Namespace:  "default",
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	isAttached, detail := newReconciler().isPVCAttachedToVM(pvc)

	assert.True(t, isAttached, "PVC should be attached when CNSPvcFinalizer is present")
	assert.Contains(t, detail, cnsoperatortypes.CNSPvcFinalizer, "detail should reference the finalizer")
}

// TestIsPVCAttachedToVM_AnnotationTakesPrecedence verifies that when both signals are present
// the annotation path is used (and surfaces the VM UUID).
func TestIsPVCAttachedToVM_AnnotationTakesPrecedence(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				cnsoperatortypes.UsedByVMAnnotationPrefix + "vm-uuid-2": "",
			},
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	isAttached, detail := newReconciler().isPVCAttachedToVM(pvc)

	assert.True(t, isAttached, "PVC should be attached")
	assert.Contains(t, detail, "vm-uuid-2", "detail should surface the VM instance UUID from the annotation")
}

// TestIsPVCAttachedToVM_UnrelatedAnnotationAndFinalizer verifies that annotations/finalizers
// that merely resemble but do not match the markers are not treated as attachment.
func TestIsPVCAttachedToVM_UnrelatedAnnotationAndFinalizer(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				"cns.vmware.com/some-other-annotation": "value",
			},
			Finalizers: []string{"kubernetes.io/pvc-protection"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	isAttached, detail := newReconciler().isPVCAttachedToVM(pvc)

	assert.False(t, isAttached, "unrelated annotation/finalizer should not count as VM attachment")
	assert.Empty(t, detail)
}

// TestIsEncryptedStoragePolicyForPVC_NoVAC verifies that a PVC with no VAC set resolves
// encryption purely via the StorageClass path.
func TestIsEncryptedStoragePolicyForPVC_NoVAC(t *testing.T) {
	scName := "plain-sc"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scName},
	}
	fakeCrypto := &fakeCryptoClient{
		isEncryptedStorageClassResult:  true,
		isEncryptedStorageClassProfile: "sc-profile-id",
	}
	r := &reconciler{
		Client:       fake.NewClientBuilder().WithScheme(createTestScheme()).Build(),
		logger:       logger.GetLoggerWithNoContext().Named("test"),
		cryptoClient: fakeCrypto,
	}

	encrypted, profileID, err := r.isEncryptedStoragePolicyForPVC(context.Background(), pvc)

	require.NoError(t, err)
	assert.True(t, encrypted)
	assert.Equal(t, "sc-profile-id", profileID)
	assert.Equal(t, scName, fakeCrypto.storageClassCalledWith)
	assert.Equal(t, 0, fakeCrypto.profileCallCount, "VAC path must not be consulted when no VAC is set")
}

// TestIsEncryptedStoragePolicyForPVC_VACModifyInProgress verifies the ModifyVolumeStatus
// guardrail: a VAC that has not yet taken effect must not be trusted, and the SC path is
// used instead — this is the regression case the review comment flagged.
func TestIsEncryptedStoragePolicyForPVC_VACModifyInProgress(t *testing.T) {
	setVACSupportForTest(t, true)
	scName := "plain-sc"
	vacName := "encrypted-vac"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName:          &scName,
			VolumeAttributesClassName: &vacName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			ModifyVolumeStatus: &corev1.ModifyVolumeStatus{
				TargetVolumeAttributesClassName: vacName,
				Status:                          corev1.PersistentVolumeClaimModifyVolumeInProgress,
			},
		},
	}
	fakeCrypto := &fakeCryptoClient{isEncryptedStorageClassResult: false}
	r := &reconciler{
		Client:       fake.NewClientBuilder().WithScheme(createTestScheme()).Build(),
		logger:       logger.GetLoggerWithNoContext().Named("test"),
		cryptoClient: fakeCrypto,
	}

	encrypted, _, err := r.isEncryptedStoragePolicyForPVC(context.Background(), pvc)

	require.NoError(t, err)
	assert.False(t, encrypted)
	assert.Equal(t, scName, fakeCrypto.storageClassCalledWith, "must fall back to the SC while modify is in flight")
	assert.Equal(t, 0, fakeCrypto.profileCallCount, "VAC path must not be consulted while modify is in progress")
}

// TestIsEncryptedStoragePolicyForPVC_VACEffective verifies that once the VAC has taken effect
// (ModifyVolumeStatus cleared and CurrentVolumeAttributesClassName caught up), the VAC's
// policy is resolved and takes precedence over the StorageClass's.
func TestIsEncryptedStoragePolicyForPVC_VACEffective(t *testing.T) {
	setVACSupportForTest(t, true)
	scName := "plain-sc"
	vacName := "encrypted-vac"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName:          &scName,
			VolumeAttributesClassName: &vacName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			CurrentVolumeAttributesClassName: &vacName,
		},
	}
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: vacName},
		Parameters: map[string]string{"storagePolicyID": "vac-profile-id"},
	}
	fakeCrypto := &fakeCryptoClient{
		isEncryptedStorageClassResult: false, // if this were consulted, the test should fail
		isEncryptedProfileResult:      true,
	}
	r := &reconciler{
		Client:       fake.NewClientBuilder().WithScheme(createTestScheme()).WithObjects(vac).Build(),
		logger:       logger.GetLoggerWithNoContext().Named("test"),
		cryptoClient: fakeCrypto,
	}

	encrypted, profileID, err := r.isEncryptedStoragePolicyForPVC(context.Background(), pvc)

	require.NoError(t, err)
	assert.True(t, encrypted)
	assert.Equal(t, "vac-profile-id", profileID)
	assert.Equal(t, "vac-profile-id", fakeCrypto.profileCalledWith)
	assert.Equal(t, 0, fakeCrypto.storageClassCallCount, "SC path must not be consulted once the VAC is effective")
}

// TestIsEncryptedStoragePolicyForPVC_VACEffectiveButMissing verifies that a VAC referenced by
// an effective PVC but no longer present in the cluster (e.g. deleted concurrently) is treated
// as not encrypted, without erroring.
func TestIsEncryptedStoragePolicyForPVC_VACEffectiveButMissing(t *testing.T) {
	setVACSupportForTest(t, true)
	scName := "plain-sc"
	vacName := "deleted-vac"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName:          &scName,
			VolumeAttributesClassName: &vacName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			CurrentVolumeAttributesClassName: &vacName,
		},
	}
	fakeCrypto := &fakeCryptoClient{}
	r := &reconciler{
		Client:       fake.NewClientBuilder().WithScheme(createTestScheme()).Build(),
		logger:       logger.GetLoggerWithNoContext().Named("test"),
		cryptoClient: fakeCrypto,
	}

	encrypted, profileID, err := r.isEncryptedStoragePolicyForPVC(context.Background(), pvc)

	require.NoError(t, err)
	assert.False(t, encrypted)
	assert.Empty(t, profileID)
	assert.Equal(t, 0, fakeCrypto.profileCallCount)
}

// TestIsEncryptedStoragePolicyForPVC_VACEffectiveNoPolicyID verifies a VAC with no
// storagePolicyID parameter is treated as not encrypted without calling the crypto client.
func TestIsEncryptedStoragePolicyForPVC_VACEffectiveNoPolicyID(t *testing.T) {
	setVACSupportForTest(t, true)
	scName := "plain-sc"
	vacName := "no-policy-vac"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName:          &scName,
			VolumeAttributesClassName: &vacName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			CurrentVolumeAttributesClassName: &vacName,
		},
	}
	vac := &storagev1.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{Name: vacName},
	}
	fakeCrypto := &fakeCryptoClient{}
	r := &reconciler{
		Client:       fake.NewClientBuilder().WithScheme(createTestScheme()).WithObjects(vac).Build(),
		logger:       logger.GetLoggerWithNoContext().Named("test"),
		cryptoClient: fakeCrypto,
	}

	encrypted, profileID, err := r.isEncryptedStoragePolicyForPVC(context.Background(), pvc)

	require.NoError(t, err)
	assert.False(t, encrypted)
	assert.Empty(t, profileID)
	assert.Equal(t, 0, fakeCrypto.profileCallCount)
}

// TestIsEncryptedStoragePolicyForPVC_VACSupportDisabled verifies that when the
// VMPVCStoragePolicyMutability capability is disabled, the pre-existing StorageClass-only
// behavior is preserved even for a PVC whose VAC would otherwise be effective — VAC support
// must not be assumed to always be enabled.
func TestIsEncryptedStoragePolicyForPVC_VACSupportDisabled(t *testing.T) {
	setVACSupportForTest(t, false)
	scName := "plain-sc"
	vacName := "encrypted-vac"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName:          &scName,
			VolumeAttributesClassName: &vacName,
		},
		Status: corev1.PersistentVolumeClaimStatus{
			CurrentVolumeAttributesClassName: &vacName,
		},
	}
	fakeCrypto := &fakeCryptoClient{
		isEncryptedStorageClassResult:  true,
		isEncryptedStorageClassProfile: "sc-profile-id",
	}
	r := &reconciler{
		Client:       fake.NewClientBuilder().WithScheme(createTestScheme()).Build(),
		logger:       logger.GetLoggerWithNoContext().Named("test"),
		cryptoClient: fakeCrypto,
	}

	encrypted, profileID, err := r.isEncryptedStoragePolicyForPVC(context.Background(), pvc)

	require.NoError(t, err)
	assert.True(t, encrypted)
	assert.Equal(t, "sc-profile-id", profileID)
	assert.Equal(t, scName, fakeCrypto.storageClassCalledWith,
		"must use the SC even though the VAC would otherwise be effective, since VAC support is disabled")
	assert.Equal(t, 0, fakeCrypto.profileCallCount, "VAC path must not be consulted when VAC support is disabled")
}
