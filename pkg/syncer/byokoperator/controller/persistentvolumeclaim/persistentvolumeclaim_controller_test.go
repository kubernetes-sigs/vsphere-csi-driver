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
	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	cnsoperator "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsbatchv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	commonco "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// Test helper to create a scheme with required types
func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = byokv1.AddToScheme(scheme)
	// Register CnsNodeVmAttachment and CnsNodeVMBatchAttachment
	scheme.AddKnownTypes(cnsoperator.SchemeGroupVersion,
		&cnsv1alpha1.CnsNodeVmAttachment{},
		&cnsv1alpha1.CnsNodeVmAttachmentList{},
		&cnsbatchv1alpha1.CnsNodeVMBatchAttachment{},
		&cnsbatchv1alpha1.CnsNodeVMBatchAttachmentList{},
	)
	metav1.AddToGroupVersion(scheme, cnsoperator.SchemeGroupVersion)
	return scheme
}

func TestIsPVCAttachedToVM_NotAttached(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Initialize fake CO interface
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmUUID, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.False(t, isAttached, "PVC should not be attached")
	assert.Empty(t, vmUUID, "VM UUID should be empty when not attached")
}

func TestIsPVCAttachedToVM_AttachedViaCnsNodeVmAttachment(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Initialize fake CO interface
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	attachment := &cnsv1alpha1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-attachment",
			Namespace: "default",
		},
		Spec: cnsv1alpha1.CnsNodeVmAttachmentSpec{
			VolumeName: "pv-test",
			NodeUUID:   "node-uuid-123",
		},
		Status: cnsv1alpha1.CnsNodeVmAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, attachment).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmUUID, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be attached")
	assert.Equal(t, "node-uuid-123", vmUUID, "VM UUID should match NodeUUID from spec")
}

func TestIsPVCAttachedToVM_AttachedViaCnsNodeVmAttachment_NotAttachedStatus(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Initialize fake CO interface
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	attachment := &cnsv1alpha1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-attachment",
			Namespace: "default",
		},
		Spec: cnsv1alpha1.CnsNodeVmAttachmentSpec{
			VolumeName: "pv-test",
			NodeUUID:   "node-uuid",
		},
		Status: cnsv1alpha1.CnsNodeVmAttachmentStatus{
			Attached: false, // Not attached
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, attachment).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmUUID, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.False(t, isAttached, "PVC should not be attached when status.attached is false")
	assert.Empty(t, vmUUID, "VM UUID should be empty when not attached")
}

func TestIsPVCAttachedToVM_AttachedViaBatchAttachment(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Initialize fake CO interface and enable SharedDiskFss
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO
	// Enable SharedDiskFss feature for batch attachment testing
	err = fakeCO.EnableFSS(ctx, "supports_shared_disks_with_VM_service_VMs")
	if err != nil {
		t.Fatal(err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	controllerKey := int32(1000)
	unitNumber := int32(0)

	batchAttachment := &cnsbatchv1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-batch-attachment",
			Namespace: "default",
		},
		Spec: cnsbatchv1alpha1.CnsNodeVMBatchAttachmentSpec{
			InstanceUUID: "vm-uuid",
			Volumes: []cnsbatchv1alpha1.VolumeSpec{
				{
					Name: "volume-1",
					PersistentVolumeClaim: cnsbatchv1alpha1.PersistentVolumeClaimSpec{
						ClaimName:     "test-pvc",
						DiskMode:      cnsbatchv1alpha1.Persistent,
						SharingMode:   cnsbatchv1alpha1.SharingMultiWriter,
						ControllerKey: &controllerKey,
						UnitNumber:    &unitNumber,
					},
				},
			},
		},
		Status: cnsbatchv1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []cnsbatchv1alpha1.VolumeStatus{
				{
					Name: "volume-1",
					PersistentVolumeClaim: cnsbatchv1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "test-pvc",
						Attached:  true,
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, batchAttachment).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	// SharedDiskFss is enabled, so batch attachments should be checked
	isAttached, vmUUID, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be attached via batch attachment when SharedDiskFss is enabled")
	assert.Equal(t, "vm-uuid", vmUUID, "VM UUID should match InstanceUUID from batch attachment spec")
}

func TestIsPVCAttachedToVM_MultipleAttachments(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Initialize fake CO interface
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// Create multiple attachments, only one with attached=true
	attachment1 := &cnsv1alpha1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-attachment-1",
			Namespace: "default",
		},
		Spec: cnsv1alpha1.CnsNodeVmAttachmentSpec{
			VolumeName: "pv-other",
			NodeUUID:   "node-uuid-1",
		},
		Status: cnsv1alpha1.CnsNodeVmAttachmentStatus{
			Attached: true,
		},
	}

	attachment2 := &cnsv1alpha1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-attachment-2",
			Namespace: "default",
		},
		Spec: cnsv1alpha1.CnsNodeVmAttachmentSpec{
			VolumeName: "pv-test",
			NodeUUID:   "node-uuid-2",
		},
		Status: cnsv1alpha1.CnsNodeVmAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, attachment1, attachment2).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmUUID, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be attached when matching attachment exists")
	assert.Equal(t, "node-uuid-2", vmUUID, "VM UUID should match the NodeUUID of the attachment with matching volume")
}
