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
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	csicommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// createTestScheme creates a scheme with all required types for testing
func createTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = vmoperatortypes.AddToScheme(scheme)
	return scheme
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

// TestIsPVCAttachedToVM_NotAttached tests the case where a PVC is not referenced in any VM
func TestIsPVCAttachedToVM_NotAttached(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// Create a VM that doesn't reference this PVC
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "other-volume",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "other-pvc",
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.False(t, isAttached, "PVC should not be attached")
	assert.Empty(t, vmName, "VM name should be empty when not attached")
}

// TestIsPVCAttachedToVM_Attached tests the case where a PVC is referenced in a VM
func TestIsPVCAttachedToVM_Attached(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// Create a VM that references this PVC
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "my-disk",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be attached")
	assert.Equal(t, "test-vm", vmName, "VM name should match")
}

// TestIsPVCAttachedToVM_NoVMs tests the case where no VMs exist in the namespace
func TestIsPVCAttachedToVM_NoVMs(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

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

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.False(t, isAttached, "PVC should not be attached when no VMs exist")
	assert.Empty(t, vmName, "VM name should be empty")
}

// TestIsPVCAttachedToVM_MultipleVMs tests the case where multiple VMs exist and one references the PVC
func TestIsPVCAttachedToVM_MultipleVMs(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// VM 1 - doesn't reference the PVC
	vm1 := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm-1",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "other-volume",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "other-pvc",
							},
						},
					},
				},
			},
		},
	}

	// VM 2 - references the PVC
	vm2 := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm-2",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "my-disk",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		},
	}

	// VM 3 - doesn't reference the PVC
	vm3 := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vm-3",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm1, vm2, vm3).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be attached")
	assert.Equal(t, "vm-2", vmName, "VM name should match the VM that references the PVC")
}

// TestIsPVCAttachedToVM_VMWithMultipleVolumes tests a VM with multiple volumes including the target PVC
func TestIsPVCAttachedToVM_VMWithMultipleVolumes(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// VM with multiple volumes
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "disk-1",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "pvc-1",
							},
						},
					},
				},
				{
					Name: "disk-2",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
				{
					Name: "disk-3",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "pvc-3",
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be attached")
	assert.Equal(t, "test-vm", vmName, "VM name should match")
}

// TestIsPVCAttachedToVM_DifferentNamespace tests that VMs in different namespaces are not considered
func TestIsPVCAttachedToVM_DifferentNamespace(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "namespace-1",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// VM in a different namespace that references a PVC with the same name
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "namespace-2",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "my-disk",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.False(t, isAttached, "PVC should not be attached (VM is in different namespace)")
	assert.Empty(t, vmName, "VM name should be empty")
}

// TestIsPVCAttachedToVM_VMWithNoPVCVolumes tests a VM that has no PVC volumes
func TestIsPVCAttachedToVM_VMWithNoPVCVolumes(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test",
		},
	}

	// VM with a non-PVC volume (e.g., ConfigMap)
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "config-volume",
					// PersistentVolumeClaim is nil - could be a ConfigMap or other volume type
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	assert.NoError(t, err)
	assert.False(t, isAttached, "PVC should not be attached")
	assert.Empty(t, vmName, "VM name should be empty")
}

// TestIsPVCAttachedToVM_PVCAnnotationChanged_AttachedToVM tests the scenario where
// a PVC's csi.vsphere.encryption-class annotation is changed while the PVC is attached to a VM.
// The isPVCAttachedToVM function should detect the attachment and return true.
func TestIsPVCAttachedToVM_PVCAnnotationChanged_AttachedToVM(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Create PVC with NEW encryption class annotation (simulating annotation change from old-class to new-class)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				"csi.vsphere.encryption-class": "new-encryption-class", // Changed annotation
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test-123",
		},
	}

	// Create VM that references this PVC
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "my-disk",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	// Execute - this simulates the reconciliation triggered by PVC annotation change
	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	// Verify - should detect attachment and return true
	assert.NoError(t, err)
	assert.True(t, isAttached, "PVC should be detected as attached when annotation changes while attached to VM")
	assert.Equal(t, "test-vm", vmName, "VM name should be returned")
}

// TestIsPVCAttachedToVM_EncryptionClassUpdated_PVCAttachedToVM tests the scenario where
// an EncryptionClass is updated (e.g., KeyID or KeyProvider changed) and a PVC referencing
// it is attached to a VM. The isPVCAttachedToVM function should detect the attachment.
func TestIsPVCAttachedToVM_EncryptionClassUpdated_PVCAttachedToVM(t *testing.T) {
	ctx := context.Background()
	scheme := createTestScheme()

	// Create PVC that references an EncryptionClass (which will be updated externally)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "default",
			Annotations: map[string]string{
				"csi.vsphere.encryption-class": "my-encryption-class",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pv-test-456",
		},
	}

	// Create VM that references this PVC
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prod-vm",
			Namespace: "default",
		},
		Spec: vmoperatortypes.VirtualMachineSpec{
			Volumes: []vmoperatortypes.VirtualMachineVolume{
				{
					Name: "data-disk",
					VirtualMachineVolumeSource: vmoperatortypes.VirtualMachineVolumeSource{
						PersistentVolumeClaim: &vmoperatortypes.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pvc, vm).
		Build()

	r := &reconciler{
		Client: fakeClient,
		logger: logger.GetLoggerWithNoContext().Named("test"),
	}

	// Execute - this simulates the reconciliation triggered by EncryptionClass update
	// (The EncryptionClassToPersistentVolumeClaimMapper would have triggered reconciliation of this PVC)
	isAttached, vmName, err := r.isPVCAttachedToVM(ctx, pvc)

	// Verify - should detect attachment and return true
	assert.NoError(t, err)
	assert.True(t, isAttached,
		"PVC should be detected as attached when EncryptionClass is updated and PVC is attached to VM")
	assert.Equal(t, "prod-vm", vmName, "VM name should be returned")
}
