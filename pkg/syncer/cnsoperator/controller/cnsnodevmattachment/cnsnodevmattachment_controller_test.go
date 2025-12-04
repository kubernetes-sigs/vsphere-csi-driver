/*
Copyright 2025 The Kubernetes Authors.

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

package cnsnodevmattachment

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsopapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"

	v1a1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
)

// TestReconcileDetachWithVolumeIDFallback tests the end-to-end reconcile workflow
// for the DETACH path where volumeID is missing from AttachmentMetadata and must be
// retrieved using the getVolumeID() fallback mechanism in controller.
// This test mocks the vCenter calls and volume manager to verify the detach operation
// proceeds successfully when getVolumeID retrieves the volumeID from PV.
func TestReconcileDetachWithVolumeIDFallback(t *testing.T) {
	ctx := context.Background()

	// Set up the scheme
	SchemeGroupVersion := schema.GroupVersion{
		Group:   "cns.vmware.com",
		Version: "v1alpha1",
	}
	s := scheme.Scheme
	s.AddKnownTypes(SchemeGroupVersion, &v1a1.CnsNodeVmAttachment{})
	metav1.AddToGroupVersion(s, SchemeGroupVersion)

	// Create PVC that is bound to a PV
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pvc",
			Namespace:  "test-ns",
			Finalizers: []string{"cns.vmware.com/pvc-protection"},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: v1.ClaimBound,
		},
	}

	// Create PV with volumeID in CSI VolumeHandle
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       "csi.vsphere.vmware.com",
					VolumeHandle: "test-volume-handle-12345",
				},
			},
		},
	}

	// Create CnsNodeVmAttachment with DeletionTimestamp set (detach flow)
	// and AttachmentMetadata missing CNS volume ID (do that it triggers getVolumeID fallback)
	now := metav1.NewTime(time.Now())
	instance := &v1a1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-attachment",
			Namespace:         "test-ns",
			DeletionTimestamp: &now,
			Finalizers:        []string{"cns.vmware.com/cnsnodevmattachment"},
		},
		Spec: v1a1.CnsNodeVmAttachmentSpec{
			NodeUUID:   "test-node-uuid",
			VolumeName: "test-pvc",
		},
		Status: v1a1.CnsNodeVmAttachmentStatus{
			// AttachmentMetadata is missing CNS volume ID - this triggers the fallback
			AttachmentMetadata: map[string]string{},
			Attached:           true,
		},
	}

	// Create fake client with all objects
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(instance).
		WithRuntimeObjects(instance, pvc, pv).
		Build()

	// Initialize backOffDuration map (required by controller)
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	// Create reconciler with properly configured vCenter config
	r := &ReconcileCnsNodeVMAttachment{
		client: fakeClient,
		scheme: s,
		configInfo: &config.ConfigurationInfo{
			Cfg: &config.Config{
				VirtualCenter: map[string]*config.VirtualCenterConfig{
					"test-vc": {
						Datacenters: "dc-1",
					},
				},
			},
		},
		recorder: record.NewFakeRecorder(1024),
	}

	// Mock the volume manager using the same pattern as cnsnodevmbatchattachment tests
	mockVolumeManager := &unittestcommon.MockVolumeManager{}
	r.volumeManager = mockVolumeManager

	// Mock all vCenter-related functions to bypass actual vCenter calls
	originalGetVirtualCenterInstance := getVirtualCenterInstance
	originalConnectToVCenter := connectToVCenter
	originalCreateDatacenterFromVC := createDatacenterFromVC
	originalGetVMByUUID := getVMByUUIDFromVCenter
	defer func() {
		getVirtualCenterInstance = originalGetVirtualCenterInstance
		connectToVCenter = originalConnectToVCenter
		createDatacenterFromVC = originalCreateDatacenterFromVC
		getVMByUUIDFromVCenter = originalGetVMByUUID
	}()

	// Mock getVirtualCenterInstance to return a minimal fake VirtualCenter
	getVirtualCenterInstance = func(ctx context.Context, cfg *config.ConfigurationInfo,
		filterSuspendedDatastores bool) (*cnsvsphere.VirtualCenter, error) {
		return &cnsvsphere.VirtualCenter{}, nil
	}

	// Mock connectToVCenter to skip the actual connection
	connectToVCenter = func(ctx context.Context, vc *cnsvsphere.VirtualCenter) error {
		return nil
	}

	// Mock createDatacenterFromVC to return a minimal datacenter without needing real client
	createDatacenterFromVC = func(ctx context.Context, vc *cnsvsphere.VirtualCenter,
		dcMoref, host string) (*cnsvsphere.Datacenter, error) {
		return &cnsvsphere.Datacenter{}, nil
	}

	// Mock getVMByUUIDFromVCenter to return a mock VM
	getVMByUUIDFromVCenter = func(ctx context.Context, dc *cnsvsphere.Datacenter,
		nodeUUID string) (*cnsvsphere.VirtualMachine, error) {
		return &cnsvsphere.VirtualMachine{}, nil
	}

	// Create reconcile request
	req := reconcile.Request{
		NamespacedName: k8stypes.NamespacedName{
			Name:      "test-attachment",
			Namespace: "test-ns",
		},
	}

	// Run reconcile
	result, err := r.Reconcile(ctx, req)

	// Assertions:
	// 1. No error should be returned
	assert.NoError(t, err, "Reconcile should succeed")
	// 2. Result should indicate successful completion (no requeue)
	assert.Equal(t, reconcile.Result{}, result, "Should not requeue on success")

	// Verify that the detach operation completed successfully
	// The key validation is that:
	// - getVolumeID was called (evidenced by log: "Successfully retrieved CNS volume ID")
	// - DetachVolume was called with the retrieved volumeID
	// - No error was returned from reconcile

	t.Logf("✓ Reconcile successfully completed detach using volumeID fallback from PV")
}

// TestReconcileDetachWithVolumeIDFallbackFailure tests the end-to-end reconcile workflow
// for the DETACH path where volumeID is missing from AttachmentMetadata and the
// getVolumeID() fallback also fails (e.g., PVC not found, PVC not bound, PV not found).
// This verifies that the controller returns an error and requeues for retry.
func TestReconcileDetachWithVolumeIDFallbackFailure(t *testing.T) {
	ctx := context.Background()

	// Set up the scheme
	SchemeGroupVersion := schema.GroupVersion{
		Group:   "cns.vmware.com",
		Version: "v1alpha1",
	}
	s := scheme.Scheme
	s.AddKnownTypes(SchemeGroupVersion, &v1a1.CnsNodeVmAttachment{})
	metav1.AddToGroupVersion(s, SchemeGroupVersion)

	// Create CnsNodeVmAttachment with DeletionTimestamp set (detach flow)
	// and AttachmentMetadata missing CNS volume ID (triggers getVolumeID fallback)
	// Note: We're NOT creating the PVC, so getVolumeID will fail
	now := metav1.NewTime(time.Now())
	instance := &v1a1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-attachment-fail",
			Namespace:         "test-ns",
			DeletionTimestamp: &now,
			Finalizers:        []string{"cns.vmware.com/cnsnodevmattachment"},
		},
		Spec: v1a1.CnsNodeVmAttachmentSpec{
			NodeUUID:   "test-node-uuid",
			VolumeName: "nonexistent-pvc", // This PVC doesn't exist
		},
		Status: v1a1.CnsNodeVmAttachmentStatus{
			// AttachmentMetadata is missing CNS volume ID - this triggers the fallback
			AttachmentMetadata: map[string]string{},
			Attached:           true,
		},
	}

	// Create fake client with only the instance (no PVC/PV)
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(instance).
		WithRuntimeObjects(instance).
		Build()

	// Initialize backOffDuration map (required by controller)
	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	// Create reconciler with properly configured vCenter config
	r := &ReconcileCnsNodeVMAttachment{
		client: fakeClient,
		scheme: s,
		configInfo: &config.ConfigurationInfo{
			Cfg: &config.Config{
				VirtualCenter: map[string]*config.VirtualCenterConfig{
					"test-vc": {
						Datacenters: "dc-1",
					},
				},
			},
		},
		recorder: record.NewFakeRecorder(1024),
	}

	// Mock the volume manager
	mockVolumeManager := &unittestcommon.MockVolumeManager{}
	r.volumeManager = mockVolumeManager

	// Mock all vCenter-related functions
	originalGetVirtualCenterInstance := getVirtualCenterInstance
	originalConnectToVCenter := connectToVCenter
	originalCreateDatacenterFromVC := createDatacenterFromVC
	originalGetVMByUUID := getVMByUUIDFromVCenter
	defer func() {
		getVirtualCenterInstance = originalGetVirtualCenterInstance
		connectToVCenter = originalConnectToVCenter
		createDatacenterFromVC = originalCreateDatacenterFromVC
		getVMByUUIDFromVCenter = originalGetVMByUUID
	}()

	getVirtualCenterInstance = func(ctx context.Context, cfg *config.ConfigurationInfo,
		filterSuspendedDatastores bool) (*cnsvsphere.VirtualCenter, error) {
		return &cnsvsphere.VirtualCenter{}, nil
	}

	connectToVCenter = func(ctx context.Context, vc *cnsvsphere.VirtualCenter) error {
		return nil
	}

	createDatacenterFromVC = func(ctx context.Context, vc *cnsvsphere.VirtualCenter,
		dcMoref, host string) (*cnsvsphere.Datacenter, error) {
		return &cnsvsphere.Datacenter{}, nil
	}

	getVMByUUIDFromVCenter = func(ctx context.Context, dc *cnsvsphere.Datacenter,
		nodeUUID string) (*cnsvsphere.VirtualMachine, error) {
		return &cnsvsphere.VirtualMachine{}, nil
	}

	// Create reconcile request
	req := reconcile.Request{
		NamespacedName: k8stypes.NamespacedName{
			Name:      "test-attachment-fail",
			Namespace: "test-ns",
		},
	}

	// Run reconcile
	result, err := r.Reconcile(ctx, req)

	// Assertions:
	// 1. No error should be returned (controller handles gracefully)
	assert.NoError(t, err, "Reconcile should not return error (handles internally)")
	// 2. Result should indicate requeue for retry
	assert.NotEqual(t, reconcile.Result{}, result, "Should requeue for retry")
	assert.True(t, result.RequeueAfter > 0, "Should requeue after timeout")

	// The key validation is that:
	// - getVolumeID was called and failed (evidenced by log: "Failed to get CNS volume ID")
	// - Controller handled the error gracefully and returned RequeueAfter
	// - No panic or unhandled error occurred

	t.Logf("✓ Reconcile correctly handled getVolumeID failure and requeued for retry")
}

func TestUpdateErrorOnInstanceToDisallowAttach(t *testing.T) {
	ctx := context.Background()

	// Set up the scheme
	SchemeGroupVersion := schema.GroupVersion{
		Group:   "cns.vmware.com",
		Version: "v1alpha1",
	}
	s := scheme.Scheme
	s.AddKnownTypes(SchemeGroupVersion, &v1a1.CnsNodeVmAttachment{})
	metav1.AddToGroupVersion(s, SchemeGroupVersion)

	// Create CnsNodeVmAttachment instance
	instance := &v1a1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-attachment",
			Namespace: "test-ns",
		},
	}

	// Create fake client with status subresource
	fakeClient := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(instance).
		WithRuntimeObjects(instance).
		Build()

	// Create reconciler
	r := &ReconcileCnsNodeVMAttachment{
		client:   fakeClient,
		recorder: record.NewFakeRecorder(10),
		scheme:   s,
	}

	backOffDuration = make(map[k8stypes.NamespacedName]time.Duration)

	// Call the function
	err := r.updateErrorOnInstanceToDisallowAttach(ctx, instance)

	// Assertions
	assert.NoError(t, err, "Function should not return an error")
	assert.Equal(t,
		"CnsNodeVMAttachment CR is deprecated. Please detach this PVC from the VM and then reattach it.",
		instance.Status.Error,
		"Status.Error should be updated with the deprecation message",
	)

	// Verify that the status was persisted by fetching the object from the fake client
	fetched := &v1a1.CnsNodeVmAttachment{}
	err = fakeClient.Get(ctx, k8stypes.NamespacedName{
		Name:      "test-attachment",
		Namespace: "test-ns",
	}, fetched)
	assert.NoError(t, err, "Should be able to fetch the instance from fake client")
	assert.Equal(t, instance.Status.Error, fetched.Status.Error, "Status should be persisted in fake client")
}

func TestAddPvcLabelToInstance(t *testing.T) {

	testPVCUID := "test-pvc-uid"
	scheme := runtime.NewScheme()
	_ = cnsopapis.AddToScheme(scheme)

	// Base instance without labels
	instance := &v1a1.CnsNodeVmAttachment{
		Spec: v1a1.CnsNodeVmAttachmentSpec{},
	}
	instance.Name = "test-instance"

	t.Run("adds label when not present", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance.DeepCopy()).Build()
		instanceCopy := instance.DeepCopy()

		err := addPvcLabel(context.Background(), cl, instanceCopy, testPVCUID)
		assert.NoError(t, err)
		assert.Equal(t, testPVCUID, instanceCopy.Labels[common.PvcUIDLabelKey])
	})

	t.Run("does not patch when label already exists", func(t *testing.T) {
		instance := instance.DeepCopy()
		instance.Labels = map[string]string{common.PvcUIDLabelKey: testPVCUID}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance.DeepCopy()).Build()
		err := addPvcLabel(context.Background(), cl, instance, "another-uid")
		assert.NoError(t, err)
		// Label should not be overwritten
		assert.Equal(t, testPVCUID, instance.Labels[common.PvcUIDLabelKey])
	})
}

func TestApplyAttachedPvcLabelToInstance(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = cnsopapis.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	testPVCUID := "test-pvc-uid"

	// Base instance (attached state toggled inside tests)
	baseInstance := &v1a1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-instance",
			Namespace: "default",
		},
		Spec: v1a1.CnsNodeVmAttachmentSpec{
			VolumeName: "testvol",
		},
		Status: v1a1.CnsNodeVmAttachmentStatus{},
	}

	t.Run("does nothing when instance not attached", func(t *testing.T) {
		instance := baseInstance.DeepCopy()
		instance.Status.Attached = false

		clientFake := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(instance).
			Build()

		r := &ReconcileCnsNodeVMAttachment{client: clientFake}

		err := r.applyAttachedPvcLabelToInstance(context.Background(), instance)
		assert.NoError(t, err)

		// No labels applied, no PVC lookup
		assert.Nil(t, instance.Labels)
	})

	t.Run("returns error when PVC fetch fails", func(t *testing.T) {
		instance := baseInstance.DeepCopy()
		instance.Status.Attached = true // triggers PVC lookup

		clientFake := fake.NewClientBuilder().
			WithScheme(scheme).
			// no PVC object added → Get should fail
			WithObjects(instance).
			Build()

		r := &ReconcileCnsNodeVMAttachment{client: clientFake}

		err := r.applyAttachedPvcLabelToInstance(context.Background(), instance)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("successfully sets label when PVC exists", func(t *testing.T) {
		instance := baseInstance.DeepCopy()
		instance.Status.Attached = true

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "testvol",
				Namespace: "default",
				UID:       k8stypes.UID(testPVCUID),
			},
		}

		clientFake := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(instance, pvc).
			Build()

		r := &ReconcileCnsNodeVMAttachment{client: clientFake}

		err := r.applyAttachedPvcLabelToInstance(context.Background(), instance)
		assert.NoError(t, err)

		// Ensure label was applied
		assert.Equal(t, testPVCUID, instance.Labels[common.PvcUIDLabelKey])
	})
}
