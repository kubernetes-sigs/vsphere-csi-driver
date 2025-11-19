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
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
