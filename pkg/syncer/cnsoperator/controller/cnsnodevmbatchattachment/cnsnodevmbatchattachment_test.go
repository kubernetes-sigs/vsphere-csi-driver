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

package cnsnodevmbatchattachment

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/object"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"

	v1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
)

var (
	testBufferSize                   = 1024
	testCnsNodeVmBatchAttachmentName = "test-cnsnodevmbatchattachemnt"
	testNamespace                    = "test-ns"
)

// setupTestCnsNodeVmBatchAttachment created CnsNodeVmBatchAttachment CR with volumes for testing.
func setupTestCnsNodeVmBatchAttachment() v1alpha1.CnsNodeVmBatchAttachment {
	var (
		testNodeUUID                 = "test-1"
		disk1                        = "disk-1"
		disk2                        = "disk-2"
		pvc1                         = "pvc-1"
		pvc2                         = "pvc-2"
		testCnsNodeVmBatchAttachment = v1alpha1.CnsNodeVmBatchAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name:            testCnsNodeVmBatchAttachmentName,
				Namespace:       testNamespace,
				ResourceVersion: "1",
			},
			Spec: v1alpha1.CnsNodeVmBatchAttachmentSpec{
				NodeUUID: testNodeUUID,
				Volumes: []v1alpha1.VolumeSpec{
					{
						Name: disk1,
						PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
							ClaimName: pvc1,
						},
					},
					{
						Name: disk2,
						PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
							ClaimName: pvc2,
						},
					},
				},
			},
			Status: v1alpha1.CnsNodeVmBatchAttachmentStatus{
				VolumeStatus: []v1alpha1.VolumeStatus{
					{
						Name: disk1,
						PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
							ClaimName:   pvc1,
							Diskuuid:    "123456",
							CnsVolumeID: "67890",
							Attached:    true,
						},
					},
					{
						Name: disk2,
						PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
							ClaimName:   pvc2,
							Diskuuid:    "123456",
							CnsVolumeID: "67890",
							Attached:    true,
						},
					},
				},
			},
		}
	)

	return testCnsNodeVmBatchAttachment

}

func setTestEnvironment(testCnsNodeVmBatchAttachment *v1alpha1.CnsNodeVmBatchAttachment,
	setDeletionTimestamp bool) *Reconciler {
	cnsNodeVmBatchAttachment := testCnsNodeVmBatchAttachment.DeepCopy()
	//objs := []runtime.Object{cnsNodeVmBatchAttachment}

	if setDeletionTimestamp {
		currentTime := time.Now()
		// Convert current time to v1.Time and take the address to assign to a pointer
		k8sTime := metav1.NewTime(currentTime)
		k8sTimePtr := &k8sTime
		cnsNodeVmBatchAttachment.DeletionTimestamp = k8sTimePtr
		cnsNodeVmBatchAttachment.Finalizers = []string{"cns.vmware.com"}
	}

	SchemeGroupVersion := schema.GroupVersion{
		Group:   "cns.vmware.com",
		Version: "v1alpha1",
	}

	// Register operator types with the runtime scheme.
	s := scheme.Scheme
	s.AddKnownTypes(SchemeGroupVersion, cnsNodeVmBatchAttachment)
	metav1.AddToGroupVersion(s, SchemeGroupVersion)

	fakeClient := fake.NewClientBuilder().
		WithStatusSubresource(cnsNodeVmBatchAttachment).
		WithScheme(s).
		WithRuntimeObjects(cnsNodeVmBatchAttachment).
		Build()

	r := &Reconciler{
		client:       fakeClient,
		scheme:       s,
		configInfo:   config.ConfigurationInfo{},
		recorder:     record.NewFakeRecorder(testBufferSize),
		instanceLock: sync.Map{},
	}

	backOffDuration = make(map[types.NamespacedName]time.Duration)

	return r

}

func TestCnsNodeVmBatchAttachmentWhenVmOnVcenterReturnsError(t *testing.T) {
	t.Run("TestCnsNodeVmBatchAttachmentWhenVmOnVcenterReturnsError", func(t *testing.T) {
		testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
		testCnsNodeVmBatchAttachment.Spec.NodeUUID = "test-2"

		r := setTestEnvironment(&testCnsNodeVmBatchAttachment, false)

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      testCnsNodeVmBatchAttachmentName,
				Namespace: testNamespace,
			},
		}

		GetVMFromVcenter = MockGetVMFromVcenter
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		res, err := r.Reconcile(context.TODO(), req)
		assert.NoError(t, err)

		expectedReconcileResult := reconcile.Result{RequeueAfter: time.Second}
		assert.Equal(t, expectedReconcileResult, res)

		updatedCnsNodeVmBatchAttachment := &v1alpha1.CnsNodeVmBatchAttachment{}
		if err := r.client.Get(context.TODO(), req.NamespacedName, updatedCnsNodeVmBatchAttachment); err != nil {
			t.Fatalf("failed to get cnsnodevmbatchattachemnt instance")
		}

		expectedReconcileError := fmt.Errorf("some error occurred while getting VM")
		assert.EqualError(t, expectedReconcileError, updatedCnsNodeVmBatchAttachment.Status.Error)
	})
}

func TestCnsNodeVmBatchAttachmentWhenVmOnVcenterReturnsNotFoundError(t *testing.T) {

	t.Run("TestCnsNodeVmBatchAttachmentWhenVmOnVcenterReturnsNotFoundError", func(t *testing.T) {
		testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
		testCnsNodeVmBatchAttachment.Spec.NodeUUID = "test-3"

		r := setTestEnvironment(&testCnsNodeVmBatchAttachment, true)

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      testCnsNodeVmBatchAttachmentName,
				Namespace: testNamespace,
			},
		}

		GetVMFromVcenter = MockGetVMFromVcenter
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		res, err := r.Reconcile(context.TODO(), req)
		if err != nil {
			t.Fatal("Unexpected reconcile error")
		}
		expectedReconcileResult := reconcile.Result{}
		var expectedReconcileError error

		assert.Equal(t, expectedReconcileResult, res)
		assert.Equal(t, expectedReconcileError, err)

		updatedCnsNodeVmBatchAttachment := &v1alpha1.CnsNodeVmBatchAttachment{}
		err = r.client.Get(context.TODO(), req.NamespacedName, updatedCnsNodeVmBatchAttachment)
		if err == nil {
			t.Fatalf("failed to get cnsnodevmbatchattachemnt instance")
		}

		// Error should be not found
		if statusErr, ok := err.(*errors.StatusError); ok {
			assert.Equal(t, metav1.StatusReasonNotFound, statusErr.Status().Reason)
		} else {
			t.Fatalf("Unable to verify CnsNodeVmBatchAttachment error")
		}
	})
}

func TestCnsNodeVmBatchAttachmentWhenVmOnVcenterReturnsNotFoundErrorAndInstanceIsNotDeleted(t *testing.T) {
	t.Run("TestCnsNodeVmBatchAttachmentWhenVmOnVcenterReturnsNotFoundErrorAndInstanceIsNotDeleted",
		func(t *testing.T) {
			testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
			nodeUUID := "test-3"
			testCnsNodeVmBatchAttachment.Spec.NodeUUID = nodeUUID

			r := setTestEnvironment(&testCnsNodeVmBatchAttachment, false)

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testCnsNodeVmBatchAttachmentName,
					Namespace: testNamespace,
				},
			}

			GetVMFromVcenter = MockGetVMFromVcenter
			commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

			res, err := r.Reconcile(context.TODO(), req)
			assert.NoError(t, err)

			expectedReconcileResult := reconcile.Result{RequeueAfter: time.Second}
			assert.Equal(t, expectedReconcileResult, res)

			updatedCnsNodeVmBatchAttachment := &v1alpha1.CnsNodeVmBatchAttachment{}
			if err := r.client.Get(context.TODO(), req.NamespacedName, updatedCnsNodeVmBatchAttachment); err != nil {
				t.Fatalf("failed to get cnsnodevmbatchattachemnt instance")
			}

			expectedReconcileError := fmt.Errorf("virtual Machine with UUID %s on vCenter does not exist. "+
				"Vm is CR is deleted or is being deleted but"+
				"CnsNodeVmBatchAttachmentInstance %s is not being deleted", nodeUUID, testCnsNodeVmBatchAttachmentName)
			expectedErrorMsg := expectedReconcileError.Error()
			assert.Equal(t, expectedErrorMsg, updatedCnsNodeVmBatchAttachment.Status.Error)
		})
}

func TestReconcileWithDeletionTimestamp(t *testing.T) {
	t.Run("TestReconcileWithDeletionTimestamp", func(t *testing.T) {

		testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVmBatchAttachment, false)
		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager

		volumesToDetach := map[string]string{
			"pvc-1": "123-456",
			"pvc-2": "789-012",
		}
		vm := &cnsvsphere.VirtualMachine{}
		err := r.reconcileInstanceWithDeletionTimestamp(context.TODO(),
			&testCnsNodeVmBatchAttachment, volumesToDetach, vm)
		assert.NoError(t, err)
	})
}

func TestReconcileWithDeletionTimestampWhenDetachFails(t *testing.T) {
	t.Run("TestReconcileWithDeletionTimestampWhenDetachFails", func(t *testing.T) {
		testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVmBatchAttachment, false)
		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager

		volumesToDetach := map[string]string{
			"pvc-1": "fail-detach",
			"pvc-2": "789-012",
		}

		vmObj := &object.VirtualMachine{}
		vm := &cnsvsphere.VirtualMachine{
			VirtualMachine: vmObj,
		}

		err := r.reconcileInstanceWithDeletionTimestamp(context.TODO(),
			&testCnsNodeVmBatchAttachment, volumesToDetach, vm)
		if err == nil {
			t.Fatal("Expected reconcile error")
		}

		expectedError := fmt.Errorf("failed to detach volumes: pvc-1")
		assert.EqualError(t, expectedError, err.Error())
	})
}

func TestReconcileWithoutDeletionTimestamp(t *testing.T) {

	t.Run("TestReconcileWithoutDeletionTimestamp", func(t *testing.T) {
		testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVmBatchAttachment, false)
		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		volumesToDetach := map[string]string{
			"pvc-1": "123-456",
		}

		vm := &cnsvsphere.VirtualMachine{}
		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			&testCnsNodeVmBatchAttachment, volumesToDetach, vm)
		assert.NoError(t, err)
	})
}

func TestReconcileWithoutDeletionTimestampWhenAttachFails(t *testing.T) {

	t.Run("TestReconcileWithoutDeletionTimestamp", func(t *testing.T) {
		testCnsNodeVmBatchAttachment := setupTestCnsNodeVmBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVmBatchAttachment, false)
		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		volumesToDetach := map[string]string{
			"pvc-1": "123-456",
		}

		// Update PVC to fail-attach-pvc-3 to mock failure in attach
		for i, volume := range testCnsNodeVmBatchAttachment.Spec.Volumes {
			if volume.PersistentVolumeClaim.ClaimName == "pvc-2" {
				volume.PersistentVolumeClaim.ClaimName = "fail-attach-pvc-3"
				testCnsNodeVmBatchAttachment.Spec.Volumes[i] = volume
				break
			}
		}

		vm := &cnsvsphere.VirtualMachine{}
		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			&testCnsNodeVmBatchAttachment, volumesToDetach, vm)
		if err == nil {
			t.Fatal("Expected reconcile error")
		}
		expectedErrorMsg := fmt.Errorf("failed to attach volume")
		assert.EqualError(t, expectedErrorMsg, err.Error())
	})
}

func TestValidateBatchAttachRequestWithRwoPvc(t *testing.T) {

	t.Run("TestValidateBatchAttachRequestWithRwoPvc", func(t *testing.T) {

		batchAttachRequest := volumes.BatchAttachRequest{
			DiskMode: "IndependentPersistent",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		_, err := validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-1")
		expectedEr := fmt.Errorf("incorrect input for PVC pvc-1 in namespace test-ns with accessMode ReadWriteOnce. " +
			"DiskMode cannot be IndependentPersistent")
		assert.EqualError(t, expectedEr, err.Error())

		batchAttachRequest = volumes.BatchAttachRequest{
			SharingMode: "sharingMultiWriter",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		_, err = validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-1")
		expectedEr = fmt.Errorf("incorrect input for PVC pvc-1 in namespace test-ns with accessMode ReadWriteOnce. " +
			"SharingMode cannot be sharingMultiWriter")
		assert.EqualError(t, expectedEr, err.Error())

		batchAttachRequest = volumes.BatchAttachRequest{
			SharingMode: "",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		_, err = validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-1")
		assert.NoError(t, err)

	})
}

func TestValidateBatchAttachRequestWithRwxPvc(t *testing.T) {

	t.Run("TestValidateBatchAttachRequestWithRwxPvc", func(t *testing.T) {

		batchAttachRequest := volumes.BatchAttachRequest{
			DiskMode:      "persistent",
			ControllerKey: "12345",
			UnitNumber:    "9",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		_, err := validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-rwx")
		expectedErr := fmt.Errorf("incorrect input for PVC pvc-rwx in namespace test-ns with accessMode ReadWriteMany. " +
			"DiskMode cannot be persistent")
		assert.EqualError(t, expectedErr, err.Error())

		batchAttachRequest = volumes.BatchAttachRequest{
			ControllerKey: "",
			UnitNumber:    "12",
			DiskMode:      "independent_persistent",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		_, err = validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-rwx")
		expectedErr = fmt.Errorf("incorrect input for PVC pvc-rwx in namespace test-ns with accessMode ReadWriteMany. " +
			"ControllerKey cannot be empty")
		assert.EqualError(t, expectedErr, err.Error())

		batchAttachRequest = volumes.BatchAttachRequest{
			ControllerKey: "1001",
			UnitNumber:    "12",
			DiskMode:      "",
			SharingMode:   "None",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		attacheReq, err := validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-rwx")
		assert.NoError(t, err)
		assert.Equal(t, "independent_persistent", attacheReq.DiskMode)
	})
}

func MockGetVMFromVcenter(ctx context.Context, nodeUUID string,
	configInfo config.ConfigurationInfo) (*cnsvsphere.VirtualMachine, error) {
	var vm *cnsvsphere.VirtualMachine
	if nodeUUID == "test-2" {
		return vm, fmt.Errorf("some error occurred while getting VM")
	}
	if nodeUUID == "test-3" {
		return vm, cnsvsphere.ErrVMNotFound
	}
	return &cnsvsphere.VirtualMachine{}, nil
}
