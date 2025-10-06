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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sFake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
	VolumeLock = &sync.Map{}

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

func getClientSetWithPvc() *k8sFake.Clientset {
	// Define a RWO PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-rwo-pvc",
			Namespace: "default",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// Initialize fake clientset with the PVC
	clientset := k8sFake.NewSimpleClientset(pvc)

	return clientset
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

		// Override with fake client
		newClientFunc = func(ctx context.Context) (kubernetes.Interface, error) {
			fakeK8sClient := getClientSetWithPvc()
			return fakeK8sClient, nil
		}

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
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		r.volumeManager = mockVolumeManager

		volumesToDetach := map[string]string{
			"pvc-1": "123-456",
			"pvc-2": "789-012",
		}
		clientset := getClientSetWithPvc()

		vm := &cnsvsphere.VirtualMachine{}
		err := r.reconcileInstanceWithDeletionTimestamp(context.TODO(),
			clientset,
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

		clientset := getClientSetWithPvc()

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		err := r.reconcileInstanceWithDeletionTimestamp(context.TODO(),
			clientset,
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
		clientset := getClientSetWithPvc()

		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			clientset,
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
		clientset := getClientSetWithPvc()

		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			clientset,
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
		err := validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-1")
		expectedEr := fmt.Errorf("incorrect input for PVC pvc-1 in namespace test-ns with accessMode ReadWriteOnce. " +
			"DiskMode cannot be IndependentPersistent")
		assert.EqualError(t, expectedEr, err.Error())

		batchAttachRequest = volumes.BatchAttachRequest{
			SharingMode: "sharingMultiWriter",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		err = validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-1")
		expectedEr = fmt.Errorf("incorrect input for PVC pvc-1 in namespace test-ns with accessMode ReadWriteOnce. " +
			"SharingMode cannot be sharingMultiWriter")
		assert.EqualError(t, expectedEr, err.Error())
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
		err := validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-rwx")
		expectedErr := fmt.Errorf("incorrect input for PVC pvc-rwx in namespace test-ns with accessMode ReadWriteMany. " +
			"DiskMode cannot be persistent")
		assert.EqualError(t, expectedErr, err.Error())

		batchAttachRequest = volumes.BatchAttachRequest{
			ControllerKey: "",
			UnitNumber:    "12",
			DiskMode:      "independent_persistent",
		}

		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		err = validateBatchAttachRequest(context.TODO(), batchAttachRequest, testNamespace, "pvc-rwx")
		expectedErr = fmt.Errorf("incorrect input for PVC pvc-rwx in namespace test-ns with accessMode ReadWriteMany. " +
			"ControllerKey cannot be empty")
		assert.EqualError(t, expectedErr, err.Error())
	})
}

func TestIsSharedPvc(t *testing.T) {
	tests := []struct {
		name        string
		accessModes []v1.PersistentVolumeAccessMode
		want        bool
	}{
		{
			name:        "Only ReadWriteOnce - not shared",
			accessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			want:        false,
		},
		{
			name:        "Includes ReadWriteMany - shared",
			accessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			want:        true,
		},
		{
			name:        "Includes ReadOnlyMany - shared",
			accessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
			want:        true,
		},
		{
			name:        "Includes all modes - shared",
			accessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadWriteMany, v1.ReadOnlyMany},
			want:        true,
		},
		{
			name:        "Empty access modes - not shared",
			accessModes: []v1.PersistentVolumeAccessMode{},
			want:        false,
		},
	}

	ctx := context.TODO()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isSharedPvc(ctx, tc.accessModes)
			if got != tc.want {
				t.Errorf("isSharedPvc() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAddPvcAnnotation(t *testing.T) {
	ctx := context.TODO()
	vmUUID := "test-vm-uuid"
	expectedKey := attachedVmPrefix + vmUUID

	tests := []struct {
		name               string
		initialAnnotations map[string]string
		expectError        bool
	}{
		{
			name:               "PVC with no annotations",
			initialAnnotations: nil,
			expectError:        false,
		},
		{
			name:               "PVC with existing annotations",
			initialAnnotations: map[string]string{"existing": "value"},
			expectError:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := k8sFake.NewSimpleClientset()

			// Create PVC in fake cluster
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pvc",
					Namespace:   "default",
					Annotations: tt.initialAnnotations,
				},
			}

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create pvc in fake client: %v", err)
			}

			// Get PVC from fake client
			pvcFromClient, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "test-pvc", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get pvc: %v", err)
			}

			err = addPvcAnnotation(ctx, client, vmUUID, pvcFromClient)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify annotation is added
			updatedPVC, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "test-pvc", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get updated pvc: %v", err)
			}

			val, ok := updatedPVC.Annotations[expectedKey]
			if !ok {
				t.Errorf("expected annotation %s not found", expectedKey)
			}
			if val != "" {
				t.Errorf("expected annotation value '', got '%s'", val)
			}
		})
	}
}

func TestRemovePvcAnnotation(t *testing.T) {
	ctx := context.TODO()
	vmUUID := "test-vm-uuid"
	annotationKey := attachedVmPrefix + vmUUID

	tests := []struct {
		name               string
		initialAnnotations map[string]string
		expectError        bool
	}{
		{
			name:               "PVC with no annotations",
			initialAnnotations: nil,
			expectError:        false,
		},
		{
			name:               "PVC without target annotation",
			initialAnnotations: map[string]string{"some-other": "value"},
			expectError:        false,
		},
		{
			name:               "PVC with target annotation",
			initialAnnotations: map[string]string{annotationKey: ""},
			expectError:        false,
		},
		{
			name: "PVC with multiple annotations including target",
			initialAnnotations: map[string]string{
				annotationKey: "",
				"keep-me":     "yes",
				"another-key": "value",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := k8sFake.NewSimpleClientset()

			// Create PVC in fake cluster
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pvc",
					Namespace:   "default",
					Annotations: tt.initialAnnotations,
				},
			}

			_, err := client.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create pvc in fake client: %v", err)
			}

			// Get PVC from fake client to pass to removePvcAnnotation
			pvcFromClient, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "test-pvc", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get pvc: %v", err)
			}

			err = removePvcAnnotation(ctx, client, vmUUID, pvcFromClient)
			if (err != nil) != tt.expectError {
				t.Fatalf("unexpected error status: got %v, want error? %v", err, tt.expectError)
			}

			// Get PVC again to verify annotations
			updatedPVC, err := client.CoreV1().PersistentVolumeClaims("default").Get(ctx, "test-pvc", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("failed to get updated pvc: %v", err)
			}

			_, found := updatedPVC.Annotations[annotationKey]

			if found {
				t.Errorf("annotation not expected after removal. Found annotations: %+v", updatedPVC.Annotations)
			}

			// Verify that other annotations remain intact
			for k, v := range tt.initialAnnotations {
				if k == annotationKey {
					continue
				}
				if updatedPVC.Annotations[k] != v {
					t.Errorf("annotation %q changed: got %q, want %q", k, updatedPVC.Annotations[k], v)
				}
			}
		})
	}
}

func TestPvcHasUsedByAnnotation(t *testing.T) {
	ctx := context.TODO()

	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "No annotations",
			annotations: nil,
			expected:    false,
		},
		{
			name: "Annotations without matching prefix",
			annotations: map[string]string{
				"some.other/annotation": "value",
			},
			expected: false,
		},
		{
			name: "Annotations with matching prefix",
			annotations: map[string]string{
				"cns.vmware.com/usedby-test-vm-uuid": "attached",
			},
			expected: true,
		},
		{
			name: "Multiple annotations, one with matching prefix",
			annotations: map[string]string{
				"foo":                              "bar",
				"cns.vmware.com/usedby-another-vm": "attached",
				"something.else":                   "value",
			},
			expected: true,
		},
		{
			name: "Multiple annotations, none with matching prefix",
			annotations: map[string]string{
				"foo":               "bar",
				"cns.vmware.com/no": "value",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-pvc",
					Annotations: tt.annotations,
				},
			}

			result := pvcHasUsedByAnnotaion(ctx, pvc)
			assert.Equal(t, tt.expected, result)
		})
	}
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
