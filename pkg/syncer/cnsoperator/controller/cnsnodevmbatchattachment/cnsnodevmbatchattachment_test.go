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
	testCnsNodeVMBatchAttachmentName = "test-cnsnodevmbatchattachemnt"
	testNamespace                    = "test-ns"
)

// setupTestCnsNodeVMBatchAttachment created CnsNodeVMBatchAttachment CR with volumes for testing.
func setupTestCnsNodeVMBatchAttachment() v1alpha1.CnsNodeVMBatchAttachment {
	var (
		testNodeUUID                 = "test-1"
		disk1                        = "disk-1"
		disk2                        = "disk-2"
		pvc1                         = "pvc-1"
		pvc2                         = "pvc-2"
		testCnsNodeVMBatchAttachment = v1alpha1.CnsNodeVMBatchAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name:            testCnsNodeVMBatchAttachmentName,
				Namespace:       testNamespace,
				ResourceVersion: "1",
			},
			Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
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
			Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
				VolumeStatus: []v1alpha1.VolumeStatus{
					{
						Name: disk1,
						PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
							ClaimName:   pvc1,
							DiskUUID:    "123456",
							CnsVolumeID: "67890",
						},
					},
					{
						Name: disk2,
						PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
							ClaimName:   pvc2,
							DiskUUID:    "123456",
							CnsVolumeID: "67890",
						},
					},
				},
			},
		}
	)

	return testCnsNodeVMBatchAttachment

}

func setTestEnvironment(testCnsNodeVMBatchAttachment *v1alpha1.CnsNodeVMBatchAttachment,
	setDeletionTimestamp bool) *Reconciler {
	cnsNodeVmBatchAttachment := testCnsNodeVMBatchAttachment.DeepCopy()
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
	pvc1 := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-1",
			Namespace: "test-ns",
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

	// Define pvc-2
	pvc2 := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-2",
			Namespace: "test-ns",
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteOnce,
			},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("2Gi"),
				},
			},
		},
	}

	// Initialize fake clientset with the PVC
	clientset := k8sFake.NewSimpleClientset(pvc1, pvc2)

	return clientset
}

func TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsError(t *testing.T) {
	t.Run("TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsError", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		testCnsNodeVMBatchAttachment.Spec.NodeUUID = "test-2"

		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      testCnsNodeVMBatchAttachmentName,
				Namespace: testNamespace,
			},
		}

		GetVMFromVcenter = MockGetVMFromVcenter
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		res, err := r.Reconcile(context.TODO(), req)
		assert.NoError(t, err)

		expectedReconcileResult := reconcile.Result{RequeueAfter: time.Second}
		assert.Equal(t, expectedReconcileResult, res)

		updatedCnsNodeVMBatchAttachment := &v1alpha1.CnsNodeVMBatchAttachment{}
		if err := r.client.Get(context.TODO(), req.NamespacedName, updatedCnsNodeVMBatchAttachment); err != nil {
			t.Fatalf("failed to get cnsnodevmbatchattachemnt instance")
		}

		expectedReconcileError := fmt.Errorf("some error occurred while getting VM")
		assert.EqualError(t, expectedReconcileError,
			updatedCnsNodeVMBatchAttachment.Status.Conditions[0].Message)
	})
}

/*func TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsNotFoundError(t *testing.T) {

	t.Run("TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsNotFoundError", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		testCnsNodeVMBatchAttachment.Spec.NodeUUID = "test-3"

		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, true)

		req := reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      testCnsNodeVMBatchAttachmentName,
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

		updatedCnsNodeVMBatchAttachment := &v1alpha1.CnsNodeVMBatchAttachment{}
		err = r.client.Get(context.TODO(), req.NamespacedName, updatedCnsNodeVMBatchAttachment)
		if err == nil {
			t.Fatalf("failed to get cnsnodevmbatchattachemnt instance")
		}

		// Error should be not found
		if statusErr, ok := err.(*errors.StatusError); ok {
			assert.Equal(t, metav1.StatusReasonNotFound, statusErr.Status().Reason)
		} else {
			t.Fatalf("Unable to verify CnsNodeVMBatchAttachment error")
		}
	})
}*/

func TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsNotFoundErrorAndInstanceIsNotDeleted(t *testing.T) {
	t.Run("TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsNotFoundErrorAndInstanceIsNotDeleted",
		func(t *testing.T) {
			testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
			nodeUUID := "test-3"
			testCnsNodeVMBatchAttachment.Spec.NodeUUID = nodeUUID

			r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      testCnsNodeVMBatchAttachmentName,
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
			assert.NoError(t, err)

			expectedReconcileResult := reconcile.Result{RequeueAfter: time.Second}
			assert.Equal(t, expectedReconcileResult, res)

			updatedCnsNodeVMBatchAttachment := &v1alpha1.CnsNodeVMBatchAttachment{}
			if err := r.client.Get(context.TODO(), req.NamespacedName, updatedCnsNodeVMBatchAttachment); err != nil {
				t.Fatalf("failed to get cnsnodevmbatchattachemnt instance")
			}

			expectedReconcileError := fmt.Errorf("virtual Machine with UUID %s on vCenter does not exist. "+
				"Vm is CR is deleted or is being deleted but"+
				"CnsNodeVMBatchAttachmentInstance %s is not being deleted", nodeUUID, testCnsNodeVMBatchAttachmentName)
			expectedErrorMsg := expectedReconcileError.Error()
			assert.Equal(t, expectedErrorMsg, updatedCnsNodeVMBatchAttachment.Status.Conditions[0].Message)
		})
}

func TestReconcileWithDeletionTimestamp(t *testing.T) {
	t.Run("TestReconcileWithDeletionTimestamp", func(t *testing.T) {

		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
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
			&testCnsNodeVMBatchAttachment, volumesToDetach, vm)
		assert.NoError(t, err)
	})
}

func TestReconcileWithDeletionTimestampWhenDetachFails(t *testing.T) {
	t.Run("TestReconcileWithDeletionTimestampWhenDetachFails", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
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
			&testCnsNodeVMBatchAttachment, volumesToDetach, vm)
		if err == nil {
			t.Fatal("Expected reconcile error")
		}

		expectedError := fmt.Errorf("failed to detach volumes: pvc-1")
		assert.EqualError(t, expectedError, err.Error())
	})
}

func TestReconcileWithoutDeletionTimestamp(t *testing.T) {

	t.Run("TestReconcileWithoutDeletionTimestamp", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
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
			&testCnsNodeVMBatchAttachment, volumesToDetach, vm)
		assert.NoError(t, err)
	})
}

func TestReconcileWithoutDeletionTimestampWhenAttachFails(t *testing.T) {

	t.Run("TestReconcileWithoutDeletionTimestamp", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		volumesToDetach := map[string]string{
			"pvc-1": "123-456",
		}

		// Update PVC to fail-attach-pvc-3 to mock failure in attach
		for i, volume := range testCnsNodeVMBatchAttachment.Spec.Volumes {
			if volume.PersistentVolumeClaim.ClaimName == "pvc-2" {
				volume.PersistentVolumeClaim.ClaimName = "fail-attach-pvc-3"
				testCnsNodeVMBatchAttachment.Spec.Volumes[i] = volume
				break
			}
		}

		vm := &cnsvsphere.VirtualMachine{}
		clientset := getClientSetWithPvc()

		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			clientset,
			&testCnsNodeVMBatchAttachment, volumesToDetach, vm)
		if err == nil {
			t.Fatal("Expected reconcile error")
		}
		expectedErrorMsg := fmt.Errorf("failed to attach volume")
		assert.EqualError(t, expectedErrorMsg, err.Error())
	})
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
				attachedVmPrefix + "vm-uuid": "attached",
			},
			expected: true,
		},
		{
			name: "Multiple annotations, one with matching prefix",
			annotations: map[string]string{
				"foo":                           "bar",
				attachedVmPrefix + "another-vm": "attached",
				"something.else":                "value",
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
