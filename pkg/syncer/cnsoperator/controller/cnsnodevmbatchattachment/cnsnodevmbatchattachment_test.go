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
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sFake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	vim25types "github.com/vmware/govmomi/vim25/types"
	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	cnsopapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsopv1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"

	"github.com/agiledragon/gomonkey/v2"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	"k8s.io/apimachinery/pkg/runtime"
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
		testNodeUUID                 = "test-uuid"
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
				InstanceUUID: testNodeUUID,
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
	_ = cnsopapis.AddToScheme(s)
	_ = vmoperatortypes.AddToScheme(s)

	cnsOperatorClient := fake.NewClientBuilder().WithScheme(s).WithObjects().Build()

	vm1 := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCnsNodeVMBatchAttachmentName,
			Namespace: testNamespace,
		},
		Status: vmoperatortypes.VirtualMachineStatus{
			InstanceUUID: "test-uuid",
		},
	}

	vmClient := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(vm1).
		Build()

	VolumeLock = &sync.Map{}

	fakeClient := fake.NewClientBuilder().
		WithStatusSubresource(cnsNodeVmBatchAttachment).
		WithScheme(s).
		WithRuntimeObjects(cnsNodeVmBatchAttachment).
		Build()

	r := &Reconciler{
		client:            fakeClient,
		scheme:            s,
		configInfo:        &config.ConfigurationInfo{},
		recorder:          record.NewFakeRecorder(testBufferSize),
		instanceLock:      sync.Map{},
		cnsOperatorClient: cnsOperatorClient,
		vmOperatorClient:  vmClient,
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
	clientset := k8sFake.NewClientset(pvc1, pvc2)

	return clientset
}

func TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsError(t *testing.T) {
	t.Run("TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsError", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		testCnsNodeVMBatchAttachment.Spec.InstanceUUID = "test-2"

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
		assert.EqualError(t, expectedReconcileError, updatedCnsNodeVMBatchAttachment.Status.Conditions[0].Message)
	})
}

func TestReconcileRepairsZeroBackOffDuration(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	testCnsNodeVMBatchAttachment.Spec.InstanceUUID = "test-2"
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      testCnsNodeVMBatchAttachmentName,
			Namespace: testNamespace,
		},
	}
	backOffDuration[req.NamespacedName] = 0

	GetVMFromVcenter = MockGetVMFromVcenter
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	res, err := r.Reconcile(context.TODO(), req)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res)
	assert.Equal(t, 2*time.Second, backOffDuration[req.NamespacedName])
}

func TestReconcileRemovesBackOffDurationWhenInstanceNotFound(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "does-not-exist", Namespace: testNamespace},
	}
	backOffDuration[req.NamespacedName] = 4 * time.Minute

	res, err := r.Reconcile(context.TODO(), req)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, res)
	backOffDurationMapMutex.Lock()
	_, exists := backOffDuration[req.NamespacedName]
	backOffDurationMapMutex.Unlock()
	assert.False(t, exists)
}

func TestRecordNormalEventDoesNotCreateBackOffDuration(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	namespacedName := types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}
	delete(backOffDuration, namespacedName)

	recordEvent(context.TODO(), r, &instance, v1.EventTypeNormal, "test success")

	_, exists := backOffDuration[namespacedName]
	assert.False(t, exists)
}

func TestRecordWarningEventInitializesMissingBackOffDuration(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&instance, false)
	namespacedName := types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}

	recordEvent(context.TODO(), r, &instance, v1.EventTypeWarning, "test warning")

	assert.Equal(t, 2*time.Second, backOffDuration[namespacedName])
}

func TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsNotFoundErrorAndInstanceIsNotDeleted(t *testing.T) {
	t.Run("TestCnsNodeVMBatchAttachmentWhenVmOnVcenterReturnsNotFoundErrorAndInstanceIsNotDeleted",
		func(t *testing.T) {
			testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
			nodeUUID := "test-3"
			testCnsNodeVMBatchAttachment.Spec.InstanceUUID = nodeUUID

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

// detachTrackingMockVolumeManager records DetachVolume calls.
type detachTrackingMockVolumeManager struct {
	unittestcommon.MockVolumeManager
	detachedIDs []string
}

func (m *detachTrackingMockVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	m.detachedIDs = append(m.detachedIDs, volumeID)
	return "", nil
}

func TestProcessDetachRejectsVmNotInSameNamespace(t *testing.T) {
	tests := []struct {
		name         string
		instanceName string
		instanceUUID string
	}{
		{
			// VM CR with the instance name exists in the namespace but has a different InstanceUUID,
			// e.g. InstanceUUID belongs to a VM in another namespace.
			name:         "InstanceUUIDMismatch",
			instanceName: testCnsNodeVMBatchAttachmentName,
			instanceUUID: "uuid-of-vm-in-other-namespace",
		},
		{
			// No VM CR with the instance name exists in the namespace.
			name:         "VmCrNotFound",
			instanceName: "vm-not-in-namespace",
			instanceUUID: "test-uuid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
			testCnsNodeVMBatchAttachment.Name = tt.instanceName
			testCnsNodeVMBatchAttachment.Spec.InstanceUUID = tt.instanceUUID
			r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
			mockVolumeManager := &detachTrackingMockVolumeManager{}
			r.volumeManager = mockVolumeManager
			commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

			volumesToDetach := map[string]string{
				"pvc-1": "123-456",
			}
			vm := &cnsvsphere.VirtualMachine{}
			clientset := getClientSetWithPvc()

			failed, err := r.processDetach(context.TODO(), clientset, vm, &testCnsNodeVMBatchAttachment,
				volumesToDetach)
			expectedError := fmt.Errorf("failed to find VM with instance UUID %s in namespace %s",
				tt.instanceUUID, testNamespace)
			assert.EqualError(t, err, expectedError.Error())
			assert.Empty(t, mockVolumeManager.detachedIDs, "DetachVolume must not be called")
			assert.Equal(t, map[string]bool{"pvc-1": true}, failed,
				"every PVC must be reported as not detached when processDetach fails early")
		})
	}
}

func TestProcessDetachWithNoVolumesSkipsNamespaceCheck(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	// No VM CR exists for this instance; with nothing to detach the namespace check must not run.
	testCnsNodeVMBatchAttachment.Name = "vm-not-in-namespace"
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	mockVolumeManager := &detachTrackingMockVolumeManager{}
	r.volumeManager = mockVolumeManager

	_, err := r.processDetach(context.TODO(), getClientSetWithPvc(), &cnsvsphere.VirtualMachine{},
		&testCnsNodeVMBatchAttachment, map[string]string{})
	assert.NoError(t, err)
	assert.Empty(t, mockVolumeManager.detachedIDs)
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

		volumesToAttach := map[string]string{}

		vm := &cnsvsphere.VirtualMachine{}
		clientset := getClientSetWithPvc()

		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			clientset,
			&testCnsNodeVMBatchAttachment, volumesToDetach, volumesToAttach, vm)
		assert.NoError(t, err)
	})
}

func TestReconcileWithoutDeletionTimestampWithNoVolumestoAttach(t *testing.T) {

	t.Run("TestReconcileWithoutDeletionTimestampWithNoVolumestoAttach", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		testCnsNodeVMBatchAttachment.Spec.Volumes = []v1alpha1.VolumeSpec{}
		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
		volumesToDetach := map[string]string{}
		vm := &cnsvsphere.VirtualMachine{}
		clientset := getClientSetWithPvc()
		volumesToAttach := map[string]string{}

		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			clientset,
			&testCnsNodeVMBatchAttachment, volumesToDetach, volumesToAttach, vm)
		assert.NoError(t, err)
	})
}

func TestReconcileWithoutDeletionTimestampWhenAttachFails(t *testing.T) {

	t.Run("TestReconcileWithoutDeletionTimestamp", func(t *testing.T) {
		testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
		r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
		commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

		volumesToDetach := map[string]string{
			"pvc-1": "123-456",
		}
		volumesToAttach := map[string]string{"fail-attach-pvc-3": "fail-attach"}

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

		mockVolumeManager := &unittestcommon.MockVolumeManager{}
		r.volumeManager = mockVolumeManager

		err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(),
			clientset,
			&testCnsNodeVMBatchAttachment, volumesToDetach, volumesToAttach, vm)
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
	expectedKey := cnsoperatortypes.UsedByVMAnnotationPrefix + vmUUID

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
			client := k8sFake.NewClientset()

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
	annotationKey := cnsoperatortypes.UsedByVMAnnotationPrefix + vmUUID

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
			client := k8sFake.NewClientset()

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
				cnsoperatortypes.UsedByVMAnnotationPrefix + "vm-uuid": "attached",
			},
			expected: true,
		},
		{
			name: "Multiple annotations, one with matching prefix",
			annotations: map[string]string{
				"foo": "bar",
				cnsoperatortypes.UsedByVMAnnotationPrefix + "another-vm": "attached",
				"something.else": "value",
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

func TestIsSharedPvc(t *testing.T) {
	tests := []struct {
		name     string
		accesses []v1.PersistentVolumeAccessMode
		expected bool
	}{
		{
			name:     "ReadWriteOnce - not shared",
			accesses: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			expected: false,
		},
		{
			name:     "ReadOnlyMany - shared",
			accesses: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
			expected: true,
		},
		{
			name:     "ReadWriteMany - shared",
			accesses: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			expected: true,
		},
		{
			name:     "Multiple modes including ReadWriteMany",
			accesses: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadWriteMany},
			expected: true,
		},
		{
			name:     "Multiple modes including ReadOnlyMany",
			accesses: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadOnlyMany},
			expected: true,
		},
		{
			name:     "Empty access modes - not shared",
			accesses: []v1.PersistentVolumeAccessMode{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pvc := v1.PersistentVolumeClaim{
				Spec: v1.PersistentVolumeClaimSpec{
					AccessModes: tt.accesses,
				},
			}

			result := isSharedPvc(pvc)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestUpdatePvcStatusEntryName_AddsSuffix(t *testing.T) {
	ctx := context.TODO()

	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "pvc-1",
					},
				},
			},
		},
	}

	pvcsToDetach := map[string]string{
		"pvc-1": "",
	}

	updatePvcStatusEntryName(ctx, instance, pvcsToDetach)

	got := instance.Status.VolumeStatus[0].Name
	want := "vol1" + detachSuffix

	if got != want {
		t.Fatalf("expected volume name %q, got %q", want, got)
	}
}

func TestUpdatePvcStatusEntryName_SkipsIfAlreadyHasSuffix(t *testing.T) {
	ctx := context.TODO()

	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					Name: "vol1" + detachSuffix,
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "pvc-1",
					},
				},
			},
		},
	}

	pvcsToDetach := map[string]string{
		"pvc-1": "",
	}

	updatePvcStatusEntryName(ctx, instance, pvcsToDetach)

	got := instance.Status.VolumeStatus[0].Name
	want := "vol1" + detachSuffix // should remain unchanged

	if got != want {
		t.Fatalf("expected volume name to remain %q, got %q", want, got)
	}
}

// The detaching entry may come after the regular entry in the status. A single pass which
// records detaching PVCs while iterating would rename the regular entry before seeing the
// detaching one, and create a duplicate name.
func TestUpdatePvcStatusEntryName_NoDuplicateWhenDetachingEntryComesLater(t *testing.T) {
	ctx := context.TODO()

	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					Name:                  "vol1",
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"},
				},
				{
					Name:                  "vol1" + detachSuffix,
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"},
				},
			},
		},
	}

	updatePvcStatusEntryName(ctx, instance, map[string]string{"pvc-1": ""})

	if got := instance.Status.VolumeStatus[0].Name; got != "vol1" {
		t.Fatalf("expected first entry to remain %q, got %q", "vol1", got)
	}
	if got := instance.Status.VolumeStatus[1].Name; got != "vol1"+detachSuffix {
		t.Fatalf("expected second entry %q, got %q", "vol1"+detachSuffix, got)
	}
}

func TestUpdatePvcStatusEntryName_SkipsNonTargetPVC(t *testing.T) {
	ctx := context.TODO()

	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "pvc-1",
					},
				},
			},
		},
	}

	pvcsToDetach := map[string]string{
		"some-other-pvc": "",
	}

	updatePvcStatusEntryName(ctx, instance, pvcsToDetach)

	got := instance.Status.VolumeStatus[0].Name
	want := "vol1" // unchanged

	if got != want {
		t.Fatalf("expected volume name %q (unchanged), got %q", want, got)
	}
}

func TestUpdatePvcStatusEntryName_NoDuplicateWhenDetachingEntryExists(t *testing.T) {
	ctx := context.TODO()

	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					Name:                  "vol1" + detachSuffix,
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"},
				},
				{
					Name:                  "vol1",
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc-1"},
				},
			},
		},
	}

	updatePvcStatusEntryName(ctx, instance, map[string]string{"pvc-1": ""})

	if got := instance.Status.VolumeStatus[0].Name; got != "vol1"+detachSuffix {
		t.Fatalf("expected first entry %q, got %q", "vol1"+detachSuffix, got)
	}
	if got := instance.Status.VolumeStatus[1].Name; got != "vol1" {
		t.Fatalf("expected second entry to remain %q, got %q", "vol1", got)
	}
}

func findVolumeDetachedCondition(vol v1alpha1.VolumeStatus) *metav1.Condition {
	for i := range vol.PersistentVolumeClaim.Conditions {
		if vol.PersistentVolumeClaim.Conditions[i].Type == v1alpha1.ConditionDetached {
			return &vol.PersistentVolumeClaim.Conditions[i]
		}
	}
	return nil
}

func schemeForCnsBatchAttachTests() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = v1.AddToScheme(s)
	_ = cnsopapis.AddToScheme(s)
	return s
}

func TestRemovePvcProtectionFinalizersForTrackedPVCs_AllSucceed(t *testing.T) {
	ctx := context.Background()
	base := setupTestCnsNodeVMBatchAttachment()
	instance := base.DeepCopy()

	s := schemeForCnsBatchAttachTests()
	crClient := fake.NewClientBuilder().WithScheme(s).WithObjects(instance).WithStatusSubresource(instance).Build()
	clientset := k8sFake.NewClientset()
	cnsOperatorClient := fake.NewClientBuilder().WithScheme(s).Build()

	origRemovePvcFinalizerFn := removePvcFinalizerFn
	defer func() { removePvcFinalizerFn = origRemovePvcFinalizerFn }()
	removePvcFinalizerFn = func(ctx context.Context, patchClient crclient.Client,
		k8sClient kubernetes.Interface, cnsOpClient crclient.Client,
		pvcName, namespace, vmInstanceUUID string) error {
		assert.Equal(t, testNamespace, namespace)
		assert.Equal(t, base.Spec.InstanceUUID, vmInstanceUUID)
		assert.Contains(t, []string{"pvc-1", "pvc-2"}, pvcName)
		return nil
	}

	err := removePvcProtectionFinalizersForTrackedPVCs(ctx, instance, crClient, clientset, cnsOperatorClient)
	assert.NoError(t, err)

	for _, vol := range instance.Status.VolumeStatus {
		cond := findVolumeDetachedCondition(vol)
		assert.NotNil(t, cond, "volume %q should have %s condition", vol.Name, v1alpha1.ConditionDetached)
		assert.Equal(t, metav1.ConditionTrue, cond.Status)
	}
}

func TestRemovePvcProtectionFinalizersForTrackedPVCs_RemoveFinalizerErrorUpdatesStatus(t *testing.T) {
	ctx := context.Background()
	base := setupTestCnsNodeVMBatchAttachment()
	base.Spec.Volumes = base.Spec.Volumes[:1]
	base.Status.VolumeStatus = base.Status.VolumeStatus[:1]
	instance := base.DeepCopy()

	s := schemeForCnsBatchAttachTests()
	crClient := fake.NewClientBuilder().WithScheme(s).WithObjects(instance).WithStatusSubresource(instance).Build()
	clientset := k8sFake.NewClientset()
	cnsOperatorClient := fake.NewClientBuilder().WithScheme(s).Build()

	wantErr := errors.New("mock removePvcFinalizer failure")
	origRemovePvcFinalizerFn := removePvcFinalizerFn
	defer func() { removePvcFinalizerFn = origRemovePvcFinalizerFn }()
	removePvcFinalizerFn = func(ctx context.Context, patchClient crclient.Client,
		k8sClient kubernetes.Interface, cnsOpClient crclient.Client,
		pvcName, namespace, vmInstanceUUID string) error {
		return wantErr
	}

	err := removePvcProtectionFinalizersForTrackedPVCs(ctx, instance, crClient, clientset, cnsOperatorClient)
	assert.ErrorIs(t, err, wantErr)

	require.Len(t, instance.Status.VolumeStatus, 1)
	cond := findVolumeDetachedCondition(instance.Status.VolumeStatus[0])
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ReasonDetachFailed, cond.Reason)
}

func TestRemovePvcProtectionFinalizersForTrackedPVCs_NoTrackedPVCs(t *testing.T) {
	ctx := context.Background()
	base := setupTestCnsNodeVMBatchAttachment()
	base.Spec.Volumes = nil
	base.Status.VolumeStatus = nil
	instance := base.DeepCopy()

	s := schemeForCnsBatchAttachTests()
	crClient := fake.NewClientBuilder().WithScheme(s).WithObjects(instance).Build()
	clientset := k8sFake.NewClientset()
	cnsOperatorClient := fake.NewClientBuilder().WithScheme(s).Build()

	called := false
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyFunc(removePvcFinalizer, func(ctx context.Context, patchClient crclient.Client,
		k8sClient kubernetes.Interface, cnsOpClient crclient.Client,
		pvcName, namespace, vmInstanceUUID string) error {
		called = true
		return nil
	})

	err := removePvcProtectionFinalizersForTrackedPVCs(ctx, instance, crClient, clientset, cnsOperatorClient)
	assert.NoError(t, err)
	assert.False(t, called, "removePvcFinalizer should not run when there are no tracked PVCs")
}

func TestRemovePvcFinalizer_WhenPVCIsAlreadyDeleted(t *testing.T) {
	ctx := context.Background()

	namespace := "test-ns"
	pvcName := "not-found-error"
	vmInstanceUUID := "vm-111"

	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = cnsopapis.AddToScheme(scheme)

	// fake controller-runtime client (used by PatchFinalizers)
	crClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects().Build()

	// fake k8s clientset
	clientset := k8sFake.NewClientset()

	// fake controller-runtime client for cnsOperatorClient (empty list)
	cnsOperatorClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}
	VolumeLock = &sync.Map{}
	// ---------------------------------------
	// Run function
	err := removePvcFinalizer(ctx, crClient, clientset, cnsOperatorClient,
		pvcName, namespace, vmInstanceUUID)
	// ---------------------------------------

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestRemovePvcFinalizer_WhenPVCIsPresent(t *testing.T) {
	ctx := context.Background()

	namespace := "test-ns"
	pvcName := "mypvc"
	vmInstanceUUID := "vm-111"

	// Prepare pvc with finalizer and shared annotation
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       pvcName,
			Namespace:  namespace,
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
			Annotations: map[string]string{
				"cns.vmware.com/used-by": vmInstanceUUID,
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = cnsopapis.AddToScheme(scheme)

	// fake controller-runtime client (used by PatchFinalizers)
	crClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()

	// fake k8s clientset
	clientset := k8sFake.NewClientset(pvc)

	// fake controller-runtime client for cnsOperatorClient (empty list)
	cnsOperatorClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// ---- Monkey patches ----

	patches := gomonkey.NewPatches()
	defer patches.Reset()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	// removePvcAnnotation is expected to succeed
	patches.ApplyFunc(
		removePvcAnnotation,
		func(ctx context.Context, k8sClient interface{}, vmID string, pvcObj *v1.PersistentVolumeClaim) error {
			return nil
		})

	// pvcHasUsedByAnnotaion → no: this is last VM
	patches.ApplyFunc(
		pvcHasUsedByAnnotaion,
		func(ctx context.Context, p *v1.PersistentVolumeClaim) bool {
			return false
		})

	// PatchFinalizers: expect correct finalizers passed
	patches.ApplyFunc(
		k8s.PatchFinalizers,
		func(ctx context.Context, c crclient.Client, obj crclient.Object, finals []string) error {
			_, ok := obj.(*v1.PersistentVolumeClaim)
			if !ok {
				t.Fatalf("expected PVC object, got %T", obj)
			}

			if len(finals) != 0 {
				t.Errorf("expected finalizers to be removed, got %v", finals)
			}

			return nil
		},
	)

	VolumeLock = &sync.Map{}
	// ---------------------------------------
	// Run function
	err := removePvcFinalizer(ctx, crClient, clientset, cnsOperatorClient, pvcName, namespace, vmInstanceUUID)
	// ---------------------------------------

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
func TestGetVolumesToDetachFromInstanceWhenAVolumeIsRemoved(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"with-used-by-annotation-1": {ControllerKey: 1000, UnitNumber: 1,
			SharingMode: "None", DiskMode: "persistent"},
		"with-used-by-annotation-2": {ControllerKey: 1001, UnitNumber: 2,
			SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1001, UnitNumber: 2,
			SharingMode: "None", DiskMode: "persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"with-used-by-annotation": "with-used-by-annotation-1"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenNothingHasChanged(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1001,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1001,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenControllerKeyIsChanged(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1001,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"with-used-by-annotation": "with-used-by-annotation-2"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenUnitNumberIsChanged(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 4, SharingMode: "None", DiskMode: "persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"with-used-by-annotation": "with-used-by-annotation-2"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenSharingIsChanged(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "SharingMultiWriter",
			DiskMode: "persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"with-used-by-annotation": "with-used-by-annotation-2"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenDiskModeIsChanged(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"with-used-by-annotation-2": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None",
			DiskMode: "independent_persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"with-used-by-annotation": "with-used-by-annotation-2"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenUsedByAndPvcFinalizerIsMissing(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"no-finalizer-no-usedby-1": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"no-finalizer-no-usedby-1": {ControllerKey: 1000,
			UnitNumber: 3, SharingMode: "None",
			DiskMode: "independent_persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"no-finalizer-no-usedby-1": "no-finalizer-no-usedby-1"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenOnlyPvcFinalizerIsMissing(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"no-finalizer": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"no-finalizer": {ControllerKey: 1000,
			UnitNumber: 3, SharingMode: "None",
			DiskMode: "independent_persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{"no-finalizer-1": "no-finalizer"}, pvcsToDetach)
}

func TestGetVolumesToDetachFromInstanceWhenOnlyUsedByIsMissingFinalizerIsPresent(t *testing.T) {
	ctx := context.Background()
	instance := setupTestCnsNodeVMBatchAttachment()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCDs := map[string]FCDBackingDetails{
		"12345": {ControllerKey: 1000,
			UnitNumber: 2, SharingMode: "None", DiskMode: "persistent"},
	}
	volumeIdsInSpec := map[string]FCDBackingDetails{
		"12345": {ControllerKey: 1000,
			UnitNumber: 3, SharingMode: "None",
			DiskMode: "independent_persistent"},
	}

	detach := getVolumesToDetachFromInstance(ctx, &instance, k8sFake.NewClientset(),
		attachedFCDs, volumeIdsInSpec, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Empty(t, unresolved)
	assert.Empty(t, detach.unknownVolumes)
	assert.Equal(t, map[string]string{}, pvcsToDetach)
}

func TestGetVolumesToAttach(t *testing.T) {
	ctx := context.Background()

	var controllerKey int32 = 100
	var unitNumber int32 = 1

	makeVolSpec := func(pvcName string, sharing v1alpha1.SharingMode, disk v1alpha1.DiskMode) v1alpha1.VolumeSpec {
		return v1alpha1.VolumeSpec{
			Name: pvcName + "-vol",
			PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
				ClaimName:     pvcName,
				ControllerKey: &controllerKey,
				UnitNumber:    &unitNumber,
				SharingMode:   sharing,
				DiskMode:      disk,
			},
		}
	}

	validBacking := FCDBackingDetails{
		ControllerKey: controllerKey,
		UnitNumber:    unitNumber,
		SharingMode:   string(v1alpha1.SharingNone),
		DiskMode:      string(v1alpha1.Persistent),
	}

	tests := []struct {
		name                    string
		instance                *v1alpha1.CnsNodeVMBatchAttachment
		attachedFCDs            map[string]FCDBackingDetails
		pvcNameToVolumeIDInSpec map[string]string
		want                    map[string]string
	}{
		{
			name: "missing volumeID mapping → skipped, others still attached",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
						makeVolSpec("pvc2", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc2": "vol-2"},
			attachedFCDs:            map[string]FCDBackingDetails{},
			want:                    map[string]string{"pvc2": "vol-2"},
		},
		{
			name: "PVC in spec but not attached → attach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc1"}},
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs:            map[string]FCDBackingDetails{},
			want:                    map[string]string{"pvc1": "vol-1"},
		},
		{
			name: "backing mismatch → reattach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{ClaimName: "pvc1"}},
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs: map[string]FCDBackingDetails{
				"vol-1": {
					ControllerKey: 999, // mismatch to force reattach
					UnitNumber:    unitNumber,
					SharingMode:   string(v1alpha1.SharingNone),
					DiskMode:      string(v1alpha1.Persistent),
				},
			},
			want: map[string]string{"pvc1": "vol-1"},
		},
		{
			name: "attached and status missing → attach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs:            map[string]FCDBackingDetails{"vol-1": validBacking},
			want:                    map[string]string{"pvc1": "vol-1"},
		},
		{
			name: "attached but status has error → attach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc1",
								Conditions: []metav1.Condition{
									{
										Type:               v1alpha1.ConditionAttached,
										Status:             metav1.ConditionFalse,
										Reason:             v1alpha1.ReasonAttachFailed,
										Message:            "some-error",
										LastTransitionTime: metav1.Now(),
									},
								},
							},
						},
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs:            map[string]FCDBackingDetails{"vol-1": validBacking},
			want:                    map[string]string{"pvc1": "vol-1"},
		},
		{
			name: "attached and status OK → no attach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc1",
								Conditions: []metav1.Condition{
									{
										Type:   v1alpha1.ConditionAttached,
										Status: metav1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs:            map[string]FCDBackingDetails{"vol-1": validBacking},
			want:                    map[string]string{},
		},
		{
			name: "attached but status has error with multiple conditions → attach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc1",
								Conditions: []metav1.Condition{
									{
										Type:               v1alpha1.ConditionAttached,
										Status:             metav1.ConditionFalse,
										Reason:             v1alpha1.ReasonAttachFailed,
										Message:            "some-error-attach",
										LastTransitionTime: metav1.Now(),
									},
									{
										Type:               v1alpha1.ConditionDetached,
										Status:             metav1.ConditionFalse,
										Reason:             v1alpha1.ReasonAttachFailed,
										Message:            "some-error-detach",
										LastTransitionTime: metav1.Now(),
									},
								},
							},
						},
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs:            map[string]FCDBackingDetails{"vol-1": validBacking},
			want:                    map[string]string{"pvc1": "vol-1"},
		},
		{
			name: "attached but status has error for detach and not attach → no attach",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						makeVolSpec("pvc1", v1alpha1.SharingNone, v1alpha1.Persistent),
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc1",
								Conditions: []metav1.Condition{
									{
										Type:               v1alpha1.ConditionDetached,
										Status:             metav1.ConditionFalse,
										Reason:             v1alpha1.ReasonAttachFailed,
										Message:            "some-error-detach",
										LastTransitionTime: metav1.Now(),
									},
									{
										Type:               v1alpha1.ConditionAttached,
										Status:             metav1.ConditionTrue,
										Reason:             v1alpha1.ConditionReady,
										Message:            "",
										LastTransitionTime: metav1.Now(),
									},
								},
							},
						},
					},
				},
			},
			pvcNameToVolumeIDInSpec: map[string]string{"pvc1": "vol-1"},
			attachedFCDs:            map[string]FCDBackingDetails{"vol-1": validBacking},
			want:                    map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getVolumesToAttach(ctx,
				tc.instance,
				tc.attachedFCDs,
				nil, // volumeIdsInSpec param unused
				tc.pvcNameToVolumeIDInSpec,
			)

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("volumesToAttach mismatch: got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestIsPvcEncrypted(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			want:        false,
		},
		{
			name:        "empty map",
			annotations: map[string]string{},
			want:        false,
		},
		{
			name: "annotation present",
			annotations: map[string]string{
				PVCEncryptionClassAnnotationName: "true",
			},
			want: true,
		},
		{
			name: "other annotations present but not encryption",
			annotations: map[string]string{
				"other": "value",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPvcEncrypted(tt.annotations)
			if got != tt.want {
				t.Errorf("isPvcEncrypted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPvcsFromSpecAndStatus(t *testing.T) {
	tests := []struct {
		name     string
		instance *v1alpha1.CnsNodeVMBatchAttachment
		want     map[string]string
	}{
		{
			name: "PVCs only in spec",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						{
							Name: "vol-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName: "pvc-a",
							},
						},
						{
							Name: "vol-2",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName: "pvc-b",
							},
						},
					},
				},
			},
			want: map[string]string{
				"pvc-a": "vol-1",
				"pvc-b": "vol-2",
			},
		},
		{
			name: "PVCs only in status",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							Name: "vol-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc-x",
							},
						},
						{
							Name: "vol-2",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc-y",
							},
						},
					},
				},
			},
			want: map[string]string{
				"pvc-x": "vol-1",
				"pvc-y": "vol-2",
			},
		},
		{
			name: "PVCs in both spec and status",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						{
							Name: "vol-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName: "pvc-a",
							},
						},
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							Name: "vol-2",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc-b",
							},
						},
					},
				},
			},
			want: map[string]string{
				"pvc-a": "vol-1",
				"pvc-b": "vol-2",
			},
		},
		{
			name: "Duplicate PVCs across spec and status",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						{
							Name: "vol-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName: "pvc-dup",
							},
						},
					},
				},
				Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
					VolumeStatus: []v1alpha1.VolumeStatus{
						{
							Name: "vol-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
								ClaimName: "pvc-dup",
							},
						},
					},
				},
			},
			want: map[string]string{
				"pvc-dup": "vol-1",
			},
		},
		{
			name:     "Empty spec and status",
			instance: &v1alpha1.CnsNodeVMBatchAttachment{},
			want:     map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getPvcsFromSpecAndStatus(context.Background(), tt.instance)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getPvcsFromSpecAndStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetAllPvcsAttachedToVMSuccess(t *testing.T) {
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCD := map[string]FCDBackingDetails{
		"vol-1": {},
		"vol-2": {},
	}

	got := getAllPvcsAttachedToVM(context.Background(), attachedFCD)
	want := map[string]string{
		"mock-pvc-1": "vol-1",
		"mock-pvc":   "vol-2",
	}

	assert.Equal(t, want, got)
}

func TestGetAllPvcsAttachedToVM_EmptyInput(t *testing.T) {

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCD := map[string]FCDBackingDetails{}

	got := getAllPvcsAttachedToVM(context.Background(), attachedFCD)
	want := map[string]string{}

	assert.Equal(t, want, got)
}

func TestGetAllPvcsAttachedToVM_PVCNotFound(t *testing.T) {

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	attachedFCD := map[string]FCDBackingDetails{
		"invalid": {},
	}
	got := getAllPvcsAttachedToVM(context.Background(), attachedFCD)
	want := map[string]string{}

	assert.Equal(t, want, got)
}
func TestRemovePvcFinalizer_WhenCnsNodeVmAttachmentExists(t *testing.T) {
	ctx := context.Background()

	namespace := "test-ns"
	pvcName := "mypvc"
	vmInstanceUUID := "vm-111"

	// Prepare PVC with finalizer
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       pvcName,
			Namespace:  namespace,
			UID:        types.UID("pvc-uid-123"),
			Finalizers: []string{cnsoperatortypes.CNSPvcFinalizer},
		},
	}

	// Prepare a fake CnsNodeVmAttachment with label referencing PVC UID
	cnsAttachment := &cnsopv1.CnsNodeVmAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "attachment-1",
			Namespace: namespace,
			Labels: map[string]string{
				common.PvcUIDLabelKey: string(pvc.UID), // <-- required for matching
			},
		},
	}

	// Setup schemes
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = cnsopapis.AddToScheme(scheme)

	// Fake clients
	crClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()
	cnsOperatorClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cnsAttachment).Build()
	clientset := k8sFake.NewClientset(pvc)

	// Monkey patch removePvcAnnotation and pvcHasUsedByAnnotaion
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	patches.ApplyFunc(removePvcAnnotation, func(ctx context.Context, k8sClient interface{},
		vmID string, pvcObj *v1.PersistentVolumeClaim) error {
		return nil
	})

	patches.ApplyFunc(pvcHasUsedByAnnotaion, func(ctx context.Context, pvc *v1.PersistentVolumeClaim) bool {
		return false // last VM
	})

	VolumeLock = &sync.Map{}

	// Call removePvcFinalizer
	err := removePvcFinalizer(ctx, crClient, clientset, cnsOperatorClient, pvcName, namespace, vmInstanceUUID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify PVC still has the finalizer
	fetchedPVC, _ := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if !controllerutil.ContainsFinalizer(fetchedPVC, cnsoperatortypes.CNSPvcFinalizer) {
		t.Errorf("expected finalizer to remain because CnsNodeVmAttachment exists")
	}
}

func MockGetVMFromVcenter(ctx context.Context, nodeUUID string,
	configInfo *config.ConfigurationInfo) (*cnsvsphere.VirtualMachine, error) {
	var vm *cnsvsphere.VirtualMachine
	if nodeUUID == "test-2" {
		return vm, fmt.Errorf("some error occurred while getting VM")
	}
	if nodeUUID == "test-3" {
		return vm, cnsvsphere.ErrVMNotFound
	}
	return &cnsvsphere.VirtualMachine{}, nil
}

func TestUpdateInstanceVolume_AddsNewVolumeStatus_WhenNotFound(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{}

	updateInstanceVolumeStatus(
		context.TODO(),
		instance,
		"vol1", "pvc1",
		"vol-id-123", "uuid-456",
		nil,
		v1alpha1.ConditionAttached, "Success",
	)

	assert.Len(t, instance.Status.VolumeStatus, 1)
	vs := instance.Status.VolumeStatus[0]
	assert.Equal(t, "vol1", vs.Name)
	assert.Equal(t, "pvc1", vs.PersistentVolumeClaim.ClaimName)
	assert.Equal(t, "vol-id-123", vs.PersistentVolumeClaim.CnsVolumeID)
	assert.Equal(t, "uuid-456", vs.PersistentVolumeClaim.DiskUUID)
	assert.Equal(t, "", vs.PersistentVolumeClaim.Conditions[0].Message)
	assert.Equal(t, "True", vs.PersistentVolumeClaim.Conditions[0].Reason)
	assert.Equal(t, metav1.ConditionTrue, vs.PersistentVolumeClaim.Conditions[0].Status)
	assert.Equal(t, v1alpha1.ConditionAttached, vs.PersistentVolumeClaim.Conditions[0].Type)
}

func TestUpdateInstanceVolume_UpdatesExistingVolumeStatus(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					Name: "vol1",
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "pvc1",
					},
				},
			},
		},
	}

	updateInstanceVolumeStatus(
		context.TODO(),
		instance,
		"vol1", "pvc1",
		"new-vol-id", "new-uuid",
		nil,
		v1alpha1.ConditionAttached, "Success",
	)

	assert.Len(t, instance.Status.VolumeStatus, 1)
	vs := instance.Status.VolumeStatus[0]
	assert.Equal(t, "new-vol-id", vs.PersistentVolumeClaim.CnsVolumeID)
	assert.Equal(t, "new-uuid", vs.PersistentVolumeClaim.DiskUUID)
	assert.Equal(t, "", vs.PersistentVolumeClaim.Conditions[0].Message)
	assert.Equal(t, "True", vs.PersistentVolumeClaim.Conditions[0].Reason)
	assert.Equal(t, metav1.ConditionTrue, vs.PersistentVolumeClaim.Conditions[0].Status)
	assert.Equal(t, v1alpha1.ConditionAttached, vs.PersistentVolumeClaim.Conditions[0].Type)
}

func TestUpdateInstanceVolume_MarksErrorCondition_WhenErrorProvided(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{}

	err := errors.New("failed to attach")
	updateInstanceVolumeStatus(
		context.TODO(),
		instance,
		"vol2", "pvc2",
		"", "",
		err,
		v1alpha1.ConditionAttached, v1alpha1.ReasonAttachFailed,
	)

	assert.Len(t, instance.Status.VolumeStatus, 1)
	vs := instance.Status.VolumeStatus[0]
	assert.Equal(t, "pvc2", vs.PersistentVolumeClaim.ClaimName)
	assert.Equal(t, "failed to attach", vs.PersistentVolumeClaim.Conditions[0].Message)
	assert.Equal(t, v1alpha1.ReasonAttachFailed, vs.PersistentVolumeClaim.Conditions[0].Reason)
	assert.Equal(t, metav1.ConditionFalse, vs.PersistentVolumeClaim.Conditions[0].Status)
	assert.Equal(t, v1alpha1.ConditionAttached, vs.PersistentVolumeClaim.Conditions[0].Type)

}

func TestUpdateInstanceVolume_WhenVolumeNameIsEmpty(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{}

	updateInstanceVolumeStatus(
		context.TODO(),
		instance,
		"", "pvc-xyz",
		"vol-id-123", "uuid-456",
		nil,
		v1alpha1.ConditionAttached, "Success",
	)

	assert.Len(t, instance.Status.VolumeStatus, 0)
}

func TestUpdateInstanceVolumeStatusByPvc_SetsSuccessCondition_WhenNoError(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "mypvc",
					},
				},
			},
		},
	}

	updateInstanceVolumeStatusByPvc(
		context.TODO(),
		instance,
		"vol1",
		"mypvc",
		"vol-id-123",
		"disk-uuid-123",
		nil,
		v1alpha1.ConditionAttached,
		"",
	)

	assert.Len(t, instance.Status.VolumeStatus, 1)
	vs := instance.Status.VolumeStatus[0]

	assert.Equal(t, "mypvc", vs.PersistentVolumeClaim.ClaimName)
	assert.Equal(t, "vol-id-123", vs.PersistentVolumeClaim.CnsVolumeID)
	assert.Equal(t, "disk-uuid-123", vs.PersistentVolumeClaim.DiskUUID)

	// Condition
	assert.Len(t, vs.PersistentVolumeClaim.Conditions, 1)
	cond := vs.PersistentVolumeClaim.Conditions[0]
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1alpha1.ConditionAttached, cond.Type)
	assert.Equal(t, "True", cond.Reason)
}

func TestUpdateInstanceVolumeStatusByPvc_MarksErrorCondition_WhenErrorProvided(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "pvc2",
					},
				},
			},
		},
	}

	err := errors.New("failed to attach")

	updateInstanceVolumeStatusByPvc(
		context.TODO(),
		instance,
		"vol2",
		"pvc2",
		"",
		"",
		err,
		v1alpha1.ConditionAttached,
		v1alpha1.ReasonAttachFailed,
	)

	assert.Len(t, instance.Status.VolumeStatus, 1)
	vs := instance.Status.VolumeStatus[0]

	assert.Equal(t, "pvc2", vs.PersistentVolumeClaim.ClaimName)

	assert.Len(t, vs.PersistentVolumeClaim.Conditions, 1)
	cond := vs.PersistentVolumeClaim.Conditions[0]
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1alpha1.ConditionAttached, cond.Type)
	assert.Equal(t, v1alpha1.ReasonAttachFailed, cond.Reason)
	assert.Equal(t, "failed to attach", cond.Message)
}

func TestUpdateInstanceVolumeStatusByPvc_InitializesConditions_WhenNil(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName:  "mypvc",
						Conditions: nil, // explicitly nil
					},
				},
			},
		},
	}

	updateInstanceVolumeStatusByPvc(
		context.TODO(),
		instance,
		"vol",
		"mypvc",
		"",
		"",
		nil,
		v1alpha1.ConditionAttached,
		"",
	)

	vs := instance.Status.VolumeStatus[0]
	assert.NotNil(t, vs.PersistentVolumeClaim.Conditions)
	assert.NotEmpty(t, vs.PersistentVolumeClaim.Conditions)
}

func TestUpdateInstanceVolumeStatusByPvc_DoesNothing_WhenPVCNotFound(t *testing.T) {
	instance := &v1alpha1.CnsNodeVMBatchAttachment{
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				{
					PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
						ClaimName: "other",
					},
				},
			},
		},
	}

	// capture original snapshot
	original := instance.Status.VolumeStatus[0]

	updateInstanceVolumeStatusByPvc(
		context.TODO(),
		instance,
		"vol",
		"nonexistent-pvc",
		"new-id",
		"new-disk",
		nil,
		v1alpha1.ConditionAttached,
		"",
	)

	// Expect unchanged
	assert.Equal(t, original, instance.Status.VolumeStatus[0])
}

func TestConstructBatchAttachRequest(t *testing.T) {
	ctx := context.Background()

	volumeEncryptedFalse := false
	tests := []struct {
		name               string
		volumesToAttach    map[string]string
		instance           *v1alpha1.CnsNodeVMBatchAttachment
		expectedUnresolved []string
		expectedPvcs       map[string]string
		expectedVolumeIDs  map[string]string
		expectedRequests   []volumes.BatchAttachRequest
	}{
		{
			name: "successfully construct batch attach request",
			volumesToAttach: map[string]string{
				"with-sparse-backing-type": "vol-id-1",
			},
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-instance",
					Namespace: "test-ns",
				},
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						{
							Name: "volume-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName:     "with-sparse-backing-type",
								SharingMode:   v1alpha1.SharingNone,
								DiskMode:      v1alpha1.IndependentPersistent,
								ControllerKey: vim25types.NewInt32(1000),
								UnitNumber:    vim25types.NewInt32(1),
							},
						},
					},
				},
			},
			expectedPvcs: map[string]string{
				"with-sparse-backing-type": "volume-1",
			},
			expectedVolumeIDs: map[string]string{
				"vol-id-1": "with-sparse-backing-type",
			},
			expectedRequests: []volumes.BatchAttachRequest{
				{
					VolumeID:        "vol-id-1",
					SharingMode:     string(v1alpha1.SharingNone),
					DiskMode:        string(v1alpha1.IndependentPersistent),
					ControllerKey:   vim25types.NewInt32(1000),
					UnitNumber:      vim25types.NewInt32(1),
					BackingType:     "Sparse",
					VolumeEncrypted: &volumeEncryptedFalse,
				},
			},
		},
		{
			name: "successfully construct batch attach request when backingdisktype is missing",
			volumesToAttach: map[string]string{
				"pvc-1": "vol-id-1",
			},
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-instance",
					Namespace: "test-ns",
				},
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						{
							Name: "volume-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName:     "pvc-1",
								SharingMode:   v1alpha1.SharingNone,
								DiskMode:      v1alpha1.IndependentPersistent,
								ControllerKey: vim25types.NewInt32(1000),
								UnitNumber:    vim25types.NewInt32(1),
							},
						},
					},
				},
			},
			expectedPvcs: map[string]string{
				"pvc-1": "volume-1",
			},
			expectedVolumeIDs: map[string]string{
				"vol-id-1": "pvc-1",
			},
			expectedRequests: []volumes.BatchAttachRequest{
				{
					VolumeID:        "vol-id-1",
					SharingMode:     string(v1alpha1.SharingNone),
					DiskMode:        string(v1alpha1.IndependentPersistent),
					ControllerKey:   vim25types.NewInt32(1000),
					UnitNumber:      vim25types.NewInt32(1),
					BackingType:     string(cnstypes.CnsVolumeBackingTypeFlatVer2BackingInfo),
					VolumeEncrypted: &volumeEncryptedFalse,
				},
			},
		},
		{
			name: "PVC object not found is left out and the other PVC is still in the request",
			volumesToAttach: map[string]string{
				"pvc-1":           "vol-id-1",
				"not-found-error": "vol-id-2",
			},
			instance: &v1alpha1.CnsNodeVMBatchAttachment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-instance",
					Namespace: "test-ns",
				},
				Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
					Volumes: []v1alpha1.VolumeSpec{
						{
							Name: "volume-1",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName:     "pvc-1",
								SharingMode:   v1alpha1.SharingNone,
								DiskMode:      v1alpha1.IndependentPersistent,
								ControllerKey: vim25types.NewInt32(1000),
								UnitNumber:    vim25types.NewInt32(1),
							},
						},
						{
							Name: "volume-2",
							PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
								ClaimName:     "not-found-error",
								SharingMode:   v1alpha1.SharingNone,
								DiskMode:      v1alpha1.IndependentPersistent,
								ControllerKey: vim25types.NewInt32(1000),
								UnitNumber:    vim25types.NewInt32(2),
							},
						},
					},
				},
			},
			expectedUnresolved: []string{"not-found-error"},
			expectedPvcs: map[string]string{
				"pvc-1": "volume-1",
			},
			expectedVolumeIDs: map[string]string{
				"vol-id-1": "pvc-1",
			},
			expectedRequests: []volumes.BatchAttachRequest{
				{
					VolumeID:        "vol-id-1",
					SharingMode:     string(v1alpha1.SharingNone),
					DiskMode:        string(v1alpha1.IndependentPersistent),
					ControllerKey:   vim25types.NewInt32(1000),
					UnitNumber:      vim25types.NewInt32(1),
					BackingType:     string(cnstypes.CnsVolumeBackingTypeFlatVer2BackingInfo),
					VolumeEncrypted: &volumeEncryptedFalse,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

			batch := constructBatchAttachRequest(ctx, tt.volumesToAttach, tt.instance)
			pvcs, volumeIDs, requests, unresolved := batch.pvcsInSpec, batch.volumeIdsInSpec,
				batch.batchAttachRequest, batch.unresolvedPVCs

			assert.ElementsMatch(t, tt.expectedUnresolved, slices.Collect(maps.Keys(unresolved)))
			for _, pvcErr := range unresolved {
				assert.True(t, apierrors.IsNotFound(pvcErr), "the PVC lookup error must be kept: %v", pvcErr)
			}

			if !reflect.DeepEqual(pvcs, tt.expectedPvcs) {
				t.Errorf("pvcsInSpec = %v, want %v", pvcs, tt.expectedPvcs)
			}

			if !reflect.DeepEqual(volumeIDs, tt.expectedVolumeIDs) {
				t.Errorf("volumeIdsInSpec = %v, want %v", volumeIDs, tt.expectedVolumeIDs)
			}

			assert.EqualValues(t, tt.expectedRequests, requests)

		})
	}
}

func TestIsVmInSameNamespace_Found(t *testing.T) {
	ctx := context.Background()

	namespace := "test-ns"
	instanceUUID := "uuid-123"

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vm",
			Namespace: namespace,
		},
		Status: vmoperatortypes.VirtualMachineStatus{
			InstanceUUID: instanceUUID,
		},
	}

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	vmClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vm).
		Build()

	found, err := isVmInSameNamespace(ctx, vmClient, "test-vm", instanceUUID, namespace)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !found {
		t.Fatalf("expected VM to be found, got false")
	}
}

func TestIsVmInSameNamespace_NotFound(t *testing.T) {
	ctx := context.Background()

	namespace := "test-ns"
	instanceUUID := "missing-uuid"

	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-vm",
			Namespace: namespace,
		},
		Status: vmoperatortypes.VirtualMachineStatus{
			InstanceUUID: "different-uuid",
		},
	}

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	vmClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(vm).
		Build()

	// "other-vm" exists but its UUID ("different-uuid") does not match
	// "missing-uuid", so the function must return false.
	found, err := isVmInSameNamespace(ctx, vmClient, "other-vm", instanceUUID, namespace)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if found {
		t.Fatalf("expected VM not to be found, got true")
	}
}

// batchTrackingMockVolumeManager records CBT flag calls for SyncVolumeCBTState tests.
type batchTrackingMockVolumeManager struct {
	unittestcommon.MockVolumeManager
	setCalled   bool
	clearCalled bool
}

func (m *batchTrackingMockVolumeManager) SetVolumeControlFlags(ctx context.Context,
	volumeID string, controlFlags []string) error {
	m.setCalled = true
	return nil
}

func (m *batchTrackingMockVolumeManager) ClearVolumeControlFlags(ctx context.Context,
	volumeID string, controlFlags []string) error {
	m.clearCalled = true
	return nil
}

type sharedDiskBatchVolumeManager struct {
	unittestcommon.MockVolumeManager
	enabledIDs  []string
	disabledIDs []string
	attachedIDs []string
}

func (m *sharedDiskBatchVolumeManager) SetVolumeControlFlags(ctx context.Context,
	volumeID string, controlFlags []string) error {
	m.enabledIDs = append(m.enabledIDs, volumeID)
	return nil
}

func (m *sharedDiskBatchVolumeManager) ClearVolumeControlFlags(ctx context.Context,
	volumeID string, controlFlags []string) error {
	m.disabledIDs = append(m.disabledIDs, volumeID)
	return nil
}

func (m *sharedDiskBatchVolumeManager) BatchAttachVolumes(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, requests []volumes.BatchAttachRequest) ([]volumes.BatchAttachResult, string, error) {
	for _, request := range requests {
		m.attachedIDs = append(m.attachedIDs, request.VolumeID)
	}
	return nil, "", nil
}

func TestProcessBatchAttachSkipsCBTForSharedDisks(t *testing.T) {
	ctx := context.Background()
	originalCO := commonco.ContainerOrchestratorUtility
	originalBackupEnabled := isCSIBackupAPIEnabled
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		isCSIBackupAPIEnabled = originalBackupEnabled
	})
	isCSIBackupAPIEnabled = true

	for _, state := range []cbtconfigv1alpha1.CBTState{
		cbtconfigv1alpha1.CBTStateActive, cbtconfigv1alpha1.CBTStateInactive,
	} {
		for _, sharingModes := range [][]v1alpha1.SharingMode{
			{v1alpha1.SharingMultiWriter, v1alpha1.SharingMultiWriter},
			{v1alpha1.SharingMultiWriter, v1alpha1.SharingNone},
			{v1alpha1.SharingNone, ""},
		} {
			t.Run(fmt.Sprintf("%s/%v", state, sharingModes), func(t *testing.T) {
				instance := setupTestCnsNodeVMBatchAttachment()
				orchestrator, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
				require.NoError(t, err)
				commonco.ContainerOrchestratorUtility = orchestrator
				var pvcs []*v1.PersistentVolumeClaim
				toAttach := make(map[string]string)
				var expectedCBTIDs, expectedAttachIDs []string
				for i := range instance.Spec.Volumes {
					claim := &instance.Spec.Volumes[i].PersistentVolumeClaim
					claim.SharingMode = sharingModes[i]
					volumeID := fmt.Sprintf("vol-%d", i)
					toAttach[claim.ClaimName] = volumeID
					pvcs = append(pvcs, &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
						Name: claim.ClaimName, Namespace: instance.Namespace,
					}})
					expectedAttachIDs = append(expectedAttachIDs, volumeID)
					if sharingModes[i] != v1alpha1.SharingMultiWriter {
						expectedCBTIDs = append(expectedCBTIDs, volumeID)
					}
				}
				orchestrator.(*unittestcommon.FakeK8SOrchestrator).SetPVCs(pvcs)
				s := runtime.NewScheme()
				require.NoError(t, vmoperatortypes.AddToScheme(s))
				vm := &vmoperatortypes.VirtualMachine{
					ObjectMeta: metav1.ObjectMeta{Name: instance.Name, Namespace: instance.Namespace},
					Status:     vmoperatortypes.VirtualMachineStatus{InstanceUUID: instance.Spec.InstanceUUID},
				}
				manager := &sharedDiskBatchVolumeManager{}
				r := &Reconciler{
					vmOperatorClient: fake.NewClientBuilder().WithScheme(s).WithObjects(vm).Build(),
					volumeManager:    manager,
					cbtClient: newBatchCBTTestClient(t,
						batchCBTConfigObject("default", instance.Namespace, &state)),
				}
				require.NoError(t, r.processBatchAttach(ctx, k8sFake.NewSimpleClientset(), nil, &instance, toAttach))
				assert.ElementsMatch(t, expectedAttachIDs, manager.attachedIDs)
				if state == cbtconfigv1alpha1.CBTStateActive {
					assert.ElementsMatch(t, expectedCBTIDs, manager.enabledIDs)
					assert.Empty(t, manager.disabledIDs)
				} else {
					assert.ElementsMatch(t, expectedCBTIDs, manager.disabledIDs)
					assert.Empty(t, manager.enabledIDs)
				}
			})
		}
	}
}

// newBatchCBTTestClient returns a crclient.Client pre-populated with the given CBTConfig objects.
func newBatchCBTTestClient(t *testing.T, objs ...crclient.Object) crclient.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := cbtconfigv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add CBTConfig scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

// batchCBTConfigObject builds a typed CBTConfig CR for use with crclient/fake.
func batchCBTConfigObject(name, namespace string,
	statusState *cbtconfigv1alpha1.CBTState) *cbtconfigv1alpha1.CBTConfig {
	specState := cbtconfigv1alpha1.CBTStateActive
	obj := &cbtconfigv1alpha1.CBTConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       cbtconfigv1alpha1.CBTConfigSpec{State: specState},
	}
	if statusState != nil {
		obj.Status = &cbtconfigv1alpha1.CBTConfigStatus{State: statusState}
	}
	return obj
}

// TestSyncCBTBeforeBatchAttach_NoCBTConfig skips flag changes when CBTStateForNamespace
// reports no CBTConfig CR in the namespace (configured=false).
func TestSyncCBTBeforeBatchAttach_NoCBTConfigSkipsFlagChange(t *testing.T) {
	ctx := context.Background()

	mockVM := &batchTrackingMockVolumeManager{}
	c := newBatchCBTTestClient(t)
	common.SyncVolumeCBTState(ctx, c, "unknown-ns", mockVM, "vol-1", "vol-2")

	assert.False(t, mockVM.setCalled)
	assert.False(t, mockVM.clearCalled)
}

func TestSyncCBTBeforeBatchAttach_CBTActive(t *testing.T) {
	ctx := context.Background()

	active := cbtconfigv1alpha1.CBTStateActive
	c := newBatchCBTTestClient(t, batchCBTConfigObject("default", "ns-cbt", &active))
	mockVM := &batchTrackingMockVolumeManager{}
	common.SyncVolumeCBTState(ctx, c, "ns-cbt", mockVM, "vol-a", "vol-b")

	assert.True(t, mockVM.setCalled)
	assert.False(t, mockVM.clearCalled)
}

func TestSyncCBTBeforeBatchAttach_CBTInactive(t *testing.T) {
	ctx := context.Background()

	inactive := cbtconfigv1alpha1.CBTStateInactive
	c := newBatchCBTTestClient(t, batchCBTConfigObject("default", "ns-off", &inactive))
	mockVM := &batchTrackingMockVolumeManager{}
	common.SyncVolumeCBTState(ctx, c, "ns-off", mockVM, "vol-x", "vol-y")

	assert.False(t, mockVM.setCalled)
	assert.True(t, mockVM.clearCalled)
}

// TestSyncCBTBeforeBatchAttach_ClientNil verifies that a nil cbtClient is a no-op.
func TestSyncCBTBeforeBatchAttach_ClientNil(t *testing.T) {
	ctx := context.Background()

	mockVM := &batchTrackingMockVolumeManager{}
	r := &Reconciler{
		volumeManager: mockVM,
		cbtClient:     nil,
	}

	// Guard in the call site: nil client means SyncVolumeCBTState is never called.
	if r.cbtClient != nil {
		common.SyncVolumeCBTState(ctx, r.cbtClient, "any-ns", mockVM, "vol-1", "vol-2")
	}

	assert.False(t, mockVM.setCalled)
	assert.False(t, mockVM.clearCalled)
}

// terminalFaultMockVolumeManager fails DetachVolume with a terminal error for the volume IDs
// in terminalIDs, with a transient error for the IDs in failedIDs, and succeeds otherwise.
type terminalFaultMockVolumeManager struct {
	unittestcommon.MockVolumeManager
	terminalIDs map[string]bool
	failedIDs   map[string]bool
}

func (m *terminalFaultMockVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	if m.terminalIDs[volumeID] {
		return "vim.fault.InvalidDeviceSpec", &volumes.TerminalError{
			Cause: "the disk is part of a VM snapshot",
			Err:   fmt.Errorf("failed to detach cns volume: %q", volumeID),
		}
	}
	if m.failedIDs[volumeID] {
		return "vim.fault.InvalidDeviceSpec", fmt.Errorf("failed to detach cns volume: %q", volumeID)
	}
	return "", nil
}

func testVM() *cnsvsphere.VirtualMachine {
	return &cnsvsphere.VirtualMachine{VirtualMachine: &object.VirtualMachine{}}
}

func TestProcessDetachWhenAllDetachesFailWithTerminalFaults(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	r.volumeManager = &terminalFaultMockVolumeManager{terminalIDs: map[string]bool{"123-456": true}}
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
		&testCnsNodeVMBatchAttachment, map[string]string{"pvc-1": "123-456"})
	require.Error(t, err)
	assert.True(t, volumes.IsTerminalError(err))
	assert.Equal(t, map[string]bool{"pvc-1": true}, failed)

	var cond *metav1.Condition
	for _, vol := range testCnsNodeVMBatchAttachment.Status.VolumeStatus {
		if vol.PersistentVolumeClaim.ClaimName == "pvc-1" {
			cond = findVolumeDetachedCondition(vol)
		}
	}
	require.NotNil(t, cond, "expected a VolumeDetached condition for pvc-1")
	assert.Equal(t, v1alpha1.ReasonDetachFailedTerminal, cond.Reason)
}

func TestProcessDetachWhenOnlySomeDetachesFailWithTerminalFaults(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	r.volumeManager = &terminalFaultMockVolumeManager{
		terminalIDs: map[string]bool{"123-456": true},
		failedIDs:   map[string]bool{"789-012": true},
	}
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	failed, err := r.processDetach(context.TODO(), getClientSetWithPvc(), testVM(),
		&testCnsNodeVMBatchAttachment, map[string]string{"pvc-1": "123-456", "pvc-2": "789-012"})
	require.Error(t, err)
	assert.False(t, volumes.IsTerminalError(err),
		"the reconcile must be retried when some detaches failed with transient faults")
	assert.Equal(t, map[string]bool{"pvc-1": true, "pvc-2": true}, failed)
}

func TestReconcileWithoutDeletionTimestampSkipsAttachAfterTerminalDetachFailure(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	r.volumeManager = &terminalFaultMockVolumeManager{terminalIDs: map[string]bool{"123-456": true}}
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	// getVolumesToDetachForVmFromVC has already renamed the entry of the PVC being detached.
	testCnsNodeVMBatchAttachment.Status.VolumeStatus[0].Name = "disk-1" + detachSuffix

	volumesToAttach := map[string]string{"pvc-1": "123-456"}
	err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(), getClientSetWithPvc(),
		&testCnsNodeVMBatchAttachment, map[string]string{"pvc-1": "123-456"}, volumesToAttach, testVM())
	require.Error(t, err)
	assert.True(t, volumes.IsTerminalError(err), "a skipped attach must not make the error non-terminal")
	assert.Empty(t, volumesToAttach, "attach must be skipped for a PVC whose detach failed with a terminal fault")

	// The skipped attach must be visible under the spec volume name, where readers such as
	// VM Operator look it up. The detach error stays on the detaching entry.
	var attachedCond, detachedCond *metav1.Condition
	for _, vol := range testCnsNodeVMBatchAttachment.Status.VolumeStatus {
		switch vol.Name {
		case "disk-1":
			assert.Equal(t, "pvc-1", vol.PersistentVolumeClaim.ClaimName)
			for i, cond := range vol.PersistentVolumeClaim.Conditions {
				if cond.Type == v1alpha1.ConditionAttached {
					attachedCond = &vol.PersistentVolumeClaim.Conditions[i]
				}
			}
		case "disk-1" + detachSuffix:
			detachedCond = findVolumeDetachedCondition(vol)
		}
	}
	require.NotNil(t, attachedCond, "expected an Attached condition under the spec volume name")
	assert.Equal(t, metav1.ConditionFalse, attachedCond.Status)
	assert.Equal(t, v1alpha1.ReasonAttachFailed, attachedCond.Reason)
	require.NotNil(t, detachedCond, "expected the detach error on the detaching entry")
	assert.Equal(t, v1alpha1.ReasonDetachFailedTerminal, detachedCond.Reason)
}

func TestCompleteReconciliationWithErrorRequeuesTerminalErrorsAfterMaxBackOff(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	namespacedName := types.NamespacedName{Name: testCnsNodeVMBatchAttachment.Name,
		Namespace: testCnsNodeVMBatchAttachment.Namespace}
	terminalErr := &volumes.TerminalError{Cause: "the disk is part of a VM snapshot",
		Err: errors.New("failed to detach volumes: pvc-1")}

	res, reconcileErr := r.completeReconciliationWithError(context.TODO(), &testCnsNodeVMBatchAttachment,
		namespacedName, time.Second, errors.Join(nil, terminalErr))
	require.NoError(t, reconcileErr)
	assert.Equal(t, reconcile.Result{RequeueAfter: cnsoperatortypes.MaxBackOffDurationForReconciler}, res,
		"terminal errors must be requeued after the max backoff")

	recorder := r.recorder.(*record.FakeRecorder)
	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, terminalFaultEventReason)
	default:
		t.Fatal("expected a terminal fault event")
	}

	// A terminal error joined with a transient one must use the regular backoff.
	res, reconcileErr = r.completeReconciliationWithError(context.TODO(), &testCnsNodeVMBatchAttachment,
		namespacedName, time.Second, errors.Join(errors.New("attach failed"), terminalErr))
	require.NoError(t, reconcileErr)
	assert.Equal(t, time.Second, res.RequeueAfter)

	// An instance being deleted is requeued after the max backoff too.
	now := metav1.Now()
	testCnsNodeVMBatchAttachment.DeletionTimestamp = &now
	res, reconcileErr = r.completeReconciliationWithError(context.TODO(), &testCnsNodeVMBatchAttachment,
		namespacedName, time.Second, terminalErr)
	require.NoError(t, reconcileErr)
	assert.Equal(t, cnsoperatortypes.MaxBackOffDurationForReconciler, res.RequeueAfter)
}

// When the status update fails, a terminal error must still be requeued after the max backoff,
// so that the failing operations are not issued again only to retry the status update.
func TestCompleteReconciliationWithErrorStatusUpdateFailure(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	namespacedName := types.NamespacedName{Name: testCnsNodeVMBatchAttachment.Name,
		Namespace: testCnsNodeVMBatchAttachment.Namespace}
	terminalErr := &volumes.TerminalError{Cause: "the disk is part of a VM snapshot",
		Err: errors.New("failed to detach volumes: pvc-1")}

	// A stale resourceVersion makes the status update fail with a conflict.
	stale := testCnsNodeVMBatchAttachment.DeepCopy()
	stale.ResourceVersion = "999"
	res, reconcileErr := r.completeReconciliationWithError(context.TODO(), stale,
		namespacedName, time.Second, terminalErr)
	require.NoError(t, reconcileErr)
	assert.Equal(t, cnsoperatortypes.MaxBackOffDurationForReconciler, res.RequeueAfter)

	// A transient error still uses the regular backoff.
	stale = testCnsNodeVMBatchAttachment.DeepCopy()
	stale.ResourceVersion = "999"
	res, reconcileErr = r.completeReconciliationWithError(context.TODO(), stale,
		namespacedName, time.Second, errors.New("attach failed"))
	require.NoError(t, reconcileErr)
	assert.Equal(t, time.Second, res.RequeueAfter)
}

// When processDetach fails before detaching anything, for example on the VM namespace check,
// a PVC whose backing changed is still attached with its old backing, so its attach with the
// new backing must be skipped too.
func TestReconcileWithoutDeletionTimestampSkipsAttachWhenDetachFailsEarly(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	// No VM with this instance UUID exists in the namespace, so the namespace check fails.
	testCnsNodeVMBatchAttachment.Spec.InstanceUUID = "uuid-not-in-namespace"
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	mockVolumeManager := &detachTrackingMockVolumeManager{}
	r.volumeManager = mockVolumeManager
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	volumesToAttach := map[string]string{"pvc-1": "123-456"}
	err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(), getClientSetWithPvc(),
		&testCnsNodeVMBatchAttachment, map[string]string{"pvc-1": "123-456"}, volumesToAttach, testVM())
	require.Error(t, err)
	assert.False(t, volumes.IsTerminalError(err))
	assert.Empty(t, mockVolumeManager.detachedIDs, "DetachVolume must not be called")
	assert.Empty(t, volumesToAttach, "attach must be skipped for a PVC which was not detached")
}

func TestReconcileWithoutDeletionTimestampSkipsAttachAfterTransientDetachFailure(t *testing.T) {
	testCnsNodeVMBatchAttachment := setupTestCnsNodeVMBatchAttachment()
	r := setTestEnvironment(&testCnsNodeVMBatchAttachment, false)
	r.volumeManager = &terminalFaultMockVolumeManager{failedIDs: map[string]bool{"123-456": true}}
	commonco.ContainerOrchestratorUtility = &unittestcommon.FakeK8SOrchestrator{}

	volumesToAttach := map[string]string{"pvc-1": "123-456"}
	err := r.reconcileInstanceWithoutDeletionTimestamp(context.TODO(), getClientSetWithPvc(),
		&testCnsNodeVMBatchAttachment, map[string]string{"pvc-1": "123-456"}, volumesToAttach, testVM())
	require.Error(t, err)
	assert.False(t, volumes.IsTerminalError(err))
	assert.Empty(t, volumesToAttach, "attach must be skipped for a PVC whose detach failed")
}

// pvcLookupOrchestrator is a FakeK8SOrchestrator whose PVC lookups are backed by maps, so that tests
// can make individual PVCs unresolvable.
type pvcLookupOrchestrator struct {
	*unittestcommon.FakeK8SOrchestrator
	// volumeIDs maps the namespace/name of a PVC to the volumeID of the PV bound to it.
	volumeIDs map[string]string
	// pvcNames maps a volumeID to the namespace/name of the PVC its PV is bound to.
	pvcNames map[string]string
	// pvcs are the PVC objects in the informer cache, by namespace/name.
	pvcs map[string]*v1.PersistentVolumeClaim
}

func (o *pvcLookupOrchestrator) GetVolumeIDFromPVCName(namespace string, pvcName string) (string, bool) {
	volumeID, ok := o.volumeIDs[namespace+"/"+pvcName]
	return volumeID, ok
}

func (o *pvcLookupOrchestrator) GetPVCNameFromCSIVolumeID(volumeID string) (string, string, bool) {
	namespacedName, ok := o.pvcNames[volumeID]
	if !ok {
		return "", "", false
	}
	namespace, name, _ := strings.Cut(namespacedName, "/")
	return name, namespace, true
}

func (o *pvcLookupOrchestrator) GetPvcObjectByName(ctx context.Context, pvcName string,
	namespace string) (*v1.PersistentVolumeClaim, error) {
	if pvc, ok := o.pvcs[namespace+"/"+pvcName]; ok {
		return pvc, nil
	}
	return nil, apierrors.NewNotFound(v1.Resource("persistentvolumeclaims"), pvcName)
}

// usePvcLookupOrchestrator installs a pvcLookupOrchestrator for the duration of the test.
func usePvcLookupOrchestrator(t *testing.T, o *pvcLookupOrchestrator) {
	t.Helper()
	original := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = original })
	o.FakeK8SOrchestrator = &unittestcommon.FakeK8SOrchestrator{}
	commonco.ContainerOrchestratorUtility = o
}

// stubAttachedFCDs makes listAttachedFcdsForVMFn return fcds for the duration of the test.
func stubAttachedFCDs(t *testing.T, fcds map[string]FCDBackingDetails) {
	t.Helper()
	original := listAttachedFcdsForVMFn
	t.Cleanup(func() { listAttachedFcdsForVMFn = original })
	listAttachedFcdsForVMFn = func(ctx context.Context,
		vm *cnsvsphere.VirtualMachine) (map[string]FCDBackingDetails, error) {
		return fcds, nil
	}
}

// batchVolumeSpec returns a spec entry attaching pvc as a non-shared persistent disk.
func batchVolumeSpec(volumeName, pvc string, unitNumber int32) v1alpha1.VolumeSpec {
	return v1alpha1.VolumeSpec{
		Name: volumeName,
		PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimSpec{
			ClaimName:     pvc,
			ControllerKey: vim25types.NewInt32(1000),
			UnitNumber:    vim25types.NewInt32(unitNumber),
			SharingMode:   v1alpha1.SharingNone,
			DiskMode:      v1alpha1.Persistent,
		},
	}
}

// batchBacking returns the backing of a disk attached as described by batchVolumeSpec.
func batchBacking(unitNumber int32) FCDBackingDetails {
	return FCDBackingDetails{ControllerKey: 1000, UnitNumber: unitNumber,
		SharingMode: string(v1alpha1.SharingNone), DiskMode: string(v1alpha1.Persistent)}
}

// attachedVolumeStatus returns the status entry of a successfully attached volume.
func attachedVolumeStatus(volumeName, pvc, volumeID string) v1alpha1.VolumeStatus {
	return v1alpha1.VolumeStatus{
		Name: volumeName,
		PersistentVolumeClaim: v1alpha1.PersistentVolumeClaimStatus{
			ClaimName:   pvc,
			CnsVolumeID: volumeID,
			Conditions: []metav1.Condition{{Type: v1alpha1.ConditionAttached, Status: metav1.ConditionTrue,
				Reason: v1alpha1.ConditionAttached, LastTransitionTime: metav1.Now()}},
		},
	}
}

// batchTestPVC returns an RWO PVC in testNamespace, used by the VM under test if usedBy is set.
func batchTestPVC(name string, usedBy bool, finalizers ...string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Finalizers: finalizers},
		Spec:       v1.PersistentVolumeClaimSpec{AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}},
	}
	if usedBy {
		pvc.Annotations = map[string]string{cnsoperatortypes.UsedByVMAnnotationPrefix + "test-uuid": ""}
	}
	return pvc
}

// findVolumeStatus returns the status entry with the given volume name, or nil.
func findVolumeStatus(instance *v1alpha1.CnsNodeVMBatchAttachment, volumeName string) *v1alpha1.VolumeStatus {
	for i := range instance.Status.VolumeStatus {
		if instance.Status.VolumeStatus[i].Name == volumeName {
			return &instance.Status.VolumeStatus[i]
		}
	}
	return nil
}

// assertVolumeCondition asserts that the status entry with the given volume name is for pvc and has
// a condition of the given type with the given status and reason, whose message contains msg.
func assertVolumeCondition(t *testing.T, instance *v1alpha1.CnsNodeVMBatchAttachment, volumeName, pvc string,
	conditionType string, status metav1.ConditionStatus, reason, msg string) {
	t.Helper()
	volume := findVolumeStatus(instance, volumeName)
	require.NotNil(t, volume, "expected a status entry %s", volumeName)
	assert.Equal(t, pvc, volume.PersistentVolumeClaim.ClaimName)
	for _, cond := range volume.PersistentVolumeClaim.Conditions {
		if cond.Type != conditionType {
			continue
		}
		assert.Equal(t, status, cond.Status, "status of %s on %s", conditionType, volumeName)
		if reason != "" {
			assert.Equal(t, reason, cond.Reason, "reason of %s on %s", conditionType, volumeName)
		}
		assert.Contains(t, cond.Message, msg, "message of %s on %s", conditionType, volumeName)
		return
	}
	t.Errorf("expected a %s condition on status entry %s", conditionType, volumeName)
}

// recordingVolumeManager records the volumes it attaches and detaches, and reports success.
type recordingVolumeManager struct {
	unittestcommon.MockVolumeManager
	attachedIDs []string
	detachedIDs []string
}

func (m *recordingVolumeManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	volumeID string) (string, error) {
	m.detachedIDs = append(m.detachedIDs, volumeID)
	return "", nil
}

func (m *recordingVolumeManager) BatchAttachVolumes(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	requests []volumes.BatchAttachRequest) ([]volumes.BatchAttachResult, string, error) {
	results := make([]volumes.BatchAttachResult, 0, len(requests))
	for _, request := range requests {
		m.attachedIDs = append(m.attachedIDs, request.VolumeID)
		results = append(results, volumes.BatchAttachResult{VolumeID: request.VolumeID,
			DiskUUID: "disk-uuid-" + request.VolumeID})
	}
	return results, "", nil
}

// vmOperatorCacheMissText is the text by which VM Operator detects that a PVC in the spec of an
// instance has no volumeID yet (csiErrFindFailPrefix in vm-operator pkg/util/vmopv1/storage.go).
// VM Operator looks for it in the messages of the instance conditions and of the volume conditions.
const vmOperatorCacheMissText = "failed to find volumeID for PVC"

// setupUnresolvablePVCReconcile sets up the reconcile of an instance whose spec has pvc-1, which
// is attached, pvc-3, which does not exist, and pvc-4, which is to be attached. pvc-2 was
// removed from spec and is to be detached.
func setupUnresolvablePVCReconcile(t *testing.T, volumeManager volumes.Manager) (*Reconciler,
	reconcile.Request, *k8sFake.Clientset) {
	t.Helper()
	instance := v1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: testCnsNodeVMBatchAttachmentName, Namespace: testNamespace,
			ResourceVersion: "1", Finalizers: []string{cnsoperatortypes.CNSFinalizer}},
		Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
			InstanceUUID: "test-uuid",
			Volumes: []v1alpha1.VolumeSpec{
				batchVolumeSpec("disk-1", "pvc-1", 1),
				batchVolumeSpec("disk-3", "pvc-3", 3),
				batchVolumeSpec("disk-4", "pvc-4", 4),
			},
		},
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				attachedVolumeStatus("disk-1", "pvc-1", "vol-1"),
				attachedVolumeStatus("disk-2", "pvc-2", "vol-2"),
			},
		},
	}
	r := setTestEnvironment(&instance, false)
	r.volumeManager = volumeManager

	usePvcLookupOrchestrator(t, &pvcLookupOrchestrator{
		volumeIDs: map[string]string{"test-ns/pvc-1": "vol-1", "test-ns/pvc-2": "vol-2", "test-ns/pvc-4": "vol-4"},
		pvcNames:  map[string]string{"vol-1": "test-ns/pvc-1", "vol-2": "test-ns/pvc-2", "vol-4": "test-ns/pvc-4"},
		pvcs: map[string]*v1.PersistentVolumeClaim{
			"test-ns/pvc-1": batchTestPVC("pvc-1", true, cnsoperatortypes.CNSPvcFinalizer),
			"test-ns/pvc-2": batchTestPVC("pvc-2", true),
			"test-ns/pvc-4": batchTestPVC("pvc-4", false),
		},
	})
	stubAttachedFCDs(t, map[string]FCDBackingDetails{"vol-1": batchBacking(1), "vol-2": batchBacking(2)})
	clientset := k8sFake.NewClientset(batchTestPVC("pvc-2", true),
		batchTestPVC("pvc-4", false, cnsoperatortypes.CNSPvcFinalizer))
	originalNewClientFunc, originalGetVMFromVcenter := newClientFunc, GetVMFromVcenter
	t.Cleanup(func() { newClientFunc, GetVMFromVcenter = originalNewClientFunc, originalGetVMFromVcenter })
	newClientFunc = func(ctx context.Context) (kubernetes.Interface, error) { return clientset, nil }
	GetVMFromVcenter = func(ctx context.Context, nodeUUID string,
		configInfo *config.ConfigurationInfo) (*cnsvsphere.VirtualMachine, error) {
		return testVM(), nil
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}}
	return r, req, clientset
}

// readyCondition returns the Ready condition of the given instance, or nil.
func readyCondition(instance *v1alpha1.CnsNodeVMBatchAttachment) *metav1.Condition {
	for i, cond := range instance.Status.Conditions {
		if cond.Type == v1alpha1.ConditionReady {
			return &instance.Status.Conditions[i]
		}
	}
	return nil
}

// A spec PVC whose volumeID cannot be found, for example because it was not created yet, must not
// block the reconcile: the volume removed from spec is still detached and the new volume attached.
// The error is recorded in the status of that PVC, and the instance is requeued to retry it.
func TestReconcileDoesNotBlockOnUnresolvablePVC(t *testing.T) {
	ctx := context.TODO()
	volumeManager := &recordingVolumeManager{}
	r, req, clientset := setupUnresolvablePVCReconcile(t, volumeManager)

	res, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, reconcile.Result{RequeueAfter: time.Second}, res,
		"the instance must be requeued with the regular backoff to retry the unresolvable PVC")

	assert.Equal(t, []string{"vol-2"}, volumeManager.detachedIDs, "the volume removed from spec must be detached")
	assert.Equal(t, []string{"vol-4"}, volumeManager.attachedIDs, "only the resolvable new volume must be attached")

	pvc2, err := clientset.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "pvc-2", metav1.GetOptions{})
	require.NoError(t, err)
	assert.False(t, pvcHasUsedByAnnotaion(ctx, pvc2), "the detached PVC must no longer be used by the VM")
	pvc4, err := clientset.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "pvc-4", metav1.GetOptions{})
	require.NoError(t, err)
	assert.True(t, pvcHasUsedByAnnotaion(ctx, pvc4), "the attached PVC must be used by the VM")

	updated := &v1alpha1.CnsNodeVMBatchAttachment{}
	require.NoError(t, r.client.Get(ctx, req.NamespacedName, updated))
	ready := readyCondition(updated)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	// The message is the same as when the unresolvable PVC failed the whole reconcile.
	assert.Equal(t, vmOperatorCacheMissText+" pvc-3", ready.Message)

	assertVolumeCondition(t, updated, "disk-3", "pvc-3", v1alpha1.ConditionAttached, metav1.ConditionFalse,
		v1alpha1.ReasonAttachFailed, vmOperatorCacheMissText+" pvc-3")
	for _, cond := range findVolumeStatus(updated, "disk-3").PersistentVolumeClaim.Conditions {
		assert.Equal(t, vmOperatorCacheMissText+" pvc-3", cond.Message)
	}
	assertVolumeCondition(t, updated, "disk-1", "pvc-1", v1alpha1.ConditionAttached, metav1.ConditionTrue, "", "")
	assertVolumeCondition(t, updated, "disk-4", "pvc-4", v1alpha1.ConditionAttached, metav1.ConditionTrue, "", "")
	assert.Nil(t, findVolumeStatus(updated, "disk-2"), "the entry of the detached volume must be removed")
	assert.Nil(t, findVolumeStatus(updated, "disk-2"+detachSuffix), "the entry of the detached volume must be removed")
}

// longAttachErrorVolumeManager fails every batch attach with an error as long as the longest
// condition message.
type longAttachErrorVolumeManager struct {
	recordingVolumeManager
}

func (m *longAttachErrorVolumeManager) BatchAttachVolumes(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	requests []volumes.BatchAttachRequest) ([]volumes.BatchAttachResult, string, error) {
	return nil, "", errors.New(strings.Repeat("x", MaxConditionMessageLength))
}

// The Ready condition message is trimmed to MaxConditionMessageLength. The error of an unresolvable
// PVC must survive the trimming, as VM Operator looks for it there.
func TestReconcileKeepsUnresolvablePVCErrorInTrimmedReadyCondition(t *testing.T) {
	ctx := context.TODO()
	r, req, _ := setupUnresolvablePVCReconcile(t, &longAttachErrorVolumeManager{})

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)

	updated := &v1alpha1.CnsNodeVMBatchAttachment{}
	require.NoError(t, r.client.Get(ctx, req.NamespacedName, updated))
	ready := readyCondition(updated)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Len(t, []rune(ready.Message), MaxConditionMessageLength, "the message must have been trimmed")
	assert.Contains(t, ready.Message, vmOperatorCacheMissText+" pvc-3")
}

// failPVCRequest returns a reactor which fails the requests for the PVC with the given name with
// an internal server error.
func failPVCRequest(name string) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		if named, ok := action.(interface{ GetName() string }); !ok || named.GetName() != name {
			return false, nil, nil
		}
		return true, nil, apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
	}
}

// A PVC which cannot be looked up must be neither attached nor detached, while the other volumes are.
func TestGetVolumesToAttachAndDetachSkipsUnresolvablePVCs(t *testing.T) {
	ctx := context.TODO()
	instance := v1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: testCnsNodeVMBatchAttachmentName, Namespace: testNamespace,
			ResourceVersion: "1"},
		Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
			InstanceUUID: "test-uuid",
			Volumes: []v1alpha1.VolumeSpec{
				// pvc-1 is attached at unit number 9 and has to be moved to unit number 1, but it has
				// been deleted. As the spec still refers to it, it is not detached.
				batchVolumeSpec("disk-1", "pvc-1", 1),
				// The volumeID of pvc-3 cannot be found, although an attached disk maps back to it.
				batchVolumeSpec("disk-3", "pvc-3", 3),
				// disk-2 now refers to pvc-6, which does not exist yet.
				batchVolumeSpec("disk-2", "pvc-6", 6),
			},
		},
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				attachedVolumeStatus("disk-1", "pvc-1", "vol-1"),
				// pvc-2, pvc-5 and pvc-7 were removed from spec. pvc-2 has been deleted, and pvc-7
				// cannot be read from the API server.
				attachedVolumeStatus("disk-2", "pvc-2", "vol-2"),
				attachedVolumeStatus("disk-5", "pvc-5", "vol-5"),
				attachedVolumeStatus("disk-7", "pvc-7", "vol-7"),
				// Left behind by an earlier detach of pvc-3 with a previous backing.
				attachedVolumeStatus("disk-3"+detachSuffix, "pvc-3", "vol-3"),
			},
		},
	}
	r := setTestEnvironment(&instance, false)
	usePvcLookupOrchestrator(t, &pvcLookupOrchestrator{
		volumeIDs: map[string]string{"test-ns/pvc-1": "vol-1", "test-ns/pvc-2": "vol-2", "test-ns/pvc-5": "vol-5",
			"test-ns/pvc-7": "vol-7"},
		pvcNames: map[string]string{"vol-1": "test-ns/pvc-1", "vol-2": "test-ns/pvc-2", "vol-3": "test-ns/pvc-3",
			"vol-5": "test-ns/pvc-5", "vol-7": "test-ns/pvc-7"},
		pvcs: map[string]*v1.PersistentVolumeClaim{
			"test-ns/pvc-3": batchTestPVC("pvc-3", true, cnsoperatortypes.CNSPvcFinalizer),
			"test-ns/pvc-5": batchTestPVC("pvc-5", true, cnsoperatortypes.CNSPvcFinalizer),
		},
	})
	stubAttachedFCDs(t, map[string]FCDBackingDetails{"vol-1": batchBacking(9), "vol-2": batchBacking(2),
		"vol-3": batchBacking(3), "vol-5": batchBacking(5), "vol-7": batchBacking(7),
		// The PV of vol-gone was deleted after the attached volumes were listed.
		"vol-gone": batchBacking(11)})
	clientset := k8sFake.NewClientset(batchTestPVC("pvc-3", true, cnsoperatortypes.CNSPvcFinalizer))
	clientset.PrependReactor("get", "persistentvolumeclaims", failPVCRequest("pvc-7"))

	pvcsToAttach, pvcsToDetach, volumeErr, err := getVolumesToAttachAndDetach(ctx, &instance, testVM(),
		r.client, clientset, r.cnsOperatorClient)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pvc-2": "vol-2", "pvc-5": "vol-5"}, pvcsToDetach,
		"the volumes removed from spec must be detached, including the one whose PVC has been deleted")
	assert.Empty(t, pvcsToAttach, "pvc-1 must not be attached with its new backing as it was not detached")
	require.Error(t, volumeErr)
	assert.False(t, volumes.IsTerminalError(volumeErr))
	for _, pvc := range []string{"pvc-1", "pvc-3", "pvc-6", "pvc-7"} {
		assert.Contains(t, volumeErr.Error(), pvc)
	}
	assert.NotContains(t, volumeErr.Error(), "pvc-2")
	assert.Contains(t, volumeErr.Error(), "failed to find PVC for volumeID vol-gone in cluster",
		"a volume whose PVC disappeared must requeue the instance")

	// pvc-1 is pending detach, and its skipped attach is visible under its spec volume name.
	assertVolumeCondition(t, &instance, "disk-1"+detachSuffix, "pvc-1", v1alpha1.ConditionDetached,
		metav1.ConditionFalse, v1alpha1.ReasonDetachFailed, "failed to find PVC obj for PVC name pvc-1")
	assertVolumeCondition(t, &instance, "disk-1", "pvc-1", v1alpha1.ConditionAttached, metav1.ConditionFalse,
		v1alpha1.ReasonAttachFailed, "detaching it with its previous backing did not complete")
	// pvc-2 is being detached, and the failed attach of pvc-6 is recorded in a separate entry under
	// the volume name disk-2, which pvc-2 had.
	assert.NotNil(t, findVolumeStatus(&instance, "disk-2"+detachSuffix))
	assertVolumeCondition(t, &instance, "disk-2", "pvc-6", v1alpha1.ConditionAttached, metav1.ConditionFalse,
		v1alpha1.ReasonAttachFailed, "failed to find volumeID for PVC pvc-6")
	// pvc-3 failed to attach, and its earlier detaching entry is not removed as stale.
	assertVolumeCondition(t, &instance, "disk-3", "pvc-3", v1alpha1.ConditionAttached, metav1.ConditionFalse,
		v1alpha1.ReasonAttachFailed, "failed to find volumeID for PVC pvc-3")
	assert.NotNil(t, findVolumeStatus(&instance, "disk-3"+detachSuffix))
	pvc3, err := clientset.CoreV1().PersistentVolumeClaims(testNamespace).Get(ctx, "pvc-3", metav1.GetOptions{})
	require.NoError(t, err)
	assert.True(t, pvcHasUsedByAnnotaion(ctx, pvc3), "the PVC protection of pvc-3 must not be removed")
	// pvc-5 is being detached, and pvc-7 is pending detach.
	assert.NotNil(t, findVolumeStatus(&instance, "disk-5"+detachSuffix))
	assertVolumeCondition(t, &instance, "disk-7"+detachSuffix, "pvc-7", v1alpha1.ConditionDetached,
		metav1.ConditionFalse, v1alpha1.ReasonDetachFailed, "failed to find PVC obj for PVC name pvc-7")
}

// When the PVC object of a volume to detach is not in the informer cache, the API server is checked.
// A PVC found there is handled as usual. The volume of a deleted PVC is detached unless the spec
// still refers to it.
func TestGetVolumesToDetachFromInstanceWhenPVCIsNotInCache(t *testing.T) {
	ctx := context.TODO()
	tests := []struct {
		name string
		// volumeInSpec is set if the volume is in spec with another backing.
		volumeInSpec   bool
		apiPVC         *v1.PersistentVolumeClaim
		apiFails       bool
		wantDetach     bool
		wantUnresolved bool
	}{
		{name: "PVC on API server used by the VM is detached", apiPVC: batchTestPVC("pvc-x", true),
			wantDetach: true},
		{name: "PVC on API server attached via CnsNodeVmAttachment is not detached",
			apiPVC: batchTestPVC("pvc-x", false, cnsoperatortypes.CNSPvcFinalizer)},
		{name: "volume of deleted PVC is detached", wantDetach: true},
		{name: "volume of deleted PVC in spec is not detached", volumeInSpec: true, wantUnresolved: true},
		{name: "PVC which cannot be read from API server is not detached", apiFails: true, wantUnresolved: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := setupTestCnsNodeVMBatchAttachment()
			usePvcLookupOrchestrator(t, &pvcLookupOrchestrator{pvcNames: map[string]string{"vol-x": "test-ns/pvc-x"}})
			clientset := k8sFake.NewClientset()
			if tt.apiPVC != nil {
				clientset = k8sFake.NewClientset(tt.apiPVC)
			}
			if tt.apiFails {
				clientset.PrependReactor("get", "persistentvolumeclaims", failPVCRequest("pvc-x"))
			}
			volumeIdsInSpec := map[string]FCDBackingDetails{}
			if tt.volumeInSpec {
				volumeIdsInSpec["vol-x"] = batchBacking(1)
			}

			detach := getVolumesToDetachFromInstance(ctx, &instance, clientset,
				map[string]FCDBackingDetails{"vol-x": batchBacking(2)}, volumeIdsInSpec, nil)
			pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
			if tt.wantDetach {
				assert.Equal(t, map[string]string{"pvc-x": "vol-x"}, pvcsToDetach)
			} else {
				assert.Empty(t, pvcsToDetach)
			}
			if tt.wantUnresolved {
				assert.Contains(t, unresolved, "pvc-x")
			} else {
				assert.Empty(t, unresolved)
			}
			assert.Empty(t, detach.unknownVolumes)
		})
	}
}

// An attached volume whose PVC can no longer be found, as its PV was deleted after the volumes were
// listed, must not be detached. It must be reported, so that the instance is requeued to look at it
// again, and the other volumes must still be detached.
func TestGetVolumesToDetachFromInstanceReportsVolumeWithoutPVC(t *testing.T) {
	instance := setupTestCnsNodeVMBatchAttachment()
	usePvcLookupOrchestrator(t, &pvcLookupOrchestrator{
		pvcNames: map[string]string{"vol-5": "test-ns/pvc-5"},
		pvcs:     map[string]*v1.PersistentVolumeClaim{"test-ns/pvc-5": batchTestPVC("pvc-5", true)},
	})

	detach := getVolumesToDetachFromInstance(context.TODO(), &instance, k8sFake.NewClientset(),
		map[string]FCDBackingDetails{"vol-5": batchBacking(5), "vol-gone": batchBacking(6)}, nil, nil)
	pvcsToDetach, unresolved := detach.pvcsToDetach, detach.pvcsNotDetached
	assert.Equal(t, map[string]string{"pvc-5": "vol-5"}, pvcsToDetach)
	assert.Empty(t, unresolved)
	require.Contains(t, detach.unknownVolumes, "vol-gone")
	assert.EqualError(t, detach.unknownVolumes["vol-gone"], "failed to find PVC for volumeID vol-gone in cluster")
}

// When the finalizer of the PVC of a stale status entry cannot be removed, the other volumes must
// still be attached and detached. The entry is kept with the error, so that removing the finalizer
// is retried, and the attach of the PVC is skipped, as that retry would otherwise remove the
// finalizer of an attached PVC.
func TestGetVolumesToAttachAndDetachContinuesWhenStaleEntryCleanupFails(t *testing.T) {
	ctx := context.TODO()
	instance := v1alpha1.CnsNodeVMBatchAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: testCnsNodeVMBatchAttachmentName, Namespace: testNamespace,
			ResourceVersion: "1"},
		Spec: v1alpha1.CnsNodeVMBatchAttachmentSpec{
			InstanceUUID: "test-uuid",
			Volumes: []v1alpha1.VolumeSpec{
				batchVolumeSpec("disk-4", "pvc-4", 4),
				// pvc-8 was detached to change its backing, but its finalizer could not be removed.
				batchVolumeSpec("disk-8", "pvc-8", 8),
			},
		},
		Status: v1alpha1.CnsNodeVMBatchAttachmentStatus{
			VolumeStatus: []v1alpha1.VolumeStatus{
				// pvc-5 was removed from spec.
				attachedVolumeStatus("disk-5", "pvc-5", "vol-5"),
				attachedVolumeStatus("disk-8"+detachSuffix, "pvc-8", "vol-8"),
				// pvc-9 was detached, but its entry could not be removed.
				attachedVolumeStatus("disk-9", "pvc-9", "vol-9"),
			},
		},
	}
	r := setTestEnvironment(&instance, false)
	usePvcLookupOrchestrator(t, &pvcLookupOrchestrator{
		volumeIDs: map[string]string{"test-ns/pvc-4": "vol-4", "test-ns/pvc-5": "vol-5", "test-ns/pvc-8": "vol-8"},
		pvcNames:  map[string]string{"vol-5": "test-ns/pvc-5"},
		pvcs: map[string]*v1.PersistentVolumeClaim{
			"test-ns/pvc-5": batchTestPVC("pvc-5", true, cnsoperatortypes.CNSPvcFinalizer),
			"test-ns/pvc-8": batchTestPVC("pvc-8", true, cnsoperatortypes.CNSPvcFinalizer),
		},
	})
	stubAttachedFCDs(t, map[string]FCDBackingDetails{"vol-5": batchBacking(5)})
	clientset := k8sFake.NewClientset(batchTestPVC("pvc-8", true, cnsoperatortypes.CNSPvcFinalizer))
	clientset.PrependReactor("patch", "persistentvolumeclaims", failPVCRequest("pvc-8"))

	pvcsToAttach, pvcsToDetach, volumeErr, err := getVolumesToAttachAndDetach(ctx, &instance, testVM(),
		r.client, clientset, r.cnsOperatorClient)
	require.NoError(t, err, "a failed stale entry cleanup must not fail the whole reconcile")
	assert.Equal(t, map[string]string{"pvc-5": "vol-5"}, pvcsToDetach)
	assert.Equal(t, map[string]string{"pvc-4": "vol-4"}, pvcsToAttach, "the attach of pvc-8 must be skipped")
	require.Error(t, volumeErr)
	assert.False(t, volumes.IsTerminalError(volumeErr))
	assert.Contains(t, volumeErr.Error(), "from PVC pvc-8")

	assertVolumeCondition(t, &instance, "disk-8"+detachSuffix, "pvc-8", v1alpha1.ConditionDetached,
		metav1.ConditionFalse, v1alpha1.ReasonDetachFailed, "failed to remove finalizer")
	assertVolumeCondition(t, &instance, "disk-8", "pvc-8", v1alpha1.ConditionAttached, metav1.ConditionFalse,
		v1alpha1.ReasonAttachFailed, "detaching it with its previous backing did not complete")
	assert.Nil(t, findVolumeStatus(&instance, "disk-9"), "the stale entry of pvc-9 must still be removed")
	assert.NotNil(t, findVolumeStatus(&instance, "disk-5"+detachSuffix))
}

// A PVC whose object cannot be found must be left out of the batch attach, and the other volumes
// must still be attached.
func TestProcessBatchAttachSkipsPVCWhoseObjectIsNotFound(t *testing.T) {
	ctx := context.TODO()
	instance := setupTestCnsNodeVMBatchAttachment()
	instance.Spec.Volumes = []v1alpha1.VolumeSpec{batchVolumeSpec("disk-1", "pvc-1", 1),
		batchVolumeSpec("disk-2", "pvc-2", 2)}
	instance.Status.VolumeStatus = nil
	r := setTestEnvironment(&instance, false)
	volumeManager := &recordingVolumeManager{}
	r.volumeManager = volumeManager
	usePvcLookupOrchestrator(t, &pvcLookupOrchestrator{
		pvcs: map[string]*v1.PersistentVolumeClaim{"test-ns/pvc-1": batchTestPVC("pvc-1", false)},
	})
	clientset := k8sFake.NewClientset(batchTestPVC("pvc-1", false, cnsoperatortypes.CNSPvcFinalizer))

	err := r.processBatchAttach(ctx, clientset, testVM(), &instance,
		map[string]string{"pvc-1": "vol-1", "pvc-2": "vol-2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `failed to find the PVC object "pvc-2"`)
	assert.Equal(t, []string{"vol-1"}, volumeManager.attachedIDs)
	assertVolumeCondition(t, &instance, "disk-1", "pvc-1", v1alpha1.ConditionAttached, metav1.ConditionTrue, "", "")
	assertVolumeCondition(t, &instance, "disk-2", "pvc-2", v1alpha1.ConditionAttached, metav1.ConditionFalse,
		v1alpha1.ReasonAttachFailed, `failed to find the PVC object "pvc-2"`)

	// When no PVC object is found, CNS is not called at all.
	volumeManager.attachedIDs = nil
	err = r.processBatchAttach(ctx, clientset, testVM(), &instance, map[string]string{"pvc-2": "vol-2"})
	require.Error(t, err)
	assert.Empty(t, volumeManager.attachedIDs)
}
