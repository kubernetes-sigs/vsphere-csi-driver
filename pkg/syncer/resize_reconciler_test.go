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

package syncer

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	testclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"
)

// Test helper functions

func createTestPVC(
	name, namespace string,
	capacity, requested string,
	conditions []v1.PersistentVolumeClaimCondition,
	allocatedResourceStatuses map[v1.ResourceName]v1.ClaimResourceStatus,
) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse(requested),
				},
			},
		},
		Status: v1.PersistentVolumeClaimStatus{
			Capacity: v1.ResourceList{
				v1.ResourceStorage: resource.MustParse(capacity),
			},
			Conditions:                conditions,
			AllocatedResourceStatuses: allocatedResourceStatuses,
		},
	}
}

func createTestPV(name, volumeHandle string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: volumeHandle,
				},
			},
		},
	}
}

func createFileSystemResizePendingCondition() v1.PersistentVolumeClaimCondition {
	return v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimFileSystemResizePending,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Message:            "Waiting for user to (re-)start a pod to finish file system resize of volume on node.",
	}
}

func newTestRateLimitingQueue(name string) workqueue.TypedRateLimitingInterface[any] {
	return workqueue.NewNamedRateLimitingQueue(
		workqueue.DefaultTypedControllerRateLimiter[any](), name)
}

// setupSyncPVCTest creates a reconciler wired to fake clients with the given
// guest and supervisor PVCs pre-populated. It returns the reconciler, the
// supervisor fake client (for post-reconcile assertions), a context, and a
// teardown function that must be deferred.
func setupSyncPVCTest(
	t *testing.T,
	guestPVC *v1.PersistentVolumeClaim,
	supervisorPVC *v1.PersistentVolumeClaim,
) (*resizeReconciler, *testclient.Clientset, context.Context, func()) {
	t.Helper()

	tkgClient := testclient.NewSimpleClientset()
	supervisorClient := testclient.NewSimpleClientset()
	ctx := context.TODO()

	guestPVC.Spec.VolumeName = "test-pv"
	guestPV := createTestPV("test-pv", supervisorPVC.Name)

	if _, err := tkgClient.CoreV1().PersistentVolumeClaims(guestPVC.Namespace).Create(
		ctx, guestPVC, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create guest PVC: %v", err)
	}
	if _, err := tkgClient.CoreV1().PersistentVolumes().Create(
		ctx, guestPV, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create guest PV: %v", err)
	}
	if _, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorPVC.Namespace).Create(
		ctx, supervisorPVC, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create supervisor PVC: %v", err)
	}

	informerFactory := informers.NewSharedInformerFactory(tkgClient, time.Hour)
	rc := &resizeReconciler{
		tkgClient:           tkgClient,
		supervisorClient:    supervisorClient,
		supervisorNamespace: supervisorPVC.Namespace,
		pvcLister:           informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
		pvLister:            informerFactory.Core().V1().PersistentVolumes().Lister(),
		claimQueue:          newTestRateLimitingQueue("test-queue"),
	}

	stopCh := make(chan struct{})
	informerFactory.Start(stopCh)
	informerFactory.WaitForCacheSync(stopCh)

	return rc, supervisorClient, ctx, func() { close(stopCh) }
}

// TestHasNodeResizePending tests the hasNodeResizePending helper function.
func TestHasNodeResizePending(t *testing.T) {
	tests := []struct {
		name     string
		pvc      *v1.PersistentVolumeClaim
		expected bool
	}{
		{
			name: "PVC with NodeResizePending",
			pvc: createTestPVC("test-pvc", "test-ns", "100Mi", "100Mi", nil,
				map[v1.ResourceName]v1.ClaimResourceStatus{
					v1.ResourceStorage: v1.PersistentVolumeClaimNodeResizePending,
				}),
			expected: true,
		},
		{
			name:     "PVC without allocatedResourceStatuses",
			pvc:      createTestPVC("test-pvc", "test-ns", "100Mi", "100Mi", nil, nil),
			expected: false,
		},
		{
			name: "PVC with empty allocatedResourceStatuses",
			pvc: createTestPVC("test-pvc", "test-ns", "100Mi", "100Mi", nil,
				map[v1.ResourceName]v1.ClaimResourceStatus{}),
			expected: false,
		},
		{
			name: "PVC with different status",
			pvc: createTestPVC("test-pvc", "test-ns", "100Mi", "100Mi", nil,
				map[v1.ResourceName]v1.ClaimResourceStatus{
					v1.ResourceStorage: "SomeOtherStatus",
				}),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasNodeResizePending(tt.pvc)
			if result != tt.expected {
				t.Errorf("hasNodeResizePending() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// TestUpdatePVCQueueConditions tests the queue trigger conditions in updatePVC.
func TestUpdatePVCQueueConditions(t *testing.T) {
	tests := []struct {
		name        string
		oldPVC      *v1.PersistentVolumeClaim
		newPVC      *v1.PersistentVolumeClaim
		shouldQueue bool
		description string
	}{
		{
			name:        "Capacity increased",
			oldPVC:      createTestPVC("test-pvc", "test-ns", "100Mi", "200Mi", nil, nil),
			newPVC:      createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi", nil, nil),
			shouldQueue: true,
			description: "Should queue when capacity increases",
		},
		{
			name: "FileSystemResizePending cleared",
			oldPVC: createTestPVC("test-pvc", "test-ns", "100Mi", "200Mi",
				[]v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()}, nil),
			newPVC:      createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi", nil, nil),
			shouldQueue: true,
			description: "Should queue when FileSystemResizePending is cleared",
		},
		{
			name: "NodeResizePending cleared",
			oldPVC: createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi", nil,
				map[v1.ResourceName]v1.ClaimResourceStatus{
					v1.ResourceStorage: v1.PersistentVolumeClaimNodeResizePending,
				}),
			newPVC:      createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi", nil, nil),
			shouldQueue: true,
			description: "Should queue when NodeResizePending is cleared",
		},
		{
			name:        "No significant changes",
			oldPVC:      createTestPVC("test-pvc", "test-ns", "100Mi", "100Mi", nil, nil),
			newPVC:      createTestPVC("test-pvc", "test-ns", "100Mi", "100Mi", nil, nil),
			shouldQueue: false,
			description: "Should not queue when nothing significant changes",
		},
		{
			name: "Both conditions present - no change",
			oldPVC: createTestPVC("test-pvc", "test-ns", "100Mi", "200Mi",
				[]v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
				map[v1.ResourceName]v1.ClaimResourceStatus{
					v1.ResourceStorage: v1.PersistentVolumeClaimNodeResizePending,
				}),
			newPVC: createTestPVC("test-pvc", "test-ns", "100Mi", "200Mi",
				[]v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
				map[v1.ResourceName]v1.ClaimResourceStatus{
					v1.ResourceStorage: v1.PersistentVolumeClaimNodeResizePending,
				}),
			shouldQueue: false,
			description: "Should not queue when both conditions remain unchanged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tkgClient := testclient.NewSimpleClientset()
			supervisorClient := testclient.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(tkgClient, time.Hour)

			rc := &resizeReconciler{
				tkgClient:           tkgClient,
				supervisorClient:    supervisorClient,
				supervisorNamespace: "test-supervisor-ns",
				pvcLister:           informerFactory.Core().V1().PersistentVolumeClaims().Lister(),
				pvLister:            informerFactory.Core().V1().PersistentVolumes().Lister(),
				claimQueue:          newTestRateLimitingQueue("test-queue"),
			}

			initialQueueLen := rc.claimQueue.Len()
			rc.updatePVC(tt.oldPVC, tt.newPVC)
			queuedItem := rc.claimQueue.Len() > initialQueueLen

			if queuedItem != tt.shouldQueue {
				t.Errorf("%s: queued = %v, expected %v", tt.description, queuedItem, tt.shouldQueue)
			}
		})
	}
}

// TestClearFileSystemResizePending verifies that syncPVC clears (or preserves)
// FileSystemResizePending on the supervisor PVC based on the guest PVC state.
func TestClearFileSystemResizePending(t *testing.T) {
	tests := []struct {
		name                 string
		guestConditions      []v1.PersistentVolumeClaimCondition
		supervisorConditions []v1.PersistentVolumeClaimCondition
		expectFSPendingOnSVC bool
	}{
		{
			name:                 "guest clean, supervisor has FileSystemResizePending",
			guestConditions:      nil,
			supervisorConditions: []v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
			expectFSPendingOnSVC: false,
		},
		{
			name:                 "guest still has FileSystemResizePending",
			guestConditions:      []v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
			supervisorConditions: []v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
			expectFSPendingOnSVC: true,
		},
		{
			name:                 "both already clean",
			guestConditions:      nil,
			supervisorConditions: nil,
			expectFSPendingOnSVC: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guestPVC := createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi", tt.guestConditions, nil)
			supervisorPVC := createTestPVC("supervisor-pvc-handle", "sv-ns", "200Mi", "200Mi", tt.supervisorConditions, nil)

			rc, supervisorClient, ctx, teardown := setupSyncPVCTest(t, guestPVC, supervisorPVC)
			defer teardown()

			if err := rc.syncPVC(ctx, "test-ns/test-pvc"); err != nil {
				t.Fatalf("syncPVC: %v", err)
			}

			updated, err := supervisorClient.CoreV1().PersistentVolumeClaims("sv-ns").Get(
				ctx, "supervisor-pvc-handle", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get supervisor PVC: %v", err)
			}

			if got := checkFileSystemPendingOnPVC(updated); got != tt.expectFSPendingOnSVC {
				t.Errorf("FileSystemResizePending on supervisor PVC = %v, want %v", got, tt.expectFSPendingOnSVC)
			}
		})
	}
}

// TestClearNodeResizePending verifies that syncPVC clears (or preserves)
// NodeResizePending in the supervisor PVC's AllocatedResourceStatuses based on
// the guest PVC state and the supervisor's FileSystemResizePending condition.
func TestClearNodeResizePending(t *testing.T) {
	nodeResizePending := map[v1.ResourceName]v1.ClaimResourceStatus{
		v1.ResourceStorage: v1.PersistentVolumeClaimNodeResizePending,
	}

	tests := []struct {
		name                      string
		guestConditions           []v1.PersistentVolumeClaimCondition
		guestAllocatedStatuses    map[v1.ResourceName]v1.ClaimResourceStatus
		supervisorConditions      []v1.PersistentVolumeClaimCondition
		supervisorAllocatedStatus map[v1.ResourceName]v1.ClaimResourceStatus
		expectNodeResizePending   bool
	}{
		{
			// Guest is fully clean; supervisor has NodeResizePending but no
			// FileSystemResizePending — guard passes, NodeResizePending cleared.
			name:                      "guest clean, supervisor has NodeResizePending only",
			guestConditions:           nil,
			guestAllocatedStatuses:    nil,
			supervisorConditions:      nil,
			supervisorAllocatedStatus: nodeResizePending,
			expectNodeResizePending:   false,
		},
		{
			// Guest has cleared NodeResizePending but still has
			// FileSystemResizePending. Supervisor has both. The
			// FileSystemResizePending guard blocks premature clearing of
			// NodeResizePending; it will be cleared in the next reconciliation
			// once FileSystemResizePending is gone from the guest too.
			name:                      "guest cleared NodeResizePending but still has FileSystemResizePending",
			guestConditions:           []v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
			guestAllocatedStatuses:    nil,
			supervisorConditions:      []v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
			supervisorAllocatedStatus: nodeResizePending,
			expectNodeResizePending:   true,
		},
		{
			// Guest still has NodeResizePending — reconciler must not clear it
			// from the supervisor.
			name:                      "guest still has NodeResizePending",
			guestConditions:           nil,
			guestAllocatedStatuses:    nodeResizePending,
			supervisorConditions:      nil,
			supervisorAllocatedStatus: nodeResizePending,
			expectNodeResizePending:   true,
		},
		{
			name:                      "both already clean",
			guestConditions:           nil,
			guestAllocatedStatuses:    nil,
			supervisorConditions:      nil,
			supervisorAllocatedStatus: nil,
			expectNodeResizePending:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guestPVC := createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi",
				tt.guestConditions, tt.guestAllocatedStatuses)
			supervisorPVC := createTestPVC("supervisor-pvc-handle", "sv-ns", "200Mi", "200Mi",
				tt.supervisorConditions, tt.supervisorAllocatedStatus)

			rc, supervisorClient, ctx, teardown := setupSyncPVCTest(t, guestPVC, supervisorPVC)
			defer teardown()

			if err := rc.syncPVC(ctx, "test-ns/test-pvc"); err != nil {
				t.Fatalf("syncPVC: %v", err)
			}

			updated, err := supervisorClient.CoreV1().PersistentVolumeClaims("sv-ns").Get(
				ctx, "supervisor-pvc-handle", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get supervisor PVC: %v", err)
			}

			if got := hasNodeResizePending(updated); got != tt.expectNodeResizePending {
				t.Errorf("NodeResizePending on supervisor PVC = %v, want %v", got, tt.expectNodeResizePending)
			}
		})
	}
}

// TestResizeReconcilerFlow tests the end-to-end resize reconciler flow.
func TestResizeReconcilerFlow(t *testing.T) {
	guestPVC := createTestPVC("test-pvc", "test-ns", "200Mi", "200Mi", nil, nil)
	supervisorPVC := createTestPVC(
		"supervisor-pvc-handle", "supervisor-ns", "200Mi", "200Mi",
		[]v1.PersistentVolumeClaimCondition{createFileSystemResizePendingCondition()},
		map[v1.ResourceName]v1.ClaimResourceStatus{v1.ResourceStorage: v1.PersistentVolumeClaimNodeResizePending},
	)

	rc, supervisorClient, ctx, teardown := setupSyncPVCTest(t, guestPVC, supervisorPVC)
	defer teardown()

	if err := rc.syncPVC(ctx, "test-ns/test-pvc"); err != nil {
		t.Fatalf("syncPVC failed: %v", err)
	}

	updatedSVC, err := supervisorClient.CoreV1().PersistentVolumeClaims("supervisor-ns").Get(
		ctx, "supervisor-pvc-handle", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get updated supervisor PVC: %v", err)
	}

	if checkFileSystemPendingOnPVC(updatedSVC) {
		t.Error("FileSystemResizePending should have been cleared from supervisor PVC")
	}
	if hasNodeResizePending(updatedSVC) {
		t.Error("NodeResizePending should have been cleared from supervisor PVC")
	}
}
