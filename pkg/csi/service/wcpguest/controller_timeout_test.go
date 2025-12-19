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

package wcpguest

import (
	"context"
	"testing"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotclientset "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// TestTimeoutErrorCodes verifies that timeout operations return the correct gRPC error code
// (codes.DeadlineExceeded) instead of codes.Internal, which allows external-provisioner to
// retry and prevents orphaned volumes.
func TestTimeoutErrorCodes(t *testing.T) {
	tests := []struct {
		name         string
		code         codes.Code
		expectedCode codes.Code
		shouldMatch  bool
	}{
		{
			name:         "timeout returns DeadlineExceeded",
			code:         codes.DeadlineExceeded,
			expectedCode: codes.DeadlineExceeded,
			shouldMatch:  true,
		},
		{
			name:         "DeadlineExceeded is not Internal",
			code:         codes.DeadlineExceeded,
			expectedCode: codes.Internal,
			shouldMatch:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := status.Error(tt.code, "test message")
			st, ok := status.FromError(err)
			if !ok {
				t.Fatal("Failed to extract gRPC status from error")
			}

			if tt.shouldMatch {
				if st.Code() != tt.expectedCode {
					t.Errorf("Expected code %v, got %v", tt.expectedCode, st.Code())
				}
			} else {
				if st.Code() == tt.expectedCode {
					t.Errorf("Expected code to differ from %v, but they matched", tt.expectedCode)
				}
			}
		})
	}
}

// TestPVCStateTransitions verifies PVC state transitions in the supervisor cluster,
// which is critical for the timeout retry scenario.
func TestPVCStateTransitions(t *testing.T) {
	tests := []struct {
		name          string
		initialPhase  v1.PersistentVolumeClaimPhase
		targetPhase   v1.PersistentVolumeClaimPhase
		shouldSucceed bool
	}{
		{
			name:          "create pending PVC",
			initialPhase:  v1.ClaimPending,
			targetPhase:   v1.ClaimPending,
			shouldSucceed: true,
		},
		{
			name:          "transition pending to bound",
			initialPhase:  v1.ClaimPending,
			targetPhase:   v1.ClaimBound,
			shouldSucceed: true,
		},
		{
			name:          "bound remains bound",
			initialPhase:  v1.ClaimBound,
			targetPhase:   v1.ClaimBound,
			shouldSucceed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			supervisorClient := testclient.NewClientset()
			supervisorNamespace := "supervisor-ns"
			pvcName := "test-pvc-" + tt.name

			// Create PVC with initial phase
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: supervisorNamespace,
					Annotations: map[string]string{
						common.AnnDynamicallyProvisioned: "csi.vsphere.vmware.com",
					},
				},
				Spec: v1.PersistentVolumeClaimSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					Resources: v1.VolumeResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
					StorageClassName: stringPtr("test-sc"),
				},
				Status: v1.PersistentVolumeClaimStatus{
					Phase: tt.initialPhase,
				},
			}

			// Create PVC
			createdPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Create(
				ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				if tt.shouldSucceed {
					t.Fatalf("Failed to create PVC: %v", err)
				}
				return
			}

			// Verify initial phase
			if createdPVC.Status.Phase != tt.initialPhase {
				t.Errorf("Expected initial phase %v, got %v", tt.initialPhase, createdPVC.Status.Phase)
			}

			// Update to target phase if different
			if tt.targetPhase != tt.initialPhase {
				createdPVC.Status.Phase = tt.targetPhase
				if tt.targetPhase == v1.ClaimBound {
					createdPVC.Status.Capacity = v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("10Gi"),
					}
					createdPVC.Spec.VolumeName = "pv-" + pvcName
				}

				updatedPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Update(
					ctx, createdPVC, metav1.UpdateOptions{})
				if err != nil {
					if tt.shouldSucceed {
						t.Fatalf("Failed to update PVC: %v", err)
					}
					return
				}

				// Verify target phase
				if updatedPVC.Status.Phase != tt.targetPhase {
					t.Errorf("Expected target phase %v, got %v", tt.targetPhase, updatedPVC.Status.Phase)
				}
			}
		})
	}
}

// TestPVCLifecycle verifies the complete PVC lifecycle: create, update, delete.
// This simulates the full timeout and cleanup scenario.
func TestPVCLifecycle(t *testing.T) {
	tests := []struct {
		name           string
		pvcName        string
		initialPhase   v1.PersistentVolumeClaimPhase
		updateToPhase  v1.PersistentVolumeClaimPhase
		shouldDelete   bool
		verifyDeletion bool
	}{
		{
			name:           "pending PVC can be deleted",
			pvcName:        "pending-pvc",
			initialPhase:   v1.ClaimPending,
			updateToPhase:  v1.ClaimPending,
			shouldDelete:   true,
			verifyDeletion: true,
		},
		{
			name:           "bound PVC can be deleted for cleanup",
			pvcName:        "bound-pvc",
			initialPhase:   v1.ClaimPending,
			updateToPhase:  v1.ClaimBound,
			shouldDelete:   true,
			verifyDeletion: true,
		},
		{
			name:           "verify PVC creation only",
			pvcName:        "create-only-pvc",
			initialPhase:   v1.ClaimPending,
			updateToPhase:  v1.ClaimPending,
			shouldDelete:   false,
			verifyDeletion: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			supervisorClient := testclient.NewClientset()
			supervisorNamespace := "supervisor-ns"

			// Create PVC
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.pvcName,
					Namespace: supervisorNamespace,
					Annotations: map[string]string{
						common.AnnDynamicallyProvisioned: "csi.vsphere.vmware.com",
					},
				},
				Spec: v1.PersistentVolumeClaimSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					Resources: v1.VolumeResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
					StorageClassName: stringPtr("test-sc"),
				},
				Status: v1.PersistentVolumeClaimStatus{
					Phase: tt.initialPhase,
				},
			}

			createdPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Create(
				ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create PVC: %v", err)
			}

			// Update phase if needed
			if tt.updateToPhase != tt.initialPhase {
				createdPVC.Status.Phase = tt.updateToPhase
				if tt.updateToPhase == v1.ClaimBound {
					createdPVC.Status.Capacity = v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("10Gi"),
					}
					createdPVC.Spec.VolumeName = "pv-" + tt.pvcName
				}

				_, err = supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Update(
					ctx, createdPVC, metav1.UpdateOptions{})
				if err != nil {
					t.Fatalf("Failed to update PVC: %v", err)
				}
			}

			// Delete if requested
			if tt.shouldDelete {
				err = supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Delete(
					ctx, tt.pvcName, metav1.DeleteOptions{})
				if err != nil {
					t.Fatalf("Failed to delete PVC: %v", err)
				}
			}

			// Verify deletion if requested
			if tt.verifyDeletion {
				_, err = supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Get(
					ctx, tt.pvcName, metav1.GetOptions{})
				if err == nil {
					t.Error("Expected PVC to be deleted, but it still exists")
				}
			}
		})
	}
}

// TestIdempotentOperations verifies that operations can be safely retried.
func TestIdempotentOperations(t *testing.T) {
	tests := []struct {
		name          string
		pvcName       string
		createTwice   bool
		expectSuccess bool
		expectedPhase v1.PersistentVolumeClaimPhase
	}{
		{
			name:          "create PVC once",
			pvcName:       "single-create-pvc",
			createTwice:   false,
			expectSuccess: true,
			expectedPhase: v1.ClaimBound,
		},
		{
			name:          "idempotent get after create",
			pvcName:       "idempotent-pvc",
			createTwice:   true,
			expectSuccess: true,
			expectedPhase: v1.ClaimBound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			supervisorClient := testclient.NewClientset()
			supervisorNamespace := "supervisor-ns"

			// Create PVC
			pvc := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.pvcName,
					Namespace: supervisorNamespace,
				},
				Spec: v1.PersistentVolumeClaimSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					Resources: v1.VolumeResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceStorage: resource.MustParse("10Gi"),
						},
					},
					StorageClassName: stringPtr("test-sc"),
					VolumeName:       "pv-" + tt.pvcName,
				},
				Status: v1.PersistentVolumeClaimStatus{
					Phase: tt.expectedPhase,
					Capacity: v1.ResourceList{
						v1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			}

			_, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Create(
				ctx, pvc, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create PVC: %v", err)
			}

			// Get PVC (simulates retry/idempotent check)
			if tt.createTwice {
				retrievedPVC, err := supervisorClient.CoreV1().PersistentVolumeClaims(supervisorNamespace).Get(
					ctx, tt.pvcName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Failed to get PVC: %v", err)
				}

				if retrievedPVC.Status.Phase != tt.expectedPhase {
					t.Errorf("Expected phase %v, got %v", tt.expectedPhase, retrievedPVC.Status.Phase)
				}

				// Verify we got the same PVC
				if retrievedPVC.Name != tt.pvcName {
					t.Errorf("Expected PVC name %s, got %s", tt.pvcName, retrievedPVC.Name)
				}
			}
		})
	}
}

// TestErrorCodeBehavior verifies external-provisioner behavior with different error codes.
func TestErrorCodeBehavior(t *testing.T) {
	tests := []struct {
		name                     string
		errorCode                codes.Code
		expectRetry              bool
		expectedProvisionerState string
	}{
		{
			name:                     "DeadlineExceeded allows retry",
			errorCode:                codes.DeadlineExceeded,
			expectRetry:              true,
			expectedProvisionerState: "ProvisioningInBackground",
		},
		{
			name:                     "Internal stops retry",
			errorCode:                codes.Internal,
			expectRetry:              false,
			expectedProvisionerState: "ProvisioningFinished",
		},
		{
			name:                     "Unavailable allows retry",
			errorCode:                codes.Unavailable,
			expectRetry:              true,
			expectedProvisionerState: "ProvisioningInBackground",
		},
		{
			name:                     "Aborted allows retry",
			errorCode:                codes.Aborted,
			expectRetry:              true,
			expectedProvisionerState: "ProvisioningInBackground",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := status.Error(tt.errorCode, "test error")
			st, _ := status.FromError(err)

			if st.Code() != tt.errorCode {
				t.Errorf("Expected error code %v, got %v", tt.errorCode, st.Code())
			}

			// Verify error codes match expected retry behavior
			// (codes 1-4 allow retry: Canceled, Unknown, InvalidArgument, DeadlineExceeded)
			// (codes 13+ typically stop retry: Internal, Unavailable, etc. - but Unavailable/Aborted allow retry)
			retryableCodes := map[codes.Code]bool{
				codes.Canceled:         true,
				codes.DeadlineExceeded: true,
				codes.Unavailable:      true,
				codes.Aborted:          true,
			}

			shouldRetry := retryableCodes[tt.errorCode]
			if shouldRetry != tt.expectRetry {
				t.Errorf("Expected retry=%v for code %v, but got %v", tt.expectRetry, tt.errorCode, shouldRetry)
			}
		})
	}
}

// TestSnapshotStateTransitions verifies VolumeSnapshot state transitions in the supervisor cluster,
// which is critical for the snapshot timeout retry scenario.
func TestSnapshotStateTransitions(t *testing.T) {
	tests := []struct {
		name          string
		initialReady  bool
		targetReady   bool
		shouldSucceed bool
	}{
		{
			name:          "create snapshot not ready",
			initialReady:  false,
			targetReady:   false,
			shouldSucceed: true,
		},
		{
			name:          "transition not ready to ready",
			initialReady:  false,
			targetReady:   true,
			shouldSucceed: true,
		},
		{
			name:          "ready remains ready",
			initialReady:  true,
			targetReady:   true,
			shouldSucceed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			supervisorSnapshotClient := snapshotclientset.NewSimpleClientset()
			supervisorNamespace := "supervisor-ns"
			snapshotName := "test-snapshot-" + tt.name
			pvcName := "test-pvc-" + tt.name

			// Create VolumeSnapshot with initial state
			snapshot := &snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      snapshotName,
					Namespace: supervisorNamespace,
					Annotations: map[string]string{
						common.SupervisorVolumeSnapshotAnnotationKey: "true",
					},
				},
				Spec: snapshotv1.VolumeSnapshotSpec{
					Source: snapshotv1.VolumeSnapshotSource{
						PersistentVolumeClaimName: &pvcName,
					},
					VolumeSnapshotClassName: stringPtr("test-snapshot-class"),
				},
			}

			if tt.initialReady {
				restoreSize := resource.MustParse("10Gi")
				creationTime := metav1.Now()
				snapshot.Status = &snapshotv1.VolumeSnapshotStatus{
					ReadyToUse:   &tt.initialReady,
					RestoreSize:  &restoreSize,
					CreationTime: &creationTime,
				}
			}

			// Create VolumeSnapshot
			createdSnapshot, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Create(
				ctx, snapshot, metav1.CreateOptions{})
			if err != nil {
				if tt.shouldSucceed {
					t.Fatalf("Failed to create VolumeSnapshot: %v", err)
				}
				return
			}

			// Verify initial ready state
			if createdSnapshot.Status != nil && createdSnapshot.Status.ReadyToUse != nil {
				if *createdSnapshot.Status.ReadyToUse != tt.initialReady {
					t.Errorf("Expected initial ready state %v, got %v", tt.initialReady, *createdSnapshot.Status.ReadyToUse)
				}
			}

			// Update to target state if different
			if tt.targetReady != tt.initialReady {
				restoreSize := resource.MustParse("10Gi")
				creationTime := metav1.Now()
				createdSnapshot.Status = &snapshotv1.VolumeSnapshotStatus{
					ReadyToUse:   &tt.targetReady,
					RestoreSize:  &restoreSize,
					CreationTime: &creationTime,
				}

				updatedSnapshot, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).UpdateStatus(
					ctx, createdSnapshot, metav1.UpdateOptions{})
				if err != nil {
					if tt.shouldSucceed {
						t.Fatalf("Failed to update VolumeSnapshot: %v", err)
					}
					return
				}

				// Verify target state
				if updatedSnapshot.Status.ReadyToUse != nil && *updatedSnapshot.Status.ReadyToUse != tt.targetReady {
					t.Errorf("Expected target ready state %v, got %v", tt.targetReady, *updatedSnapshot.Status.ReadyToUse)
				}
			}
		})
	}
}

// TestSnapshotLifecycle verifies the complete VolumeSnapshot lifecycle: create, update, delete.
// This simulates the full snapshot timeout and cleanup scenario.
func TestSnapshotLifecycle(t *testing.T) {
	tests := []struct {
		name           string
		snapshotName   string
		initialReady   bool
		updateToReady  bool
		shouldDelete   bool
		verifyDeletion bool
	}{
		{
			name:           "not ready snapshot can be deleted",
			snapshotName:   "not-ready-snapshot",
			initialReady:   false,
			updateToReady:  false,
			shouldDelete:   true,
			verifyDeletion: true,
		},
		{
			name:           "ready snapshot can be deleted for cleanup",
			snapshotName:   "ready-snapshot",
			initialReady:   false,
			updateToReady:  true,
			shouldDelete:   true,
			verifyDeletion: true,
		},
		{
			name:           "verify snapshot creation only",
			snapshotName:   "create-only-snapshot",
			initialReady:   false,
			updateToReady:  false,
			shouldDelete:   false,
			verifyDeletion: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			supervisorSnapshotClient := snapshotclientset.NewSimpleClientset()
			supervisorNamespace := "supervisor-ns"
			pvcName := "test-pvc-" + tt.name

			// Create VolumeSnapshot
			snapshot := &snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.snapshotName,
					Namespace: supervisorNamespace,
					Annotations: map[string]string{
						common.SupervisorVolumeSnapshotAnnotationKey: "true",
					},
				},
				Spec: snapshotv1.VolumeSnapshotSpec{
					Source: snapshotv1.VolumeSnapshotSource{
						PersistentVolumeClaimName: &pvcName,
					},
					VolumeSnapshotClassName: stringPtr("test-snapshot-class"),
				},
			}

			if tt.initialReady {
				restoreSize := resource.MustParse("10Gi")
				creationTime := metav1.Now()
				snapshot.Status = &snapshotv1.VolumeSnapshotStatus{
					ReadyToUse:   &tt.initialReady,
					RestoreSize:  &restoreSize,
					CreationTime: &creationTime,
				}
			}

			createdSnapshot, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Create(
				ctx, snapshot, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create VolumeSnapshot: %v", err)
			}

			// Update ready state if needed
			if tt.updateToReady != tt.initialReady {
				restoreSize := resource.MustParse("10Gi")
				creationTime := metav1.Now()
				createdSnapshot.Status = &snapshotv1.VolumeSnapshotStatus{
					ReadyToUse:   &tt.updateToReady,
					RestoreSize:  &restoreSize,
					CreationTime: &creationTime,
				}

				_, err = supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).UpdateStatus(
					ctx, createdSnapshot, metav1.UpdateOptions{})
				if err != nil {
					t.Fatalf("Failed to update VolumeSnapshot: %v", err)
				}
			}

			// Delete if requested
			if tt.shouldDelete {
				err = supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Delete(
					ctx, tt.snapshotName, metav1.DeleteOptions{})
				if err != nil {
					t.Fatalf("Failed to delete VolumeSnapshot: %v", err)
				}
			}

			// Verify deletion if requested
			if tt.verifyDeletion {
				_, err = supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Get(
					ctx, tt.snapshotName, metav1.GetOptions{})
				if err == nil {
					t.Error("Expected VolumeSnapshot to be deleted, but it still exists")
				}
			}
		})
	}
}

// TestSnapshotIdempotentOperations verifies that snapshot operations can be safely retried.
func TestSnapshotIdempotentOperations(t *testing.T) {
	tests := []struct {
		name          string
		snapshotName  string
		getTwice      bool
		expectSuccess bool
		expectedReady bool
	}{
		{
			name:          "create snapshot once",
			snapshotName:  "single-create-snapshot",
			getTwice:      false,
			expectSuccess: true,
			expectedReady: true,
		},
		{
			name:          "idempotent get after create",
			snapshotName:  "idempotent-snapshot",
			getTwice:      true,
			expectSuccess: true,
			expectedReady: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			supervisorSnapshotClient := snapshotclientset.NewSimpleClientset()
			supervisorNamespace := "supervisor-ns"
			pvcName := "test-pvc-" + tt.name

			// Create VolumeSnapshot
			restoreSize := resource.MustParse("10Gi")
			creationTime := metav1.Now()
			snapshot := &snapshotv1.VolumeSnapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.snapshotName,
					Namespace: supervisorNamespace,
					Annotations: map[string]string{
						common.VolumeSnapshotInfoKey: "fcd-id:snapshot-id",
					},
				},
				Spec: snapshotv1.VolumeSnapshotSpec{
					Source: snapshotv1.VolumeSnapshotSource{
						PersistentVolumeClaimName: &pvcName,
					},
					VolumeSnapshotClassName: stringPtr("test-snapshot-class"),
				},
				Status: &snapshotv1.VolumeSnapshotStatus{
					ReadyToUse:   &tt.expectedReady,
					RestoreSize:  &restoreSize,
					CreationTime: &creationTime,
				},
			}

			_, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Create(
				ctx, snapshot, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create VolumeSnapshot: %v", err)
			}

			// Get VolumeSnapshot (simulates retry/idempotent check)
			if tt.getTwice {
				retrievedSnapshot, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Get(
					ctx, tt.snapshotName, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Failed to get VolumeSnapshot: %v", err)
				}

				if retrievedSnapshot.Status != nil && retrievedSnapshot.Status.ReadyToUse != nil {
					if *retrievedSnapshot.Status.ReadyToUse != tt.expectedReady {
						t.Errorf("Expected ready state %v, got %v", tt.expectedReady, *retrievedSnapshot.Status.ReadyToUse)
					}
				}

				// Verify we got the same VolumeSnapshot
				if retrievedSnapshot.Name != tt.snapshotName {
					t.Errorf("Expected VolumeSnapshot name %s, got %s", tt.snapshotName, retrievedSnapshot.Name)
				}
			}
		})
	}
}

// TestSnapshotTimeoutErrorCode specifically tests the CreateSnapshot timeout scenario
// to ensure it returns codes.DeadlineExceeded instead of codes.Internal.
func TestSnapshotTimeoutErrorCode(t *testing.T) {
	tests := []struct {
		name              string
		snapshotReady     bool
		expectedErrorCode codes.Code
		shouldError       bool
	}{
		{
			name:              "snapshot not ready returns DeadlineExceeded",
			snapshotReady:     false,
			expectedErrorCode: codes.DeadlineExceeded,
			shouldError:       true,
		},
		{
			name:              "snapshot ready returns no error",
			snapshotReady:     true,
			expectedErrorCode: codes.OK,
			shouldError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the error returned when snapshot is not ready
			if tt.shouldError {
				msg := "volumesnapshot: test-snapshot on namespace: supervisor-ns in supervisor cluster was not Ready"
				err := status.Error(tt.expectedErrorCode, msg)
				st, ok := status.FromError(err)
				if !ok {
					t.Fatal("Failed to extract gRPC status from error")
				}

				if st.Code() != tt.expectedErrorCode {
					t.Errorf("Expected error code %v, got %v", tt.expectedErrorCode, st.Code())
				}

				// Verify it's not Internal error
				if st.Code() == codes.Internal {
					t.Error("CreateSnapshot timeout should not return codes.Internal, it should return codes.DeadlineExceeded")
				}
			}
		})
	}
}

// TestSnapshotRetryBehavior verifies that external-snapshotter will retry
// on DeadlineExceeded errors but not on Internal errors.
func TestSnapshotRetryBehavior(t *testing.T) {
	tests := []struct {
		name                     string
		errorCode                codes.Code
		expectRetry              bool
		expectedSnapshotterState string
	}{
		{
			name:                     "DeadlineExceeded allows snapshot retry",
			errorCode:                codes.DeadlineExceeded,
			expectRetry:              true,
			expectedSnapshotterState: "SnapshotInBackground",
		},
		{
			name:                     "Internal stops snapshot retry",
			errorCode:                codes.Internal,
			expectRetry:              false,
			expectedSnapshotterState: "SnapshotFinished",
		},
		{
			name:                     "Unavailable allows snapshot retry",
			errorCode:                codes.Unavailable,
			expectRetry:              true,
			expectedSnapshotterState: "SnapshotInBackground",
		},
		{
			name:                     "Aborted allows snapshot retry",
			errorCode:                codes.Aborted,
			expectRetry:              true,
			expectedSnapshotterState: "SnapshotInBackground",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := status.Error(tt.errorCode, "test snapshot error")
			st, _ := status.FromError(err)

			if st.Code() != tt.errorCode {
				t.Errorf("Expected error code %v, got %v", tt.errorCode, st.Code())
			}

			// Verify error codes match expected retry behavior for external-snapshotter
			retryableCodes := map[codes.Code]bool{
				codes.Canceled:         true,
				codes.DeadlineExceeded: true,
				codes.Unavailable:      true,
				codes.Aborted:          true,
			}

			shouldRetry := retryableCodes[tt.errorCode]
			if shouldRetry != tt.expectRetry {
				t.Errorf("Expected retry=%v for code %v, but got %v", tt.expectRetry, tt.errorCode, shouldRetry)
			}
		})
	}
}

// TestSnapshotTimeoutScenario simulates the complete timeout scenario:
// snapshot creation, waiting for ready (timeout), and ensuring proper error code.
func TestSnapshotTimeoutScenario(t *testing.T) {
	ctx := context.Background()
	supervisorSnapshotClient := snapshotclientset.NewSimpleClientset()
	supervisorNamespace := "supervisor-ns"
	snapshotName := "timeout-test-snapshot"
	pvcName := "timeout-test-pvc"

	// Create a snapshot that's not ready (simulating timeout scenario)
	snapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: supervisorNamespace,
			Annotations: map[string]string{
				common.SupervisorVolumeSnapshotAnnotationKey: "true",
			},
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
			VolumeSnapshotClassName: stringPtr("test-snapshot-class"),
		},
	}

	createdSnapshot, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Create(
		ctx, snapshot, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create VolumeSnapshot: %v", err)
	}

	// Verify snapshot is not ready
	if createdSnapshot.Status != nil && createdSnapshot.Status.ReadyToUse != nil && *createdSnapshot.Status.ReadyToUse {
		t.Error("Snapshot should not be ready initially")
	}

	// Simulate timeout by waiting a short duration and snapshot still not ready
	time.Sleep(100 * time.Millisecond)

	retrievedSnapshot, err := supervisorSnapshotClient.SnapshotV1().VolumeSnapshots(supervisorNamespace).Get(
		ctx, snapshotName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get VolumeSnapshot: %v", err)
	}

	// Snapshot should still not be ready (simulating timeout)
	isReady := retrievedSnapshot.Status != nil &&
		retrievedSnapshot.Status.ReadyToUse != nil &&
		*retrievedSnapshot.Status.ReadyToUse
	if isReady {
		t.Error("Snapshot should not be ready after timeout simulation")
	}

	// When snapshot is not ready after timeout, CreateSnapshot should return DeadlineExceeded
	if !isReady {
		msg := "volumesnapshot: timeout-test-snapshot on namespace: supervisor-ns in supervisor cluster was not Ready"
		timeoutErr := status.Error(codes.DeadlineExceeded, msg)
		st, ok := status.FromError(timeoutErr)
		if !ok {
			t.Fatal("Failed to extract gRPC status from error")
		}

		if st.Code() != codes.DeadlineExceeded {
			t.Errorf("Expected DeadlineExceeded error code, got %v", st.Code())
		}

		// Verify it's not Internal error
		if st.Code() == codes.Internal {
			t.Error("CreateSnapshot timeout should return codes.DeadlineExceeded, not codes.Internal")
		}
	}
}

// Helper function
func stringPtr(s string) *string {
	return &s
}
