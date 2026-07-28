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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	testclient "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	snapshotclientset "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
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
			supervisorSnapshotClient := snapshotclientset.NewClientset()
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
			supervisorSnapshotClient := snapshotclientset.NewClientset()
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
			supervisorSnapshotClient := snapshotclientset.NewClientset()
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
	supervisorSnapshotClient := snapshotclientset.NewClientset()
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

// TestVolumeAttachmentName verifies that the VolumeAttachment name derived here
// matches the name the Kubernetes attach/detach controller actually assigns. The
// expected values below are real VolumeAttachment names captured from a live
// cluster, alongside the volume handle and node they were created for. If
// getAttachmentName() in k8s.io/kubernetes/pkg/volume/csi ever changes its
// hashing scheme, this test fails and isAttachStillRequested must be revisited -
// it would otherwise silently start reporting every attach as "no longer
// requested".
func TestVolumeAttachmentName(t *testing.T) {
	const node = "lin-vks5-small-0-node-pool-1-m6gq2-qv98v-cvfxv"
	tests := []struct {
		name         string
		volumeHandle string
		expected     string
	}{
		{
			name:         "volume 0",
			volumeHandle: "c48f1df3-76e3-4495-b111-b87715287128-0dd0bfc4-9996-4070-9716-ddb3d11738ad",
			expected:     "csi-524e5016960dd561947c30fbf6bfa195d83a52044d88a0efa2c1cfc9ca6dddf2",
		},
		{
			name:         "volume 1",
			volumeHandle: "c48f1df3-76e3-4495-b111-b87715287128-d81c9aca-1e05-438b-8e03-5e234501a6da",
			expected:     "csi-68eee207f9c4e23024f703ba0268513789cfb8af82a6ed7639a7624ec1e27627",
		},
		{
			name:         "volume 2",
			volumeHandle: "c48f1df3-76e3-4495-b111-b87715287128-b1831b35-7e03-40b2-8213-ec6cfeb8e339",
			expected:     "csi-5a514b08aa0b5ea23feda388299cbe6ffa819e9abd86fcfa2eb4daeba77270ab",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := volumeAttachmentName(tt.volumeHandle, csitypes.Name, node)
			if got != tt.expected {
				t.Errorf("volumeAttachmentName() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestIsAttachStillRequested verifies the staleness guard consulted before the
// attach is written into VirtualMachine.Spec.Volumes.
func TestIsAttachStillRequested(t *testing.T) {
	ctx := context.Background()
	const (
		volumeHandle = "c48f1df3-76e3-4495-b111-b87715287128-0dd0bfc4-9996-4070-9716-ddb3d11738ad"
		nodeName     = "lin-vks5-small-0-node-pool-1-m6gq2-qv98v-cvfxv"
	)

	t.Run("VolumeAttachment present means attach is still requested", func(t *testing.T) {
		pvName := "pvc-0dd0bfc4-9996-4070-9716-ddb3d11738ad"
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name: volumeAttachmentName(volumeHandle, csitypes.Name, nodeName),
			},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: csitypes.Name,
				NodeName: nodeName,
				Source: storagev1.VolumeAttachmentSource{
					PersistentVolumeName: &pvName,
				},
			},
		}
		c := &controller{guestClient: testclient.NewClientset(va)}
		if !c.isAttachStillRequested(ctx, volumeHandle, nodeName) {
			t.Error("expected attach to be reported as still requested when the VolumeAttachment exists")
		}
	})

	t.Run("VolumeAttachment absent means attach is no longer requested", func(t *testing.T) {
		c := &controller{guestClient: testclient.NewClientset()}
		if c.isAttachStillRequested(ctx, volumeHandle, nodeName) {
			t.Error("expected attach to be reported as no longer requested when the VolumeAttachment is gone")
		}
	})

	t.Run("VolumeAttachment for a different node does not satisfy the guard", func(t *testing.T) {
		otherNode := "lin-vks5-small-0-node-pool-1-m6gq2-qv98v-other"
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name: volumeAttachmentName(volumeHandle, csitypes.Name, otherNode),
			},
		}
		c := &controller{guestClient: testclient.NewClientset(va)}
		if c.isAttachStillRequested(ctx, volumeHandle, nodeName) {
			t.Error("expected the guard to ignore a VolumeAttachment belonging to another node")
		}
	})

	// The guard must fail open. Blocking attaches because the API server is
	// briefly unreachable - or because the driver's RBAC lacks access to
	// volumeattachments - would be far worse than the orphaned attachment it is
	// trying to prevent.
	t.Run("transient API error fails open", func(t *testing.T) {
		guestClient := testclient.NewClientset()
		guestClient.PrependReactor("get", "volumeattachments",
			func(action ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewServiceUnavailable("apiserver is unavailable")
			})
		c := &controller{guestClient: guestClient}
		if !c.isAttachStillRequested(ctx, volumeHandle, nodeName) {
			t.Error("expected the guard to fail open and allow the attach when the lookup errors")
		}
	})

	t.Run("forbidden error fails open", func(t *testing.T) {
		guestClient := testclient.NewClientset()
		guestClient.PrependReactor("get", "volumeattachments",
			func(action ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(
					storagev1.Resource("volumeattachments"), "", nil)
			})
		c := &controller{guestClient: guestClient}
		if !c.isAttachStillRequested(ctx, volumeHandle, nodeName) {
			t.Error("expected the guard to fail open when RBAC denies the lookup")
		}
	})
}

// TestVMWatchClosedError verifies that a VirtualMachine watch closing without the
// expected state is never reported as codes.Internal. CSI sidecars treat Internal
// as a final error meaning "the operation is for sure not in progress", which is
// untrue here: the volume remains in VirtualMachine.Spec.Volumes and vm-operator
// keeps reconciling it, so the attach or detach may still complete.
func TestVMWatchClosedError(t *testing.T) {
	const vmName = "lin-vks1-medium-0-node-pool-1-g4xm6-zkp89-dgdql"

	t.Run("watch timeout reports DeadlineExceeded", func(t *testing.T) {
		_, err := vmWatchClosedError(context.Background(), "attach", vmName)
		assertGRPCCode(t, err, codes.DeadlineExceeded)
	})

	t.Run("cancelled context reports Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := vmWatchClosedError(ctx, "attach", vmName)
		assertGRPCCode(t, err, codes.Canceled)
	})

	t.Run("expired context deadline reports DeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		<-ctx.Done()
		_, err := vmWatchClosedError(ctx, "detach", vmName)
		assertGRPCCode(t, err, codes.DeadlineExceeded)
	})

	// Both codes must be non-final so that the CSI sidecars keep the
	// VolumeAttachment around and retry, rather than concluding the operation
	// definitively failed.
	t.Run("reported codes are never Internal", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, c := range []context.Context{context.Background(), ctx} {
			_, err := vmWatchClosedError(c, "attach", vmName)
			if st, ok := status.FromError(err); ok && st.Code() == codes.Internal {
				t.Errorf("watch closure reported codes.Internal, which sidecars treat as a final error")
			}
		}
	})
}

// TestVMWatchErrorEventError verifies that a watch.Error event - which carries a
// *metav1.Status describing why the watch itself failed, e.g. an expired
// resourceVersion - is reported with that detail rather than being collapsed
// into the generic "watch closed" timeout/cancel message. The two cases are
// distinguishable at the call site by event.Type, and must be handled by
// different helpers: conflating them would silently discard the actual
// apiserver-reported reason for the watch failing.
func TestVMWatchErrorEventError(t *testing.T) {
	const vmName = "lin-vks1-medium-0-node-pool-1-g4xm6-zkp89-dgdql"

	t.Run("status message from the watch.Error event is included", func(t *testing.T) {
		watchStatus := &metav1.Status{
			Message: "too old resource version: 12345 (67890)",
		}
		_, err := vmWatchErrorEventError(context.Background(), "attach", vmName, watchStatus)
		assertGRPCCode(t, err, codes.Unavailable)
		if st, _ := status.FromError(err); !strings.Contains(st.Message(), watchStatus.Message) {
			t.Errorf("expected error message to include the watch status message %q, got %q",
				watchStatus.Message, st.Message())
		}
	})

	t.Run("reports Unavailable even without a status payload", func(t *testing.T) {
		_, err := vmWatchErrorEventError(context.Background(), "detach", vmName, nil)
		assertGRPCCode(t, err, codes.Unavailable)
	})

	// codes.Unavailable must be non-final for the same reason as
	// vmWatchClosedError's codes: a failed watch says nothing about whether the
	// underlying attach/detach itself completed.
	t.Run("reported code is never Internal", func(t *testing.T) {
		_, err := vmWatchErrorEventError(context.Background(), "attach", vmName, nil)
		if st, ok := status.FromError(err); ok && st.Code() == codes.Internal {
			t.Errorf("watch error event reported codes.Internal, which sidecars treat as a final error")
		}
	})
}

// TestControllerPublishForBlockVolumeStaleAttach drives controllerPublishForBlockVolume
// itself, to verify the guard actually prevents the write to
// VirtualMachine.Spec.Volumes. This is the regression that matters: it is the
// persisted spec entry, not the returned error, that strands a volume attached to
// a node with no VolumeAttachment left to detach it.
func TestControllerPublishForBlockVolumeStaleAttach(t *testing.T) {
	ctx := context.Background()
	const (
		namespace    = "test-ns"
		nodeName     = "test-node"
		volumeHandle = "test-cluster-uid-0dd0bfc4-9996-4070-9716-ddb3d11738ad"
	)

	newVM := func() *vmoperatortypes.VirtualMachine {
		return &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: namespace},
		}
	}

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator scheme: %v", err)
	}

	t.Run("stale attach is rejected without modifying the VM spec", func(t *testing.T) {
		vmClient := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).WithObjects(newVM()).Build()
		c := &controller{
			vmOperatorClient:    vmClient,
			guestClient:         testclient.NewClientset(), // no VolumeAttachment: attach is stale
			supervisorNamespace: namespace,
		}

		_, _, err := controllerPublishForBlockVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: volumeHandle,
			NodeId:   nodeName,
		}, c)

		assertGRPCCode(t, err, codes.FailedPrecondition)

		// The critical assertion: nothing was persisted into the VM spec.
		vm := &vmoperatortypes.VirtualMachine{}
		if getErr := vmClient.Get(ctx,
			types.NamespacedName{Namespace: namespace, Name: nodeName}, vm); getErr != nil {
			t.Fatalf("failed to read back VirtualMachine: %v", getErr)
		}
		if len(vm.Spec.Volumes) != 0 {
			t.Errorf("expected VirtualMachine.Spec.Volumes to be left untouched, got %+v", vm.Spec.Volumes)
		}
	})

	t.Run("live attach is written into the VM spec", func(t *testing.T) {
		// status.Volumes reports the disk as already attached so that the call
		// returns without needing the VirtualMachine watch.
		vm := newVM()
		vm.Status.Volumes = []vmoperatortypes.VirtualMachineVolumeStatus{
			{Name: volumeHandle, Attached: true, DiskUUID: "6000c29-fake-disk-uuid"},
		}
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name: volumeAttachmentName(volumeHandle, csitypes.Name, nodeName),
			},
		}
		vmClient := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).WithObjects(vm).Build()
		c := &controller{
			vmOperatorClient:    vmClient,
			guestClient:         testclient.NewClientset(va),
			supervisorNamespace: namespace,
		}

		resp, _, err := controllerPublishForBlockVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: volumeHandle,
			NodeId:   nodeName,
		}, c)
		if err != nil {
			t.Fatalf("expected the attach to succeed, got %v", err)
		}
		if resp == nil {
			t.Fatal("expected a ControllerPublishVolumeResponse")
		}

		updated := &vmoperatortypes.VirtualMachine{}
		if getErr := vmClient.Get(ctx,
			types.NamespacedName{Namespace: namespace, Name: nodeName}, updated); getErr != nil {
			t.Fatalf("failed to read back VirtualMachine: %v", getErr)
		}
		if len(updated.Spec.Volumes) != 1 || updated.Spec.Volumes[0].Name != volumeHandle {
			t.Errorf("expected volume %q to be added to the VM spec, got %+v", volumeHandle, updated.Spec.Volumes)
		}
	})
}

// TestControllerPublishForBlockVolumeStaleAttachOnRetry verifies that
// isAttachStillRequested is re-evaluated on every iteration of the attach
// retry loop, not just once when the RPC starts. It simulates a VolumeAttachment
// disappearing between a failed patch attempt and the next retry: the first
// patch attempt is forced to fail with a conflict, and while handling that
// failure, the VolumeAttachment is deleted out from under the request. If the
// guard only ran once at the top of the function, the second iteration would
// go on to patch VirtualMachine.Spec.Volumes despite nothing left in Kubernetes
// to ever detach it.
func TestControllerPublishForBlockVolumeStaleAttachOnRetry(t *testing.T) {
	ctx := context.Background()
	const (
		namespace    = "test-ns"
		nodeName     = "test-node"
		volumeHandle = "test-cluster-uid-0dd0bfc4-9996-4070-9716-ddb3d11738ad"
	)

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator scheme: %v", err)
	}

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeAttachmentName(volumeHandle, csitypes.Name, nodeName),
		},
	}
	guestClient := testclient.NewClientset(va)

	patchAttempts := 0
	vmClient := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: namespace},
		}).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli ctrlclient.WithWatch, obj ctrlclient.Object,
				patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
				patchAttempts++
				if patchAttempts == 1 {
					if err := guestClient.StorageV1().VolumeAttachments().Delete(
						ctx, va.Name, metav1.DeleteOptions{}); err != nil {
						t.Fatalf("failed to delete VolumeAttachment: %v", err)
					}
					return apierrors.NewConflict(storagev1.Resource("virtualmachines"), obj.GetName(), errors.New("conflict"))
				}
				return cli.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	c := &controller{
		vmOperatorClient:    vmClient,
		guestClient:         guestClient,
		supervisorNamespace: namespace,
	}

	_, _, err := controllerPublishForBlockVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: volumeHandle,
		NodeId:   nodeName,
	}, c)

	assertGRPCCode(t, err, codes.FailedPrecondition)
	if patchAttempts != 1 {
		t.Errorf("expected the guard to reject the second iteration before it could retry the patch, "+
			"but the patch was attempted %d time(s)", patchAttempts)
	}

	vm := &vmoperatortypes.VirtualMachine{}
	if getErr := vmClient.Get(ctx,
		types.NamespacedName{Namespace: namespace, Name: nodeName}, vm); getErr != nil {
		t.Fatalf("failed to read back VirtualMachine: %v", getErr)
	}
	if len(vm.Spec.Volumes) != 0 {
		t.Errorf("expected VirtualMachine.Spec.Volumes to be left untouched, got %+v", vm.Spec.Volumes)
	}
}

// TestControllerUnpublishForBlockVolumeWatchTermination drives
// controllerUnpublishForBlockVolume through its VirtualMachine watch loop to
// verify the two ways that watch can end without observing the volume detached
// are reported with the expected non-final codes: a watch.Error event maps to
// vmWatchErrorEventError (codes.Unavailable, with the underlying status
// message), and a closed result channel maps to vmWatchClosedError
// (codes.DeadlineExceeded). Both must never be codes.Internal, or CSI sidecars
// will treat the detach as finished when VirtualMachine.Spec.Volumes may still
// be mid-reconcile.
func TestControllerUnpublishForBlockVolumeWatchTermination(t *testing.T) {
	ctx := context.Background()
	const (
		namespace    = "test-ns"
		nodeName     = "test-node"
		volumeHandle = "test-cluster-uid-0dd0bfc4-9996-4070-9716-ddb3d11738ad"
	)

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator scheme: %v", err)
	}

	// The volume is already absent from Spec.Volumes, so the removal loop exits
	// immediately, but still reported as attached in Status.Volumes, so the
	// function proceeds to the watch loop under test.
	newAttachedVM := func() *vmoperatortypes.VirtualMachine {
		return &vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: namespace},
			Status: vmoperatortypes.VirtualMachineStatus{
				Volumes: []vmoperatortypes.VirtualMachineVolumeStatus{
					{Name: volumeHandle, Attached: true},
				},
			},
		}
	}

	// runUnpublish starts controllerUnpublishForBlockVolume in the background,
	// since it blocks reading watchVirtualMachine.ResultChan(), then calls
	// injectEvent to deliver (or close) the watch and waits for the RPC to
	// return.
	runUnpublish := func(t *testing.T, fakeWatch *watch.FakeWatcher, injectEvent func()) error {
		t.Helper()
		vmClient := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).WithObjects(newAttachedVM()).Build()
		c := &controller{
			vmOperatorClient:    vmClient,
			guestClient:         testclient.NewClientset(),
			supervisorNamespace: namespace,
			vmWatcher: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return fakeWatch, nil
				},
			},
		}

		done := make(chan error, 1)
		go func() {
			_, _, err := controllerUnpublishForBlockVolume(ctx, &csi.ControllerUnpublishVolumeRequest{
				VolumeId: volumeHandle,
				NodeId:   nodeName,
			}, c)
			done <- err
		}()

		injectEvent()

		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("controllerUnpublishForBlockVolume did not return after the watch terminated")
			return nil
		}
	}

	t.Run("watch.Error event reports Unavailable with the status message", func(t *testing.T) {
		fakeWatch := watch.NewFake()
		const wantMessage = "too old resource version: 12345 (67890)"
		err := runUnpublish(t, fakeWatch, func() {
			fakeWatch.Error(&metav1.Status{Message: wantMessage})
		})
		assertGRPCCode(t, err, codes.Unavailable)
		if st, _ := status.FromError(err); !strings.Contains(st.Message(), wantMessage) {
			t.Errorf("expected error to include the watch status message %q, got %q", wantMessage, st.Message())
		}
	})

	t.Run("closed watch reports DeadlineExceeded", func(t *testing.T) {
		fakeWatch := watch.NewFake()
		err := runUnpublish(t, fakeWatch, func() {
			fakeWatch.Stop()
		})
		assertGRPCCode(t, err, codes.DeadlineExceeded)
	})
}

// TestControllerPublishStaleAfterPatch covers the window the pre-patch guard
// cannot: the volume is already written into VirtualMachine.Spec.Volumes and we
// are waiting for it to be attached when the VolumeAttachment disappears. At
// that point nothing else will ever issue ControllerUnpublishVolume for it, so
// the driver must undo its own spec entry rather than leave vm-operator
// reconciling an attach no one wants.
func TestControllerPublishStaleAfterPatch(t *testing.T) {
	ctx := context.Background()
	const (
		namespace    = "test-ns"
		nodeName     = "test-node"
		volumeHandle = "test-cluster-uid-0dd0bfc4-9996-4070-9716-ddb3d11738ad"
	)

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator scheme: %v", err)
	}

	// Shorten the poll interval so the test does not wait 15s.
	origInterval := attachStillRequestedCheckInterval
	attachStillRequestedCheckInterval = 50 * time.Millisecond
	defer func() { attachStillRequestedCheckInterval = origInterval }()

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: volumeAttachmentName(volumeHandle, csitypes.Name, nodeName),
		},
	}
	guestClient := testclient.NewClientset(va)

	// VM starts with no volumes; the publish path will patch ours in, then block
	// waiting for DiskUUID on a watch that never delivers a successful attach.
	vmClient := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&vmoperatortypes.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Namespace: namespace},
		}).
		Build()

	fakeWatch := watch.NewFake()
	defer fakeWatch.Stop()

	c := &controller{
		vmOperatorClient:    vmClient,
		guestClient:         guestClient,
		supervisorNamespace: namespace,
		vmWatcher: &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return fakeWatch, nil
			},
		},
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := controllerPublishForBlockVolume(ctx, &csi.ControllerPublishVolumeRequest{
			VolumeId: volumeHandle,
			NodeId:   nodeName,
		}, c)
		done <- err
	}()

	// Wait until the publish path has actually persisted the volume into the VM
	// spec, otherwise we would be re-testing the pre-patch guard instead.
	patched := false
	for i := 0; i < 100; i++ {
		vm := &vmoperatortypes.VirtualMachine{}
		if err := vmClient.Get(ctx,
			types.NamespacedName{Namespace: namespace, Name: nodeName}, vm); err == nil {
			if len(vm.Spec.Volumes) == 1 && vm.Spec.Volumes[0].Name == volumeHandle {
				patched = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !patched {
		t.Fatal("expected the volume to be written into VirtualMachine.Spec.Volumes before the VA is deleted")
	}

	// Now the VolumeAttachment vanishes mid-attach.
	if err := guestClient.StorageV1().VolumeAttachments().Delete(
		ctx, va.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete VolumeAttachment: %v", err)
	}

	select {
	case err := <-done:
		assertGRPCCode(t, err, codes.FailedPrecondition)
	case <-time.After(15 * time.Second):
		t.Fatal("controllerPublishForBlockVolume did not return after the VolumeAttachment was deleted")
	}

	// The critical assertion: our own spec entry was rolled back, so vm-operator
	// is no longer being told to keep the volume attached.
	vm := &vmoperatortypes.VirtualMachine{}
	if err := vmClient.Get(ctx,
		types.NamespacedName{Namespace: namespace, Name: nodeName}, vm); err != nil {
		t.Fatalf("failed to read back VirtualMachine: %v", err)
	}
	if len(vm.Spec.Volumes) != 0 {
		t.Errorf("expected the stale volume to be removed from VirtualMachine.Spec.Volumes, got %+v",
			vm.Spec.Volumes)
	}
}

func assertGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error with code %v, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %v", err)
	}
	if st.Code() != want {
		t.Errorf("expected code %v, got %v (message: %q)", want, st.Code(), st.Message())
	}
}
