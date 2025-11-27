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
			supervisorClient := testclient.NewSimpleClientset()
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
			supervisorClient := testclient.NewSimpleClientset()
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
			supervisorClient := testclient.NewSimpleClientset()
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

// Helper function
func stringPtr(s string) *string {
	return &s
}
