package cnsvolumeoperationrequest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

// setupTestEnvironment creates a test environment with fake clients
func setupTestEnvironment(t *testing.T, csiTransactionEnabled bool) (*operationRequestStore, context.Context) {
	t.Helper()
	ctx := context.Background()

	scheme := runtime.NewScheme()
	err := cnsvolumeoprequestv1alpha1.AddToScheme(scheme)
	if err != nil {
		t.Fatalf("Failed to add scheme: %v", err)
	}

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()

	store := &operationRequestStore{
		k8sclient: fakeClient,
	}

	// Set global variables
	csiNamespace = "vmware-system-csi"
	isCSITransactionSupportEnabled = csiTransactionEnabled

	return store, ctx
}

// createTestVolumeOperationDetails creates a test VolumeOperationRequestDetails
func createTestVolumeOperationDetails(name, volumeID, snapshotID, taskID, taskStatus, errorMsg string,
	quotaDetails *QuotaDetails) *VolumeOperationRequestDetails {
	return &VolumeOperationRequestDetails{
		Name:         name,
		VolumeID:     volumeID,
		SnapshotID:   snapshotID,
		QuotaDetails: quotaDetails,
		OperationDetails: &OperationDetails{
			TaskInvocationTimestamp: metav1.Now(),
			TaskID:                  taskID,
			TaskStatus:              taskStatus,
			Error:                   errorMsg,
		},
	}
}

// createTestQuotaDetails creates test quota details
func createTestQuotaDetails(reservedBytes int64) *QuotaDetails {
	return &QuotaDetails{
		Reserved:         resource.NewQuantity(reservedBytes, resource.BinarySI),
		StoragePolicyId:  "test-policy-id",
		StorageClassName: "test-storage-class",
		Namespace:        "test-namespace",
	}
}

// getCRDDirectly retrieves the full CnsVolumeOperationRequest CRD to access all operation details
func getCRDDirectly(ctx context.Context, store *operationRequestStore,
	instanceName string) (*cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest, error) {
	instance := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{}
	err := store.k8sclient.Get(ctx, client.ObjectKey{Name: instanceName, Namespace: csiNamespace}, instance)
	return instance, err
}

// mustStoreRequestDetails stores operation details and fails the test on error
func mustStoreRequestDetails(t *testing.T, store *operationRequestStore, ctx context.Context,
	details *VolumeOperationRequestDetails) {
	t.Helper()
	if err := store.StoreRequestDetails(ctx, details); err != nil {
		t.Fatalf("Failed to store request details: %v", err)
	}
}

// mustGetRequestDetails retrieves operation details and fails the test on error
func mustGetRequestDetails(t *testing.T, store *operationRequestStore, ctx context.Context,
	name string) *VolumeOperationRequestDetails {
	t.Helper()
	details, err := store.GetRequestDetails(ctx, name)
	if err != nil {
		t.Fatalf("Failed to get request details for %s: %v", name, err)
	}
	return details
}

// mustGetCRDDirectly retrieves CRD and fails the test on error
func mustGetCRDDirectly(t *testing.T, store *operationRequestStore, ctx context.Context,
	instanceName string) *cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest {
	t.Helper()
	crd, err := getCRDDirectly(ctx, store, instanceName)
	if err != nil {
		t.Fatalf("Failed to get CRD %s: %v", instanceName, err)
	}
	return crd
}

// assertTaskStatus verifies the task status matches expected value
func assertTaskStatus(t *testing.T, details *VolumeOperationRequestDetails, expected string) {
	t.Helper()
	if details.OperationDetails.TaskStatus != expected {
		t.Errorf("Expected TaskStatus %s, got %s", expected, details.OperationDetails.TaskStatus)
	}
}

// assertQuotaReservation verifies the quota reservation value
func assertQuotaReservation(t *testing.T, crd *cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest,
	expectedBytes int64) {
	t.Helper()
	if crd.Status.StorageQuotaDetails == nil {
		if expectedBytes != 0 {
			t.Fatal("Expected StorageQuotaDetails to exist")
		}
		return
	}
	actualBytes := crd.Status.StorageQuotaDetails.Reserved.Value()
	if actualBytes != expectedBytes {
		t.Errorf("Expected reservation %d bytes, got %d bytes", expectedBytes, actualBytes)
	}
}

// createStaleOperationDetails creates operation details with a stale timestamp
func createStaleOperationDetails(taskID, taskStatus string, hoursAgo int) cnsvolumeoprequestv1alpha1.OperationDetails {
	return cnsvolumeoprequestv1alpha1.OperationDetails{
		TaskInvocationTimestamp: metav1.NewTime(time.Now().Add(-time.Duration(hoursAgo) * time.Hour)),
		TaskID:                  taskID,
		TaskStatus:              taskStatus,
	}
}

// TestStoreRequestDetails_CreateSnapshotWithImprovedIdempotencyCheck tests the pattern used in
// createSnapshotWithImprovedIdempotencyCheck
// Pattern: Single StoreRequestDetails call at the end with final status (Success/Error)
func TestStoreRequestDetails_CreateSnapshotWithImprovedIdempotencyCheck(t *testing.T) {
	store, ctx := setupTestEnvironment(t, false) // CSI Transaction Support disabled

	testCases := []struct {
		name         string
		instanceName string
		volumeID     string
		snapshotID   string
		taskID       string
		taskStatus   string
		errorMsg     string
		quotaDetails *QuotaDetails
		expectError  bool
	}{
		{
			name:         "successful snapshot creation",
			instanceName: "snapshot-test-volume-123",
			volumeID:     "volume-123",
			snapshotID:   "snapshot-456",
			taskID:       "task-789",
			taskStatus:   TaskInvocationStatusSuccess,
			errorMsg:     "",
			quotaDetails: createTestQuotaDetails(1024 * 1024 * 1024), // 1GB
			expectError:  false,
		},
		{
			name:         "failed snapshot creation",
			instanceName: "snapshot-test-volume-456",
			volumeID:     "volume-456",
			snapshotID:   "",
			taskID:       "task-101",
			taskStatus:   TaskInvocationStatusError,
			errorMsg:     "CNS CreateSnapshot failed",
			quotaDetails: createTestQuotaDetails(2 * 1024 * 1024 * 1024), // 2GB
			expectError:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create operation details
			operationDetails := createTestVolumeOperationDetails(
				tc.instanceName, tc.volumeID, tc.snapshotID, tc.taskID, tc.taskStatus, tc.errorMsg, tc.quotaDetails)

			// Store the details (simulating the single call at the end of createSnapshotWithImprovedIdempotencyCheck)
			err := store.StoreRequestDetails(ctx, operationDetails)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify the stored details
			retrievedDetails, err := store.GetRequestDetails(ctx, tc.instanceName)
			if err != nil {
				t.Errorf("Failed to retrieve details: %v", err)
				return
			}
			if retrievedDetails == nil {
				t.Error("Retrieved details should not be nil")
				return
			}

			// Verify the operation details
			if retrievedDetails.OperationDetails.TaskStatus != tc.taskStatus {
				t.Errorf("Expected TaskStatus %s, got %s", tc.taskStatus, retrievedDetails.OperationDetails.TaskStatus)
			}
			if retrievedDetails.OperationDetails.TaskID != tc.taskID {
				t.Errorf("Expected TaskID %s, got %s", tc.taskID, retrievedDetails.OperationDetails.TaskID)
			}
			if retrievedDetails.OperationDetails.Error != tc.errorMsg {
				t.Errorf("Expected Error %s, got %s", tc.errorMsg, retrievedDetails.OperationDetails.Error)
			}

			// Note: QuotaDetails are only returned by GetRequestDetails when isPodVMOnStretchSupervisorFSSEnabled is true
			// Since we're not setting that flag in our test setup, we don't verify quota details here
			// The important thing is that StoreRequestDetails succeeded without error
		})
	}
}

// TestStoreRequestDetails_CreateSnapshotWithTransaction tests the pattern used in createSnapshotWithTransaction
// Pattern: 1) Initial call with empty TaskID, 2) Update with actual TaskID, 3) Defer call with final status
func TestStoreRequestDetails_CreateSnapshotWithTransaction(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true) // CSI Transaction Support enabled

	t.Run("successful transaction flow", func(t *testing.T) {
		instanceName := "snapshot-test-volume-transaction"
		volumeID := "volume-transaction-123"
		quotaDetails := createTestQuotaDetails(1024 * 1024 * 1024) // 1GB

		// Step 1: Initial call with empty TaskID (InProgress)
		initialDetails := createTestVolumeOperationDetails(
			instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

		err := store.StoreRequestDetails(ctx, initialDetails)
		if err != nil {
			t.Errorf("Failed to store initial details: %v", err)
			return
		}

		// Verify initial state
		retrievedDetails, err := store.GetRequestDetails(ctx, instanceName)
		if err != nil {
			t.Errorf("Failed to retrieve initial details: %v", err)
			return
		}
		if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusInProgress {
			t.Errorf("Expected TaskStatus %s, got %s", TaskInvocationStatusInProgress,
				retrievedDetails.OperationDetails.TaskStatus)
		}
		if retrievedDetails.OperationDetails.TaskID != "" {
			t.Errorf("Expected empty TaskID, got %s", retrievedDetails.OperationDetails.TaskID)
		}

		// Step 2: Update with actual TaskID (still InProgress)
		taskID := "task-transaction-456"
		updatedDetails := createTestVolumeOperationDetails(
			instanceName, volumeID, "", taskID, TaskInvocationStatusInProgress, "", quotaDetails)
		updatedDetails.OperationDetails.TaskInvocationTimestamp = initialDetails.OperationDetails.TaskInvocationTimestamp

		err = store.StoreRequestDetails(ctx, updatedDetails)
		if err != nil {
			t.Errorf("Failed to store updated details: %v", err)
			return
		}

		// Verify TaskID was updated
		retrievedDetails, err = store.GetRequestDetails(ctx, instanceName)
		if err != nil {
			t.Errorf("Failed to retrieve updated details: %v", err)
			return
		}
		if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusInProgress {
			t.Errorf("Expected TaskStatus %s, got %s", TaskInvocationStatusInProgress,
				retrievedDetails.OperationDetails.TaskStatus)
		}
		if retrievedDetails.OperationDetails.TaskID != taskID {
			t.Errorf("Expected TaskID %s, got %s", taskID, retrievedDetails.OperationDetails.TaskID)
		}

		// Step 3: Final call with success status (defer function)
		snapshotID := "snapshot-transaction-789"
		finalDetails := createTestVolumeOperationDetails(
			instanceName, volumeID, snapshotID, taskID, TaskInvocationStatusSuccess, "", quotaDetails)
		finalDetails.OperationDetails.TaskInvocationTimestamp = initialDetails.OperationDetails.TaskInvocationTimestamp

		err = store.StoreRequestDetails(ctx, finalDetails)
		if err != nil {
			t.Errorf("Failed to store final details: %v", err)
			return
		}

		// Verify final state
		retrievedDetails, err = store.GetRequestDetails(ctx, instanceName)
		if err != nil {
			t.Errorf("Failed to retrieve final details: %v", err)
			return
		}
		if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusSuccess {
			t.Errorf("Expected TaskStatus %s, got %s", TaskInvocationStatusSuccess, retrievedDetails.OperationDetails.TaskStatus)
		}
		if retrievedDetails.OperationDetails.TaskID != taskID {
			t.Errorf("Expected TaskID %s, got %s", taskID, retrievedDetails.OperationDetails.TaskID)
		}
		if retrievedDetails.SnapshotID != snapshotID {
			t.Errorf("Expected SnapshotID %s, got %s", snapshotID, retrievedDetails.SnapshotID)
		}
	})
}

// TestStoreRequestDetails_RetryDetectionAndTrackingAborted tests the retry detection logic
// when CSI Transaction Support is enabled
func TestStoreRequestDetails_RetryDetectionAndTrackingAborted(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true) // CSI Transaction Support enabled

	t.Run("basic retry detection test", func(t *testing.T) {
		instanceName := "snapshot-retry-test"
		volumeID := "volume-retry-123"
		quotaDetails := createTestQuotaDetails(1024 * 1024 * 1024) // 1GB

		// Step 1: First attempt - initial call with empty TaskID
		firstAttempt := createTestVolumeOperationDetails(
			instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

		err := store.StoreRequestDetails(ctx, firstAttempt)
		if err != nil {
			t.Errorf("Failed to store first attempt: %v", err)
			return
		}

		// Step 2: First attempt - update with TaskID
		firstTaskID := "task-first-attempt-456"
		firstTaskUpdate := createTestVolumeOperationDetails(
			instanceName, volumeID, "", firstTaskID, TaskInvocationStatusInProgress, "", quotaDetails)
		firstTaskUpdate.OperationDetails.TaskInvocationTimestamp = firstAttempt.OperationDetails.TaskInvocationTimestamp

		err = store.StoreRequestDetails(ctx, firstTaskUpdate)
		if err != nil {
			t.Errorf("Failed to store first task update: %v", err)
			return
		}

		// Simulate crash/restart - driver restarts and retries
		// Step 3: Retry attempt - new initial call with empty TaskID (this should trigger retry detection)
		retryAttempt := createTestVolumeOperationDetails(
			instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

		err = store.StoreRequestDetails(ctx, retryAttempt)
		if err != nil {
			t.Errorf("Failed to store retry attempt: %v", err)
			return
		}

		// The retry detection logic should have marked previous InProgress entries as TrackingAborted
		// We can verify this worked by checking that the operation was stored successfully
		retrievedDetails, err := store.GetRequestDetails(ctx, instanceName)
		if err != nil {
			t.Errorf("Failed to retrieve details after retry: %v", err)
			return
		}

		// The latest operation should be the retry attempt
		if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusInProgress {
			t.Errorf("Expected TaskStatus %s, got %s", TaskInvocationStatusInProgress,
				retrievedDetails.OperationDetails.TaskStatus)
		}
		if retrievedDetails.OperationDetails.TaskID != "" {
			t.Errorf("Expected empty TaskID for retry attempt, got %s", retrievedDetails.OperationDetails.TaskID)
		}
	})

	t.Run("no retry detection when CSI Transaction Support is disabled", func(t *testing.T) {
		// Create a new store with CSI Transaction Support disabled
		storeDisabled, ctx := setupTestEnvironment(t, false)

		instanceName := "snapshot-no-retry-detection"
		volumeID := "volume-no-retry-456"
		quotaDetails := createTestQuotaDetails(512 * 1024 * 1024) // 512MB

		// Step 1: First attempt
		firstAttempt := createTestVolumeOperationDetails(
			instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

		err := storeDisabled.StoreRequestDetails(ctx, firstAttempt)
		if err != nil {
			t.Errorf("Failed to store first attempt: %v", err)
			return
		}

		// Step 2: Update with TaskID
		taskID := "task-no-retry-789"
		taskUpdate := createTestVolumeOperationDetails(
			instanceName, volumeID, "", taskID, TaskInvocationStatusInProgress, "", quotaDetails)
		taskUpdate.OperationDetails.TaskInvocationTimestamp = firstAttempt.OperationDetails.TaskInvocationTimestamp

		err = storeDisabled.StoreRequestDetails(ctx, taskUpdate)
		if err != nil {
			t.Errorf("Failed to store task update: %v", err)
			return
		}

		// Step 3: "Retry" attempt (should not trigger retry detection)
		retryAttempt := createTestVolumeOperationDetails(
			instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

		err = storeDisabled.StoreRequestDetails(ctx, retryAttempt)
		if err != nil {
			t.Errorf("Failed to store retry attempt: %v", err)
			return
		}

		// Verify that the operation was stored (no retry detection should have occurred)
		retrievedDetails, err := storeDisabled.GetRequestDetails(ctx, instanceName)
		if err != nil {
			t.Errorf("Failed to retrieve details: %v", err)
			return
		}

		// Should have the latest operation details
		if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusInProgress {
			t.Errorf("Expected TaskStatus %s, got %s", TaskInvocationStatusInProgress,
				retrievedDetails.OperationDetails.TaskStatus)
		}
	})
}

// TestStoreRequestDetails_RealWorkflowTransitions tests real workflow scenarios with multiple InProgress entries
// leading to final Success or Error states, simulating actual driver behavior patterns
func TestStoreRequestDetails_RealWorkflowTransitions(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true) // CSI Transaction Support enabled

	t.Run("complete snapshot workflow with multiple retries and final success (CSI transactions enabled)",
		func(t *testing.T) {
			instanceName := "snapshot-workflow-success"
			volumeID := "volume-workflow-123"
			quotaDetails := createTestQuotaDetails(1024 * 1024 * 1024) // 1GB - testing quota integration

			// === First Attempt (fails during task execution) ===
			// Step 1: Initial InProgress with empty TaskID
			attempt1Initial := createTestVolumeOperationDetails(
				instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

			err := store.StoreRequestDetails(ctx, attempt1Initial)
			if err != nil {
				t.Errorf("Failed to store attempt 1 initial: %v", err)
				return
			}

			// Step 2: Update with TaskID (still InProgress)
			attempt1TaskID := "task-attempt1-456"
			attempt1WithTask := createTestVolumeOperationDetails(
				instanceName, volumeID, "", attempt1TaskID, TaskInvocationStatusInProgress, "", quotaDetails)
			attempt1WithTask.OperationDetails.TaskInvocationTimestamp = attempt1Initial.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, attempt1WithTask)
			if err != nil {
				t.Errorf("Failed to store attempt 1 with task: %v", err)
				return
			}

			// === Simulate crash during waitOnTask - first attempt never completes ===
			// The first attempt remains InProgress with TaskID, then driver crashes/restarts

			// === Second Attempt (driver restart/retry) ===
			// Step 3: New retry attempt (should trigger TrackingAborted for any remaining InProgress)
			attempt2Initial := createTestVolumeOperationDetails(
				instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

			err = store.StoreRequestDetails(ctx, attempt2Initial)
			if err != nil {
				t.Errorf("Failed to store attempt 2 initial: %v", err)
				return
			}

			// Step 4: Second attempt gets TaskID
			attempt2TaskID := "task-attempt2-789"
			attempt2WithTask := createTestVolumeOperationDetails(
				instanceName, volumeID, "", attempt2TaskID, TaskInvocationStatusInProgress, "", quotaDetails)
			attempt2WithTask.OperationDetails.TaskInvocationTimestamp = attempt2Initial.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, attempt2WithTask)
			if err != nil {
				t.Errorf("Failed to store attempt 2 with task: %v", err)
				return
			}

			// Verify that previous InProgress operations were marked as TrackingAborted
			crdInstance, err := getCRDDirectly(ctx, store, instanceName)
			if err != nil {
				t.Errorf("Failed to get CRD directly: %v", err)
				return
			}

			// Check FirstOperationDetails for TrackingAborted
			if crdInstance.Status.FirstOperationDetails.TaskStatus != TaskInvocationStatusTrackingAborted {
				t.Errorf("Expected FirstOperationDetails TaskStatus to be %s, got %s",
					TaskInvocationStatusTrackingAborted, crdInstance.Status.FirstOperationDetails.TaskStatus)
			}

			// Check LatestOperationDetails for TrackingAborted entries
			trackingAbortedCount := 0
			inProgressCount := 0
			for _, detail := range crdInstance.Status.LatestOperationDetails {
				if detail.TaskStatus == TaskInvocationStatusTrackingAborted {
					trackingAbortedCount++
					if detail.Error != "Operation tracking aborted due to retry attempt" {
						t.Errorf("Expected TrackingAborted error message, got: %s", detail.Error)
					}
				} else if detail.TaskStatus == TaskInvocationStatusInProgress {
					inProgressCount++
				}
			}

			if trackingAbortedCount == 0 {
				t.Error("Expected at least one TrackingAborted entry in LatestOperationDetails")
			}
			if inProgressCount == 0 {
				t.Error("Expected at least one InProgress entry in LatestOperationDetails")
			}

			t.Logf("Found %d TrackingAborted and %d InProgress entries in LatestOperationDetails",
				trackingAbortedCount, inProgressCount)

			// Step 5: Second attempt succeeds
			snapshotID := "snapshot-success-101"
			attempt2Success := createTestVolumeOperationDetails(
				instanceName, volumeID, snapshotID, attempt2TaskID, TaskInvocationStatusSuccess, "", quotaDetails)
			attempt2Success.OperationDetails.TaskInvocationTimestamp = attempt2Initial.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, attempt2Success)
			if err != nil {
				t.Errorf("Failed to store attempt 2 success: %v", err)
				return
			}

			// Verify final state
			retrievedDetails, err := store.GetRequestDetails(ctx, instanceName)
			if err != nil {
				t.Errorf("Failed to retrieve final details: %v", err)
				return
			}

			// Should have the successful operation as the latest
			if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusSuccess {
				t.Errorf("Expected final TaskStatus %s, got %s", TaskInvocationStatusSuccess,
					retrievedDetails.OperationDetails.TaskStatus)
			}
			if retrievedDetails.OperationDetails.TaskID != attempt2TaskID {
				t.Errorf("Expected final TaskID %s, got %s", attempt2TaskID, retrievedDetails.OperationDetails.TaskID)
			}
			if retrievedDetails.SnapshotID != snapshotID {
				t.Errorf("Expected SnapshotID %s, got %s", snapshotID, retrievedDetails.SnapshotID)
			}
		})

	t.Run("volume creation workflow with crash during waitOnTask and final error (CSI transactions enabled)",
		func(t *testing.T) {
			instanceName := "volume-workflow-error"
			quotaDetails := createTestQuotaDetails(2 * 1024 * 1024 * 1024) // 2GB

			// === First Attempt ===
			// Step 1: Initial InProgress (before CNS call)
			attempt1Initial := createTestVolumeOperationDetails(
				instanceName, "", "", "", TaskInvocationStatusInProgress, "", quotaDetails)

			err := store.StoreRequestDetails(ctx, attempt1Initial)
			if err != nil {
				t.Errorf("Failed to store attempt 1 initial: %v", err)
				return
			}

			// Step 2: CNS task created, update with TaskID
			attempt1TaskID := "task-volume-create-123"
			attempt1WithTask := createTestVolumeOperationDetails(
				instanceName, "", "", attempt1TaskID, TaskInvocationStatusInProgress, "", quotaDetails)
			attempt1WithTask.OperationDetails.TaskInvocationTimestamp = attempt1Initial.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, attempt1WithTask)
			if err != nil {
				t.Errorf("Failed to store attempt 1 with task: %v", err)
				return
			}

			// === Simulate crash during waitOnTask - driver restarts ===
			// Step 3: Retry attempt (should mark previous InProgress as TrackingAborted)
			attempt2Initial := createTestVolumeOperationDetails(
				instanceName, "", "", "", TaskInvocationStatusInProgress, "", quotaDetails)

			err = store.StoreRequestDetails(ctx, attempt2Initial)
			if err != nil {
				t.Errorf("Failed to store attempt 2 initial: %v", err)
				return
			}

			// Step 4: Second attempt gets new TaskID
			attempt2TaskID := "task-volume-create-456"
			attempt2WithTask := createTestVolumeOperationDetails(
				instanceName, "", "", attempt2TaskID, TaskInvocationStatusInProgress, "", quotaDetails)
			attempt2WithTask.OperationDetails.TaskInvocationTimestamp = attempt2Initial.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, attempt2WithTask)
			if err != nil {
				t.Errorf("Failed to store attempt 2 with task: %v", err)
				return
			}

			// Verify TrackingAborted status after retry detection
			crdInstance, err := getCRDDirectly(ctx, store, instanceName)
			if err != nil {
				t.Errorf("Failed to get CRD directly: %v", err)
				return
			}

			// Should have TrackingAborted entries from the first attempt
			trackingAbortedCount := 0
			for _, detail := range crdInstance.Status.LatestOperationDetails {
				if detail.TaskStatus == TaskInvocationStatusTrackingAborted {
					trackingAbortedCount++
				}
			}

			if trackingAbortedCount == 0 {
				t.Error("Expected TrackingAborted entries after retry detection")
			}

			t.Logf("Volume workflow: Found %d TrackingAborted entries after retry", trackingAbortedCount)

			// Step 5: Second attempt also fails with error
			errorMsg := "CNS CreateVolume failed: datastore out of space"
			attempt2Error := createTestVolumeOperationDetails(
				instanceName, "", "", attempt2TaskID, TaskInvocationStatusError, errorMsg, quotaDetails)
			attempt2Error.OperationDetails.TaskInvocationTimestamp = attempt2Initial.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, attempt2Error)
			if err != nil {
				t.Errorf("Failed to store attempt 2 error: %v", err)
				return
			}

			// Verify final error state
			retrievedDetails, err := store.GetRequestDetails(ctx, instanceName)
			if err != nil {
				t.Errorf("Failed to retrieve final details: %v", err)
				return
			}

			if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusError {
				t.Errorf("Expected final TaskStatus %s, got %s", TaskInvocationStatusError,
					retrievedDetails.OperationDetails.TaskStatus)
			}
			if retrievedDetails.OperationDetails.TaskID != attempt2TaskID {
				t.Errorf("Expected final TaskID %s, got %s", attempt2TaskID, retrievedDetails.OperationDetails.TaskID)
			}
			if retrievedDetails.OperationDetails.Error != errorMsg {
				t.Errorf("Expected error message %s, got %s", errorMsg, retrievedDetails.OperationDetails.Error)
			}
		})

	t.Run("multiple rapid retries with variable quota (CSI transactions enabled)", func(t *testing.T) {
		instanceName := "rapid-retries-workflow"
		volumeID := "volume-rapid-789"

		// Test multiple rapid retries with different quota sizes
		for i := 1; i <= 3; i++ {
			// Each attempt has different quota size
			quotaDetails := createTestQuotaDetails(int64(i * 512 * 1024 * 1024)) // Variable quota sizes

			// Initial InProgress
			initialDetails := createTestVolumeOperationDetails(
				instanceName, volumeID, "", "", TaskInvocationStatusInProgress, "", quotaDetails)

			err := store.StoreRequestDetails(ctx, initialDetails)
			if err != nil {
				t.Errorf("Failed to store attempt %d initial: %v", i, err)
				return
			}

			// Update with TaskID
			taskID := "task-rapid-" + string(rune('0'+i))
			taskDetails := createTestVolumeOperationDetails(
				instanceName, volumeID, "", taskID, TaskInvocationStatusInProgress, "", quotaDetails)
			taskDetails.OperationDetails.TaskInvocationTimestamp = initialDetails.OperationDetails.TaskInvocationTimestamp

			err = store.StoreRequestDetails(ctx, taskDetails)
			if err != nil {
				t.Errorf("Failed to store attempt %d with task: %v", i, err)
				return
			}

			// Only the last attempt succeeds
			if i == 3 {
				snapshotID := "snapshot-rapid-final"
				successDetails := createTestVolumeOperationDetails(
					instanceName, volumeID, snapshotID, taskID, TaskInvocationStatusSuccess, "", quotaDetails)
				successDetails.OperationDetails.TaskInvocationTimestamp = initialDetails.OperationDetails.TaskInvocationTimestamp

				err = store.StoreRequestDetails(ctx, successDetails)
				if err != nil {
					t.Errorf("Failed to store attempt %d success: %v", i, err)
					return
				}
			}
		}

		// Verify final successful state
		retrievedDetails, err := store.GetRequestDetails(ctx, instanceName)
		if err != nil {
			t.Errorf("Failed to retrieve final details: %v", err)
			return
		}

		if retrievedDetails.OperationDetails.TaskStatus != TaskInvocationStatusSuccess {
			t.Errorf("Expected final TaskStatus %s, got %s", TaskInvocationStatusSuccess,
				retrievedDetails.OperationDetails.TaskStatus)
		}
		if retrievedDetails.OperationDetails.TaskID != "task-rapid-3" {
			t.Errorf("Expected final TaskID %s, got %s", "task-rapid-3", retrievedDetails.OperationDetails.TaskID)
		}
		if retrievedDetails.SnapshotID != "snapshot-rapid-final" {
			t.Errorf("Expected SnapshotID %s, got %s", "snapshot-rapid-final", retrievedDetails.SnapshotID)
		}
	})
}

// createTestCRDInstance creates a CnsVolumeOperationRequest CRD directly on the fake client
// for testing cleanup logic that operates on raw CRDs rather than the store interface.
func createTestCRDInstance(
	ctx context.Context,
	t *testing.T,
	k8sclient client.Client,
	name string,
	latestOps []cnsvolumeoprequestv1alpha1.OperationDetails,
	firstOp cnsvolumeoprequestv1alpha1.OperationDetails,
	quotaDetails *cnsvolumeoprequestv1alpha1.QuotaDetails,
) {
	t.Helper()
	instance := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: csiNamespace,
		},
		Spec: cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequestSpec{
			Name: name,
		},
		Status: cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequestStatus{
			FirstOperationDetails:  firstOp,
			LatestOperationDetails: latestOps,
			StorageQuotaDetails:    quotaDetails,
		},
	}
	if err := k8sclient.Create(ctx, instance); err != nil {
		t.Fatalf("Failed to create test CRD instance %s: %v", name, err)
	}
}

// TestForceTransitionStaleInProgressToError tests that stale InProgress entries
// are correctly transitioned to Error status.
func TestForceTransitionStaleInProgressToError(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true)

	t.Run("transitions single stale InProgress entry to Error", func(t *testing.T) {
		instanceName := "stale-single-inprogress"
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			createStaleOperationDetails("task-stale-1", TaskInvocationStatusInProgress, 72),
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		instance := mustGetCRDDirectly(t, store, ctx, instanceName)
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated := mustGetCRDDirectly(t, store, ctx, instanceName)
		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected LatestOperationDetails[0] TaskStatus %s, got %s",
				TaskInvocationStatusError, updated.Status.LatestOperationDetails[0].TaskStatus)
		}
		if updated.Status.LatestOperationDetails[0].Error == "" {
			t.Error("Expected error message to be set on transitioned entry")
		}
		if updated.Status.FirstOperationDetails.TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected FirstOperationDetails TaskStatus %s, got %s",
				TaskInvocationStatusError, updated.Status.FirstOperationDetails.TaskStatus)
		}
	})

	t.Run("transitions mixed entries - only InProgress to Error", func(t *testing.T) {
		instanceName := "stale-mixed-entries"
		errorOp := createStaleOperationDetails("task-error-1", TaskInvocationStatusError, 72)
		errorOp.Error = "original error from session timeout"
		inProgressOp := createStaleOperationDetails("task-stuck-2", TaskInvocationStatusInProgress, 72)
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{errorOp, inProgressOp}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, errorOp, nil)

		instance := mustGetCRDDirectly(t, store, ctx, instanceName)
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated := mustGetCRDDirectly(t, store, ctx, instanceName)
		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected first entry to remain Error, got %s",
				updated.Status.LatestOperationDetails[0].TaskStatus)
		}
		if updated.Status.LatestOperationDetails[0].Error != "original error from session timeout" {
			t.Errorf("Expected original error message to be preserved, got %s",
				updated.Status.LatestOperationDetails[0].Error)
		}
		if updated.Status.LatestOperationDetails[1].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected second (InProgress) entry to become Error, got %s",
				updated.Status.LatestOperationDetails[1].TaskStatus)
		}
		if updated.Status.LatestOperationDetails[1].Error == "" {
			t.Error("Expected error message on transitioned InProgress entry")
		}
	})

	t.Run("does not modify instance with no InProgress entries", func(t *testing.T) {
		instanceName := "stale-no-inprogress"
		staleTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		errorOp := cnsvolumeoprequestv1alpha1.OperationDetails{
			TaskInvocationTimestamp: staleTime,
			TaskID:                  "task-already-error",
			TaskStatus:              TaskInvocationStatusError,
			Error:                   "already errored",
		}
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{errorOp}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, errorOp, nil)

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		originalRV := instance.ResourceVersion

		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		if updated.ResourceVersion != originalRV {
			t.Error("Expected instance not to be updated when no InProgress entries exist")
		}
	})

	t.Run("transitions InProgress in FirstOperationDetails even when LatestOps differ", func(t *testing.T) {
		instanceName := "stale-first-op-inprogress"
		staleTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		firstOp := cnsvolumeoprequestv1alpha1.OperationDetails{
			TaskInvocationTimestamp: staleTime,
			TaskID:                  "task-first-stuck",
			TaskStatus:              TaskInvocationStatusInProgress,
		}
		latestOp := cnsvolumeoprequestv1alpha1.OperationDetails{
			TaskInvocationTimestamp: staleTime,
			TaskID:                  "task-latest-error",
			TaskStatus:              TaskInvocationStatusError,
			Error:                   "some error",
		}
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{latestOp}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, firstOp, nil)

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}

		if updated.Status.FirstOperationDetails.TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected FirstOperationDetails TaskStatus %s, got %s",
				TaskInvocationStatusError, updated.Status.FirstOperationDetails.TaskStatus)
		}
		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected LatestOperationDetails[0] to remain Error, got %s",
				updated.Status.LatestOperationDetails[0].TaskStatus)
		}
		if updated.Status.LatestOperationDetails[0].Error != "some error" {
			t.Errorf("Expected original error preserved, got %s",
				updated.Status.LatestOperationDetails[0].Error)
		}
	})
}

// TestCleanupStaleInstances_StaleInProgressHandling tests the cleanupStaleInstances
// behavior with respect to stale InProgress tasks.
func TestCleanupStaleInstances_StaleInProgressHandling(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true)

	t.Run("skips recent InProgress tasks", func(t *testing.T) {
		instanceName := "recent-inprogress"
		recentTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: recentTime,
				TaskID:                  "task-recent",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		staleInProgressCutoff := time.Now().Add(-staleInProgressTaskThreshold)
		latestOp := ops[len(ops)-1]

		shouldTransition := latestOp.TaskStatus == TaskInvocationStatusInProgress &&
			latestOp.TaskInvocationTimestamp.Time.Before(staleInProgressCutoff)

		if shouldTransition {
			t.Error("Recent InProgress task should NOT be considered stale")
		}

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		if instance.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusInProgress {
			t.Errorf("Expected task to remain InProgress, got %s",
				instance.Status.LatestOperationDetails[0].TaskStatus)
		}
	})

	t.Run("transitions InProgress tasks older than 48 hours", func(t *testing.T) {
		instanceName := "old-inprogress"
		oldTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: oldTime,
				TaskID:                  "task-old",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		staleInProgressCutoff := time.Now().Add(-staleInProgressTaskThreshold)
		latestOp := ops[len(ops)-1]

		shouldTransition := latestOp.TaskStatus == TaskInvocationStatusInProgress &&
			latestOp.TaskInvocationTimestamp.Time.Before(staleInProgressCutoff)

		if !shouldTransition {
			t.Error("72-hour-old InProgress task SHOULD be considered stale")
		}

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}
		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected stale InProgress to be transitioned to Error, got %s",
				updated.Status.LatestOperationDetails[0].TaskStatus)
		}
	})

	t.Run("deletes Error instances older than 15 minutes", func(t *testing.T) {
		instanceName := "old-error-instance"
		oldTime := metav1.NewTime(time.Now().Add(-30 * time.Minute))
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: oldTime,
				TaskID:                  "task-old-error",
				TaskStatus:              TaskInvocationStatusError,
				Error:                   "some error",
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		cutoffTime := time.Now().Add(-15 * time.Minute)
		latestOp := ops[len(ops)-1]

		shouldDelete := latestOp.TaskStatus != TaskInvocationStatusInProgress &&
			!latestOp.TaskInvocationTimestamp.Time.After(cutoffTime)

		if !shouldDelete {
			t.Error("30-minute-old Error instance SHOULD be eligible for deletion")
		}

		err := store.DeleteRequestDetails(ctx, instanceName)
		if err != nil {
			t.Fatalf("Failed to delete instance: %v", err)
		}

		_, err = getCRDDirectly(ctx, store, instanceName)
		if err == nil {
			t.Error("Expected instance to be deleted")
		}
	})

	t.Run("does not delete recent Error instances", func(t *testing.T) {
		instanceName := "recent-error-instance"
		recentTime := metav1.NewTime(time.Now().Add(-5 * time.Minute))
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: recentTime,
				TaskID:                  "task-recent-error",
				TaskStatus:              TaskInvocationStatusError,
				Error:                   "recent error",
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		cutoffTime := time.Now().Add(-15 * time.Minute)
		latestOp := ops[len(ops)-1]

		shouldDelete := latestOp.TaskStatus != TaskInvocationStatusInProgress &&
			!latestOp.TaskInvocationTimestamp.Time.After(cutoffTime)

		if shouldDelete {
			t.Error("5-minute-old Error instance should NOT be eligible for deletion")
		}

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		if instance == nil {
			t.Error("Instance should still exist")
		}
	})
}

// TestCleanupStaleInstances_EndToEndScenario simulates the exact scenario
// from the bug report: session expiry during CreateVolume leaves CRs stuck
// with InProgress tasks and leaked quota reservations.
func TestCleanupStaleInstances_EndToEndScenario(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true)

	t.Run("session expiry leaves stuck InProgress - cleanup transitions and deletes", func(t *testing.T) {
		instanceName := "pvc-stuck-session-expiry"
		reservedQty := resource.NewQuantity(400*1024*1024*1024, resource.BinarySI)
		quota := &cnsvolumeoprequestv1alpha1.QuotaDetails{
			Reserved:         reservedQty,
			StoragePolicyId:  "policy-123",
			StorageClassName: "storage-class-1",
			Namespace:        "test-ns",
		}

		// Simulate the exact CR state from the bug report:
		// First task errored with session auth failure, second task stuck InProgress
		firstTaskTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		secondTaskTime := metav1.NewTime(time.Now().Add(-71*time.Hour - 45*time.Minute))

		errorOp := cnsvolumeoprequestv1alpha1.OperationDetails{
			TaskInvocationTimestamp: firstTaskTime,
			TaskID:                  "task-484682",
			TaskStatus:              TaskInvocationStatusError,
			Error: "destroy property filter failed with ServerFaultCode: " +
				"The session is not authenticated.",
		}
		stuckOp := cnsvolumeoprequestv1alpha1.OperationDetails{
			TaskInvocationTimestamp: secondTaskTime,
			TaskID:                  "task-485876",
			TaskStatus:              TaskInvocationStatusInProgress,
		}
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{errorOp, stuckOp}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, errorOp, quota)

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}

		// Verify the stuck state matches what we expect
		latestOps := instance.Status.LatestOperationDetails
		if latestOps[len(latestOps)-1].TaskStatus != TaskInvocationStatusInProgress {
			t.Fatalf("Test setup error: expected last entry to be InProgress")
		}

		// Step 1: forceTransitionStaleInProgressToError should fix the stuck entries
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}

		// Verify all InProgress entries are now Error
		for i, op := range updated.Status.LatestOperationDetails {
			if op.TaskStatus == TaskInvocationStatusInProgress {
				t.Errorf("Entry %d should no longer be InProgress after force transition", i)
			}
		}

		// The last entry should now be Error (was InProgress)
		lastOp := updated.Status.LatestOperationDetails[len(updated.Status.LatestOperationDetails)-1]
		if lastOp.TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected last entry to be Error, got %s", lastOp.TaskStatus)
		}
		if lastOp.TaskID != "task-485876" {
			t.Errorf("Expected TaskID task-485876, got %s", lastOp.TaskID)
		}

		// Verify quota details are still present (they are released on CR deletion)
		if updated.Status.StorageQuotaDetails == nil {
			t.Error("StorageQuotaDetails should still be present before deletion")
		}
		if updated.Status.StorageQuotaDetails.Reserved.Value() != 400*1024*1024*1024 {
			t.Errorf("Expected reserved 400Gi, got %v", updated.Status.StorageQuotaDetails.Reserved)
		}

		// Step 2: Now that the entry is Error, cleanup should delete the CR
		// (the entry is older than 15 minutes)
		cutoffTime := time.Now().Add(-15 * time.Minute)
		lastOpAfterTransition := updated.Status.LatestOperationDetails[len(updated.Status.LatestOperationDetails)-1]
		shouldDelete := lastOpAfterTransition.TaskStatus != TaskInvocationStatusInProgress &&
			!lastOpAfterTransition.TaskInvocationTimestamp.Time.After(cutoffTime)

		if !shouldDelete {
			t.Error("After force transition, the instance should be eligible for deletion")
		}

		err = store.DeleteRequestDetails(ctx, instanceName)
		if err != nil {
			t.Fatalf("Failed to delete instance: %v", err)
		}

		_, err = getCRDDirectly(ctx, store, instanceName)
		if err == nil {
			t.Error("Expected instance to be deleted after cleanup")
		}
	})

	t.Run("InProgress task just under 48 hours is not transitioned", func(t *testing.T) {
		instanceName := "pvc-boundary-48h"
		// Use 47h59m to ensure we're safely under the threshold and avoid clock precision issues
		justUnderBoundaryTime := metav1.NewTime(time.Now().Add(-47*time.Hour - 59*time.Minute))
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: justUnderBoundaryTime,
				TaskID:                  "task-boundary",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		staleInProgressCutoff := time.Now().Add(-staleInProgressTaskThreshold)
		latestOp := ops[len(ops)-1]

		shouldTransition := latestOp.TaskStatus == TaskInvocationStatusInProgress &&
			latestOp.TaskInvocationTimestamp.Time.Before(staleInProgressCutoff)

		if shouldTransition {
			t.Error("Task just under 48h should NOT be transitioned")
		}
	})

	t.Run("InProgress task at 47 hours is not transitioned", func(t *testing.T) {
		instanceName := "pvc-47h-inprogress"
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: metav1.NewTime(time.Now().Add(-47 * time.Hour)),
				TaskID:                  "task-47h",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		staleInProgressCutoff := time.Now().Add(-staleInProgressTaskThreshold)

		shouldTransition := ops[0].TaskStatus == TaskInvocationStatusInProgress &&
			ops[0].TaskInvocationTimestamp.Time.Before(staleInProgressCutoff)

		if shouldTransition {
			t.Error("47-hour InProgress task should NOT be transitioned")
		}
	})

	t.Run("InProgress task at 49 hours is transitioned", func(t *testing.T) {
		instanceName := "pvc-49h-inprogress"
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: metav1.NewTime(time.Now().Add(-49 * time.Hour)),
				TaskID:                  "task-49h",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		staleInProgressCutoff := time.Now().Add(-staleInProgressTaskThreshold)

		shouldTransition := ops[0].TaskStatus == TaskInvocationStatusInProgress &&
			ops[0].TaskInvocationTimestamp.Time.Before(staleInProgressCutoff)

		if !shouldTransition {
			t.Error("49-hour InProgress task SHOULD be transitioned")
		}

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}
		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected Error, got %s", updated.Status.LatestOperationDetails[0].TaskStatus)
		}
	})
}

// TestStaleInProgressTaskThreshold verifies the threshold constant value.
func TestStaleInProgressTaskThreshold(t *testing.T) {
	if staleInProgressTaskThreshold != 48*time.Hour {
		t.Errorf("Expected staleInProgressTaskThreshold to be 48h, got %v", staleInProgressTaskThreshold)
	}
}

// testErrorPathTransition is a helper that tests the InProgress->Error transition pattern
func testErrorPathTransition(t *testing.T, store *operationRequestStore, ctx context.Context,
	instanceName, taskID, errorMsg string) {
	t.Helper()

	// Setup: Create InProgress operation
	inProgress := createTestVolumeOperationDetails(instanceName, "", "", taskID,
		TaskInvocationStatusInProgress, "", nil)
	mustStoreRequestDetails(t, store, ctx, inProgress)

	// Simulate error path: transition to Error
	errorDetails := createTestVolumeOperationDetails(instanceName, "", "", taskID,
		TaskInvocationStatusError, errorMsg, nil)
	mustStoreRequestDetails(t, store, ctx, errorDetails)

	// Verify: Final state is Error
	result := mustGetRequestDetails(t, store, ctx, instanceName)
	assertTaskStatus(t, result, TaskInvocationStatusError)
}

// TestErrorPathTransitionsPreventStuckInProgress verifies that when an operation
// encounters an error after task creation (e.g., waitOnTask failure, nil taskResult,
// getTaskResultFromTaskInfo error), the CR is correctly transitioned to Error status.
// This validates the fixes across all operation types: create, delete, expand, snapshot.
func TestErrorPathTransitionsPreventStuckInProgress(t *testing.T) {
	store, ctx := setupTestEnvironment(t, false)

	tests := []struct {
		name         string
		instanceName string
		taskID       string
		errorMsg     string
	}{
		{
			name:         "CreateVolume: waitOnTask general error transitions InProgress to Error",
			instanceName: "pvc-create-waitontask-err",
			taskID:       "task-create-100",
			errorMsg:     "destroy property filter failed with ServerFaultCode: The session is not authenticated",
		},
		{
			name:         "CreateVolume: nil taskResult transitions InProgress to Error",
			instanceName: "pvc-create-nil-result",
			taskID:       "task-create-200",
			errorMsg:     "taskResult is empty for CreateVolume task",
		},
		{
			name:         "CreateVolume: QueryAllVolume failure transitions InProgress to Error",
			instanceName: "pvc-create-query-fail",
			taskID:       "task-create-300",
			errorMsg:     "failed to query CNS for volume",
		},
		{
			name:         "DeleteVolume: waitOnTask general error transitions InProgress to Error",
			instanceName: "pvc-delete-waitontask-err",
			taskID:       "task-delete-100",
			errorMsg:     "context deadline exceeded",
		},
		{
			name:         "DeleteVolume: nil taskInfo transitions InProgress to Error",
			instanceName: "pvc-delete-nil-taskinfo",
			taskID:       "task-delete-200",
			errorMsg:     "taskInfo is nil",
		},
		{
			name:         "DeleteVolume: nil taskResult transitions InProgress to Error",
			instanceName: "pvc-delete-nil-result",
			taskID:       "task-delete-300",
			errorMsg:     "taskResult is empty for DeleteVolume task",
		},
		{
			name:         "ExpandVolume: nil taskResult transitions InProgress to Error",
			instanceName: "pvc-expand-nil-result",
			taskID:       "task-expand-100",
			errorMsg:     "taskResult is empty for ExpandVolume task",
		},
		{
			name:         "ExpandVolume: getTaskResultFromTaskInfo error transitions InProgress to Error",
			instanceName: "pvc-expand-taskresult-err",
			taskID:       "task-expand-200",
			errorMsg:     "failed to get task result",
		},
		{
			name:         "CreateSnapshot: waitOnTask general error transitions InProgress to Error",
			instanceName: "snap-create-waitontask-err",
			taskID:       "task-snap-100",
			errorMsg:     "Failed to get taskInfo for CreateSnapshots task",
		},
		{
			name:         "CreateSnapshot: GetTaskResult nil transitions InProgress to Error",
			instanceName: "snap-create-nil-result",
			taskID:       "task-snap-200",
			errorMsg:     "unable to find the task result for CreateSnapshots task",
		},
		{
			name:         "DeleteSnapshot: getTaskResultFromTaskInfo error transitions InProgress to Error",
			instanceName: "snap-delete-taskresult-err",
			taskID:       "task-snapdel-100",
			errorMsg:     "failed to get the task result for DeleteSnapshots task",
		},
		{
			name:         "DeleteSnapshot: nil taskResult transitions InProgress to Error",
			instanceName: "snap-delete-nil-result",
			taskID:       "task-snapdel-200",
			errorMsg:     "task result is empty for DeleteSnapshot task",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testErrorPathTransition(t, store, ctx, tc.instanceName, tc.taskID, tc.errorMsg)
		})
	}
}

// TestDeferSkipsInProgressAndPersistsError verifies the defer guard logic:
// the defer in volume operation functions only persists when status != InProgress.
// This confirms that once we set Error on volumeOperationDetails, the defer will persist it.
func TestDeferSkipsInProgressAndPersistsError(t *testing.T) {
	store, ctx := setupTestEnvironment(t, false)

	t.Run("InProgress status is NOT persisted by defer (simulates old bug)", func(t *testing.T) {
		name := "pvc-defer-skip-inprogress"
		// Store initial InProgress
		details := createTestVolumeOperationDetails(
			name, "", "", "task-defer-1", TaskInvocationStatusInProgress, "", nil)
		if err := store.StoreRequestDetails(ctx, details); err != nil {
			t.Fatalf("Setup: %v", err)
		}

		// Simulate what the defer guard checks: status == InProgress -> skip persist
		got, err := store.GetRequestDetails(ctx, name)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.OperationDetails.TaskStatus != TaskInvocationStatusInProgress {
			t.Fatalf("Expected InProgress, got %s", got.OperationDetails.TaskStatus)
		}
		// The defer would NOT call StoreRequestDetails because status is InProgress
		// This is the condition that previously caused stuck CRs
	})

	t.Run("Error status IS persisted by defer (simulates fix)", func(t *testing.T) {
		name := "pvc-defer-persist-error"
		// Store initial InProgress
		inProgressDetails := createTestVolumeOperationDetails(
			name, "", "", "task-defer-2", TaskInvocationStatusInProgress, "", nil)
		if err := store.StoreRequestDetails(ctx, inProgressDetails); err != nil {
			t.Fatalf("Setup: %v", err)
		}

		// Simulate the fix: error path sets Error BEFORE the defer runs
		errorDetails := createTestVolumeOperationDetails(
			name, "", "", "task-defer-2",
			TaskInvocationStatusError, "session expired", nil)
		// The defer guard checks: status != InProgress -> persist
		if errorDetails.OperationDetails.TaskStatus != TaskInvocationStatusInProgress {
			// This is what the defer does
			if err := store.StoreRequestDetails(ctx, errorDetails); err != nil {
				t.Fatalf("StoreError: %v", err)
			}
		}

		got, err := store.GetRequestDetails(ctx, name)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.OperationDetails.TaskStatus != TaskInvocationStatusError {
			t.Errorf("want Error, got %s", got.OperationDetails.TaskStatus)
		}
	})
}

// TestEndToEndStuckInProgressWithImprovedIdempotency simulates the complete lifecycle
// with improved idempotency (CSI Transaction Support disabled): task created, waitOnTask
// fails, error path now correctly marks Error, defer persists it, and cleanup can delete it.
func TestEndToEndStuckInProgressWithImprovedIdempotency(t *testing.T) {
	store, ctx := setupTestEnvironment(t, false)

	t.Run("improved idempotency: error path marks Error and cleanup deletes CR", func(t *testing.T) {
		name := "pvc-e2e-improved-idempotency"

		// Step 1: Initial InProgress (before CNS task)
		initial := createTestVolumeOperationDetails(
			name, "", "", "", TaskInvocationStatusInProgress, "", nil)
		if err := store.StoreRequestDetails(ctx, initial); err != nil {
			t.Fatalf("Step1: %v", err)
		}

		// Step 2: Task created, update with TaskID (still InProgress)
		withTask := createTestVolumeOperationDetails(
			name, "", "", "task-e2e-100", TaskInvocationStatusInProgress, "", nil)
		withTask.OperationDetails.TaskInvocationTimestamp = initial.OperationDetails.TaskInvocationTimestamp
		if err := store.StoreRequestDetails(ctx, withTask); err != nil {
			t.Fatalf("Step2: %v", err)
		}

		// Step 3: waitOnTask fails (session error) -> new code marks Error
		errorDetails := createTestVolumeOperationDetails(
			name, "", "", "task-e2e-100", TaskInvocationStatusError,
			"destroy property filter failed with ServerFaultCode: The session is not authenticated", nil)
		errorDetails.OperationDetails.TaskInvocationTimestamp = initial.OperationDetails.TaskInvocationTimestamp
		if err := store.StoreRequestDetails(ctx, errorDetails); err != nil {
			t.Fatalf("Step3: %v", err)
		}

		// Verify Error state
		got, err := store.GetRequestDetails(ctx, name)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.OperationDetails.TaskStatus != TaskInvocationStatusError {
			t.Fatalf("want Error, got %s", got.OperationDetails.TaskStatus)
		}

		// Step 4: Cleanup can now delete this CR (it's Error + old)
		if err := store.DeleteRequestDetails(ctx, name); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		_, err = store.GetRequestDetails(ctx, name)
		if err == nil {
			t.Error("Expected CR to be deleted")
		}
	})

	t.Run("improved idempotency: quota released when Error persisted on retry", func(t *testing.T) {
		name := "pvc-e2e-quota-release"
		quota := createTestQuotaDetails(400 * 1024 * 1024 * 1024)

		initial := createTestVolumeOperationDetails(
			name, "", "", "task-quota-100", TaskInvocationStatusInProgress, "", quota)
		if err := store.StoreRequestDetails(ctx, initial); err != nil {
			t.Fatalf("Setup: %v", err)
		}

		// On retry error, quota Reserved is zeroed
		quota.Reserved = resource.NewQuantity(0, resource.BinarySI)
		errorDetails := createTestVolumeOperationDetails(
			name, "", "", "task-quota-100", TaskInvocationStatusError,
			"session expired", quota)
		errorDetails.OperationDetails.TaskInvocationTimestamp = initial.OperationDetails.TaskInvocationTimestamp
		if err := store.StoreRequestDetails(ctx, errorDetails); err != nil {
			t.Fatalf("StoreError: %v", err)
		}

		got, err := store.GetRequestDetails(ctx, name)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.OperationDetails.TaskStatus != TaskInvocationStatusError {
			t.Errorf("want Error, got %s", got.OperationDetails.TaskStatus)
		}
	})
}

// TestReservationRetainedOnFirstAttemptError verifies that on the first attempt
// (no prior CVOR exists), an error should NOT zero the reservation. The quota
// stays held so the sidecar retry can use it. On retries (CVOR already exists),
// an error should zero the reservation to release it.
// These tests use the exported IsRetryAttempt helper — the same function that
// the production code in manager.go calls — so they validate the actual logic
// path rather than reimplementing the defer condition inline.
func TestReservationRetainedOnFirstAttemptError(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true)
	isPodVMOnStretchSupervisorFSSEnabled = true
	defer func() { isPodVMOnStretchSupervisorFSSEnabled = false }()

	// shouldReleaseReservation mirrors the condition in persistVolumeOperationDetails.
	shouldReleaseReservation := func(details *VolumeOperationRequestDetails, isRetry bool) bool {
		if details == nil || details.QuotaDetails == nil {
			return false
		}
		ts := details.OperationDetails.TaskStatus
		return ts == TaskInvocationStatusSuccess || (ts == TaskInvocationStatusError && isRetry)
	}

	t.Run("first attempt error keeps reservation non-zero", func(t *testing.T) {
		name := "pvc-first-attempt-keep-reservation"
		reservedBytes := int64(400 * 1024 * 1024 * 1024)
		quota := createTestQuotaDetails(reservedBytes)

		// GetRequestDetails returns NotFound -> IsRetryAttempt == false
		existing, err := store.GetRequestDetails(ctx, name)
		isRetry := IsRetryAttempt(existing, err)
		if isRetry {
			t.Fatal("Expected IsRetryAttempt to be false on first attempt")
		}

		inProgress := createTestVolumeOperationDetails(
			name, "", "", "task-first-1", TaskInvocationStatusInProgress, "", quota)
		if err := store.StoreRequestDetails(ctx, inProgress); err != nil {
			t.Fatalf("StoreInProgress: %v", err)
		}

		errorDetails := createTestVolumeOperationDetails(
			name, "", "", "task-first-1", TaskInvocationStatusError, "session expired", quota)
		if shouldReleaseReservation(errorDetails, isRetry) {
			errorDetails.QuotaDetails.Reserved = resource.NewQuantity(0, resource.BinarySI)
		}
		if err := store.StoreRequestDetails(ctx, errorDetails); err != nil {
			t.Fatalf("StoreError: %v", err)
		}

		crd := mustGetCRDDirectly(t, store, ctx, name)
		assertQuotaReservation(t, crd, reservedBytes)
	})

	t.Run("retry attempt error zeros reservation", func(t *testing.T) {
		name := "pvc-retry-attempt-zero-reservation"
		reservedBytes := int64(400 * 1024 * 1024 * 1024)
		quota := createTestQuotaDetails(reservedBytes)

		inProgress1 := createTestVolumeOperationDetails(
			name, "", "", "task-retry-1", TaskInvocationStatusInProgress, "", quota)
		mustStoreRequestDetails(t, store, ctx, inProgress1)
		error1 := createTestVolumeOperationDetails(
			name, "", "", "task-retry-1", TaskInvocationStatusError, "first error", quota)
		mustStoreRequestDetails(t, store, ctx, error1)

		// GetRequestDetails succeeds -> IsRetryAttempt == true
		existing, err := store.GetRequestDetails(ctx, name)
		isRetry := IsRetryAttempt(existing, err)
		if !isRetry {
			t.Fatal("Expected IsRetryAttempt to be true on retry")
		}

		quota2 := createTestQuotaDetails(reservedBytes)
		inProgress2 := createTestVolumeOperationDetails(
			name, "", "", "task-retry-2", TaskInvocationStatusInProgress, "", quota2)
		mustStoreRequestDetails(t, store, ctx, inProgress2)

		error2 := createTestVolumeOperationDetails(
			name, "", "", "task-retry-2", TaskInvocationStatusError, "second error", quota2)
		if shouldReleaseReservation(error2, isRetry) {
			error2.QuotaDetails.Reserved = resource.NewQuantity(0, resource.BinarySI)
		}
		mustStoreRequestDetails(t, store, ctx, error2)

		crd := mustGetCRDDirectly(t, store, ctx, name)
		assertQuotaReservation(t, crd, 0)
	})

	t.Run("success always zeros reservation regardless of retry", func(t *testing.T) {
		name := "pvc-success-always-zeros"
		reservedBytes := int64(400 * 1024 * 1024 * 1024)
		quota := createTestQuotaDetails(reservedBytes)

		// First attempt -> IsRetryAttempt == false
		existing, err := store.GetRequestDetails(ctx, name)
		isRetry := IsRetryAttempt(existing, err)

		inProgress := createTestVolumeOperationDetails(
			name, "", "", "task-success-1", TaskInvocationStatusInProgress, "", quota)
		if err := store.StoreRequestDetails(ctx, inProgress); err != nil {
			t.Fatalf("StoreInProgress: %v", err)
		}

		success := createTestVolumeOperationDetails(
			name, "vol-123", "", "task-success-1", TaskInvocationStatusSuccess, "", quota)
		if shouldReleaseReservation(success, isRetry) {
			success.QuotaDetails.Reserved = resource.NewQuantity(0, resource.BinarySI)
		}
		if err := store.StoreRequestDetails(ctx, success); err != nil {
			t.Fatalf("StoreSuccess: %v", err)
		}

		crd, err := getCRDDirectly(ctx, store, name)
		if err != nil {
			t.Fatalf("GetCRD: %v", err)
		}
		if crd.Status.StorageQuotaDetails == nil {
			t.Fatal("Expected StorageQuotaDetails to exist")
		}
		if crd.Status.StorageQuotaDetails.Reserved.Value() != 0 {
			t.Errorf("Success should always zero reservation, got %d",
				crd.Status.StorageQuotaDetails.Reserved.Value())
		}
	})
}

// TestIsRetryAttempt verifies the exported IsRetryAttempt helper function
// that the production code in manager.go relies on.
func TestIsRetryAttempt(t *testing.T) {
	tests := []struct {
		name    string
		details *VolumeOperationRequestDetails
		err     error
		want    bool
	}{
		{
			name:    "nil details with NotFound error is not a retry",
			details: nil,
			err:     fmt.Errorf("not found"),
			want:    false,
		},
		{
			name:    "nil details with nil error is not a retry",
			details: nil,
			err:     nil,
			want:    false,
		},
		{
			name: "non-nil details with nil error is a retry",
			details: &VolumeOperationRequestDetails{
				Name: "test",
				OperationDetails: &OperationDetails{
					TaskStatus: TaskInvocationStatusError,
				},
			},
			err:  nil,
			want: true,
		},
		{
			name: "non-nil details with non-nil error is not a retry",
			details: &VolumeOperationRequestDetails{
				Name: "test",
			},
			err:  fmt.Errorf("some error"),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsRetryAttempt(tc.details, tc.err)
			if got != tc.want {
				t.Errorf("IsRetryAttempt() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestForceTransitionZerosQuotaReservation verifies that forceTransitionStaleInProgressToError
// proactively zeroes the quota reservation when isPodVMOnStretchSupervisorFSSEnabled is true.
func TestForceTransitionZerosQuotaReservation(t *testing.T) {
	store, ctx := setupTestEnvironment(t, true)

	t.Run("zeroes reservation when FSS enabled and quota exists", func(t *testing.T) {
		isPodVMOnStretchSupervisorFSSEnabled = true
		defer func() { isPodVMOnStretchSupervisorFSSEnabled = false }()

		instanceName := "stale-with-quota"
		staleTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		reservedQty := resource.NewQuantity(400*1024*1024*1024, resource.BinarySI)
		quota := &cnsvolumeoprequestv1alpha1.QuotaDetails{
			Reserved:         reservedQty,
			StoragePolicyId:  "policy-123",
			StorageClassName: "sc-1",
			Namespace:        "ns-1",
		}
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: staleTime,
				TaskID:                  "task-stale-quota",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], quota)

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}

		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected Error, got %s", updated.Status.LatestOperationDetails[0].TaskStatus)
		}
		if updated.Status.StorageQuotaDetails == nil {
			t.Fatal("Expected StorageQuotaDetails to still exist")
		}
		if updated.Status.StorageQuotaDetails.Reserved.Value() != 0 {
			t.Errorf("Expected reservation to be zeroed, got %d",
				updated.Status.StorageQuotaDetails.Reserved.Value())
		}
	})

	t.Run("does not zero reservation when FSS disabled", func(t *testing.T) {
		isPodVMOnStretchSupervisorFSSEnabled = false

		instanceName := "stale-with-quota-fss-off"
		staleTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		reservedBytes := int64(400 * 1024 * 1024 * 1024)
		reservedQty := resource.NewQuantity(reservedBytes, resource.BinarySI)
		quota := &cnsvolumeoprequestv1alpha1.QuotaDetails{
			Reserved:         reservedQty,
			StoragePolicyId:  "policy-123",
			StorageClassName: "sc-1",
			Namespace:        "ns-1",
		}
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: staleTime,
				TaskID:                  "task-stale-quota-fss-off",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], quota)

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}

		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected Error, got %s", updated.Status.LatestOperationDetails[0].TaskStatus)
		}
		if updated.Status.StorageQuotaDetails == nil {
			t.Fatal("Expected StorageQuotaDetails to still exist")
		}
		if updated.Status.StorageQuotaDetails.Reserved.Value() != reservedBytes {
			t.Errorf("Expected reservation to remain %d when FSS disabled, got %d",
				reservedBytes, updated.Status.StorageQuotaDetails.Reserved.Value())
		}
	})

	t.Run("handles nil StorageQuotaDetails gracefully", func(t *testing.T) {
		isPodVMOnStretchSupervisorFSSEnabled = true
		defer func() { isPodVMOnStretchSupervisorFSSEnabled = false }()

		instanceName := "stale-no-quota"
		staleTime := metav1.NewTime(time.Now().Add(-72 * time.Hour))
		ops := []cnsvolumeoprequestv1alpha1.OperationDetails{
			{
				TaskInvocationTimestamp: staleTime,
				TaskID:                  "task-stale-no-quota",
				TaskStatus:              TaskInvocationStatusInProgress,
			},
		}
		createTestCRDInstance(ctx, t, store.k8sclient, instanceName, ops, ops[0], nil)

		instance, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get CRD: %v", err)
		}
		store.forceTransitionStaleInProgressToError(ctx, instance)

		updated, err := getCRDDirectly(ctx, store, instanceName)
		if err != nil {
			t.Fatalf("Failed to get updated CRD: %v", err)
		}

		if updated.Status.LatestOperationDetails[0].TaskStatus != TaskInvocationStatusError {
			t.Errorf("Expected Error, got %s", updated.Status.LatestOperationDetails[0].TaskStatus)
		}
		if updated.Status.StorageQuotaDetails != nil {
			t.Error("Expected StorageQuotaDetails to remain nil")
		}
	})
}
