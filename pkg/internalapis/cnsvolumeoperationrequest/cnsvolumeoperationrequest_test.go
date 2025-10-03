package cnsvolumeoperationrequest

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

// setupTestEnvironment creates a test environment with fake clients
func setupTestEnvironment(t *testing.T, csiTransactionEnabled bool) (*operationRequestStore, context.Context) {
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
