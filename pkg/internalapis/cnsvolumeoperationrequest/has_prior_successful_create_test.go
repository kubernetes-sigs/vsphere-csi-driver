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

package cnsvolumeoperationrequest

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

// seedCR creates a CnsVolumeOperationRequest CR directly via the fake k8s
// client so we can exercise HasPriorSuccessfulCreate against a known
// LatestOperationDetails history.
func seedCR(t *testing.T, store *operationRequestStore, name string,
	ops []cnsvolumeoprequestv1alpha1.OperationDetails) {
	t.Helper()
	cr := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: csiNamespace,
		},
		Status: cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequestStatus{
			LatestOperationDetails: ops,
		},
	}
	if err := store.k8sclient.Create(t.Context(), cr); err != nil {
		t.Fatalf("seedCR: failed to create CR %s: %v", name, err)
	}
}

func mkOp(taskID, status string) cnsvolumeoprequestv1alpha1.OperationDetails {
	return cnsvolumeoprequestv1alpha1.OperationDetails{
		TaskInvocationTimestamp: metav1.Now(),
		TaskID:                  taskID,
		TaskStatus:              status,
	}
}

// TestHasPriorSuccessfulCreate covers the controller-side idempotency guard
// used in createVolumeWithTransaction to suppress QuotaDetails.Reserved
// repopulation on retry paths (e.g. after CnsVolumeAlreadyExistsFault).
func TestHasPriorSuccessfulCreate(t *testing.T) {
	t.Run("CR not found returns false", func(t *testing.T) {
		store, ctx := setupTestEnvironment(t, true)
		if store.HasPriorSuccessfulCreate(ctx, "missing") {
			t.Errorf("expected false when CR does not exist")
		}
	})

	t.Run("empty LatestOperationDetails returns false", func(t *testing.T) {
		store, ctx := setupTestEnvironment(t, true)
		seedCR(t, store, "empty-ops", nil)
		if store.HasPriorSuccessfulCreate(ctx, "empty-ops") {
			t.Errorf("expected false when LatestOperationDetails is empty")
		}
	})

	t.Run("only InProgress / Error returns false", func(t *testing.T) {
		store, ctx := setupTestEnvironment(t, true)
		seedCR(t, store, "no-success", []cnsvolumeoprequestv1alpha1.OperationDetails{
			mkOp("task-1", TaskInvocationStatusInProgress),
			mkOp("task-2", TaskInvocationStatusError),
		})
		if store.HasPriorSuccessfulCreate(ctx, "no-success") {
			t.Errorf("expected false when no Success entries are present")
		}
	})

	t.Run("single Success returns true", func(t *testing.T) {
		store, ctx := setupTestEnvironment(t, true)
		seedCR(t, store, "one-success", []cnsvolumeoprequestv1alpha1.OperationDetails{
			mkOp("task-1", TaskInvocationStatusSuccess),
		})
		if !store.HasPriorSuccessfulCreate(ctx, "one-success") {
			t.Errorf("expected true when a Success entry is present")
		}
	})

	// Models the production CR for the bug:
	//   task-842 Success
	//   task-847 Error  (CnsVolumeAlreadyExistsFault)
	//   task-854 Success
	// The guard MUST detect the prior Success even when the most recent
	// entry on the path is an Error or another Success.
	t.Run("Success then Error: returns true", func(t *testing.T) {
		store, ctx := setupTestEnvironment(t, true)
		seedCR(t, store, "succ-then-err", []cnsvolumeoprequestv1alpha1.OperationDetails{
			mkOp("task-842", TaskInvocationStatusSuccess),
			mkOp("task-847", TaskInvocationStatusError),
		})
		if !store.HasPriorSuccessfulCreate(ctx, "succ-then-err") {
			t.Errorf("expected true: prior Success should be detected even if latest entry is Error")
		}
	})

	t.Run("Success, Error, Success: returns true", func(t *testing.T) {
		store, ctx := setupTestEnvironment(t, true)
		seedCR(t, store, "bug-shape", []cnsvolumeoprequestv1alpha1.OperationDetails{
			mkOp("task-842", TaskInvocationStatusSuccess),
			mkOp("task-847", TaskInvocationStatusError),
			mkOp("task-854", TaskInvocationStatusSuccess),
		})
		if !store.HasPriorSuccessfulCreate(ctx, "bug-shape") {
			t.Errorf("expected true on bug-shape CR with two Success entries and one intermediate Error")
		}
	})
}
