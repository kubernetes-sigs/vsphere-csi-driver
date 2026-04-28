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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	cnsvolumeoperationrequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

// TestHasPriorSuccessForVolume verifies the syncer-side idempotency guard that
// prevents StoragePolicyUsage.used from being incremented more than once when
// a single CnsVolumeOperationRequest CR accumulates multiple Success entries
// (e.g. via the CnsVolumeAlreadyExistsFault retry path that does
// CreateVolume -> AlreadyExists -> deleteVolume -> CreateVolume).
func TestHasPriorSuccessForVolume(t *testing.T) {
	mkOp := func(taskID, status string) cnsvolumeoperationrequestv1alpha1.OperationDetails {
		return cnsvolumeoperationrequestv1alpha1.OperationDetails{
			TaskInvocationTimestamp: metav1.Now(),
			TaskID:                  taskID,
			TaskStatus:              status,
		}
	}

	tests := []struct {
		name          string
		latestOps     []cnsvolumeoperationrequestv1alpha1.OperationDetails
		currentTaskID string
		want          bool
	}{
		{
			name:          "empty list returns false",
			latestOps:     nil,
			currentTaskID: "task-1",
			want:          false,
		},
		{
			name: "single Success entry which is the current task returns false",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
			},
			currentTaskID: "task-1",
			want:          false,
		},
		{
			name: "single Error entry returns false",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusError),
			},
			currentTaskID: "task-1",
			want:          false,
		},
		{
			// Models the bug scenario:
			//   task-842 Success (first create)
			//   task-847 Error   (CnsVolumeAlreadyExistsFault)
			//   task-854 Success (recreate after delete) <- current
			// The guard MUST fire so the syncer does not bump used a 2nd time.
			name: "prior Success exists with intermediate Error: returns true",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-842", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
				mkOp("task-847", cnsvolumeoperationrequest.TaskInvocationStatusError),
				mkOp("task-854", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
			},
			currentTaskID: "task-854",
			want:          true,
		},
		{
			name: "two Success entries back-to-back: returns true for the latest",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
				mkOp("task-2", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
			},
			currentTaskID: "task-2",
			want:          true,
		},
		{
			name: "only Errors before current Success returns false",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusError),
				mkOp("task-2", cnsvolumeoperationrequest.TaskInvocationStatusError),
				mkOp("task-3", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
			},
			currentTaskID: "task-3",
			want:          false,
		},
		{
			name: "in-progress entries are ignored",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusInProgress),
				mkOp("task-2", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
			},
			currentTaskID: "task-2",
			want:          false,
		},
		{
			name: "current task id matches multiple entries: only non-current Success counts",
			latestOps: []cnsvolumeoperationrequestv1alpha1.OperationDetails{
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
				mkOp("task-1", cnsvolumeoperationrequest.TaskInvocationStatusSuccess),
			},
			currentTaskID: "task-1",
			want:          false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hasPriorSuccessForVolume(tc.latestOps, tc.currentTaskID)
			if got != tc.want {
				t.Errorf("hasPriorSuccessForVolume() = %v, want %v", got, tc.want)
			}
		})
	}
}
