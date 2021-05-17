/*
Copyright 2021 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvolumeoperationrequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
)

const (
	// CRDName represent the name of cnsvolumeoperationrequest CRD
	crdName = "cnsvolumeoperationrequests.cns.vmware.com"
	// CRDSingular represent the singular name of cnsvolumeoperationrequest CRD
	crdSingular = "cnsvolumeoperationrequest"
	// CRDPlural represent the plural name of cnsvolumeoperationrequest CRD
	crdPlural = "cnsvolumeoperationrequests"
	// maxEntriesInLatestOperationDetails specifies the maximum length of
	// the LatestOperationDetails allowed in a cnsvolumeoperationrequest instance
	maxEntriesInLatestOperationDetails = 10
	// TaskInvocationStatusInProgress represents a task thats status is InProgress.
	TaskInvocationStatusInProgress = "InProgress"
	// TaskInvocationStatusError represents a task thats status is Error.
	TaskInvocationStatusError = "Error"
	// TaskInvocationStatusSuccess represents a task thats status is Success.
	TaskInvocationStatusSuccess = "Success"
)

// VolumeOperationRequestDetails stores details about a single operation
// on the given volume. These details are persisted by
// VolumeOperationRequestInterface and the persisted details will be
// returned by the interface on request by the caller via this structure.
type VolumeOperationRequestDetails struct {
	Name             string
	VolumeID         string
	SnapshotID       string
	Capacity         int64
	OperationDetails *OperationDetails
}

// OperationDetails stores information about a particular operation.
type OperationDetails struct {
	TaskInvocationTimestamp metav1.Time
	TaskID                  string
	OpID                    string
	TaskStatus              string
	Error                   string
}

// CreateVolumeOperationRequestDetails returns an object of type VolumeOperationRequestDetails from the input parameters.
func CreateVolumeOperationRequestDetails(name, volumeID, snapshotID string, capacity int64, taskInvocationTimestamp metav1.Time, taskID, opID, taskStatus, error string) *VolumeOperationRequestDetails {
	return &VolumeOperationRequestDetails{
		Name:       name,
		VolumeID:   volumeID,
		SnapshotID: snapshotID,
		Capacity:   capacity,
		OperationDetails: &OperationDetails{
			TaskInvocationTimestamp: taskInvocationTimestamp,
			TaskID:                  taskID,
			OpID:                    opID,
			TaskStatus:              taskStatus,
			Error:                   error,
		},
	}
}

// convertToCnsVolumeOperationRequestDetails converts an object of type OperationDetails to the OperationDetails type
// defined by the CnsVolumeOperationRequest Custom Resource.
func convertToCnsVolumeOperationRequestDetails(details OperationDetails) *cnsvolumeoperationrequestv1alpha1.OperationDetails {
	return &cnsvolumeoperationrequestv1alpha1.OperationDetails{
		TaskInvocationTimestamp: details.TaskInvocationTimestamp,
		TaskID:                  details.TaskID,
		OpID:                    details.OpID,
		TaskStatus:              details.TaskStatus,
		Error:                   details.Error,
	}
}
