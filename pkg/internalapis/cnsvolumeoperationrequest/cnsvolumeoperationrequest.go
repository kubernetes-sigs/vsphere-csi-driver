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
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	csiconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsvolumeoperationrequestconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/config"
	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// CRDSingular represents the singular name of cnsvolumeoperationrequest CRD.
	CRDSingular = "cnsvolumeoperationrequest"
	// CRDPlural represents the plural name of cnsvolumeoperationrequest CRD.
	CRDPlural = "cnsvolumeoperationrequests"
	// EnvCSINamespace represents the environment variable which
	// stores the namespace in which the CSI driver is running.
	EnvCSINamespace = "CSI_NAMESPACE"
)

// VolumeOperationRequest is an interface that supports handling idempotency
// in CSI volume manager. This interface persists operation details invoked
// on CNS and returns the persisted information to callers whenever it is requested.
type VolumeOperationRequest interface {
	// GetRequestDetails returns the details of the operation on the volume
	// that is persisted by the VolumeOperationRequest interface.
	// Returns an error if any error is encountered while attempting to
	// read the previously persisted information.
	GetRequestDetails(ctx context.Context, name string) (*VolumeOperationRequestDetails, error)
	// StoreRequestDetails persists the details of the operation taking
	// place on the volume.
	// Returns an error if any error is encountered. Clients must assume
	// that the attempt to persist the information failed if an error is returned.
	StoreRequestDetails(ctx context.Context, instance *VolumeOperationRequestDetails) error
	// DeleteRequestDetails deletes the details of the operation on the volume
	// that was persisted by the VolumeOperationRequest interface.
	DeleteRequestDetails(ctx context.Context, name string) error
}

// operationRequestStore implements the VolumeOperationsRequest interface.
// This implementation persists the operation information on etcd via a client
// to the API server. Reads are also done directly on etcd; there is no caching
// layer involved.
type operationRequestStore struct {
	k8sclient client.Client
}

var (
	csiNamespace                         string
	operationRequestStoreInstance        *operationRequestStore
	operationStoreInitLock               = &sync.Mutex{}
	isPodVMOnStretchSupervisorFSSEnabled bool
	isCSITransactionSupportEnabled       bool
)

// InitVolumeOperationRequestInterface creates the CnsVolumeOperationRequest
// definition on the API server and returns an implementation of
// VolumeOperationRequest interface. Clients are unaware of the implementation
// details to read and persist volume operation details.
func InitVolumeOperationRequestInterface(ctx context.Context, cleanupInterval int,
	isBlockVolumeSnapshotEnabled func() bool, isPodVMOnStretchSupervisorEnabled bool,
	csiTransactionSupportEnabled bool) (
	VolumeOperationRequest, error) {
	log := logger.GetLogger(ctx)
	csiNamespace = getCSINamespace()

	operationStoreInitLock.Lock()
	defer operationStoreInitLock.Unlock()
	if operationRequestStoreInstance == nil {
		// Create CnsVolumeOperationRequest definition on API server.
		log.Info(
			"Creating CnsVolumeOperationRequest definition on API server and initializing VolumeOperationRequest instance",
		)
		err := k8s.CreateCustomResourceDefinitionFromManifest(ctx,
			cnsvolumeoperationrequestconfig.EmbedCnsVolumeOperationRequestFile,
			cnsvolumeoperationrequestconfig.EmbedCnsVolumeOperationRequestFileName)
		if err != nil {
			log.Errorf("failed to create CnsVolumeOperationRequest CRD with error: %v", err)
			return nil, err
		}

		// Get in cluster config for client to API server.
		config, err := k8s.GetKubeConfig(ctx)
		if err != nil {
			log.Errorf("failed to get kubeconfig with error: %v", err)
			return nil, err
		}

		// Create client to API server.
		k8sclient, err := k8s.NewClientForGroup(ctx, config, cnsvolumeoprequestv1alpha1.SchemeGroupVersion.Group)
		if err != nil {
			log.Errorf("failed to create k8sClient with error: %v", err)
			return nil, err
		}

		// Initialize the operationRequestStoreOnETCD implementation of
		// VolumeOperationRequest interface.
		// NOTE: Currently there is only a single implementation of this
		// interface. Future implementations will need modify this step.
		operationRequestStoreInstance = &operationRequestStore{
			k8sclient: k8sclient,
		}
		go operationRequestStoreInstance.cleanupStaleInstances(cleanupInterval)
	}
	// Store PodVMOnStretchedSupervisor FSS value for later use.
	isPodVMOnStretchSupervisorFSSEnabled = isPodVMOnStretchSupervisorEnabled
	// Store CSI Transaction Support FSS value for later use.
	isCSITransactionSupportEnabled = csiTransactionSupportEnabled

	return operationRequestStoreInstance, nil
}

// GetRequestDetails returns the details of the operation on the volume
// that is persisted by the VolumeOperationRequest interface, by querying
// API server for a CnsVolumeOperationRequest instance with the given
// name.
// Returns an error if any error is encountered while attempting to
// read the previously persisted information from the API server.
// Callers need to differentiate NotFound errors if required.
func (or *operationRequestStore) GetRequestDetails(
	ctx context.Context,
	name string,
) (*VolumeOperationRequestDetails, error) {
	log := logger.GetLogger(ctx)
	instanceKey := client.ObjectKey{Name: name, Namespace: csiNamespace}
	log.Debugf("Getting CnsVolumeOperationRequest instance with name %s/%s", instanceKey.Namespace, instanceKey.Name)

	instance := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{}
	err := or.k8sclient.Get(ctx, instanceKey, instance)
	if err != nil {
		return nil, err
	}
	log.Debugf("Found CnsVolumeOperationRequest instance %v", spew.Sdump(instance))

	if len(instance.Status.LatestOperationDetails) == 0 {
		return nil, fmt.Errorf("length of LatestOperationDetails expected to be greater than 1 if the instance exists")
	}

	// Callers only need to know about the last operation that was invoked on a volume.
	operationDetailsToReturn := instance.Status.LatestOperationDetails[len(instance.Status.LatestOperationDetails)-1]

	var quotaDetails *QuotaDetails
	if isPodVMOnStretchSupervisorFSSEnabled && instance.Status.StorageQuotaDetails != nil {
		quotaDetails = &QuotaDetails{
			Reserved:         instance.Status.StorageQuotaDetails.Reserved,
			StorageClassName: instance.Status.StorageQuotaDetails.StorageClassName,
			StoragePolicyId:  instance.Status.StorageQuotaDetails.StoragePolicyId,
			Namespace:        instance.Status.StorageQuotaDetails.Namespace,
		}
	}

	return CreateVolumeOperationRequestDetails(instance.Spec.Name, instance.Status.VolumeID, instance.Status.SnapshotID,
			instance.Status.Capacity, quotaDetails, operationDetailsToReturn.TaskInvocationTimestamp,
			operationDetailsToReturn.TaskID, operationDetailsToReturn.VCenterServer, operationDetailsToReturn.OpID,
			operationDetailsToReturn.TaskStatus, operationDetailsToReturn.Error),
		nil
}

// StoreRequestDetails persists the details of the operation taking
// place on the volume by storing it on the API server.
// Returns an error if any error is encountered. Clients must assume
// that the attempt to persist the information failed if an error is returned.
func (or *operationRequestStore) StoreRequestDetails(
	ctx context.Context,
	operationToStore *VolumeOperationRequestDetails,
) error {
	log := logger.GetLogger(ctx)
	if operationToStore == nil {
		return logger.LogNewError(log, "cannot store empty operation")
	}
	log.Debugf("Storing CnsVolumeOperationRequest instance with spec %v", spew.Sdump(operationToStore))

	operationDetailsToStore := convertToCnsVolumeOperationRequestDetails(*operationToStore.OperationDetails)
	instance := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{}
	instanceKey := client.ObjectKey{Name: operationToStore.Name, Namespace: csiNamespace}

	if err := or.k8sclient.Get(ctx, instanceKey, instance); err != nil {
		if apierrors.IsNotFound(err) {
			// Create new instance on API server if it doesnt exist.
			// Implies that this is the first time this object is
			// being stored.
			newInstance := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instanceKey.Name,
					Namespace: instanceKey.Namespace,
				},
				Spec: cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequestSpec{
					Name: instanceKey.Name,
				},
				Status: cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequestStatus{
					VolumeID:              operationToStore.VolumeID,
					SnapshotID:            operationToStore.SnapshotID,
					Capacity:              operationToStore.Capacity,
					FirstOperationDetails: *operationDetailsToStore,
					LatestOperationDetails: []cnsvolumeoprequestv1alpha1.OperationDetails{
						*operationDetailsToStore,
					},
				},
			}
			if isPodVMOnStretchSupervisorFSSEnabled && operationToStore.QuotaDetails != nil {
				newInstance.Status.StorageQuotaDetails = &cnsvolumeoprequestv1alpha1.QuotaDetails{
					Reserved:                            operationToStore.QuotaDetails.Reserved,
					StoragePolicyId:                     operationToStore.QuotaDetails.StoragePolicyId,
					StorageClassName:                    operationToStore.QuotaDetails.StorageClassName,
					Namespace:                           operationToStore.QuotaDetails.Namespace,
					AggregatedSnapshotSize:              operationToStore.QuotaDetails.AggregatedSnapshotSize,
					SnapshotLatestOperationCompleteTime: operationToStore.QuotaDetails.SnapshotLatestOperationCompleteTime,
				}
			}
			err = or.k8sclient.Create(ctx, newInstance)
			if err != nil {
				log.Errorf(
					"failed to create CnsVolumeOperationRequest instance %s/%s with error: %v",
					instanceKey.Namespace,
					instanceKey.Name,
					err,
				)
				return err
			}
			log.Debugf(
				"Created CnsVolumeOperationRequest instance %s/%s with latest information for task with ID: %s",
				instanceKey.Namespace,
				instanceKey.Name,
				operationDetailsToStore.TaskID,
			)
			return nil
		}
		log.Errorf(
			"failed to get CnsVolumeOperationRequest instance %s/%s with error: %v",
			instanceKey.Namespace,
			instanceKey.Name,
			err,
		)
		return err
	}

	// Create a deep copy since we modify the object.
	updatedInstance := instance.DeepCopy()

	// If CSI Transaction Support is enabled and we're storing a new InProgress operation with empty TaskID,
	// mark any existing InProgress entries as TrackingAborted since this indicates a retry scenario.
	if isCSITransactionSupportEnabled && operationDetailsToStore.TaskStatus == TaskInvocationStatusInProgress &&
		operationDetailsToStore.TaskID == "" {
		// This is a new operation attempt (Phase 1: Intent Registration)
		// Mark any existing InProgress entries as TrackingAborted since this is clearly a retry
		for index := range updatedInstance.Status.LatestOperationDetails {
			existingOp := &updatedInstance.Status.LatestOperationDetails[index]
			if existingOp.TaskStatus == TaskInvocationStatusInProgress {
				// This is a retry - mark the previous attempt as aborted
				existingOp.TaskStatus = TaskInvocationStatusTrackingAborted
				existingOp.Error = "Operation tracking aborted due to retry attempt"
				log.Infof("Marked previous InProgress operation as TrackingAborted due to retry detection. Instance: %s",
					operationToStore.Name)
			}
		}

		// Also check FirstOperationDetails
		if updatedInstance.Status.FirstOperationDetails.TaskStatus == TaskInvocationStatusInProgress {
			updatedInstance.Status.FirstOperationDetails.TaskStatus = TaskInvocationStatusTrackingAborted
			updatedInstance.Status.FirstOperationDetails.Error = "Operation tracking aborted due to retry attempt"
			log.Infof("Marked FirstOperationDetails as TrackingAborted due to retry detection. Instance: %s",
				operationToStore.Name)
		}
	}

	// Modify VolumeID, SnapshotID and Capacity
	updatedInstance.Status.VolumeID = operationToStore.VolumeID
	updatedInstance.Status.SnapshotID = operationToStore.SnapshotID
	updatedInstance.Status.Capacity = operationToStore.Capacity
	if isPodVMOnStretchSupervisorFSSEnabled && operationToStore.QuotaDetails != nil {
		updatedInstance.Status.StorageQuotaDetails = &cnsvolumeoprequestv1alpha1.QuotaDetails{
			Reserved:                            operationToStore.QuotaDetails.Reserved,
			StoragePolicyId:                     operationToStore.QuotaDetails.StoragePolicyId,
			StorageClassName:                    operationToStore.QuotaDetails.StorageClassName,
			Namespace:                           operationToStore.QuotaDetails.Namespace,
			AggregatedSnapshotSize:              operationToStore.QuotaDetails.AggregatedSnapshotSize,
			SnapshotLatestOperationCompleteTime: operationToStore.QuotaDetails.SnapshotLatestOperationCompleteTime,
		}
	}

	// Modify FirstOperationDetails only if TaskID's match or the initial TaskID is empty.
	firstOp := instance.Status.FirstOperationDetails
	if firstOp.TaskStatus == TaskInvocationStatusInProgress &&
		(firstOp.TaskID == operationToStore.OperationDetails.TaskID || firstOp.TaskID == "") {
		updatedInstance.Status.FirstOperationDetails = *operationDetailsToStore
	}

	operationExistsInList := false
	// If the task details already exist in the status, update it with the
	// latest information.
	for index := len(instance.Status.LatestOperationDetails) - 1; index >= 0; index-- {
		operationDetail := instance.Status.LatestOperationDetails[index]
		if operationDetail.TaskStatus == TaskInvocationStatusInProgress &&
			(operationDetailsToStore.TaskID == operationDetail.TaskID || operationDetail.TaskID == "") {
			updatedInstance.Status.LatestOperationDetails[index] = *operationDetailsToStore
			operationExistsInList = true
			break
		}
	}

	if !operationExistsInList {
		// Append the latest task details to the local instance and
		// ensure length of LatestOperationDetails is not greater
		// than 10.
		updatedInstance.Status.LatestOperationDetails = append(
			updatedInstance.Status.LatestOperationDetails,
			*operationDetailsToStore,
		)
		if len(updatedInstance.Status.LatestOperationDetails) > maxEntriesInLatestOperationDetails {
			updatedInstance.Status.LatestOperationDetails = updatedInstance.Status.LatestOperationDetails[1:]
		}
	}

	// Store the local instance on the API server.
	err := or.k8sclient.Update(ctx, updatedInstance)
	if err != nil {
		log.Errorf(
			"failed to update CnsVolumeOperationRequest instance %s/%s with error: %v",
			instanceKey.Namespace,
			instanceKey.Name,
			err,
		)
		return err
	}
	log.Debugf(
		"Updated CnsVolumeOperationRequest instance %s/%s with latest information for task with ID: %s",
		instanceKey.Namespace,
		instanceKey.Name,
		operationDetailsToStore.TaskID,
	)
	return nil
}

// DeleteRequestDetails deletes the input CnsVolumeOperationRequest instance
// from the operationRequestStore.
func (or *operationRequestStore) DeleteRequestDetails(ctx context.Context, name string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("Deleting CnsVolumeOperationRequest instance with name %s/%s",
		csiNamespace, name)
	err := or.k8sclient.Delete(ctx, &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: csiNamespace,
		},
	})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Errorf("failed to delete CnsVolumeOperationRequest instance %s/%s with error: %v",
				csiNamespace, name, err)
			return err
		}
	}
	return nil
}

// cleanupStaleInstances cleans up CnsVolumeOperationRequest instances
// with latest TaskInvocationTimestamp older than 15 minutes
func (or *operationRequestStore) cleanupStaleInstances(cleanupInterval int) {
	ticker := time.NewTicker(time.Duration(cleanupInterval) * time.Minute)
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("CnsVolumeOperationRequest clean up interval is set to %d minutes", cleanupInterval)
	for ; true; <-ticker.C {
		cutoffTime := time.Now().Add(-15 * time.Minute)
		continueToken := ""
		log.Infof("Cleaning up stale CnsVolumeOperationRequest instances.")
		for {
			listOptions := &client.ListOptions{
				Limit:    5000,
				Continue: continueToken,
			}
			cnsVolumeOperationRequestList := &cnsvolumeoprequestv1alpha1.CnsVolumeOperationRequestList{}
			err := or.k8sclient.List(ctx, cnsVolumeOperationRequestList, listOptions)
			if err != nil {
				log.Errorf("failed to list CnsVolumeOperationRequests with error %v. Abandoning "+
					"CnsVolumeOperationRequests clean up ...", err)
				break
			}
			for _, instance := range cnsVolumeOperationRequestList.Items {
				latestOperationDetailsLength := len(instance.Status.LatestOperationDetails)
				// Skip if task is still in progress
				if latestOperationDetailsLength != 0 &&
					instance.Status.LatestOperationDetails[latestOperationDetailsLength-1].TaskStatus ==
						TaskInvocationStatusInProgress {
					continue
				}
				// Delete instance if TaskInvocationTimestamp is older than 15 minutes
				if latestOperationDetailsLength != 0 &&
					instance.Status.LatestOperationDetails[latestOperationDetailsLength-1].
						TaskInvocationTimestamp.Time.After(cutoffTime) {
					log.Debugf("CnsVolumeOperationRequest instance %q is skipped for deletion", instance.Name)
					continue
				}
				log.Debugf("Calling DeleteRequestDetails for %q", instance.Name)
				err = or.DeleteRequestDetails(ctx, instance.Name)
				if err != nil {
					log.Errorf("failed to delete CnsVolumeOperationRequest instance %s with error %v",
						instance.Name, err)
				}
				log.Debugf("CnsVolumeOperationRequest instance %q is deleted", instance.Name)
			}
			// Exit if there are no more pages
			continueToken = cnsVolumeOperationRequestList.GetContinue()
			log.Debugf("continueToken to process remaining CnsVolumeOperationRequest "+
				"insances is %q", continueToken)
			if continueToken == "" {
				log.Infof("all CnsVolumeOperationRequest instances are processed")
				break
			}
		}
		log.Infof("Clean up of stale CnsVolumeOperationRequest complete.")
	}
}

// SetCSITransactionSupport sets the CSI Transaction Support feature flag.
// This function allows runtime modification of the isCSITransactionSupportEnabled variable.
func SetCSITransactionSupport(enabled bool) {
	isCSITransactionSupportEnabled = enabled
}

func getCSINamespace() string {
	csiNamespace := os.Getenv(EnvCSINamespace)
	if strings.TrimSpace(csiNamespace) == "" {
		return csiconfig.DefaultCSINamespace
	}
	return csiNamespace
}
