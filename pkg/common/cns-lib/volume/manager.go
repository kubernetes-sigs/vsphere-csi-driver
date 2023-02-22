/*
Copyright 2019 The Kubernetes Authors.

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

package volume

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vslm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

const (
	// defaultTaskCleanupIntervalInMinutes is default interval for cleaning up
	// expired create volume tasks.
	// TODO: This timeout will be configurable in future releases.
	defaultTaskCleanupIntervalInMinutes = 1
	// defaultListViewCleanupInvalidTasksInMinutes is the interval for removing tasks from the listview
	// and internal map that couldn't be removed due to any vc issues
	defaultListViewCleanupInvalidTasksInMinutes = 15

	// timeout duration for a http request
	// used only for listView
	noTimeout = 0 * time.Minute

	// defaultOpsExpirationTimeInHours is expiration time for create volume operations.
	// TODO: This timeout will be configurable in future releases
	defaultOpsExpirationTimeInHours = 1

	// maxLengthOfVolumeNameInCNS is the maximum length of CNS volume name.
	maxLengthOfVolumeNameInCNS = 80
	// Alias for TaskInvocationStatus constants.
	taskInvocationStatusInProgress = cnsvolumeoperationrequest.TaskInvocationStatusInProgress
	taskInvocationStatusSuccess    = cnsvolumeoperationrequest.TaskInvocationStatusSuccess
	taskInvocationStatusError      = cnsvolumeoperationrequest.TaskInvocationStatusError
)

// Manager provides functionality to manage volumes.
type Manager interface {
	// CreateVolume creates a new volume given its spec.
	// When CreateVolume failed, the second return value (faultType) and third return value(error) need to be set, and
	// should not be nil.
	CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec) (*CnsVolumeInfo, string, error)
	// AttachVolume attaches a volume to a virtual machine given the spec.
	// When AttachVolume failed, the second return value (faultType) and third return value(error) need to be set, and
	// should not be nil.
	AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine,
		volumeID string, checkNVMeController bool) (string, string, error)
	// DetachVolume detaches a volume from the virtual machine given the spec.
	// When DetachVolume failed, the first return value (faultType) and second return value(error) need to be set, and
	// should not be nil.
	DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) (string, error)
	// DeleteVolume deletes a volume given its spec.
	// When DeleteVolume failed, the first return value (faultType) and second return value(error) need to be set, and
	// should not be nil.
	DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error)
	// UpdateVolumeMetadata updates a volume metadata given its spec.
	UpdateVolumeMetadata(ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec) error
	// QueryVolumeInfo calls the CNS QueryVolumeInfo API and return a task, from
	// which CnsQueryVolumeInfoResult is extracted.
	QueryVolumeInfo(ctx context.Context, volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error)
	// QueryAllVolume returns all volumes matching the given filter and selection.
	QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
		querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error)
	// QueryVolumeAsync returns CnsQueryResult matching the given filter by using
	// CnsQueryAsync API. QueryVolumeAsync takes querySelection spec, which helps
	// to specify which fields have to be returned for the query entities. All
	// volume fields would be returned as part of the CnsQueryResult, if the
	// querySelection parameters are not specified.
	QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
		querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error)
	// QueryVolume returns volumes matching the given filter.
	QueryVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error)
	// RelocateVolume migrates volumes to their target datastore as specified in relocateSpecList.
	RelocateVolume(ctx context.Context, relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error)
	// ExpandVolume expands a volume to a new size.
	// When ExpandVolume failed, the first return value (faultType) and second return value(error) need to be set, and
	// should not be nil.
	ExpandVolume(ctx context.Context, volumeID string, size int64) (string, error)
	// ResetManager helps set new manager instance and VC configuration.
	ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error
	// ConfigureVolumeACLs configures net permissions for a given CnsVolumeACLConfigureSpec.
	ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error
	// RegisterDisk registers virtual disks as FCDs using Vslm endpoint.
	RegisterDisk(ctx context.Context, path string, name string) (string, error)
	// RetrieveVStorageObject helps in retreiving virtual disk information for a given volume id.
	RetrieveVStorageObject(ctx context.Context, volumeID string) (*vim25types.VStorageObject, error)
	// ProtectVolumeFromVMDeletion sets keepAfterDeleteVm control flag on migrated volume
	ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error
	// CreateSnapshot helps create a snapshot for a block volume
	CreateSnapshot(ctx context.Context, volumeID string, desc string) (*CnsSnapshotInfo, error)
	// DeleteSnapshot helps delete a snapshot for a block volume
	DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string) error
	// QuerySnapshots retrieves the list of snapshots based on the query filter.
	QuerySnapshots(ctx context.Context, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (
		*cnstypes.CnsSnapshotQueryResult, error)
	// MonitorCreateVolumeTask monitors the CNS task which is created for volume creation
	// as part of volume idempotency feature
	MonitorCreateVolumeTask(ctx context.Context,
		volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails,
		task *object.Task, volNameFromInputSpec string, clusterID string) (*CnsVolumeInfo, string, error)
	// GetOperationStore returns the VolumeOperationRequest interface
	GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest
}

// CnsVolumeInfo hold information related to volume created by CNS.
type CnsVolumeInfo struct {
	DatastoreURL string
	VolumeID     cnstypes.CnsVolumeId
}

type CnsSnapshotInfo struct {
	SnapshotID                string
	SourceVolumeID            string
	SnapshotDescription       string
	SnapshotCreationTimestamp time.Time
}

var (
	// managerInstance is a Manager singleton.
	managerInstance *defaultManager
	// managerInstanceMap hold volume manager for vCenter servers
	managerInstanceMap = make(map[string]*defaultManager)
	// managerInstanceLock is used for mitigating race condition during
	// read/write on manager instance.
	managerInstanceLock sync.Mutex
	volumeTaskMap       = make(map[string]*createVolumeTaskDetails)
	// volumeTaskMapLock is used to serialize writes to volumeTaskMap.
	volumeTaskMapLock sync.Mutex
	snapshotTaskMap   = make(map[string]*createSnapshotTaskDetails)
	// snapshotTaskMapLock is used to serialize writes to snapshotTaskMap.
	// Alternatively, we may use Sync.Map for snapshotTaskMap without using a lock explicitly
	snapshotTaskMapLock sync.Mutex
	// Alias for CreateVolumeOperationRequestDetails function declaration.
	createRequestDetails = cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails
)

// createSnapshotTaskDetails has the same structure as createVolumeTaskDetails
type createSnapshotTaskDetails struct {
	createVolumeTaskDetails
}

// createVolumeTaskDetails contains taskInfo object and expiration time.
type createVolumeTaskDetails struct {
	task           *object.Task
	expirationTime time.Time
}

// GetManager returns the Manager instance.
func GetManager(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	operationStore cnsvolumeoperationrequest.VolumeOperationRequest,
	idempotencyHandlingEnabled bool, multivCenterEnabled bool,
	multivCenterTopologyDeployment bool, tasksListViewEnabled bool) (Manager, error) {
	log := logger.GetLogger(ctx)
	managerInstanceLock.Lock()
	defer managerInstanceLock.Unlock()
	if !multivCenterEnabled {
		if managerInstance != nil {
			log.Infof("Retrieving existing defaultManager...")
			return managerInstance, nil
		}
		log.Infof("Initializing new defaultManager...")
		managerInstance = &defaultManager{
			virtualCenter:              vc,
			operationStore:             operationStore,
			idempotencyHandlingEnabled: idempotencyHandlingEnabled,
			tasksListViewEnabled:       tasksListViewEnabled,
		}
	} else {
		managerInstance = managerInstanceMap[vc.Config.Host]
		if managerInstance != nil {
			log.Infof("Retrieving existing defaultManager for vCenter: %q", vc.Config.Host)
			return managerInstance, nil
		}
		log.Infof("Initializing new defaultManager for vCenter: %q", vc.Config.Host)
		managerInstance = &defaultManager{
			virtualCenter:                  vc,
			operationStore:                 operationStore,
			idempotencyHandlingEnabled:     idempotencyHandlingEnabled,
			multivCenterTopologyDeployment: multivCenterTopologyDeployment,
			tasksListViewEnabled:           tasksListViewEnabled,
		}
		managerInstanceMap[vc.Config.Host] = managerInstance
	}
	if tasksListViewEnabled {
		err := managerInstance.initListView()
		if err != nil {
			return nil, err
		}
	}
	return managerInstance, nil
}

// DefaultManager provides functionality to manage volumes.
type defaultManager struct {
	virtualCenter                  *cnsvsphere.VirtualCenter
	operationStore                 cnsvolumeoperationrequest.VolumeOperationRequest
	idempotencyHandlingEnabled     bool
	multivCenterTopologyDeployment bool
	tasksListViewEnabled           bool
	listViewIf                     ListViewIf
}

// ClearTaskInfoObjects is a go routine which runs in the background to clean
// up expired taskInfo objects from volumeTaskMap.
func ClearTaskInfoObjects() {
	log := logger.GetLoggerWithNoContext()
	// At a frequency of every 1 minute, check if there are expired taskInfo
	// objects and delete them from the volumeTaskMap.
	ticker := time.NewTicker(time.Duration(defaultTaskCleanupIntervalInMinutes) * time.Minute)
	for range ticker.C {
		for pvc, taskDetails := range volumeTaskMap {
			// Get the time difference between current time and the expiration
			// time from the volumeTaskMap.
			diff := time.Until(taskDetails.expirationTime)
			// Checking if the expiration time has elapsed.
			if int(diff.Hours()) < 0 || int(diff.Minutes()) < 0 || int(diff.Seconds()) < 0 {
				// If one of the parameters in the time object is negative, it means
				// the entry has to be deleted.
				log.Debugf("Found an expired taskInfo: %+v for volume %q. Deleting it from task map",
					volumeTaskMap[pvc].task, pvc)
				func() {
					volumeTaskMapLock.Lock()
					defer volumeTaskMapLock.Unlock()
					delete(volumeTaskMap, pvc)
				}()
			}
		}
		// clear task info objects for CreateSnapshot task
		for snapshotTaskKey, taskDetails := range snapshotTaskMap {
			// Get the time difference between current time and the expiration
			// time from the snapshotTaskMap.
			diff := time.Until(taskDetails.expirationTime)
			// Checking if the expiration time has elapsed.
			if int(diff.Hours()) < 0 || int(diff.Minutes()) < 0 || int(diff.Seconds()) < 0 {
				// If one of the parameters in the time object is negative, it means
				// the entry has to be deleted.
				log.Debugf("Found an expired taskInfo: %+v for snapshot %q. Deleting it from task map",
					snapshotTaskMap[snapshotTaskKey].task, snapshotTaskKey)
				func() {
					// put the snapshotTaskMap update operation with locking into an anonymous func
					// to ensure the deferred unlock is called right after the update.
					snapshotTaskMapLock.Lock()
					defer snapshotTaskMapLock.Unlock()
					delete(snapshotTaskMap, snapshotTaskKey)
				}()
			}
		}
	}
}

func ClearInvalidTasksFromListView(multivCenterCSITopologyEnabled bool) {
	ticker := time.NewTicker(time.Duration(defaultListViewCleanupInvalidTasksInMinutes) * time.Minute)
	for range ticker.C {
		if multivCenterCSITopologyEnabled {
			for _, mgr := range managerInstanceMap {
				if mgr.listViewIf != nil {
					RemoveTasksMarkedForDeletion(mgr.listViewIf.(*ListViewImpl))
				}
			}
		} else {
			if managerInstance.listViewIf != nil {
				RemoveTasksMarkedForDeletion(managerInstance.listViewIf.(*ListViewImpl))
			}
		}
	}
}

// ResetManager helps set manager instance with new VC configuration.
func (m *defaultManager) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) error {
	log := logger.GetLogger(ctx)
	managerInstanceLock.Lock()
	defer managerInstanceLock.Unlock()
	log.Infof("Re-initializing defaultManager.virtualCenter")
	managerInstance.virtualCenter = vcenter
	if m.tasksListViewEnabled {
		err := m.listViewIf.SetVirtualCenter(ctx, managerInstance.virtualCenter)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to set virtual center to listView instance. err: %v", err)
		}
	}
	if m.virtualCenter.Client != nil {
		m.virtualCenter.Client.Timeout = time.Duration(vcenter.Config.VCClientTimeout) * time.Minute
		log.Infof("VC client timeout is set to %v", m.virtualCenter.Client.Timeout)
	}
	log.Infof("Done resetting volume.defaultManager")
	return nil
}

// GetOperationStore returns the VolumeOperationRequest interface
func (m *defaultManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	return m.operationStore
}

// MonitorCreateVolumeTask monitors the CNS task which is created for volume creation
// as part of volume idempotency feature
func (m *defaultManager) MonitorCreateVolumeTask(ctx context.Context,
	volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails, task *object.Task,
	volNameFromInputSpec string, clusterID string) (*CnsVolumeInfo, string, error) {
	var faultType string
	log := logger.GetLogger(ctx)
	var vCenterServerForVolumeOperationCR string
	if m.multivCenterTopologyDeployment {
		vCenterServerForVolumeOperationCR = m.virtualCenter.Config.Host
	}

	var taskInfo *vim25types.TaskInfo
	var err error
	if m.tasksListViewEnabled {
		taskInfo, err = m.waitOnTask(ctx, task.Reference())
	} else {
		taskInfo, err = task.WaitForResult(ctx, nil)
	}
	if err != nil {
		if cnsvsphere.IsManagedObjectNotFound(err, task.Reference()) {
			log.Debugf("CreateVolume task %s not found in vCenter %s. Querying CNS "+
				"to determine if the volume %s was successfully created.",
				m.virtualCenter.Config.Host, task.Reference().Value, volNameFromInputSpec)
			queryFilter := cnstypes.CnsQueryFilter{
				Names:               []string{volNameFromInputSpec},
				ContainerClusterIds: []string{clusterID},
			}
			queryResult, queryAllVolumeErr := m.QueryAllVolume(ctx, queryFilter, cnstypes.CnsQuerySelection{})
			if queryAllVolumeErr != nil {
				log.Errorf("failed to query CNS for volume %s with error: %v. Cannot "+
					"determine if CreateVolume task %s was successful.", volNameFromInputSpec,
					queryAllVolumeErr, task.Reference().Value)
				return nil, ExtractFaultTypeFromErr(ctx, err), err
			}
			if len(queryResult.Volumes) > 0 {
				if len(queryResult.Volumes) != 1 {
					log.Infof("CNS Query returned multiple entries for volume %s: %v. Returning volume ID "+
						"of first entry: %s", volNameFromInputSpec, spew.Sdump(queryResult.Volumes),
						queryResult.Volumes[0].VolumeId.Id)
				}
				volumeID := queryResult.Volumes[0].VolumeId.Id
				*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, volumeID, "", 0,
					(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
					vCenterServerForVolumeOperationCR,
					(*volumeOperationDetails).OperationDetails.TaskID, taskInvocationStatusSuccess, "")
				log.Debugf("Found volume %s with id %s", volNameFromInputSpec, volumeID)
				return &CnsVolumeInfo{
					DatastoreURL: "",
					VolumeID: cnstypes.CnsVolumeId{
						Id: volumeID,
					},
				}, "", nil
			}
			log.Errorf("volume with name %s not present in CNS. Marking task %s as failed.",
				volNameFromInputSpec, task.Reference().Value)
			*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
				vCenterServerForVolumeOperationCR,
				(*volumeOperationDetails).OperationDetails.TaskID, taskInvocationStatusError, err.Error())

			return nil, ExtractFaultTypeFromErr(ctx, err), err
		}
		// WaitForResult can fail for many reasons, including:
		// - CNS restarted and marked "InProgress" tasks as "Failed".
		// - Any failures from CNS.
		// - Any failures at lower layers like FCD and SPS.
		// In all cases, mark task as failed and retry.
		log.Errorf("failed to get CreateVolume taskInfo from CNS with error: %v", err)
		*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
			vCenterServerForVolumeOperationCR,
			(*volumeOperationDetails).OperationDetails.TaskID, taskInvocationStatusError, err.Error())

		return nil, ExtractFaultTypeFromErr(ctx, err), err
	}

	log.Infof("CreateVolume: VolumeName: %q, opId: %q", volNameFromInputSpec, taskInfo.ActivationId)
	taskResult, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return nil, csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for CreateVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if err != nil {
		log.Errorf("failed to get task result for task %s and volume name %s with error: %v",
			task.Reference().Value, volNameFromInputSpec, err)
		faultType = ExtractFaultTypeFromErr(ctx, err)
		return nil, faultType, err
	}
	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		// Validate if the volume is already registered.
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		resp, err := validateCreateVolumeResponseFault(ctx, volNameFromInputSpec, volumeOperationRes)
		if err != nil {
			*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
				vCenterServerForVolumeOperationCR,
				taskInfo.ActivationId, taskInvocationStatusError, err.Error())
		} else {
			*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, resp.VolumeID.Id, "", 0,
				(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
				vCenterServerForVolumeOperationCR,
				taskInfo.ActivationId, taskInvocationStatusSuccess, "")
		}
		return resp, faultType, err
	}
	// Extract the CnsVolumeInfo from the taskResult.
	resp, faultType, err := getCnsVolumeInfoFromTaskResult(ctx, m.virtualCenter, volNameFromInputSpec,
		volumeOperationRes.VolumeId, taskResult)
	if err != nil {
		*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
			vCenterServerForVolumeOperationCR,
			taskInfo.ActivationId, taskInvocationStatusError, err.Error())
	} else {
		*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, resp.VolumeID.Id, "", 0,
			(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
			vCenterServerForVolumeOperationCR,
			taskInfo.ActivationId, taskInvocationStatusSuccess, "")
	}
	return resp, faultType, err
}

// createVolumeWithImprovedIdempotency leverages the VolumeOperationRequest
// interface to persist CNS task information. It uses this persisted information
// to handle idempotency of CreateVolume callbacks to CNS for the same volume.
func (m *defaultManager) createVolumeWithImprovedIdempotency(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec) (
	*CnsVolumeInfo, string, error) {
	log := logger.GetLogger(ctx)
	var (
		// Store the volume name passed in by input spec, this
		// name may exceed 80 characters.
		volNameFromInputSpec = spec.Name
		// Reference to the CreateVolume task on CNS.
		task *object.Task
		// Local instance of CreateVolume details that needs to
		// be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// Information related to volume created by CNS
		resp *CnsVolumeInfo
	)

	if m.operationStore == nil {
		return nil, csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}
	var vCenterServerForVolumeOperationCR string
	if m.multivCenterTopologyDeployment {
		vCenterServerForVolumeOperationCR = m.virtualCenter.Config.Host
	}

	var faultType string
	// Determine if CNS CreateVolume needs to be invoked.
	volumeOperationDetails, err := m.operationStore.GetRequestDetails(ctx, volNameFromInputSpec)
	switch {
	case err == nil:
		if volumeOperationDetails.OperationDetails != nil {
			// Validate if previous attempt was successful.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess &&
				volumeOperationDetails.VolumeID != "" {
				log.Infof("Volume with name %q and id %q is already created on CNS with opId: %q.",
					volNameFromInputSpec, volumeOperationDetails.VolumeID,
					volumeOperationDetails.OperationDetails.OpID)
				return &CnsVolumeInfo{
					DatastoreURL: "",
					VolumeID: cnstypes.CnsVolumeId{
						Id: volumeOperationDetails.VolumeID,
					},
				}, "", nil
			}
			// Validate if previous operation is pending.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusInProgress &&
				volumeOperationDetails.OperationDetails.TaskID != "" {
				log.Infof("Volume with name %s has CreateVolume task %s pending on CNS.",
					volNameFromInputSpec,
					volumeOperationDetails.OperationDetails.TaskID)
				taskMoRef := vim25types.ManagedObjectReference{
					Type:  "Task",
					Value: volumeOperationDetails.OperationDetails.TaskID,
				}
				task = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
			}
		}
	case apierrors.IsNotFound(err):
		// Instance doesn't exist. This is likely the
		// first attempt to create the volume.
		volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			metav1.Now(), "", vCenterServerForVolumeOperationCR, "",
			taskInvocationStatusInProgress, "")
	default:
		return nil, csifault.CSIInternalFault, err
	}
	defer func() {
		// Persist the operation details before returning. Only success or error
		// needs to be stored as InProgress details are stored when the task is
		// created on CNS.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {
			err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
			if err != nil {
				log.Warnf("failed to store CreateVolume details with error: %v", err)
			}
		}
	}()
	if task == nil {
		// The task object is nil in two cases:
		// - No previous CreateVolume task for this volume was retrieved from the
		//   persistent store.
		// - The previous CreateVolume task failed.
		// In both cases, invoke CNS CreateVolume again.
		task, err = invokeCNSCreateVolume(ctx, m.virtualCenter, spec)
		if err != nil {
			log.Errorf("failed to create volume with error: %v", err)
			volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "",
				vCenterServerForVolumeOperationCR, "",
				taskInvocationStatusError, err.Error())
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return nil, faultType, err
		}
		if !isStaticallyProvisioned(spec) {
			// Persist task details only for dynamically provisioned volumes.
			volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				metav1.Now(), task.Reference().Value, vCenterServerForVolumeOperationCR,
				"", taskInvocationStatusInProgress,
				"")
			err = m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
			if err != nil {
				// Don't return if CreateVolume details can't be stored.
				log.Warnf("failed to store CreateVolume details with error: %v", err)
			}
		}
	}

	resp, faultType, err = m.MonitorCreateVolumeTask(ctx, &volumeOperationDetails, task,
		volNameFromInputSpec, spec.Metadata.ContainerClusterArray[0].ClusterId)
	return resp, faultType, err
}

func (m *defaultManager) waitOnTask(csiOpContext context.Context,
	taskMoRef vim25types.ManagedObjectReference) (*vim25types.TaskInfo, error) {
	log := logger.GetLogger(csiOpContext)
	if m.listViewIf == nil {
		err := m.initListView()
		if err != nil {
			return nil, err
		}
	}
	ch := make(chan TaskResult)
	err := m.listViewIf.AddTask(csiOpContext, taskMoRef, ch)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to add task to list view. err: %v", err)
	}

	// deferring removal of task after response from CNS
	// called only after successful addition to listView, so we don't need any check for the removal
	defer func() {
		err := m.listViewIf.RemoveTask(csiOpContext, taskMoRef)
		if err != nil {
			log.Errorf("failed to remove task from list view. err: %v", err)
			err = m.listViewIf.MarkTaskForDeletion(csiOpContext, taskMoRef)
			if err != nil {
				log.Errorf("failed to mark task for deletion. err: %v", err)
			}
		}
	}()

	return waitForResultOrTimeout(csiOpContext, taskMoRef, ch)
}

// waitForResultOrTimeout uses the context provided by the sidecars when CSI driver operations are called.
// This context has a timeout associated with it (see manifests for more details).
// Once this caller timeout is over, we want to return an error back to the caller
func waitForResultOrTimeout(csiOpContext context.Context, taskMoRef vim25types.ManagedObjectReference,
	ch chan TaskResult) (*vim25types.TaskInfo, error) {
	var taskInfo *vim25types.TaskInfo
	var err error
	select {
	case <-csiOpContext.Done():
		err = fmt.Errorf("time out for task %v before response from CNS", taskMoRef)
		taskInfo = nil
	case result := <-ch:
		err = result.Err
		taskInfo = result.TaskInfo
	}
	return taskInfo, err
}

func (m *defaultManager) initListView() error {
	ctx := logger.NewContextWithLogger(context.Background())
	log := logger.GetLogger(ctx)
	log.Debugf("Initializing new listView object for vc: %+v", m.virtualCenter)
	if m.virtualCenter.Client == nil {
		log.Debugf("vimClient is nil. calling vc.connect()")
		err := m.virtualCenter.Connect(ctx)
		if err != nil {
			return logger.LogNewErrorf(log, "failed to connect to vCenter. err: %v", err)
		}
	}

	govmomiClient, err := m.virtualCenter.NewClient(ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create a separate govmomi client for listView. error: %+v", err)
	}
	govmomiClient.Timeout = noTimeout
	log.Infof("created new govmomi client for listView")
	m.listViewIf, err = NewListViewImpl(ctx, m.virtualCenter, govmomiClient)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to initialize listView object. err: %v", err)
	}
	return nil
}

// createVolume invokes CNS CreateVolume. It stores task information in an
// in-memory map to handle idempotency of CreateVolume calls for the same
// volume.
func (m *defaultManager) createVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec) (*CnsVolumeInfo,
	string, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the CreateVolume task on CNS.
		task *object.Task
		err  error
		// Store the volume name passed in by input spec, this
		// name may exceed 80 characters.
		volNameFromInputSpec = spec.Name
	)
	var faultType string
	task = getPendingCreateVolumeTaskFromMap(ctx, volNameFromInputSpec)
	if task == nil {
		task, err = invokeCNSCreateVolume(ctx, m.virtualCenter, spec)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return nil, faultType, err
		}
		if !isStaticallyProvisioned(spec) {
			var taskDetails createVolumeTaskDetails
			// Store task details and expiration time in volumeTaskMap.
			taskDetails.task = task
			taskDetails.expirationTime = time.Now().Add(time.Hour * time.Duration(
				defaultOpsExpirationTimeInHours))
			func() {
				volumeTaskMapLock.Lock()
				defer volumeTaskMapLock.Unlock()
				volumeTaskMap[volNameFromInputSpec] = &taskDetails
			}()
		}
	} else {
		// Create new task object with latest vCenter Client to avoid
		// NotAuthenticated fault for cached tasks objects.
		task = object.NewTask(m.virtualCenter.Client.Client, task.Reference())
	}

	var taskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		taskInfo, err = m.waitOnTask(ctx, task.Reference())
	} else {
		taskInfo, err = cns.GetTaskInfo(ctx, task)
	}

	if err != nil || taskInfo == nil {
		log.Errorf("failed to get taskInfo for CreateVolume task with err: %v", err)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
		} else {
			faultType = csifault.CSITaskInfoEmptyFault
		}
		return nil, faultType, err
	}

	log.Infof("CreateVolume: VolumeName: %q, opId: %q", volNameFromInputSpec, taskInfo.ActivationId)
	taskResult, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return nil, csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for CreateVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if err != nil {
		log.Errorf("failed to get task result for task %s and volume name %s with error: %v",
			task.Reference().Value, volNameFromInputSpec, err)
		faultType = ExtractFaultTypeFromErr(ctx, err)
		return nil, faultType, err
	}
	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		// Validate if the volume is already registered.
		resp, err := validateCreateVolumeResponseFault(ctx, volNameFromInputSpec, volumeOperationRes)
		if err != nil {
			// Remove the taskInfo object associated with the volume name when the
			// current task fails. This is needed to ensure the sub-sequent create
			// volume call from the external provisioner invokes Create Volume.
			_, ok := volumeTaskMap[volNameFromInputSpec]
			if ok {
				func() {
					volumeTaskMapLock.Lock()
					defer volumeTaskMapLock.Unlock()
					log.Debugf("Deleted task for %s from volumeTaskMap because the task has failed",
						volNameFromInputSpec)
					delete(volumeTaskMap, volNameFromInputSpec)
				}()
			}
			log.Errorf("failed to create cns volume %s. createSpec: %q, fault: %q",
				volNameFromInputSpec, spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault))
		}
		return resp, faultType, err
	}

	// Return CnsVolumeInfo from the taskResult.
	return getCnsVolumeInfoFromTaskResult(ctx, m.virtualCenter, volNameFromInputSpec, volumeOperationRes.VolumeId,
		taskResult)
}

// CreateVolume creates a new volume given its spec.
func (m *defaultManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec) (*CnsVolumeInfo,
	string, error) {
	internalCreateVolume := func() (*CnsVolumeInfo, string, error) {
		log := logger.GetLogger(ctx)
		var faultType string
		err := validateManager(ctx, m)
		if err != nil {
			log.Errorf("failed to validate manager with error: %v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return nil, faultType, err
		}
		err = setupConnection(ctx, m.virtualCenter, spec)
		if err != nil {
			log.Errorf("failed to setup connection to CNS with error: %v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return nil, faultType, err
		}
		// Call CreateVolume implementation based on FSS value.
		if m.idempotencyHandlingEnabled {
			return m.createVolumeWithImprovedIdempotency(ctx, spec)
		}
		return m.createVolume(ctx, spec)
	}
	start := time.Now()
	resp, faultType, err := internalCreateVolume()
	log := logger.GetLogger(ctx)
	log.Debugf("internalCreateVolume: returns fault %q", faultType)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsCreateVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsCreateVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}

	return resp, faultType, err
}

// AttachVolume attaches a volume to a virtual machine given the spec.
func (m *defaultManager) AttachVolume(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, volumeID string, checkNVMeController bool) (string, string, error) {
	internalAttachVolume := func() (string, string, error) {
		log := logger.GetLogger(ctx)
		var faultType string
		err := validateManager(ctx, m)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return "", faultType, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return "", faultType, err
		}
		// Construct the CNS AttachSpec list.
		var cnsAttachSpecList []cnstypes.CnsVolumeAttachDetachSpec
		cnsAttachSpec := cnstypes.CnsVolumeAttachDetachSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeID,
			},
			Vm: vm.Reference(),
		}
		cnsAttachSpecList = append(cnsAttachSpecList, cnsAttachSpec)
		// Call the CNS AttachVolume.
		task, err := m.virtualCenter.CnsClient.AttachVolume(ctx, cnsAttachSpecList)
		if err != nil {
			log.Errorf("CNS AttachVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return "", faultType, err
		}
		// Get the taskInfo.

		var taskInfo *vim25types.TaskInfo
		if m.tasksListViewEnabled {
			taskInfo, err = m.waitOnTask(ctx, task.Reference())
		} else {
			taskInfo, err = cns.GetTaskInfo(ctx, task)
		}

		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for AttachVolume task from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			if err != nil {
				faultType = ExtractFaultTypeFromErr(ctx, err)
			} else {
				faultType = csifault.CSITaskInfoEmptyFault
			}
			return "", faultType, err
		}
		log.Infof("AttachVolume: volumeID: %q, vm: %q, opId: %q", volumeID, vm.String(), taskInfo.ActivationId)
		// Get the taskResult
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			log.Errorf("unable to find AttachVolume result from vCenter %q with taskID %s and attachResults %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return "", faultType, err
		}

		if taskResult == nil {
			return "", csifault.CSITaskResultEmptyFault,
				logger.LogNewErrorf(log, "taskResult is empty for AttachVolume task: %q, opId: %q",
					taskInfo.Task.Value, taskInfo.ActivationId)
		}

		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
			_, isResourceInUseFault := volumeOperationRes.Fault.Fault.(*vim25types.ResourceInUse)
			if isResourceInUseFault {
				log.Infof("observed ResourceInUse fault while attaching volume: %q with vm: %q", volumeID, vm.String())
				// Check if volume is already attached to the requested node.
				diskUUID, err := IsDiskAttached(ctx, vm, volumeID, checkNVMeController)
				if err != nil {
					return "", faultType, err
				}
				if diskUUID != "" {
					return diskUUID, "", nil
				}
			}
			return "", faultType, logger.LogNewErrorf(log, "failed to attach cns volume: %q to node vm: %q. fault: %q. opId: %q",
				volumeID, vm.String(), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		}
		diskUUID := interface{}(taskResult).(*cnstypes.CnsVolumeAttachResult).DiskUUID
		log.Infof("AttachVolume: Volume attached successfully. volumeID: %q, opId: %q, vm: %q, diskUUID: %q",
			volumeID, taskInfo.ActivationId, vm.String(), diskUUID)
		return diskUUID, "", nil
	}
	start := time.Now()
	resp, faultType, err := internalAttachVolume()
	log := logger.GetLogger(ctx)
	log.Debugf("internalAttachVolume: returns fault %q for volume %q", faultType, volumeID)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsAttachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsAttachVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, faultType, err
}

// DetachVolume detaches a volume from the virtual machine given the spec.
func (m *defaultManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) (string,
	error) {
	internalDetachVolume := func() (string, error) {
		log := logger.GetLogger(ctx)
		var faultType string
		err := validateManager(ctx, m)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return faultType, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return faultType, err
		}
		// Construct the CNS DetachSpec list.
		var cnsDetachSpecList []cnstypes.CnsVolumeAttachDetachSpec
		cnsDetachSpec := cnstypes.CnsVolumeAttachDetachSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeID,
			},
			Vm: vm.Reference(),
		}
		cnsDetachSpecList = append(cnsDetachSpecList, cnsDetachSpec)
		// Call the CNS DetachVolume.
		task, err := m.virtualCenter.CnsClient.DetachVolume(ctx, cnsDetachSpecList)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			if cnsvsphere.IsManagedObjectNotFound(err, cnsDetachSpec.Vm) {
				// Detach failed with managed object not found, marking detach as
				// successful, as Node VM is deleted and not present in the vCenter
				// inventory.
				log.Infof("Node VM: %v not found on vCenter. Marking Detach for volume:%q successful. err: %v",
					vm, volumeID, err)
				return "", nil
			}
			if cnsvsphere.IsNotFoundError(err) {
				// Detach failed with NotFound error, check if the volume is already detached.
				log.Infof("VolumeID: %q, not found. Checking whether the volume is already detached", volumeID)
				diskUUID, err := IsDiskAttached(ctx, vm, volumeID, false)
				if err != nil {
					log.Errorf("DetachVolume err: %+v. Unable to check if volume: %q is already detached from vm: %+v",
						err, volumeID, vm)
					return faultType, err
				}
				if diskUUID == "" {
					log.Infof("volumeID: %q not found on vm: %+v. Assuming it is already detached", volumeID, vm)
					return "", nil
				}
			}
			return faultType, logger.LogNewErrorf(log, "failed to detach cns volume:%q from node vm: %+v. err: %v",
				volumeID, vm, err)
		}
		// Get the taskInfo.
		var taskInfo *vim25types.TaskInfo
		if m.tasksListViewEnabled {
			taskInfo, err = m.waitOnTask(ctx, task.Reference())
		} else {
			taskInfo, err = cns.GetTaskInfo(ctx, task)
		}
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for DetachVolume task from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			if err != nil {
				faultType = ExtractFaultTypeFromErr(ctx, err)
			} else {
				faultType = csifault.CSITaskInfoEmptyFault
			}
			return faultType, err
		}
		log.Infof("DetachVolume: volumeID: %q, vm: %q, opId: %q", volumeID, vm.String(), taskInfo.ActivationId)
		// Get the task results for the given task.
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find DetachVolume task result from vCenter %q with taskID %s and detachResults %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return faultType, err
		}
		if taskResult == nil {
			return csifault.CSITaskResultEmptyFault,
				logger.LogNewErrorf(log, "taskResult is empty for DetachVolume task: %q, opId: %q",
					taskInfo.Task.Value, taskInfo.ActivationId)
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)

			if volumeOperationRes.Fault.Fault != nil {
				fault, isManagedObjectNotFoundFault := volumeOperationRes.Fault.Fault.(*vim25types.ManagedObjectNotFound)
				if isManagedObjectNotFoundFault && fault.Obj.Type == cnsDetachSpec.Vm.Type &&
					fault.Obj.Value == cnsDetachSpec.Vm.Value {
					// Detach failed with managed object not found, marking detach as
					// successful, as Node VM is deleted and not present in the vCenter
					// inventory.
					log.Infof("DetachVolume: Node VM: %v not found on vCenter. Marking Detach for volume:%q successful.",
						vm, volumeID)
					return "", nil
				}
				_, isNotFoundFault := volumeOperationRes.Fault.Fault.(*vim25types.NotFound)
				if isNotFoundFault {
					// Check if volume is already detached from the VM
					log.Infof("DetachVolume: VolumeID: %q not found. Checking whether the volume is already detached",
						volumeID)
					diskUUID, err := IsDiskAttached(ctx, vm, volumeID, false)
					if err != nil {
						log.Errorf("DetachVolume fault: %+v. Unable to check if volume: %q is already detached from vm: %+v",
							spew.Sdump(volumeOperationRes.Fault), volumeID, vm)
						return faultType, err
					}
					if diskUUID == "" {
						log.Infof("DetachVolume: volumeID: %q not found on vm: %+v. Assuming it is already detached",
							volumeID, vm)
						return "", nil
					}
				}
			}
			return faultType, logger.LogNewErrorf(log, "failed to detach cns volume: %q from node vm: %+v. fault: %+v, opId: %q",
				volumeID, vm, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		}
		log.Infof("DetachVolume: Volume detached successfully. volumeID: %q, vm: %q, opId: %q",
			volumeID, vm.String(), taskInfo.ActivationId)
		return "", nil
	}
	start := time.Now()
	faultType, err := internalDetachVolume()
	log := logger.GetLogger(ctx)
	log.Debugf("internalDetachVolume: returns fault %q for volume %q", faultType, volumeID)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDetachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDetachVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return faultType, err
}

// DeleteVolume deletes a volume given its spec.
func (m *defaultManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	internalDeleteVolume := func() (string, error) {
		log := logger.GetLogger(ctx)
		var faultType string
		err := validateManager(ctx, m)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			log.Errorf("failed to validate manager with error: %v", err)
			return faultType, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			log.Errorf("ConnectCns failed with err: %+v", err)
			return faultType, err
		}
		if m.idempotencyHandlingEnabled {
			return m.deleteVolumeWithImprovedIdempotency(ctx, volumeID, deleteDisk)
		}
		return m.deleteVolume(ctx, volumeID, deleteDisk)
	}
	start := time.Now()
	faultType, err := internalDeleteVolume()
	log := logger.GetLogger(ctx)
	log.Debugf("internalDeleteVolume: returns fault %q for volume %q", faultType, volumeID)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return faultType, err
}

// deleteVolume attempts to delete the volume on CNS. CNS task information
// is not persisted.
func (m *defaultManager) deleteVolume(ctx context.Context, volumeID string, deleteDisk bool) (string, error) {
	log := logger.GetLogger(ctx)
	// Construct the CNS VolumeId list.
	var cnsVolumeIDList []cnstypes.CnsVolumeId
	cnsVolumeID := cnstypes.CnsVolumeId{
		Id: volumeID,
	}
	// Call the CNS DeleteVolume.
	cnsVolumeIDList = append(cnsVolumeIDList, cnsVolumeID)
	task, err := m.virtualCenter.CnsClient.DeleteVolume(ctx, cnsVolumeIDList, deleteDisk)
	var faultType string
	if err != nil {
		faultType = ExtractFaultTypeFromErr(ctx, err)
		if cnsvsphere.IsNotFoundError(err) {
			log.Infof("VolumeID: %q, not found, thus returning success", volumeID)
			return "", nil
		}
		log.Errorf("CNS DeleteVolume failed from the  vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return faultType, err
	}
	// Get the taskInfo.
	var taskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		taskInfo, err = m.waitOnTask(ctx, task.Reference())
	} else {
		taskInfo, err = cns.GetTaskInfo(ctx, task)
	}
	if err != nil || taskInfo == nil {
		log.Errorf("failed to get DeleteVolume taskInfo from vCenter %q with err: %v",
			m.virtualCenter.Config.Host, err)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
		} else {
			faultType = csifault.CSITaskInfoEmptyFault
		}
		return faultType, err
	}
	log.Infof("DeleteVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task.
	taskResult, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for DeleteVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if err != nil {
		log.Errorf("failed to get DeleteVolume task result with error: %v", err)
		faultType = ExtractFaultTypeFromErr(ctx, err)
		return faultType, err
	}
	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		return faultType, logger.LogNewErrorf(log, "failed to delete volume: %q, fault: %q, opID: %q",
			volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
	}
	log.Infof("DeleteVolume: Volume deleted successfully. volumeID: %q, opId: %q",
		volumeID, taskInfo.ActivationId)
	return "", nil
}

// deleteVolumeWithImprovedIdempotency attempts to delete the volume on CNS.
// CNS task information is persisted by leveraging the VolumeOperationRequest
// interface.
func (m *defaultManager) deleteVolumeWithImprovedIdempotency(ctx context.Context,
	volumeID string, deleteDisk bool) (string, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the DeleteVolume task on CNS.
		task *object.Task
		// Name of the CnsVolumeOperationRequest instance.
		instanceName = "delete-" + volumeID
		// Local instance of DeleteVolume details that needs to
		// be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
	)
	var faultType string
	if m.operationStore == nil {
		return csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}

	if strings.Contains(instanceName, "file") {
		instanceName = strings.ReplaceAll(instanceName, ":", "-")
	}

	// Determine if CNS needs to be invoked.
	volumeOperationDetails, err := m.operationStore.GetRequestDetails(ctx, instanceName)
	switch {
	case err == nil:
		if volumeOperationDetails.OperationDetails != nil {
			// Validate if previous attempt was successful.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess {
				return "", nil
			}
			// Validate if previous operation is pending.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusInProgress &&
				volumeOperationDetails.OperationDetails.TaskID != "" {
				taskMoRef := vim25types.ManagedObjectReference{
					Type:  "Task",
					Value: volumeOperationDetails.OperationDetails.TaskID,
				}
				task = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
			}
		}
	case apierrors.IsNotFound(err):
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, metav1.Now(),
			"", "", "", taskInvocationStatusInProgress, "")
	default:
		return csifault.CSIInternalFault, err
	}

	defer func() {
		// Persist the operation details before returning. Only success or error
		// needs to be stored as InProgress details are stored when the task is
		// created on CNS.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {
			err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
			if err != nil {
				log.Warnf("failed to store DeleteVolume operation details with error: %v", err)
			}
		}
	}()

	if task == nil {
		// The task object is nil in two cases:
		// - No previous DeleteVolume task for this volume was retrieved from the
		//   persistent store.
		// - The previous DeleteVolume task failed.
		// In both cases, invoke CNS DeleteVolume again.
		var cnsVolumeIDList []cnstypes.CnsVolumeId
		cnsVolumeID := cnstypes.CnsVolumeId{
			Id: volumeID,
		}
		// Call the CNS DeleteVolume.
		cnsVolumeIDList = append(cnsVolumeIDList, cnsVolumeID)
		task, err = m.virtualCenter.CnsClient.DeleteVolume(ctx, cnsVolumeIDList, deleteDisk)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			if cnsvsphere.IsNotFoundError(err) {
				log.Infof("VolumeID: %q, not found, thus returning success", volumeID)
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
					metav1.Now(), "", "",
					"", taskInvocationStatusSuccess, "")
				return "", nil
			}
			log.Errorf("CNS DeleteVolume failed from the  vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
				metav1.Now(), "", "",
				"", taskInvocationStatusError, err.Error())
			return faultType, err
		}
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, metav1.Now(),
			task.Reference().Value, "",
			"", taskInvocationStatusInProgress, "")
		err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			log.Warnf("failed to store DeleteVolume details with error: %v", err)
		}
	}

	// Get the taskInfo.
	var taskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		taskInfo, err = m.waitOnTask(ctx, task.Reference())
	} else {
		taskInfo, err = cns.GetTaskInfo(ctx, task)
	}
	if err != nil || taskInfo == nil {
		log.Errorf("failed to get taskInfo for DeleteVolume task from vCenter %q with err: %v",
			m.virtualCenter.Config.Host, err)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			if cnsvsphere.IsManagedObjectNotFound(err, task.Reference()) {
				// Mark the volumeOperationRequest as an error if the corresponding VC task is missing/NotFound.
				msg := fmt.Sprintf("DeleteVolume task %s not found in vCenter.", task.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
					metav1.Now(), task.Reference().Value, "",
					"", taskInvocationStatusError, msg)
				return faultType, logger.LogNewError(log, msg)
			}
		} else {
			faultType = csifault.CSITaskInfoEmptyFault
		}
		return faultType, err
	}

	log.Infof("DeleteVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task.
	taskResult, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for DeleteVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if err != nil {
		log.Errorf("unable to find DeleteVolume task result from vCenter %q with taskID %s and deleteResults %v",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
		faultType = ExtractFaultTypeFromErr(ctx, err)
		return faultType, err
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		msg := fmt.Sprintf("failed to delete volume: %q, fault: %q, opID: %q",
			volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, "",
			taskInfo.ActivationId, taskInvocationStatusError, msg)
		return faultType, logger.LogNewError(log, msg)
	}
	log.Infof("DeleteVolume: Volume deleted successfully. volumeID: %q, opId: %q",
		volumeID, taskInfo.ActivationId)
	volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
		volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
		"", taskInfo.ActivationId, taskInvocationStatusSuccess, "")
	return "", nil
}

// UpdateVolumeMetadata updates a volume given its spec.
func (m *defaultManager) UpdateVolumeMetadata(ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	internalUpdateVolumeMetadata := func() error {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return err
		}
		// If the VSphereUser in the VolumeMetadataUpdateSpec is different from
		// session user, update the VolumeMetadataUpdateSpec.
		s, err := m.virtualCenter.Client.SessionManager.UserSession(ctx)
		if err != nil {
			log.Errorf("failed to get usersession with err: %v", err)
			return err
		}
		// Refer to this issue - https://github.com/vmware/govmomi/issues/2922
		// Session Manager -> UserSession can return nil user session with nil error
		// so handling the case for nil session.
		if s == nil {
			return errors.New("nil session obtained from session manager")
		}
		if s.UserName != spec.Metadata.ContainerCluster.VSphereUser {
			log.Debugf("Update VSphereUser from %s to %s", spec.Metadata.ContainerCluster.VSphereUser, s.UserName)
			spec.Metadata.ContainerCluster.VSphereUser = s.UserName
		}
		var cnsUpdateSpecList []cnstypes.CnsVolumeMetadataUpdateSpec
		cnsUpdateSpec := cnstypes.CnsVolumeMetadataUpdateSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: spec.VolumeId.Id,
			},
			Metadata: spec.Metadata,
		}
		cnsUpdateSpecList = append(cnsUpdateSpecList, cnsUpdateSpec)
		task, err := m.virtualCenter.CnsClient.UpdateVolumeMetadata(ctx, cnsUpdateSpecList)
		if err != nil {
			log.Errorf("CNS UpdateVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		// Get the taskInfo.
		var taskInfo *vim25types.TaskInfo
		if m.tasksListViewEnabled {
			taskInfo, err = m.waitOnTask(ctx, task.Reference())
		} else {
			taskInfo, err = cns.GetTaskInfo(ctx, task)
		}
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get UpdateVolume taskInfo from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			return err
		}
		log.Infof("UpdateVolumeMetadata: volumeID: %q, opId: %q", spec.VolumeId.Id, taskInfo.ActivationId)
		// Get the task results for the given task.
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find UpdateVolume result from vCenter %q: taskID %q, opId %q and updateResults %+v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult)
			return err
		}
		if taskResult == nil {
			return logger.LogNewErrorf(log, "taskResult is empty for UpdateVolume task: %q, opId: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			return logger.LogNewErrorf(log, "failed to update volume. updateSpec: %q, fault: %q, opID: %q",
				spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		}
		log.Infof("UpdateVolumeMetadata: Volume metadata updated successfully. volumeID: %q, opId: %q",
			spec.VolumeId.Id, taskInfo.ActivationId)
		return nil
	}
	start := time.Now()
	err := internalUpdateVolumeMetadata()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsUpdateVolumeMetadataOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsUpdateVolumeMetadataOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// ExpandVolume expands a volume given its spec.
func (m *defaultManager) ExpandVolume(ctx context.Context, volumeID string, size int64) (string, error) {
	internalExpandVolume := func() (string, error) {
		log := logger.GetLogger(ctx)
		var faultType string
		err := validateManager(ctx, m)
		if err != nil {
			log.Errorf("validateManager failed with err: %+v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return faultType, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return faultType, err
		}
		if m.idempotencyHandlingEnabled {
			return m.expandVolumeWithImprovedIdempotency(ctx, volumeID, size)
		}
		return m.expandVolume(ctx, volumeID, size)

	}
	start := time.Now()
	faultType, err := internalExpandVolume()
	log := logger.GetLogger(ctx)
	log.Debugf("internalExpandVolume: returns fault %q for volume %q", faultType, volumeID)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsExpandVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsExpandVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return faultType, err
}

// expandVolume invokes CNS ExpandVolume.
func (m *defaultManager) expandVolume(ctx context.Context, volumeID string, size int64) (string, error) {
	log := logger.GetLogger(ctx)
	// Construct the CNS ExtendSpec list.
	var cnsExtendSpecList []cnstypes.CnsVolumeExtendSpec
	cnsExtendSpec := cnstypes.CnsVolumeExtendSpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
		CapacityInMb: size,
	}
	cnsExtendSpecList = append(cnsExtendSpecList, cnsExtendSpec)
	// Call the CNS ExtendVolume.
	log.Infof("Calling CnsClient.ExtendVolume: VolumeID [%q] Size [%d] cnsExtendSpecList [%#v]",
		volumeID, size, cnsExtendSpecList)
	var faultType string
	task, err := m.virtualCenter.CnsClient.ExtendVolume(ctx, cnsExtendSpecList)
	if err != nil {
		faultType = ExtractFaultTypeFromErr(ctx, err)
		if cnsvsphere.IsNotFoundError(err) {
			return faultType, logger.LogNewErrorf(log, "volume %q not found. Cannot expand volume.", volumeID)
		}
		log.Errorf("CNS ExtendVolume failed from the vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return faultType, err
	}
	// Get the taskInfo.
	var taskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		taskInfo, err = m.waitOnTask(ctx, task.Reference())
	} else {
		taskInfo, err = cns.GetTaskInfo(ctx, task)
	}
	if err != nil || taskInfo == nil {
		log.Errorf("failed to get taskInfo for ExtendVolume task from vCenter %q with err: %v",
			m.virtualCenter.Config.Host, err)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
		} else {
			faultType = csifault.CSITaskInfoEmptyFault
		}
		return faultType, err
	}
	log.Infof("ExpandVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task.
	taskResult, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for ExpandVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if err != nil {
		log.Errorf("failed to get task result for ExtendVolume task %s with error: %v",
			task.Reference().Value, err)
		faultType = ExtractFaultTypeFromErr(ctx, err)
		return faultType, err
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		return faultType, logger.LogNewErrorf(log, "failed to extend volume: %q, fault: %q, opID: %q",
			volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
	}
	log.Infof("ExpandVolume: Volume expanded successfully. volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	return "", nil
}

// expandVolumeWithImprovedIdempotency leverages the VolumeOperationRequest
// interface to persist CNS task information. It uses this persisted information
// to handle idempotency of ExpandVolume callbacks to CNS for the same volume.
func (m *defaultManager) expandVolumeWithImprovedIdempotency(ctx context.Context, volumeID string,
	size int64) (string, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the ExtendVolume task.
		task *object.Task
		// Details to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// CnsVolumeOperationRequest instance name.
		instanceName = "expand-" + volumeID
		err          error
	)
	var faultType string
	if m.operationStore == nil {
		return csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}
	volumeOperationDetails, err = m.operationStore.GetRequestDetails(ctx, instanceName)
	switch {
	case err == nil:
		if volumeOperationDetails.OperationDetails != nil {
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess &&
				volumeOperationDetails.Capacity >= size {
				log.Infof("Volume with ID %s already expanded to size %v", volumeID, size)
				return "", nil
			}
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusInProgress &&
				volumeOperationDetails.OperationDetails.TaskID != "" {
				log.Infof("Volume with ID %s has ExtendVolume task %s pending on CNS.",
					volumeID,
					volumeOperationDetails.OperationDetails.TaskID)
				taskMoRef := vim25types.ManagedObjectReference{
					Type:  "Task",
					Value: volumeOperationDetails.OperationDetails.TaskID,
				}
				task = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
			}
		}
	case apierrors.IsNotFound(err):
		volumeOperationDetails = createRequestDetails(instanceName, "", "", size, metav1.Now(),
			"", "", "", "", "")
	default:
		return csifault.CSIInternalFault, err
	}
	defer func() {
		// Persist the operation details before returning.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {
			err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
			if err != nil {
				log.Warnf("failed to store ExpandVolume details with error: %v", err)
			}
		}
	}()

	if task == nil {
		// The task object is nil in two cases:
		// - No previous ExtendVolume task for this volume was retrieved from the
		//   persistent store.
		// - The previous ExtendVolume task failed.
		// In both cases, invoke CNS ExtendVolume.
		var cnsExtendSpecList []cnstypes.CnsVolumeExtendSpec
		cnsExtendSpec := cnstypes.CnsVolumeExtendSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeID,
			},
			CapacityInMb: size,
		}
		cnsExtendSpecList = append(cnsExtendSpecList, cnsExtendSpec)
		// Call the CNS ExtendVolume.
		log.Infof("Calling CnsClient.ExtendVolume: VolumeID [%q] Size [%d] cnsExtendSpecList [%#v]",
			volumeID, size, cnsExtendSpecList)
		task, err = m.virtualCenter.CnsClient.ExtendVolume(ctx, cnsExtendSpecList)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			if cnsvsphere.IsNotFoundError(err) {
				return faultType, logger.LogNewErrorf(log, "volume %q not found. Cannot expand volume.", volumeID)
			}
			log.Errorf("CNS ExtendVolume failed from the vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			volumeOperationDetails = createRequestDetails(instanceName, "", "", size, metav1.Now(),
				"", "", "", taskInvocationStatusError, err.Error())
			return faultType, err
		}
		volumeOperationDetails = createRequestDetails(instanceName, "", "", size, metav1.Now(),
			task.Reference().Value, "", "", taskInvocationStatusInProgress, "")
		err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			log.Warnf("failed to store ExpandVolume details with error: %v", err)
		}
	}

	var taskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		taskInfo, err = m.waitOnTask(ctx, task.Reference())
	} else {
		taskInfo, err = cns.GetTaskInfo(ctx, task)
	}
	if err != nil {
		if cnsvsphere.IsManagedObjectNotFound(err, task.Reference()) {
			log.Debugf("ExtendVolume task %s not found in vCenter. Querying CNS "+
				"to determine if volume with ID %s was successfully expanded.",
				task.Reference().Value, volumeID)
			if validateVolumeCapacity(ctx, m, volumeID, size) {
				log.Infof("ExpandVolume: Volume expanded successfully to size %d. volumeID: %q, task: %q",
					volumeOperationDetails.Capacity, volumeID, task.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, "", "", volumeOperationDetails.Capacity,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
					"",
					volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusSuccess, "")
				return "", nil
			}
		}
		// WaitForResult can fail for many reasons, including:
		// - CNS restarted and marked "InProgress" tasks as "Failed".
		// - Any other CNS failures.
		// - Any other failures at lower layers like FCD and SPS.
		// In all cases, mark task as failed and retry.
		log.Errorf("failed to expand volume with ID %s with error %+v", volumeID, err)
		volumeOperationDetails = createRequestDetails(instanceName, "", "", volumeOperationDetails.Capacity,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
			"",
			volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusError, err.Error())
		return ExtractFaultTypeFromErr(ctx, err), err
	}

	log.Infof("ExpandVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task.
	taskResult, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for ExpandVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if err != nil {
		log.Errorf("failed to get task result for task %s and volume ID %s with error: %v",
			task.Reference().Value, volumeID, err)
		faultType = ExtractFaultTypeFromErr(ctx, err)
		return faultType, err
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		if _, ok := volumeOperationRes.Fault.Fault.(cnstypes.CnsFault); ok {
			log.Debugf("ExtendVolume task %s returned with CnsFault. Querying CNS to "+
				"determine if volume with ID %s was successfully expanded.",
				task.Reference().Value, volumeID)
			if validateVolumeCapacity(ctx, m, volumeID, size) {
				log.Infof("ExpandVolume: Volume expanded successfully to size %d. volumeID: %q, task: %q",
					volumeOperationDetails.Capacity, volumeID, task.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, "", "", volumeOperationDetails.Capacity,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
					"",
					volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusSuccess, "")
				return "", nil
			}
		}
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		volumeOperationDetails = createRequestDetails(instanceName, "", "", volumeOperationDetails.Capacity,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
			"",
			taskInfo.ActivationId, taskInvocationStatusError, volumeOperationRes.Fault.LocalizedMessage)
		return faultType, logger.LogNewErrorf(log, "failed to extend volume: %q, fault: %q, opID: %q",
			volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
	}

	log.Infof("ExpandVolume: Volume expanded successfully to size %d. volumeID: %q, opId: %q",
		volumeOperationDetails.Capacity, volumeID, taskInfo.ActivationId)
	volumeOperationDetails = createRequestDetails(instanceName, "", "", volumeOperationDetails.Capacity,
		volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
		"",
		taskInfo.ActivationId, taskInvocationStatusSuccess, "")
	if volumeOperationDetails.Capacity >= size {
		return "", nil
	}
	// Return an error if the new size is less than the requested size.
	// The volume will be expanded again on a retry.
	return csifault.CSIInternalFault, logger.LogNewErrorf(log, "volume expanded to size %d but requested size is %d",
		volumeOperationDetails.Capacity, size)
}

// QueryVolume returns volumes matching the given filter.
func (m *defaultManager) QueryVolume(ctx context.Context,
	queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	internalQueryVolume := func() (*cnstypes.CnsQueryResult, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}
		// Call the CNS QueryVolume.
		res, err := m.virtualCenter.CnsClient.QueryVolume(ctx, queryFilter)
		if err != nil {
			log.Errorf("CNS QueryVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}
		res = updateQueryResult(ctx, m, res)
		return res, err
	}
	start := time.Now()
	resp, err := internalQueryVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// QueryAllVolume returns all volumes matching the given filter and selection.
func (m *defaultManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	internalQueryAllVolume := func() (*cnstypes.CnsQueryResult, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}
		// Call the CNS QueryAllVolume.
		res, err := m.virtualCenter.CnsClient.QueryAllVolume(ctx, queryFilter, querySelection)
		if err != nil {
			log.Errorf("CNS QueryAllVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}
		res = updateQueryResult(ctx, m, res)
		return res, err
	}
	start := time.Now()
	resp, err := internalQueryAllVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryAllVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryAllVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// QueryVolumeInfo calls the CNS QueryVolumeInfo API and return a task, from
// which CnsQueryVolumeInfoResult is extracted.
func (m *defaultManager) QueryVolumeInfo(ctx context.Context,
	volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	internalQueryVolumeInfo := func() (*cnstypes.CnsQueryVolumeInfoResult, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}
		// Call the CNS QueryVolumeInfo.
		queryVolumeInfoTask, err := m.virtualCenter.CnsClient.QueryVolumeInfo(ctx, volumeIDList)
		if err != nil {
			log.Errorf("CNS QueryAllVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}

		// Get the taskInfo.
		var taskInfo *vim25types.TaskInfo
		if m.tasksListViewEnabled {
			taskInfo, err = m.waitOnTask(ctx, queryVolumeInfoTask.Reference())
		} else {
			taskInfo, err = cns.GetTaskInfo(ctx, queryVolumeInfoTask)
		}
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get QueryVolumeInfo taskInfo from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			return nil, err
		}
		log.Infof("QueryVolumeInfo: volumeIDList: %v, opId: %q", volumeIDList, taskInfo.ActivationId)
		// Get the task results for the given task.
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find QueryVolumeInfo task result from vCenter %q: taskID %s and result %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return nil, err
		}
		if taskResult == nil {
			return nil, logger.LogNewErrorf(log, "taskResult is empty for DeleteVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			return nil, logger.LogNewErrorf(log, "failed to Query volumes: %v, fault: %q, opID: %q",
				volumeIDList, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		}
		volumeInfoResult := interface{}(taskResult).(*cnstypes.CnsQueryVolumeInfoResult)
		log.Infof("QueryVolumeInfo successfully returned volumeInfo volumeIDList %v:, opId: %q",
			volumeIDList, taskInfo.ActivationId)
		return volumeInfoResult, nil
	}
	start := time.Now()
	resp, err := internalQueryVolumeInfo()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryVolumeInfoOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryVolumeInfoOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

func (m *defaultManager) RelocateVolume(ctx context.Context,
	relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	internalRelocateVolume := func() (*object.Task, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			log.Errorf("validateManager failed with err: %+v", err)
			return nil, err
		}

		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}
		res, err := m.virtualCenter.CnsClient.RelocateVolume(ctx, relocateSpecList...)
		if err != nil {
			log.Errorf("CNS RelocateVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}
		return res, err
	}
	start := time.Now()
	resp, err := internalRelocateVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsRelocateVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsRelocateVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// ConfigureVolumeACLs configures net permissions for a given CnsVolumeACLConfigureSpec.
func (m *defaultManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	internalConfigureVolumeACLs := func() error {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return err
		}

		var task *object.Task
		var taskInfo *vim25types.TaskInfo

		task, err = m.virtualCenter.CnsClient.ConfigureVolumeACLs(ctx, spec)
		if err != nil {
			log.Errorf("CNS ConfigureVolumeACLs failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}

		// Get the taskInfo.
		if m.tasksListViewEnabled {
			taskInfo, err = m.waitOnTask(ctx, task.Reference())
		} else {
			taskInfo, err = cns.GetTaskInfo(ctx, task)
		}
		if err != nil {
			log.Errorf("failed to get ConfigureVolumeACLs taskInfo from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			return err
		}
		// Get the taskResult.
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find ConfigureVolumeACLs result from vCenter %q: taskID %q opId %q result: %+v err: %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult, err)
			return err
		}

		if taskResult == nil {
			return logger.LogNewErrorf(log,
				"taskResult is empty for ConfigureVolumeACLs task: %q. ConfigureVolumeACLsSpec: %q",
				taskInfo.ActivationId, spew.Sdump(spec))
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			return logger.LogNewErrorf(log,
				"failed to apply ConfigureVolumeACLs. VolumeID: %s spec: %q, fault: %q, opId: %q",
				spec.VolumeId.Id, spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		}

		log.Infof("ConfigureVolumeACLs: Volume ACLs configured successfully. VolumeName: %q, opId: %q, volumeID: %q",
			spec.VolumeId.Id, taskInfo.ActivationId, volumeOperationRes.VolumeId.Id)
		return nil
	}
	start := time.Now()
	err := internalConfigureVolumeACLs()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsConfigureVolumeACLOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsConfigureVolumeACLOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// RegisterDisk registers a virtual disk as a First Class Disk.
// This method helps in registering VCP volumes as FCD using vslm endpoint
// The method takes 2 parameters, path and name. Path refers to backingDiskURLPath
// containing the vmdkPath and name is any given string for the FCD.
// RegisterDisk API takes this name as optional parameter, so it need not be
// a unique string or anything.
func (m *defaultManager) RegisterDisk(ctx context.Context, path string, name string) (string, error) {
	log := logger.GetLogger(ctx)
	err := validateManager(ctx, m)
	if err != nil {
		log.Errorf("failed to validate volume manager with err: %+v", err)
		return "", err
	}
	// Set up the VC connection.
	err = m.virtualCenter.ConnectVslm(ctx)
	if err != nil {
		log.Errorf("ConnectVslm failed with err: %+v", err)
		return "", err
	}
	globalObjectManager := vslm.NewGlobalObjectManager(m.virtualCenter.VslmClient)
	vStorageObject, err := globalObjectManager.RegisterDisk(ctx, path, name)
	if err != nil {
		alreadyExists, objectID := cnsvsphere.IsAlreadyExists(err)
		if alreadyExists {
			log.Infof("vStorageObject: %q, already exists and registered as FCD, returning success", objectID)
			return objectID, nil
		}
		log.Errorf("failed to register virtual disk %q as first class disk with err: %v", path, err)
		return "", err
	}
	return vStorageObject.Config.Id.Id, nil
}

// RetrieveVStorageObject helps in retreiving virtual disk information for
// a given volume id.
func (m *defaultManager) RetrieveVStorageObject(ctx context.Context,
	volumeID string) (*vim25types.VStorageObject, error) {
	log := logger.GetLogger(ctx)
	err := validateManager(ctx, m)
	if err != nil {
		log.Errorf("failed to validate volume manager with err: %+v", err)
		return nil, err
	}
	// Set up the VC connection
	err = m.virtualCenter.ConnectVslm(ctx)
	if err != nil {
		log.Errorf("ConnectVslm failed with err: %+v", err)
		return nil, err
	}
	globalObjectManager := vslm.NewGlobalObjectManager(m.virtualCenter.VslmClient)
	vStorageObject, err := globalObjectManager.Retrieve(ctx, vim25types.ID{Id: volumeID})
	if err != nil {
		log.Errorf("failed to retrieve virtual disk for volumeID %q with err: %v", volumeID, err)
		return nil, err
	}
	log.Infof("Successfully retrieved vStorageObject object for volumeID: %q", volumeID)
	log.Debugf("vStorageObject for volumeID: %q is %+v", volumeID, vStorageObject)
	return vStorageObject, nil
}

// QueryVolumeAsync returns volumes matching the given filter by using
// CnsQueryAsync API. QueryVolumeAsync takes querySelection spec which helps
// to specify which fields for the query entities to be returned. All volume
// fields would be returned as part of the CnsQueryResult if the querySelection
// parameters are not specified.
func (m *defaultManager) QueryVolumeAsync(ctx context.Context, queryFilter cnstypes.CnsQueryFilter,
	querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	log := logger.GetLogger(ctx)
	err := validateManager(ctx, m)
	if err != nil {
		log.Errorf("validateManager failed with err: %+v", err)
		return nil, err
	}
	// Set up the VC connection.
	err = m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		log.Errorf("ConnectCns failed with err: %+v", err)
		return nil, err
	}
	isvSphere70U3orAbove, err := cnsvsphere.IsvSphereVersion70U3orAbove(ctx, m.virtualCenter.Client.ServiceContent.About)
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"Error while checking the vSphere Version %q to invoke QueryVolumeAsync, Err= %+v",
			m.virtualCenter.Client.ServiceContent.About.Version, err)
	}
	if !isvSphere70U3orAbove {
		msg := fmt.Sprintf("QueryVolumeAsync is not supported in vSphere Version %q",
			m.virtualCenter.Client.ServiceContent.About.Version)
		log.Warnf(msg)
		return nil, cnsvsphere.ErrNotSupported
	}

	// Call the CNS QueryVolumeAsync.
	queryVolumeAsyncTask, err := m.virtualCenter.CnsClient.QueryVolumeAsync(ctx, queryFilter, querySelection)
	if err != nil {
		log.Errorf("CNS QueryVolumeAsync failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
		return nil, err
	}
	var queryVolumeAsyncTaskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		queryVolumeAsyncTaskInfo, err = m.waitOnTask(ctx, queryVolumeAsyncTask.Reference())
	} else {
		queryVolumeAsyncTaskInfo, err = cns.GetTaskInfo(ctx, queryVolumeAsyncTask)
	}
	if err != nil {
		log.Errorf("CNS QueryVolumeAsync failed to get TaskInfo with err: %v", err)
		return nil, err
	}
	queryVolumeAsyncTaskResult, err := cns.GetTaskResult(ctx, queryVolumeAsyncTaskInfo)
	if err != nil {
		log.Errorf("CNS QueryVolumeAsync failed to get TaskResult with err: %v", err)
		return nil, err
	}
	if queryVolumeAsyncTaskResult == nil {
		return nil, logger.LogNewErrorf(log, "taskResult is empty for QueryVolumeAsync task: %q, opID: %q",
			queryVolumeAsyncTaskInfo.Task.Value, queryVolumeAsyncTaskInfo.ActivationId)
	}
	volumeOperationRes := queryVolumeAsyncTaskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to query volumes using CnsQueryVolumeAsync, fault: %q, opID: %q",
			spew.Sdump(volumeOperationRes.Fault), queryVolumeAsyncTaskInfo.ActivationId)
	}
	queryVolumeAsyncResult := interface{}(queryVolumeAsyncTaskResult).(*cnstypes.CnsAsyncQueryResult)
	log.Infof("QueryVolumeAsync successfully returned CnsQueryResult, opId: %q", queryVolumeAsyncTaskInfo.ActivationId)
	log.Debugf("QueryVolumeAsync returned CnsQueryResult: %+v", spew.Sdump(queryVolumeAsyncResult.QueryResult))
	return &queryVolumeAsyncResult.QueryResult, nil
}

func (m *defaultManager) QuerySnapshots(ctx context.Context, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (
	*cnstypes.CnsSnapshotQueryResult, error) {
	internalQuerySnapshots := func() (*cnstypes.CnsSnapshotQueryResult, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}
		// Call the CNS QuerySnapshots.
		querySnapshotsTask, err := m.virtualCenter.CnsClient.QuerySnapshots(ctx, snapshotQueryFilter)
		if err != nil {
			log.Errorf("Failed to get the task of CNS QuerySnapshots with err: %v", err)
			return nil, err
		}
		var querySnapshotsTaskInfo *vim25types.TaskInfo
		if m.tasksListViewEnabled {
			querySnapshotsTaskInfo, err = m.waitOnTask(ctx, querySnapshotsTask.Reference())
		} else {
			querySnapshotsTaskInfo, err = cns.GetTaskInfo(ctx, querySnapshotsTask)
		}
		if err != nil {
			log.Errorf("failed to get taskInfo for QuerySnapshots task from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			return nil, err
		}
		res, err := cns.GetQuerySnapshotsTaskResult(ctx, querySnapshotsTaskInfo)
		if err != nil {
			log.Errorf("failed to get task result for QuerySnapshots task %s with error: %v",
				querySnapshotsTask.Reference().Value, err)
			return nil, err
		}
		return res, err
	}
	start := time.Now()
	resp, err := internalQuerySnapshots()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusQuerySnapshotsOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusQuerySnapshotsOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// Helper function for create snapshot with different behaviors in the idempotency handling
// depends on whether the improved idempotency FSS is enabled.
func (m *defaultManager) createSnapshotWithImprovedIdempotencyCheck(ctx context.Context, volumeID string,
	snapshotName string) (*CnsSnapshotInfo, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the CreateSnapshot task on CNS.
		createSnapshotsTask *object.Task
		// Name of the CnsVolumeOperationRequest instance.
		instanceName = snapshotName + "-" + volumeID
		// Local instance of CreateSnapshot details that needs to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// error
		err error
	)

	if m.idempotencyHandlingEnabled {
		if m.operationStore == nil {
			return nil, logger.LogNewError(log, "operation store cannot be nil")
		}

		volumeOperationDetails, err = m.operationStore.GetRequestDetails(ctx, instanceName)
		switch {
		case err == nil:
			// Validate if previous operation was successful.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess &&
				volumeOperationDetails.VolumeID != "" && volumeOperationDetails.SnapshotID != "" {
				log.Infof("Snapshot with name %q and id %q on Volume %q is already created on CNS with opId: %q.",
					instanceName, volumeOperationDetails.SnapshotID, volumeOperationDetails.VolumeID,
					volumeOperationDetails.OperationDetails.OpID)

				return &CnsSnapshotInfo{
					SnapshotID:                volumeOperationDetails.SnapshotID,
					SourceVolumeID:            volumeOperationDetails.VolumeID,
					SnapshotDescription:       volumeOperationDetails.Name,
					SnapshotCreationTimestamp: volumeOperationDetails.OperationDetails.TaskInvocationTimestamp.Time,
				}, nil
			}
			// Validate if previous operation is pending.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusInProgress &&
				volumeOperationDetails.OperationDetails.TaskID != "" {
				log.Infof("Snapshot with name %s has CreateSnapshot task %s pending on CNS.",
					instanceName,
					volumeOperationDetails.OperationDetails.TaskID)
				taskMoRef := vim25types.ManagedObjectReference{
					Type:  "Task",
					Value: volumeOperationDetails.OperationDetails.TaskID,
				}
				createSnapshotsTask = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
			}
		case apierrors.IsNotFound(err):
			// Instance doesn't exist. This is likely the first attempt to create the snapshot.
			volumeOperationDetails = createRequestDetails(
				instanceName, volumeID, "", 0, metav1.Now(), "", "", "",
				taskInvocationStatusInProgress, "")
		default:
			return nil, err
		}
	} else {
		// get snapshot task details from an in-memory map
		createSnapshotsTask = getPendingCreateSnapshotTaskFromMap(ctx, instanceName)
	}

	defer func() {
		// Persist the operation details before returning if the improved idempotency is enabled. Only success or error
		// needs to be stored as InProgress details are stored when the task is created on CNS.
		if m.idempotencyHandlingEnabled &&
			volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				log.Warnf("failed to store CreateSnapshot details with error: %v", err)
			}
		}
	}()

	if createSnapshotsTask == nil {
		createSnapshotsTask, err = invokeCNSCreateSnapshot(ctx, m.virtualCenter, volumeID, instanceName)
		if err != nil {
			if m.idempotencyHandlingEnabled {
				volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "", "", "",
					taskInvocationStatusError, err.Error())
			}
			return nil, logger.LogNewErrorf(log, "failed to create snapshot with error: %v", err)
		}

		if m.idempotencyHandlingEnabled {
			// Persist the volume operation details.
			volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0,
				volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
				createSnapshotsTask.Reference().Value, "", "", taskInvocationStatusInProgress, "")
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				// Don't return if CreateSnapshot details can't be stored.
				log.Warnf("failed to store CreateSnapshot details with error: %v", err)
			}
		} else {
			// store task details into snapshotTaskMap
			var taskDetails createSnapshotTaskDetails
			taskDetails.task = createSnapshotsTask
			taskDetails.expirationTime = time.Now().Add(time.Hour * time.Duration(
				defaultOpsExpirationTimeInHours))
			func() {
				// put the snapshotTaskMap update operation with locking into an anonymous func
				// to ensure the deferred unlock is called right after the update.
				snapshotTaskMapLock.Lock()
				defer snapshotTaskMapLock.Unlock()
				snapshotTaskMap[instanceName] = &taskDetails
			}()

		}
	}

	// Get the taskInfo and more!
	var createSnapshotsTaskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		createSnapshotsTaskInfo, err = m.waitOnTask(ctx, createSnapshotsTask.Reference())
	} else {
		createSnapshotsTaskInfo, err = cns.GetTaskInfo(ctx, createSnapshotsTask)
	}
	if err != nil {
		if cnsvsphere.IsManagedObjectNotFound(err, createSnapshotsTask.Reference()) {
			log.Infof("CreateSnapshot task %s not found in vCenter. Querying CNS "+
				"to determine if the snapshot %s was successfully created.",
				createSnapshotsTask.Reference().Value, instanceName)
			queriedCnsSnapshot, ok := queryCreatedSnapshotByName(ctx, m, volumeID, instanceName)
			if ok {
				// Create the volumeOperationDetails object for persistence
				volumeOperationDetails = createRequestDetails(
					instanceName, volumeID, queriedCnsSnapshot.SnapshotId.Id, 0,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
					createSnapshotsTask.Reference().Value, "",
					"", taskInvocationStatusSuccess, "")

				log.Infof("CreateSnapshot: Snapshot with name %s on volume %q is confirmed to be created "+
					"successfully with SnapshotID %q", instanceName, volumeID, queriedCnsSnapshot.SnapshotId.Id)

				return &CnsSnapshotInfo{
					SnapshotID:                queriedCnsSnapshot.SnapshotId.Id,
					SourceVolumeID:            volumeID,
					SnapshotDescription:       snapshotName,
					SnapshotCreationTimestamp: queriedCnsSnapshot.CreateTime,
				}, nil
			} else {
				errMsg := fmt.Sprintf("Snapshot with name %s on volume %q is not present in CNS. "+
					"Marking task %s as failed.", snapshotName, volumeID, createSnapshotsTask.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
					createSnapshotsTask.Reference().Value, "",
					"", taskInvocationStatusError, errMsg)
				return nil, logger.LogNewError(log, errMsg)
			}
		}
		return nil, logger.LogNewErrorf(log, "Failed to get taskInfo for CreateSnapshots task "+
			"from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
	}
	log.Infof("CreateSnapshots: VolumeID: %q, opId: %q", volumeID, createSnapshotsTaskInfo.ActivationId)

	// Get the taskResult
	createSnapshotsTaskResult, err := cns.GetTaskResult(ctx, createSnapshotsTaskInfo)
	if err != nil || createSnapshotsTaskResult == nil {
		return nil, logger.LogNewErrorf(log, "unable to find the task result for CreateSnapshots task "+
			"from vCenter %q. taskID: %q, opId: %q createResults: %+v",
			m.virtualCenter.Config.Host, createSnapshotsTaskInfo.Task.Value, createSnapshotsTaskInfo.ActivationId,
			createSnapshotsTaskResult)
	}

	// Handle snapshot operation result
	createSnapshotsOperationRes := createSnapshotsTaskResult.GetCnsVolumeOperationResult()
	if createSnapshotsOperationRes.Fault != nil {
		errMsg := fmt.Sprintf("failed to create snapshot %q on volume %q with fault: %q, opID: %q",
			instanceName, volumeID, spew.Sdump(createSnapshotsOperationRes.Fault),
			createSnapshotsTaskInfo.ActivationId)

		if m.idempotencyHandlingEnabled {
			volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0,
				volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, createSnapshotsTask.Reference().Value,
				"", createSnapshotsTaskInfo.ActivationId, taskInvocationStatusError, errMsg)
		} else {
			// Remove the task details from map when the current task fails
			_, ok := snapshotTaskMap[instanceName]
			if ok {
				func() {
					// put the snapshotTaskMap update operation with locking into an anonymous func
					// to ensure the deferred unlock is called right after the update.
					snapshotTaskMapLock.Lock()
					defer snapshotTaskMapLock.Unlock()
					log.Debugf("Deleted task for %s from snapshotTaskMap because the task has failed",
						instanceName)
					delete(snapshotTaskMap, instanceName)
				}()
			}
		}

		return nil, logger.LogNewError(log, errMsg)
	}

	snapshotCreateResult := interface{}(createSnapshotsTaskResult).(*cnstypes.CnsSnapshotCreateResult)

	cnsSnapshotInfo := &CnsSnapshotInfo{
		SnapshotID:                snapshotCreateResult.Snapshot.SnapshotId.Id,
		SourceVolumeID:            snapshotCreateResult.Snapshot.VolumeId.Id,
		SnapshotDescription:       snapshotCreateResult.Snapshot.Description,
		SnapshotCreationTimestamp: snapshotCreateResult.Snapshot.CreateTime,
	}

	if m.idempotencyHandlingEnabled {
		// create the volumeOperationDetails object for persistence
		volumeOperationDetails = createRequestDetails(
			instanceName, cnsSnapshotInfo.SourceVolumeID, cnsSnapshotInfo.SnapshotID, 0,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, createSnapshotsTask.Reference().Value,
			"", createSnapshotsTaskInfo.ActivationId, taskInvocationStatusSuccess, "")
	}

	log.Infof("CreateSnapshot: Snapshot created successfully. VolumeID: %q, SnapshotID: %q, "+
		"SnapshotCreateTime: %q, opId: %q", cnsSnapshotInfo.SourceVolumeID, cnsSnapshotInfo.SnapshotID,
		cnsSnapshotInfo.SnapshotCreationTimestamp, createSnapshotsTaskInfo.ActivationId)

	return cnsSnapshotInfo, nil
}

// The snapshotName parameter denotes the snapshot description required by CNS CreateSnapshot API.
// This parameter is expected to be filled with the CSI CreateSnapshotRequest Name,
// which is generated by the CSI snapshotter sidecar.
func (m *defaultManager) CreateSnapshot(
	ctx context.Context, volumeID string, snapshotName string) (*CnsSnapshotInfo, error) {
	internalCreateSnapshot := func() (*CnsSnapshotInfo, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		// Set up the VC connection
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "ConnectCns failed with err: %+v", err)
		}

		return m.createSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, snapshotName)
	}

	start := time.Now()
	cnsSnapshotInfo, err := internalCreateSnapshot()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsCreateSnapshotOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsCreateSnapshotOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return cnsSnapshotInfo, err
}

// Helper function for create snapshot with different behaviors in the idempotency handling
// depends on whether the improved idempotency FSS is enabled.
func (m *defaultManager) deleteSnapshotWithImprovedIdempotencyCheck(
	ctx context.Context, volumeID string, snapshotID string) error {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the DeleteVolume task on CNS.
		deleteSnapshotTask *object.Task
		// Name of the CnsVolumeOperationRequest instance.
		instanceName = "deletesnapshot-" + volumeID + "-" + snapshotID
		// Local instance of DeleteSnapshot details that needs to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// error
		err error
	)

	if m.idempotencyHandlingEnabled {
		if m.operationStore == nil {
			return logger.LogNewError(log, "operation store cannot be nil")
		}

		volumeOperationDetails, err = m.operationStore.GetRequestDetails(ctx, instanceName)
		switch {
		case err == nil:
			if volumeOperationDetails.OperationDetails != nil {
				// Validate if previous attempt was successful.
				if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess {
					log.Infof("Snapshot id %q on Volume %q is already deleted on CNS with opId: %q.",
						snapshotID, volumeID, volumeOperationDetails.OperationDetails.OpID)
					return nil
				}
				// Validate if previous operation is pending.
				if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusInProgress &&
					volumeOperationDetails.OperationDetails.TaskID != "" {
					taskMoRef := vim25types.ManagedObjectReference{
						Type:  "Task",
						Value: volumeOperationDetails.OperationDetails.TaskID,
					}
					deleteSnapshotTask = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
				}
			}
		case apierrors.IsNotFound(err):
			volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, metav1.Now(),
				"", "", "", taskInvocationStatusInProgress, "")
		default:
			return err
		}
	}

	defer func() {
		// Persist the operation details before returning. Only success or error
		// needs to be stored as InProgress details are stored when the task is
		// created on CNS.
		if m.idempotencyHandlingEnabled &&
			volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				log.Warnf("failed to store DeleteSnapshot operation details with error: %v", err)
			}
		}
	}()

	if deleteSnapshotTask == nil {
		deleteSnapshotTask, err = invokeCNSDeleteSnapshot(ctx, m.virtualCenter, volumeID, snapshotID)
		if err != nil {
			if cnsvsphere.IsNotFoundError(err) {
				log.Infof("SnapshotID: %q on volume with volumeID: %q, not found, thus returning success",
					snapshotID, volumeID)
				if m.idempotencyHandlingEnabled {
					volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
						metav1.Now(), "", "", "", taskInvocationStatusSuccess, "")
				}
				return nil
			}

			if m.idempotencyHandlingEnabled {
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
					metav1.Now(), "", "", "", taskInvocationStatusError, err.Error())
			}

			return logger.LogNewErrorf(log, "CNS DeleteSnapshot failed from the vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
		}

		if m.idempotencyHandlingEnabled {
			volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
				metav1.Now(), deleteSnapshotTask.Reference().Value, "", "", taskInvocationStatusInProgress, "")
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				log.Warnf("failed to store DeleteSnapshot operation details with error: %v", err)
			}
		}
	}

	// Get the taskInfo
	var deleteSnapshotsTaskInfo *vim25types.TaskInfo
	if m.tasksListViewEnabled {
		deleteSnapshotsTaskInfo, err = m.waitOnTask(ctx, deleteSnapshotTask.Reference())
	} else {
		deleteSnapshotsTaskInfo, err = cns.GetTaskInfo(ctx, deleteSnapshotTask)
	}

	if err != nil {
		if cnsvsphere.IsManagedObjectNotFound(err, deleteSnapshotTask.Reference()) {
			log.Infof("Snapshot %q on volume %q might have already been deleted "+
				"with the error %v. Calling CNS QuerySnapshots API to confirm it", snapshotID, volumeID, err)
			if validateSnapshotDeleted(ctx, m, volumeID, snapshotID) {
				if m.idempotencyHandlingEnabled {
					// Create the volumeOperationDetails object for persistence
					volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
						volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
						deleteSnapshotTask.Reference().Value, "", volumeOperationDetails.OperationDetails.OpID,
						taskInvocationStatusSuccess, "")
				}

				log.Infof("DeleteSnapshot: Snapshot %q on volume %q is confirmed to be deleted successfully",
					snapshotID, volumeID)

				return nil
			}
		}

		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, deleteSnapshotTask.Reference().Value, "",
			volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusError, err.Error())

		return logger.LogNewErrorf(log, "Failed to get taskInfo for DeleteSnapshots task from vCenter %q with err: %v",
			m.virtualCenter.Config.Host, err)
	}

	log.Infof("DeleteSnapshot: VolumeID: %q, SnapshotID: %q, opId: %q", volumeID, snapshotID,
		deleteSnapshotsTaskInfo.ActivationId)

	// Get the taskResult
	deleteSnapshotsTaskResult, err := getTaskResultFromTaskInfo(ctx, deleteSnapshotsTaskInfo)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get the task result for DeleteSnapshots task "+
			"from vCenter %q. taskID: %q, opId: %q createResults: %+v", m.virtualCenter.Config.Host,
			deleteSnapshotsTaskInfo.Task.Value, deleteSnapshotsTaskInfo.ActivationId, deleteSnapshotsTaskResult)
	}
	if deleteSnapshotsTaskResult == nil {
		return logger.LogNewErrorf(log, "task result is empty for DeleteSnapshot task: %q, opID: %q",
			deleteSnapshotsTaskInfo.Task.Value, deleteSnapshotsTaskInfo.ActivationId)
	}

	// Handle snapshot operation result
	deleteSnapshotsOperationRes := deleteSnapshotsTaskResult.GetCnsVolumeOperationResult()
	if deleteSnapshotsOperationRes.Fault != nil {
		err = soap.WrapVimFault(deleteSnapshotsOperationRes.Fault.Fault)

		isInvalidArgumentError := cnsvsphere.IsInvalidArgumentError(err)
		var invalidProperty string
		if isInvalidArgumentError {
			invalidProperty = deleteSnapshotsOperationRes.Fault.Fault.(*vim25types.InvalidArgument).InvalidProperty
		}

		// Ignore errors, NotFound and InvalidArgument, in DeleteSnapshot
		if cnsvsphere.IsVimFaultNotFoundError(err) {
			log.Infof("Snapshot %q on volume %q might have already been deleted "+
				"with the error %v. Ignore the error for DeleteSnapshot", snapshotID, volumeID,
				spew.Sdump(deleteSnapshotsOperationRes.Fault))
		} else if isInvalidArgumentError && invalidProperty == "" {
			log.Infof("Snapshot %q on volume %q might have already been deleted "+
				"with the error %v. Ignore the error for DeleteSnapshot", snapshotID, volumeID,
				spew.Sdump(deleteSnapshotsOperationRes.Fault))
		} else {
			errMsg := fmt.Sprintf("failed to delete snapshot %q on volume %q. fault: %q, opId: %q",
				snapshotID, volumeID, spew.Sdump(deleteSnapshotsOperationRes.Fault), deleteSnapshotsTaskInfo.ActivationId)

			if m.idempotencyHandlingEnabled {
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, deleteSnapshotTask.Reference().Value,
					"", deleteSnapshotsTaskInfo.ActivationId, taskInvocationStatusError, errMsg)
			}

			return logger.LogNewError(log, errMsg)
		}
	}

	if m.idempotencyHandlingEnabled {
		// create the volumeOperationDetails object for persistence
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, deleteSnapshotTask.Reference().Value, "",
			deleteSnapshotsTaskInfo.ActivationId, taskInvocationStatusSuccess, "")
	}

	log.Infof("DeleteSnapshot: Snapshot %q on volume %q is deleted successfully. opId: %q", snapshotID,
		volumeID, deleteSnapshotsTaskInfo.ActivationId)

	return nil
}

func (m *defaultManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string) error {
	internalDeleteSnapshot := func() error {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return err
		}
		// Set up the VC connection
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return err
		}

		return m.deleteSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, snapshotID)
	}

	start := time.Now()
	err := internalDeleteSnapshot()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteSnapshotOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteSnapshotOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// ProtectVolumeFromVMDeletion helps set keepAfterDeleteVm control flag for given volumeID
func (m *defaultManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	err := validateManager(ctx, m)
	if err != nil {
		log.Errorf("failed to validate volume manager with err: %+v", err)
		return err
	}
	// Set up the VC connection
	err = m.virtualCenter.ConnectVslm(ctx)
	if err != nil {
		log.Errorf("ConnectVslm failed with err: %+v", err)
		return err
	}
	globalObjectManager := vslm.NewGlobalObjectManager(m.virtualCenter.VslmClient)
	err = globalObjectManager.SetControlFlags(ctx, vim25types.ID{Id: volumeID}, []string{
		string(vim25types.VslmVStorageObjectControlFlagKeepAfterDeleteVm)})
	if err != nil {
		log.Errorf("failed to set control flag keepAfterDeleteVm  for volumeID %q with err: %v", volumeID, err)
		return err
	}
	log.Infof("Successfully set keepAfterDeleteVm control flag for volumeID: %q", volumeID)
	return nil
}
