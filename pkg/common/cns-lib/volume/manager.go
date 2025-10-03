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
	"strconv"
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
	"k8s.io/apimachinery/pkg/api/resource"
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

	// VolumeOperationTimeoutInSeconds specifies the default CSI operation timeout in seconds
	VolumeOperationTimeoutInSeconds = 300

	listviewAdditionError = "failed to add task to list view"

	// defaultOpsExpirationTimeInHours is expiration time for create volume operations.
	// TODO: This timeout will be configurable in future releases
	defaultOpsExpirationTimeInHours = 1

	// maxLengthOfVolumeNameInCNS is the maximum length of CNS volume name.
	maxLengthOfVolumeNameInCNS = 80
	// Alias for TaskInvocationStatus constants.
	taskInvocationStatusInProgress = cnsvolumeoperationrequest.TaskInvocationStatusInProgress
	taskInvocationStatusSuccess    = cnsvolumeoperationrequest.TaskInvocationStatusSuccess
	taskInvocationStatusError      = cnsvolumeoperationrequest.TaskInvocationStatusError
	// Task status PartiallyFailed should be used when object creation completes successfully, but post-processing
	// fails. e.g. When snapshot creation is successful, but update db failed, then task status will be marked as
	// PartiallyFailed.
	taskInvocationStatusPartiallyFailed = cnsvolumeoperationrequest.TaskInvocationStatusPartiallyFailed

	// MbInBytes is the number of bytes in one mebibyte.
	MbInBytes = int64(1024 * 1024)
)

// Manager provides functionality to manage volumes.
type Manager interface {
	// CreateVolume creates a new volume given its spec.
	// When CreateVolume failed, the second return value (faultType) and third return value(error) need to be set, and
	// should not be nil.
	// extraParams can be used to send in any values not present in the CnsVolumeCreateSpec param.
	CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec, extraParams interface{}) (
		*CnsVolumeInfo, string, error)
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
	// UpdateVolumeCrypto encrypts a volume given its spec.
	UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error
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
	ExpandVolume(ctx context.Context, volumeID string, size int64, extraParams interface{}) (string, error)
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
	CreateSnapshot(ctx context.Context, volumeID string, desc string, extraParams interface{}) (*CnsSnapshotInfo, error)
	// DeleteSnapshot helps delete a snapshot for a block volume
	DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
		extraParams interface{}) (*CnsSnapshotInfo, error)
	// QuerySnapshots retrieves the list of snapshots based on the query filter.
	QuerySnapshots(ctx context.Context, snapshotQueryFilter cnstypes.CnsSnapshotQueryFilter) (
		*cnstypes.CnsSnapshotQueryResult, error)
	// MonitorCreateVolumeTask monitors the CNS task which is created for volume creation
	// as part of volume idempotency feature
	MonitorCreateVolumeTask(ctx context.Context,
		volumeOperationDetails **cnsvolumeoperationrequest.VolumeOperationRequestDetails, task *object.Task,
		volNameFromInputSpec, clusterID string) (*CnsVolumeInfo,
		string, error)
	// GetOperationStore returns the VolumeOperationRequest interface
	GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest
	// IsListViewReady returns the status of the listview + property collector mechanism
	IsListViewReady() bool
	// SetListViewNotReady explicitly states the listview state as not ready
	// use case: unit tests
	SetListViewNotReady(ctx context.Context)
	// BatchAttachVolumes attaches multiple volumes to a virtual machine.
	BatchAttachVolumes(ctx context.Context,
		vm *cnsvsphere.VirtualMachine, batchAttachRequest []BatchAttachRequest) ([]BatchAttachResult, string, error)
	// UnregisterVolume unregisters a volume from CNS.
	// If unregisterDisk is true, it will also unregister the disk from FCD.
	UnregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) error
	// SyncVolume returns the aggregated capacity for volumes
	SyncVolume(ctx context.Context, syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error)
}

// CnsVolumeInfo hold information related to volume created by CNS.
type CnsVolumeInfo struct {
	DatastoreURL string
	VolumeID     cnstypes.CnsVolumeId
	Clusters     []vim25types.ManagedObjectReference
}

type CnsSnapshotInfo struct {
	SnapshotID                          string
	SourceVolumeID                      string
	SnapshotDescription                 string
	AggregatedSnapshotCapacityInMb      int64
	SnapshotLatestOperationCompleteTime time.Time
}

// CreateVolumeExtraParams consist of values required by the CreateVolume interface and
// are not present in the CNS CreateVolume spec.
type CreateVolumeExtraParams struct {
	VolSizeBytes                            int64
	StorageClassName                        string
	Namespace                               string
	IsPodVMOnStretchSupervisorFSSEnabled    bool
	IsMultipleClustersPerVsphereZoneEnabled bool
}

// CreateSnapshotExtraParams consist of values required by the CreateSnapshot interface and
// are not present in the CNS CreateSnapshot spec.
type CreateSnapshotExtraParams struct {
	StorageClassName               string
	StoragePolicyID                string
	Namespace                      string
	Capacity                       *resource.Quantity
	IsStorageQuotaM2FSSEnabled     bool
	IsCSITransactionSupportEnabled bool
}

// DeleteSnapshotExtraParams consist of values required by the DeleteSnapshot interface and
// are not present in the CNS DeleteSnapshot spec.
type DeletesnapshotExtraParams struct {
	IsStorageQuotaM2FSSEnabled bool
	StorageClassName           string
	StoragePolicyID            string
	Namespace                  string
	Capacity                   *resource.Quantity
}

// ExpandVolumeExtraParams consist of values required by the ExpandVolume interface and
// are not present in the CNS ExpandVolume spec
type ExpandVolumeExtraParams struct {
	StorageClassName string
	StoragePolicyID  string
	Namespace        string
	// Capacity stores the original volume size which is from CNSVolumeInfo
	Capacity                             *resource.Quantity
	IsPodVMOnStretchSupervisorFSSEnabled bool
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
	idempotencyHandlingEnabled, multivCenterEnabled,
	multivCenterTopologyDeployment bool,
	clusterFlavor cnstypes.CnsClusterFlavor) (Manager, error) {
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
			clusterFlavor:              clusterFlavor,
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
			clusterFlavor:                  clusterFlavor,
		}
		managerInstanceMap[vc.Config.Host] = managerInstance
	}
	err := managerInstance.initListView(ctx)
	if err != nil {
		return nil, err
	}
	return managerInstance, nil
}

// DefaultManager provides functionality to manage volumes.
type defaultManager struct {
	virtualCenter                  *cnsvsphere.VirtualCenter
	operationStore                 cnsvolumeoperationrequest.VolumeOperationRequest
	idempotencyHandlingEnabled     bool
	multivCenterTopologyDeployment bool
	listViewIf                     ListViewIf
	clusterFlavor                  cnstypes.CnsClusterFlavor
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
	m.listViewIf.ResetVirtualCenter(ctx, managerInstance.virtualCenter)
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
	volNameFromInputSpec, clusterID string) (*CnsVolumeInfo, string, error) {
	var (
		err                               error
		faultType                         string
		taskInfo                          *vim25types.TaskInfo
		vCenterServerForVolumeOperationCR string
	)
	log := logger.GetLogger(ctx)
	log.Debugf("Invoked MonitorCreateVolumeTask for task: %v", task.Reference())
	if m.multivCenterTopologyDeployment {
		vCenterServerForVolumeOperationCR = m.virtualCenter.Config.Host
	}

	taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
					(*volumeOperationDetails).QuotaDetails,
					(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
					vCenterServerForVolumeOperationCR, (*volumeOperationDetails).OperationDetails.TaskID,
					taskInvocationStatusSuccess, "")
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
				(*volumeOperationDetails).QuotaDetails, (*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp,
				task.Reference().Value, vCenterServerForVolumeOperationCR,
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
			(*volumeOperationDetails).QuotaDetails, (*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, vCenterServerForVolumeOperationCR,
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
		if IsNotSupportedFault(ctx, volumeOperationRes.Fault) {
			faultType = "vim25:NotSupported"
			err = fmt.Errorf("failed to create volume with fault: %q", faultType)
		} else if _, ok := volumeOperationRes.Fault.Fault.(*cnstypes.CnsVolumeAlreadyExistsFault); ok {
			faultType = "vim.fault.CnsVolumeAlreadyExistsFault"
			err = fmt.Errorf("failed to create volume with fault: %q", faultType)
		} else {
			// Validate if the volume is already registered.
			faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
			var resp *CnsVolumeInfo
			resp, err = validateCreateVolumeResponseFault(ctx, volNameFromInputSpec, volumeOperationRes)
			if err == nil {
				*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, resp.VolumeID.Id, "", 0,
					(*volumeOperationDetails).QuotaDetails,
					(*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
					vCenterServerForVolumeOperationCR, taskInfo.ActivationId, taskInvocationStatusSuccess, "")
				return resp, faultType, err
			}
		}
		if err != nil {
			log.Errorf("err:%v observed for task: %v, volume name: %q, OpID: %q",
				err.Error(), task.Reference().Value, volNameFromInputSpec, taskInfo.ActivationId)
			*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				(*volumeOperationDetails).QuotaDetails, (*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp,
				task.Reference().Value, vCenterServerForVolumeOperationCR, taskInfo.ActivationId,
				taskInvocationStatusError, err.Error())
			return nil, faultType, err
		}
	}
	// Extract the CnsVolumeInfo from the taskResult.
	resp, faultType, err := getCnsVolumeInfoFromTaskResult(ctx, m.virtualCenter, volNameFromInputSpec,
		volumeOperationRes.VolumeId, taskResult)
	if err != nil {
		*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			(*volumeOperationDetails).QuotaDetails, (*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, vCenterServerForVolumeOperationCR, taskInfo.ActivationId,
			taskInvocationStatusError, err.Error())
	} else {
		*volumeOperationDetails = createRequestDetails(volNameFromInputSpec, resp.VolumeID.Id, "", 0,
			(*volumeOperationDetails).QuotaDetails, (*volumeOperationDetails).OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, vCenterServerForVolumeOperationCR, taskInfo.ActivationId,
			taskInvocationStatusSuccess, "")
	}
	return resp, faultType, err
}

// createVolumeWithImprovedIdempotency leverages the VolumeOperationRequest
// interface to persist CNS task information. It uses this persisted information
// to handle idempotency of CreateVolume callbacks to CNS for the same volume.
func (m *defaultManager) createVolumeWithImprovedIdempotency(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (resp *CnsVolumeInfo, faultType string, finalErr error) {
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
		// quotaInfo consists of values required to populate QuotaDetails in CnsVolumeOperationRequest CR.
		quotaInfo                            *cnsvolumeoperationrequest.QuotaDetails
		isPodVMOnStretchSupervisorFSSEnabled bool
	)

	if extraParams != nil {
		createVolParams, ok := extraParams.(*CreateVolumeExtraParams)
		if !ok {
			return nil, csifault.CSIInternalFault,
				logger.LogNewErrorf(log, "unrecognised type for CreateVolume params: %+v", extraParams)
		}
		log.Debugf("Received CreateVolume extraParams: %+v", *createVolParams)

		isPodVMOnStretchSupervisorFSSEnabled = createVolParams.IsPodVMOnStretchSupervisorFSSEnabled
		if isPodVMOnStretchSupervisorFSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			var storagePolicyID string
			if len(spec.Profile) >= 1 {
				storagePolicyID = spec.Profile[0].(*vim25types.VirtualMachineDefinedProfileSpec).ProfileId
			}
			reservedQty := resource.NewQuantity(createVolParams.VolSizeBytes, resource.BinarySI)
			quotaInfo = &cnsvolumeoperationrequest.QuotaDetails{
				Reserved:         reservedQty,
				StoragePolicyId:  storagePolicyID,
				StorageClassName: createVolParams.StorageClassName,
				Namespace:        createVolParams.Namespace,
			}
			log.Infof("QuotaInfo during CreateVolume call: %+v", *quotaInfo)
		}
	}

	if m.operationStore == nil {
		return nil, csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}
	var vCenterServerForVolumeOperationCR string
	if m.multivCenterTopologyDeployment {
		vCenterServerForVolumeOperationCR = m.virtualCenter.Config.Host
	}

	// Determine if CNS CreateVolume needs to be invoked.
	volumeOperationDetails, finalErr = m.operationStore.GetRequestDetails(ctx, volNameFromInputSpec)
	switch {
	case finalErr == nil:
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
			if IsTaskPending(volumeOperationDetails) {
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
	case !apierrors.IsNotFound(finalErr):
		return nil, csifault.CSIInternalFault, finalErr
	}
	defer func() {
		// Persist the operation details before returning. Only success or error
		// needs to be stored as InProgress details are stored when the task is
		// created on CNS.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {

			if m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && isPodVMOnStretchSupervisorFSSEnabled {
				// Decrease the reserved field in QuotaDetails when the CreateVolume task is
				// successful or has errored out.
				taskStatus := volumeOperationDetails.OperationDetails.TaskStatus
				if (taskStatus == taskInvocationStatusSuccess || taskStatus == taskInvocationStatusError) &&
					volumeOperationDetails.QuotaDetails != nil {
					volumeOperationDetails.QuotaDetails.Reserved = resource.NewQuantity(0,
						resource.BinarySI)
					log.Infof("Setting the reserved field for VolumeOperationDetails instance %s to 0",
						volumeOperationDetails.Name)
				}
				tempErr := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if finalErr == nil && tempErr != nil {
					log.Errorf("failed to store CreateVolume details with error: %v", tempErr)
					finalErr = tempErr
				}
			} else {
				err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if err != nil {
					log.Warnf("failed to store CreateVolume details with error: %v", err)
				}
			}
		}
	}()
	if task == nil {
		// The task object is nil in two cases:
		// - No previous CreateVolume task for this volume was retrieved from the
		//   persistent store.
		// - The previous CreateVolume task failed.
		// In both cases, invoke CNS CreateVolume again.
		volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			quotaInfo, metav1.Now(), "", vCenterServerForVolumeOperationCR, "",
			taskInvocationStatusInProgress, "")
		err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			// Don't return if CreateVolume details can't be stored.
			log.Warnf("failed to store CreateVolume details with error: %v", err)
		}
		task, finalErr = invokeCNSCreateVolume(ctx, m.virtualCenter, spec)
		if finalErr != nil {
			log.Errorf("failed to create volume with error: %v", finalErr)
			volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				quotaInfo, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "",
				vCenterServerForVolumeOperationCR, "", taskInvocationStatusError, finalErr.Error())
			faultType = ExtractFaultTypeFromErr(ctx, finalErr)
			return nil, faultType, finalErr
		}
		if !isStaticallyProvisioned(spec) {
			// Persist task details only for dynamically provisioned volumes.
			volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
				quotaInfo, metav1.Now(), task.Reference().Value, vCenterServerForVolumeOperationCR, "",
				taskInvocationStatusInProgress, "")
			err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
			if err != nil {
				// Don't return if CreateVolume details can't be stored.
				log.Warnf("failed to store CreateVolume details with error: %v", err)
			}
		}
	}

	return m.MonitorCreateVolumeTask(ctx, &volumeOperationDetails, task, volNameFromInputSpec,
		spec.Metadata.ContainerClusterArray[0].ClusterId)
}

// createVolumeWithTransaction creates volume with supplied PVC UUID as VolumeID in the CreateVolumeSpec
func (m *defaultManager) createVolumeWithTransaction(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (resp *CnsVolumeInfo, faultType string, finalErr error) {
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
		// quotaInfo consists of values required to populate QuotaDetails in CnsVolumeOperationRequest CR.
		quotaInfo                            *cnsvolumeoperationrequest.QuotaDetails
		isPodVMOnStretchSupervisorFSSEnabled bool
	)

	if extraParams != nil {
		createVolParams, ok := extraParams.(*CreateVolumeExtraParams)
		if !ok {
			return nil, csifault.CSIInternalFault,
				logger.LogNewErrorf(log, "unrecognised type for CreateVolume params: %+v", extraParams)
		}
		log.Debugf("Received CreateVolume extraParams: %+v", *createVolParams)

		isPodVMOnStretchSupervisorFSSEnabled = createVolParams.IsPodVMOnStretchSupervisorFSSEnabled
		if isPodVMOnStretchSupervisorFSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			var storagePolicyID string
			if len(spec.Profile) >= 1 {
				storagePolicyID = spec.Profile[0].(*vim25types.VirtualMachineDefinedProfileSpec).ProfileId
			}
			reservedQty := resource.NewQuantity(createVolParams.VolSizeBytes, resource.BinarySI)
			quotaInfo = &cnsvolumeoperationrequest.QuotaDetails{
				Reserved:         reservedQty,
				StoragePolicyId:  storagePolicyID,
				StorageClassName: createVolParams.StorageClassName,
				Namespace:        createVolParams.Namespace,
			}
			log.Infof("QuotaInfo during CreateVolume call: %+v", *quotaInfo)
		}
	}

	if m.operationStore == nil {
		return nil, csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}
	var vCenterServerForVolumeOperationCR string
	if m.multivCenterTopologyDeployment {
		vCenterServerForVolumeOperationCR = m.virtualCenter.Config.Host
	}

	volumeOperationDetails, finalErr = m.operationStore.GetRequestDetails(ctx, volNameFromInputSpec)
	if finalErr != nil && !apierrors.IsNotFound(finalErr) {
		return nil, csifault.CSIInternalFault, finalErr
	}
	defer func() {
		// Persist the operation details before returning. Only success or error
		// needs to be stored as InProgress details are stored when the task is
		// created on CNS.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {

			if m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && isPodVMOnStretchSupervisorFSSEnabled {
				// Decrease the reserved field in QuotaDetails when the CreateVolume task is
				// successful or has errored out.
				taskStatus := volumeOperationDetails.OperationDetails.TaskStatus
				if (taskStatus == taskInvocationStatusSuccess || taskStatus == taskInvocationStatusError) &&
					volumeOperationDetails.QuotaDetails != nil {
					volumeOperationDetails.QuotaDetails.Reserved = resource.NewQuantity(0,
						resource.BinarySI)
					log.Infof("Setting the reserved field for VolumeOperationDetails instance %s to 0",
						volumeOperationDetails.Name)
				}
				tempErr := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if finalErr == nil && tempErr != nil {
					log.Errorf("failed to store CreateVolume details with error: %v", tempErr)
					finalErr = tempErr
				}
			} else {
				err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if err != nil {
					log.Warnf("failed to store CreateVolume details with error: %v", err)
				}
			}
		}
	}()
	volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
		quotaInfo, metav1.Now(), "", vCenterServerForVolumeOperationCR, "",
		taskInvocationStatusInProgress, "")
	err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
	if err != nil {
		// Don't return if CreateVolume details can't be stored.
		log.Warnf("failed to store CreateVolume details with error: %v", err)
	}
	task, finalErr = invokeCNSCreateVolume(ctx, m.virtualCenter, spec)
	if finalErr != nil {
		log.Errorf("failed to create volume with error: %v", finalErr)
		volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			quotaInfo, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "",
			vCenterServerForVolumeOperationCR, "", taskInvocationStatusError, finalErr.Error())
		faultType = ExtractFaultTypeFromErr(ctx, finalErr)
		return nil, faultType, finalErr
	}
	if !isStaticallyProvisioned(spec) {
		// Persist task details only for dynamically provisioned volumes.
		volumeOperationDetails = createRequestDetails(volNameFromInputSpec, "", "", 0,
			quotaInfo, metav1.Now(), task.Reference().Value, vCenterServerForVolumeOperationCR, "",
			taskInvocationStatusInProgress, "")
		err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			// Don't return if CreateVolume details can't be stored.
			log.Warnf("failed to store CreateVolume details with error: %v", err)
		}
	}

	return m.MonitorCreateVolumeTask(ctx, &volumeOperationDetails, task, volNameFromInputSpec,
		spec.Metadata.ContainerClusterArray[0].ClusterId)
}

// IsTaskPending returns true in two cases -
// 1. if the task status was in progress
// 2. if the status was an error but the error was for adding the task to the listview
// (as we don't know the status of the task on CNS)
func IsTaskPending(volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails) bool {
	if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusInProgress &&
		volumeOperationDetails.OperationDetails.TaskID != "" {
		return true
	} else if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusError &&
		strings.Contains(volumeOperationDetails.OperationDetails.Error, listviewAdditionError) {
		return true
	}
	return false
}

func (m *defaultManager) waitOnTask(csiOpContext context.Context,
	taskMoRef vim25types.ManagedObjectReference) (*vim25types.TaskInfo, error) {
	log := logger.GetLogger(csiOpContext)
	if m.listViewIf == nil {
		err := m.initListView(context.Background())
		if err != nil {
			return nil, err
		}
	}
	ch := make(chan TaskResult, 1)
	err := m.listViewIf.AddTask(csiOpContext, taskMoRef, ch)
	if errors.Is(err, ErrListViewTaskAddition) {
		return nil, logger.LogNewErrorf(log, "%s. err: %v", listviewAdditionError, err)
	} else if err != nil {
		// in case the task is not found in VC, we are returning a ManagedObjectNotFound error wrapped as a soap fault
		// we need to return it as is to ensure that further CNS Volume Operations CR gets updated correctly
		return nil, err
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

func (m *defaultManager) initListView(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err error
	m.listViewIf, err = NewListViewImpl(ctx, m.virtualCenter)
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
	taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
func (m *defaultManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec,
	extraParams interface{}) (*CnsVolumeInfo, string, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
			if spec.VolumeId != nil && spec.VolumeId.Id != "" {
				var cnsVolumeInfo *CnsVolumeInfo
				cnsVolumeInfo, faultType, err = m.createVolumeWithTransaction(ctx, spec, extraParams)
				if err != nil {
					log.Errorf("failed to create volume with VolumeID: %q, faultType: %q, err: %v",
						spec.VolumeId.Id, faultType, err)
					if IsCnsVolumeAlreadyExistsFault(ctx, faultType) {
						log.Infof("Observed volume with Id: %q is already Exists. Deleting Volume.", spec.VolumeId.Id)
						deleteFaultType, deleteError := m.deleteVolume(ctx, spec.VolumeId.Id, true)
						if deleteError != nil {
							log.Errorf("failed to delete volume: %q to handle CnsVolumeAlreadyExistsFault , err :%v", spec.VolumeId.Id, err)
							return nil, deleteFaultType, deleteError
						}
						log.Infof("Attempt to re-create volume with Id: %q", spec.VolumeId.Id)
						cnsVolumeInfo, faultType, err = m.createVolumeWithTransaction(ctx, spec, extraParams)
						return cnsVolumeInfo, faultType, err
					}
					return nil, faultType, err
				}
				return cnsVolumeInfo, faultType, err
			} else {
				return m.createVolumeWithImprovedIdempotency(ctx, spec, extraParams)
			}
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

// ensureOperationContextHasATimeout checks if the passed context has a timeout associated with it.
// If there is no timeout set, we set it to 300 seconds. This is the same as set by sidecars.
// If a timeout is already set, we don't change it.
func ensureOperationContextHasATimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	_, ok := ctx.Deadline()
	if !ok {
		// no timeout is set, so we need to set it
		return context.WithTimeout(ctx, VolumeOperationTimeoutInSeconds*time.Second)
	}
	return context.WithCancel(ctx)
}

// AttachVolume attaches a volume to a virtual machine given the spec.
func (m *defaultManager) AttachVolume(ctx context.Context,
	vm *cnsvsphere.VirtualMachine, volumeID string, checkNVMeController bool) (string, string, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
		if volumeOperationRes.Fault != nil && volumeOperationRes.Fault.Fault != nil {
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

			// Check if this is a CnsFault with NotSupported fault cause
			if cnsFault, isCnsFault := volumeOperationRes.Fault.Fault.(*cnstypes.CnsFault); isCnsFault {
				if cnsFault.FaultCause != nil {
					notSupportedFault, isNotSupportedFault := cnsFault.FaultCause.Fault.(*vim25types.NotSupported)
					if isNotSupportedFault {
						log.Infof("observed CnsFault with NotSupported fault cause while attaching volume: %q with vm: %q",
							volumeID, vm.String())

						// Extract the specific error message from NotSupported fault's FaultMessage array
						var errorMessages []string
						for _, faultMsg := range notSupportedFault.FaultMessage {
							if faultMsg.Message != "" {
								errorMessages = append(errorMessages, faultMsg.Message)
							}
						}

						if len(errorMessages) > 0 {
							extractedMessage := strings.Join(errorMessages, " - ")
							log.Infof("NotSupported fault extracted message: %s", extractedMessage)
							return "", faultType, logger.LogNewErrorf(log,
								"%q Failed to attach cns volume: %q to node vm: %q. fault: %q. opId: %q",
								extractedMessage, volumeID, vm.String(), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
						}

						// Fallback to detailed dump for debugging
						log.Debugf("NotSupported fault details: %+v", spew.Sdump(cnsFault.FaultCause))
					}
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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
	taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
		// If volume is not found on host, but is present in CNS DB, we will get vim.fault.NotFound fault.
		// Send back success as the volume is already deleted.
		if IsNotFoundFault(ctx, faultType) {
			log.Infof("DeleteVolume: VolumeID %q, not found, thus returning success", volumeID)
			return "", nil
		}
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
			if IsTaskPending(volumeOperationDetails) {
				taskMoRef := vim25types.ManagedObjectReference{
					Type:  "Task",
					Value: volumeOperationDetails.OperationDetails.TaskID,
				}
				task = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
			}
		}
	case apierrors.IsNotFound(err):
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, nil,
			metav1.Now(), "", "", "", taskInvocationStatusInProgress, "")
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
					nil, metav1.Now(), "", "", "", taskInvocationStatusSuccess, "")
				err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if err != nil {
					log.Warnf("failed to store DeleteVolume details with error: %v", err)
				}
				return "", nil
			}
			log.Errorf("CNS DeleteVolume failed from the  vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
				nil, metav1.Now(), "", "", "", taskInvocationStatusError, err.Error())
			err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
			if err != nil {
				log.Warnf("failed to store DeleteVolume details with error: %v", err)
			}
			return faultType, err
		}
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
			nil, metav1.Now(), task.Reference().Value, "",
			"", taskInvocationStatusInProgress, "")
		err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			log.Warnf("failed to store DeleteVolume details with error: %v", err)
		}
	}

	// Get the taskInfo.
	var taskInfo *vim25types.TaskInfo
	taskInfo, err = m.waitOnTask(ctx, task.Reference())

	if err != nil || taskInfo == nil {
		log.Errorf("failed to get taskInfo for DeleteVolume task from vCenter %q with err: %v",
			m.virtualCenter.Config.Host, err)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			if cnsvsphere.IsManagedObjectNotFound(err, task.Reference()) {
				// Mark the volumeOperationRequest as an error if the corresponding VC task is missing/NotFound.
				msg := fmt.Sprintf("DeleteVolume task %s not found in vCenter.", task.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
					nil, metav1.Now(), task.Reference().Value, "", "", taskInvocationStatusError, msg)
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

		// If volume is not found on host, but is present in CNS DB, we will get vim.fault.NotFound fault.
		// In such a case, send back success as the volume is already deleted.
		if IsNotFoundFault(ctx, faultType) {
			log.Infof("DeleteVolume: VolumeID %q, not found, thus returning success", volumeID)
			return "", nil
		}

		msg := fmt.Sprintf("failed to delete volume: %q, fault: %q, opID: %q",
			volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
			nil, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, "", taskInfo.ActivationId, taskInvocationStatusError, msg)
		return faultType, logger.LogNewError(log, msg)
	}
	log.Infof("DeleteVolume: Volume deleted successfully. volumeID: %q, opId: %q",
		volumeID, taskInfo.ActivationId)
	volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
		nil, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
		task.Reference().Value, "", taskInfo.ActivationId, taskInvocationStatusSuccess, "")
	return "", nil
}

// UpdateVolumeMetadata updates a volume given its spec.
func (m *defaultManager) UpdateVolumeMetadata(ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		taskInfo, err = m.waitOnTask(ctx, task.Reference())

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

// UpdateVolumeCrypto updates a volume given its spec.
func (m *defaultManager) UpdateVolumeCrypto(ctx context.Context, spec *cnstypes.CnsVolumeCryptoUpdateSpec) error {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
	internalUpdateVolumeCrypto := func() error {
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

		cnsUpdateSpecList := []cnstypes.CnsVolumeCryptoUpdateSpec{*spec}
		task, err := m.virtualCenter.CnsClient.UpdateVolumeCrypto(ctx, cnsUpdateSpecList)
		if err != nil {
			log.Errorf("CNS UpdateVolumeCrypto failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		// Get the taskInfo.
		var taskInfo *vim25types.TaskInfo
		taskInfo, err = m.waitOnTask(ctx, task.Reference())

		if err != nil || taskInfo == nil {
			log.Errorf("failed to get UpdateVolumeCrypto taskInfo from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			return err
		}
		log.Infof("UpdateVolumeCrypto: volumeID: %q, opId: %q", spec.VolumeId.Id, taskInfo.ActivationId)
		// Get the task results for the given task.
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find UpdateVolumeCrypto result from vCenter %q: taskID %q, opId %q and updateResults %+v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult)
			return err
		}
		if taskResult == nil {
			return logger.LogNewErrorf(log, "taskResult is empty for UpdateVolumeCrypto task: %q, opId: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			return logger.LogNewErrorf(log, "failed to update volume. updateSpec: %q, fault: %q, opID: %q",
				spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
		}
		log.Infof("UpdateVolumeCrypto: Volume crypto updated successfully. volumeID: %q, opId: %q",
			spec.VolumeId.Id, taskInfo.ActivationId)
		return nil
	}
	start := time.Now()
	err := internalUpdateVolumeCrypto()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsUpdateVolumeCryptoOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsUpdateVolumeCryptoOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// ExpandVolume expands a volume given its spec.
func (m *defaultManager) ExpandVolume(ctx context.Context, volumeID string, size int64,
	extraParams interface{}) (string, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
			return m.expandVolumeWithImprovedIdempotency(ctx, volumeID, size, extraParams)
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
	taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
	size int64, extraParams interface{}) (faultType string, finalErr error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the ExtendVolume task.
		task *object.Task
		// Details to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// CnsVolumeOperationRequest instance name.
		instanceName = "expand-" + volumeID
		// quotaInfo consists of values required to populate QuotaDetails in CnsVolumeOperationRequest CR.
		quotaInfo                            *cnsvolumeoperationrequest.QuotaDetails
		isPodVMOnStretchSupervisorFSSEnabled bool
	)

	if extraParams != nil {
		expandVolParams, ok := extraParams.(*ExpandVolumeExtraParams)
		if !ok {
			return csifault.CSIInternalFault,
				logger.LogNewErrorf(log, "unrecognised type for ExpandVolume params: %+v", extraParams)
		}
		log.Debugf("Received ExpandVolume extraParams: %+v", expandVolParams)

		isPodVMOnStretchSupervisorFSSEnabled = expandVolParams.IsPodVMOnStretchSupervisorFSSEnabled
		if isPodVMOnStretchSupervisorFSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			// For expand volume, reserved field in quotaInfo needs to be set to the difference
			// between the new volume size and the original volume size
			// param "size" is the new volume size passed in. This param is passed in as sizeInMb,
			// and we need to convert it to sizeInBytes
			// expandVolParams.Capacity is the original volume size which is from CNSVolumeInfo.
			sizeInBytes := size * MbInBytes
			reservedQty := resource.NewQuantity(sizeInBytes, resource.BinarySI)
			reservedQty.Sub(*expandVolParams.Capacity)
			quotaInfo = &cnsvolumeoperationrequest.QuotaDetails{
				Reserved:         reservedQty,
				StoragePolicyId:  expandVolParams.StoragePolicyID,
				StorageClassName: expandVolParams.StorageClassName,
				Namespace:        expandVolParams.Namespace,
			}
			log.Infof("QuotaInfo during ExpandVolume call: %+v", *quotaInfo)
		}
	}
	if m.operationStore == nil {
		return csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}

	volumeOperationDetails, finalErr = m.operationStore.GetRequestDetails(ctx, instanceName)
	switch {
	case finalErr == nil:
		if volumeOperationDetails.OperationDetails != nil {
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess &&
				volumeOperationDetails.Capacity >= size {
				log.Infof("Volume with ID %s already expanded to size %v", volumeID, size)
				return "", nil
			}
			if IsTaskPending(volumeOperationDetails) {
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
	case !apierrors.IsNotFound(finalErr):
		return csifault.CSIInternalFault, finalErr
	}
	defer func() {
		// Persist the operation details before returning.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {

			if m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload && isPodVMOnStretchSupervisorFSSEnabled {
				taskStatus := volumeOperationDetails.OperationDetails.TaskStatus
				// Decrease the reserved field in QuotaDetails when the ExpandVolume task is
				// successful or has errored out.
				if (taskStatus == taskInvocationStatusSuccess || taskStatus == taskInvocationStatusError) &&
					volumeOperationDetails.QuotaDetails != nil {
					volumeOperationDetails.QuotaDetails.Reserved = resource.NewQuantity(0,
						resource.BinarySI)
					log.Infof("Setting the reserved field for VolumeOperationDetails instance %s to 0",
						volumeOperationDetails.Name)
				}
				tempErr := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if finalErr == nil && tempErr != nil {
					log.Errorf("failed to store ExpandVolume details with error: %v", tempErr)
					finalErr = tempErr
				}
			} else {
				err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
				if err != nil {
					log.Warnf("failed to store ExpandVolume details with error: %v", err)
				}
			}
		}
	}()

	if task == nil {
		// The task object is nil in two cases:
		// - No previous ExtendVolume task for this volume was retrieved from the
		//   persistent store.
		// - The previous ExtendVolume task failed.
		// In both cases, invoke CNS ExtendVolume.
		volumeOperationDetails = createRequestDetails(instanceName, "", "", size,
			quotaInfo, metav1.Now(), "", "", "", taskInvocationStatusInProgress, "")
		err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			log.Warnf("failed to store ExpandVolume details with error: %v", err)
		}

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
		task, finalErr = m.virtualCenter.CnsClient.ExtendVolume(ctx, cnsExtendSpecList)
		if finalErr != nil {
			faultType = ExtractFaultTypeFromErr(ctx, finalErr)
			if cnsvsphere.IsNotFoundError(finalErr) {
				return faultType, logger.LogNewErrorf(log, "volume %q not found. Cannot expand volume.", volumeID)
			}
			log.Errorf("CNS ExtendVolume failed from the vCenter %q with finalErr: %v",
				m.virtualCenter.Config.Host, finalErr)
			volumeOperationDetails = createRequestDetails(instanceName, "", "", size, quotaInfo,
				metav1.Now(), "", "", "", taskInvocationStatusError, finalErr.Error())
			return faultType, finalErr
		}
		volumeOperationDetails = createRequestDetails(instanceName, "", "", size, quotaInfo,
			metav1.Now(), task.Reference().Value, "", "", taskInvocationStatusInProgress, "")
		err = m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails)
		if err != nil {
			log.Warnf("failed to store ExpandVolume details with error: %v", err)
		}
	}

	var taskInfo *vim25types.TaskInfo
	taskInfo, finalErr = m.waitOnTask(ctx, task.Reference())

	if finalErr != nil {
		if cnsvsphere.IsManagedObjectNotFound(finalErr, task.Reference()) {
			log.Debugf("ExtendVolume task %s not found in vCenter. Querying CNS "+
				"to determine if volume with ID %s was successfully expanded.",
				task.Reference().Value, volumeID)
			if validateVolumeCapacity(ctx, m, volumeID, size) {
				log.Infof("ExpandVolume: Volume expanded successfully to size %d. volumeID: %q, task: %q",
					volumeOperationDetails.Capacity, volumeID, task.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, "", "",
					volumeOperationDetails.Capacity, quotaInfo,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value, "",
					volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusSuccess, "")
				return "", nil
			}
		}
		// WaitForResult can fail for many reasons, including:
		// - CNS restarted and marked "InProgress" tasks as "Failed".
		// - Any other CNS failures.
		// - Any other failures at lower layers like FCD and SPS.
		// In all cases, mark task as failed and retry.
		log.Errorf("failed to expand volume with ID %s with error %+v", volumeID, finalErr)
		volumeOperationDetails = createRequestDetails(instanceName, "", "",
			volumeOperationDetails.Capacity, quotaInfo, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, "", volumeOperationDetails.OperationDetails.TaskID,
			taskInvocationStatusError, finalErr.Error())
		return ExtractFaultTypeFromErr(ctx, finalErr), finalErr
	}

	log.Infof("ExpandVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
	// Get the task results for the given task.
	var taskResult cnstypes.BaseCnsVolumeOperationResult
	taskResult, finalErr = getTaskResultFromTaskInfo(ctx, taskInfo)
	if taskResult == nil {
		return csifault.CSITaskResultEmptyFault,
			logger.LogNewErrorf(log, "taskResult is empty for ExpandVolume task: %q, opID: %q",
				taskInfo.Task.Value, taskInfo.ActivationId)
	}
	if finalErr != nil {
		log.Errorf("failed to get task result for task %s and volume ID %s with error: %v",
			task.Reference().Value, volumeID, finalErr)
		faultType = ExtractFaultTypeFromErr(ctx, finalErr)
		return faultType, finalErr
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		if _, ok := volumeOperationRes.Fault.Fault.(*cnstypes.CnsFault); ok {
			log.Debugf("ExtendVolume task %s returned with CnsFault. Querying CNS to "+
				"determine if volume with ID %s was successfully expanded.",
				task.Reference().Value, volumeID)
			if validateVolumeCapacity(ctx, m, volumeID, size) {
				log.Infof("ExpandVolume: Volume expanded successfully to size %d. volumeID: %q, task: %q",
					volumeOperationDetails.Capacity, volumeID, task.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, "", "",
					volumeOperationDetails.Capacity, quotaInfo,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, task.Reference().Value,
					"", volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusSuccess, "")
				return "", nil
			}
		}
		faultType = ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		volumeOperationDetails = createRequestDetails(instanceName, "", "",
			volumeOperationDetails.Capacity, quotaInfo, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
			task.Reference().Value, "", taskInfo.ActivationId, taskInvocationStatusError,
			volumeOperationRes.Fault.LocalizedMessage)
		return faultType, logger.LogNewErrorf(log, "failed to extend volume: %q, fault: %q, opID: %q",
			volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
	}

	log.Infof("ExpandVolume: Volume expanded successfully to size %d. volumeID: %q, opId: %q",
		volumeOperationDetails.Capacity, volumeID, taskInfo.ActivationId)
	volumeOperationDetails = createRequestDetails(instanceName, "", "",
		volumeOperationDetails.Capacity, quotaInfo, volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
		task.Reference().Value, "", taskInfo.ActivationId, taskInvocationStatusSuccess, "")
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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		res, err := m.virtualCenter.CnsClient.QueryVolume(ctx, &queryFilter)
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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		taskInfo, err = m.waitOnTask(ctx, queryVolumeInfoTask.Reference())

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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		taskInfo, err = m.waitOnTask(ctx, task.Reference())

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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
	queryVolumeAsyncTaskInfo, err = m.waitOnTask(ctx, queryVolumeAsyncTask.Reference())

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
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		querySnapshotsTaskInfo, err = m.waitOnTask(ctx, querySnapshotsTask.Reference())

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
	snapshotName string, extraParams interface{}) (*CnsSnapshotInfo, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the CreateSnapshot task on CNS.
		createSnapshotsTask *object.Task
		// Name of the CnsVolumeOperationRequest instance.
		instanceName = snapshotName + "-" + volumeID
		// Local instance of CreateSnapshot details that needs to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// error
		err                        error
		quotaInfo                  *cnsvolumeoperationrequest.QuotaDetails
		isStorageQuotaM2FSSEnabled bool
	)
	if extraParams != nil {
		createSnapParams, ok := extraParams.(*CreateSnapshotExtraParams)
		if !ok {
			return nil, logger.LogNewErrorf(log, "unrecognised type for CreateSnapshot params: %+v", extraParams)
		}
		log.Debugf("Received CreateSnapshot extraParams: %+v", *createSnapParams)

		isStorageQuotaM2FSSEnabled = createSnapParams.IsStorageQuotaM2FSSEnabled
		if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			quotaInfo = &cnsvolumeoperationrequest.QuotaDetails{
				Reserved:         createSnapParams.Capacity,
				StoragePolicyId:  createSnapParams.StoragePolicyID,
				StorageClassName: createSnapParams.StorageClassName,
				Namespace:        createSnapParams.Namespace,
			}
			log.Infof("QuotaInfo during CreateSnapshot call: %+v", *quotaInfo)
		}
	}
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
				cnsSnapshotInfo := &CnsSnapshotInfo{
					SnapshotID:          volumeOperationDetails.SnapshotID,
					SourceVolumeID:      volumeOperationDetails.VolumeID,
					SnapshotDescription: volumeOperationDetails.Name,
				}
				if volumeOperationDetails.QuotaDetails != nil {
					if volumeOperationDetails.QuotaDetails.AggregatedSnapshotSize != nil {
						cnsSnapshotInfo.AggregatedSnapshotCapacityInMb =
							volumeOperationDetails.QuotaDetails.AggregatedSnapshotSize.Value()
					}
					if !volumeOperationDetails.QuotaDetails.SnapshotLatestOperationCompleteTime.IsZero() {
						cnsSnapshotInfo.SnapshotLatestOperationCompleteTime =
							volumeOperationDetails.QuotaDetails.SnapshotLatestOperationCompleteTime.Time
					}
				}
				return cnsSnapshotInfo, nil
			}
			// Validate if previous operation was PartiallyFailed. If yes, return error from here itself instead
			// of making a CNS call. If we don't return from here, then orphan snapshot gets created in each CNS call
			// and we want to avoid more orphan snapshots being created.
			// User will have to manually cleanup the snapshot that got created, but whose post-processing failed.
			if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusPartiallyFailed &&
				volumeOperationDetails.VolumeID != "" && volumeOperationDetails.SnapshotID != "" {
				return nil, logger.LogNewErrorf(log, "Snapshot with name %q and id %q on volume %q is created on "+
					"CNS, but post-processing failed. You need to manually cleanup this snapshot.",
					instanceName, volumeOperationDetails.SnapshotID, volumeOperationDetails.VolumeID)
			}
			// Validate if previous operation is pending.
			if IsTaskPending(volumeOperationDetails) {
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
				instanceName, volumeID, "", 0, quotaInfo, metav1.Now(), "", "", "",
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
			taskStatus := volumeOperationDetails.OperationDetails.TaskStatus
			if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
				if (taskStatus == taskInvocationStatusSuccess || taskStatus == taskInvocationStatusError) &&
					volumeOperationDetails.QuotaDetails != nil {
					volumeOperationDetails.QuotaDetails.Reserved = resource.NewQuantity(0,
						resource.BinarySI)
					log.Infof("Setting the reserved field for VolumeOperationDetails instance %s to 0",
						volumeOperationDetails.Name)
				}
			}
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				log.Warnf("failed to store CreateSnapshot details with error: %v", err)
			}
		}
	}()

	if createSnapshotsTask == nil {
		createSnapshotsTask, err = invokeCNSCreateSnapshot(ctx, m.virtualCenter, volumeID, instanceName, "")
		if err != nil {
			if m.idempotencyHandlingEnabled {
				volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "", "", "",
					taskInvocationStatusError, err.Error())
			}
			return nil, logger.LogNewErrorf(log, "failed to create snapshot with error: %v", err)
		}

		if m.idempotencyHandlingEnabled {
			// Persist the volume operation details.
			volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
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
	createSnapshotsTaskInfo, err = m.waitOnTask(ctx, createSnapshotsTask.Reference())

	if err != nil {
		if cnsvsphere.IsManagedObjectNotFound(err, createSnapshotsTask.Reference()) {
			log.Infof("CreateSnapshot task %s not found in vCenter. Querying CNS "+
				"to determine if the snapshot %s was successfully created.",
				createSnapshotsTask.Reference().Value, instanceName)
			queriedCnsSnapshot, ok := queryCreatedSnapshotByName(ctx, m, volumeID, instanceName)
			if ok {
				log.Infof("CreateSnapshot: Snapshot with name %s on volume %q is confirmed to be created "+
					"successfully with SnapshotID %q", instanceName, volumeID, queriedCnsSnapshot.SnapshotId.Id)
				cnsSnapshotInfo := &CnsSnapshotInfo{
					SnapshotID:                          queriedCnsSnapshot.SnapshotId.Id,
					SourceVolumeID:                      volumeID,
					SnapshotDescription:                 snapshotName,
					SnapshotLatestOperationCompleteTime: queriedCnsSnapshot.CreateTime,
				}
				if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
					log.Infof("get aggregated Snapshot Capacity for volume with volumeID %q", volumeID)
					aggregatedSnapshotCapacityInMb, err := m.getAggregatedSnapshotSize(ctx, volumeID)
					if err != nil {
						return nil, logger.LogNewErrorf(log, "Failed to get aggregated snapshot size for volume %q with"+
							" error %v", volumeID, err)
					}
					log.Infof("Fetched aggregated Snapshot Capacity is %d for volume with volumeID %q",
						aggregatedSnapshotCapacityInMb, volumeID)
					cnsSnapshotInfo.AggregatedSnapshotCapacityInMb = aggregatedSnapshotCapacityInMb
					aggregatedSnapshotCapacity := resource.NewQuantity(aggregatedSnapshotCapacityInMb*MbInBytes, resource.BinarySI)
					quotaInfo.AggregatedSnapshotSize = aggregatedSnapshotCapacity
					quotaInfo.SnapshotLatestOperationCompleteTime.Time = queriedCnsSnapshot.CreateTime
					log.Infof("Snapshot %q for volume %q confirmed to be created, update quotainfo: %+v",
						queriedCnsSnapshot.SnapshotId.Id, volumeID, *quotaInfo)
				}

				// Create the volumeOperationDetails object for persistence
				volumeOperationDetails = createRequestDetails(
					instanceName, volumeID, queriedCnsSnapshot.SnapshotId.Id, 0, quotaInfo,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
					createSnapshotsTask.Reference().Value, "",
					"", taskInvocationStatusSuccess, "")
				log.Warnf("Using create snapshot timestamp %q instead of create snapshot task"+
					" completion time for snapshot %q and volumeID %q",
					queriedCnsSnapshot.CreateTime, volumeOperationDetails.SnapshotID, volumeID)
				return cnsSnapshotInfo, nil
			} else {
				errMsg := fmt.Sprintf("Snapshot with name %s on volume %q is not present in CNS. "+
					"Marking task %s as failed.", snapshotName, volumeID, createSnapshotsTask.Reference().Value)
				volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
					createSnapshotsTask.Reference().Value, "", "", taskInvocationStatusError, errMsg)
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
			// If we get CnsSnapshotCreatedFault, then it means that snapshot creation is successful, but something in
			// post-processing failed. Persist this task as PartiallyFailed status along with relevant details.
			err = soap.WrapVimFault(createSnapshotsOperationRes.Fault.Fault)
			if cnsvsphere.IsCnsSnapshotCreatedFaultError(err) {
				errMsg = fmt.Sprintf("snapshot %q on volume %q got created, but post-processing failed. "+
					"Fault: %q, opID: %q", instanceName, volumeID, spew.Sdump(createSnapshotsOperationRes.Fault),
					createSnapshotsTaskInfo.ActivationId)
				snapshotId := createSnapshotsOperationRes.Fault.Fault.(*cnstypes.CnsSnapshotCreatedFault).SnapshotId.Id
				volumeOperationDetails = createRequestDetails(instanceName, volumeID, snapshotId, 0, nil,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, createSnapshotsTask.Reference().Value,
					"", createSnapshotsTaskInfo.ActivationId, taskInvocationStatusPartiallyFailed, errMsg)
				return nil, logger.LogNewError(log, errMsg)
			}
			volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, nil,
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
		SnapshotID:                          snapshotCreateResult.Snapshot.SnapshotId.Id,
		SourceVolumeID:                      snapshotCreateResult.Snapshot.VolumeId.Id,
		SnapshotDescription:                 snapshotCreateResult.Snapshot.Description,
		SnapshotLatestOperationCompleteTime: *createSnapshotsTaskInfo.CompleteTime,
	}
	if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		log.Infof("For volumeID %q new AggregatedSnapshotSize is %d and SnapshotLatestOperationCompleteTime is %q",
			volumeID, snapshotCreateResult.AggregatedSnapshotCapacityInMb, *createSnapshotsTaskInfo.CompleteTime)
		cnsSnapshotInfo.AggregatedSnapshotCapacityInMb = snapshotCreateResult.AggregatedSnapshotCapacityInMb
		aggregatedSnapshotCapacity := resource.NewQuantity(snapshotCreateResult.AggregatedSnapshotCapacityInMb*MbInBytes,
			resource.BinarySI)
		quotaInfo.AggregatedSnapshotSize = aggregatedSnapshotCapacity
		quotaInfo.SnapshotLatestOperationCompleteTime.Time = *createSnapshotsTaskInfo.CompleteTime
		log.Infof("Update quotainfo on successful snapshot %q creation for volume %q: %+v",
			cnsSnapshotInfo.SnapshotID, volumeID, *quotaInfo)
	}
	if m.idempotencyHandlingEnabled {
		// create the volumeOperationDetails object for persistence
		volumeOperationDetails = createRequestDetails(
			instanceName, cnsSnapshotInfo.SourceVolumeID, cnsSnapshotInfo.SnapshotID, 0, quotaInfo,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, createSnapshotsTask.Reference().Value,
			"", createSnapshotsTaskInfo.ActivationId, taskInvocationStatusSuccess, "")
	}

	log.Infof("CreateSnapshot: Snapshot created successfully. VolumeID: %q, SnapshotID: %q, "+
		"SnapshotCreateTime: %q, opId: %q", cnsSnapshotInfo.SourceVolumeID, cnsSnapshotInfo.SnapshotID,
		cnsSnapshotInfo.SnapshotLatestOperationCompleteTime, createSnapshotsTaskInfo.ActivationId)

	return cnsSnapshotInfo, nil
}

// createSnapshotWithTransaction is a helper function used to create a snapshot with CSI transaction support.
// It is invoked when the feature flag "csi-transaction-support" is enabled.
// This function ensures that no orphaned snapshots are left behind on the vSphere backend
// in case of failures during the snapshot creation process
func (m *defaultManager) createSnapshotWithTransaction(ctx context.Context, volumeID string,
	snapshotName string, extraParams interface{}) (*CnsSnapshotInfo, string, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the CreateSnapshot task on CNS.
		createSnapshotsTask *object.Task
		// Name of the CnsVolumeOperationRequest instance.
		instanceName = snapshotName + "-" + volumeID
		// Local instance of CreateSnapshot details that needs to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// error
		err                        error
		quotaInfo                  *cnsvolumeoperationrequest.QuotaDetails
		isStorageQuotaM2FSSEnabled bool
	)
	// By default, external-snapshotter sets the snapshot name prefix to "snapshot-".
	// This logic will break if the prefix configuration is changed.
	// In Supervisor deployments, we assume this configuration remains unchanged by admin/DevOps.
	// In Vanilla deployments, we publish the deployment manifest with the default configuration to ensure consistency.
	if !strings.HasPrefix(snapshotName, "snapshot-") {
		return nil, csifault.CSIInternalFault,
			logger.LogNewErrorf(log, "invalid snapshotName %q: must start with 'snapshot-'", snapshotName)
	}
	snapshotID := strings.TrimPrefix(snapshotName, "snapshot-")
	if extraParams != nil {
		createSnapParams, ok := extraParams.(*CreateSnapshotExtraParams)
		if !ok {
			return nil, csifault.CSIInternalFault,
				logger.LogNewErrorf(log, "unrecognised type for CreateSnapshot params: %+v", extraParams)
		}
		log.Debugf("Received CreateSnapshot extraParams: %+v", *createSnapParams)

		isStorageQuotaM2FSSEnabled = createSnapParams.IsStorageQuotaM2FSSEnabled
		if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			quotaInfo = &cnsvolumeoperationrequest.QuotaDetails{
				Reserved:         createSnapParams.Capacity,
				StoragePolicyId:  createSnapParams.StoragePolicyID,
				StorageClassName: createSnapParams.StorageClassName,
				Namespace:        createSnapParams.Namespace,
			}
			log.Infof("QuotaInfo during CreateSnapshot call: %+v", *quotaInfo)
		}
	}
	if m.operationStore == nil {
		return nil, csifault.CSIInternalFault, logger.LogNewError(log, "operation store cannot be nil")
	}
	volumeOperationDetails, err = m.operationStore.GetRequestDetails(ctx, instanceName)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, csifault.CSIInternalFault, err
	}
	defer func() {
		// Persist the operation details before returning if the improved idempotency is enabled. Only success or error
		// needs to be stored as InProgress details are stored when the task is created on CNS.
		if volumeOperationDetails != nil && volumeOperationDetails.OperationDetails != nil &&
			volumeOperationDetails.OperationDetails.TaskStatus != taskInvocationStatusInProgress {
			taskStatus := volumeOperationDetails.OperationDetails.TaskStatus
			if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
				if (taskStatus == taskInvocationStatusSuccess || taskStatus == taskInvocationStatusError) &&
					volumeOperationDetails.QuotaDetails != nil {
					volumeOperationDetails.QuotaDetails.Reserved = resource.NewQuantity(0,
						resource.BinarySI)
					log.Infof("Setting the reserved field for VolumeOperationDetails instance %s to 0",
						volumeOperationDetails.Name)
				}
			}
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				log.Warnf("failed to store CreateSnapshot details with error: %v", err)
			}
		}
	}()
	volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
		metav1.Now(),
		"", "", "", taskInvocationStatusInProgress, "")
	if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
		// Don't return if CreateSnapshot details can't be stored.
		log.Warnf("failed to store CreateSnapshot details with error: %v", err)
	}
	createSnapshotsTask, err = invokeCNSCreateSnapshot(ctx, m.virtualCenter, volumeID, instanceName, snapshotID)
	if err != nil {
		volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "", "", "",
			taskInvocationStatusError, err.Error())
		faultType := ExtractFaultTypeFromErr(ctx, err)
		return nil, faultType, logger.LogNewErrorf(log, "failed to create snapshot with error: %v", err)
	}
	// Persist the volume operation details.
	volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
		volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
		createSnapshotsTask.Reference().Value, "", "", taskInvocationStatusInProgress, "")
	if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
		// Don't return if CreateSnapshot details can't be stored.
		log.Warnf("failed to store CreateSnapshot details with error: %v", err)
	}

	var createSnapshotsTaskInfo *vim25types.TaskInfo
	var faultType string
	createSnapshotsTaskInfo, err = m.waitOnTask(ctx, createSnapshotsTask.Reference())
	if err != nil {
		if createSnapshotsTaskInfo != nil && IsNotSupportedFault(ctx, createSnapshotsTaskInfo.Error) {
			faultType = "vim25:NotSupported"
			err = fmt.Errorf("failed to create snapshot with fault: %q", faultType)
		} else {
			faultType = ExtractFaultTypeFromErr(ctx, err)
		}
		volumeOperationDetails = createRequestDetails(instanceName, volumeID, "", 0, quotaInfo,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, "", "", "",
			taskInvocationStatusError, err.Error())
		return nil, faultType, logger.LogNewErrorf(log, "Failed to get taskInfo for CreateSnapshots task "+
			"from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
	}
	log.Infof("CreateSnapshots: VolumeID: %q, opId: %q", volumeID, createSnapshotsTaskInfo.ActivationId)
	createSnapshotsTaskResult, err := cns.GetTaskResult(ctx, createSnapshotsTaskInfo)
	if err != nil || createSnapshotsTaskResult == nil {
		return nil, "", logger.LogNewErrorf(log, "unable to find the task result for CreateSnapshots task: %q "+
			"from vCenter %q with err: %v", createSnapshotsTaskInfo.Task.Value, m.virtualCenter.Config.Host, err)
	}
	snapshotCreateResult, ok := createSnapshotsTaskResult.(*cnstypes.CnsSnapshotCreateResult)
	if !ok || snapshotCreateResult == nil {
		return nil, "", logger.LogNewErrorf(log,
			"invalid task result: got %T with value %+v", createSnapshotsTaskResult, createSnapshotsTaskResult)
	}
	if snapshotCreateResult.Fault != nil {
		return nil, "", logger.LogNewErrorf(log, "failed to create snapshot %q on volume %q with fault: %+v",
			instanceName, volumeID, snapshotCreateResult.Fault)
	}
	cnsSnapshotInfo := &CnsSnapshotInfo{
		SnapshotID:                          snapshotCreateResult.Snapshot.SnapshotId.Id,
		SourceVolumeID:                      snapshotCreateResult.Snapshot.VolumeId.Id,
		SnapshotDescription:                 snapshotCreateResult.Snapshot.Description,
		SnapshotLatestOperationCompleteTime: *createSnapshotsTaskInfo.CompleteTime,
	}
	if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		log.Infof("For volumeID %q new AggregatedSnapshotSize is %d and SnapshotLatestOperationCompleteTime is %q",
			volumeID, snapshotCreateResult.AggregatedSnapshotCapacityInMb, *createSnapshotsTaskInfo.CompleteTime)
		cnsSnapshotInfo.AggregatedSnapshotCapacityInMb = snapshotCreateResult.AggregatedSnapshotCapacityInMb
		aggregatedSnapshotCapacity := resource.NewQuantity(snapshotCreateResult.AggregatedSnapshotCapacityInMb*MbInBytes,
			resource.BinarySI)
		quotaInfo.AggregatedSnapshotSize = aggregatedSnapshotCapacity
		quotaInfo.SnapshotLatestOperationCompleteTime.Time = *createSnapshotsTaskInfo.CompleteTime
		log.Infof("Update quotainfo on successful snapshot %q creation for volume %q: %+v",
			cnsSnapshotInfo.SnapshotID, volumeID, *quotaInfo)
	}
	// create the volumeOperationDetails object for persistence
	volumeOperationDetails = createRequestDetails(
		instanceName, cnsSnapshotInfo.SourceVolumeID, cnsSnapshotInfo.SnapshotID, 0, quotaInfo,
		volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, createSnapshotsTask.Reference().Value,
		"", createSnapshotsTaskInfo.ActivationId, taskInvocationStatusSuccess, "")

	log.Infof("CreateSnapshot: Snapshot created successfully. VolumeID: %q, SnapshotID: %q, "+
		"SnapshotCreateTime: %q, opId: %q", cnsSnapshotInfo.SourceVolumeID, cnsSnapshotInfo.SnapshotID,
		cnsSnapshotInfo.SnapshotLatestOperationCompleteTime, createSnapshotsTaskInfo.ActivationId)

	return cnsSnapshotInfo, "", nil
}

// The snapshotName parameter denotes the snapshot description required by CNS CreateSnapshot API.
// This parameter is expected to be filled with the CSI CreateSnapshotRequest Name,
// which is generated by the CSI snapshotter sidecar.
func (m *defaultManager) CreateSnapshot(
	ctx context.Context, volumeID string, snapshotName string, extraParams interface{}) (*CnsSnapshotInfo, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
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
		var createSnapParams *CreateSnapshotExtraParams
		if extraParams != nil {
			var typeAssertOk bool
			createSnapParams, typeAssertOk = extraParams.(*CreateSnapshotExtraParams)
			if !typeAssertOk {
				return nil, logger.LogNewErrorf(log, "unrecognised type for CreateSnapshot params: %+v", extraParams)
			}
		}
		if createSnapParams != nil && createSnapParams.IsCSITransactionSupportEnabled {
			cnssnapshotInfo, fault, err := m.createSnapshotWithTransaction(ctx, volumeID,
				snapshotName, extraParams)
			if err != nil {
				if IsNotSupportedFaultType(ctx, fault) {
					log.Infof("Creating Snapshot with Transaction is not supported. " +
						"Re-creating Snapshot without setting Snapshot ID in the spec")
					return m.createSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, snapshotName, extraParams)
				} else {
					return nil, logger.LogNewErrorf(log, "failed to create snapshot. error :%+v", err)
				}
			}
			return cnssnapshotInfo, nil

		} else {
			return m.createSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, snapshotName, extraParams)
		}
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
	ctx context.Context, volumeID string, snapshotID string, extraParams interface{}) (*CnsSnapshotInfo, error) {
	log := logger.GetLogger(ctx)
	var (
		// Reference to the DeleteVolume task on CNS.
		deleteSnapshotTask *object.Task
		// Name of the CnsVolumeOperationRequest instance.
		instanceName = "deletesnapshot-" + volumeID + "-" + snapshotID
		// quotaInfo consists of values required to populate QuotaDetails in CnsVolumeOperationRequest CR.
		quotaInfo *cnsvolumeoperationrequest.QuotaDetails
		// Local instance of DeleteSnapshot details that needs to be persisted.
		volumeOperationDetails *cnsvolumeoperationrequest.VolumeOperationRequestDetails
		// error
		err error
		// StorageQuotaM2
		isStorageQuotaM2FSSEnabled bool
		// cnsSnapshotInfo
		cnsSnapshotInfo *CnsSnapshotInfo
	)

	if extraParams != nil {
		deleteSnapParams, ok := extraParams.(*DeletesnapshotExtraParams)
		if !ok {
			return nil, logger.LogNewErrorf(log, "unrecognised type for Deletesnapshot params: %+v", extraParams)
		}
		log.Infof("Received Deletesnapshot extraParams: %+v", *deleteSnapParams)
		isStorageQuotaM2FSSEnabled = deleteSnapParams.IsStorageQuotaM2FSSEnabled
		if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			quotaInfo = &cnsvolumeoperationrequest.QuotaDetails{
				Reserved:         deleteSnapParams.Capacity,
				StoragePolicyId:  deleteSnapParams.StoragePolicyID,
				StorageClassName: deleteSnapParams.StorageClassName,
				Namespace:        deleteSnapParams.Namespace,
			}
			log.Infof("QuotaInfo during DeleteSnapshot call: %+v", *quotaInfo)
		}
	}
	// Get the taskInfo
	var deleteSnapshotsTaskInfo *vim25types.TaskInfo
	if m.idempotencyHandlingEnabled {
		if m.operationStore == nil {
			return nil, logger.LogNewError(log, "operation store cannot be nil")
		}

		volumeOperationDetails, err = m.operationStore.GetRequestDetails(ctx, instanceName)
		switch {
		case err == nil:
			if volumeOperationDetails.OperationDetails != nil {
				// Validate if previous attempt was successful.
				if volumeOperationDetails.OperationDetails.TaskStatus == taskInvocationStatusSuccess {
					log.Infof("Snapshot id %q on Volume %q is already deleted on CNS with opId: %q.",
						snapshotID, volumeID, volumeOperationDetails.OperationDetails.OpID)
					cnsSnapshotInfo = &CnsSnapshotInfo{
						SnapshotID:     volumeOperationDetails.SnapshotID,
						SourceVolumeID: volumeOperationDetails.VolumeID,
					}
					if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
						if volumeOperationDetails.QuotaDetails != nil {
							if volumeOperationDetails.QuotaDetails.AggregatedSnapshotSize != nil {
								cnsSnapshotInfo.AggregatedSnapshotCapacityInMb =
									volumeOperationDetails.QuotaDetails.AggregatedSnapshotSize.Value()

							}
							if !volumeOperationDetails.QuotaDetails.SnapshotLatestOperationCompleteTime.IsZero() {
								cnsSnapshotInfo.SnapshotLatestOperationCompleteTime =
									volumeOperationDetails.QuotaDetails.SnapshotLatestOperationCompleteTime.Time
							}
						}
					}
					return cnsSnapshotInfo, nil
				}
				// Validate if previous operation is pending.
				if IsTaskPending(volumeOperationDetails) {
					taskMoRef := vim25types.ManagedObjectReference{
						Type:  "Task",
						Value: volumeOperationDetails.OperationDetails.TaskID,
					}
					deleteSnapshotTask = object.NewTask(m.virtualCenter.Client.Client, taskMoRef)
				}
			}
		case apierrors.IsNotFound(err):
			volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, nil, metav1.Now(),
				"", "", "", taskInvocationStatusInProgress, "")
		default:
			return nil, err
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
				if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
					//  return updated cnssnapshotinfo
					aggregatedSnapshotCapacityInMb, err := m.getAggregatedSnapshotSize(ctx, volumeID)
					if err != nil {
						return nil, logger.LogNewErrorf(log,
							"Failed to get aggregated snapshot size for volume %q with error %v", volumeID, err)
					}
					currentTime := time.Now()
					log.Infof("unable to get operation completion time for snapshot %d "+
						"will use current time %v instead", snapshotID, currentTime)
					cnsSnapshotInfo = &CnsSnapshotInfo{
						SnapshotID:                          snapshotID,
						SourceVolumeID:                      volumeID,
						AggregatedSnapshotCapacityInMb:      aggregatedSnapshotCapacityInMb,
						SnapshotLatestOperationCompleteTime: currentTime,
					}
					aggregatedSnapshotCapacity := resource.NewQuantity(aggregatedSnapshotCapacityInMb*MbInBytes, resource.BinarySI)
					quotaInfo.AggregatedSnapshotSize = aggregatedSnapshotCapacity
					quotaInfo.SnapshotLatestOperationCompleteTime.Time = currentTime
				}

				if m.idempotencyHandlingEnabled {
					volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, quotaInfo,
						metav1.Now(), "", "", "", taskInvocationStatusSuccess, "")
					if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
						log.Warnf("failed to store DeleteSnapshot operation details with error: %v", err)
					}
				}
				return cnsSnapshotInfo, nil
			}

			if m.idempotencyHandlingEnabled {
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0,
					nil, metav1.Now(), "", "", "", taskInvocationStatusError,
					err.Error())
				if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
					log.Warnf("failed to store DeleteSnapshot operation details with error: %v", err)
				}
			}
			return nil, logger.LogNewErrorf(log, "CNS DeleteSnapshot failed from the vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
		}
		if m.idempotencyHandlingEnabled {
			volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, nil,
				metav1.Now(), deleteSnapshotTask.Reference().Value, "", "", taskInvocationStatusInProgress, "")
			if err := m.operationStore.StoreRequestDetails(ctx, volumeOperationDetails); err != nil {
				log.Warnf("failed to store DeleteSnapshot operation details with error: %v", err)
			}
		}
	}

	deleteSnapshotsTaskInfo, err = m.waitOnTask(ctx, deleteSnapshotTask.Reference())
	if err != nil {
		if cnsvsphere.IsManagedObjectNotFound(err, deleteSnapshotTask.Reference()) {
			log.Infof("Snapshot %q on volume %q might have already been deleted "+
				"with the error %v. Calling CNS QuerySnapshots API to confirm it", snapshotID, volumeID, err)
			if validateSnapshotDeleted(ctx, m, volumeID, snapshotID) {
				if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
					log.Infof("get aggregated Snapshot Capacity for volume with volumeID %q", volumeID)
					aggregatedSnapshotCapacityInMb, err := m.getAggregatedSnapshotSize(ctx, volumeID)
					if err != nil {
						return nil, logger.LogNewErrorf(log, "Failed to get aggregated snapshot size for volume %q with"+
							" error %v", volumeID, err)
					}
					currentTime := time.Now()
					log.Infof("unable to get operation completion time for snapshot %d "+
						"will use current time %v instead", snapshotID, currentTime)
					cnsSnapshotInfo = &CnsSnapshotInfo{
						SnapshotID:                          snapshotID,
						SourceVolumeID:                      volumeID,
						AggregatedSnapshotCapacityInMb:      aggregatedSnapshotCapacityInMb,
						SnapshotLatestOperationCompleteTime: currentTime,
					}
					log.Infof("Fetched aggregated Snapshot Capacity is %d for volume with volumeID %q",
						aggregatedSnapshotCapacityInMb, volumeID)
					aggregatedSnapshotCapacity := resource.NewQuantity(aggregatedSnapshotCapacityInMb*MbInBytes, resource.BinarySI)
					quotaInfo.AggregatedSnapshotSize = aggregatedSnapshotCapacity
					quotaInfo.SnapshotLatestOperationCompleteTime.Time = currentTime
					log.Infof("Snapshot %q for volume %q confirmed to be deleted, update quotainfo: %+v",
						snapshotID, volumeID, *quotaInfo)
				}
				if m.idempotencyHandlingEnabled {
					// Create the volumeOperationDetails object for persistence
					volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, quotaInfo,
						volumeOperationDetails.OperationDetails.TaskInvocationTimestamp,
						deleteSnapshotTask.Reference().Value, "", volumeOperationDetails.OperationDetails.OpID,
						taskInvocationStatusSuccess, "")
				}
				log.Infof("DeleteSnapshot: Snapshot %q on volume %q is confirmed to be deleted successfully",
					snapshotID, volumeID)
				return cnsSnapshotInfo, nil
			}
		}

		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, nil,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, deleteSnapshotTask.Reference().Value, "",
			volumeOperationDetails.OperationDetails.TaskID, taskInvocationStatusError, err.Error())

		return nil, logger.LogNewErrorf(log, "Failed to get taskInfo for DeleteSnapshots task from vCenter %q with err: %v",
			m.virtualCenter.Config.Host, err)
	}

	log.Infof("DeleteSnapshot: VolumeID: %q, SnapshotID: %q, opId: %q", volumeID, snapshotID,
		deleteSnapshotsTaskInfo.ActivationId)

	// Get the taskResult
	deleteSnapshotsTaskResult, err := getTaskResultFromTaskInfo(ctx, deleteSnapshotsTaskInfo)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to get the task result for DeleteSnapshots task "+
			"from vCenter %q. taskID: %q, opId: %q createResults: %+v", m.virtualCenter.Config.Host,
			deleteSnapshotsTaskInfo.Task.Value, deleteSnapshotsTaskInfo.ActivationId, deleteSnapshotsTaskResult)
	}
	if deleteSnapshotsTaskResult == nil {
		return nil, logger.LogNewErrorf(log, "task result is empty for DeleteSnapshot task: %q, opID: %q",
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
				volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, nil,
					volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, deleteSnapshotTask.Reference().Value,
					"", deleteSnapshotsTaskInfo.ActivationId, taskInvocationStatusError, errMsg)
			}

			return nil, logger.LogNewError(log, errMsg)
		}
	}
	if isStorageQuotaM2FSSEnabled && m.clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		snapshotDeleteResult := interface{}(deleteSnapshotsTaskResult).(*cnstypes.CnsSnapshotDeleteResult)
		cnsSnapshotInfo = &CnsSnapshotInfo{
			SnapshotID:                          snapshotDeleteResult.SnapshotId.Id,
			SourceVolumeID:                      snapshotDeleteResult.VolumeId.Id,
			SnapshotLatestOperationCompleteTime: *deleteSnapshotsTaskInfo.CompleteTime,
			AggregatedSnapshotCapacityInMb:      snapshotDeleteResult.AggregatedSnapshotCapacityInMb,
		}
		aggregatedSnapshotCapacity := resource.NewQuantity(snapshotDeleteResult.AggregatedSnapshotCapacityInMb*MbInBytes,
			resource.BinarySI)
		quotaInfo.AggregatedSnapshotSize = aggregatedSnapshotCapacity
		quotaInfo.SnapshotLatestOperationCompleteTime.Time = *deleteSnapshotsTaskInfo.CompleteTime
		log.Infof("Snapshot %q for volume %q is deleted successfully, update quotainfo: %+v",
			snapshotID, volumeID, *quotaInfo)
	}
	if m.idempotencyHandlingEnabled {
		// create the volumeOperationDetails object for persistence
		volumeOperationDetails = createRequestDetails(instanceName, "", "", 0, quotaInfo,
			volumeOperationDetails.OperationDetails.TaskInvocationTimestamp, deleteSnapshotTask.Reference().Value, "",
			deleteSnapshotsTaskInfo.ActivationId, taskInvocationStatusSuccess, "")
	}
	log.Infof("DeleteSnapshot: Snapshot %q on volume %q is deleted successfully. opId: %q", snapshotID,
		volumeID, deleteSnapshotsTaskInfo.ActivationId)

	return cnsSnapshotInfo, nil
}

func (m *defaultManager) DeleteSnapshot(ctx context.Context, volumeID string, snapshotID string,
	extraParams interface{}) (*CnsSnapshotInfo, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
	internalDeleteSnapshot := func() (*CnsSnapshotInfo, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		// Set up the VC connection
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}

		return m.deleteSnapshotWithImprovedIdempotencyCheck(ctx, volumeID, snapshotID, extraParams)
	}

	start := time.Now()
	cnsSnapshotInfo, err := internalDeleteSnapshot()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteSnapshotOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteSnapshotOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return cnsSnapshotInfo, err
}

// ProtectVolumeFromVMDeletion helps set keepAfterDeleteVm control flag for given volumeID
func (m *defaultManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)
	err := validateManager(ctx, m)
	if err != nil {
		log.Errorf("failed to validate volume manager with err: %+v", err)
		return err
	}
	isvSphere80U3orAbove, err := cnsvsphere.IsvSphereVersion80U3orAbove(ctx, m.virtualCenter.Client.ServiceContent.About)
	if err != nil {
		return logger.LogNewErrorf(log,
			"Error while checking the vSphere Version %q Err= %+v",
			m.virtualCenter.Client.ServiceContent.About.Version, err)
	}
	if !isvSphere80U3orAbove {
		log.Info("Set keepAfterDeleteVm control flag using Vslm APIs")
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
	} else {
		// Re-Register volume again to set the control flag
		// This code flow does not call Vslm API to set the control flag
		querySelection := cnstypes.CnsQuerySelection{
			Names: []string{
				string(cnstypes.QuerySelectionNameTypeVolumeType),
				string(cnstypes.QuerySelectionNameTypeVolumeName),
				"VOLUME_METADATA",
			},
		}
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
		}
		queryResult, err := m.QueryVolumeAsync(ctx, queryFilter, &querySelection)
		if err != nil {
			log.Errorf("failed to set control flag keepAfterDeleteVm for "+
				"volumeID %q QueryVolumeAsync failed with err: %v.", volumeID, err)
			return err
		}
		if len(queryResult.Volumes) == 0 {
			return logger.LogNewErrorf(log, "failed to set control flag keepAfterDeleteVm "+
				"for volumeID %q QueryVolumeAsync did not return volume.", volumeID)
		}
		createSpec := &cnstypes.CnsVolumeCreateSpec{
			Name:       queryResult.Volumes[0].Name,
			VolumeType: queryResult.Volumes[0].VolumeType,
			Metadata: cnstypes.CnsVolumeMetadata{
				ContainerCluster:      queryResult.Volumes[0].Metadata.ContainerCluster,
				ContainerClusterArray: queryResult.Volumes[0].Metadata.ContainerClusterArray,
			},
		}
		createSpec.BackingObjectDetails = &cnstypes.CnsBlockBackingDetails{BackingDiskId: volumeID}
		_, _, err = m.createVolume(ctx, createSpec)
		// Note: m.createVolume handles CnsAlreadyRegisteredFault and does not return error for that case
		// For other failure we will mark this Operation as failed.
		if err != nil {
			log.Errorf("failed to register volume %q with createSpec: %v. error: %+v",
				volumeID, createSpec, err)
			return err
		}
		log.Infof("Successfully re-registered volume to set control flag to " +
			"protect volume from vm deletion")
	}
	return nil
}

// GetAllManagerInstances returns all Manager instances
func GetAllManagerInstances(ctx context.Context) map[string]*defaultManager {
	newManagerInstanceMap := make(map[string]*defaultManager)
	if len(managerInstanceMap) != 0 {
		newManagerInstanceMap = managerInstanceMap
	} else if managerInstance != nil {
		newManagerInstanceMap[managerInstance.virtualCenter.Config.Host] = managerInstance
	}
	return newManagerInstanceMap
}

func (m *defaultManager) getAggregatedSnapshotSize(ctx context.Context, volumeID string) (int64, error) {
	log := logger.GetLogger(ctx)
	var aggregatedSnapshotCapacity int64
	log.Infof("getAggregatedSnapshotSize: Query Volume to fetch aggregatedsnapshotsize for volumeID %q", volumeID)
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
	}
	queryResult, err := m.QueryVolume(ctx, queryFilter)
	if err != nil {
		log.Errorf("QueryVolume failed for volumeID %q with err: %v.", volumeID, err)
		return aggregatedSnapshotCapacity, err
	}
	if len(queryResult.Volumes) == 0 {
		return aggregatedSnapshotCapacity,
			logger.LogNewErrorf(log, "for volumeID %q QueryVolume did not return volume.", volumeID)
	}
	if queryResult != nil && len(queryResult.Volumes) > 0 {
		val, ok := queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
		if ok {
			log.Infof("getAggregatedSnapshotSize: received aggregatedsnapshotsize %d for volumeID %q",
				val.AggregatedSnapshotCapacityInMb, volumeID)
			aggregatedSnapshotCapacity = val.AggregatedSnapshotCapacityInMb
		} else {
			return aggregatedSnapshotCapacity,
				logger.LogNewErrorf(log, "Unable to retrieve CnsBlockBackingDetails for volumeID %q", volumeID)
		}
	}
	return aggregatedSnapshotCapacity, nil
}

// compileBatchAttachTaskResult consolidates Batch AttachVolume API's task result.
func compileBatchAttachTaskResult(ctx context.Context, result cnstypes.BaseCnsVolumeOperationResult,
	vm *cnsvsphere.VirtualMachine, activationId string) (BatchAttachResult, error) {
	log := logger.GetLogger(ctx)
	volumeOperationResult := result.GetCnsVolumeOperationResult()
	volumeId := volumeOperationResult.VolumeId.Id
	if volumeId == "" {
		return BatchAttachResult{},
			logger.LogNewErrorf(log,
				"volumeID is empty for BatchAttachVolume operation result. OpId: %q", activationId)
	}

	// Add volumeID to the result.
	batchAttachResult := BatchAttachResult{VolumeID: volumeId}

	fault := volumeOperationResult.Fault
	if fault != nil {
		// In case of failure, set faultType and error.
		faultType := ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationResult)
		batchAttachResult.FaultType = faultType
		msg := fmt.Sprintf("failed to batch attach cns volume: %q to node vm: %q. fault: %q. opId: %q",
			volumeId, vm.String(), faultType, activationId)
		batchAttachResult.Error = errors.New(msg)
		log.Infof("Constructed batch attach result for volume %s with failure", volumeId)
		return batchAttachResult, nil
	}

	attachResult, ok := result.(*cnstypes.CnsVolumeAttachResult)
	if !ok {
		return batchAttachResult, logger.LogNewErrorf(log, "type assertion failed: "+
			"expected *cnstypes.CnsVolumeAttachResult, got %T", result)
	}
	// In case of success, set DiskUUID, and set error as nil.
	diskUUID := attachResult.DiskUUID
	batchAttachResult.DiskUUID = diskUUID
	batchAttachResult.Error = nil

	log.Infof("Constructed batch attach result for volume %s with success", volumeId)
	return batchAttachResult, nil
}

// constructBatchAttachSpecList constructs the CnsVolumeAttachDetachSpec list by
// going through all volumes batchAttachRequest.
func constructBatchAttachSpecList(ctx context.Context, vm *cnsvsphere.VirtualMachine,
	batchAttachRequest []BatchAttachRequest) ([]cnstypes.CnsVolumeAttachDetachSpec, error) {
	log := logger.GetLogger(ctx)
	var cnsAttachSpecList []cnstypes.CnsVolumeAttachDetachSpec
	for _, volume := range batchAttachRequest {
		// Initialise cnsAttachDetachSpec
		cnsAttachDetachSpec := cnstypes.CnsVolumeAttachDetachSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volume.VolumeID,
			},
			Vm:       vm.Reference(),
			Sharing:  volume.SharingMode,
			DiskMode: volume.DiskMode,
		}

		// Set controllerKey and unitNumber only if they are provided by the user.
		if volume.ControllerKey != "" {
			// Convert to int64
			controllerKey, err := strconv.ParseInt(volume.ControllerKey, 10, 64)
			if err != nil {
				log.Errorf("failed to convert controllerKey %s to integer", controllerKey)
				return cnsAttachSpecList, err
			}
			cnsAttachDetachSpec.ControllerKey = controllerKey
		}
		if volume.UnitNumber != "" {
			// Convert to int64
			unitNumber, err := strconv.ParseInt(volume.UnitNumber, 10, 64)
			if err != nil {
				log.Errorf("failed to convert unitNumber %s to integer", unitNumber)
				return cnsAttachSpecList, err
			}
			cnsAttachDetachSpec.UnitNumber = unitNumber
		}
		cnsAttachSpecList = append(cnsAttachSpecList, cnsAttachDetachSpec)
	}

	log.Infof("Constucted CnsVolumeAttachDetachSpec for VM %s", vm.UUID)
	return cnsAttachSpecList, nil

}

// BatchAttachVolumes calls CNS Attach Volume API with a list of volumes for a single VM.
func (m *defaultManager) BatchAttachVolumes(ctx context.Context,
	vm *cnsvsphere.VirtualMachine,
	batchAttachRequest []BatchAttachRequest) ([]BatchAttachResult, string, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
	internalBatchAttachVolumes := func() ([]BatchAttachResult, string, error) {
		log := logger.GetLogger(ctx)
		var faultType string
		batchAttachResult := make([]BatchAttachResult, 0)

		log.Infof("Starting batch attach for VM %s", vm.UUID)

		err := validateManager(ctx, m)
		if err != nil {
			log.Errorf("failed to validate manger. Err: %s", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return batchAttachResult, faultType, err
		}
		// Set up the VC connection.
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %s", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return batchAttachResult, faultType, err
		}

		// Construct the CNS AttachSpec list.
		cnsAttachSpecList, err := constructBatchAttachSpecList(ctx, vm, batchAttachRequest)
		if err != nil {
			log.Errorf("constructBatchAttachSpecList failed with err: %v", err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return batchAttachResult, faultType, err
		}

		// Call the CNS AttachVolume.
		task, err := m.virtualCenter.CnsClient.AttachVolume(ctx, cnsAttachSpecList)
		if err != nil {
			log.Errorf("CNS AttachVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			faultType = ExtractFaultTypeFromErr(ctx, err)
			return batchAttachResult, faultType, err
		}

		// Get the taskInfo.
		taskInfo, err := m.waitOnTask(ctx, task.Reference())
		if err != nil || taskInfo == nil {
			log.Errorf("failed to wait for task completion. Err: %s", err)
			if err != nil {
				faultType = ExtractFaultTypeFromErr(ctx, err)
			} else {
				faultType = csifault.CSITaskInfoEmptyFault
			}
			return batchAttachResult, faultType, err
		}

		taskResults, err := cns.GetTaskResultArray(ctx, taskInfo)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			log.Errorf("unable to find BatchAttachVolumes result from vCenter %q with taskID %s and attachResults %+v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResults)
			return batchAttachResult, faultType, err
		}

		if taskResults == nil {
			return batchAttachResult, csifault.CSITaskResultEmptyFault,
				logger.LogNewErrorf(log, "taskResult is empty for BatchAttachVolumes task: %q, opId: %q",
					taskInfo.Task.Value, taskInfo.ActivationId)
		}

		volumesThatFailedToAttach := make([]string, 0)
		for _, result := range taskResults {
			currentBatchAttachResult, err := compileBatchAttachTaskResult(ctx, result, vm, taskInfo.ActivationId)
			if err != nil {
				log.Errorf("failed to compile task results. Err: %s", err)
				return []BatchAttachResult{}, csifault.CSIInternalFault, err
			}
			if currentBatchAttachResult.Error != nil {
				// If this volume could not get attached, add it to volumesThatFailedToAttach list.
				volumesThatFailedToAttach = append(volumesThatFailedToAttach, currentBatchAttachResult.VolumeID)
			}
			// Add result for each volume to batchAttachResult
			batchAttachResult = append(batchAttachResult, currentBatchAttachResult)
		}

		var overallError error
		var errMsg string
		if len(volumesThatFailedToAttach) != 0 {
			errMsg = strings.Join(volumesThatFailedToAttach, ",")
			overallError = errors.New("failed to attach volumes: " + errMsg)
			return batchAttachResult, csifault.CSIBatchAttachFault, overallError
		}
		log.Infof("BatchAttach: all volumes attached successfully with opID %s", taskInfo.ActivationId)
		return batchAttachResult, "", nil
	}

	log := logger.GetLogger(ctx)
	start := time.Now()
	batchAttachResult, faultType, err := internalBatchAttachVolumes()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsBatchAttachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
		log.Errorf("CNS BatchAttachVolumes failed with err: %s", err)
		return batchAttachResult, faultType, err
	}

	prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsBatchAttachVolumeOpType,
		prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())

	return batchAttachResult, faultType, err
}

// SyncVolume creates a new volume given its spec.
func (m *defaultManager) SyncVolume(ctx context.Context, syncVolumeSpecs []cnstypes.CnsSyncVolumeSpec) (string, error) {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
	internalSyncVolumeInfo := func() (string, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		var faultType string
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
		// Call the CNS QueryVolumeInfo.
		syncVolumeInfoTask, err := m.virtualCenter.CnsClient.SyncVolume(ctx, syncVolumeSpecs)
		if err != nil {
			faultType = ExtractFaultTypeFromErr(ctx, err)
			log.Errorf("CNS SyncVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return faultType, err
		}

		// Get the taskInfo.
		var taskInfo *vim25types.TaskInfo
		taskInfo, err = m.waitOnTask(ctx, syncVolumeInfoTask.Reference())
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get SyncVolume taskInfo from vCenter %q with err: %v",
				m.virtualCenter.Config.Host, err)
			if err != nil {
				faultType = ExtractFaultTypeFromErr(ctx, err)
			} else {
				faultType = csifault.CSITaskInfoEmptyFault
			}
			return faultType, err
		}
		log.Infof("SyncVolume: CnsSyncVolumeSpecs: %v, opId: %q", syncVolumeSpecs, taskInfo.ActivationId)
		if taskInfo.State != vim25types.TaskInfoStateSuccess {
			log.Errorf("Failed to sync volume. Error: %+v \n", taskInfo.Error)
			err = errors.New(taskInfo.Error.LocalizedMessage)
			return csifault.CSIInternalFault, err
		}
		return "", nil
	}
	start := time.Now()
	faultType, err := internalSyncVolumeInfo()
	log := logger.GetLogger(ctx)
	log.Debugf("internalSyncVolumeInfo: returns fault %q for CnsSyncVolumeSpecs %v", faultType, syncVolumeSpecs)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryVolumeInfoOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsQueryVolumeInfoOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return faultType, err
}

func (m *defaultManager) IsListViewReady() bool {
	if m.listViewIf == nil {
		return false
	}
	return m.listViewIf.IsListViewReady()
}

func (m *defaultManager) SetListViewNotReady(ctx context.Context) {
	if m.listViewIf != nil {
		m.listViewIf.SetListViewNotReady(ctx)
	}
}

// UnregisterVolume unregisters a volume from CNS.
// If unregisterDisk is true, it will also unregister the disk from FCD.
func (m *defaultManager) UnregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) error {
	ctx, cancelFunc := ensureOperationContextHasATimeout(ctx)
	defer cancelFunc()
	start := time.Now()
	err := m.unregisterVolume(ctx, volumeID, unregisterDisk)
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusUnregisterVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
		return err
	}

	prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusUnregisterVolumeOpType,
		prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	return nil
}

func (m *defaultManager) unregisterVolume(ctx context.Context, volumeID string, unregisterDisk bool) error {
	log := logger.GetLogger(ctx)

	if m.virtualCenter == nil {
		return errors.New("invalid manager instance")
	}

	err := m.virtualCenter.ConnectCns(ctx)
	if err != nil {
		return errors.New("connecting to CNS failed")
	}

	targetVolumeType := "FCD"
	if unregisterDisk {
		targetVolumeType = "LEGACY_DISK"
	}
	spec := []cnstypes.CnsUnregisterVolumeSpec{
		{
			VolumeId:         cnstypes.CnsVolumeId{Id: volumeID},
			TargetVolumeType: targetVolumeType,
		},
	}
	task, err := m.virtualCenter.CnsClient.UnregisterVolume(ctx, spec)
	if err != nil {
		msg := "failed to invoke UnregisterVolume API"
		log.Errorf("%s from vCenter %q with err: %v", msg, m.virtualCenter.Config.Host, err)
		return errors.New(msg)
	}

	taskInfo, err := m.waitOnTask(ctx, task.Reference())
	if err != nil {
		msg := "failed to get UnregisterVolume taskInfo"
		log.Errorf("%s from vCenter %q with err: %v",
			msg, m.virtualCenter.Config.Host, err)
		return errors.New(msg)
	}

	if taskInfo == nil {
		msg := "taskInfo is empty for UnregisterVolume task"
		log.Errorf("%s from vCenter %q. task: %q",
			msg, m.virtualCenter.Config.Host, task.Reference().Value)
		return errors.New(msg)
	}

	log.Infof("processing UnregisterVolume task: %q, opId: %q",
		taskInfo.Task.Value, taskInfo.ActivationId)
	res, err := getTaskResultFromTaskInfo(ctx, taskInfo)
	if err != nil {
		msg := "failed to get UnregisterVolume task result"
		log.Errorf("%s with error: %v", msg, err)
		return errors.New(msg)
	}

	if res == nil {
		msg := "task result is empty for UnregisterVolume task"
		log.Errorf("%s from vCenter %q. taskID: %q, opId: %q",
			m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId)
		return errors.New(msg)
	}

	volOpRes := res.GetCnsVolumeOperationResult()
	if volOpRes.Fault != nil {
		msg := "observed a fault in UnregisterVolume result"
		log.Errorf("%s volume: %q, fault: %q, opID: %q",
			msg, volumeID, ExtractFaultTypeFromVolumeResponseResult(ctx, volOpRes), taskInfo.ActivationId)
		return errors.New(msg)
	}

	log.Infof("volume %q unregistered successfully", volumeID)
	return nil
}
