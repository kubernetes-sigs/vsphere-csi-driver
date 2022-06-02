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
	"sync"
	"time"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"github.com/davecgh/go-spew/spew"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vslm"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

const (
	// defaultTaskCleanupIntervalInMinutes is default interval for cleaning up expired create volume tasks
	// TODO: This timeout will be configurable in future releases
	defaultTaskCleanupIntervalInMinutes = 1

	// defaultOpsExpirationTimeInHours is expiration time for create volume operations
	// TODO: This timeout will be configurable in future releases
	defaultOpsExpirationTimeInHours = 1

	// maxLengthOfVolumeNameInCNS is the maximum length of CNS volume name
	maxLengthOfVolumeNameInCNS = 80
)

// Manager provides functionality to manage volumes.
type Manager interface {
	// CreateVolume creates a new volume given its spec.
	CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec) (*CnsVolumeInfo, error)
	// AttachVolume attaches a volume to a virtual machine given the spec.
	AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) (string, error)
	// DetachVolume detaches a volume from the virtual machine given the spec.
	DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) error
	// DeleteVolume deletes a volume given its spec.
	DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) error
	// UpdateVolumeMetadata updates a volume metadata given its spec.
	UpdateVolumeMetadata(ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec) error
	// QueryVolume returns volumes matching the given filter.
	QueryVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error)
	// QueryVolumeInfo calls the CNS QueryVolumeInfo API and return a task, from which CnsQueryVolumeInfoResult is extracted
	QueryVolumeInfo(ctx context.Context, volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error)
	// QueryAllVolume returns all volumes matching the given filter and selection.
	QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error)
	// RelocateVolume migrates volumes to their target datastore as specified in relocateSpecList
	RelocateVolume(ctx context.Context, relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error)
	// ExpandVolume expands a volume to a new size.
	ExpandVolume(ctx context.Context, volumeID string, size int64) error
	// ResetManager helps set new manager instance and VC configuration
	ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter)
	// ConfigureVolumeACLs configures net permissions for a given CnsVolumeACLConfigureSpec
	ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error
	// ProtectVolumeFromVMDeletion sets keepAfterDeleteVm control flag on migrated volume
	ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error
}

// CnsVolumeInfo hold information related to volume created by CNS
type CnsVolumeInfo struct {
	DatastoreURL string
	VolumeID     cnstypes.CnsVolumeId
}

var (
	// managerInstance is a Manager singleton.
	managerInstance *defaultManager
	// managerInstanceLock is used for mitigating race condition during read/write on manager instance.
	managerInstanceLock sync.Mutex
	volumeTaskMap       = make(map[string]*createVolumeTaskDetails)
)

// createVolumeTaskDetails contains taskInfo object and expiration time
type createVolumeTaskDetails struct {
	sync.Mutex
	task           *object.Task
	expirationTime time.Time
}

// GetManager returns the Manager instance.
func GetManager(ctx context.Context, vc *cnsvsphere.VirtualCenter) Manager {
	log := logger.GetLogger(ctx)
	managerInstanceLock.Lock()
	defer managerInstanceLock.Unlock()
	if managerInstance != nil {
		log.Infof("Retrieving existing volume.defaultManager...")
		return managerInstance
	}
	log.Infof("Initializing new volume.defaultManager...")
	managerInstance = &defaultManager{
		virtualCenter: vc,
	}
	return managerInstance
}

// DefaultManager provides functionality to manage volumes.
type defaultManager struct {
	virtualCenter *cnsvsphere.VirtualCenter
}

// ClearTaskInfoObjects is a go routine which runs in the background to clean up expired taskInfo objects from volumeTaskMap
func ClearTaskInfoObjects() {
	log := logger.GetLoggerWithNoContext()
	// At a frequency of every 1 minute, check if there are expired taskInfo objects and delete them from the volumeTaskMap
	ticker := time.NewTicker(time.Duration(defaultTaskCleanupIntervalInMinutes) * time.Minute)
	for range ticker.C {
		for pvc, taskDetails := range volumeTaskMap {
			// Get the time difference between current time and the expiration time from the volumeTaskMap
			diff := time.Until(taskDetails.expirationTime)
			// Checking if the expiration time has elapsed
			if int(diff.Hours()) < 0 || int(diff.Minutes()) < 0 || int(diff.Seconds()) < 0 {
				// If one of the parameters in the time object is negative, it means the entry has to be deleted
				log.Debugf("ClearTaskInfoObjects : Found an expired taskInfo object : %+v for the VolumeName: %q. Deleting the object entry from volumeTaskMap", volumeTaskMap[pvc].task, pvc)
				taskDetails.Lock()
				delete(volumeTaskMap, pvc)
				taskDetails.Unlock()
			}
		}
	}
}

// ResetManager helps set new manager instance and VC configuration
func (m *defaultManager) ResetManager(ctx context.Context, vcenter *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	managerInstanceLock.Lock()
	defer managerInstanceLock.Unlock()
	if vcenter.Config.Host != managerInstance.virtualCenter.Config.Host {
		log.Infof("Re-initializing volume.defaultManager")
		managerInstance = &defaultManager{
			virtualCenter: vcenter,
		}
	}
	m.virtualCenter.Config = vcenter.Config
	if m.virtualCenter.Client != nil {
		m.virtualCenter.Client.Timeout = time.Duration(vcenter.Config.VCClientTimeout) * time.Minute
		log.Infof("VC client timeout is set to %v", m.virtualCenter.Client.Timeout)
	}
	log.Infof("Done resetting volume.defaultManager")
}

// CreateVolume creates a new volume given its spec.
func (m *defaultManager) CreateVolume(ctx context.Context, spec *cnstypes.CnsVolumeCreateSpec) (*CnsVolumeInfo, error) {
	internalCreateVolume := func() (*CnsVolumeInfo, error) {
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
		// If the VSphereUser in the CreateSpec is different from session user, update the CreateSpec
		s, err := m.virtualCenter.Client.SessionManager.UserSession(ctx)
		if err != nil {
			log.Errorf("failed to get usersession with err: %v", err)
			return nil, err
		}
		if s.UserName != spec.Metadata.ContainerCluster.VSphereUser {
			log.Debugf("Update VSphereUser from %s to %s", spec.Metadata.ContainerCluster.VSphereUser, s.UserName)
			spec.Metadata.ContainerCluster.VSphereUser = s.UserName
		}

		// Construct the CNS VolumeCreateSpec list
		var cnsCreateSpecList []cnstypes.CnsVolumeCreateSpec
		var task *object.Task
		var taskInfo *vim25types.TaskInfo
		// store the volume name passed in by input spec, this name may exceed 80 characters
		volNameFromInputSpec := spec.Name
		// Call the CNS CreateVolume
		taskDetailsInMap, ok := volumeTaskMap[volNameFromInputSpec]
		if ok {
			task = taskDetailsInMap.task
			log.Infof("CreateVolume task still pending for VolumeName: %q, with taskInfo: %+v", volNameFromInputSpec, task)
		} else {
			// truncate the volume name to make sure the name is within 80 characters before calling CNS
			if len(spec.Name) > maxLengthOfVolumeNameInCNS {
				volNameAfterTruncate := spec.Name[0 : maxLengthOfVolumeNameInCNS-1]
				spec.Name = volNameAfterTruncate
				log.Infof("Create Volume with name %s is too long, truncate the name to %s", volNameFromInputSpec, spec.Name)
				log.Debugf("CNS Create Volume is called with %v", spew.Sdump(*spec))
			}
			cnsCreateSpecList = append(cnsCreateSpecList, *spec)
			task, err = m.virtualCenter.CnsClient.CreateVolume(ctx, cnsCreateSpecList)
			if err != nil {
				log.Errorf("CNS CreateVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
				return nil, err
			}
			var isStaticallyProvisionedBlockVolume bool
			var isStaticallyProvisionedFileVolume bool
			if spec.VolumeType == string(cnstypes.CnsVolumeTypeBlock) {
				blockBackingDetails, ok := spec.BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails)
				if ok && (blockBackingDetails.BackingDiskId != "" || blockBackingDetails.BackingDiskUrlPath != "") {
					isStaticallyProvisionedBlockVolume = true
				}
			}
			if spec.VolumeType == string(cnstypes.CnsVolumeTypeFile) {
				fileBackingDetails, ok := spec.BackingObjectDetails.(*cnstypes.CnsVsanFileShareBackingDetails)
				if ok && fileBackingDetails.BackingFileId != "" {
					isStaticallyProvisionedFileVolume = true
				}
			}
			// Add the task details to volumeTaskMap only for dynamically provisioned volumes.
			// For static volume provisioning we need not store the taskDetails as it doesn't result in orphaned volumes
			if !isStaticallyProvisionedBlockVolume && !isStaticallyProvisionedFileVolume {
				var taskDetails createVolumeTaskDetails
				// Store the task details and task object expiration time in volumeTaskMap
				taskDetails.task = task
				taskDetails.expirationTime = time.Now().Add(time.Hour * time.Duration(defaultOpsExpirationTimeInHours))
				volumeTaskMap[volNameFromInputSpec] = &taskDetails
			}
		}
		// Get the taskInfo
		taskInfo, err = cns.GetTaskInfo(ctx, task)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for CreateVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}
		log.Infof("CreateVolume: VolumeName: %q, opId: %q", volNameFromInputSpec, taskInfo.ActivationId)
		// Get the taskResult
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)

		if err != nil {
			log.Errorf("unable to find the task result for CreateVolume task from vCenter %q. taskID: %q, opId: %q createResults: %+v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult)
			return nil, err
		}

		if taskResult == nil {
			log.Errorf("taskResult is empty for CreateVolume task: %q", taskInfo.ActivationId)
			return nil, errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			fault, ok := volumeOperationRes.Fault.Fault.(cnstypes.CnsAlreadyRegisteredFault)
			if ok {
				log.Infof("CreateVolume: Volume is already registered with CNS. VolumeName: %q, volumeID: %q, opId: %q", spec.Name, fault.VolumeId.Id, taskInfo.ActivationId)
				return &CnsVolumeInfo{
					DatastoreURL: "",
					VolumeID:     fault.VolumeId,
				}, nil
			}
			// Remove the taskInfo object associated with the volume name when the current task fails.
			//  This is needed to ensure the sub-sequent create volume call from the external provisioner invokes Create Volume
			taskDetailsInMap, ok := volumeTaskMap[volNameFromInputSpec]
			if ok {
				taskDetailsInMap.Lock()
				log.Debugf("Deleted task for %s from volumeTaskMap because the task has failed", volNameFromInputSpec)
				delete(volumeTaskMap, volNameFromInputSpec)
				taskDetailsInMap.Unlock()
			}
			msg := fmt.Sprintf("failed to create cns volume %s. createSpec: %q, fault: %q, opId: %q", volNameFromInputSpec, spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return nil, errors.New(msg)
		}
		var datastoreURL string
		volumeCreateResult := interface{}(taskResult).(*cnstypes.CnsVolumeCreateResult)
		if volumeCreateResult.PlacementResults != nil {
			var datastoreMoRef vim25types.ManagedObjectReference
			for _, placementResult := range volumeCreateResult.PlacementResults {
				// For the datastore which the volume is provisioned, placementFaults will not be set
				if len(placementResult.PlacementFaults) == 0 {
					datastoreMoRef = placementResult.Datastore
					break
				}
			}
			var dsMo mo.Datastore
			pc := property.DefaultCollector(m.virtualCenter.Client.Client)
			err := pc.RetrieveOne(ctx, datastoreMoRef, []string{"summary"}, &dsMo)
			if err != nil {
				msg := fmt.Sprintf("failed to retrieve datastore summary property: %v", err)
				log.Error(msg)
				return nil, errors.New(msg)
			}
			datastoreURL = dsMo.Summary.Url
		}

		log.Infof("CreateVolume: Volume created successfully. VolumeName: %q, opId: %q, volumeID: %q", volNameFromInputSpec, taskInfo.ActivationId, volumeOperationRes.VolumeId.Id)
		log.Debugf("CreateVolume volumeId %q is placed on datastore %q", volumeOperationRes.VolumeId.Id, datastoreURL)
		return &CnsVolumeInfo{
			DatastoreURL: datastoreURL,
			VolumeID:     volumeOperationRes.VolumeId,
		}, nil
	}
	start := time.Now()
	resp, err := internalCreateVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsCreateVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsCreateVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}

	return resp, err
}

// AttachVolume attaches a volume to a virtual machine given the spec.
func (m *defaultManager) AttachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) (string, error) {
	internalAttachVolume := func() (string, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return "", err
		}
		// Set up the VC connection
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return "", err
		}
		// Construct the CNS AttachSpec list
		var cnsAttachSpecList []cnstypes.CnsVolumeAttachDetachSpec
		cnsAttachSpec := cnstypes.CnsVolumeAttachDetachSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeID,
			},
			Vm: vm.Reference(),
		}
		cnsAttachSpecList = append(cnsAttachSpecList, cnsAttachSpec)
		// Call the CNS AttachVolume
		task, err := m.virtualCenter.CnsClient.AttachVolume(ctx, cnsAttachSpecList)
		if err != nil {
			log.Errorf("CNS AttachVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return "", err
		}
		// Get the taskInfo
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for AttachVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return "", err
		}
		log.Infof("AttachVolume: volumeID: %q, vm: %q, opId: %q", volumeID, vm.String(), taskInfo.ActivationId)
		// Get the taskResult
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find the task result for AttachVolume task from vCenter %q with taskID %s and attachResults %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return "", err
		}

		if taskResult == nil {
			log.Errorf("taskResult is empty for AttachVolume task: %q, opId: %q", taskInfo.Task.Value, taskInfo.ActivationId)
			return "", errors.New("taskResult is empty")
		}

		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			_, isResourceInUseFault := volumeOperationRes.Fault.Fault.(*vim25types.ResourceInUse)
			if isResourceInUseFault {
				log.Infof("observed ResourceInUse fault while attaching volume: %q with vm: %q", volumeID, vm.String())
				// check if volume is already attached to the requested node
				diskUUID, err := IsDiskAttached(ctx, vm, volumeID)
				if err != nil {
					return "", err
				}
				if diskUUID != "" {
					return diskUUID, nil
				}
			}
			msg := fmt.Sprintf("failed to attach cns volume: %q to node vm: %q. fault: %q. opId: %q", volumeID, vm.String(), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return "", errors.New(msg)
		}
		diskUUID := interface{}(taskResult).(*cnstypes.CnsVolumeAttachResult).DiskUUID
		log.Infof("AttachVolume: Volume attached successfully. volumeID: %q, opId: %q, vm: %q, diskUUID: %q", volumeID, taskInfo.ActivationId, vm.String(), diskUUID)
		return diskUUID, nil
	}
	start := time.Now()
	resp, err := internalAttachVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsAttachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsAttachVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// DetachVolume detaches a volume from the virtual machine given the spec.
func (m *defaultManager) DetachVolume(ctx context.Context, vm *cnsvsphere.VirtualMachine, volumeID string) error {
	internalDetachVolume := func() error {
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
		// Construct the CNS DetachSpec list
		var cnsDetachSpecList []cnstypes.CnsVolumeAttachDetachSpec
		cnsDetachSpec := cnstypes.CnsVolumeAttachDetachSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeID,
			},
			Vm: vm.Reference(),
		}
		cnsDetachSpecList = append(cnsDetachSpecList, cnsDetachSpec)
		// Call the CNS DetachVolume
		task, err := m.virtualCenter.CnsClient.DetachVolume(ctx, cnsDetachSpecList)
		if err != nil {
			if cnsvsphere.IsManagedObjectNotFound(err, cnsDetachSpec.Vm) {
				// Detach failed with managed object not found, marking detach as successful, as Node VM is deleted and not present in the vCenter inventory
				log.Infof("Node VM: %v not found on the vCenter. Marking Detach for volume:%q successful. err: %v", vm, volumeID, err)
				return nil
			}
			if cnsvsphere.IsNotFoundError(err) {
				// Detach failed with NotFound error, check if the volume is already detached
				log.Infof("VolumeID: %q, not found. Checking whether the volume is already detached", volumeID)
				diskUUID, err := IsDiskAttached(ctx, vm, volumeID)
				if err != nil {
					log.Errorf("DetachVolume: CNS Detach has failed with err: %+v. Unable to check if volume: %q is already detached from vm: %+v",
						err, volumeID, vm)
					return err
				}
				if diskUUID == "" {
					log.Infof("DetachVolume: volumeID: %q not found on vm: %+v. Assuming volume is already detached", volumeID, vm)
					return nil
				}
			}
			msg := fmt.Sprintf("failed to detach cns volume:%q from node vm: %+v. err: %v", volumeID, vm, err)
			log.Error(msg)
			return errors.New(msg)
		}
		// Get the taskInfo
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for DetachVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		log.Infof("DetachVolume: volumeID: %q, vm: %q, opId: %q", volumeID, vm.String(), taskInfo.ActivationId)
		// Get the task results for the given task
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find the task result for DetachVolume task from vCenter %q with taskID %s and detachResults %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return err
		}
		if taskResult == nil {
			log.Errorf("taskResult is empty for DetachVolume task: %q, opId: %q", taskInfo.Task.Value, taskInfo.ActivationId)
			return errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			_, isNotFoundFault := volumeOperationRes.Fault.Fault.(*vim25types.NotFound)
			if isNotFoundFault {
				// check if volume is already detached from the VM
				diskUUID, err := IsDiskAttached(ctx, vm, volumeID)
				if err != nil {
					log.Errorf("DetachVolume: CNS Detach has failed with fault: %+v. Unable to check if volume: %q is already detached from vm: %+v",
						spew.Sdump(volumeOperationRes.Fault), volumeID, vm)
					return err
				}
				if diskUUID == "" {
					log.Infof("DetachVolume: volumeID: %q not found on vm: %+v. Assuming volume is already detached", volumeID, vm)
					return nil
				}
			}
			msg := fmt.Sprintf("failed to detach cns volume:%q from node vm: %+v. fault: %+v, opId: %q", volumeID, vm, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return errors.New(msg)
		}
		log.Infof("DetachVolume: Volume detached successfully. volumeID: %q, vm: %q, opId: %q", volumeID, taskInfo.ActivationId, vm.String())
		return nil
	}
	start := time.Now()
	err := internalDetachVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDetachVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDetachVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// DeleteVolume deletes a volume given its spec.
func (m *defaultManager) DeleteVolume(ctx context.Context, volumeID string, deleteDisk bool) error {
	internalDeleteVolume := func() error {
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
		// Construct the CNS VolumeId list
		var cnsVolumeIDList []cnstypes.CnsVolumeId
		cnsVolumeID := cnstypes.CnsVolumeId{
			Id: volumeID,
		}
		// Call the CNS DeleteVolume
		cnsVolumeIDList = append(cnsVolumeIDList, cnsVolumeID)
		task, err := m.virtualCenter.CnsClient.DeleteVolume(ctx, cnsVolumeIDList, deleteDisk)
		if err != nil {
			if cnsvsphere.IsNotFoundError(err) {
				log.Infof("VolumeID: %q, not found. Returning success for this operation since the volume is not present", volumeID)
				return nil
			}
			log.Errorf("CNS DeleteVolume failed from the  vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		// Get the taskInfo
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for DeleteVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		log.Infof("DeleteVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
		// Get the task results for the given task
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find the task result for DeleteVolume task from vCenter %q with taskID %s and deleteResults %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return err
		}
		if taskResult == nil {
			log.Errorf("taskResult is empty for DeleteVolume task: %q, opID: %q", taskInfo.Task.Value, taskInfo.ActivationId)
			return errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			msg := fmt.Sprintf("failed to delete volume: %q, fault: %q, opID: %q", volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return errors.New(msg)
		}
		log.Infof("DeleteVolume: Volume deleted successfully. volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
		return nil
	}
	start := time.Now()
	err := internalDeleteVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsDeleteVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// UpdateVolume updates a volume given its spec.
func (m *defaultManager) UpdateVolumeMetadata(ctx context.Context, spec *cnstypes.CnsVolumeMetadataUpdateSpec) error {
	internalUpdateVolumeMetadata := func() error {
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
		// If the VSphereUser in the VolumeMetadataUpdateSpec is different from session user, update the VolumeMetadataUpdateSpec
		s, err := m.virtualCenter.Client.SessionManager.UserSession(ctx)
		if err != nil {
			log.Errorf("failed to get usersession with err: %v", err)
			return err
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
		// Get the taskInfo
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for UpdateVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		log.Infof("UpdateVolumeMetadata: volumeID: %q, opId: %q", spec.VolumeId.Id, taskInfo.ActivationId)
		// Get the task results for the given task
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find the task result for UpdateVolume task from vCenter %q with taskID %q, opId: %q and updateResults %+v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult)
			return err
		}
		if taskResult == nil {
			log.Errorf("taskResult is empty for UpdateVolume task: %q, opId: %q", taskInfo.Task.Value, taskInfo.ActivationId)
			return errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			msg := fmt.Sprintf("failed to update volume. updateSpec: %q, fault: %q, opID: %q", spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return errors.New(msg)
		}
		log.Infof("UpdateVolumeMetadata: Volume metadata updated successfully. volumeID: %q, opId: %q", spec.VolumeId.Id, taskInfo.ActivationId)
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
func (m *defaultManager) ExpandVolume(ctx context.Context, volumeID string, size int64) error {
	internalExpandVolume := func() error {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			log.Errorf("validateManager failed with err: %+v", err)
			return err
		}
		// Set up the VC connection
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return err
		}
		// Construct the CNS ExtendSpec list
		var cnsExtendSpecList []cnstypes.CnsVolumeExtendSpec
		cnsExtendSpec := cnstypes.CnsVolumeExtendSpec{
			VolumeId: cnstypes.CnsVolumeId{
				Id: volumeID,
			},
			CapacityInMb: size,
		}
		cnsExtendSpecList = append(cnsExtendSpecList, cnsExtendSpec)
		// Call the CNS ExtendVolume
		log.Infof("Calling CnsClient.ExtendVolume: VolumeID [%q] Size [%d] cnsExtendSpecList [%#v]", volumeID, size, cnsExtendSpecList)
		task, err := m.virtualCenter.CnsClient.ExtendVolume(ctx, cnsExtendSpecList)
		if err != nil {
			if cnsvsphere.IsNotFoundError(err) {
				log.Errorf("VolumeID: %q, not found. Cannot expand volume.", volumeID)
				return errors.New("volume not found")
			}
			log.Errorf("CNS ExtendVolume failed from the vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		// Get the taskInfo
		taskInfo, err := cns.GetTaskInfo(ctx, task)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for ExtendVolume task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		log.Infof("ExpandVolume: volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
		// Get the task results for the given task
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("Unable to find the task result for ExtendVolume task from vCenter %q with taskID %s and extend volume Results %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return err
		}
		if taskResult == nil {
			log.Errorf("TaskResult is empty for ExtendVolume task: %q, opID: %q", taskInfo.Task.Value, taskInfo.ActivationId)
			return errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			msg := fmt.Sprintf("failed to extend volume: %q, fault: %q, opID: %q", volumeID, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return errors.New(msg)
		}
		log.Infof("ExpandVolume: Volume expanded successfully. volumeID: %q, opId: %q", volumeID, taskInfo.ActivationId)
		return nil
	}
	start := time.Now()
	err := internalExpandVolume()
	if err != nil {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsExpandVolumeOpType,
			prometheus.PrometheusFailStatus).Observe(time.Since(start).Seconds())
	} else {
		prometheus.CnsControlOpsHistVec.WithLabelValues(prometheus.PrometheusCnsExpandVolumeOpType,
			prometheus.PrometheusPassStatus).Observe(time.Since(start).Seconds())
	}
	return err
}

// QueryVolume returns volumes matching the given filter.
func (m *defaultManager) QueryVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	internalQueryVolume := func() (*cnstypes.CnsQueryResult, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Set up the VC connection
		err = m.virtualCenter.ConnectCns(ctx)
		if err != nil {
			log.Errorf("ConnectCns failed with err: %+v", err)
			return nil, err
		}
		//Call the CNS QueryVolume
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
func (m *defaultManager) QueryAllVolume(ctx context.Context, queryFilter cnstypes.CnsQueryFilter, querySelection cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	internalQueryAllVolume := func() (*cnstypes.CnsQueryResult, error) {
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
		//Call the CNS QueryAllVolume
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

// QueryVolumeInfo calls the CNS QueryVolumeInfo API and return a task, from which CnsQueryVolumeInfoResult is extracted
func (m *defaultManager) QueryVolumeInfo(ctx context.Context, volumeIDList []cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	internalQueryVolumeInfo := func() (*cnstypes.CnsQueryVolumeInfoResult, error) {
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
		//Call the CNS QueryVolumeInfo
		queryVolumeInfoTask, err := m.virtualCenter.CnsClient.QueryVolumeInfo(ctx, volumeIDList)
		if err != nil {
			log.Errorf("CNS QueryAllVolume failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}

		// Get the taskInfo
		taskInfo, err := cns.GetTaskInfo(ctx, queryVolumeInfoTask)
		if err != nil || taskInfo == nil {
			log.Errorf("failed to get taskInfo for QueryVolumeInfo task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return nil, err
		}
		log.Infof("QueryVolumeInfo: volumeIDList: %v, opId: %q", volumeIDList, taskInfo.ActivationId)
		// Get the task results for the given task
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find the task result for QueryVolumeInfo task from vCenter %q with taskID %s and taskResult %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskResult)
			return nil, err
		}
		if taskResult == nil {
			log.Errorf("taskResult is empty for DeleteVolume task: %q, opID: %q", taskInfo.Task.Value, taskInfo.ActivationId)
			return nil, errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			msg := fmt.Sprintf("failed to Query volumes: %v, fault: %q, opID: %q", volumeIDList, spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return nil, errors.New(msg)
		}
		volumeInfoResult := interface{}(taskResult).(*cnstypes.CnsQueryVolumeInfoResult)
		log.Infof("QueryVolumeInfo successfully returned volumeInfo volumeIDList %v:, opId: %q", volumeIDList, taskInfo.ActivationId)
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

func (m *defaultManager) RelocateVolume(ctx context.Context, relocateSpecList ...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	internalRelocateVolume := func() (*object.Task, error) {
		log := logger.GetLogger(ctx)
		err := validateManager(ctx, m)
		if err != nil {
			log.Errorf("validateManager failed with err: %+v", err)
			return nil, err
		}

		// Set up the VC connection
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

// ConfigureVolumeACLs configures net permissions for a given CnsVolumeACLConfigureSpec
func (m *defaultManager) ConfigureVolumeACLs(ctx context.Context, spec cnstypes.CnsVolumeACLConfigureSpec) error {
	internalConfigureVolumeACLs := func() error {
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

		var task *object.Task
		var taskInfo *vim25types.TaskInfo

		task, err = m.virtualCenter.CnsClient.ConfigureVolumeACLs(ctx, spec)
		if err != nil {
			log.Errorf("CNS ConfigureVolumeACLs failed from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}

		// Get the taskInfo
		taskInfo, err = cns.GetTaskInfo(ctx, task)
		if err != nil {
			log.Errorf("failed to get taskInfo for ConfigureVolumeACLs task from vCenter %q with err: %v", m.virtualCenter.Config.Host, err)
			return err
		}
		// Get the taskResult
		taskResult, err := cns.GetTaskResult(ctx, taskInfo)
		if err != nil {
			log.Errorf("unable to find the task result for ConfigureVolumeACLs task from vCenter %q. taskID: %q, opId: %q ConfigureVolumeACL results: %+v . Error: %v",
				m.virtualCenter.Config.Host, taskInfo.Task.Value, taskInfo.ActivationId, taskResult, err)
			return err
		}

		if taskResult == nil {
			log.Errorf("taskResult is empty for ConfigureVolumeACLs task: %q. ConfigureVolumeACLsSpec: %q", taskInfo.ActivationId, spew.Sdump(spec))
			return errors.New("taskResult is empty")
		}
		volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
		if volumeOperationRes.Fault != nil {
			msg := fmt.Sprintf("failed to apply ConfigureVolumeACLs. Volume ID: %s. ConfigureVolumeACLsSpec: %q, fault: %q, opId: %q", spec.VolumeId.Id, spew.Sdump(spec), spew.Sdump(volumeOperationRes.Fault), taskInfo.ActivationId)
			log.Error(msg)
			return errors.New(msg)
		}

		log.Infof("ConfigureVolumeACLs: Volume ACLs configured successfully. VolumeName: %q, opId: %q, volumeID: %q", spec.VolumeId.Id, taskInfo.ActivationId, volumeOperationRes.VolumeId.Id)
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
