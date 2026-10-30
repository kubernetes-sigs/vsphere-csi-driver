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

package volume

import (
	"context"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// This file provides a submit/poll pair for CNS CreateVolume, for callers that
// must not hold a goroutine for the duration of the task.
//
// Manager.CreateVolume is synchronous: it submits the task and then parks the
// calling goroutine in waitOnTask until CNS responds, which for a registration
// can be minutes. A controller that wants to release its worker between the
// submit and the result needs the two halves separately, plus a way to name the
// task across reconciles. These two functions are that, and nothing else:
// neither the Manager interface nor any existing code path is affected, and the
// listview is not involved.
//
// Everything after the task completes is deliberately delegated to the same
// unexported helpers Manager.CreateVolume uses, so that the two paths cannot
// disagree about what a CNS response means. In particular
// validateCreateVolumeResponseFault maps CnsAlreadyRegisteredFault to success,
// which is what makes a re-submission safe after a lost task reference; a
// divergent second copy of that rule is how a disk ends up registered twice.

// CreateVolumeTaskState is the outcome of polling a CreateVolume task.
type CreateVolumeTaskState string

const (
	// CreateVolumeTaskRunning means the task exists on vCenter and has not
	// reached a terminal state. Poll again.
	CreateVolumeTaskRunning CreateVolumeTaskState = "Running"
	// CreateVolumeTaskSucceeded means the volume exists. This includes a task
	// that came back with CnsAlreadyRegisteredFault, which is a success for
	// registration purposes.
	CreateVolumeTaskSucceeded CreateVolumeTaskState = "Succeeded"
	// CreateVolumeTaskFailed means CNS reported a terminal fault for this task.
	// The volume was not created and the task reference is spent.
	CreateVolumeTaskFailed CreateVolumeTaskState = "Failed"
	// CreateVolumeTaskUnknown means the task's state could not be determined:
	// the vCenter session is down, the property read failed, or the task object
	// is gone from vCenter.
	//
	// This is NOT a failure and must never be treated as one. The task may well
	// be running on CNS right now. A caller that maps Unknown to Failed and
	// resubmits would issue a second CreateVolume against a disk whose first
	// task is still in flight.
	CreateVolumeTaskUnknown CreateVolumeTaskState = "Unknown"
)

// SubmitCreateVolumeTask submits a CNS CreateVolume task and returns as soon as
// vCenter has accepted it, without waiting for it to run. The returned
// reference is stable and can be persisted and polled later with
// GetCreateVolumeTaskResult, including by a different process after a restart.
//
// The caller owns the task reference from here on. If it is lost before being
// persisted, the task becomes unobservable: CNS will still complete it, but
// nothing will be watching. Recovering from that means looking the volume up by
// name, which only works if the create spec used a deterministic one.
func SubmitCreateVolumeTask(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	spec *cnstypes.CnsVolumeCreateSpec) (vim25types.ManagedObjectReference, string, error) {
	log := logger.GetLogger(ctx)

	if err := validateManagerForVC(ctx, virtualCenter); err != nil {
		return vim25types.ManagedObjectReference{}, ExtractFaultTypeFromErr(ctx, err), err
	}
	if err := setupConnection(ctx, virtualCenter, spec); err != nil {
		log.Errorf("failed to setup connection to CNS with error: %v", err)
		return vim25types.ManagedObjectReference{}, ExtractFaultTypeFromErr(ctx, err), err
	}

	task, err := invokeCNSCreateVolume(ctx, virtualCenter, spec)
	if err != nil {
		return vim25types.ManagedObjectReference{}, ExtractFaultTypeFromErr(ctx, err), err
	}
	log.Infof("Submitted CNS CreateVolume task %q for volume %q without waiting",
		task.Reference().Value, spec.Name)
	return task.Reference(), "", nil
}

// GetCreateVolumeTaskResult reports the current state of a previously submitted
// CreateVolume task without blocking. It performs a single property read of the
// task's info and, when the task has finished, decodes the CNS result using the
// same helpers as the synchronous path.
//
// volumeName must be the name from the original create spec; it is used only
// for the already-registered check and for log messages.
//
// The returned CnsVolumeInfo is non-nil only for CreateVolumeTaskSucceeded.
func GetCreateVolumeTaskResult(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	taskMoRef vim25types.ManagedObjectReference, volumeName string) (
	CreateVolumeTaskState, *CnsVolumeInfo, string, error) {
	log := logger.GetLogger(ctx)

	if err := virtualCenter.Connect(ctx); err != nil {
		// Cannot reach vCenter, so nothing can be concluded about the task.
		log.Errorf("failed to connect to vCenter while polling CreateVolume task %q: %v",
			taskMoRef.Value, err)
		return CreateVolumeTaskUnknown, nil, ExtractFaultTypeFromErr(ctx, err), err
	}

	var taskObj mo.Task
	err := property.DefaultCollector(virtualCenter.Client.Client).
		RetrieveOne(ctx, taskMoRef, []string{"info"}, &taskObj)
	if err != nil {
		// Includes ManagedObjectNotFound when the task has aged out of vCenter.
		// Still Unknown rather than Failed: the task not being readable says
		// nothing about whether the volume was created.
		log.Errorf("failed to read info for CreateVolume task %q: %v", taskMoRef.Value, err)
		return CreateVolumeTaskUnknown, nil, ExtractFaultTypeFromErr(ctx, err), err
	}

	taskInfo := taskObj.Info
	switch taskInfo.State {
	case vim25types.TaskInfoStateQueued, vim25types.TaskInfoStateRunning:
		return CreateVolumeTaskRunning, nil, "", nil
	case vim25types.TaskInfoStateError:
		// The task itself reported an error, as opposed to us failing to read
		// it. This is a real terminal failure.
		var err error
		if taskInfo.Error != nil {
			err = logger.LogNewErrorf(log, "CreateVolume task %q for volume %q failed: %s",
				taskMoRef.Value, volumeName, taskInfo.Error.LocalizedMessage)
		} else {
			err = logger.LogNewErrorf(log, "CreateVolume task %q for volume %q failed with no error detail",
				taskMoRef.Value, volumeName)
		}
		return CreateVolumeTaskFailed, nil, ExtractFaultTypeFromErr(ctx, err), err
	case vim25types.TaskInfoStateSuccess:
		// Fall through to decoding the CNS result below. A vSphere-level
		// success still carries a CNS-level per-volume result that may hold a
		// fault.
	default:
		return CreateVolumeTaskUnknown, nil, "", logger.LogNewErrorf(log,
			"CreateVolume task %q is in unrecognized state %q", taskMoRef.Value, taskInfo.State)
	}

	taskResult, err := getTaskResultFromTaskInfo(ctx, &taskInfo)
	if err != nil {
		return CreateVolumeTaskFailed, nil, ExtractFaultTypeFromErr(ctx, err), err
	}
	if taskResult == nil {
		return CreateVolumeTaskFailed, nil, csifault.CSITaskResultEmptyFault, logger.LogNewErrorf(log,
			"taskResult is empty for CreateVolume task %q, opID: %q", taskMoRef.Value, taskInfo.ActivationId)
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		faultType := ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		// validateCreateVolumeResponseFault turns CnsAlreadyRegisteredFault
		// into a successful CnsVolumeInfo; any other fault comes back as an
		// error. Reusing it here is what keeps this path's notion of "already
		// done" identical to the synchronous path's.
		resp, err := validateCreateVolumeResponseFault(ctx, volumeName, volumeOperationRes)
		if err != nil {
			return CreateVolumeTaskFailed, nil, faultType, err
		}
		return CreateVolumeTaskSucceeded, resp, faultType, nil
	}

	volInfo, faultType, err := getCnsVolumeInfoFromTaskResult(ctx, virtualCenter, volumeName,
		volumeOperationRes.VolumeId, taskResult)
	if err != nil {
		return CreateVolumeTaskFailed, nil, faultType, err
	}
	return CreateVolumeTaskSucceeded, volInfo, "", nil
}

// validateManagerForVC mirrors the nil checks validateManager performs, for the
// manager-less entry points above.
func validateManagerForVC(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) error {
	log := logger.GetLogger(ctx)
	if virtualCenter == nil {
		return logger.LogNewError(log, "virtual center is not set")
	}
	if virtualCenter.Client == nil {
		return logger.LogNewError(log, "virtual center client is not set")
	}
	return nil
}

// TaskListView returns the manager's listview, initializing it on first use the
// same way waitOnTask does.
//
// This exists so a caller can register a CreateVolume task for push updates
// without parking a goroutine in waitOnTask. The listview multiplexes every
// registered task onto a single WaitForUpdates session against vCenter, so
// watching N tasks this way costs one connection and no polling; replacing it
// with per-task property reads would reintroduce exactly the load it avoids.
func TaskListView(ctx context.Context, m Manager) (ListViewIf, error) {
	log := logger.GetLogger(ctx)
	dm, ok := m.(*defaultManager)
	if !ok {
		return nil, logger.LogNewErrorf(log, "volume manager of type %T does not expose a listview", m)
	}
	if dm.listViewIf == nil {
		if err := dm.initListView(context.Background()); err != nil {
			return nil, err
		}
	}
	return dm.listViewIf, nil
}

// DecodeCreateVolumeTaskResult turns a TaskResult delivered by the listview into
// the same outcome GetCreateVolumeTaskResult would report, reusing the identical
// CNS decoding helpers as the synchronous path.
//
// The TaskInfo==nil case needs care. processTaskUpdate sets TaskInfo to nil both
// for a task that genuinely failed and, via reportErrorOnAllPendingTasks, for
// every pending task when the listview merely loses its vCenter session. Only
// the caller knows which it is looking at, so this function reports
// CreateVolumeTaskUnknown for a nil TaskInfo and leaves the decision to escalate
// to the caller. Treating it as Failed here would let a session blip trigger a
// second CreateVolume against a disk whose first task is still running.
func DecodeCreateVolumeTaskResult(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	result TaskResult, volumeName string) (CreateVolumeTaskState, *CnsVolumeInfo, string, error) {
	log := logger.GetLogger(ctx)

	if result.TaskInfo == nil {
		err := result.Err
		if err == nil {
			err = logger.LogNewErrorf(log, "listview reported no taskInfo and no error for volume %q", volumeName)
		}
		return CreateVolumeTaskUnknown, nil, ExtractFaultTypeFromErr(ctx, err), err
	}

	taskResult, err := getTaskResultFromTaskInfo(ctx, result.TaskInfo)
	if err != nil {
		return CreateVolumeTaskFailed, nil, ExtractFaultTypeFromErr(ctx, err), err
	}
	if taskResult == nil {
		return CreateVolumeTaskFailed, nil, csifault.CSITaskResultEmptyFault, logger.LogNewErrorf(log,
			"taskResult is empty for CreateVolume task of volume %q, opID: %q",
			volumeName, result.TaskInfo.ActivationId)
	}

	volumeOperationRes := taskResult.GetCnsVolumeOperationResult()
	if volumeOperationRes.Fault != nil {
		faultType := ExtractFaultTypeFromVolumeResponseResult(ctx, volumeOperationRes)
		resp, err := validateCreateVolumeResponseFault(ctx, volumeName, volumeOperationRes)
		if err != nil {
			return CreateVolumeTaskFailed, nil, faultType, err
		}
		return CreateVolumeTaskSucceeded, resp, faultType, nil
	}

	volInfo, faultType, err := getCnsVolumeInfoFromTaskResult(ctx, virtualCenter, volumeName,
		volumeOperationRes.VolumeId, taskResult)
	if err != nil {
		return CreateVolumeTaskFailed, nil, faultType, err
	}
	return CreateVolumeTaskSucceeded, volInfo, "", nil
}
