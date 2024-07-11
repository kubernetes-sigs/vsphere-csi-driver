package volume

import (
	"context"

	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

// ListViewIf provides methods to start and modify a listView object to monitor CNS tasks.
// NewListViewImpl satisfies ListViewIf and should be initialized to use the interface methods
type ListViewIf interface {
	// AddTask adds task to listView and the internal map
	AddTask(ctx context.Context, taskMoRef types.ManagedObjectReference, ch chan TaskResult) error
	// RemoveTask removes task from listview and the internal map
	RemoveTask(ctx context.Context, taskMoRef types.ManagedObjectReference) error
	// ResetVirtualCenter updates the VC object reference.
	// It also triggers a restart of listview object and connection to VC.
	// This is required as VC.Connect() can return true as the VC object points to latest config
	// but adding a task to a listview object created with an older VC object will error out
	// use case: ReloadConfiguration
	ResetVirtualCenter(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter)
	// MarkTaskForDeletion marks a given task MoRef for deletion by a cleanup goroutine
	// use case: failure to remove task due to a vc issue
	MarkTaskForDeletion(ctx context.Context, taskMoRef types.ManagedObjectReference) error
}
