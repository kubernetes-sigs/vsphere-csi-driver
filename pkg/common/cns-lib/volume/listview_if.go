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
	// SetVirtualCenter is a setter method for the reference to the global vcenter object.
	// use case: ReloadConfiguration
	SetVirtualCenter(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) error
	// MarkTaskForDeletion marks a given task MoRef for deletion by a cleanup goroutine
	// use case: failure to remove task due to a vc issue
	MarkTaskForDeletion(ctx context.Context, taskMoRef types.ManagedObjectReference) error
}
