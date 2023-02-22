package volume

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/cns"
	cnssim "github.com/vmware/govmomi/cns/simulator"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

func TestAddRemoveListView(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	model := simulator.VPX()
	defer model.Remove()
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}

	s := model.Service.NewServer()
	defer s.Close()

	model.Service.RegisterSDK(cnssim.New())

	virtualCenter, err := getVirtualCenterForTest(ctx, s)
	if err != nil {
		t.Fatal(err)
	}

	listViewImpl, err := NewListViewImpl(ctx, virtualCenter, virtualCenter.Client)
	assert.NoError(t, err)
	listViewImpl.shouldStopListening = true
	createSpecList := getCreateSpecList()
	ch := make(chan TaskResult)
	assert.NotNil(t, ch)

	createTask, err := virtualCenter.CnsClient.CreateVolume(ctx, createSpecList)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("createTask created: %+v", createTask)
	err = listViewImpl.AddTask(ctx, createTask.Reference(), ch)
	assert.NoError(t, err)
	assert.Equal(t, 1, listViewImpl.taskMap.Count())
	result := <-ch
	if result.Err != nil {
		t.Errorf("result.Err: %v", result.Err)
		return
	}
	t.Logf("result: %+v", result.TaskInfo)
	assert.Equal(t, types.TaskInfoStateSuccess, result.TaskInfo.State)
	err = listViewImpl.RemoveTask(ctx, createTask.Reference())
	assert.NoError(t, err)
	assert.Equal(t, 0, listViewImpl.taskMap.Count())
}

func TestMarkForDeletion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	model := simulator.VPX()
	defer model.Remove()
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}

	s := model.Service.NewServer()
	defer s.Close()

	model.Service.RegisterSDK(cnssim.New())

	virtualCenter, err := getVirtualCenterForTest(ctx, s)
	if err != nil {
		t.Fatal(err)
	}

	listViewImpl, err := NewListViewImpl(ctx, virtualCenter, virtualCenter.Client)
	assert.NoError(t, err)
	listViewImpl.shouldStopListening = true
	createSpecList := getCreateSpecList()
	ch := make(chan TaskResult)
	assert.NotNil(t, ch)

	createTask, err := virtualCenter.CnsClient.CreateVolume(ctx, createSpecList)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("createTask created: %+v", createTask)
	err = listViewImpl.AddTask(ctx, createTask.Reference(), ch)
	assert.NoError(t, err)
	assert.Equal(t, 1, listViewImpl.taskMap.Count())

	result := <-ch
	err = result.Err
	taskInfo := result.TaskInfo
	assert.NoError(t, err)
	assert.NotNil(t, taskInfo)

	t.Logf("marking task for deletion")
	err = listViewImpl.MarkTaskForDeletion(ctx, createTask.Reference())
	assert.NoError(t, err)

	RemoveTasksMarkedForDeletion(listViewImpl)
	assert.Equal(t, 0, listViewImpl.taskMap.Count())
}

func getVirtualCenterForTest(ctx context.Context, s *simulator.Server) (*vsphere.VirtualCenter, error) {
	newClient, err := govmomi.NewClient(ctx, s.URL, true)
	if err != nil {
		return nil, err
	}

	cnsClient, err := cns.NewClient(ctx, newClient.Client)
	if err != nil {
		return nil, err
	}

	virtualCenter := &vsphere.VirtualCenter{
		Client:    newClient,
		CnsClient: cnsClient,
	}
	return virtualCenter, nil
}

func getCreateSpecList() []cnstypes.CnsVolumeCreateSpec {
	// Get a simulator DS
	datastore := simulator.Map.Any("Datastore").(*simulator.Datastore)
	var capacityInMb int64 = 1024

	return []cnstypes.CnsVolumeCreateSpec{
		{
			Name:       "test",
			VolumeType: "TestVolumeType",
			Datastores: []types.ManagedObjectReference{
				datastore.Self,
			},
			BackingObjectDetails: &cnstypes.CnsBackingObjectDetails{
				CapacityInMb: capacityInMb,
			},
			Profile: []types.BaseVirtualMachineProfileSpec{
				&types.VirtualMachineDefinedProfileSpec{
					ProfileId: uuid.New().String(),
				},
			},
		},
	}
}
