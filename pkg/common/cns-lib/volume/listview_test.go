package volume

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// MockTaskMap is a mock implementation of InMemoryMapIf for testing
type MockTaskMap struct {
	mock.Mock
	mu    sync.RWMutex
	tasks map[types.ManagedObjectReference]TaskDetails
}

func NewMockTaskMap() *MockTaskMap {
	return &MockTaskMap{
		tasks: make(map[types.ManagedObjectReference]TaskDetails),
	}
}

func (m *MockTaskMap) Upsert(ref types.ManagedObjectReference, details TaskDetails) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[ref] = details
	m.Called(ref, details)
}

func (m *MockTaskMap) Delete(ref types.ManagedObjectReference) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, ref)
	m.Called(ref)
}

func (m *MockTaskMap) Get(ref types.ManagedObjectReference) (TaskDetails, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	details, exists := m.tasks[ref]
	m.Called(ref)
	return details, exists
}

func (m *MockTaskMap) GetAll() []TaskDetails {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []TaskDetails
	for _, details := range m.tasks {
		result = append(result, details)
	}
	m.Called()
	return result
}

func (m *MockTaskMap) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := len(m.tasks)
	m.Called()
	return count
}

// Note: We use real types where possible and focus on testing the logic
// rather than mocking complex vSphere API interactions

// Note: NewListViewImpl is difficult to test in isolation due to its dependency on
// real vSphere API components. Integration tests would be more appropriate for this function.

func TestListViewImpl_IsListViewReady(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	listView := &ListViewImpl{
		taskMap:       NewMockTaskMap(),
		virtualCenter: &cnsvsphere.VirtualCenter{},
		ctx:           ctx,
		isReady:       false,
	}

	t.Run("NotReady", func(t *testing.T) {
		assert.False(t, listView.IsListViewReady())
	})

	t.Run("Ready", func(t *testing.T) {
		listView.mu.Lock()
		listView.isReady = true
		listView.mu.Unlock()

		assert.True(t, listView.IsListViewReady())
	})
}

func TestListViewImpl_SetListViewNotReady(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	listView := &ListViewImpl{
		taskMap:       NewMockTaskMap(),
		virtualCenter: &cnsvsphere.VirtualCenter{},
		ctx:           ctx,
		isReady:       true,
	}

	t.Run("SetNotReady", func(t *testing.T) {
		listView.SetListViewNotReady(ctx)
		assert.False(t, listView.IsListViewReady())
	})

	t.Run("SetNotReadyWithCancelFunc", func(t *testing.T) {
		// Set up a cancel function
		_, cancelFunc := context.WithCancel(context.Background())
		listView.waitForUpdatesCancelFunc = cancelFunc
		listView.isReady = true

		listView.SetListViewNotReady(ctx)
		assert.False(t, listView.IsListViewReady())
	})
}

func TestListViewImpl_AddTask(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	taskRef := types.ManagedObjectReference{
		Type:  "Task",
		Value: "task-123",
	}

	resultCh := make(chan TaskResult, 1)

	t.Run("ListViewNotReady", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
			isReady:       false,
		}

		err := listView.AddTask(ctx, taskRef, resultCh)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listview not ready")
		assert.ErrorIs(t, err, ErrListViewTaskAddition)
	})

	t.Run("SessionNotValid", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{}, // Empty VC will make session invalid
			ctx:           ctx,
			isReady:       true,
		}

		err := listView.AddTask(ctx, taskRef, resultCh)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listview not ready")
		assert.ErrorIs(t, err, ErrListViewTaskAddition)
	})
}

func TestListViewImpl_RemoveTask(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	taskRef := types.ManagedObjectReference{
		Type:  "Task",
		Value: "task-123",
	}

	t.Run("ListViewNotReady", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
			isReady:       false,
		}

		err := listView.RemoveTask(ctx, taskRef)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listview not ready")
		assert.ErrorIs(t, err, ErrListViewTaskAddition)
	})

	t.Run("SessionNotValid", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{}, // Empty VC will make session invalid
			ctx:           ctx,
			isReady:       true,
		}

		err := listView.RemoveTask(ctx, taskRef)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listview not ready")
		assert.ErrorIs(t, err, ErrListViewTaskAddition)
	})

	t.Run("ContextDeadlineExceeded", func(t *testing.T) {
		// Create a context with a past deadline
		pastCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Hour))
		defer cancel()

		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
			isReady:       false,
		}

		err := listView.RemoveTask(pastCtx, taskRef)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "listview not ready")
	})
}

func TestListViewImpl_ResetVirtualCenter(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	originalVC := &cnsvsphere.VirtualCenter{
		Config: &cnsvsphere.VirtualCenterConfig{
			Host: "old-vc.example.com",
		},
	}

	newVC := &cnsvsphere.VirtualCenter{
		Config: &cnsvsphere.VirtualCenterConfig{
			Host: "new-vc.example.com",
		},
	}

	listView := &ListViewImpl{
		taskMap:       NewMockTaskMap(),
		virtualCenter: originalVC,
		ctx:           ctx,
	}

	t.Run("Success", func(t *testing.T) {
		listView.ResetVirtualCenter(ctx, newVC)
		assert.Equal(t, newVC, listView.virtualCenter)
		assert.Equal(t, "new-vc.example.com", listView.virtualCenter.Config.Host)
	})
}

func TestListViewImpl_MarkTaskForDeletion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	taskRef := types.ManagedObjectReference{
		Type:  "Task",
		Value: "task-123",
	}

	t.Run("TaskNotFound", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		mockTaskMap.On("Get", taskRef).Return(TaskDetails{}, false)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		err := listView.MarkTaskForDeletion(ctx, taskRef)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to retrieve taskDetails")
	})

	t.Run("Success", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		taskDetails := TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: false,
			ResultCh:         make(chan TaskResult, 1),
		}

		// Pre-populate the mock map
		mockTaskMap.tasks[taskRef] = taskDetails
		mockTaskMap.On("Get", taskRef).Return(taskDetails, true)
		mockTaskMap.On("Upsert", taskRef, mock.MatchedBy(func(td TaskDetails) bool {
			return td.MarkedForRemoval == true
		})).Return()

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		err := listView.MarkTaskForDeletion(ctx, taskRef)
		assert.NoError(t, err)
		mockTaskMap.AssertExpectations(t)
	})
}

func TestListViewImpl_processTaskUpdate(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	taskRef := types.ManagedObjectReference{
		Type:  "Task",
		Value: "task-123",
	}

	t.Run("InvalidPropertyType", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  "invalid-type", // Should be TaskInfo
		}

		// This should not panic and should handle the invalid type gracefully
		listView.processTaskUpdate(prop)
	})

	t.Run("TaskQueued", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		taskInfo := types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateQueued,
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  taskInfo,
		}

		// Should return early for queued tasks
		listView.processTaskUpdate(prop)
	})

	t.Run("TaskRunning", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		taskInfo := types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateRunning,
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  taskInfo,
		}

		// Should return early for running tasks
		listView.processTaskUpdate(prop)
	})

	t.Run("TaskNotInMap", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		mockTaskMap.On("Get", taskRef).Return(TaskDetails{}, false)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		taskInfo := types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateSuccess,
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  taskInfo,
		}

		// Should handle missing task gracefully
		listView.processTaskUpdate(prop)
		mockTaskMap.AssertExpectations(t)
	})

	t.Run("TaskError", func(t *testing.T) {
		resultCh := make(chan TaskResult, 1)
		mockTaskMap := NewMockTaskMap()
		taskDetails := TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: false,
			ResultCh:         resultCh,
		}

		// Pre-populate the mock map
		mockTaskMap.tasks[taskRef] = taskDetails
		mockTaskMap.On("Get", taskRef).Return(taskDetails, true)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		taskInfo := types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateError,
			Error: &types.LocalizedMethodFault{
				LocalizedMessage: "Test error message",
			},
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  taskInfo,
		}

		listView.processTaskUpdate(prop)

		// Check that error was sent to channel
		select {
		case result := <-resultCh:
			assert.Nil(t, result.TaskInfo)
			assert.Error(t, result.Err)
			assert.Equal(t, "Test error message", result.Err.Error())
		case <-time.After(1 * time.Second):
			t.Fatal("Expected result on channel")
		}

		mockTaskMap.AssertExpectations(t)
	})

	t.Run("TaskSuccess", func(t *testing.T) {
		resultCh := make(chan TaskResult, 1)
		mockTaskMap := NewMockTaskMap()
		taskDetails := TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: false,
			ResultCh:         resultCh,
		}

		// Pre-populate the mock map
		mockTaskMap.tasks[taskRef] = taskDetails
		mockTaskMap.On("Get", taskRef).Return(taskDetails, true)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		taskInfo := types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateSuccess,
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  taskInfo,
		}

		listView.processTaskUpdate(prop)

		// Check that success was sent to channel
		select {
		case result := <-resultCh:
			assert.NotNil(t, result.TaskInfo)
			assert.NoError(t, result.Err)
			assert.Equal(t, types.TaskInfoStateSuccess, result.TaskInfo.State)
		case <-time.After(1 * time.Second):
			t.Fatal("Expected result on channel")
		}

		mockTaskMap.AssertExpectations(t)
	})

	t.Run("ChannelBlocked", func(t *testing.T) {
		// Create a channel with no buffer
		resultCh := make(chan TaskResult)
		mockTaskMap := NewMockTaskMap()
		taskDetails := TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: false,
			ResultCh:         resultCh,
		}

		mockTaskMap.On("Get", taskRef).Return(taskDetails, true)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		taskInfo := types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateSuccess,
		}

		prop := types.PropertyChange{
			Name: infoPropertyName,
			Val:  taskInfo,
		}

		// This should not block and should handle the blocked channel gracefully
		done := make(chan bool)
		go func() {
			listView.processTaskUpdate(prop)
			done <- true
		}()

		select {
		case <-done:
			// Should complete without blocking
		case <-time.After(1 * time.Second):
			t.Fatal("processTaskUpdate should not block on full channel")
		}

		mockTaskMap.AssertExpectations(t)
	})
}

func TestListViewImpl_reportErrorOnAllPendingTasks(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	taskRef1 := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	taskRef2 := types.ManagedObjectReference{Type: "Task", Value: "task-2"}

	testError := errors.New("test error")

	t.Run("Success", func(t *testing.T) {
		resultCh1 := make(chan TaskResult, 1)
		resultCh2 := make(chan TaskResult, 1)

		mockTaskMap := NewMockTaskMap()
		taskDetails := []TaskDetails{
			{Reference: taskRef1, ResultCh: resultCh1},
			{Reference: taskRef2, ResultCh: resultCh2},
		}

		// Pre-populate the mock map
		mockTaskMap.tasks[taskRef1] = taskDetails[0]
		mockTaskMap.tasks[taskRef2] = taskDetails[1]
		mockTaskMap.On("GetAll").Return(taskDetails)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		listView.reportErrorOnAllPendingTasks(testError)

		// Check that errors were sent to both channels
		select {
		case result := <-resultCh1:
			assert.Nil(t, result.TaskInfo)
			assert.Equal(t, testError, result.Err)
		case <-time.After(1 * time.Second):
			t.Fatal("Expected result on channel 1")
		}

		select {
		case result := <-resultCh2:
			assert.Nil(t, result.TaskInfo)
			assert.Equal(t, testError, result.Err)
		case <-time.After(1 * time.Second):
			t.Fatal("Expected result on channel 2")
		}

		mockTaskMap.AssertExpectations(t)
	})

	t.Run("BlockedChannel", func(t *testing.T) {
		// Create a channel with no buffer that will block
		resultCh := make(chan TaskResult)

		mockTaskMap := NewMockTaskMap()
		taskDetails := []TaskDetails{
			{Reference: taskRef1, ResultCh: resultCh},
		}

		mockTaskMap.On("GetAll").Return(taskDetails)

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
		}

		// This should not block even with a blocked channel
		done := make(chan bool)
		go func() {
			listView.reportErrorOnAllPendingTasks(testError)
			done <- true
		}()

		select {
		case <-done:
			// Should complete without blocking
		case <-time.After(1 * time.Second):
			t.Fatal("reportErrorOnAllPendingTasks should not block on full channel")
		}

		mockTaskMap.AssertExpectations(t)
	})
}

func TestRemoveTasksMarkedForDeletion(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	t.Run("NilListView", func(t *testing.T) {
		mockTaskMap := NewMockTaskMap()
		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
			listView:      nil, // Nil listView
		}

		// Should handle nil listView gracefully
		RemoveTasksMarkedForDeletion(listView)
	})

	t.Run("Success", func(t *testing.T) {
		// Since RemoveTasksMarkedForDeletion returns early when listView is nil,
		// we can't easily test the success case without a real ListView.
		// This test verifies the function doesn't panic with nil listView
		mockTaskMap := NewMockTaskMap()

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
			listView:      nil, // Nil listView for testing
		}

		// This will handle nil listView gracefully and return early
		RemoveTasksMarkedForDeletion(listView)

		// No expectations to assert since function returns early
	})

	t.Run("RemoveError", func(t *testing.T) {
		// Since RemoveTasksMarkedForDeletion returns early when listView is nil,
		// we can't easily test the error case without a real ListView.
		// This test verifies the function doesn't panic with nil listView
		mockTaskMap := NewMockTaskMap()

		listView := &ListViewImpl{
			taskMap:       mockTaskMap,
			virtualCenter: &cnsvsphere.VirtualCenter{},
			ctx:           ctx,
			listView:      nil, // Nil listView for testing
		}

		// Should handle nil listView gracefully and return early
		RemoveTasksMarkedForDeletion(listView)

		// No expectations to assert since function returns early
	})
}

func TestGetListViewWaitFilter(t *testing.T) {
	// Note: This test would require a real ListView object to work properly
	// For now, we'll test that the function exists and has the right structure
	t.Run("FunctionExists", func(t *testing.T) {
		// We can't easily test this function without a real ListView
		// but we can verify it compiles and the constants are correct
		assert.Equal(t, "info", infoPropertyName)
	})
}

func TestListViewImpl_isSessionValid(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	t.Run("NilClient", func(t *testing.T) {
		listView := &ListViewImpl{
			taskMap: NewMockTaskMap(),
			virtualCenter: &cnsvsphere.VirtualCenter{
				Client: nil,
			},
			ctx: ctx,
		}

		assert.False(t, listView.isSessionValid(ctx))
	})

	t.Run("NilClientClient", func(t *testing.T) {
		// Note: We can't easily test the Client.Client field without proper mocking
		// This test verifies the nil client case is handled
		listView := &ListViewImpl{
			taskMap: NewMockTaskMap(),
			virtualCenter: &cnsvsphere.VirtualCenter{
				Client: nil, // This will trigger the nil check
			},
			ctx: ctx,
		}

		assert.False(t, listView.isSessionValid(ctx))
	})
}

func TestTaskResult(t *testing.T) {
	t.Run("TaskResultStructure", func(t *testing.T) {
		taskInfo := &types.TaskInfo{
			State: types.TaskInfoStateSuccess,
		}
		testError := errors.New("test error")

		// Test success result
		successResult := TaskResult{
			TaskInfo: taskInfo,
			Err:      nil,
		}
		assert.Equal(t, taskInfo, successResult.TaskInfo)
		assert.NoError(t, successResult.Err)

		// Test error result
		errorResult := TaskResult{
			TaskInfo: nil,
			Err:      testError,
		}
		assert.Nil(t, errorResult.TaskInfo)
		assert.Equal(t, testError, errorResult.Err)
	})
}

func TestTaskDetails(t *testing.T) {
	t.Run("TaskDetailsStructure", func(t *testing.T) {
		taskRef := types.ManagedObjectReference{
			Type:  "Task",
			Value: "task-123",
		}
		resultCh := make(chan TaskResult, 1)

		taskDetails := TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: true,
			ResultCh:         resultCh,
		}

		assert.Equal(t, taskRef, taskDetails.Reference)
		assert.True(t, taskDetails.MarkedForRemoval)
		assert.Equal(t, resultCh, taskDetails.ResultCh)
	})
}

func TestErrorConstants(t *testing.T) {
	t.Run("ErrorConstants", func(t *testing.T) {
		assert.Equal(t, "failure to add task to listview", ErrListViewTaskAddition.Error())
		assert.Equal(t, "session is not authenticated", ErrSessionNotAuthenticated.Error())
	})
}

func TestConstants(t *testing.T) {
	t.Run("Constants", func(t *testing.T) {
		assert.Equal(t, 1*time.Minute, waitForUpdatesRetry)
		assert.Equal(t, "info", infoPropertyName)
	})
}
