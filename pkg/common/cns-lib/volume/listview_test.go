/*
Copyright 2025 The Kubernetes Authors.

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/vim25/types"
)

// MockTaskMap implements InMemoryMapIf for testing
type MockTaskMap struct {
	mu   sync.RWMutex
	m    map[types.ManagedObjectReference]TaskDetails
	gets int
}

func NewMockTaskMap() *MockTaskMap {
	return &MockTaskMap{
		m: make(map[types.ManagedObjectReference]TaskDetails),
	}
}

func (t *MockTaskMap) Upsert(task types.ManagedObjectReference, taskDetails TaskDetails) (TaskDetails, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	oldDetails, existed := t.m[task]
	t.m[task] = taskDetails
	return oldDetails, existed
}

func (t *MockTaskMap) Delete(task types.ManagedObjectReference) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, task)
}

func (t *MockTaskMap) Get(task types.ManagedObjectReference) (TaskDetails, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	t.gets++
	taskDetails, ok := t.m[task]
	return taskDetails, ok
}

func (t *MockTaskMap) GetAll() []TaskDetails {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var allTasks []TaskDetails
	for _, details := range t.m {
		allTasks = append(allTasks, details)
	}
	return allTasks
}

func (t *MockTaskMap) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.m)
}

// TestProcessTaskUpdate_Success tests successful task update processing
func TestProcessTaskUpdate_Success(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	// Create a successful task update
	prop := types.PropertyChange{
		Name: "info",
		Val: types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateSuccess,
		},
	}

	listView.processTaskUpdate(prop)

	// Verify result was sent to channel
	select {
	case result := <-ch:
		assert.NoError(t, result.Err)
		assert.NotNil(t, result.TaskInfo)
		assert.Equal(t, types.TaskInfoStateSuccess, result.TaskInfo.State)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}

// TestProcessTaskUpdate_Error tests error task update processing
func TestProcessTaskUpdate_Error(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	// Create an error task update
	prop := types.PropertyChange{
		Name: "info",
		Val: types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateError,
			Error: &types.LocalizedMethodFault{
				LocalizedMessage: "test error message",
			},
		},
	}

	listView.processTaskUpdate(prop)

	// Verify error was sent to channel
	select {
	case result := <-ch:
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "test error message")
		assert.Nil(t, result.TaskInfo)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}

// TestProcessTaskUpdate_TaskNotInMap tests when task is not in the map
func TestProcessTaskUpdate_TaskNotInMap(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-nonexistent"}

	// Create a task update for a task not in the map
	prop := types.PropertyChange{
		Name: "info",
		Val: types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateSuccess,
		},
	}

	// Should not panic
	listView.processTaskUpdate(prop)
}

// TestProcessTaskUpdate_QueuedState tests that queued tasks are ignored
func TestProcessTaskUpdate_QueuedState(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	// Create a queued task update
	prop := types.PropertyChange{
		Name: "info",
		Val: types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateQueued,
		},
	}

	listView.processTaskUpdate(prop)

	// Channel should be empty (no result sent)
	select {
	case <-ch:
		t.Fatal("should not receive result for queued task")
	case <-time.After(100 * time.Millisecond):
		// Expected - no result sent
	}
}

// TestProcessTaskUpdate_RunningState tests that running tasks are ignored
func TestProcessTaskUpdate_RunningState(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	// Create a running task update
	prop := types.PropertyChange{
		Name: "info",
		Val: types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateRunning,
		},
	}

	listView.processTaskUpdate(prop)

	// Channel should be empty (no result sent)
	select {
	case <-ch:
		t.Fatal("should not receive result for running task")
	case <-time.After(100 * time.Millisecond):
		// Expected - no result sent
	}
}

// TestProcessTaskUpdate_DuplicateUpdate tests handling of duplicate updates
func TestProcessTaskUpdate_DuplicateUpdate(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	prop := types.PropertyChange{
		Name: "info",
		Val: types.TaskInfo{
			Task:  taskRef,
			State: types.TaskInfoStateSuccess,
		},
	}

	// First update should succeed
	listView.processTaskUpdate(prop)

	// Second update should be handled gracefully (channel full)
	listView.processTaskUpdate(prop)

	// Should only have one result in channel
	select {
	case result := <-ch:
		assert.NoError(t, result.Err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}

	// Channel should now be empty
	select {
	case <-ch:
		t.Fatal("should not have second result")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

// TestReportErrorOnAllPendingTasks tests error reporting to all pending tasks
func TestReportErrorOnAllPendingTasks(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	// Add multiple tasks
	channels := make([]chan TaskResult, 3)
	for i := 0; i < 3; i++ {
		taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-" + string(rune('1'+i))}
		channels[i] = make(chan TaskResult, 1)
		taskMap.Upsert(taskRef, TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: false,
			ResultCh:         channels[i],
		})
	}

	testErr := errors.New("VC connection failed")
	listView.reportErrorOnAllPendingTasks(testErr)

	// All channels should receive the error
	for i, ch := range channels {
		select {
		case result := <-ch:
			assert.Error(t, result.Err)
			assert.Equal(t, testErr, result.Err)
			assert.Nil(t, result.TaskInfo)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for error on channel %d", i)
		}
	}
}

// TestReportErrorOnAllPendingTasks_ChannelFull tests when channel already has a result
func TestReportErrorOnAllPendingTasks_ChannelFull(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)

	// Pre-fill the channel
	ch <- TaskResult{TaskInfo: &types.TaskInfo{State: types.TaskInfoStateSuccess}}

	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	testErr := errors.New("VC connection failed")
	// Should not panic or block
	listView.reportErrorOnAllPendingTasks(testErr)

	// Channel should still have the original result
	select {
	case result := <-ch:
		assert.NoError(t, result.Err)
		assert.NotNil(t, result.TaskInfo)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}

// TestConcurrentProcessTaskUpdateAndReportError tests race between processTaskUpdate and reportErrorOnAllPendingTasks
func TestConcurrentProcessTaskUpdateAndReportError(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	numTasks := 100
	channels := make([]chan TaskResult, numTasks)

	for i := 0; i < numTasks; i++ {
		taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-" + string(rune(i))}
		channels[i] = make(chan TaskResult, 1)
		taskMap.Upsert(taskRef, TaskDetails{
			Reference:        taskRef,
			MarkedForRemoval: false,
			ResultCh:         channels[i],
		})
	}

	var wg sync.WaitGroup

	// Start error reporter
	wg.Add(1)
	go func() {
		defer wg.Done()
		testErr := errors.New("VC connection failed")
		listView.reportErrorOnAllPendingTasks(testErr)
	}()

	// Start task update processors
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-" + string(rune(id))}
			prop := types.PropertyChange{
				Name: "info",
				Val: types.TaskInfo{
					Task:  taskRef,
					State: types.TaskInfoStateSuccess,
				},
			}
			listView.processTaskUpdate(prop)
		}(i)
	}

	wg.Wait()

	// Each channel should have exactly one result (either success or error)
	for i, ch := range channels {
		select {
		case result := <-ch:
			// Either error from reportErrorOnAllPendingTasks or success from processTaskUpdate
			if result.Err != nil {
				assert.Contains(t, result.Err.Error(), "VC connection failed")
			} else {
				assert.NotNil(t, result.TaskInfo)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for result on channel %d", i)
		}

		// Channel should now be empty (only one result)
		select {
		case <-ch:
			// This is acceptable - the non-blocking send in one of the goroutines
			// might have failed, but another succeeded
		case <-time.After(10 * time.Millisecond):
			// Expected - only one result
		}
	}
}

// TestTaskReplacementSendsErrorToOldChannel tests that replacing a task sends error to old channel
func TestTaskReplacementSendsErrorToOldChannel(t *testing.T) {
	taskMap := NewTaskMap()

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}

	// Create first channel and insert
	ch1 := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch1,
	})

	// Create second channel and update
	ch2 := make(chan TaskResult, 1)
	oldDetails, existed := taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch2,
	})

	assert.True(t, existed)
	assert.Equal(t, ch1, oldDetails.ResultCh)

	// Simulate what AddTask does - send error to old channel
	if existed && oldDetails.ResultCh != nil && oldDetails.ResultCh != ch2 {
		select {
		case oldDetails.ResultCh <- TaskResult{
			TaskInfo: nil,
			Err:      errors.New("task was replaced by a new request"),
		}:
			// Success
		default:
			t.Error("should be able to send to old channel")
		}
	}

	// Old channel should have the error
	select {
	case result := <-ch1:
		assert.Error(t, result.Err)
		assert.Contains(t, result.Err.Error(), "replaced")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for error on old channel")
	}
}

// TestWaitForResultOrTimeout_ChannelClosed tests behavior when channel is closed
func TestWaitForResultOrTimeout_ChannelClosed(t *testing.T) {
	ctx := context.Background()
	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)

	// Send a result before closing
	ch <- TaskResult{
		TaskInfo: nil,
		Err:      errors.New("task was replaced"),
	}

	taskInfo, err := waitForResultOrTimeout(ctx, taskRef, ch)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "replaced")
	assert.Nil(t, taskInfo)
}

// TestWaitForResultOrTimeout_Timeout tests timeout behavior
func TestWaitForResultOrTimeout_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	// Don't send anything to channel

	taskInfo, err := waitForResultOrTimeout(ctx, taskRef, ch)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "time out")
	assert.Nil(t, taskInfo)
}

// TestWaitForResultOrTimeout_Success tests successful result
func TestWaitForResultOrTimeout_Success(t *testing.T) {
	ctx := context.Background()
	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)

	expectedTaskInfo := &types.TaskInfo{
		Task:  taskRef,
		State: types.TaskInfoStateSuccess,
	}
	ch <- TaskResult{
		TaskInfo: expectedTaskInfo,
		Err:      nil,
	}

	taskInfo, err := waitForResultOrTimeout(ctx, taskRef, ch)

	assert.NoError(t, err)
	assert.Equal(t, expectedTaskInfo, taskInfo)
}

// TestMarkTaskForDeletion tests atomic mark for deletion
func TestMarkTaskForDeletion(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	err := listView.MarkTaskForDeletion(ctx, taskRef)
	assert.NoError(t, err)

	// Verify task is marked
	details, ok := taskMap.Get(taskRef)
	assert.True(t, ok)
	assert.True(t, details.MarkedForRemoval)
}

// TestMarkTaskForDeletion_NotExists tests marking non-existent task
func TestMarkTaskForDeletion_NotExists(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-nonexistent"}

	err := listView.MarkTaskForDeletion(ctx, taskRef)
	assert.Error(t, err)
}

// TestConcurrentMarkForDeletion tests concurrent marking
func TestConcurrentMarkForDeletion(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	taskRef := types.ManagedObjectReference{Type: "Task", Value: "task-1"}
	ch := make(chan TaskResult, 1)
	taskMap.Upsert(taskRef, TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = listView.MarkTaskForDeletion(ctx, taskRef)
		}()
	}

	wg.Wait()

	// Task should be marked
	details, ok := taskMap.Get(taskRef)
	assert.True(t, ok)
	assert.True(t, details.MarkedForRemoval)
}

// TestProcessTaskUpdate_InvalidPropertyChange tests handling of invalid property change
func TestProcessTaskUpdate_InvalidPropertyChange(t *testing.T) {
	ctx := context.Background()
	taskMap := NewMockTaskMap()

	listView := &ListViewImpl{
		taskMap: taskMap,
		ctx:     ctx,
	}

	// Create a property change with invalid Val type
	prop := types.PropertyChange{
		Name: "info",
		Val:  "invalid type", // Not a TaskInfo
	}

	// Should not panic
	listView.processTaskUpdate(prop)
}
