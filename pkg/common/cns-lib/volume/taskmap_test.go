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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/vim25/types"
)

func createTestTaskMoRef(value string) types.ManagedObjectReference {
	return types.ManagedObjectReference{
		Type:  "Task",
		Value: value,
	}
}

func createTestTaskDetails(taskRef types.ManagedObjectReference) TaskDetails {
	return TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         make(chan TaskResult, 1),
	}
}

func TestNewTaskMap(t *testing.T) {
	tm := NewTaskMap()
	assert.NotNil(t, tm)
	assert.Equal(t, 0, tm.Count())
}

func TestTaskMap_Upsert_Insert(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-1")
	taskDetails := createTestTaskDetails(taskRef)

	oldDetails, existed := tm.Upsert(taskRef, taskDetails)

	assert.False(t, existed, "should not exist on first insert")
	assert.Equal(t, types.ManagedObjectReference{}, oldDetails.Reference, "old details should be zero value")
	assert.Equal(t, 1, tm.Count())

	// Verify the task was inserted
	retrieved, ok := tm.Get(taskRef)
	assert.True(t, ok)
	assert.Equal(t, taskRef, retrieved.Reference)
}

func TestTaskMap_Upsert_Update(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-1")

	// First insert
	ch1 := make(chan TaskResult, 1)
	taskDetails1 := TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch1,
	}
	tm.Upsert(taskRef, taskDetails1)

	// Second insert (update)
	ch2 := make(chan TaskResult, 1)
	taskDetails2 := TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: true,
		ResultCh:         ch2,
	}
	oldDetails, existed := tm.Upsert(taskRef, taskDetails2)

	assert.True(t, existed, "should exist on update")
	assert.Equal(t, ch1, oldDetails.ResultCh, "old details should have the first channel")
	assert.False(t, oldDetails.MarkedForRemoval, "old details should not be marked for removal")
	assert.Equal(t, 1, tm.Count(), "count should still be 1")

	// Verify the task was updated
	retrieved, ok := tm.Get(taskRef)
	assert.True(t, ok)
	assert.Equal(t, ch2, retrieved.ResultCh, "should have the new channel")
	assert.True(t, retrieved.MarkedForRemoval, "should be marked for removal")
}

func TestTaskMap_Delete(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-1")
	taskDetails := createTestTaskDetails(taskRef)

	tm.Upsert(taskRef, taskDetails)
	assert.Equal(t, 1, tm.Count())

	tm.Delete(taskRef)
	assert.Equal(t, 0, tm.Count())

	// Verify the task was deleted
	_, ok := tm.Get(taskRef)
	assert.False(t, ok)
}

func TestTaskMap_Delete_NonExistent(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-nonexistent")

	// Should not panic when deleting non-existent task
	tm.Delete(taskRef)
	assert.Equal(t, 0, tm.Count())
}

func TestTaskMap_Get_Exists(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-1")
	taskDetails := createTestTaskDetails(taskRef)

	tm.Upsert(taskRef, taskDetails)

	retrieved, ok := tm.Get(taskRef)
	assert.True(t, ok)
	assert.Equal(t, taskRef, retrieved.Reference)
}

func TestTaskMap_Get_NotExists(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-nonexistent")

	retrieved, ok := tm.Get(taskRef)
	assert.False(t, ok)
	assert.Equal(t, types.ManagedObjectReference{}, retrieved.Reference)
}

func TestTaskMap_GetAll(t *testing.T) {
	tm := NewTaskMap()

	// Empty map
	all := tm.GetAll()
	assert.Empty(t, all)

	// Add multiple tasks
	taskRef1 := createTestTaskMoRef("task-1")
	taskRef2 := createTestTaskMoRef("task-2")
	taskRef3 := createTestTaskMoRef("task-3")

	tm.Upsert(taskRef1, createTestTaskDetails(taskRef1))
	tm.Upsert(taskRef2, createTestTaskDetails(taskRef2))
	tm.Upsert(taskRef3, createTestTaskDetails(taskRef3))

	all = tm.GetAll()
	assert.Len(t, all, 3)

	// Verify all tasks are present
	refs := make(map[string]bool)
	for _, td := range all {
		refs[td.Reference.Value] = true
	}
	assert.True(t, refs["task-1"])
	assert.True(t, refs["task-2"])
	assert.True(t, refs["task-3"])
}

func TestTaskMap_Count(t *testing.T) {
	tm := NewTaskMap()
	assert.Equal(t, 0, tm.Count())

	taskRef1 := createTestTaskMoRef("task-1")
	taskRef2 := createTestTaskMoRef("task-2")

	tm.Upsert(taskRef1, createTestTaskDetails(taskRef1))
	assert.Equal(t, 1, tm.Count())

	tm.Upsert(taskRef2, createTestTaskDetails(taskRef2))
	assert.Equal(t, 2, tm.Count())

	tm.Delete(taskRef1)
	assert.Equal(t, 1, tm.Count())

	tm.Delete(taskRef2)
	assert.Equal(t, 0, tm.Count())
}

func TestTaskMap_ConcurrentAccess(t *testing.T) {
	tm := NewTaskMap()
	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 100

	// Concurrent inserts
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				taskRef := createTestTaskMoRef("task-" + string(rune(id)) + "-" + string(rune(j)))
				tm.Upsert(taskRef, createTestTaskDetails(taskRef))
			}
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				tm.GetAll()
				tm.Count()
			}
		}()
	}
	wg.Wait()

	// Concurrent mixed operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskRef := createTestTaskMoRef("mixed-task-" + string(rune(id)))
			tm.Upsert(taskRef, createTestTaskDetails(taskRef))
			tm.Get(taskRef)
			tm.Delete(taskRef)
		}(i)
	}
	wg.Wait()
}

func TestTaskMap_Upsert_ReturnsOldChannel(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-1")

	// Create first channel and insert
	ch1 := make(chan TaskResult, 1)
	taskDetails1 := TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch1,
	}
	tm.Upsert(taskRef, taskDetails1)

	// Create second channel and update
	ch2 := make(chan TaskResult, 1)
	taskDetails2 := TaskDetails{
		Reference:        taskRef,
		MarkedForRemoval: false,
		ResultCh:         ch2,
	}
	oldDetails, existed := tm.Upsert(taskRef, taskDetails2)

	assert.True(t, existed)
	assert.Equal(t, ch1, oldDetails.ResultCh, "should return the old channel")
	assert.NotEqual(t, ch2, oldDetails.ResultCh, "old channel should not be the new channel")

	// The caller can now send to the old channel to unblock waiters
	select {
	case oldDetails.ResultCh <- TaskResult{Err: nil}:
		// Successfully sent to old channel
	default:
		t.Error("should be able to send to old channel")
	}
}

func TestTaskMap_ConcurrentUpsertSameTask(t *testing.T) {
	tm := NewTaskMap()
	taskRef := createTestTaskMoRef("task-1")
	var wg sync.WaitGroup
	numGoroutines := 100

	channels := make([]chan TaskResult, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		channels[i] = make(chan TaskResult, 1)
	}

	// Concurrent upserts on the same task
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskDetails := TaskDetails{
				Reference:        taskRef,
				MarkedForRemoval: false,
				ResultCh:         channels[id],
			}
			tm.Upsert(taskRef, taskDetails)
		}(i)
	}
	wg.Wait()

	// Should only have one task in the map
	assert.Equal(t, 1, tm.Count())

	// The final task should have one of the channels
	retrieved, ok := tm.Get(taskRef)
	assert.True(t, ok)
	assert.NotNil(t, retrieved.ResultCh)
}
