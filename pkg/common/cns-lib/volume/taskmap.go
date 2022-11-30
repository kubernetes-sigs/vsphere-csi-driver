package volume

import (
	"sync"

	"github.com/vmware/govmomi/vim25/types"
)

// InMemoryMapIf is used to not provide direct access to internal mutex or map to methods in the same pkg
type InMemoryMapIf interface {
	// Upsert : if task exists in the map, the taskDetails will be overwritten.
	// if the task doesn't exist, the taskDetails will be stored
	Upsert(types.ManagedObjectReference, TaskDetails)
	// Delete task from map
	Delete(types.ManagedObjectReference)
	// Get retrieves a single item from a map
	Get(types.ManagedObjectReference) (TaskDetails, bool)
	// GetAll retrieves all map items
	GetAll() []TaskDetails
	// Count returns the count of all items present in the map
	Count() int
}

// TaskMap binds together the task details map and a mutex to guard it
type TaskMap struct {
	// sync.RWMutex: a mutex is required for concurrent access to a go map
	// reference - see the section on Concurrency at https://go.dev/blog/maps
	// important note - sync.RWMutex can be held by an arbitrary number of readers or a single writer.
	// an example of how sync.RWMutex works - https://gist.github.com/adikul30/31fad45c2b77bf70cd7e0a352b6d98fb
	mu sync.RWMutex
	// taskMap: is an in-memory map to associate task details
	// taskMap             map[types.ManagedObjectReference]*TaskDetails
	m map[types.ManagedObjectReference]TaskDetails
}

// NewTaskMap returns a new instance of TaskMap
func NewTaskMap() InMemoryMapIf {
	return &TaskMap{
		m: make(map[types.ManagedObjectReference]TaskDetails),
	}
}

// Upsert adds/updates items and requires an exclusive write lock
func (t *TaskMap) Upsert(task types.ManagedObjectReference, taskDetails TaskDetails) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[task] = taskDetails
}

// Delete deletes items from the map and requires an exclusive write lock
func (t *TaskMap) Delete(task types.ManagedObjectReference) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, task)
}

// Get retrieves a single item from the map and requires a read lock
func (t *TaskMap) Get(task types.ManagedObjectReference) (TaskDetails, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	taskDetails, ok := t.m[task]
	return taskDetails, ok
}

// GetAll retrieves all tasks from the map and requires a read lock
func (t *TaskMap) GetAll() []TaskDetails {
	var allTasks []TaskDetails
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, details := range t.m {
		allTasks = append(allTasks, details)
	}
	return allTasks
}

// Count returns the number of tasks present in the map
func (t *TaskMap) Count() int {
	return len(t.m)
}
