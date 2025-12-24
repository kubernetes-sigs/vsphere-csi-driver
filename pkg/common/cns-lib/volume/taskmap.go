package volume

import (
	"sync"

	"github.com/vmware/govmomi/vim25/types"
)

// InMemoryMapIf is used to not provide direct access to internal mutex or map to methods in the same pkg
type InMemoryMapIf interface {
	// Upsert adds or updates a task. Returns old TaskDetails and whether it existed.
	Upsert(types.ManagedObjectReference, TaskDetails) (TaskDetails, bool)
	Delete(types.ManagedObjectReference)
	Get(types.ManagedObjectReference) (TaskDetails, bool)
	GetAll() []TaskDetails
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

func NewTaskMap() InMemoryMapIf {
	return &TaskMap{
		m: make(map[types.ManagedObjectReference]TaskDetails),
	}
}

func (t *TaskMap) Upsert(task types.ManagedObjectReference, taskDetails TaskDetails) (TaskDetails, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	oldDetails, existed := t.m[task]
	t.m[task] = taskDetails
	return oldDetails, existed
}

func (t *TaskMap) Delete(task types.ManagedObjectReference) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, task)
}

func (t *TaskMap) Get(task types.ManagedObjectReference) (TaskDetails, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	taskDetails, ok := t.m[task]
	return taskDetails, ok
}

func (t *TaskMap) GetAll() []TaskDetails {
	var allTasks []TaskDetails
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, details := range t.m {
		allTasks = append(allTasks, details)
	}
	return allTasks
}

func (t *TaskMap) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.m)
}
