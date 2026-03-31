package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// in case of vc connection failure, we wait for a minute
	// to not overwhelm the vc with continuous requests
	waitForUpdatesRetry = 1 * time.Minute
	// `info` is a defined property in a Task object
	// we want to monitor the TaskInfo that includes the status of a task
	infoPropertyName = "info"
)

// ListViewImpl is the struct used to manage a single listView instance.
type ListViewImpl struct {
	// taskMap provides methods to store, retrieve, and delete tasks stored in the in-memory map
	// this map holds a mapping between the task and the channel used to return the response to the caller
	taskMap InMemoryMapIf
	// virtualCenter: holds a reference to the global VC object
	virtualCenter *cnsvsphere.VirtualCenter
	// listView: holds the managed object used to monitor multiple concurrent VC tasks
	listView *view.ListView
	// context.Context: new context for the life of the listview object.
	// it's separate from the context set by CSI ops
	// to the channel created by the caller to receive the task result
	ctx context.Context
	// waitForUpdatesContext and waitForUpdatesCancelFunc allows us to break out of the WaitForUpdates loop
	// use case: session expiry
	waitForUpdatesContext    context.Context
	waitForUpdatesCancelFunc context.CancelFunc
	// shouldStopListening: in case of regular CSI operation, even after receiving a batch of updates,
	// we want to continue listening for subsequent updates.
	// in case of unit tests, we need to stop listening for the test to complete execution
	shouldStopListening bool
	// this mutex is used while logging out expired VC session and creating a new one
	mu sync.RWMutex
	// isReady defines the ready state of the listview + property collector mechanism
	isReady bool
}

// TaskDetails is used to hold state for a task
type TaskDetails struct {
	Reference types.ManagedObjectReference
	// MarkedForRemoval helps in retrying the removal of tasks in case of failures
	MarkedForRemoval bool
	// channel to return results. the caller (CSI op) is waiting on this channel
	ResultCh chan TaskResult
}

type TaskResult struct {
	TaskInfo *types.TaskInfo
	Err      error
}

var ErrListViewTaskAddition = errors.New("failure to add task to listview")
var ErrSessionNotAuthenticated = errors.New("session is not authenticated")

// NewListViewImpl creates a new listView object and starts a goroutine to listen to property collector task updates
func NewListViewImpl(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) (*ListViewImpl, error) {
	log := logger.GetLogger(ctx)
	t := &ListViewImpl{
		taskMap:       NewTaskMap(),
		virtualCenter: virtualCenter,
		ctx:           ctx,
	}
	err := t.createListView(ctx, nil)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to create a ListView. error: %+v", err)
	}
	go t.listenToTaskUpdates()
	go t.restartContainer()
	return t, nil
}

// restartContainer runs as a goroutine that checks every 30 seconds
// if credentials are valid but listview state is not ready, it will start a timer of 2 minutes.
// after 2 minutes, if listview state is still not ready,
// we've run into some irrecoverable scenario and should restart the container
func (l *ListViewImpl) restartContainer() {
	log := logger.GetLogger(l.ctx)
	ticker := time.NewTicker(30 * time.Second)
	var waitMu sync.Mutex
	waiting := false
	for range ticker.C {
		waitMu.Lock()
		if !waiting && l.connect() == nil && !l.IsListViewReady() {
			log.Debugf("credentials are correct but listview is not ready. " +
				"will wait 2 minutes before restarting the container")
			waiting = true
			waitMu.Unlock()
			time.AfterFunc(2*time.Minute, func() {
				waitMu.Lock()
				defer waitMu.Unlock()
				if !l.IsListViewReady() {
					log.Infof("credentials are correct but listview is not ready within 2 minutes. " +
						"restarting the container")
					os.Exit(1)
				}
				log.Debugf("credentials are correct and listview is ready within 2 minutes")
				waiting = false
			})
		} else {
			waitMu.Unlock()
		}
	}
}

func (l *ListViewImpl) createListView(ctx context.Context, tasks []types.ManagedObjectReference) error {
	log := logger.GetLogger(ctx)
	if err := l.virtualCenter.Connect(ctx); err != nil {
		return logger.LogNewErrorf(log, "failed to create a ListView. error: %+v", err)
	} else {
		log.Debugf("connection to vc successful")
	}
	// doing a direct assignment to t.listView in case of failure
	// leads to NPE while accessing listView elsewhere
	listView, err := view.NewManager(l.virtualCenter.Client.Client).CreateListView(ctx, tasks)
	if err != nil {
		return err
	}
	l.listView = listView
	log.Infof("created listView object %+v for virtualCenter: %+v",
		l.listView.Reference(), l.virtualCenter.Config.Host)
	return nil
}

// ResetVirtualCenter updates the VC object reference.
// use case: ReloadConfiguration
func (l *ListViewImpl) ResetVirtualCenter(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	log.Info("attempting to acquire lock before updating vc object")
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Info("acquired lock before updating vc object")
	l.virtualCenter = virtualCenter
	log.Info("updated VirtualCenter object reference in ListView")
}

func getListViewWaitFilter(listView *view.ListView) *property.WaitFilter {
	ts := types.TraversalSpec{
		Type: "ListView",
		Path: "view",
		Skip: types.NewBool(false),
	}
	filter := new(property.WaitFilter)

	filter.Add(listView.Reference(), "Task", []string{infoPropertyName}, &ts)
	reportMissingObjectsInResults := true
	filter.Spec.ReportMissingObjectsInResults = &reportMissingObjectsInResults
	return filter
}

func (l *ListViewImpl) isSessionValid(ctx context.Context) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	log := logger.GetLogger(ctx)
	if l.virtualCenter.Client == nil || l.virtualCenter.Client.Client == nil {
		return false
	}
	// If session hasn't expired, nothing to do.
	sessionMgr := session.NewManager(l.virtualCenter.Client.Client)
	// SessionMgr.UserSession(ctx) retrieves and returns the SessionManager's
	// CurrentSession field. Nil is returned if the session is not
	// authenticated or timed out.
	if userSession, err := sessionMgr.UserSession(ctx); err != nil {
		log.Errorf("failed to obtain user session with err: %v", err)
		return false
	} else if userSession != nil {
		return true
	}
	return false
}

// IsListViewReady wraps a read lock to access the state of isReady
func (l *ListViewImpl) IsListViewReady() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isReady
}

// AddTask adds task to listView and the internal map
func (l *ListViewImpl) AddTask(ctx context.Context, taskMoRef types.ManagedObjectReference, ch chan TaskResult) error {
	log := logger.GetLogger(ctx)
	log.Infof("AddTask called for %+v", taskMoRef)

	if !l.IsListViewReady() {
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}

	if !l.isSessionValid(ctx) {
		log.Infof("current session is not valid")
		l.SetListViewNotReady(ctx)
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}

	l.taskMap.Upsert(taskMoRef, TaskDetails{
		Reference:        taskMoRef,
		MarkedForRemoval: false,
		ResultCh:         ch,
	})
	log.Debugf("task %+v added to map", taskMoRef)
	log.Infof("client is valid. trying to add task to listview object")

	response, err := l.listView.Add(l.ctx, []types.ManagedObjectReference{taskMoRef})
	if err != nil {
		l.taskMap.Delete(taskMoRef)
		l.SetListViewNotReady(ctx)
		return fmt.Errorf("%w. task: %v, err: %v", ErrListViewTaskAddition, taskMoRef, err)
	}
	if len(response) > 0 {
		for _, unresolvedTaskRef := range response {
			l.taskMap.Delete(unresolvedTaskRef)
			fault := &soap.Fault{
				Code: "ServerFaultCode",
				String: fmt.Sprintf("The object %v has already been deleted "+
					"or has not been completely created", taskMoRef),
			}
			fault.Detail.Fault = types.ManagedObjectNotFound{
				Obj: taskMoRef,
			}
			return soap.WrapSoapFault(fault)
		}
	}

	log.Infof("task %+v added to listView", taskMoRef)
	return nil
}

// RemoveTask removes task from listview and the internal map
func (l *ListViewImpl) RemoveTask(ctx context.Context, taskMoRef types.ManagedObjectReference) error {
	log := logger.GetLogger(ctx)
	// the op context has a timeout of 5 mins.
	// if CNS doesn't respond within that time, the context deadline is exceeded.
	// in that case, we need to use the context used my ListViewImpl which is init from controller or syncer main
	deadline, ok := ctx.Deadline()
	if !ok || time.Now().After(deadline) {
		log.Infof("op timeout. context deadline exceeded. using listview context without a timeout")
		ctx = l.ctx
	}

	if !l.IsListViewReady() {
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}

	if !l.isSessionValid(ctx) {
		log.Infof("current session is not valid")
		l.SetListViewNotReady(ctx)
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}

	log.Infof("client is valid. trying to remove task from listview object")
	_, err := l.listView.Remove(l.ctx, []types.ManagedObjectReference{taskMoRef})
	if err != nil {
		l.SetListViewNotReady(ctx)
		return logger.LogNewErrorf(log, "failed to remove task %v from ListView. error: %+v", taskMoRef, err)
	}
	log.Infof("task %+v removed from listView", taskMoRef)
	l.taskMap.Delete(taskMoRef)
	log.Debugf("task %+v removed from map", taskMoRef)
	return nil
}

func (l *ListViewImpl) SetListViewNotReady(ctx context.Context) {
	log := logger.GetLogger(ctx)
	log.Infof("waiting for lock before setting listview to not ready")
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Infof("acquired lock before setting listview to not ready")
	l.isReady = false
	if l.waitForUpdatesCancelFunc != nil {
		l.waitForUpdatesCancelFunc()
	}
	log.Info("cancelled context")
}

func (l *ListViewImpl) connect() error {
	log := logger.GetLogger(l.ctx)
	log.Debugf("waiting for lock before calling connect")
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Debugf("acquired lock before calling connect")
	return l.virtualCenter.Connect(l.ctx)
}

// listenToTaskUpdates is a long-running goroutine
// that uses a property collector to listen for task updates
// CSI ops add CNS tasks to listview and wait for a response from CNS
// when update(s) are received by the property collector,
// it spawns a new goroutine to process each task update and return the result to the caller
func (l *ListViewImpl) listenToTaskUpdates() {
	log := logger.GetLogger(l.ctx)
	filter := getListViewWaitFilter(l.listView)
	l.waitForUpdatesContext, l.waitForUpdatesCancelFunc = context.WithCancel(context.Background())
	// we need to recreate the listView and the wait filter after any error from vc
	// for the first iteration we already have the listView and filter initialized
	recreateView := false
	for {
		// calling Connect at the beginning to ensure the current session is neither nil nor NotAuthenticated
		if err := l.connect(); err != nil {
			log.Errorf("failed to connect to vCenter. err: %v", err)
			time.Sleep(waitForUpdatesRetry)
			continue
		} else {
			log.Infof("connection to vc successful")
		}

		log.Infof("attempting lock before re-creating listview")
		l.mu.Lock()
		log.Infof("acquired lock before re-creating listview")
		if recreateView {
			if l.listView != nil {
				destroyListviewErr := l.listView.Destroy(l.ctx)
				if destroyListviewErr != nil {
					// ignoring the error and re-creating the list view
					log.Errorf("failed to destroy listview object. err: %v", destroyListviewErr)
				} else {
					log.Info("successfully destroyed existing listview")
				}
			}
			log.Info("re-creating the listView object")
			err := l.createListView(l.ctx, nil)
			if err != nil {
				log.Errorf("failed to create a ListView. error: %+v", err)
				l.mu.Unlock()
				continue
			}
			log.Info("successfully created listview")

			filter = getListViewWaitFilter(l.listView)
			l.waitForUpdatesContext, l.waitForUpdatesCancelFunc = context.WithCancel(context.Background())
			recreateView = false
		}

		log.Info("Starting listening for task updates...")
		log.Infof("waitForUpdatesContext %v", l.waitForUpdatesContext)
		pc, pcErr := property.DefaultCollector(l.virtualCenter.Client.Client).Create(l.ctx)
		if pcErr != nil {
			log.Errorf("failed to create PropertyCollector for WaitForUpdatesEx for ListView. error: %+v", pcErr)
			l.mu.Unlock()
			continue
		}
		l.isReady = true
		log.Infof("listview ready state is %v", l.isReady)
		l.mu.Unlock()
		err := property.WaitForUpdatesEx(l.waitForUpdatesContext, pc, filter, func(updates []types.ObjectUpdate) bool {
			log.Debugf("Got %d property collector update(s)", len(updates))
			for _, update := range updates {
				for _, prop := range update.ChangeSet {
					log.Debugf("Got update for object %v properties %v", update.Obj, prop)
					// we don't need a lock at this line as we aren't accessing any map item
					go l.processTaskUpdate(prop)
				}
			}

			// this return value is used by the WaitForUpdates method.
			// we only want this true while running the unit tests so the test can finish
			return l.shouldStopListening
		})

		// Attempt to clean up the property collector using a new context to
		// ensure it goes through. This call *might* fail if the session's
		// auth has expired, but it is worth trying.
		_ = pc.Destroy(context.Background())

		// if property collector returns any errors,
		// we want to immediately return a fault for all the pending tasks in the map
		// note: this is not a task error but an error from the vc
		if err != nil {
			log.Errorf("WaitForUpdates returned err: %v for vc: %+v", err,
				l.virtualCenter.Config.Host)
			recreateView = true
			l.reportErrorOnAllPendingTasks(err)
			log.Info("waiting for lock before setting listview ready state to false")
			l.mu.Lock()
			log.Info("acquired lock before setting listview ready state to false")
			log.Infof("setting listview ready state to false. current ready state: %v", l.isReady)
			l.isReady = false
			log.Infof("listview ready state is %v", l.isReady)
			l.mu.Unlock()
		}
		// use case: unit tests: this will help us stop listening
		// and finish the unit test
		if l.shouldStopListening {
			log.Infof("stopped listening for task updates")
			return
		}
	}
}

// reportErrorOnAllPendingTasks returns failure to all pending tasks in the map in case of vc failure
func (l *ListViewImpl) reportErrorOnAllPendingTasks(err error) {
	log := logger.GetLogger(context.Background())
	for _, taskDetails := range l.taskMap.GetAll() {
		result := TaskResult{
			TaskInfo: nil,
			Err:      err,
		}
		// Non-blocking send
		select {
		case taskDetails.ResultCh <- result:
			log.Infof("reported error for task %+v", taskDetails.Reference)
		default:
			log.Warnf("failed to report error for task %+v: channel blocked", taskDetails.Reference)
		}
	}
}

// processTaskUpdate is processes each task update in a separate goroutine
func (l *ListViewImpl) processTaskUpdate(prop types.PropertyChange) {
	log := logger.GetLogger(l.ctx)
	log.Infof("processTaskUpdate for property change update: %+v", prop)
	taskInfo, ok := prop.Val.(types.TaskInfo)
	if !ok {
		log.Errorf("failed to cast taskInfo for property change update: %+v", prop)
		return
	}
	if taskInfo.State == types.TaskInfoStateQueued || taskInfo.State == types.TaskInfoStateRunning {
		return
	}
	result := TaskResult{}
	taskDetails, ok := l.taskMap.Get(taskInfo.Task)
	if !ok {
		// if vc sends a duplicate success event for a task,
		// and we've already processed an earlier success event for the same task
		// and removed it from our map, we will see this error
		log.Errorf("failed to retrieve receiver channel for task %+v", taskInfo.Task)
		return
	} else if taskInfo.State == types.TaskInfoStateError {
		result.TaskInfo = nil
		result.Err = errors.New(taskInfo.Error.LocalizedMessage)
	} else {
		result.TaskInfo = &taskInfo
		result.Err = nil
	}
	// Use a non-blocking send to prevent deadlocks when multiple goroutines
	// try to send to the same channel (e.g., due to duplicate task updates from vSphere)
	select {
	case taskDetails.ResultCh <- result:
		log.Infof("Successfully sent task result for task %+v", taskInfo.Task)
	default:
		// Channel is full/blocked, which means another goroutine already sent the result
		// This can happen when vSphere sends duplicate task update events
		log.Warnf("result channel full for task %+v, ignoring duplicate update", taskInfo.Task)
	}
}

// RemoveTasksMarkedForDeletion goes over the list of tasks in the map
// and removes tasks that have been marked for deletion
func RemoveTasksMarkedForDeletion(l *ListViewImpl) {
	ctx := logger.NewContextWithLogger(context.Background())
	log := logger.GetLogger(ctx)
	if l.listView == nil {
		log.Errorf("ListView is empty. Will attempt to remove invalid tasks in next attempt. ")
		return
	}
	log.Debugf("pending tasks count before purging: %v", l.taskMap.Count())
	var tasksToDelete []types.ManagedObjectReference
	for _, taskDetails := range l.taskMap.GetAll() {
		if taskDetails.MarkedForRemoval {
			_, err := l.listView.Remove(l.ctx, []types.ManagedObjectReference{taskDetails.Reference})
			if err != nil {
				log.Errorf("failed to remove task from ListView. error: %+v", err)
				continue
			}
			tasksToDelete = append(tasksToDelete, taskDetails.Reference)
		}
	}
	for _, task := range tasksToDelete {
		l.taskMap.Delete(task)
	}
	log.Debugf("pending tasks count after purging: %v", l.taskMap.Count())
}

// MarkTaskForDeletion marks a given task MoRef for deletion by setting a boolean flag in the TaskDetails object
func (l *ListViewImpl) MarkTaskForDeletion(ctx context.Context, taskMoRef types.ManagedObjectReference) error {
	log := logger.GetLogger(ctx)
	taskDetails, ok := l.taskMap.Get(taskMoRef)
	if !ok {
		return logger.LogNewErrorf(log, "failed to retrieve taskDetails for %+v", taskMoRef)
	}
	taskDetails.MarkedForRemoval = true
	l.taskMap.Upsert(taskMoRef, taskDetails)
	log.Infof("%v marked for deletion", taskMoRef)
	return nil
}
