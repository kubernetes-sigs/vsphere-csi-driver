package volume

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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
	// govmomiClient: separate client created with http.Client.Timeout set to 0
	govmomiClient *govmomi.Client
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
func NewListViewImpl(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter,
	client *govmomi.Client) (*ListViewImpl, error) {
	log := logger.GetLogger(ctx)
	t := &ListViewImpl{
		taskMap:       NewTaskMap(),
		virtualCenter: virtualCenter,
		ctx:           ctx,
		govmomiClient: client,
	}
	err := t.createListView(ctx, nil)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to create a ListView. error: %+v", err)
	}
	go t.listenToTaskUpdates()
	return t, nil
}

func (l *ListViewImpl) createListView(ctx context.Context, tasks []types.ManagedObjectReference) error {
	log := logger.GetLogger(ctx)
	var err error
	// doing an assignment to t.listView at line 91 in case of failure
	// leads to NPE while accessing listView elsewhere
	listView, err := view.NewManager(l.govmomiClient.Client).CreateListView(ctx, tasks)
	if err != nil {
		return err
	}
	l.listView = listView
	log.Infof("created listView object %+v for virtualCenter: %+v",
		l.listView.Reference(), l.virtualCenter.Config.Host)
	return nil
}

// SetVirtualCenter is a setter method for vc. use case: ReloadConfiguration
func (l *ListViewImpl) SetVirtualCenter(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) {
	log := logger.GetLogger(ctx)
	l.virtualCenter = virtualCenter
	log.Infof("updated VirtualCenter object reference in ListView")
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

// AddTask adds task to listView and the internal map
func (l *ListViewImpl) AddTask(ctx context.Context, taskMoRef types.ManagedObjectReference, ch chan TaskResult) error {
	log := logger.GetLogger(ctx)
	log.Infof("AddTask called for %+v", taskMoRef)

	if err := l.isClientValid(false); err != nil {
		return fmt.Errorf("%w. task: %v, err: %v", ErrListViewTaskAddition, taskMoRef, err)
	} else {
		log.Debugf("connection to vc successful")
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
	if l.listView == nil {
		return logger.LogNewErrorf(log, "failed to remove task from listView: listView not initialized")
	}
	if err := l.isClientValid(false); err != nil {
		return logger.LogNewErrorf(log, "failed to remove task %v from ListView. error: %+v", taskMoRef, err)
	} else {
		log.Debugf("connection to vc successful")
	}
	log.Infof("client is valid. trying to remove task from listview object")
	_, err := l.listView.Remove(l.ctx, []types.ManagedObjectReference{taskMoRef})
	if err != nil {
		return logger.LogNewErrorf(log, "failed to remove task %v from ListView. error: %+v", taskMoRef, err)
	}
	log.Infof("task %+v removed from listView", taskMoRef)
	l.taskMap.Delete(taskMoRef)
	log.Debugf("task %+v removed from map", taskMoRef)
	return nil
}

// The re-connect param is set to false for calls from AddTask() and RemoveTask()
// as we want the re-connection to happen via the call from listenToTaskUpdates() method
// as it will also re-create the ListView object which is what we want to do.
func (l *ListViewImpl) isClientValid(reconnect bool) error {
	log := logger.GetLogger(l.ctx)
	if !reconnect {
		l.mu.RLock()
		defer l.mu.RUnlock()
	} else {
		l.mu.Lock()
		defer l.mu.Unlock()
	}
	// If session hasn't expired, nothing to do.
	sessionMgr := session.NewManager(l.govmomiClient.Client)
	// SessionMgr.UserSession(ctx) retrieves and returns the SessionManager's
	// CurrentSession field. Nil is returned if the session is not
	// authenticated or timed out.
	if userSession, err := sessionMgr.UserSession(l.ctx); err != nil {
		log.Errorf("failed to obtain user session with err: %v", err)
	} else if userSession != nil {
		return nil
	}

	log.Infof("current session is either nil or not authenticated")

	if !reconnect {
		l.waitForUpdatesCancelFunc()
		return ErrSessionNotAuthenticated
	}

	log.Infof("creating a new session...")

	err := cnsvsphere.ReadVCConfigs(l.ctx, l.virtualCenter)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to read VC config. err: %v", err)
	}
	// If session has expired, create a new instance.
	useragent, err := config.GetSessionUserAgent(l.ctx)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to get useragent for vCenter session. error: %+v", err)
	}
	useragent = useragent + "-listview"
	client, err := l.virtualCenter.NewClient(l.ctx, useragent)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to create a govmomi client for listView. error: %+v", err)
	}
	client.Timeout = noTimeout
	l.govmomiClient = client
	log.Infof("successfully created new VC session")
	return nil
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
		if err := l.isClientValid(true); err != nil {
			log.Errorf("failed to connect to vCenter. err: %v", err)
			time.Sleep(waitForUpdatesRetry)
			continue
		} else {
			log.Infof("connection to vc successful")
		}

		if recreateView {
			log.Info("re-creating the listView object")
			err := l.createListView(l.ctx, nil)
			if err != nil {
				log.Errorf("failed to create a ListView. error: %+v", err)
				continue
			}

			filter = getListViewWaitFilter(l.listView)
			l.waitForUpdatesContext, l.waitForUpdatesCancelFunc = context.WithCancel(context.Background())
			recreateView = false
		}

		log.Info("Starting listening for task updates...")
		pc := property.DefaultCollector(l.govmomiClient.Client)
		err := property.WaitForUpdates(l.waitForUpdatesContext, pc, filter, func(updates []types.ObjectUpdate) bool {
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
		// if property collector returns any errors,
		// we want to immediately return a fault for all the pending tasks in the map
		// note: this is not a task error but an error from the vc
		if err != nil {
			log.Errorf("WaitForUpdates returned err: %v for vc: %+v", err,
				l.virtualCenter.Config.Host)
			recreateView = true
			l.reportErrorOnAllPendingTasks(err)
		}
		// use case: unit tests: this will help us stop listening
		// and finish the unit test
		if l.shouldStopListening {
			return
		}
	}
}

// reportErrorOnAllPendingTasks returns failure to all pending tasks in the map in case of vc failure
func (l *ListViewImpl) reportErrorOnAllPendingTasks(err error) {
	for _, taskDetails := range l.taskMap.GetAll() {
		result := TaskResult{
			TaskInfo: nil,
			Err:      err,
		}
		taskDetails.ResultCh <- result
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
		log.Errorf("failed to retrieve receiver channel for task %+v", taskInfo.Task)
		return
	} else if taskInfo.State == types.TaskInfoStateError {
		result.TaskInfo = nil
		result.Err = errors.New(taskInfo.Error.LocalizedMessage)
	} else {
		result.TaskInfo = &taskInfo
		result.Err = nil
	}

	taskDetails.ResultCh <- result
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

// LogoutSession is a setter method to logout vcenter session created
func (l *ListViewImpl) LogoutSession(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	err := l.govmomiClient.Logout(l.ctx)
	if err != nil {
		log.Errorf("Error while logout vCenter session (list-view) for host %s, Error: %+v", l.virtualCenter.Config.Host, err)
		return err
	}
	log.Infof("Logged out list-view vCenter session for host %s", l.virtualCenter.Config.Host)
	return nil
}
