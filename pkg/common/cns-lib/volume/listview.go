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
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// in case of vc connection failure, we wait for a minute
	// to not overwhelm the vc with continuous requests
	waitForUpdatesRetry = 1 * time.Minute
	// Bound serialized vCenter calls so one stalled request cannot block all tasks.
	listViewCallTimeout = 30 * time.Second
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
	// viewMu serializes task registration/removal with view replacement and
	// cleanup, so an operation cannot modify an obsolete ListView generation.
	viewMu contextMutex
	// isReady defines the ready state of the listview + property collector mechanism
	isReady bool
	// client is the vim25 client the current ListView generation was built on,
	// cached here under mu so the request path never has to reach into the
	// VirtualCenter for it. Reading vc.Client needs the VirtualCenter's
	// ClientMutex, which a login holds for as long as the login takes, and
	// AddTask/RemoveTask reach the client while holding viewMu -- so a stalled
	// login would block registration and cleanup however short their own
	// deadlines were. The listen loop refreshes this after each connect.
	client *vim25.Client
}

// contextMutex permits callers waiting for ListView serialization to stop when
// their context ends. Its zero value is ready for use.
type contextMutex struct {
	once sync.Once
	ch   chan struct{}
}

func (m *contextMutex) Lock(ctx context.Context) error {
	m.once.Do(func() { m.ch = make(chan struct{}, 1) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.ch <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.ch
			return err
		}
		return nil
	}
}

func (m *contextMutex) Unlock() { <-m.ch }

// TaskDetails is used to hold state for a task
type TaskDetails struct {
	// channel to return results. the caller (CSI op) is waiting on this channel
	ResultCh  chan TaskResult
	Reference types.ManagedObjectReference
	// MarkedForRemoval helps in retrying the removal of tasks in case of failures
	MarkedForRemoval bool
}

type TaskResult struct {
	TaskInfo *types.TaskInfo
	Err      error
}

var ErrListViewTaskAddition = errors.New("failure to add task to listview")
var ErrSessionNotAuthenticated = errors.New("session is not authenticated")

// ErrTaskMonitoring indicates that the task outcome is unknown because the
// driver could not observe it. It is not a vCenter task failure.
var ErrTaskMonitoring = errors.New("task monitoring interrupted")

// ErrTaskFailed is emitted only for a terminal vCenter task error.
var ErrTaskFailed = errors.New("vCenter task failed")

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

// vc returns the current VirtualCenter. ResetVirtualCenter swaps the pointer
// under l.mu, so the read is guarded -- but only the read: anything slow done
// with the result (a login, a session check, a property collector call) must
// happen after this returns, never with l.mu held. See connect.
func (l *ListViewImpl) vc() *cnsvsphere.VirtualCenter {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.virtualCenter
}

// setClient records the vim25 client the ListView is being (re)built on.
func (l *ListViewImpl) setClient(client *vim25.Client) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.client = client
}

// getClient returns the cached vim25 client. The lock is only ever held for
// this read, never across a call that uses the result.
func (l *ListViewImpl) getClient() *vim25.Client {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.client
}

// createListView connects to vCenter and builds the ListView object. Callers
// that have already connected should call newListView instead -- in particular
// anything holding viewMu or l.mu, which must not be held across a login.
func (l *ListViewImpl) createListView(ctx context.Context, tasks []types.ManagedObjectReference) error {
	log := logger.GetLogger(ctx)
	vc := l.vc()
	if err := vc.Connect(ctx); err != nil {
		return logger.LogNewErrorf(log, "failed to create a ListView. error: %+v", err)
	} else {
		log.Debugf("connection to vc successful")
	}
	client := vc.VimClient()
	if client == nil {
		return logger.LogNewError(log, "failed to create a ListView. error: vCenter client is nil after connect")
	}
	l.setClient(client)
	return l.newListView(ctx, client, tasks)
}

// newListView builds the ListView object on an already connected client.
func (l *ListViewImpl) newListView(ctx context.Context, client *vim25.Client,
	tasks []types.ManagedObjectReference) error {
	log := logger.GetLogger(ctx)
	// doing a direct assignment to t.listView in case of failure
	// leads to NPE while accessing listView elsewhere
	listView, err := view.NewManager(client).CreateListView(ctx, tasks)
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
	// Read the new VirtualCenter's client before taking l.mu: VimClient waits
	// on ClientMutex, and l.mu must not be held across that wait.
	client := virtualCenter.VimClient()
	log.Info("attempting to acquire lock before updating vc object")
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Info("acquired lock before updating vc object")
	l.virtualCenter = virtualCenter
	l.client = client
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
	log := logger.GetLogger(ctx)
	// The cached client, and no lock held across the round trip that follows.
	// Holding l.mu across it -- or reaching through VimClient for ClientMutex --
	// is what let one slow call block every AddTask and RemoveTask queued on
	// viewMu behind this one.
	client := l.getClient()
	if client == nil {
		return false
	}
	// If session hasn't expired, nothing to do.
	sessionMgr := session.NewManager(client)
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
	ctx, cancel := context.WithTimeout(ctx, listViewCallTimeout)
	defer cancel()
	if err := l.viewMu.Lock(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrTaskMonitoring, err)
	}
	defer l.viewMu.Unlock()
	log := logger.GetLogger(ctx)
	log.Infof("AddTask called for %+v", taskMoRef)

	if !l.IsListViewReady() {
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrTaskMonitoring, err)
	}

	if !l.isSessionValid(ctx) {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrTaskMonitoring, ctx.Err())
		}
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

	response, err := l.listView.Add(ctx, []types.ManagedObjectReference{taskMoRef})
	if err != nil {
		l.taskMap.Delete(taskMoRef)
		// An interrupted Add has an ambiguous remote outcome. Recreate the
		// view to remove any registration accepted before cancellation.
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
	// Cleanup must still be attempted after the request has been canceled, but
	// neither the lock nor the vCenter calls may block indefinitely.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), listViewCallTimeout)
	defer cancel()
	if err := l.viewMu.Lock(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrTaskMonitoring, err)
	}
	defer l.viewMu.Unlock()
	log := logger.GetLogger(ctx)

	if !l.IsListViewReady() {
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}

	if !l.isSessionValid(ctx) {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrTaskMonitoring, ctx.Err())
		}
		log.Infof("current session is not valid")
		l.SetListViewNotReady(ctx)
		return fmt.Errorf("%w. task: %v, err: listview not ready", ErrListViewTaskAddition, taskMoRef)
	}

	log.Infof("client is valid. trying to remove task from listview object")
	_, err := l.listView.Remove(ctx, []types.ManagedObjectReference{taskMoRef})
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

// connect establishes the vCenter session the ListView rides on.
//
// l.mu is deliberately not held here. l.mu guards ListView state (isReady, the
// listView object, the waitForUpdates context); the connection is guarded by
// the VirtualCenter's own ClientMutex, which Connect takes. Holding l.mu as
// well put a lock every AddTask and RemoveTask needs -- while they hold viewMu
// -- behind however long a login takes, so the deadline those calls set for
// themselves bounded nothing.
func (l *ListViewImpl) connect() error {
	ctx, cancel := context.WithTimeout(l.ctx, listViewCallTimeout)
	defer cancel()
	return l.vc().Connect(ctx)
}

// listenToTaskUpdates is a long-running goroutine
// that uses a property collector to listen for task updates
// CSI ops add CNS tasks to listview and wait for a response from CNS
// when update(s) are received by the property collector,
// it spawns a new goroutine to process each task update and return the result to the caller
func (l *ListViewImpl) listenToTaskUpdates() {
	log := logger.GetLogger(l.ctx)
	filter := getListViewWaitFilter(l.listView)
	l.mu.Lock()
	l.waitForUpdatesContext, l.waitForUpdatesCancelFunc = context.WithCancel(context.Background())
	l.mu.Unlock()
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

		// Read the client before taking any ListView lock: VimClient waits on
		// the VirtualCenter's ClientMutex, and nothing below may hold viewMu or
		// l.mu while waiting on a connection.
		vimClient := l.vc().VimClient()
		if vimClient == nil {
			log.Error("vCenter client is nil after a successful connect")
			time.Sleep(waitForUpdatesRetry)
			continue
		}
		l.setClient(vimClient)

		log.Infof("attempting lock before re-creating listview")
		setupCtx, setupCancel := context.WithTimeout(l.ctx, listViewCallTimeout)
		if err := l.viewMu.Lock(setupCtx); err != nil {
			setupCancel()
			log.Errorf("failed to acquire listview lock: %v", err)
			continue
		}
		l.mu.Lock()
		log.Infof("acquired lock before re-creating listview")
		if recreateView {
			if l.listView != nil {
				destroyListviewErr := l.listView.Destroy(setupCtx)
				if destroyListviewErr != nil {
					// ignoring the error and re-creating the list view
					log.Errorf("failed to destroy listview object. err: %v", destroyListviewErr)
				} else {
					log.Info("successfully destroyed existing listview")
				}
			}
			log.Info("re-creating the listView object")
			// newListView, not createListView: the loop connected above, and
			// re-connecting here would do it with viewMu and l.mu held.
			err := l.newListView(setupCtx, vimClient, nil)
			if err != nil {
				log.Errorf("failed to create a ListView. error: %+v", err)
				l.mu.Unlock()
				l.viewMu.Unlock()
				setupCancel()
				continue
			}
			log.Info("successfully created listview")

			filter = getListViewWaitFilter(l.listView)
			l.waitForUpdatesContext, l.waitForUpdatesCancelFunc = context.WithCancel(context.Background())
			recreateView = false
		}

		log.Info("Starting listening for task updates...")
		log.Infof("waitForUpdatesContext %v", l.waitForUpdatesContext)
		pc, pcErr := property.DefaultCollector(vimClient).Create(setupCtx)
		if pcErr != nil {
			log.Errorf("failed to create PropertyCollector for WaitForUpdatesEx for ListView. error: %+v", pcErr)
			l.mu.Unlock()
			l.viewMu.Unlock()
			setupCancel()
			continue
		}
		l.isReady = true
		log.Infof("listview ready state is %v", l.isReady)
		l.mu.Unlock()
		l.viewMu.Unlock()
		setupCancel()
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
		destroyCtx, destroyCancel := context.WithTimeout(context.Background(), listViewCallTimeout)
		_ = pc.Destroy(destroyCtx)
		destroyCancel()

		// if property collector returns any errors,
		// we want to immediately return a fault for all the pending tasks in the map
		// note: this is not a task error but an error from the vc
		if err != nil {
			log.Errorf("WaitForUpdates returned err: %v for vc: %+v", err,
				l.virtualCenter.Config.Host)
			recreateView = true
			if err := l.viewMu.Lock(l.ctx); err != nil {
				log.Errorf("failed to acquire listview lock after collector failure: %v", err)
				return
			}
			log.Info("waiting for lock before setting listview ready state to false")
			l.mu.Lock()
			log.Info("acquired lock before setting listview ready state to false")
			log.Infof("setting listview ready state to false. current ready state: %v", l.isReady)
			l.isReady = false
			log.Infof("listview ready state is %v", l.isReady)
			l.mu.Unlock()
			l.reportErrorOnAllPendingTasks(err)
			l.viewMu.Unlock()
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
			Err:      fmt.Errorf("%w: %w", ErrTaskMonitoring, err),
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
		result.Err = fmt.Errorf("%w: %s", ErrTaskFailed, taskInfo.Error.LocalizedMessage)
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
	ctx, cancel := context.WithTimeout(logger.NewContextWithLogger(context.Background()), listViewCallTimeout)
	defer cancel()
	if err := l.viewMu.Lock(ctx); err != nil {
		return
	}
	defer l.viewMu.Unlock()
	log := logger.GetLogger(ctx)
	if l.listView == nil {
		log.Errorf("ListView is empty. Will attempt to remove invalid tasks in next attempt. ")
		return
	}
	log.Debugf("pending tasks count before purging: %v", l.taskMap.Count())
	var tasksToDelete []types.ManagedObjectReference
	for _, taskDetails := range l.taskMap.GetAll() {
		if taskDetails.MarkedForRemoval {
			if ctx.Err() != nil {
				break
			}
			_, err := l.listView.Remove(ctx, []types.ManagedObjectReference{taskDetails.Reference})
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
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), listViewCallTimeout)
	defer cancel()
	if err := l.viewMu.Lock(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrTaskMonitoring, err)
	}
	defer l.viewMu.Unlock()
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
