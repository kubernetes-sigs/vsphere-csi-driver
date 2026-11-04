/*
Copyright 2026 The Kubernetes Authors.

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// hangingLoginTransport never answers, standing in for a vCenter that accepts
// the connection and then goes quiet. NewClient sets soap.Client.Timeout to 0,
// so in production this hangs for as long as the peer keeps the socket open --
// all of it inside Connect, holding the VirtualCenter's ClientMutex.
type hangingLoginTransport struct{ started chan struct{} }

func (h *hangingLoginTransport) RoundTrip(ctx context.Context, _, _ soap.HasFault) error {
	select {
	case <-h.started:
	default:
		close(h.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestStalledLoginDoesNotBlockTaskRegistration covers the lock chain the 30s
// deadlines on AddTask and RemoveTask were supposed to bound but did not.
// connect() used to hold l.mu for the whole login, and AddTask reads the ready
// state -- while holding viewMu -- so a single stalled login stopped every
// registration, cleanup and recovery in the driver, each one blowing through
// its own deadline on a lock that does not take a context.
func TestStalledLoginDoesNotBlockTaskRegistration(t *testing.T) {
	transport := &hangingLoginTransport{started: make(chan struct{})}
	endpoint, err := soap.ParseURL("https://127.0.0.1:1/sdk")
	require.NoError(t, err)
	// A real soap.Client underneath (the teardown path closes idle
	// connections on it) with the stalling RoundTripper layered over it.
	client := &vim25.Client{Client: soap.NewClient(endpoint, true),
		RoundTripper: transport, ServiceContent: types.ServiceContent{
			SessionManager:    &types.ManagedObjectReference{Type: "SessionManager", Value: "session"},
			PropertyCollector: types.ManagedObjectReference{Type: "PropertyCollector", Value: "collector"},
		}}
	vc := &cnsvsphere.VirtualCenter{
		Config:      &cnsvsphere.VirtualCenterConfig{},
		Client:      &govmomi.Client{Client: client, SessionManager: session.NewManager(client)},
		ClientMutex: &sync.Mutex{},
	}
	connectCtx, cancelConnect := context.WithCancel(context.Background())
	defer cancelConnect()
	l := &ListViewImpl{ctx: logger.NewContextWithLogger(connectCtx), isReady: true,
		taskMap: NewTaskMap(), virtualCenter: vc, client: client}

	connectDone := make(chan error, 1)
	go func() { connectDone <- l.connect() }()
	<-transport.started

	// The login is in flight and holding ClientMutex. Everything the request
	// path touches must still answer.
	ready := make(chan bool, 1)
	go func() { ready <- l.IsListViewReady() }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("IsListViewReady blocked behind an in-flight login")
	}

	// AddTask must reach its own deadline rather than the login's. Its context
	// is already canceled, so it has to come back immediately.
	addCtx, cancelAdd := context.WithCancel(logger.NewContextWithLogger(context.Background()))
	cancelAdd()
	added := make(chan error, 1)
	go func() {
		added <- l.AddTask(addCtx, types.ManagedObjectReference{Type: "Task", Value: "task-1"},
			make(chan TaskResult, 1))
	}()
	select {
	case err := <-added:
		require.ErrorIs(t, err, ErrTaskMonitoring)
	case <-time.After(5 * time.Second):
		t.Fatal("AddTask blocked behind an in-flight login")
	}

	// And the lock it takes is free again, so the next caller is not stuck
	// behind a holder that is itself stuck on the login.
	require.NoError(t, l.viewMu.Lock(context.Background()))
	l.viewMu.Unlock()

	cancelConnect()
	select {
	case <-connectDone:
	case <-time.After(5 * time.Second):
		t.Fatal("connect did not unwind after its context was canceled")
	}
}
