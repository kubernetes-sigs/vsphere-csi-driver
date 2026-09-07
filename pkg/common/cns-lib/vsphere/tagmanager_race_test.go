package vsphere

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vapi/rest"
)

// TestGetTagManagerConcurrentAccess drives concurrent GetTagManager and Connect
// calls against a single, shared VirtualCenter, the way a controller-runtime
// reconciler with MaxConcurrentReconciles > 1 does in production (see
// csinodetopology_controller.go). VirtualCenter is a process-wide singleton, so
// two workers reconciling different objects can call GetTagManager, or trigger a
// reconnect, on the same instance at the same time.
//
// GetTagManager used to cache its result on a vc.tagManager field, written both
// here (unlocked, after Connect released ClientMutex) and inside connect()
// (locked). Run under -race, that was two writers to one field with only one of
// them holding the lock. This test only has teeth under `go test -race`.
func TestGetTagManagerConcurrentAccess(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"race-test\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	model.Cluster = 1
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true // needed for the tag manager's REST endpoints

	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host:     server.URL.Hostname(),
			Port:     port,
			Insecure: true,
			Username: "user", // simulator.DefaultLogin
			Password: "pass",
		},
		ClientMutex: &sync.Mutex{},
	}
	ctx := context.Background()
	require.NoError(t, vc.Connect(ctx))

	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = vc.GetTagManager(ctx)
		}()
		go func() {
			defer wg.Done()
			_ = vc.Connect(ctx)
		}()
	}
	wg.Wait()
}

// TestGetTagManagerConnectsWhenNeverConnected covers a VirtualCenter that has
// never connected. GetTagManager used to reject this upfront with "vCenter not
// initialized", even though the very next line calls Connect, which creates
// vc.Client when it is nil -- exactly this VirtualCenter's condition. The
// check ran before the fix that would have satisfied it.
func TestGetTagManagerConnectsWhenNeverConnected(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"never-connected-test\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	model.Cluster = 1
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true // needed for the tag manager's REST endpoints

	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host:     server.URL.Hostname(),
			Port:     port,
			Insecure: true,
			Username: "user", // simulator.DefaultLogin
			Password: "pass",
		},
		ClientMutex: &sync.Mutex{},
	}
	// vc.Client is deliberately left nil: this VirtualCenter has never connected.

	tagManager, err := vc.GetTagManager(context.Background())
	require.NoError(t, err, "GetTagManager should connect on demand, not reject an unconnected VirtualCenter")
	assert.NotNil(t, tagManager)
}

// TestGetTagManagerRejectsBrokenInnerClient covers a VirtualCenter whose
// Client is non-nil but whose own inner Client (the *vim25.Client) is nil --
// the shape produced by &govmomi.Client{}, which three test fixtures elsewhere
// in this repo construct deliberately (as a lightweight stand-in for
// object.NewDatastore, which does not dereference it). None of those fixtures
// are wired into GetTagManager or Connect today, but if one ever were, Connect
// cannot repair this state: connect() only replaces vc.Client when the outer
// pointer itself is nil, so a non-nil vc.Client with a nil inner Client slips
// past that check and panics inside govmomi's property collector. GetTagManager
// must keep rejecting it before calling Connect.
func TestGetTagManagerRejectsBrokenInnerClient(t *testing.T) {
	vc := &VirtualCenter{
		Config:      &VirtualCenterConfig{Host: "127.0.0.1", Port: 1},
		Client:      &govmomi.Client{}, // non-nil, but its own Client field is nil
		RestClient:  &rest.Client{},
		ClientMutex: &sync.Mutex{},
	}

	_, err := vc.GetTagManager(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "vCenter not initialized")
}
