package vsphere

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
)

func writeGetActiveUserTestConfig(t *testing.T) {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"c\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)
}

// newConnectedVC starts a vcsim instance and returns a VirtualCenter already
// connected to it via simulator.DefaultLogin.
func newConnectedVC(t *testing.T, ctx context.Context) *VirtualCenter {
	t.Helper()
	writeGetActiveUserTestConfig(t)

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: server.URL.Hostname(), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(ctx))
	return vc
}

// TestGetActiveUserSkipsConnectOnTheHappyPath proves GetActiveUser does not
// pay for a second liveness check when the first UserSession call already
// succeeds. Every current caller reaches GetActiveUser right after something
// else (GetVCenter, GetDatacenters, ...) has just called Connect, so a second,
// unconditional Connect here would double an RPC that already just ran.
//
// Connect always takes vc.ClientMutex first. Holding that lock in the test
// turns "GetActiveUser calls Connect on the happy path" into a hang, which is
// a decisive, not just probabilistic, way to prove it does not.
func TestGetActiveUserSkipsConnectOnTheHappyPath(t *testing.T) {
	ctx := context.Background()
	vc := newConnectedVC(t, ctx)

	vc.ClientMutex.Lock()
	defer vc.ClientMutex.Unlock()

	done := make(chan struct{})
	var username string
	var err error
	go func() {
		defer close(done)
		username, err = vc.GetActiveUser(ctx)
	}()

	select {
	case <-done:
		require.NoError(t, err)
		assert.Equal(t, "user", username)
	case <-time.After(3 * time.Second):
		t.Fatal("GetActiveUser blocked on ClientMutex, meaning it called Connect on the happy path")
	}
}

// TestGetActiveUserRecoversWithNoClient covers the case GetActiveUser used to
// hard-fail on: a VirtualCenter that has never connected. Before, this
// returned "client or sessionmanager are nil" immediately, even though
// Connect would have fixed it, since Connect creates a client when none
// exists. Also the case a stale/expired session would fall into on the
// std govmomi #2922 nil-session-nil-error path, since both are recovered
// the same way: by connecting once and retrying.
func TestGetActiveUserRecoversWithNoClient(t *testing.T) {
	writeGetActiveUserTestConfig(t)
	ctx := context.Background()

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: server.URL.Hostname(), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	// vc.Client is deliberately left nil: this VirtualCenter has never connected.

	username, err := vc.GetActiveUser(ctx)
	require.NoError(t, err, "GetActiveUser should recover by connecting, not fail immediately")
	assert.Equal(t, "user", username)
}

// TestActiveUserWithNilSessionManager covers activeUser's other nil-guard
// branch: a non-nil vc.Client whose own SessionManager field is nil. Distinct
// from vc.Client == nil, which GetActiveUser recovers from via Connect --
// there is no live vCenter here to connect to, so this only exercises the
// single-attempt activeUser helper directly.
func TestActiveUserWithNilSessionManager(t *testing.T) {
	vc := &VirtualCenter{
		Client: &govmomi.Client{}, // non-nil, but SessionManager is left nil
	}

	_, err := vc.activeUser(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "client or sessionmanager are nil")
}

// TestActiveUserWithUserSessionError covers UserSession returning a hard RPC
// error (as opposed to the govmomi #2922 nil-session-nil-error case covered
// separately below). Built with a SessionManager pointed at an address
// nothing listens on, so the property-collector call behind UserSession fails
// fast with a connection error.
func TestActiveUserWithUserSessionError(t *testing.T) {
	u, err := soap.ParseURL("https://127.0.0.1:1")
	require.NoError(t, err)
	soapClient := soap.NewClient(u, true)
	// A real handshake (vim25.NewClient) sets RoundTripper and populates
	// ServiceContent; skipped here since the host is unreachable, so both are
	// set by hand. Without RoundTripper, vim25.Client.RoundTrip panics on a nil
	// field instead of attempting the call; without ServiceContent.SessionManager
	// (the well-known SessionManager MOID), session.Manager.Reference() panics
	// dereferencing a nil pointer. Both would fail before the call this test
	// actually wants to observe failing.
	vimClient := &vim25.Client{Client: soapClient, RoundTripper: soapClient}
	vimClient.ServiceContent.SessionManager = &vim25types.ManagedObjectReference{
		Type: "SessionManager", Value: "SessionManager",
	}
	vc := &VirtualCenter{
		Client: &govmomi.Client{
			Client:         vimClient,
			SessionManager: session.NewManager(vimClient),
		},
	}

	_, err = vc.activeUser(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "error getting current user")
}

// TestGetActiveUserRecoversFromExpiredSession drives the actual scenario
// GetActiveUser's retry-via-Connect logic exists for: govmomi issue #2922,
// where SessionManager.UserSession returns a nil session with a nil error
// once a session is no longer authenticated, rather than an error. Logging
// out the underlying client directly (bypassing vc.Connect and
// cleanupVCClient, so vc.Client stays set to the now-invalid client)
// reproduces that exact return value against vcsim.
func TestGetActiveUserRecoversFromExpiredSession(t *testing.T) {
	ctx := context.Background()
	vc := newConnectedVC(t, ctx)

	require.NoError(t, vc.Client.Logout(ctx))

	userSession, err := vc.Client.SessionManager.UserSession(ctx)
	require.NoError(t, err)
	require.Nil(t, userSession, "precondition: UserSession should be (nil, nil) after logout")

	_, activeErr := vc.activeUser(ctx)
	require.Error(t, activeErr, "a single attempt must not treat this as success")
	assert.ErrorContains(t, activeErr, "nil session obtained from session manager")

	username, err := vc.GetActiveUser(ctx)
	require.NoError(t, err, "GetActiveUser should recover via Connect")
	assert.Equal(t, "user", username)
}
