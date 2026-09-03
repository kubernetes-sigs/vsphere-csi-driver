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

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"

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
			Host: commontypes.NewFQDN(server.URL.Hostname()), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(ctx))
	return vc
}

// TestGetActiveUserSkipsConnect proves GetActiveUser never calls Connect.
// Every caller reaches it right after something else (GetVCenter, GetVCenters,
// GetDatacenters, ...) has just called Connect, so connecting here would double
// an RPC that already just ran.
//
// Connect always takes vc.ClientMutex first. Holding that lock in the test
// turns "GetActiveUser calls Connect" into a hang, which is a decisive, not
// just probabilistic, way to prove it does not.
func TestGetActiveUserSkipsConnect(t *testing.T) {
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

// TestGetActiveUserFailsWithNoClient covers a VirtualCenter that has never
// connected. GetActiveUser reports this rather than connecting itself; callers
// reach it only after Connect, so a nil client here means something upstream
// already went wrong, and the caller's retry is what re-establishes a session.
func TestGetActiveUserFailsWithNoClient(t *testing.T) {
	// vc.Client is deliberately left nil: this VirtualCenter has never connected.
	vc := &VirtualCenter{Config: &VirtualCenterConfig{}}

	_, err := vc.GetActiveUser(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "client or sessionmanager are nil")
}

// TestGetActiveUserWithNilSessionManager covers the other half of the same
// nil-guard: a non-nil vc.Client whose own SessionManager field is nil.
func TestGetActiveUserWithNilSessionManager(t *testing.T) {
	vc := &VirtualCenter{
		Client: &govmomi.Client{}, // non-nil, but SessionManager is left nil
	}

	_, err := vc.GetActiveUser(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "client or sessionmanager are nil")
}

// TestGetActiveUserWithUserSessionError covers UserSession returning a hard RPC
// error (as opposed to the govmomi #2922 nil-session-nil-error case covered
// separately below). Built with a SessionManager pointed at an address
// nothing listens on, so the property-collector call behind UserSession fails
// fast with a connection error.
func TestGetActiveUserWithUserSessionError(t *testing.T) {
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

	_, err = vc.GetActiveUser(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "error getting current user")
}

// TestGetActiveUserFailsOnExpiredSession covers govmomi issue #2922, where
// SessionManager.UserSession returns a nil session with a nil error once a
// session is no longer authenticated, rather than an error. Without the
// nil-session guard that would be reported as a successful lookup of an empty
// username, which would then be written into CNS volume metadata or passed to
// a privilege check. Logging out the underlying client directly (bypassing
// vc.Connect and cleanupVCClient, so vc.Client stays set to the now-invalid
// client) reproduces that exact return value against vcsim.
func TestGetActiveUserFailsOnExpiredSession(t *testing.T) {
	ctx := context.Background()
	vc := newConnectedVC(t, ctx)

	require.NoError(t, vc.Client.Logout(ctx))

	userSession, err := vc.Client.SessionManager.UserSession(ctx)
	require.NoError(t, err)
	require.Nil(t, userSession, "precondition: UserSession should be (nil, nil) after logout")

	_, err = vc.GetActiveUser(ctx)
	require.Error(t, err, "an expired session must not be reported as success")
	assert.ErrorContains(t, err, "nil session obtained from session manager")
}
