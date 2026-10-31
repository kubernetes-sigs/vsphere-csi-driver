package vsphere

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
)

// brokenRestClient returns a rest.Client pointed at an address nothing listens
// on, so any call it makes fails fast with a connection error. Used to prove
// whether connect() actually invokes RestClient.Session, without needing to
// intercept traffic to a live server.
func brokenRestClient(t *testing.T) *rest.Client {
	t.Helper()
	u, err := soap.ParseURL("https://127.0.0.1:1")
	require.NoError(t, err)
	soapClient := soap.NewClient(u, true)
	vimClient, err := vim25.NewClient(context.Background(), soapClient)
	// vim25.NewClient itself may fail to reach the (unreachable) host for its
	// initial ServiceContent fetch; either way the resulting client's rest
	// wrapper will fail identically when asked to make a call.
	if err != nil {
		vimClient = &vim25.Client{Client: soapClient}
	}
	return rest.NewClient(vimClient)
}

// TestConnectSkipsRedundantRestSessionCheckForSessionManager covers the
// connect() fast path: once vc.Client and vc.RestClient already exist, it
// checks both sessions are still valid before deciding not to re-login. For
// credential-based auth (username/password, cert) the SOAP and REST sessions
// are two independent logins, so both must be checked. For the shared session
// manager, the REST session ID is the SOAP session's own cookie (see login()),
// so they are the same vCenter session object; checking the REST session
// again is a redundant network call connect() should skip.
//
// The session-manager case is proven by swapping in a RestClient that fails any
// call it makes: a connect() that skipped the check leaves that broken client in
// place, untouched. The credential cases then cover what connect() does when it
// does look and finds the rest session unusable -- it re-logins the rest client
// in place, and leaves the SOAP session and the clients built on it alone,
// whether or not that rest re-login succeeds. An unusable rest session is
// deliberately not fatal to connect(), since failing there would tear down a
// SOAP session already known to be healthy.
func TestConnectSkipsRedundantRestSessionCheckForSessionManager(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"c\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("session-manager auth: skips the redundant rest session check", func(t *testing.T) {
		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Host: server.URL.Hostname(), Port: port, Insecure: true,
				VCSessionManagerURL:   sessionManagerFor(t, ctx, server.URL),
				VCSessionManagerToken: "a-token",
			},
			ClientMutex: &sync.Mutex{},
		}
		require.NoError(t, vc.Connect(ctx), "initial connect should succeed")

		// Force any real call on the rest client to fail.
		broken := brokenRestClient(t)
		vc.RestClient = broken

		err := vc.connect(ctx)
		assert.NoError(t, err,
			"connect() should not have needed the rest client at all in session-manager mode")
		assert.Same(t, broken, vc.RestClient,
			"connect() should have left the rest client untouched, proving it never consulted it")
	})

	t.Run("credential auth: repairs the rest session without touching the soap session", func(t *testing.T) {
		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Host: server.URL.Hostname(), Port: port, Insecure: true,
				Username: "user", Password: "pass", // simulator.DefaultLogin
			},
			ClientMutex: &sync.Mutex{},
		}
		require.NoError(t, vc.Connect(ctx), "initial connect should succeed")

		soapClient, restClient := vc.Client, vc.RestClient
		soapSession, err := soapClient.SessionManager.UserSession(ctx)
		require.NoError(t, err)
		require.NotNil(t, soapSession)

		// Drop only the rest session, leaving the SOAP session live: this is
		// the shape of a vAPI outage, where the rest login is the only half
		// that is unusable.
		require.NoError(t, restClient.Logout(ctx))
		staleRestSession, err := restClient.Session(ctx)
		require.NoError(t, err)
		require.Nil(t, staleRestSession, "precondition: rest session should read as gone")

		require.NoError(t, vc.connect(ctx),
			"an unusable rest session should be recovered, not returned as a connect() failure")

		// The healthy SOAP session, and everything built on it, is left alone.
		assert.Same(t, soapClient, vc.Client,
			"connect() should not have recreated a SOAP session that was still valid")
		newSoapSession, err := vc.Client.SessionManager.UserSession(ctx)
		require.NoError(t, err)
		require.NotNil(t, newSoapSession, "the SOAP session should still be live")
		assert.Equal(t, soapSession.Key, newSoapSession.Key,
			"the SOAP session should be the same one, not a re-login")

		// And the rest client was re-authenticated in place.
		assert.Same(t, restClient, vc.RestClient,
			"connect() should re-login the existing rest client rather than replacing it")
		restSession, err := vc.RestClient.Session(ctx)
		require.NoError(t, err)
		assert.NotNil(t, restSession, "the rest client should hold a live session again")
	})

	t.Run("credential auth: a rest login that keeps failing leaves the soap session up", func(t *testing.T) {
		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Host: server.URL.Hostname(), Port: port, Insecure: true,
				Username: "user", Password: "pass", // simulator.DefaultLogin
			},
			ClientMutex: &sync.Mutex{},
		}
		require.NoError(t, vc.Connect(ctx), "initial connect should succeed")

		soapClient := vc.Client
		soapSession, err := soapClient.SessionManager.UserSession(ctx)
		require.NoError(t, err)
		require.NotNil(t, soapSession)

		// A rest client that fails every call, including its re-login: vAPI is
		// down for the whole of this connect().
		broken := brokenRestClient(t)
		vc.RestClient = broken

		require.NoError(t, vc.connect(ctx),
			"a rest client that cannot be re-logged-in should not fail connect()")

		// This is the loop being avoided: repeated connect() calls during a vAPI
		// outage must not keep logging out and rebuilding a working SOAP session.
		for range 3 {
			require.NoError(t, vc.connect(ctx))
		}
		assert.Same(t, soapClient, vc.Client,
			"a vAPI outage should never tear down the SOAP client")
		liveSession, err := vc.Client.SessionManager.UserSession(ctx)
		require.NoError(t, err)
		require.NotNil(t, liveSession, "the SOAP session should still be live")
		assert.Equal(t, soapSession.Key, liveSession.Key,
			"the SOAP session should be the original one, never logged out and re-created")
	})
}

// sessionManagerFor starts a minimal session manager that hands out a real
// clone ticket from the given vcsim server, and returns its URL.
func sessionManagerFor(t *testing.T, ctx context.Context, vcsimURL *url.URL) string {
	t.Helper()
	ticketClient, err := govmomi.NewClient(ctx, vcsimURL, true)
	require.NoError(t, err)

	sessionManager := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ticket, err := ticketClient.SessionManager.AcquireCloneTicket(ctx)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(SharedSessionResponse{Token: ticket})
		}))
	t.Cleanup(sessionManager.Close)
	return sessionManager.URL
}

// TestConnectRecreatesClientsAfterSessionExpiry drives connect()'s dual-logout
// path: an existing session that has expired, as opposed to a VirtualCenter
// that never connected (vc.Client == nil, handled earlier and separately in
// connect()). Logging out the underlying client directly -- bypassing
// vc.Connect and cleanupVCClient, so vc.Client and vc.RestClient stay set to
// now-invalid clients -- reproduces an expired session against vcsim.
func TestConnectRecreatesClientsAfterSessionExpiry(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"c\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	ctx := context.Background()
	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: server.URL.Hostname(), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(ctx))

	staleClient, staleRestClient := vc.Client, vc.RestClient
	require.NoError(t, staleClient.Logout(ctx))

	// Precondition: connect()'s fast path ("no need to re-login") must not
	// apply here, or the rest of this test would be exercising the wrong branch.
	userSession, err := staleClient.SessionManager.UserSession(ctx)
	require.NoError(t, err)
	require.Nil(t, userSession, "precondition: session should read as expired after logout")

	require.NoError(t, vc.Connect(ctx), "connect() should recover from an expired session")

	assert.NotSame(t, staleClient, vc.Client,
		"expired session should be replaced with a freshly logged-in client")
	assert.NotSame(t, staleRestClient, vc.RestClient,
		"expired session should replace the rest client too")

	newUserSession, err := vc.Client.SessionManager.UserSession(ctx)
	require.NoError(t, err)
	require.NotNil(t, newUserSession, "the new client should have a live session")
	assert.Equal(t, "user", newUserSession.UserName)
}
