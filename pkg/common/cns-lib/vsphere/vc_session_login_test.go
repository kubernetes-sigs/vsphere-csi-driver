package vsphere

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/session"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
)

// writeSessionTestConfig points config.GetConfig at a throwaway file, which
// VirtualCenter.connect needs in order to build the session user agent.
func writeSessionTestConfig(t *testing.T) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	contents := "[Global]\ncluster-id = \"session-test\"\n\n" +
		"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n" +
		"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"
	require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", path)
}

func TestSoapSessionID(t *testing.T) {
	t.Run("should fail when the client has no session cookie", func(t *testing.T) {
		// A client that has never logged in has an empty cookie jar, so
		// SessionCookie returns nil. Dereferencing it would panic.
		url, err := soap.ParseURL("https://vcenter.invalid")
		require.NoError(t, err)
		soapClient := soap.NewClient(url, true)
		vimClient := &vim25.Client{Client: soapClient}
		client := &govmomi.Client{
			Client:         vimClient,
			SessionManager: session.NewManager(vimClient),
		}
		require.Nil(t, client.SessionCookie(), "precondition: no session cookie")

		_, err = soapSessionID(client)
		require.Error(t, err)
		assert.ErrorContains(t, err, "no vCenter session cookie")
	})
}

// newSharedSessionVC starts a vcsim instance and a session manager stub that
// hands out real clone tickets for it, and returns a VirtualCenter configured to
// authenticate through that session manager.
func newSharedSessionVC(t *testing.T) *VirtualCenter {
	t.Helper()
	ctx := context.Background()

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	model.Service.RegisterEndpoints = true

	vcsim := model.Service.NewServer()
	t.Cleanup(vcsim.Close)

	ticketClient, err := govmomi.NewClient(ctx, vcsim.URL, true)
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

	port, err := strconv.Atoi(vcsim.URL.Port())
	require.NoError(t, err)

	return &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host:                  vcsim.URL.Hostname(),
			Port:                  port,
			Insecure:              true,
			VCSessionManagerURL:   sessionManager.URL,
			VCSessionManagerToken: "a-session-manager-token",
		},
		ClientMutex: &sync.Mutex{},
	}
}

// TestNewClientWithSessionManager covers the shared session login end to end:
// fetch a token from the session manager, clone the vCenter session with it, and
// carry that session over to the rest client.
func TestNewClientWithSessionManager(t *testing.T) {
	writeSessionTestConfig(t)
	ctx := context.Background()

	t.Run("should authenticate through the session manager", func(t *testing.T) {
		vc := newSharedSessionVC(t)

		client, restClient, err := vc.NewClient(ctx, "session-manager-test")
		require.NoError(t, err)
		require.NotNil(t, client)
		require.NotNil(t, restClient)

		// The cloned session must be a real, authenticated vCenter session.
		userSession, err := client.SessionManager.UserSession(ctx)
		require.NoError(t, err)
		require.NotNil(t, userSession, "clone should have produced a live session")

		// And the rest client must have picked up that same session.
		assert.Equal(t, client.SessionCookie().Value, restClient.SessionID(),
			"rest client should ride on the cloned soap session")
	})

	t.Run("should fail when the session manager rejects the request", func(t *testing.T) {
		vc := newSharedSessionVC(t)
		vc.Config.VCSessionManagerURL = "https://127.0.0.1:1/session"

		_, _, err := vc.NewClient(ctx, "session-manager-test")
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed calling vc session manager")
	})

	t.Run("should fail when the clone ticket is not accepted", func(t *testing.T) {
		vc := newSharedSessionVC(t)
		badTicket := httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(SharedSessionResponse{Token: "not-a-real-ticket"})
			}))
		t.Cleanup(badTicket.Close)
		vc.Config.VCSessionManagerURL = badTicket.URL

		_, _, err := vc.NewClient(ctx, "session-manager-test")
		require.Error(t, err)
		assert.ErrorContains(t, err, "Login failure")
	})
}
