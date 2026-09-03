package vsphere

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vapi/rest"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// restClientTo builds a rest.Client that talks to the given base URL, so a test
// can stand a fake vAPI endpoint in front of it while vc.Client keeps a real
// SOAP session against vcsim. That split is the point: connect()'s repair path
// only reaches the rest client, so the two halves can be failed independently.
func restClientTo(t *testing.T, rawURL string) *rest.Client {
	t.Helper()
	u, err := soap.ParseURL(rawURL)
	require.NoError(t, err)
	return rest.NewClient(&vim25.Client{Client: soap.NewClient(u, true)})
}

// vcsimFor starts a vcsim instance and returns its host and port.
func vcsimFor(t *testing.T) (string, int) {
	t.Helper()
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
	return server.URL.Hostname(), port
}

// connectedVC returns a VirtualCenter with a live SOAP session against vcsim.
func connectedVC(t *testing.T, ctx context.Context, host string, port int) *VirtualCenter {
	t.Helper()
	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: types.NewFQDN(host), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(ctx), "initial connect should succeed")
	return vc
}

// TestRepairRestSessionCoolsDownAfterAuthFailure covers the lockout guard on
// connect()'s rest repair path. The path runs on every connect() and has no
// attempt limit of its own, so a rest login that vCenter rejects -- a stale
// password after a rotation, say, which a still-valid SOAP session hides -- must
// not be retried at connect() speed: vCenter SSO locks an account after 5 failed
// attempts in 180s, which an active cluster would reach in seconds.
//
// Note a vAPI rejection is an HTTP 401, not a SOAP fault, so this is exactly the
// case IsInvalidLoginError does not recognise on its own.
func TestRepairRestSessionCoolsDownAfterAuthFailure(t *testing.T) {
	ctx := context.Background()
	host, port := vcsimFor(t)

	var logins atomic.Int32
	vapi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Session(): POST .../session?~action=get. Login(): POST .../session
		// with no query. Both are answered 401 -- the session reads as gone,
		// and the login to re-establish it is rejected.
		if r.URL.RawQuery == "" {
			logins.Add(1)
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(vapi.Close)

	vc := connectedVC(t, ctx, host, port)
	soapClient := vc.Client
	vc.RestClient = restClientTo(t, vapi.URL)

	for range 5 {
		require.NoError(t, vc.connect(ctx),
			"a rejected rest login should not fail the connection")
	}

	assert.Equal(t, int32(1), logins.Load(),
		"a rejected rest login should be attempted once, then held off until the cooldown expires")
	assert.False(t, vc.restLoginCooldownUntil.IsZero(), "the cooldown should have been armed")
	assert.Same(t, soapClient, vc.Client, "the SOAP client should be left alone throughout")

	// Once the cooldown expires, it tries again rather than giving up for good.
	vc.restLoginCooldownUntil = time.Now().Add(-time.Second)
	require.NoError(t, vc.connect(ctx))
	assert.Equal(t, int32(2), logins.Load(),
		"an expired cooldown should allow another attempt")
}

// TestRepairRestSessionRetriesTransportFailuresImmediately is the other half of
// the guard: a vAPI that is down rather than rejecting credentials must keep
// being retried at connect() speed, since that is what makes recovery take a
// single round trip once it comes back. Only authentication failures are held
// off.
func TestRepairRestSessionRetriesTransportFailuresImmediately(t *testing.T) {
	ctx := context.Background()
	host, port := vcsimFor(t)

	var attempts atomic.Int32
	var down atomic.Bool
	down.Store(true)
	vapi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "" {
			attempts.Add(1)
			if down.Load() {
				// 503, as a vapi-endpoint that is restarting would answer.
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":"a-session-id"}`))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(vapi.Close)

	vc := connectedVC(t, ctx, host, port)
	vc.RestClient = restClientTo(t, vapi.URL)

	for range 3 {
		require.NoError(t, vc.connect(ctx))
	}
	assert.Equal(t, int32(3), attempts.Load(),
		"an outage should be retried on every connect(), not held off like an auth failure")
	assert.True(t, vc.restLoginCooldownUntil.IsZero(),
		"a transport failure should not arm the lockout cooldown")

	// vAPI comes back: the very next connect() re-establishes the session.
	down.Store(false)
	require.NoError(t, vc.connect(ctx))
	assert.Equal(t, "a-session-id", vc.RestClient.SessionID(),
		"the next connect() after recovery should have re-logged in")
}

// TestRepairRestSessionTimesOut covers a vAPI endpoint that accepts the
// connection and then never answers. NewClient sets soap.Client.Timeout to 0 and
// the rest client shares that transport, so without the repair path's own
// timeout this attempt would hang forever -- holding ClientMutex, and with it
// every other caller of Connect, including the SOAP-only ones that have no need
// of vAPI at all.
func TestRepairRestSessionTimesOut(t *testing.T) {
	ctx := context.Background()
	host, port := vcsimFor(t)

	blocked := make(chan struct{})
	vapi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "" {
			select {
			case <-blocked:
			case <-r.Context().Done():
			}
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(vapi.Close)
	// Registered after vapi.Close so it runs before it: cleanups are LIFO, and
	// Server.Close waits on in-flight handlers, which this one releases.
	t.Cleanup(func() { close(blocked) })

	originalTimeout := restLoginTimeout
	restLoginTimeout = 200 * time.Millisecond
	t.Cleanup(func() { restLoginTimeout = originalTimeout })

	vc := connectedVC(t, ctx, host, port)
	soapClient := vc.Client
	vc.RestClient = restClientTo(t, vapi.URL)

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- vc.connect(ctx) }()

	select {
	case err := <-done:
		require.NoError(t, err, "a hung rest login should not fail the connection")
		assert.Less(t, time.Since(start), 5*time.Second,
			"connect() should have given up on the hung rest login, not waited on it")
	case <-time.After(15 * time.Second):
		t.Fatal("connect() hung on an unresponsive vAPI endpoint")
	}

	assert.Same(t, soapClient, vc.Client,
		"giving up on the rest login should leave the SOAP session in place")
}
