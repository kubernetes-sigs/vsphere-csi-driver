package vsphere

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnssim "github.com/vmware/govmomi/cns/simulator"
	pbmsim "github.com/vmware/govmomi/pbm/simulator"
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

// vcsimFor starts a vcsim instance, with the CNS and PBM APIs registered so
// tests can build real, working clients against it, and returns its host and
// port.
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
	model.Service.RegisterSDK(cnssim.New())
	model.Service.RegisterSDK(pbmsim.New())

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)
	return server.URL.Hostname(), port
}

// unreachableAddr returns a host:port that refuses TCP connections, standing
// in for vpxd being briefly unreachable mid-patch.
func unreachableAddr(t *testing.T) (string, int) {
	t.Helper()
	server := httptest.NewServer(http.NotFoundHandler())
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	server.Close() // now refuses connections
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	return u.Hostname(), port
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

// TestConnectRecreatesStaleDependentClients reproduces a vCenter service
// disruption (an in-place patch restarting vpxd/vsanvcmgmtd) that makes
// exactly one reconnect attempt fail after the session has already gone
// bad. Before this fix, NewClient()'s failure
// nulled vc.Client (every one of its error paths returns (nil, nil, err),
// and that was assigned straight into vc.Client/vc.RestClient), so the
// *next*, successful connect() took the "client was never initialized"
// branch and returned early without rebuilding CnsClient -- leaving it
// bound to the vim25 client from before the disruption indefinitely, since
// connect()'s own health check only ever looks at vc.Client, which by then
// is healthy again. That is why the driver kept failing every CNS call with
// NotAuthenticated long after vCenter itself had recovered.
func TestConnectRecreatesStaleDependentClients(t *testing.T) {
	ctx := context.Background()
	host, port := vcsimFor(t)
	vc := connectedVC(t, ctx, host, port)

	require.NoError(t, vc.ConnectCns(ctx), "initial ConnectCns should succeed")

	originalClient := vc.Client
	staleCnsClient := vc.CnsClient
	require.NotNil(t, staleCnsClient)

	// Kill the session server-side, so the next connect() sees it as invalid
	// and attempts a full re-login -- the same trigger a vCenter service
	// restart produces.
	require.NoError(t, vc.Client.Logout(ctx))

	// Point at an address that refuses connections, so the reconnect
	// NewClient() call fails exactly once.
	downHost, downPort := unreachableAddr(t)
	realHost, realPort := vc.Config.Host, vc.Config.Port
	vc.Config.Host, vc.Config.Port = types.NewFQDN(downHost), downPort

	err := vc.connect(ctx)
	require.Error(t, err, "a reconnect attempt against an unreachable vCenter should fail")
	assert.Same(t, originalClient, vc.Client,
		"a failed reconnect attempt must not null out vc.Client -- that is what let connect() "+
			"skip rebuilding CnsClient on the next, successful attempt")

	// vCenter is back: restore the real address and let the next connect()
	// succeed.
	vc.Config.Host, vc.Config.Port = realHost, realPort
	require.NoError(t, vc.connect(ctx))

	assert.NotSame(t, originalClient, vc.Client,
		"the session was invalid, so connect() should have built a fresh client")
	require.NotNil(t, vc.CnsClient)
	assert.NotSame(t, staleCnsClient, vc.CnsClient,
		"CnsClient must be rebuilt on the fresh vim25 client, not left bound to the one "+
			"that existed before the disruption")
}

// TestDisconnectClearsDependentClients covers the second path to the same
// staleness: Disconnect() used to nil only vc.Client/vc.RestClient, leaving
// PbmClient/CnsClient/VsanClient/VslmClient non-nil and bound to the session
// it had just logged out. A subsequent connect() that takes the "client was
// never initialized" branch (vc.Client == nil) has no reason to look at
// them, so they would stay stale indefinitely.
func TestDisconnectClearsDependentClients(t *testing.T) {
	ctx := context.Background()
	host, port := vcsimFor(t)
	vc := connectedVC(t, ctx, host, port)

	require.NoError(t, vc.ConnectCns(ctx))
	require.NoError(t, vc.ConnectPbm(ctx))
	require.NotNil(t, vc.CnsClient)
	require.NotNil(t, vc.PbmClient)

	require.NoError(t, vc.Disconnect(ctx))

	assert.Nil(t, vc.Client)
	assert.Nil(t, vc.RestClient)
	assert.Nil(t, vc.CnsClient, "Disconnect must drop CnsClient too, or a future connect() "+
		"that takes the initialisation branch would leave it bound to the session just logged out")
	assert.Nil(t, vc.PbmClient)
}
