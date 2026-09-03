package vsphere

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/mo"
)

// TestNewClientKeepsSoapSessionOnRestLoginFailure covers the username/password
// login path adding a second, independent REST login after the existing SOAP
// login. A failure there must not fail the login as a whole: REST is only
// needed for tag/topology operations (GetTagManager), which fail and recover
// on their own if it stays unavailable, and driver startup itself goes through
// this same login on every Connect() (controller.Init() -> Connect() ->
// NewClient() -> login()). Failing login() here would make a vAPI outage a
// hard dependency of starting the driver at all, even though CNS/volume
// operations need only SOAP.
//
// Reproduced by disabling vcsim's vapi endpoints, so the REST login this
// feature adds has nothing to talk to and fails while the SOAP login succeeds.
func TestNewClientKeepsSoapSessionOnRestLoginFailure(t *testing.T) {
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
	model.Service.RegisterEndpoints = false // no vapi -> the REST login below fails
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
	ctx := context.Background()

	client, restClient, err := vc.NewClient(ctx, "orphan-session-test")
	require.NoError(t, err,
		"NewClient must succeed on SOAP alone; a REST-only failure must not fail the whole login")
	require.NotNil(t, client)
	require.NotNil(t, restClient)
	t.Cleanup(func() { _ = client.Logout(ctx) })

	// Prove the REST endpoint is actually unreachable in this test setup (vAPI disabled).
	// This validates the test precondition (REST login cannot succeed), not the auth state.
	_, err = restClient.Session(ctx)
	assert.Error(t, err, "REST Session() should fail when vAPI endpoints are disabled")

	// Independently log in and read vCenter's own session list: the SOAP
	// session from NewClient must be the checker's session plus exactly one
	// more -- i.e. kept, not logged out as an orphan.
	checker, err := govmomi.NewClient(ctx, server.URL, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = checker.Logout(ctx) })

	var sessionManager mo.SessionManager
	pc := property.DefaultCollector(checker.Client)
	require.NoError(t, pc.RetrieveOne(
		ctx, checker.SessionManager.Reference(), []string{"sessionList"}, &sessionManager))

	assert.Len(t, sessionManager.SessionList, 2,
		"expected the checking client's session plus the kept SOAP session from NewClient; "+
			"a lower count means the SOAP session was logged out despite the login succeeding")
}
