package vsphere

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/simulator"
	"github.com/vmware/govmomi/vim25/mo"
)

// TestNewClientLogsOutOrphanedSoapSessionOnRestLoginFailure covers the
// username/password login path adding a second, independent REST login after
// the existing SOAP login. If the REST login fails, the SOAP session that just
// succeeded is never assigned to vc.Client, so nothing later reaches it to log
// it out; without an explicit logout here it would sit live on vCenter until it
// times out.
//
// Reproduced by disabling vcsim's vapi endpoints, so the REST login this
// feature adds has nothing to talk to and fails while the SOAP login succeeds.
func TestNewClientLogsOutOrphanedSoapSessionOnRestLoginFailure(t *testing.T) {
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
			Host: server.URL.Hostname(), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	ctx := context.Background()

	_, _, err = vc.NewClient(ctx, "orphan-session-test")
	require.Error(t, err, "REST login must fail: vapi endpoints are disabled")

	// Independently log in and read vCenter's own session list. If the SOAP
	// session from the failed NewClient call leaked, it shows up here too.
	checker, err := govmomi.NewClient(ctx, server.URL, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = checker.Logout(ctx) })

	var sessionManager mo.SessionManager
	pc := property.DefaultCollector(checker.Client)
	require.NoError(t, pc.RetrieveOne(
		ctx, checker.SessionManager.Reference(), []string{"sessionList"}, &sessionManager))

	require.Len(t, sessionManager.SessionList, 1,
		"expected only the checking client's own session; a higher count means the SOAP "+
			"session from the failed login was left live on vCenter")
}
