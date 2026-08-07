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
	"github.com/vmware/govmomi/simulator"
)

// TestConfiguredOrActiveUser covers both of ConfiguredOrActiveUser's paths:
// the configured username is returned directly with no vCenter call, and an
// empty configured username (the shared session manager case, where there is
// no static user to configure) falls back to the live session lookup.
func TestConfiguredOrActiveUser(t *testing.T) {
	t.Run("should return the configured username without any vCenter call", func(t *testing.T) {
		// vc.Client is nil, so GetActiveUser would fail immediately. Getting a
		// result at all proves the fallback was never reached.
		vc := &VirtualCenter{Config: &VirtualCenterConfig{Username: "configured-user"}}

		username, err := vc.ConfiguredOrActiveUser(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "configured-user", username)
	})

	t.Run("should fall back to the live session user when none is configured", func(t *testing.T) {
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

		vc := &VirtualCenter{
			Config: &VirtualCenterConfig{
				Host: server.URL.Hostname(), Port: port, Insecure: true,
				Username: "user", Password: "pass", // simulator.DefaultLogin
				// Username above is only used to authenticate; it models a
				// deployment where the config's static username is empty
				// (the session manager case) by clearing it after connect.
			},
			ClientMutex: &sync.Mutex{},
		}
		require.NoError(t, vc.Connect(context.Background()))

		// Simulate the session-manager deployment shape: authenticated, but
		// with no static username configured.
		vc.Config.Username = ""

		username, err := vc.ConfiguredOrActiveUser(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "user", username, "should reflect the live session's user")
	})
}
