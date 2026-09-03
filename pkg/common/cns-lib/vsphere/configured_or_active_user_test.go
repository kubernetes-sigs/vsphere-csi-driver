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
	"github.com/vmware/govmomi/simulator"
)

// connectedVCSim returns a VirtualCenter already connected to a vcsim instance,
// authenticated as simulator.DefaultLogin ("user"). Subtests then mutate
// vc.Config to model the deployment shape they care about; because the session
// is live, GetActiveUser answers off it and never reconnects, so config values
// that are not real endpoints are never dialed.
func connectedVCSim(t *testing.T) *VirtualCenter {
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

	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: commontypes.NewFQDN(server.URL.Hostname()), Port: port, Insecure: true,
			Username: "user", Password: "pass", // simulator.DefaultLogin
		},
		ClientMutex: &sync.Mutex{},
	}
	require.NoError(t, vc.Connect(context.Background()))
	return vc
}

// TestConfiguredOrActiveUser covers ConfiguredOrActiveUser's paths: the
// configured username is returned directly with no vCenter call, and each of
// the three cases where the configured value cannot be trusted as this
// session's username falls back to the live session lookup.
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
		vc := connectedVCSim(t)

		// The session-manager deployment shape: authenticated, but with no
		// static username configured.
		vc.Config.Username = ""

		username, err := vc.ConfiguredOrActiveUser(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "user", username, "should reflect the live session's user")
	})

	// validateConfig waives the username requirement when VCSessionManagerURL
	// is set but never clears an already-configured User, so a config can carry
	// both. The session is then a clone of whoever the session manager
	// authenticated as, which need not be the configured user -- returning the
	// configured one would attribute CNS volume metadata to the wrong user.
	t.Run("should ignore a configured username when the session manager is in use", func(t *testing.T) {
		vc := connectedVCSim(t)

		vc.Config.Username = "someone-else@vsphere.local"
		vc.Config.VCSessionManagerURL = "https://session-manager.invalid/session"

		username, err := vc.ConfiguredOrActiveUser(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "user", username,
			"the session manager's cloned session decides the user, not the configured value")
	})

	// Under certificate authentication Username holds a PEM-encoded
	// certificate, not a user. login() selects that path with the same
	// pem.Decode check.
	t.Run("should ignore a certificate held in the username field", func(t *testing.T) {
		vc := connectedVCSim(t)

		vc.Config.Username = "-----BEGIN CERTIFICATE-----\nZm9v\n-----END CERTIFICATE-----\n"

		username, err := vc.ConfiguredOrActiveUser(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "user", username, "should not return the PEM block as a username")
	})
}
