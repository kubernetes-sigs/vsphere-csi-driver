package vsphere

import (
	"context"
	"crypto/tls"
	neturl "net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"
	vimtypes "github.com/vmware/govmomi/vim25/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// vcsimCheckingPasswords is vcsimFor with vcsim's login check enabled -- by
// default it accepts any non-empty credentials, which cannot produce the
// InvalidLogin these tests are about. Only acceptPassword is accepted.
func vcsimCheckingPasswords(t *testing.T, acceptPassword string) (string, int) {
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
	server := model.Service.NewServer()
	t.Cleanup(server.Close)
	// Authenticate() short-circuits to "any non-empty credentials" while the
	// listen URL still carries simulator.DefaultLogin, so the hook below is
	// only consulted once the URL names a specific user.
	model.Service.Listen.User = neturl.UserPassword("user", acceptPassword)
	model.Service.Context.Map.SessionManager().ValidLogin = func(req *vimtypes.Login) bool {
		return req.Password == acceptPassword
	}

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)
	return server.URL.Hostname(), port
}

// rejectedVC returns a VirtualCenter pointed at vcsim with a password vcsim
// will reject, so the first Connect fails with InvalidLogin.
func rejectedVC(t *testing.T, host string, port int) *VirtualCenter {
	t.Helper()
	return &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host: types.NewFQDN(host), Port: port, Insecure: true,
			Username: "user", Password: "not-the-password",
		},
		ClientMutex: &sync.Mutex{},
	}
}

// pointAtUnreachable repoints vc at an address that refuses connections. Any
// Connect that actually goes to the network from here on fails with a
// transport error, which is what makes "no login was attempted" observable
// without counting round trips.
func pointAtUnreachable(t *testing.T, vc *VirtualCenter) {
	t.Helper()
	host, port := unreachableAddr(t)
	vc.Config.Host, vc.Config.Port = types.NewFQDN(host), port
}

// TestConnectCoolsDownAfterInvalidLogin covers the account-lockout guard on the
// full login path. vCenter SSO locks an account after 5 failed attempts in
// 180s, and Connect is called per CSI operation, by the listview watchdog on a
// 30s ticker and by the listview update loop -- so a rejected password must
// stop further attempts for the whole window, no matter how many callers ask or
// how short their own deadlines are.
func TestConnectCoolsDownAfterInvalidLogin(t *testing.T) {
	ctx := context.Background()
	host, port := vcsimCheckingPasswords(t, "pass")
	vc := rejectedVC(t, host, port)

	first := vc.Connect(ctx)
	require.Error(t, first, "a rejected password must fail the connect")
	require.False(t, vc.loginCooldownUntil.IsZero(), "an InvalidLogin must start the cooldown")

	pointAtUnreachable(t, vc)
	second := vc.Connect(ctx)
	require.Equal(t, first, second,
		"a Connect during the cooldown must replay the authentication failure, not attempt a login")
}

// TestConnectDuringCooldownIgnoresCallerDeadline is the regression this guard
// exists for. The old implementation waited out the lockout window inside
// Connect, so a caller whose own context was shorter than the window -- the
// listview bounds its connects at 30s -- returned early and the next caller
// started a fresh attempt, spending the budget at the rate callers arrived.
func TestConnectDuringCooldownIgnoresCallerDeadline(t *testing.T) {
	host, port := vcsimCheckingPasswords(t, "pass")
	vc := rejectedVC(t, host, port)

	first := vc.Connect(context.Background())
	require.Error(t, first)

	pointAtUnreachable(t, vc)
	for i := 0; i < 3; i++ {
		shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		require.Equal(t, first, vc.Connect(shortCtx),
			"a short-deadline caller must not get a fresh login attempt during the cooldown")
		cancel()
	}
}

// TestConnectRetriesAfterCooldownExpires: the cooldown delays the next attempt,
// it does not end them.
func TestConnectRetriesAfterCooldownExpires(t *testing.T) {
	ctx := context.Background()
	original := loginCooldown
	loginCooldown = 50 * time.Millisecond
	t.Cleanup(func() { loginCooldown = original })

	host, port := vcsimCheckingPasswords(t, "pass")
	vc := rejectedVC(t, host, port)
	first := vc.Connect(ctx)
	require.Error(t, first)

	pointAtUnreachable(t, vc)
	require.Eventually(t, func() bool {
		return vc.Connect(ctx) != first
	}, 5*time.Second, 25*time.Millisecond,
		"once the cooldown expires a login must be attempted again")
}

// TestSuccessfulConnectClearsLoginCooldown: a password rotation that lands
// must not leave the connection sitting out the rest of a stale cooldown.
func TestSuccessfulConnectClearsLoginCooldown(t *testing.T) {
	ctx := context.Background()
	original := loginCooldown
	loginCooldown = 50 * time.Millisecond
	t.Cleanup(func() { loginCooldown = original })

	host, port := vcsimCheckingPasswords(t, "pass")
	vc := rejectedVC(t, host, port)
	require.Error(t, vc.Connect(ctx))
	require.False(t, vc.loginCooldownUntil.IsZero())

	vc.Config.Password = "pass" // simulator.DefaultLogin
	require.Eventually(t, func() bool {
		return vc.Connect(ctx) == nil
	}, 5*time.Second, 25*time.Millisecond, "the rotated password must be accepted once the cooldown expires")
	require.True(t, vc.loginCooldownUntil.IsZero(), "a successful connect must clear the cooldown")
	require.NoError(t, vc.loginCooldownErr)
}
