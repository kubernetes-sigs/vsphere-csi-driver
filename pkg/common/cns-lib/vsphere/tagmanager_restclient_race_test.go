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

	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"
)

// TestGetTagManagerRestClientReadRace covers a race distinct from the one
// TestGetTagManagerConcurrentAccess catches: that test connects once and the
// session stays valid throughout, so connect()'s "no need to re-login" fast
// path never rewrites vc.RestClient again, and there is nothing to race
// against. This test forces genuine reconnects -- logging out the live client
// directly, bypassing Connect, so the next Connect() call detects an expired
// session and actually rewrites vc.Client/vc.RestClient under ClientMutex --
// concurrently with GetTagManager calls, which read vc.RestClient after their
// own Connect() call has already released that same lock.
//
// Each reconnect gets its own short-timeout context: Connect retries an
// InvalidLogin error with a 3-minute delay, and repeated logout/reconnect
// cycles against vcsim can occasionally be misclassified as one, which would
// otherwise make a broken version of this test hang rather than fail.
func TestGetTagManagerRestClientReadRace(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"restclient-race-test\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	model.Cluster = 1
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
			Host:     server.URL.Hostname(),
			Port:     port,
			Insecure: true,
			Username: "user", // simulator.DefaultLogin
			Password: "pass",
		},
		ClientMutex: &sync.Mutex{},
	}
	ctx := context.Background()
	require.NoError(t, vc.Connect(ctx))

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			vc.ClientMutex.Lock()
			client := vc.Client
			vc.ClientMutex.Unlock()
			if client != nil {
				_ = client.Logout(ctx)
			}
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = vc.Connect(callCtx)
			cancel()
		}
	}()

	const readers = 8
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_, _ = vc.GetTagManager(ctx)
			}
		}()
	}
	wg.Wait()
}
