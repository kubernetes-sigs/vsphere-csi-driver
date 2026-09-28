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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// TestDisconnectWaitsForClientMutex proves Disconnect participates in the same
// mutual exclusion Connect/connect give vc.Client and vc.RestClient.
//
// UBMCNS-2383: Disconnect used to write vc.Client/vc.RestClient to nil without
// ClientMutex. A concurrent caller unregistering this VirtualCenter (e.g. a
// credential-rotation reconnect) could nil them out while a long-lived
// background reader's own reconnect (ListViewImpl.listenToTaskUpdates ->
// connect) was already past connect()'s nil-check and reading vc.RestClient to
// call Session() on it -- panicking on a nil receiver mid-read.
//
// This is deliberately deterministic rather than relying on `go test -race` to
// get lucky: it holds ClientMutex itself (standing in for a goroutine
// mid-connect()) and asserts Disconnect blocks until the lock is released, then
// completes and clears both clients. Confirmed this fails without the fix:
// Disconnect returns immediately despite the lock being held.
func TestDisconnectWaitsForClientMutex(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"disconnect-lock-test\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	model.Cluster = 1
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)

	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host:     commontypes.NewFQDN(server.URL.Hostname()),
			Port:     port,
			Insecure: true,
			Username: "user", // simulator.DefaultLogin
			Password: "pass",
		},
		ClientMutex: &sync.Mutex{},
	}
	ctx := context.Background()
	require.NoError(t, vc.Connect(ctx))
	require.NotNil(t, vc.Client)
	require.NotNil(t, vc.RestClient)

	// Stand in for a goroutine mid-connect(): hold the same lock Disconnect
	// must now respect.
	vc.ClientMutex.Lock()

	done := make(chan error, 1)
	go func() {
		done <- vc.Disconnect(ctx)
	}()

	select {
	case <-done:
		t.Fatal("Disconnect returned while ClientMutex was held elsewhere; " +
			"it is not synchronizing with Connect/connect on vc.Client/vc.RestClient")
	case <-time.After(200 * time.Millisecond):
		// Expected: Disconnect is blocked waiting for the lock.
	}

	vc.ClientMutex.Unlock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect did not complete after ClientMutex was released")
	}

	assert.Nil(t, vc.Client)
	assert.Nil(t, vc.RestClient)
}
