/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

// TestDisconnectConcurrentWithConnect covers the race behind a start-up
// SIGSEGV in the syncer: UnregisterVirtualCenter calls Disconnect, which sets
// vc.Client and vc.RestClient to nil, while a listview goroutine is inside
// connect() on the same VirtualCenter. connect() had already passed its nil
// guard and then dereferenced vc.RestClient after a network round trip.
//
// Disconnect must hold ClientMutex like connect() does. Run with -race: the
// unsynchronized writes in Disconnect are reported even when the timing never
// produces the nil dereference itself.
func TestDisconnectConcurrentWithConnect(t *testing.T) {
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"disconnect-race-test\"\n\n"+
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
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			// Short per-call timeout: Connect retries InvalidLogin with a long
			// delay, which would otherwise make a regression hang, not fail.
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = vc.Connect(callCtx)
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_ = vc.Disconnect(ctx)
		}
	}()
	wg.Wait()
}
