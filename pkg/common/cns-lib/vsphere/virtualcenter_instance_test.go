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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"

	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

// newInstanceTestVCConfig starts a vcsim instance and returns a VirtualCenterConfig that points to it. The
// instance cache and the virtual center manager registry are cleared before and after the test.
func newInstanceTestVCConfig(t *testing.T) *VirtualCenterConfig {
	t.Helper()
	confPath := filepath.Join(t.TempDir(), "csi-vsphere.conf")
	require.NoError(t, os.WriteFile(confPath, []byte(
		"[Global]\ncluster-id = \"instance-test\"\n\n"+
			"[VirtualCenter \"127.0.0.1\"]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\n"+
			"datacenters = \"DC0\"\ninsecure-flag = \"true\"\n"), 0600))
	t.Setenv("VSPHERE_CSI_CONFIG", confPath)

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	port, err := strconv.Atoi(server.URL.Port())
	require.NoError(t, err)

	reset := func() {
		ctx := context.Background()
		require.NoError(t, UnregisterAllVirtualCenters(ctx))
		vCenterInstancesLock.Lock()
		vCenterInstances = make(map[commontypes.FQDN]*VirtualCenter)
		vCenterInstancesLock.Unlock()
	}
	reset()
	t.Cleanup(reset)

	return &VirtualCenterConfig{
		Host:     commontypes.NewFQDN(server.URL.Hostname()),
		Port:     port,
		Insecure: true,
		Username: "user", // simulator.DefaultLogin
		Password: "pass",
	}
}

func TestGetVirtualCenterInstanceForVCenterConfig_ReturnsCachedInstance(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	first, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)
	require.NotNil(t, first.Client)

	second, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)
	assert.Same(t, first, second, "a second call without reinitialize must return the cached instance")
}

func TestGetVirtualCenterInstanceForVCenterConfig_ReinitializeReplacesInstance(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	old, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)

	renewed, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, true)
	require.NoError(t, err)
	assert.NotSame(t, old, renewed, "reinitialize must create a new instance")
	assert.NotNil(t, renewed.Client)
	assert.Nil(t, old.Client, "the previous instance must be logged out")
}

// TestGetVirtualCenterInstanceForVCenterConfig_AdoptsDirectlyRegisteredVC covers a vCenter registered with the
// virtual center manager without going through the accessor (as the WCP CSI controller does at init). It must
// be adopted instead of failing with ErrVCAlreadyRegistered.
func TestGetVirtualCenterInstanceForVCenterConfig_AdoptsDirectlyRegisteredVC(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	registered, err := GetVirtualCenterManager(ctx).RegisterVirtualCenter(ctx, vcconfig)
	require.NoError(t, err)

	adopted, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)
	assert.Same(t, registered, adopted)
	assert.NotNil(t, adopted.Client, "the adopted instance must be connected")

	// Reinitialize replaces the adopted instance instead of failing with ErrVCAlreadyRegistered.
	renewed, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, true)
	require.NoError(t, err)
	assert.NotSame(t, registered, renewed)
}

// TestGetVirtualCenterInstanceForVCenterConfig_ConcurrentCallsShareSession guards the startup race where one
// caller logged out the session that another caller had just obtained.
func TestGetVirtualCenterInstanceForVCenterConfig_ConcurrentCallsShareSession(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	const callers = 8
	instances := make([]*VirtualCenter, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			instances[i], errs[i] = GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
		}(i)
	}
	wg.Wait()

	for i := 0; i < callers; i++ {
		require.NoError(t, errs[i])
		assert.Same(t, instances[0], instances[i])
	}
	instances[0].ClientMutex.Lock()
	defer instances[0].ClientMutex.Unlock()
	assert.NotNil(t, instances[0].Client, "the shared session must still be logged in")
}

func TestUnregisterVirtualCenterForHost(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	vc, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)

	require.NoError(t, UnregisterVirtualCenterForHost(ctx, vcconfig.Host))
	assert.Nil(t, vc.Client, "the vCenter must be logged out")
	_, err = GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, vcconfig.Host)
	assert.ErrorIs(t, err, ErrVCNotFound)
	vCenterInstancesLock.RLock()
	_, cached := vCenterInstances[vcconfig.Host]
	vCenterInstancesLock.RUnlock()
	assert.False(t, cached, "the vCenter must be dropped from the instance cache")

	// Unregistering a host that is not registered is a no-op.
	assert.NoError(t, UnregisterVirtualCenterForHost(ctx, vcconfig.Host))
}

// TestGetVirtualCenterInstanceForVCenterConfig_ConnectFailureClearsCache guards against the cache keeping the
// logged out previous instance, and the registry an unconnected one, when reinitialize fails to connect.
func TestGetVirtualCenterInstanceForVCenterConfig_ConnectFailureClearsCache(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	_, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)

	unreachable := *vcconfig
	unreachable.Port = 1
	_, err = GetVirtualCenterInstanceForVCenterConfig(ctx, &unreachable, true)
	require.Error(t, err)

	vCenterInstancesLock.RLock()
	_, cached := vCenterInstances[vcconfig.Host]
	vCenterInstancesLock.RUnlock()
	assert.False(t, cached, "the logged out instance must not stay cached")
	assert.False(t, GetVirtualCenterManager(ctx).IsVirtualCenterRegistered(vcconfig.Host),
		"the unconnected instance must not stay registered")

	// The next call starts from scratch and succeeds.
	vc, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)
	assert.NotNil(t, vc.Client)
}

func TestUnregisterAllVirtualCentersClearsCache(t *testing.T) {
	ctx := context.Background()
	vcconfig := newInstanceTestVCConfig(t)

	vc, err := GetVirtualCenterInstanceForVCenterConfig(ctx, vcconfig, false)
	require.NoError(t, err)

	require.NoError(t, UnregisterAllVirtualCenters(ctx))

	assert.Nil(t, vc.Client)
	vCenterInstancesLock.RLock()
	assert.Empty(t, vCenterInstances, "logged out instances must not be handed out again")
	vCenterInstancesLock.RUnlock()
}
