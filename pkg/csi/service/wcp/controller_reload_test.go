/*
Copyright 2019 The Kubernetes Authors.

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

package wcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/simulator"

	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// resetRecordingManager records the vCenter instances passed to ResetManager.
type resetRecordingManager struct {
	cnsvolume.Manager
	resetVCs []*cnsvsphere.VirtualCenter
	resetErr error
}

func (m *resetRecordingManager) ResetManager(_ context.Context, vc *cnsvsphere.VirtualCenter) error {
	m.resetVCs = append(m.resetVCs, vc)
	return m.resetErr
}

// reloadTestEnv is a controller wired to a vcsim vCenter that is reachable as "localhost", with the config file
// that ReloadConfiguration reads written to a temp file.
type reloadTestEnv struct {
	c        *controller
	vcconfig *cnsvsphere.VirtualCenterConfig
	volMgr   *resetRecordingManager
	port     string
	confPath string
}

func (e *reloadTestEnv) writeConfig(t *testing.T, host, password string) {
	t.Helper()
	require.NoError(t, os.WriteFile(e.confPath, []byte(fmt.Sprintf(
		"[Global]\ncluster-id = \"test-cluster\"\ninsecure-flag = \"true\"\n\n"+
			"[VirtualCenter %q]\nuser = \"user@vsphere.local\"\npassword = %q\nport = %q\ninsecure-flag = \"true\"\n",
		host, password, e.port)), 0600))
}

func newReloadTestEnv(t *testing.T) *reloadTestEnv {
	t.Helper()
	// Make sure the shared controller test fixture is initialized first, as it owns the process wide volume
	// manager instance that ReloadConfiguration retrieves.
	getControllerTest(t)

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	e := &reloadTestEnv{port: server.URL.Port(), confPath: filepath.Join(t.TempDir(), "csi-vsphere.conf")}
	t.Setenv("VSPHERE_CSI_CONFIG", e.confPath)
	e.writeConfig(t, "localhost", "pass")

	// Disable the features that make ReloadConfiguration set up the volume operation request store.
	originalCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = &fakeCOOverrides{
		COCommonInterface: originalCO,
		fssOverrides: map[string]bool{
			common.CSIVolumeManagerIdempotency: false,
			common.FCDTransactionSupport:       false,
		},
	}
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = originalCO })

	cfg, err := config.GetConfig(ctx)
	require.NoError(t, err)
	vcconfig, err := cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	require.NoError(t, err)
	e.vcconfig = vcconfig

	e.volMgr = &resetRecordingManager{}
	e.c = &controller{manager: &common.Manager{
		VcenterConfig: vcconfig,
		CnsConfig:     cfg,
		VolumeManager: e.volMgr,
	}}
	t.Cleanup(func() {
		for _, host := range []commontypes.FQDN{commontypes.NewFQDN("localhost"), commontypes.NewFQDN("old-vc.invalid")} {
			_ = cnsvsphere.UnregisterVirtualCenterForHost(ctx, host)
		}
	})
	return e
}

func TestReloadConfigurationUnchangedConfigReusesInstance(t *testing.T) {
	e := newReloadTestEnv(t)
	// Register the vCenter directly with the manager, the way Init does.
	registered, err := cnsvsphere.GetVirtualCenterManager(ctx).RegisterVirtualCenter(ctx, e.vcconfig)
	require.NoError(t, err)
	require.NoError(t, registered.Connect(ctx))
	client := registered.Client

	require.NoError(t, e.c.ReloadConfiguration(false))

	require.Len(t, e.volMgr.resetVCs, 1)
	assert.Same(t, registered, e.volMgr.resetVCs[0], "the registered vCenter must be adopted, not replaced")
	assert.Same(t, client, registered.Client, "the existing session must not be recreated")
	assert.NotSame(t, e.vcconfig, registered.Config, "the vCenter must carry the reloaded config")
}

func TestReloadConfigurationReconnectCreatesNewInstance(t *testing.T) {
	e := newReloadTestEnv(t)
	old, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, e.vcconfig, false)
	require.NoError(t, err)

	require.NoError(t, e.c.ReloadConfiguration(true))

	require.Len(t, e.volMgr.resetVCs, 1)
	assert.NotSame(t, old, e.volMgr.resetVCs[0], "a reconnect must create a new vCenter instance")
	assert.NotNil(t, e.volMgr.resetVCs[0].Client)
	assert.Nil(t, old.Client, "the previous session must be logged out")
}

// vcsim accepts any password, so an invalid new config is simulated with an unreachable vCenter port.
func TestReloadConfigurationUnusableConfigKeepsCurrentInstance(t *testing.T) {
	e := newReloadTestEnv(t)
	old, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, e.vcconfig, false)
	require.NoError(t, err)
	e.port = "1"
	e.writeConfig(t, "localhost", "pass")

	require.Error(t, e.c.ReloadConfiguration(true))

	assert.Empty(t, e.volMgr.resetVCs, "the volume manager must not be reset on a failed reload")
	assert.NotNil(t, old.Client, "the current session must be kept when the new config cannot connect")
}

func TestReloadConfigurationHostChangeUnregistersPreviousHost(t *testing.T) {
	e := newReloadTestEnv(t)
	oldHost := commontypes.NewFQDN("old-vc.invalid")
	oldConfig := *e.vcconfig
	oldConfig.Host = oldHost
	_, err := cnsvsphere.GetVirtualCenterManager(ctx).RegisterVirtualCenter(ctx, &oldConfig)
	require.NoError(t, err)
	e.c.manager.VcenterConfig = &oldConfig

	require.NoError(t, e.c.ReloadConfiguration(false))

	_, err = cnsvsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, oldHost)
	assert.ErrorIs(t, err, cnsvsphere.ErrVCNotFound, "the previous host must be unregistered")
	require.Len(t, e.volMgr.resetVCs, 1)
	assert.Equal(t, commontypes.NewFQDN("localhost"), e.volMgr.resetVCs[0].Config.Host)
	assert.Equal(t, commontypes.NewFQDN("localhost"), e.c.manager.VcenterConfig.Host)
}

// TestReloadConfigurationFailedSwitchOverKeepsPreviousHostRegistered guards against unregistering the previous
// host before the controller has switched over to the new one.
func TestReloadConfigurationFailedSwitchOverKeepsPreviousHostRegistered(t *testing.T) {
	e := newReloadTestEnv(t)
	oldHost := commontypes.NewFQDN("old-vc.invalid")
	oldConfig := *e.vcconfig
	oldConfig.Host = oldHost
	_, err := cnsvsphere.GetVirtualCenterManager(ctx).RegisterVirtualCenter(ctx, &oldConfig)
	require.NoError(t, err)
	e.c.manager.VcenterConfig = &oldConfig
	e.volMgr.resetErr = fmt.Errorf("reset failed")

	require.Error(t, e.c.ReloadConfiguration(false))

	assert.True(t, cnsvsphere.GetVirtualCenterManager(ctx).IsVirtualCenterRegistered(oldHost),
		"the previous host must stay registered while the controller still uses it")
	assert.Equal(t, oldHost, e.c.manager.VcenterConfig.Host)
}
