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

package syncer

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/simulator"

	cnsvolumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	commontypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// This file is named so that its tests run after TestSyncerWorkflows. ReloadConfiguration retrieves the
// process wide volume manager instance, which TestSyncerWorkflows initializes.

// resetRecordingManager records the vCenter instances passed to ResetManager.
type resetRecordingManager struct {
	cnsvolumes.Manager
	resetVCs []*cnsvsphere.VirtualCenter
}

func (m *resetRecordingManager) ResetManager(_ context.Context, vc *cnsvsphere.VirtualCenter) error {
	m.resetVCs = append(m.resetVCs, vc)
	return nil
}

// workloadReloadTestEnv is a Supervisor metadata syncer wired to a vcsim vCenter that is reachable as
// "localhost", with the config file that ReloadConfiguration reads written to a temp file.
type workloadReloadTestEnv struct {
	syncer   *metadataSyncInformer
	vcconfig *cnsvsphere.VirtualCenterConfig
	volMgr   *resetRecordingManager
	port     string
	confPath string
}

func (e *workloadReloadTestEnv) writeConfig(t *testing.T, host string) {
	t.Helper()
	require.NoError(t, os.WriteFile(e.confPath, []byte(fmt.Sprintf(
		"[Global]\ncluster-id = \"test-cluster\"\ninsecure-flag = \"true\"\n\n"+
			"[VirtualCenter %q]\nuser = \"user@vsphere.local\"\npassword = \"pass\"\nport = %q\n"+
			"insecure-flag = \"true\"\n", host, e.port)), 0600))
}

func newWorkloadReloadTestEnv(t *testing.T) *workloadReloadTestEnv {
	t.Helper()
	ctx := context.Background()

	model := simulator.VPX()
	t.Cleanup(model.Remove)
	require.NoError(t, model.Create())
	model.Service.TLS = new(tls.Config)
	server := model.Service.NewServer()
	t.Cleanup(server.Close)

	e := &workloadReloadTestEnv{port: server.URL.Port(), confPath: filepath.Join(t.TempDir(), "csi-vsphere.conf")}
	t.Setenv("VSPHERE_CSI_CONFIG", e.confPath)
	e.writeConfig(t, "localhost")

	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	originalCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = fakeCO
	originalQuotaM2 := isStorageQuotaM2FSSEnabled
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		isStorageQuotaM2FSSEnabled = originalQuotaM2
	})

	cfg, err := cnsconfig.GetConfig(ctx)
	require.NoError(t, err)
	e.vcconfig, err = cnsvsphere.GetVirtualCenterConfig(ctx, cfg)
	require.NoError(t, err)

	e.volMgr = &resetRecordingManager{}
	e.syncer = &metadataSyncInformer{
		clusterFlavor: cnstypes.CnsClusterFlavorWorkload,
		configInfo:    &cnsconfig.ConfigurationInfo{Cfg: cfg},
		host:          e.vcconfig.Host,
		volumeManager: e.volMgr,
	}
	t.Cleanup(func() {
		for _, host := range []commontypes.FQDN{commontypes.NewFQDN("localhost"), commontypes.NewFQDN("old-vc.invalid")} {
			_ = cnsvsphere.UnregisterVirtualCenterForHost(ctx, host)
		}
	})
	return e
}

func TestWorkloadReloadConfigurationUnchangedConfigReusesInstance(t *testing.T) {
	ctx := context.Background()
	e := newWorkloadReloadTestEnv(t)
	old, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, e.vcconfig, false)
	require.NoError(t, err)
	client := old.Client

	require.NoError(t, ReloadConfiguration(e.syncer, false))

	require.Len(t, e.volMgr.resetVCs, 1)
	assert.Same(t, old, e.volMgr.resetVCs[0], "the cached vCenter must be reused")
	assert.Same(t, client, old.Client, "the existing session must not be recreated")
	assert.NotSame(t, e.vcconfig, old.Config, "the vCenter must carry the reloaded config")
}

func TestWorkloadReloadConfigurationForcedReconnectCreatesNewInstance(t *testing.T) {
	ctx := context.Background()
	e := newWorkloadReloadTestEnv(t)
	old, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, e.vcconfig, false)
	require.NoError(t, err)

	require.NoError(t, ReloadConfiguration(e.syncer, true))

	require.Len(t, e.volMgr.resetVCs, 1)
	assert.NotSame(t, old, e.volMgr.resetVCs[0], "a forced reconnect must create a new vCenter instance")
	assert.NotNil(t, e.volMgr.resetVCs[0].Client)
	assert.Nil(t, old.Client, "the previous session must be logged out")
}

// vcsim accepts any password, so an unusable new config is simulated with an unreachable vCenter port.
func TestWorkloadReloadConfigurationUnusableConfigKeepsCurrentInstance(t *testing.T) {
	ctx := context.Background()
	e := newWorkloadReloadTestEnv(t)
	old, err := cnsvsphere.GetVirtualCenterInstanceForVCenterConfig(ctx, e.vcconfig, false)
	require.NoError(t, err)
	e.port = "1"
	e.writeConfig(t, "localhost")

	require.Error(t, ReloadConfiguration(e.syncer, true))

	assert.Empty(t, e.volMgr.resetVCs, "the volume manager must not be reset on a failed reload")
	assert.NotNil(t, old.Client, "the current session must be kept when the new config cannot connect")
	assert.Equal(t, commontypes.NewFQDN("localhost"), e.syncer.host)
}

func TestWorkloadReloadConfigurationHostChangeUnregistersPreviousHost(t *testing.T) {
	ctx := context.Background()
	e := newWorkloadReloadTestEnv(t)
	oldHost := commontypes.NewFQDN("old-vc.invalid")
	oldConfig := *e.vcconfig
	oldConfig.Host = oldHost
	_, err := cnsvsphere.GetVirtualCenterManager(ctx).RegisterVirtualCenter(ctx, &oldConfig)
	require.NoError(t, err)
	e.syncer.host = oldHost

	require.NoError(t, ReloadConfiguration(e.syncer, false))

	_, err = cnsvsphere.GetVirtualCenterManager(ctx).GetVirtualCenter(ctx, oldHost)
	assert.ErrorIs(t, err, cnsvsphere.ErrVCNotFound, "the previous host must be unregistered")
	require.Len(t, e.volMgr.resetVCs, 1)
	assert.Equal(t, commontypes.NewFQDN("localhost"), e.volMgr.resetVCs[0].Config.Host)
	assert.Equal(t, commontypes.NewFQDN("localhost"), e.syncer.host)
}
