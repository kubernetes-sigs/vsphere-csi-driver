/*
Copyright 2020 The Kubernetes Authors.

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

package k8sorchestrator

import (
	"context"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
)

func init() {
	// Create context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
}

// TestIsFSSEnabledInGcWithSync tests IsFSSEnabled in GC flavor with two FSS in sync scenarios:
// Scenario 1: FSS is enabled both in SV and GC
// Scenario 2: FSS is disabled both in SV and GC
func TestIsFSSEnabledInGcWithSync(t *testing.T) {
	svFSS := map[string]string{
		"volume-extend": "true",
		"volume-health": "false",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
	}
	internalFSS := map[string]string{
		"volume-extend": "true",
		"volume-health": "false",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
	}
	k8sOrchestrator := K8sOrchestrator{
		supervisorFSS: svFSSConfigMapInfo,
		clusterFlavor: cnstypes.CnsClusterFlavorGuest,
		internalFSS:   internalFSSConfigMapInfo,
	}
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "volume-extend")
	if !isEnabled {
		t.Errorf("volume-extend feature state is disabled!")
	}
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "volume-health")
	if isEnabled {
		t.Errorf("volume-health feature state is enabled!")
	}
}

// TestIsFSSEnabledInGcWithoutSync tests IsFSSEnabled in GC flavor with two FSS non-sync scenarios:
// Scenario 1: FSS is enabled in SV but disabled in GC
// Scenario 2: FSS is disabled in SV but enabled in GC
func TestIsFSSEnabledInGcWithoutSync(t *testing.T) {
	svFSS := map[string]string{
		"volume-extend": "true",
		"volume-health": "false",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
	}
	internalFSS := map[string]string{
		"volume-extend": "false",
		"volume-health": "true",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
	}
	k8sOrchestrator := K8sOrchestrator{
		supervisorFSS: svFSSConfigMapInfo,
		clusterFlavor: cnstypes.CnsClusterFlavorGuest,
		internalFSS:   internalFSSConfigMapInfo,
	}
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "volume-extend")
	if isEnabled {
		t.Errorf("volume-extend feature state is enabled!")
	}
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "volume-health")
	if isEnabled {
		t.Errorf("volume-health feature state is enabled!")
	}
}

// TestIsFSSEnabledInGcWrongValues tests IsFSSEnabled in GC flavor in two scenarios:
// Scenario 1: Wrong value given to feature state
// Scenario 2: Missing feature state
func TestIsFSSEnabledInGcWrongValues(t *testing.T) {
	svFSS := map[string]string{
		"volume-extend": "true",
		"volume-health": "true",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
	}
	internalFSS := map[string]string{
		"volume-extend": "enabled",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
	}
	k8sOrchestrator := K8sOrchestrator{
		supervisorFSS: svFSSConfigMapInfo,
		clusterFlavor: cnstypes.CnsClusterFlavorGuest,
		internalFSS:   internalFSSConfigMapInfo,
	}
	// Wrong value given
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "volume-extend")
	if isEnabled {
		t.Errorf("volume-extend feature state is enabled even when it was assigned a wrong value!")
	}
	// Feature state missing
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "volume-health")
	if isEnabled {
		t.Errorf("Non existing feature state volume-health is enabled!")
	}
}

// TestIsFSSEnabledInSV tests IsFSSEnabled in Supervisor flavor - all scenarios
func TestIsFSSEnabledInSV(t *testing.T) {
	svFSS := map[string]string{
		"volume-extend": "true",
		"volume-health": "false",
		"csi-migration": "enabled",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
	}
	k8sOrchestrator := K8sOrchestrator{
		supervisorFSS: svFSSConfigMapInfo,
		clusterFlavor: cnstypes.CnsClusterFlavorWorkload,
	}
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "volume-extend")
	if !isEnabled {
		t.Errorf("volume-extend feature state is disabled!")
	}
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "volume-health")
	if isEnabled {
		t.Errorf("volume-health feature state is enabled!")
	}
	// Wrong value given
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "csi-migration")
	if isEnabled {
		t.Errorf("csi-migration feature state is enabled even when it was assigned a wrong value!")
	}
	// Feature state missing
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "online-volume-extend")
	if isEnabled {
		t.Errorf("Non existing feature state online-volume-extend is enabled!")
	}
}

// TestIsFSSEnabledInVanilla tests IsFSSEnabled in vanilla flavor - all scenarios
func TestIsFSSEnabledInVanilla(t *testing.T) {
	internalFSS := map[string]string{
		"csi-migration": "true",
		"volume-extend": "false",
		"volume-health": "disabled",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
	}
	k8sOrchestrator := K8sOrchestrator{
		clusterFlavor: cnstypes.CnsClusterFlavorVanilla,
		internalFSS:   internalFSSConfigMapInfo,
	}
	// Should be enabled
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "csi-migration")
	if !isEnabled {
		t.Errorf("csi-migration feature state is disabled!")
	}
	// Should be disabled
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "volume-extend")
	if isEnabled {
		t.Errorf("volume-extend feature state is enabled!")
	}
	// Wrong value given
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "volume-health")
	if isEnabled {
		t.Errorf("volume-health feature state is enabled even when it was assigned a wrong value!")
	}
	// Feature state missing
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, "online-volume-extend")
	if isEnabled {
		t.Errorf("Non existing feature state online-volume-extend is enabled!")
	}
}

// TestIsFSSEnabledWithWrongClusterFlavor tests IsFSSEnabled when cluster flavor is not supported
func TestIsFSSEnabledWithWrongClusterFlavor(t *testing.T) {
	k8sOrchestrator := K8sOrchestrator{
		clusterFlavor: "Vanila",
	}
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "volume-extend")
	if isEnabled {
		t.Errorf("volume-extend feature state enabled even when cluster flavor is wrong")
	}
}
