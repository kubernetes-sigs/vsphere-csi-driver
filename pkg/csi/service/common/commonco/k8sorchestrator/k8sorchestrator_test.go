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
	"reflect"
	"strconv"
	"sync"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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
		featureStatesLock:  &sync.RWMutex{},
	}
	internalFSS := map[string]string{
		"volume-extend": "true",
		"volume-health": "false",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
		featureStatesLock:  &sync.RWMutex{},
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
		featureStatesLock:  &sync.RWMutex{},
	}
	internalFSS := map[string]string{
		"volume-extend": "false",
		"volume-health": "true",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
		featureStatesLock:  &sync.RWMutex{},
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
		featureStatesLock:  &sync.RWMutex{},
	}
	internalFSS := map[string]string{
		"volume-extend": "enabled",
	}
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      internalFSS,
		featureStatesLock:  &sync.RWMutex{},
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
		featureStatesLock:  &sync.RWMutex{},
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
		featureStatesLock:  &sync.RWMutex{},
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

func TestGetNodesForVolumes(t *testing.T) {
	volumeNameToNodesMap := &volumeNameToNodesMap{
		RWMutex: &sync.RWMutex{},
		items:   make(map[string][]string),
	}
	volumeIDToNameMap := &volumeIDToNameMap{
		RWMutex: &sync.RWMutex{},
		items:   make(map[string]string),
	}
	volumeIDs := []string{"ec5c1a4f-0c54-4681-b350-cbb79b08b4d7", "1994e110-7f86-4d77-aaba-d615d8e182ae",
		"364908d2-82a1-4095-a8c9-0bcd9d62bddf", "ec5c1a4f-0c54-4681-b350-d615d8e182ae"}
	for i := 1; i <= 5; i += 1 {
		volumeNameToNodesMap.items["volume-"+strconv.Itoa(i)] = []string{"node" + strconv.Itoa(i), "node" + strconv.Itoa(i+5)}
	}
	for i := 1; i <= 3; i += 1 {
		volumeIDToNameMap.items[volumeIDs[i-1]] = "volume-" + strconv.Itoa(i)
	}
	volumeIDToNameMap.items["ec5c1a4f-0c54-4681-b350-d615d8e182ae"] = "volume-6"
	k8sOrchestrator := K8sOrchestrator{
		volumeIDToNameMap:    volumeIDToNameMap,
		volumeNameToNodesMap: volumeNameToNodesMap,
	}

	nodeNames := k8sOrchestrator.GetNodesForVolumes(ctx, volumeIDs)
	expectedNodeNames := make(map[string][]string)
	expectedNodeNames["ec5c1a4f-0c54-4681-b350-cbb79b08b4d7"] = []string{"node-1", "node-6"}
	expectedNodeNames["364908d2-82a1-4095-a8c9-0bcd9d62bddf"] = []string{"node-3", "node-8"}
	if reflect.DeepEqual(nodeNames, expectedNodeNames) {
		t.Errorf("Expected node names %v but got %v", expectedNodeNames, nodeNames)
	}
}
