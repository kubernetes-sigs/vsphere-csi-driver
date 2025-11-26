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

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	wcpcapv1alph1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/wcpcapabilities/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
)

const (
	feature_flag_1 = "feature_flag_1"
	feature_flag_2 = "feature_flag_2"
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
		feature_flag_1: "true",
		feature_flag_2: "false",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
		featureStatesLock:  &sync.RWMutex{},
	}
	internalFSS := map[string]string{
		feature_flag_1: "true",
		feature_flag_2: "false",
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
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_1)
	if !isEnabled {
		t.Errorf("%s feature state is disabled!", feature_flag_1)
	}
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_2)
	if isEnabled {
		t.Errorf("%s feature state is enabled!", feature_flag_2)
	}
}

// TestIsFSSEnabledInGcWithoutSync tests IsFSSEnabled in GC flavor with two FSS non-sync scenarios:
// Scenario 1: FSS is enabled in SV but disabled in GC
// Scenario 2: FSS is disabled in SV but enabled in GC
func TestIsFSSEnabledInGcWithoutSync(t *testing.T) {
	svFSS := map[string]string{
		feature_flag_1: "true",
		feature_flag_2: "false",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
		featureStatesLock:  &sync.RWMutex{},
	}
	internalFSS := map[string]string{
		feature_flag_1: "false",
		feature_flag_2: "true",
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
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_1)
	if isEnabled {
		t.Errorf("%s feature state is enabled!", feature_flag_1)
	}
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_2)
	if isEnabled {
		t.Errorf("%s feature state is enabled!", feature_flag_2)
	}
}

// TestIsFSSEnabledInGcWrongValues tests IsFSSEnabled in GC flavor in two scenarios:
// Scenario 1: Wrong value given to feature state
// Scenario 2: Missing feature state
func TestIsFSSEnabledInGcWrongValues(t *testing.T) {
	svFSS := map[string]string{
		feature_flag_1: "true",
		feature_flag_2: "true",
	}
	svFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultSupervisorFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStates:      svFSS,
		featureStatesLock:  &sync.RWMutex{},
	}
	internalFSS := map[string]string{
		feature_flag_1: "enabled",
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
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_1)
	if isEnabled {
		t.Errorf("%s feature state is enabled even when it was assigned a wrong value!", feature_flag_1)
	}
	// Feature state missing
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_2)
	if isEnabled {
		t.Errorf("Non existing feature state %s is enabled!", feature_flag_2)
	}
}

// TestIsFSSEnabledInSV tests IsFSSEnabled in Supervisor flavor - all scenarios
func TestIsFSSEnabledInSV(t *testing.T) {
	svFSS := map[string]string{
		feature_flag_1:  "true",
		feature_flag_2:  "false",
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
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_1)
	if !isEnabled {
		t.Errorf("%s feature state is disabled!", feature_flag_1)
	}
	isEnabled = k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_2)
	if isEnabled {
		t.Errorf("%s feature state is enabled!", feature_flag_2)
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
	internalFSSConfigMapInfo := FSSConfigMapInfo{
		configMapName:      cnsconfig.DefaultInternalFSSConfigMapName,
		configMapNamespace: cnsconfig.DefaultCSINamespace,
		featureStatesLock:  &sync.RWMutex{},
	}
	k8sOrchestrator := K8sOrchestrator{
		clusterFlavor:      cnstypes.CnsClusterFlavorVanilla,
		internalFSS:        internalFSSConfigMapInfo,
		releasedVanillaFSS: getReleasedVanillaFSS(),
	}
	// Feature state missing
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, "unknown-performance-feature")
	if isEnabled {
		t.Errorf("Non existing feature state unknown-performance-feature is enabled!")
	}
}

// TestIsFSSEnabledWithWrongClusterFlavor tests IsFSSEnabled when cluster flavor is not supported
func TestIsFSSEnabledWithWrongClusterFlavor(t *testing.T) {
	k8sOrchestrator := K8sOrchestrator{
		clusterFlavor: "Vanila",
	}
	isEnabled := k8sOrchestrator.IsFSSEnabled(ctx, feature_flag_1)
	if isEnabled {
		t.Errorf("%s feature state enabled even when cluster flavor is wrong", feature_flag_1)
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

func TestIsFileVolume(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		pv   *v1.PersistentVolume
		want bool
	}{
		{
			name: "No AccessModes",
			pv: &v1.PersistentVolume{
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{},
				},
			},
			want: false,
		},
		{
			name: "AccessMode ReadWriteMany",
			pv: &v1.PersistentVolume{
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
				},
			},
			want: true,
		},
		{
			name: "AccessMode ReadOnlyMany",
			pv: &v1.PersistentVolume{
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany},
				},
			},
			want: true,
		},
		{
			name: "RWO Block volume mode with FSS disabled",
			pv: &v1.PersistentVolume{
				Spec: v1.PersistentVolumeSpec{
					AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					VolumeMode:  func() *v1.PersistentVolumeMode { m := v1.PersistentVolumeBlock; return &m }(),
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sOrchestratorInstance = &K8sOrchestrator{}
			got := isFileVolume(ctx, tt.pv)
			if got != tt.want {
				t.Errorf("isFileVolume() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetWcpCapabilitiesMap_Success(t *testing.T) {
	ctx := context.Background()
	WcpCapabilitiesMap = nil

	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{
		Group:   "wcp.vmware.com",
		Version: "v1alpha1",
		Kind:    "Capabilities",
	}

	scheme.AddKnownTypeWithName(
		gvk,
		&wcpcapv1alph1.Capabilities{},
	)
	cap := &wcpcapv1alph1.Capabilities{
		ObjectMeta: metav1.ObjectMeta{
			Name: common.WCPCapabilitiesCRName,
		},
		Status: wcpcapv1alph1.CapabilitiesStatus{
			Supervisor: map[wcpcapv1alph1.CapabilityName]wcpcapv1alph1.CapabilityStatus{
				"CapabilityA": {Activated: true},
				"CapabilityB": {Activated: false},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cap).
		Build()

	err := SetWcpCapabilitiesMap(ctx, cl)
	assert.NoError(t, err)

	val, _ := WcpCapabilitiesMap.Load("CapabilityA")
	assert.Equal(t, true, val)

	val, _ = WcpCapabilitiesMap.Load("CapabilityB")
	assert.Equal(t, false, val)
}

func TestK8sOrchestrator_GetSnapshotsForPVC(t *testing.T) {
	t.Run("WhenNoSnapshotsExistForPVC", func(tt *testing.T) {
		// Setup
		orch := K8sOrchestrator{}

		// Execute
		snaps := orch.GetSnapshotsForPVC(context.Background(), "", "")

		// Assert
		assert.Empty(tt, snaps)
	})
	t.Run("WhenSnapshotsExist", func(tt *testing.T) {
		// Setup
		pvc, ns := "test-pvc", "test-ns"
		orch := K8sOrchestrator{
			pvcToSnapshotsMap: pvcToSnapshotsMap{
				RWMutex: sync.RWMutex{},
				items: map[types.NamespacedName]map[string]struct{}{
					{Namespace: ns, Name: pvc}: {
						"snap1": struct{}{},
						"snap2": struct{}{},
						"snap3": struct{}{},
					},
				},
			},
		}
		exp := []string{"snap1", "snap2", "snap3"}

		// Execute
		snaps := orch.GetSnapshotsForPVC(context.Background(), pvc, ns)

		// Assert
		assert.ElementsMatch(tt, exp, snaps)
	})
}

func TestPvcToSnapshotsMap_Add(t *testing.T) {
	t.Run("AddSnapshotToNewPVC", func(tt *testing.T) {
		// Setup
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}
		pvc, ns, snap := "test-pvc", "test-ns", "snap1"

		// Execute
		pvcMap.add(pvc, snap, ns)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 1)
		assert.Contains(tt, snaps, snap)
	})

	t.Run("AddMultipleSnapshotsToSamePVC", func(tt *testing.T) {
		// Setup
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}
		pvc, ns := "test-pvc", "test-ns"
		snaps := []string{"snap1", "snap2", "snap3"}

		// Execute
		for _, snap := range snaps {
			pvcMap.add(pvc, snap, ns)
		}

		// Assert
		result := pvcMap.get(pvc, ns)
		assert.Len(tt, result, 3)
		for _, snap := range snaps {
			assert.Contains(tt, result, snap)
		}
	})

	t.Run("AddSameSnapshotTwice", func(tt *testing.T) {
		// Setup
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}
		pvc, ns, snap := "test-pvc", "test-ns", "snap1"

		// Execute
		pvcMap.add(pvc, snap, ns)
		pvcMap.add(pvc, snap, ns) // Add same snapshot again

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 1, "duplicate snapshot should not be added")
		assert.Contains(tt, snaps, snap)
	})
}

func TestPvcToSnapshotsMap_Get(t *testing.T) {
	t.Run("GetNonExistentPVC", func(tt *testing.T) {
		// Setup
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: "test-ns", Name: "pvc1"}: {
					"snap1": struct{}{},
				},
			},
		}

		// Execute
		snaps := pvcMap.get("non-existent-pvc", "test-ns")

		// Assert
		assert.Empty(tt, snaps)
	})

	t.Run("GetExistingPVCWithSnapshots", func(tt *testing.T) {
		// Setup
		pvc, ns := "test-pvc", "test-ns"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: ns, Name: pvc}: {
					"snap1": struct{}{},
					"snap2": struct{}{},
					"snap3": struct{}{},
				},
			},
		}
		exp := []string{"snap1", "snap2", "snap3"}

		// Execute
		snaps := pvcMap.get(pvc, ns)

		// Assert
		assert.ElementsMatch(tt, exp, snaps)
	})
}

func TestPvcToSnapshotsMap_Delete(t *testing.T) {
	t.Run("DeleteSnapshotFromPVC", func(tt *testing.T) {
		// Setup
		pvc, ns := "test-pvc", "test-ns"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: ns, Name: pvc}: {
					"snap1": struct{}{},
					"snap2": struct{}{},
				},
			},
		}

		// Execute
		pvcMap.delete(pvc, "snap1", ns)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 1)
		assert.Contains(tt, snaps, "snap2")
		assert.NotContains(tt, snaps, "snap1")
	})

	t.Run("DeleteLastSnapshotRemovesPVCEntry", func(tt *testing.T) {
		// Setup
		pvc, ns := "test-pvc", "test-ns"
		pvcKey := types.NamespacedName{Namespace: ns, Name: pvc}
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				pvcKey: {
					"snap1": struct{}{},
				},
			},
		}

		// Execute
		pvcMap.delete(pvc, "snap1", ns)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Empty(tt, snaps)
		// Verify PVC entry is removed from map
		pvcMap.RLock()
		_, exists := pvcMap.items[pvcKey]
		pvcMap.RUnlock()
		assert.False(tt, exists, "PVC entry should be removed when last snapshot is deleted")
	})

	t.Run("DeleteNonExistentSnapshot", func(tt *testing.T) {
		// Setup
		pvc, ns := "test-pvc", "test-ns"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: ns, Name: pvc}: {
					"snap1": struct{}{},
				},
			},
		}

		// Execute
		pvcMap.delete(pvc, "non-existent-snap", ns)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 1)
		assert.Contains(tt, snaps, "snap1")
	})

	t.Run("DeleteFromNonExistentPVC", func(tt *testing.T) {
		// Setup
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute
		pvcMap.delete("non-existent-pvc", "snap1", "test-ns")

		// Assert
		assert.Empty(tt, pvcMap.items)
	})

	t.Run("DeleteMultipleSnapshotsSequentially", func(tt *testing.T) {
		// Setup
		pvc, ns := "test-pvc", "test-ns"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: ns, Name: pvc}: {
					"snap1": struct{}{},
					"snap2": struct{}{},
					"snap3": struct{}{},
				},
			},
		}

		// Execute
		pvcMap.delete(pvc, "snap1", ns)
		pvcMap.delete(pvc, "snap2", ns)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 1)
		assert.Contains(tt, snaps, "snap3")
	})
}

func TestInitPVCToSnapshotsMap(t *testing.T) {
	t.Run("SkipForNonWorkloadCluster", func(tt *testing.T) {
		// Setup
		ctx := context.Background()

		// Execute
		err := initPVCToSnapshotsMap(ctx, cnstypes.CnsClusterFlavorVanilla)

		// Assert
		assert.NoError(tt, err)
	})

	// since `k8sOrchestratorInstance.informerManager` is a struct type, testing the behaviour of
	// AddSnapshotListener is not ideal. Since TestPVCToSnapshotsMapEventHandlers tests
	// the event handlers, we're good for now.
	// TODO: Add tests to verify actual informer logic
}

// Test the snapshot event handlers directly
func TestPVCToSnapshotsMapEventHandlers(t *testing.T) {
	// Helper to create a VolumeSnapshot object
	createSnapshot := func(name, namespace, pvcName string) *snapshotv1.VolumeSnapshot {
		return &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: snapshotv1.VolumeSnapshotSpec{
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: &pvcName,
				},
			},
		}
	}

	t.Run("HandleSnapshotAdded", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvc, ns, snapName := "test-pvc", "test-ns", "test-snap"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute
		snap := createSnapshot(snapName, ns, pvc)
		handleSnapshotAdded(ctx, snap, &pvcMap)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 1)
		assert.Contains(tt, snaps, snapName)
	})

	t.Run("HandleSnapshotDeleted", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvc, ns, snapName := "test-pvc", "test-ns", "test-snap"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: ns, Name: pvc}: {
					snapName: struct{}{},
				},
			},
		}

		// Execute
		snap := createSnapshot(snapName, ns, pvc)
		handleSnapshotDeleted(ctx, snap, &pvcMap)

		// Assert
		snaps := pvcMap.get(pvc, ns)
		assert.Empty(tt, snaps)
	})

	t.Run("HandleSnapshotAddedWithNilPVCName", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute
		snap := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-snap",
				Namespace: "test-ns",
			},
			Spec: snapshotv1.VolumeSnapshotSpec{
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: nil,
				},
			},
		}
		handleSnapshotAdded(ctx, snap, &pvcMap)

		// Assert - snapshot should not be added
		assert.Empty(tt, pvcMap.items)
	})

	t.Run("HandleSnapshotAddedWithInvalidObject", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute
		handleSnapshotAdded(ctx, "not-a-snapshot-object", &pvcMap)

		// Assert - snapshot should not be added
		assert.Empty(tt, pvcMap.items)
	})

	t.Run("HandleSnapshotAddedWithNilSnapshot", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute
		var nilSnap *snapshotv1.VolumeSnapshot
		handleSnapshotAdded(ctx, nilSnap, &pvcMap)

		// Assert - snapshot should not be added
		assert.Empty(tt, pvcMap.items)
	})

	t.Run("MultipleSnapshotsAddedAndDeleted", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvc, ns := "test-pvc", "test-ns"
		snap1, snap2, snap3 := "snap1", "snap2", "snap3"
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute - Add multiple snapshots
		handleSnapshotAdded(ctx, createSnapshot(snap1, ns, pvc), &pvcMap)
		handleSnapshotAdded(ctx, createSnapshot(snap2, ns, pvc), &pvcMap)
		handleSnapshotAdded(ctx, createSnapshot(snap3, ns, pvc), &pvcMap)

		// Assert all added
		snaps := pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 3)

		// Execute - Delete one snapshot
		handleSnapshotDeleted(ctx, createSnapshot(snap2, ns, pvc), &pvcMap)

		// Assert only 2 remain
		snaps = pvcMap.get(pvc, ns)
		assert.Len(tt, snaps, 2)
		assert.Contains(tt, snaps, snap1)
		assert.Contains(tt, snaps, snap3)
		assert.NotContains(tt, snaps, snap2)
	})

	t.Run("HandleSnapshotDeletedWithNilPVCName", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: "test-ns", Name: "test-pvc"}: {
					"snap1": struct{}{},
				},
			},
		}

		// Execute
		snap := &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-snap",
				Namespace: "test-ns",
			},
			Spec: snapshotv1.VolumeSnapshotSpec{
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: nil,
				},
			},
		}
		handleSnapshotDeleted(ctx, snap, &pvcMap)

		// Assert - map should remain unchanged
		assert.Len(tt, pvcMap.items, 1)
	})

	t.Run("HandleSnapshotDeletedWithInvalidObject", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items: map[types.NamespacedName]map[string]struct{}{
				{Namespace: "test-ns", Name: "test-pvc"}: {
					"snap1": struct{}{},
				},
			},
		}

		// Execute
		handleSnapshotDeleted(ctx, "not-a-snapshot-object", &pvcMap)

		// Assert - map should remain unchanged
		assert.Len(tt, pvcMap.items, 1)
	})

	t.Run("AddAndDeleteAcrossMultiplePVCsAndNamespaces", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		pvcMap := pvcToSnapshotsMap{
			RWMutex: sync.RWMutex{},
			items:   make(map[types.NamespacedName]map[string]struct{}),
		}

		// Execute - Add snapshots for different PVCs and namespaces
		handleSnapshotAdded(ctx, createSnapshot("snap1", "ns1", "pvc1"), &pvcMap)
		handleSnapshotAdded(ctx, createSnapshot("snap2", "ns1", "pvc1"), &pvcMap)
		handleSnapshotAdded(ctx, createSnapshot("snap3", "ns1", "pvc2"), &pvcMap)
		handleSnapshotAdded(ctx, createSnapshot("snap4", "ns2", "pvc1"), &pvcMap)

		// Assert all added correctly
		assert.Len(tt, pvcMap.get("pvc1", "ns1"), 2)
		assert.Len(tt, pvcMap.get("pvc2", "ns1"), 1)
		assert.Len(tt, pvcMap.get("pvc1", "ns2"), 1)

		// Execute - Delete snapshots
		handleSnapshotDeleted(ctx, createSnapshot("snap1", "ns1", "pvc1"), &pvcMap)
		handleSnapshotDeleted(ctx, createSnapshot("snap3", "ns1", "pvc2"), &pvcMap)

		// Assert correct deletions
		assert.Len(tt, pvcMap.get("pvc1", "ns1"), 1)
		assert.Contains(tt, pvcMap.get("pvc1", "ns1"), "snap2")
		assert.Empty(tt, pvcMap.get("pvc2", "ns1"))
		assert.Len(tt, pvcMap.get("pvc1", "ns2"), 1)
	})
}
