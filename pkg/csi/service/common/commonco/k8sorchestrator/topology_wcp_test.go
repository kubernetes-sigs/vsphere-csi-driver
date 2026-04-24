/*
Copyright 2025 The Kubernetes Authors.

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
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/container-storage-interface/spec/lib/go/csi"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
)

// MockDatastoreRetriever implements DatastoreRetriever interface for testing
type MockDatastoreRetriever struct {
	// ClusterDatastores maps cluster IDs to their available datastores
	ClusterDatastores map[string][]*cnsvsphere.DatastoreInfo
}

// GetCandidateDatastoresInCluster implements DatastoreRetriever interface for testing
func (m *MockDatastoreRetriever) GetCandidateDatastoresInCluster(
	ctx context.Context,
	vc *cnsvsphere.VirtualCenter,
	clusterID string,
	includeVSANDirect bool,
) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
	if datastores, exists := m.ClusterDatastores[clusterID]; exists {
		return datastores, nil, nil
	}
	return nil, nil, nil
}

// GetSharedDatastoresInClusters implements DatastoreRetriever interface for testing
func (m *MockDatastoreRetriever) GetSharedDatastoresInClusters(
	ctx context.Context,
	clusterMorefs []string,
	vc *cnsvsphere.VirtualCenter,
) ([]*cnsvsphere.DatastoreInfo, error) {
	if len(clusterMorefs) == 0 {
		return nil, nil
	}

	// Start with datastores from first cluster
	var sharedDatastores []*cnsvsphere.DatastoreInfo
	if firstClusterDatastores, exists := m.ClusterDatastores[clusterMorefs[0]]; exists {
		sharedDatastores = append(sharedDatastores, firstClusterDatastores...)
	}

	// For each subsequent cluster, keep only datastores that are also in that cluster
	for i := 1; i < len(clusterMorefs); i++ {
		clusterID := clusterMorefs[i]
		clusterDatastores, exists := m.ClusterDatastores[clusterID]
		if !exists {
			return []*cnsvsphere.DatastoreInfo{}, nil // No datastores for this cluster
		}

		var intersection []*cnsvsphere.DatastoreInfo
		for _, sharedDS := range sharedDatastores {
			for _, clusterDS := range clusterDatastores {
				if sharedDS.Info.Url == clusterDS.Info.Url {
					intersection = append(intersection, sharedDS)
					break
				}
			}
		}
		sharedDatastores = intersection

		// If no intersection found, return empty (not error for test purposes)
		if len(sharedDatastores) == 0 {
			return []*cnsvsphere.DatastoreInfo{}, nil
		}
	}

	return sharedDatastores, nil
}

// Helper function to create datastore info for testing
func createDatastoreInfo(name, url string) *cnsvsphere.DatastoreInfo {
	return &cnsvsphere.DatastoreInfo{
		Info: &vimtypes.DatastoreInfo{
			Name: name,
			Url:  url,
		},
	}
}

// Helper function to create topology requirement
func createTopologyRequirement(zones []string) *csi.TopologyRequirement {
	var preferred []*csi.Topology
	for _, zone := range zones {
		preferred = append(preferred, &csi.Topology{
			Segments: map[string]string{
				v1.LabelTopologyZone: zone,
			},
		})
	}
	return &csi.TopologyRequirement{
		Preferred: preferred,
	}
}

func TestWCPControllerVolumeTopology_GetSharedDatastoresInTopology(t *testing.T) {
	ctx := context.Background()

	// Create test datastores
	datastore114 := createDatastoreInfo("datastore-114", "ds:///vmfs/volumes/6895f0bb-1ffe619b-574b-020053013de8/")
	datastore204 := createDatastoreInfo("datastore-204", "ds:///vmfs/volumes/6f7b8796-8da795e6/")
	datastore205 := createDatastoreInfo("datastore-205", "ds:///vmfs/volumes/d3cec85f-c18d869d/")
	datastore203 := createDatastoreInfo("datastore-203", "ds:///vmfs/volumes/d1377884-6e1032bb/")

	tests := []struct {
		name                     string
		isPodVMEnabled           bool
		azClusterMap             map[string]string
		azClustersMap            map[string][]string
		clusterDatastores        map[string][]*cnsvsphere.DatastoreInfo
		topologyRequirement      *csi.TopologyRequirement
		expectedSharedDatastores []*cnsvsphere.DatastoreInfo
		expectedTopoSegMap       map[string][]*cnsvsphere.DatastoreInfo
		expectError              bool
		expectedDatastores       *int // nil means use len(expectedSharedDatastores)
		expectedZones            *int // nil means use len(expectedTopoSegMap)
	}{
		{
			name:           "Single cluster per AZ - isPodVMOnStretchedSupervisorEnabled false",
			isPodVMEnabled: false,
			azClusterMap: map[string]string{
				"zone-1": "domain-c117",
				"zone-2": "domain-c128",
				"zone-3": "domain-c139",
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114, datastore205}, // zone-1
				"domain-c128": {datastore114, datastore203}, // zone-2
				"domain-c139": {datastore114, datastore204}, // zone-3
			},
			topologyRequirement: createTopologyRequirement([]string{"zone-3", "zone-1", "zone-2"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{
				datastore114, datastore204, // zone-3
				datastore114, datastore205, // zone-1
				datastore114, datastore203, // zone-2
			},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1": {datastore114, datastore205},
				"zone-2": {datastore114, datastore203},
				"zone-3": {datastore114, datastore204},
			},
		},
		{
			name:           "Multiple clusters per AZ - isPodVMOnStretchedSupervisorEnabled true",
			isPodVMEnabled: true,
			azClustersMap: map[string][]string{
				"zone-1": {"domain-c117"},
				"zone-2": {"domain-c128"},
				"zone-3": {"domain-c139"},
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114, datastore205}, // zone-1
				"domain-c128": {datastore114, datastore203}, // zone-2
				"domain-c139": {datastore114, datastore204}, // zone-3
			},
			topologyRequirement: createTopologyRequirement([]string{"zone-3", "zone-1", "zone-2"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{
				datastore114, datastore204, // zone-3
				datastore114, datastore205, // zone-1
				datastore114, datastore203, // zone-2
			},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1": {datastore114, datastore205},
				"zone-2": {datastore114, datastore203},
				"zone-3": {datastore114, datastore204},
			},
		},
		{
			name:           "Multiple clusters per AZ - isPodVMOnStretchedSupervisorEnabled false",
			isPodVMEnabled: false,
			azClusterMap: map[string]string{
				"zone-1": "domain-c117", // When isPodVMEnabled=false, only first cluster is used
				"zone-2": "domain-c128",
				"zone-3": "domain-c139",
			},
			azClustersMap: map[string][]string{
				"zone-1": {"domain-c117", "domain-c118"}, // Multiple clusters, but only first will be used
				"zone-2": {"domain-c128", "domain-c129"},
				"zone-3": {"domain-c139", "domain-c140"},
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114, datastore205}, // zone-1, first cluster (used)
				"domain-c118": {datastore114, datastore203}, // zone-1, second cluster (ignored)
				"domain-c128": {datastore114, datastore203}, // zone-2, first cluster (used)
				"domain-c129": {datastore114, datastore204}, // zone-2, second cluster (ignored)
				"domain-c139": {datastore114, datastore204}, // zone-3, first cluster (used)
				"domain-c140": {datastore114, datastore205}, // zone-3, second cluster (ignored)
			},
			topologyRequirement: createTopologyRequirement([]string{"zone-3", "zone-1", "zone-2"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{
				datastore114, datastore204, // zone-3 (only from domain-c139)
				datastore114, datastore205, // zone-1 (only from domain-c117)
				datastore114, datastore203, // zone-2 (only from domain-c128)
			},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1": {datastore114, datastore205}, // Only from domain-c117
				"zone-2": {datastore114, datastore203}, // Only from domain-c128
				"zone-3": {datastore114, datastore204}, // Only from domain-c139
			},
		},
		{
			name:           "Nil preferred topology requirement",
			isPodVMEnabled: false,
			azClusterMap:   map[string]string{"zone-1": "domain-c117"},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114, datastore205},
			},
			topologyRequirement: &csi.TopologyRequirement{
				Preferred: nil,
			},
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{},
			expectedTopoSegMap:       map[string][]*cnsvsphere.DatastoreInfo{},
		},
		{
			name:           "Zone not found in azClusterMap",
			isPodVMEnabled: false,
			azClusterMap: map[string]string{
				"zone-1":       "domain-c117",
				"zone-unknown": "domain-c999",
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114, datastore205},
				// domain-c999 not in clusterDatastores, so will return empty
			},
			topologyRequirement:      createTopologyRequirement([]string{"zone-unknown"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{},
			expectedTopoSegMap:       map[string][]*cnsvsphere.DatastoreInfo{"zone-unknown": {}},
		},
		{
			name:           "Single zone with multiple datastores - isPodVMOnStretchedSupervisorEnabled false",
			isPodVMEnabled: false,
			azClusterMap: map[string]string{
				"zone-1": "domain-c117",
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114, datastore205, datastore203, datastore204},
			},
			topologyRequirement: createTopologyRequirement([]string{"zone-1"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{
				datastore114, datastore205, datastore203, datastore204,
			},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1": {datastore114, datastore205, datastore203, datastore204},
			},
		},
		{
			name:           "Empty topology requirement",
			isPodVMEnabled: false,
			azClusterMap:   map[string]string{"zone-1": "domain-c117"},
			topologyRequirement: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{},
			},
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{},
			expectedTopoSegMap:       map[string][]*cnsvsphere.DatastoreInfo{},
		},
		{
			name:           "Zone with no datastores",
			isPodVMEnabled: false,
			azClusterMap:   map[string]string{"zone-1": "domain-c117"},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {}, // Empty slice
			},
			topologyRequirement:      createTopologyRequirement([]string{"zone-1"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1": {},
			},
		},
		{
			name:           "Multiple clusters with no shared datastores",
			isPodVMEnabled: true,
			azClustersMap: map[string][]string{
				"zone-1": {"domain-c117", "domain-c118"},
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {createDatastoreInfo("datastore-205", "ds:///vmfs/volumes/d3cec85f-c18d869d/")},
				"domain-c118": {createDatastoreInfo("datastore-203", "ds:///vmfs/volumes/d1377884-6e1032bb/")},
			},
			topologyRequirement:      createTopologyRequirement([]string{"zone-1"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1": {},
			},
		},
		{
			name:           "Multiple zones with mixed results",
			isPodVMEnabled: false,
			azClusterMap: map[string]string{
				"zone-1":       "domain-c117",
				"zone-2":       "domain-c128",
				"zone-unknown": "domain-c999", // This zone won't have datastores
			},
			clusterDatastores: map[string][]*cnsvsphere.DatastoreInfo{
				"domain-c117": {datastore114},
				"domain-c128": {datastore114},
				// domain-c999 not in clusterDatastores map
			},
			topologyRequirement: createTopologyRequirement([]string{"zone-1", "zone-2", "zone-unknown"}),
			expectedSharedDatastores: []*cnsvsphere.DatastoreInfo{
				datastore114, datastore114, // datastore114 from zone-1 and zone-2
			},
			expectedTopoSegMap: map[string][]*cnsvsphere.DatastoreInfo{
				"zone-1":       {datastore114},
				"zone-2":       {datastore114},
				"zone-unknown": {}, // Empty but present
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up global variables to simulate the real environment
			originalIsPodVMEnabled := isPodVMOnStretchedSupervisorEnabled
			originalAzClusterMap := azClusterMap
			originalAzClustersMap := azClustersMap

			// Set test values
			isPodVMOnStretchedSupervisorEnabled = tt.isPodVMEnabled
			azClusterMap = tt.azClusterMap
			azClustersMap = tt.azClustersMap

			// Restore original values after test
			defer func() {
				isPodVMOnStretchedSupervisorEnabled = originalIsPodVMEnabled
				azClusterMap = originalAzClusterMap
				azClustersMap = originalAzClustersMap
			}()

			// Create mock VC
			var mockVC *cnsvsphere.VirtualCenter

			// Create mock datastore retriever with test data
			mockRetriever := &MockDatastoreRetriever{
				ClusterDatastores: tt.clusterDatastores,
			}

			// Create a wcpControllerVolumeTopology instance with dependency injection
			wcpTopology := &wcpControllerVolumeTopology{
				datastoreRetriever: mockRetriever,
			}

			// Prepare test parameters
			topoSegToDatastoresMap := make(map[string][]*cnsvsphere.DatastoreInfo)
			params := commoncotypes.WCPTopologyFetchDSParams{
				TopologyRequirement:    tt.topologyRequirement,
				Vc:                     mockVC,
				TopoSegToDatastoresMap: topoSegToDatastoresMap,
			}

			// Execute the function
			result, err := wcpTopology.GetSharedDatastoresInTopology(ctx, params)

			// Validate results
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
				return
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Check shared datastores count
			expectedDatastoreCount := len(tt.expectedSharedDatastores)
			if tt.expectedDatastores != nil {
				expectedDatastoreCount = *tt.expectedDatastores
			}
			if len(result) != expectedDatastoreCount {
				t.Errorf("GetSharedDatastoresInTopology() returned %d datastores, expected %d",
					len(result), expectedDatastoreCount)
			}

			// Check TopoSegToDatastoresMap is populated correctly
			expectedZoneCount := len(tt.expectedTopoSegMap)
			if tt.expectedZones != nil {
				expectedZoneCount = *tt.expectedZones
			}
			if len(topoSegToDatastoresMap) != expectedZoneCount {
				t.Errorf("TopoSegToDatastoresMap has %d zones, expected %d",
					len(topoSegToDatastoresMap), expectedZoneCount)
			}

			// Validate each zone in TopoSegToDatastoresMap
			for zone, expectedDatastores := range tt.expectedTopoSegMap {
				actualDatastores, exists := topoSegToDatastoresMap[zone]
				if !exists {
					t.Errorf("TopoSegToDatastoresMap missing zone %s", zone)
					continue
				}

				if len(actualDatastores) != len(expectedDatastores) {
					t.Errorf("Zone %s has %d datastores, expected %d",
						zone, len(actualDatastores), len(expectedDatastores))
					continue
				}

				// Check that all expected datastores are present
				for _, expectedDS := range expectedDatastores {
					found := false
					for _, actualDS := range actualDatastores {
						if actualDS.Info.Url == expectedDS.Info.Url {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Zone %s missing expected datastore %s", zone, expectedDS.Info.Url)
					}
				}
			}

			// Validate that shared datastores contain expected datastores
			for _, expectedDS := range tt.expectedSharedDatastores {
				found := false
				for _, actualDS := range result {
					if actualDS.Info.Url == expectedDS.Info.Url {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Shared datastores missing expected datastore %s", expectedDS.Info.Url)
				}
			}
		})
	}
}

// makeTestAvailabilityZoneUnstructured builds a minimal AvailabilityZone-shaped object for informer handler tests.
func makeTestAvailabilityZoneUnstructured(t *testing.T, name string, clusterMoIDs []string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := unstructured.SetNestedField(u.Object, name, "metadata", "name"); err != nil {
		t.Fatalf("SetNestedField metadata.name: %v", err)
	}
	err := unstructured.SetNestedStringSlice(
		u.Object, clusterMoIDs, "spec", "clusterComputeResourceMoIDs")
	if err != nil {
		t.Fatalf("SetNestedStringSlice spec.clusterComputeResourceMoIDs: %v", err)
	}
	return u
}

// TestAzCRUpdated_MultipleClustersPerZone tests MCZ: when a new cluster is added to an existing
// AvailabilityZone (in-place CR update), azClustersMap must include the new cluster so datastore
// intersection and register-volume topology can see datastores from that cluster.
func TestAzCRUpdated_MultipleClustersPerZone(t *testing.T) {
	origPodVM := isPodVMOnStretchedSupervisorEnabled
	origAz := azClusterMap
	origAzs := azClustersMap
	defer func() {
		isPodVMOnStretchedSupervisorEnabled = origPodVM
		azClusterMap = origAz
		azClustersMap = origAzs
	}()

	isPodVMOnStretchedSupervisorEnabled = true
	azClusterMap = make(map[string]string)
	zoneName := "wcp-vpc-compute-cluster-domain"
	azClustersMap = map[string][]string{
		zoneName: {"domain-c58"},
	}

	oldObj := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-c58"})
	newObj := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-c58", "domain-c51"})

	azCRUpdated(oldObj, newObj)

	got := azClustersMap[zoneName]
	want := []string{"domain-c58", "domain-c51"}
	if !slices.Equal(got, want) {
		t.Fatalf("azClustersMap[%q] = %v, want %v", zoneName, got, want)
	}
}

// TestAzCRUpdated_SingleClusterModeFirstElement tests non-MCZ path: cache stores the first cluster MOID only;
// updates must refresh when that first entry changes.
func TestAzCRUpdated_SingleClusterModeFirstElement(t *testing.T) {
	origPodVM := isPodVMOnStretchedSupervisorEnabled
	origAz := azClusterMap
	origAzs := azClustersMap
	defer func() {
		isPodVMOnStretchedSupervisorEnabled = origPodVM
		azClusterMap = origAz
		azClustersMap = origAzs
	}()

	isPodVMOnStretchedSupervisorEnabled = false
	azClustersMap = make(map[string][]string)
	zoneName := "zone-a"
	azClusterMap = map[string]string{zoneName: "domain-old"}

	oldObj := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-old"})
	newObj := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-new", "domain-old"})

	azCRUpdated(oldObj, newObj)

	if got := azClusterMap[zoneName]; got != "domain-new" {
		t.Fatalf("azClusterMap[%q] = %q, want domain-new (first spec.clusterComputeResourceMoID)", zoneName, got)
	}
}

// TestAzCRUpdated_NoChangeSkipsCacheWrite verifies identical old/new spec does not require a spec change
// (equivalent cluster lists are a no-op for the update path).
func TestAzCRUpdated_NoChangeSkipsCacheWrite(t *testing.T) {
	origPodVM := isPodVMOnStretchedSupervisorEnabled
	origAz := azClusterMap
	origAzs := azClustersMap
	defer func() {
		isPodVMOnStretchedSupervisorEnabled = origPodVM
		azClusterMap = origAz
		azClustersMap = origAzs
	}()

	isPodVMOnStretchedSupervisorEnabled = true
	azClusterMap = make(map[string]string)
	zoneName := "zone-unchanged"
	origSlice := []string{"domain-c1", "domain-c2"}
	azClustersMap = map[string][]string{zoneName: origSlice}

	u := makeTestAvailabilityZoneUnstructured(t, zoneName, origSlice)
	azCRUpdated(u, u)

	if got := azClustersMap[zoneName]; !slices.Equal(got, origSlice) {
		t.Fatalf("azClustersMap[%q] = %v, want unchanged %v", zoneName, got, origSlice)
	}
}

// TestGetTopologyInfoFromNodes_CnsRegisterVolumeSucceedsWhenClusterAddedToZone covers MCZ: a static volume on a
// datastore that exists only after a new vSphere cluster is added to the zone cannot be resolved until
// azCRUpdated applies the in-place AvailabilityZone spec change; then GetTopologyInfoFromNodes returns the zone.
func TestGetTopologyInfoFromNodes_CnsRegisterVolumeSucceedsWhenClusterAddedToZone(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		t.Skip("Skipping test on ARM64 due to gomonkey function patching limitations")
	}
	ctx := context.Background()
	origPodVM := isPodVMOnStretchedSupervisorEnabled
	origAz := azClusterMap
	origAzs := azClustersMap
	defer func() {
		isPodVMOnStretchedSupervisorEnabled = origPodVM
		azClusterMap = origAz
		azClustersMap = origAzs
	}()

	zoneName := "wcp-vpc-compute-cluster-zone"
	// Datastore only visible once domain-c51 is in the zone's cluster list (e.g. volume on new cluster's vSAN).
	newClusterOnlyDatastoreURL := "ds:///vmfs/volumes/vsan-new-c51-only/"

	patches := gomonkey.ApplyFunc(getSharedDatastoresInClusters, func(
		_ context.Context, clusterMorefs []string, _ *cnsvsphere.VirtualCenter,
	) ([]*cnsvsphere.DatastoreInfo, error) {
		existingURL := "ds:///vmfs/volumes/vsan-c58-shared/"
		out := []*cnsvsphere.DatastoreInfo{createDatastoreInfo("ds-c58", existingURL)}
		for _, m := range clusterMorefs {
			if m == "domain-c51" {
				out = append(out, createDatastoreInfo("ds-c51", newClusterOnlyDatastoreURL))
				break
			}
		}
		return out, nil
	})
	defer patches.Reset()

	isPodVMOnStretchedSupervisorEnabled = true
	azClusterMap = make(map[string]string)
	// Stale cache: only the original cluster (second cluster not yet applied by informer).
	azClustersMap = map[string][]string{zoneName: {"domain-c58"}}

	wcpTopo := &wcpControllerVolumeTopology{}
	params := commoncotypes.WCPRetrieveTopologyInfoParams{
		DatastoreURL:           newClusterOnlyDatastoreURL,
		StorageTopologyType:    "zonal",
		TopologyRequirement:    nil,
		Vc:                     nil,
		TopoSegToDatastoresMap: nil,
	}

	_, err := wcpTopo.GetTopologyInfoFromNodes(ctx, params)
	if err == nil {
		t.Fatal("GetTopologyInfoFromNodes with stale cache: want error, got nil")
	}
	if !strings.Contains(err.Error(), "could not find the topology of the volume provisioned on datastore") {
		t.Fatalf("expected topology-not-found with stale cache, got: %v", err)
	}

	oldCR := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-c58"})
	newCR := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-c58", "domain-c51"})
	azCRUpdated(oldCR, newCR)

	segments, err := wcpTopo.GetTopologyInfoFromNodes(ctx, params)
	if err != nil {
		t.Fatalf("GetTopologyInfoFromNodes after cluster add: unexpected error: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("len(segments) = %d, want 1: %+v", len(segments), segments)
	}
	if got := segments[0][v1.LabelTopologyZone]; got != zoneName {
		t.Fatalf("zone = %q, want %q", got, zoneName)
	}
}

// TestGetTopologyInfoFromNodes_CnsRegisterVolumeFailsWhenClusterRemovedFromZone mirrors the CnsRegisterVolume
// control path: GetTopologyInfoFromNodes with zonal + nil topology requirement, using azClustersMap. When
// a vSphere cluster is removed from the AvailabilityZone (cache updated by azCRUpdated) but the volume
// still sits on a datastore that was only part of the removed cluster, registration must not succeed with
// a made-up zone—the same "could not find the topology" error the controller returns.
func TestGetTopologyInfoFromNodes_CnsRegisterVolumeFailsWhenClusterRemovedFromZone(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		t.Skip("Skipping test on ARM64 due to gomonkey function patching limitations")
	}
	ctx := context.Background()
	origPodVM := isPodVMOnStretchedSupervisorEnabled
	origAz := azClusterMap
	origAzs := azClustersMap
	defer func() {
		isPodVMOnStretchedSupervisorEnabled = origPodVM
		azClusterMap = origAz
		azClustersMap = origAzs
	}()

	zoneName := "wcp-vpc-compute-cluster-zone"
	// URL only present when the removed vSphere cluster is still in the zone's cluster list.
	removedClusterOnlyDatastoreURL := "ds:///vmfs/volumes/vsan-removed-c51-only/"

	patches := gomonkey.ApplyFunc(getSharedDatastoresInClusters, func(
		_ context.Context, clusterMorefs []string, _ *cnsvsphere.VirtualCenter,
	) ([]*cnsvsphere.DatastoreInfo, error) {
		remainingURL := "ds:///vmfs/volumes/vsan-c58-shared/"
		out := []*cnsvsphere.DatastoreInfo{createDatastoreInfo("ds-remaining", remainingURL)}
		for _, m := range clusterMorefs {
			if m == "domain-c51" {
				out = append(out, createDatastoreInfo("ds-on-removed", removedClusterOnlyDatastoreURL))
				break
			}
		}
		return out, nil
	})
	defer patches.Reset()

	isPodVMOnStretchedSupervisorEnabled = true
	azClusterMap = make(map[string]string)
	azClustersMap = map[string][]string{zoneName: {"domain-c58", "domain-c51"}}

	wcpTopo := &wcpControllerVolumeTopology{}
	params := commoncotypes.WCPRetrieveTopologyInfoParams{
		DatastoreURL:           removedClusterOnlyDatastoreURL,
		StorageTopologyType:    "zonal",
		TopologyRequirement:    nil,
		Vc:                     nil,
		TopoSegToDatastoresMap: nil,
	}

	_, err := wcpTopo.GetTopologyInfoFromNodes(ctx, params)
	if err != nil {
		t.Fatalf("GetTopologyInfoFromNodes (both clusters in zone) unexpected error: %v", err)
	}

	// In-place CR update: domain-c51 removed from spec.clusterComputeResourceMoIDs; cache matches production.
	oldCR := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-c58", "domain-c51"})
	newCR := makeTestAvailabilityZoneUnstructured(t, zoneName, []string{"domain-c58"})
	azCRUpdated(oldCR, newCR)

	_, err = wcpTopo.GetTopologyInfoFromNodes(ctx, params)
	if err == nil {
		t.Fatalf("GetTopologyInfoFromNodes after cluster removal: want error " +
			"(CnsRegisterVolume would succeed incorrectly), got nil")
	}
	if !strings.Contains(err.Error(), "could not find the topology of the volume provisioned on datastore") {
		t.Fatalf("expected static-provision / CnsRegisterVolume style topology error, got: %v", err)
	}
}
