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

package k8scloudoperator

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/storagepool/cns/v1alpha1"
)

func TestStoragePoolInfoSorting(t *testing.T) {
	spList := byCOMBINATION{
		{Name: "sp-small", AllocatableCapInBytes: 1000},
		{Name: "sp-large", AllocatableCapInBytes: 5000},
		{Name: "sp-medium", AllocatableCapInBytes: 3000},
		{Name: "sp-same-1", AllocatableCapInBytes: 2000},
		{Name: "sp-same-2", AllocatableCapInBytes: 2000},
	}

	// Test Len
	assert.Equal(t, 5, spList.Len())

	// Test Less - should sort by capacity descending, then by name ascending
	assert.True(t, spList.Less(1, 0)) // sp-large (5000) > sp-small (1000)
	assert.True(t, spList.Less(1, 2)) // sp-large (5000) > sp-medium (3000)
	assert.True(t, spList.Less(3, 4)) // sp-same-1 < sp-same-2 (same capacity, name comparison)

	// Test Swap
	originalFirst := spList[0]
	originalSecond := spList[1]
	spList.Swap(0, 1)
	assert.Equal(t, originalSecond, spList[0])
	assert.Equal(t, originalFirst, spList[1])
}

func TestIsSPInList(t *testing.T) {
	spList := []StoragePoolInfo{
		{Name: "sp-1", AllocatableCapInBytes: 1000},
		{Name: "sp-2", AllocatableCapInBytes: 2000},
		{Name: "sp-3", AllocatableCapInBytes: 3000},
	}

	tests := []struct {
		name     string
		spName   string
		expected bool
	}{
		{
			name:     "Storage pool exists in list",
			spName:   "sp-2",
			expected: true,
		},
		{
			name:     "Storage pool does not exist in list",
			spName:   "sp-4",
			expected: false,
		},
		{
			name:     "Empty storage pool name",
			spName:   "",
			expected: false,
		},
		{
			name:     "Case sensitive check",
			spName:   "SP-1",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSPInList(tt.spName, spList)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterHost(t *testing.T) {
	tests := []struct {
		name     string
		hosts    []string
		index    int
		expected []string
	}{
		{
			name:     "Remove first host",
			hosts:    []string{"host1", "host2", "host3"},
			index:    0,
			expected: []string{"host2", "host3"},
		},
		{
			name:     "Remove middle host",
			hosts:    []string{"host1", "host2", "host3"},
			index:    1,
			expected: []string{"host1", "host3"},
		},
		{
			name:     "Remove last host",
			hosts:    []string{"host1", "host2", "host3"},
			index:    2,
			expected: []string{"host1", "host2"},
		},
		{
			name:     "Single host list",
			hosts:    []string{"host1"},
			index:    0,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterHost(tt.hosts, tt.index)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetPvcPrefixAndParentReplicaID(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		pvcName        string
		expectedPrefix string
		expectedID     int
		expectError    bool
	}{
		{
			name:           "Valid PVC name with replica ID",
			pvcName:        "data-myapp-0",
			expectedPrefix: "data-myapp",
			expectedID:     0,
			expectError:    false,
		},
		{
			name:           "Valid PVC name with higher replica ID",
			pvcName:        "storage-db-5",
			expectedPrefix: "storage-db",
			expectedID:     5,
			expectError:    false,
		},
		{
			name:        "Invalid PVC name without replica ID",
			pvcName:     "invalid-pvc-name",
			expectError: true,
		},
		{
			name:        "Empty PVC name",
			pvcName:     "",
			expectError: true,
		},
		{
			name:        "PVC name ending with non-numeric",
			pvcName:     "data-myapp-abc",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix, id, err := getPvcPrefixAndParentReplicaID(ctx, tt.pvcName)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedPrefix, prefix)
				assert.Equal(t, tt.expectedID, id)
			}
		})
	}
}

func TestRemoveSPFromList(t *testing.T) {
	originalList := []StoragePoolInfo{
		{Name: "sp-1", AllocatableCapInBytes: 1000},
		{Name: "sp-2", AllocatableCapInBytes: 2000},
		{Name: "sp-3", AllocatableCapInBytes: 3000},
	}

	tests := []struct {
		name         string
		spList       []StoragePoolInfo
		spName       string
		expectedLen  int
		shouldRemove bool
	}{
		{
			name:         "Remove existing storage pool",
			spList:       originalList,
			spName:       "sp-2",
			expectedLen:  2,
			shouldRemove: true,
		},
		{
			name:         "Remove non-existing storage pool",
			spList:       originalList,
			spName:       "sp-4",
			expectedLen:  3,
			shouldRemove: false,
		},
		{
			name:         "Remove from empty list",
			spList:       []StoragePoolInfo{},
			spName:       "sp-1",
			expectedLen:  0,
			shouldRemove: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeSPFromList(tt.spList, tt.spName)
			assert.Equal(t, tt.expectedLen, len(result))

			if tt.shouldRemove {
				// Verify the specific item was removed
				for _, sp := range result {
					assert.NotEqual(t, tt.spName, sp.Name)
				}
			}
		})
	}
}

func TestUpdateSPCapacityUsage(t *testing.T) {
	spList := []StoragePoolInfo{
		{Name: "sp-1", AllocatableCapInBytes: 5000},
		{Name: "sp-2", AllocatableCapInBytes: 3000},
		{Name: "sp-3", AllocatableCapInBytes: 2000},
	}

	tests := []struct {
		name             string
		spName           string
		pendingPVBytes   int64
		expectedCapacity int64
		shouldUpdateList bool
	}{
		{
			name:             "Update existing storage pool capacity",
			spName:           "sp-1",
			pendingPVBytes:   1000,
			expectedCapacity: 4000, // 5000 - 1000
			shouldUpdateList: true,
		},
		{
			name:             "Update non-existing storage pool",
			spName:           "sp-4",
			pendingPVBytes:   500,
			shouldUpdateList: false,
		},
		{
			name:             "Update with zero bytes",
			spName:           "sp-2",
			pendingPVBytes:   0,
			expectedCapacity: 3000, // No change
			shouldUpdateList: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the original list for each test
			testList := make([]StoragePoolInfo, len(spList))
			copy(testList, spList)

			usageUpdated, spRemoved, result := updateSPCapacityUsage(testList, tt.spName, tt.pendingPVBytes, 0)

			if tt.shouldUpdateList {
				assert.True(t, usageUpdated, "Usage should be updated")
				assert.False(t, spRemoved, "Storage pool should not be removed")
				// Find the updated storage pool
				var found bool
				for _, sp := range result {
					if sp.Name == tt.spName {
						assert.Equal(t, tt.expectedCapacity, sp.AllocatableCapInBytes)
						found = true
						break
					}
				}
				assert.True(t, found, "Storage pool should be found and updated")
			} else {
				assert.False(t, usageUpdated, "Usage should not be updated for non-existing SP")
			}
		})
	}
}

func TestGetSCNameFromPVC(t *testing.T) {
	tests := []struct {
		name        string
		pvc         *v1.PersistentVolumeClaim
		expected    string
		expectError bool
	}{
		{
			name: "PVC with storage class in spec",
			pvc: &v1.PersistentVolumeClaim{
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr("fast-ssd"),
				},
			},
			expected:    "fast-ssd",
			expectError: false,
		},
		{
			name: "PVC with storage class annotation",
			pvc: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						ScNameAnnotationKey: "slow-hdd",
					},
				},
			},
			expected:    "slow-hdd",
			expectError: false,
		},
		{
			name: "PVC with both spec and annotation (spec takes precedence)",
			pvc: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						ScNameAnnotationKey: "annotation-sc",
					},
				},
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: stringPtr("spec-sc"),
				},
			},
			expected:    "spec-sc",
			expectError: false,
		},
		{
			name: "PVC without storage class",
			pvc: &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pvc",
				},
			},
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetSCNameFromPVC(tt.pvc)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetHostNamesFromTopology(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		topology *csi.TopologyRequirement
		expected []string
	}{
		{
			name: "Topology with preferred segments",
			topology: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{
						Segments: map[string]string{
							"topology.kubernetes.io/zone": "zone-a",
							"kubernetes.io/hostname":      "node-1",
						},
					},
					{
						Segments: map[string]string{
							"topology.kubernetes.io/zone": "zone-b",
							"kubernetes.io/hostname":      "node-2",
						},
					},
				},
			},
			expected: []string{"node-1", "node-2"},
		},
		{
			name: "Topology with requisite segments (no preferred)",
			topology: &csi.TopologyRequirement{
				Requisite: []*csi.Topology{
					{
						Segments: map[string]string{
							"kubernetes.io/hostname": "node-3",
						},
					},
				},
			},
			expected: []string{}, // Function only looks at Preferred, not Requisite
		},
		{
			name: "Topology without hostname segments",
			topology: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{
					{
						Segments: map[string]string{
							"topology.kubernetes.io/zone": "zone-a",
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name:     "Nil topology",
			topology: nil,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getHostNamesFromTopology(ctx, tt.topology)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestIsHostPresentInTopology(t *testing.T) {
	tests := []struct {
		name          string
		affinityHost  string
		topologyHosts []string
		expected      bool
	}{
		{
			name:          "Host present in topology",
			affinityHost:  "node-1",
			topologyHosts: []string{"node-1", "node-2", "node-3"},
			expected:      true,
		},
		{
			name:          "Host not present in topology",
			affinityHost:  "node-4",
			topologyHosts: []string{"node-1", "node-2", "node-3"},
			expected:      false,
		},
		{
			name:          "Empty topology hosts",
			affinityHost:  "node-1",
			topologyHosts: []string{},
			expected:      false,
		},
		{
			name:          "Empty affinity host",
			affinityHost:  "",
			topologyHosts: []string{"node-1", "node-2"},
			expected:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isHostPresentInTopology(tt.affinityHost, tt.topologyHosts)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsStoragePoolHealthy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		sp       v1alpha1.StoragePool
		expected bool
	}{
		{
			name: "Healthy storage pool (no error)",
			sp: v1alpha1.StoragePool{
				Status: v1alpha1.StoragePoolStatus{
					Error: nil, // No error means healthy
				},
			},
			expected: true,
		},
		{
			name: "Unhealthy storage pool (has error)",
			sp: v1alpha1.StoragePool{
				Status: v1alpha1.StoragePoolStatus{
					Error: &v1alpha1.StoragePoolError{
						Message: "Storage pool is down",
					},
				},
			},
			expected: false,
		},
		{
			name: "Storage pool with empty status (no error field)",
			sp: v1alpha1.StoragePool{
				Status: v1alpha1.StoragePoolStatus{},
			},
			expected: true, // No error means healthy
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isStoragePoolHealthy(ctx, tt.sp)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConstants(t *testing.T) {
	assert.Equal(t, "failure-domain.beta.vmware.com/storagepool", StoragePoolAnnotationKey)
	assert.Equal(t, "volume.beta.kubernetes.io/storage-class", ScNameAnnotationKey)
	assert.Equal(t, "vsanD", vsanDirect)
	assert.Equal(t, "FAILED_PLACEMENT-InvalidParams", invalidParamsErr)
	assert.Equal(t, "FAILED_PLACEMENT-Generic", genericErr)
	assert.Equal(t, "FAILED_PLACEMENT-NotEnoughResources", notEnoughResErr)
	assert.Equal(t, "FAILED_PLACEMENT-InvalidConfiguration", invalidConfigErr)
	assert.Equal(t, "FAILED_PLACEMENT-HasSiblingReplicaBoundPVC", siblingReplicaBoundPVCErr)
	assert.Equal(t, "vsan-sna", vsanSna)
	assert.Equal(t, "appplatform.vmware.com/instance-id", appplatformLabel)
	assert.Equal(t, "psp.vmware.com/sibling-replica-check-opt-out", siblingReplicaCheckOptOutLabel)
	assert.Equal(t, "volume.kubernetes.io/selected-node", pvcSelectedNode)
}

func TestStoragePoolInfoStruct(t *testing.T) {
	sp := StoragePoolInfo{
		Name:                  "test-sp",
		AllocatableCapInBytes: 1000000,
	}

	assert.Equal(t, "test-sp", sp.Name)
	assert.Equal(t, int64(1000000), sp.AllocatableCapInBytes)
}

func TestGetVolumesOnStoragePool(t *testing.T) {
	ctx := context.Background()

	// Create fake Kubernetes client
	client := fake.NewSimpleClientset()

	// Test with empty client (no PVs)
	volumes, pvcs, err := GetVolumesOnStoragePool(ctx, client, "test-sp")
	assert.NoError(t, err)
	assert.Empty(t, volumes)
	assert.Empty(t, pvcs)
}

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}

// Helper function to create test PVC
func createTestPVC(name, namespace string, storageClass *string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: storageClass,
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

func TestVolumeInfoStruct(t *testing.T) {
	pvc := createTestPVC("test-pvc", "default", stringPtr("fast-ssd"))

	volumeInfo := VolumeInfo{
		PVC:         *pvc,
		PVName:      "test-pv",
		SizeInBytes: 1000000000, // 1GB
	}

	assert.Equal(t, "test-pvc", volumeInfo.PVC.Name)
	assert.Equal(t, "test-pv", volumeInfo.PVName)
	assert.Equal(t, int64(1000000000), volumeInfo.SizeInBytes)
}

// Test global mutex
func TestGlobalMutex(t *testing.T) {
	assert.NotNil(t, &pvcPlacementMutex)
}
