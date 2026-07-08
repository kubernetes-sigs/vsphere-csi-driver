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

package clusterstoragepolicyinfo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware/govmomi/object"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
)

// fakeDatastoreInfo builds a *cnsvsphere.DatastoreInfo whose Reference() is
// dsID, without needing a real vCenter connection: object.NewDatastore's
// Reference() only ever returns the moref it was constructed with.
func fakeDatastoreInfo(dsID string) *cnsvsphere.DatastoreInfo {
	return &cnsvsphere.DatastoreInfo{
		Datastore: &cnsvsphere.Datastore{
			Datastore: object.NewDatastore(nil, vimtypes.ManagedObjectReference{Type: "Datastore", Value: dsID}),
		},
	}
}

// withMockCompatibleDatastores replaces cnsoperatorutil.GetPolicyCompatibleDatastoresFn
// for the duration of the test, so populateTopologyCapabilities's PBM query
// resolves to a fixed set of datastore morefs without a real vCenter/PBM
// connection.
func withMockCompatibleDatastores(t *testing.T, dsIDs ...string) {
	t.Helper()
	orig := cnsoperatorutil.GetPolicyCompatibleDatastoresFn
	cnsoperatorutil.GetPolicyCompatibleDatastoresFn = func(ctx context.Context, vc *cnsvsphere.VirtualCenter,
		profileID string) (map[string]struct{}, error) {
		ids := make(map[string]struct{}, len(dsIDs))
		for _, id := range dsIDs {
			ids[id] = struct{}{}
		}
		return ids, nil
	}
	t.Cleanup(func() { cnsoperatorutil.GetPolicyCompatibleDatastoresFn = orig })
}

func TestPopulateTopologyCapabilities_PopulatesDsToPolicyCache(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "zonal-storage"},
		Parameters: map[string]string{
			"storagepolicyid":                   "profile-1",
			common.AttributeStorageTopologyType: "zonal",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sc).Build()

	withMockCompatibleDatastores(t, "ds-1")

	clusterDatastoreCache := map[string][]*cnsvsphere.DatastoreInfo{
		"cluster-1": {fakeDatastoreInfo("ds-1")},
		"cluster-2": {fakeDatastoreInfo("ds-2")},
	}

	r := &ReconcileClusterStoragePolicyInfo{
		client: k8sClient,
		scheme: scheme,
		topologyMgr: &mockControllerTopologyService{
			azClustersMap: map[string][]string{
				"zone-a": {"cluster-1"},
				"zone-b": {"cluster-2"},
			},
		},
	}

	clusterSPI := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
	}
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
	}
	// vsphereinfra.GetCache() is a process-wide singleton shared by every test
	// in this binary; clear this test's entry so it can't leak into others
	// that reuse the same datastore IDs.
	t.Cleanup(func() { vsphereinfra.GetCache().SetDatastoresForPolicy(clusterSPI.Name, nil) })

	err := r.populateTopologyCapabilities(ctx, clusterSPI, infraSPI, "profile-1", nil, clusterDatastoreCache, nil)
	require.NoError(t, err)

	// Only zone-a's cluster mounts the compatible datastore (ds-1).
	assert.ElementsMatch(t, []string{"zone-a"}, infraSPI.Status.Topology.AccessibleZones)

	assert.ElementsMatch(t, []string{"test-policy"}, vsphereinfra.GetCache().PoliciesForDatastore("ds-1"),
		"the datastore backing the policy's only accessible zone must be recorded in the shared cache")
	assert.Empty(t, vsphereinfra.GetCache().PoliciesForDatastore("ds-2"),
		"ds-2 is not compatible with this policy and must not be recorded")
}

func TestPopulateTopologyCapabilities_CacheEntryClearedWhenNoLongerCompatible(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	scheme := testScheme(t)

	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "zonal-storage"},
		Parameters: map[string]string{
			"storagepolicyid":                   "profile-1",
			common.AttributeStorageTopologyType: "zonal",
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sc).Build()

	clusterDatastoreCache := map[string][]*cnsvsphere.DatastoreInfo{
		"cluster-1": {fakeDatastoreInfo("ds-1")},
	}
	r := &ReconcileClusterStoragePolicyInfo{
		client: k8sClient,
		scheme: scheme,
		topologyMgr: &mockControllerTopologyService{
			azClustersMap: map[string][]string{"zone-a": {"cluster-1"}},
		},
	}
	clusterSPI := &clusterspiv1alpha1.ClusterStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy-2"},
	}
	t.Cleanup(func() { vsphereinfra.GetCache().SetDatastoresForPolicy(clusterSPI.Name, nil) })
	infraSPI := &infraspiv1alpha1.InfraStoragePolicyInfo{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy-2"},
	}

	// First reconcile: ds-1 is compatible.
	withMockCompatibleDatastores(t, "ds-1")
	require.NoError(t, r.populateTopologyCapabilities(ctx, clusterSPI, infraSPI, "profile-1", nil,
		clusterDatastoreCache, nil))
	require.ElementsMatch(t, []string{"test-policy-2"}, vsphereinfra.GetCache().PoliciesForDatastore("ds-1"))

	// Second reconcile: PBM now reports no compatible datastores at all.
	withMockCompatibleDatastores(t)
	require.NoError(t, r.populateTopologyCapabilities(ctx, clusterSPI, infraSPI, "profile-1", nil,
		clusterDatastoreCache, nil))

	assert.Empty(t, vsphereinfra.GetCache().PoliciesForDatastore("ds-1"),
		"stale cache entry must be cleared once the policy is no longer compatible with ds-1")
	assert.Empty(t, infraSPI.Status.Topology.AccessibleZones)
}
