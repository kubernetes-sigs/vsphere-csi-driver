/*
Copyright 2024 The Kubernetes Authors.

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
	"slices"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

func TestGenerateVolumeNodeAffinity(t *testing.T) {
	tests := []struct {
		name               string
		accessibleTopology []*csi.Topology
		expected           *v1.VolumeNodeAffinity
	}{
		{
			name: "Basic test with one topology",
			accessibleTopology: []*csi.Topology{
				{Segments: map[string]string{"topology.kubernetes.io/zone": "zone-1"}},
			},
			expected: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"zone-1"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Multiple topologies with different segments",
			accessibleTopology: []*csi.Topology{
				{Segments: map[string]string{"topology.kubernetes.io/zone": "zone-1"}},
				{Segments: map[string]string{"topology.kubernetes.io/zone": "zone-2"}},
			},
			expected: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"zone-1"},
								},
							},
						},
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "topology.kubernetes.io/zone",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"zone-2"},
								},
							},
						},
					},
				},
			},
		},
		{
			name:               "Empty topology list",
			accessibleTopology: []*csi.Topology{},
			expected:           nil,
		},
		{
			name: "Topology with empty segments",
			accessibleTopology: []*csi.Topology{
				{Segments: map[string]string{}},
			},
			expected: &v1.VolumeNodeAffinity{
				Required: &v1.NodeSelector{
					NodeSelectorTerms: nil, // No terms should be added
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateVolumeNodeAffinity(tt.accessibleTopology)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Owner-reference apiVersions a CnsVolumeMetadata can carry. A legacy guest cluster is
// owned by a TanzuKubernetesCluster; a non-legacy one by a Cluster API Cluster, whose
// served version changes over time (v1beta1 is unserved from VKS 3.10 onwards).
const (
	cleanupLegacyTKCAPIVersion = "run.tanzu.vmware.com/v1alpha1"
	cleanupCAPIV1beta1         = "cluster.x-k8s.io/v1beta1"
	cleanupCAPIV1beta2         = "cluster.x-k8s.io/v1beta2"

	cleanupThisGCUID   = "guest-cluster-a-uid"
	cleanupThisGCName  = "guest-cluster-a"
	cleanupLegacyGCUID = "guest-cluster-a-legacy-tkc-uid"
	cleanupOtherGCUID  = "guest-cluster-b-uid"
	cleanupOtherGCName = "guest-cluster-b"
	cleanupSupervisor  = "supervisor-ns"
)

// cleanupOwnedBy sets the name of the single owner reference of a test CnsVolumeMetadata.
func cleanupOwnedBy(cvm *cnsvolumemetadatav1alpha1.CnsVolumeMetadata,
	ownerName string) *cnsvolumemetadatav1alpha1.CnsVolumeMetadata {
	cvm.OwnerReferences[0].Name = ownerName
	return cvm
}

// newCleanupTestCVM builds a CnsVolumeMetadata for the cleanUpCnsVolumeMetadata tests.
// An empty ownerAPIVersion means the CR carries no owner references at all.
func newCleanupTestCVM(name, guestClusterID, entityName, entityNamespace string,
	entityType cnsvolumemetadatav1alpha1.CnsOperatorEntityType,
	ownerAPIVersion string) *cnsvolumemetadatav1alpha1.CnsVolumeMetadata {
	cvm := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cleanupSupervisor,
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			GuestClusterID: guestClusterID,
			EntityName:     entityName,
			Namespace:      entityNamespace,
			EntityType:     entityType,
		},
	}
	if ownerAPIVersion != "" {
		kind := "Cluster"
		if ownerAPIVersion == cleanupLegacyTKCAPIVersion {
			kind = "TanzuKubernetesCluster"
		}
		cvm.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: ownerAPIVersion,
			Kind:       kind,
			Name:       "owning-cluster",
		}}
	}
	return cvm
}

// TestCleanUpCnsVolumeMetadata covers the legacy-leftover cleanup performed on every pvcsi
// full sync. The cleanup must delete duplicate CnsVolumeMetadata left behind by the legacy
// to non-legacy guest cluster migration, without touching CRs owned by a Cluster API
// Cluster at any served apiVersion, and without ever looking at another guest cluster's CRs.
func TestCleanUpCnsVolumeMetadata(t *testing.T) {
	const (
		pvcEntity = cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC
		podEntity = cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD
	)

	tests := []struct {
		name string
		objs []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata
		// emptyGCName runs the cleanup with no guest cluster name configured.
		emptyGCName bool
		// kept lists the CR names expected to still exist after the cleanup.
		kept []string
	}{
		{
			// The guest cluster ID changed on upgrade (TKC UID to Cluster UID), so the leftover
			// carries the old ID and is recognised by the name of its owner.
			name: "upgrade leftover with the old cluster ID is deleted, current CR on v1beta1 is kept",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				cleanupOwnedBy(newCleanupTestCVM("leftover", cleanupLegacyGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion), cleanupThisGCName),
				cleanupOwnedBy(newCleanupTestCVM("current", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta1), cleanupThisGCName),
			},
			kept: []string{"current"},
		},
		{
			// Guards against wiping the cluster when the current CR is already on v1beta2.
			name: "upgrade leftover with the old cluster ID is deleted, current CR on v1beta2 is kept",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				cleanupOwnedBy(newCleanupTestCVM("leftover", cleanupLegacyGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion), cleanupThisGCName),
				cleanupOwnedBy(newCleanupTestCVM("current", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta2), cleanupThisGCName),
			},
			kept: []string{"current"},
		},
		{
			name: "upgrade leftover without a current duplicate is kept",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				cleanupOwnedBy(newCleanupTestCVM("leftover", cleanupLegacyGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion), cleanupThisGCName),
			},
			kept: []string{"leftover"},
		},
		{
			// Another cluster has a different ID and a different name, so its CRs are not ours
			// even when the entity matches one of ours.
			name: "another guest cluster's legacy CR is kept when this cluster has the same entity",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				cleanupOwnedBy(newCleanupTestCVM("theirs-legacy", cleanupOtherGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion), cleanupOtherGCName),
				cleanupOwnedBy(newCleanupTestCVM("current", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta2), cleanupThisGCName),
			},
			kept: []string{"theirs-legacy", "current"},
		},
		{
			// With no cluster name configured an empty owner name must not match every CR.
			name:        "no configured cluster name does not match leftovers by owner name",
			emptyGCName: true,
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				cleanupOwnedBy(newCleanupTestCVM("leftover", cleanupLegacyGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion), ""),
				cleanupOwnedBy(newCleanupTestCVM("current", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta2), cleanupThisGCName),
			},
			kept: []string{"leftover", "current"},
		},
		{
			name: "legacy owner without a duplicate is kept",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("legacy-only", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion),
			},
			kept: []string{"legacy-only"},
		},
		{
			name: "CAPI v1beta1 owners are kept even when duplicated",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("beta1-a", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta1),
				newCleanupTestCVM("beta1-b", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta1),
			},
			kept: []string{"beta1-a", "beta1-b"},
		},
		{
			// Regression test: an exact-match check against the old v1beta1 apiVersion would
			// classify both of these as legacy and delete the live metadata.
			name: "CAPI v1beta2 owners are kept even when duplicated",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("beta2-a", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta2),
				newCleanupTestCVM("beta2-b", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta2),
			},
			kept: []string{"beta2-a", "beta2-b"},
		},
		{
			// Regression test for cross-guest-cluster scoping: entity names are only unique
			// within a guest cluster, so counting duplicates across clusters would make each
			// cluster delete the other's metadata.
			name: "same entity in two guest clusters, both legacy, is not a duplicate",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("mine", cleanupThisGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion),
				newCleanupTestCVM("theirs", cleanupOtherGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion),
			},
			kept: []string{"mine", "theirs"},
		},
		{
			name: "another guest cluster's duplicates are never deleted by this syncer",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("theirs-legacy", cleanupOtherGCUID, "data-0", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion),
				newCleanupTestCVM("theirs-current", cleanupOtherGCUID, "data-0", "default",
					pvcEntity, cleanupCAPIV1beta2),
			},
			kept: []string{"theirs-legacy", "theirs-current"},
		},
		{
			// The duplicate key includes EntityType, so a Pod and a PVC that happen to share
			// a name in one namespace are distinct entities rather than duplicates.
			name: "PVC and POD sharing a name in one namespace are not duplicates",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("pvc-data", cleanupThisGCUID, "data", "default",
					pvcEntity, cleanupLegacyTKCAPIVersion),
				newCleanupTestCVM("pod-data", cleanupThisGCUID, "data", "default",
					podEntity, cleanupLegacyTKCAPIVersion),
			},
			kept: []string{"pvc-data", "pod-data"},
		},
		{
			name: "CRs without owner references are kept",
			objs: []*cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
				newCleanupTestCVM("no-owner-a", cleanupThisGCUID, "data-0", "default",
					pvcEntity, ""),
				newCleanupTestCVM("no-owner-b", cleanupThisGCUID, "data-0", "default",
					pvcEntity, ""),
			},
			kept: []string{"no-owner-a", "no-owner-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			scheme := runtime.NewScheme()
			require.NoError(t, cnsoperatorapis.AddToScheme(scheme))

			list := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
			builder := fake.NewClientBuilder().WithScheme(scheme)
			for _, o := range tt.objs {
				builder = builder.WithObjects(o.DeepCopy())
				list.Items = append(list.Items, *o.DeepCopy())
			}

			gcName := cleanupThisGCName
			if tt.emptyGCName {
				gcName = ""
			}
			metadataSyncer := &metadataSyncInformer{
				cnsOperatorClient: builder.Build(),
				configInfo: &cnsconfig.ConfigurationInfo{
					Cfg: &cnsconfig.Config{
						GC: cnsconfig.GCConfig{
							TanzuKubernetesClusterUID:  cleanupThisGCUID,
							TanzuKubernetesClusterName: gcName,
						},
					},
				},
			}

			cleanUpCnsVolumeMetadata(ctx, metadataSyncer, list)

			remaining := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
			require.NoError(t, metadataSyncer.cnsOperatorClient.List(ctx, remaining,
				client.InNamespace(cleanupSupervisor)))
			var got []string
			for _, item := range remaining.Items {
				got = append(got, item.Name)
			}
			slices.Sort(got)
			want := slices.Clone(tt.kept)
			slices.Sort(want)
			assert.Equal(t, want, got, "unexpected set of surviving CnsVolumeMetadata CRs")
		})
	}
}
