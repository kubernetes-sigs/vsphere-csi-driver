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

package k8sorchestrator

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

// setPodVMV2Capability installs a capability cache for the test. When enabled
// is false the capability is omitted, modelling a Supervisor that predates it
// without triggering the late-enablement refresh against a live API server.
func setPodVMV2Capability(t *testing.T, enabled bool) {
	t.Helper()
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	WcpCapabilitiesMap = &sync.Map{}
	WcpCapabilitiesMap.Store(common.HostLocalStorageSupport, true)
	if enabled {
		WcpCapabilitiesMap.Store(common.PodVMV2Support, true)
	}
}

func TestUpdateCSINodeIsAddOnlyWithoutPodVMV2(t *testing.T) {
	setPodVMV2Capability(t, false)
	client := fake.NewSimpleClientset()
	c := &K8sOrchestrator{k8sClient: client, clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "esx-1", UID: "uid", Annotations: map[string]string{},
		Labels: map[string]string{corev1.LabelTopologyZone: "zone-1"},
	}}
	triggered := node.DeepCopy()
	triggered.Annotations[common.CreateCSINodeAnnotation] = "true"
	rezoned := triggered.DeepCopy()
	rezoned.Labels[corev1.LabelTopologyZone] = "zone-2"
	typed := rezoned.DeepCopy()
	typed.Annotations[podvmv2.NodeTypeAnnotation] = podvmv2.VirtualNodeType

	c.updateCSINode(node, triggered)
	c.updateCSINode(triggered, rezoned)
	c.updateCSINode(rezoned, typed)
	require.Empty(t, client.Actions(), "Node updates must not register CSINodes while PodVM v2 is disabled")
}

// With PodVM v2 disabled, the Workload Domain Isolation reconcile only adds a
// missing zone key and leaves any registration that already has one unchanged.
func TestCSINodeIsolationReconcileUnchangedWithoutPodVMV2(t *testing.T) {
	zone, host := corev1.LabelTopologyZone, corev1.LabelHostname
	for _, tc := range []struct {
		name          string
		isolation     bool
		existing      []string
		want          []string
		wantRecreated bool
	}{
		{name: "isolation adds missing zone", isolation: true, existing: []string{host},
			want: []string{zone, host}, wantRecreated: true},
		{name: "isolation keeps zone-only registration", isolation: true, existing: []string{zone},
			want: []string{zone}},
		{name: "isolation keeps reordered keys", isolation: true, existing: []string{host, zone},
			want: []string{host, zone}},
		{name: "no isolation never reconciles", existing: []string{zone},
			want: []string{zone}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPodVMV2Capability(t, false)
			if tc.isolation {
				WcpCapabilitiesMap.Store(common.WorkloadDomainIsolation, true)
			}
			originalClusters := clusterComputeResourceMoIds
			t.Cleanup(func() { clusterComputeResourceMoIds = originalClusters })
			clusterComputeResourceMoIds = []string{"domain-c1"}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: "esx-1", UID: "uid",
				// A VirtualNode annotation has no effect while PodVM v2 is disabled.
				Annotations: map[string]string{common.CreateCSINodeAnnotation: "true",
					podvmv2.NodeTypeAnnotation: podvmv2.VirtualNodeType},
				Labels: map[string]string{zone: "zone-1", host: "esx-1"},
			}}
			client := fake.NewSimpleClientset(&storagev1.CSINode{
				ObjectMeta: metav1.ObjectMeta{Name: node.Name},
				Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
					Name: common.VSphereCSIDriverName, NodeID: node.Name, TopologyKeys: tc.existing,
				}}},
			})
			client.ClearActions()
			c := &K8sOrchestrator{k8sClient: client, clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
			c.createCSINode(node)

			recreated := false
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					recreated = true
				}
			}
			require.Equal(t, tc.wantRecreated, recreated)
			got, err := client.StorageV1().CSINodes().Get(context.Background(), node.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Spec.Drivers[0].TopologyKeys)
		})
	}
}
