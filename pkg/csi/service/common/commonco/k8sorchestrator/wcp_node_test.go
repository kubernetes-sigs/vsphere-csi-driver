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
	"fmt"
	"reflect"
	"sync"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

func TestCreateCSINodePodVMV2(t *testing.T) {
	originalCapabilities, originalClusters := WcpCapabilitiesMap, clusterComputeResourceMoIds
	t.Cleanup(func() { WcpCapabilitiesMap, clusterComputeResourceMoIds = originalCapabilities, originalClusters })
	zone, host := corev1.LabelTopologyZone, corev1.LabelHostname
	for _, tc := range []struct {
		name                                                string
		nodeName, nodeType                                  string
		missingZone                                         bool
		podVMV2, virtualNode, isolation, stretched, trigger bool
		existing                                            []string
		want                                                []string
	}{
		{name: "virtual node without hostname", podVMV2: true, virtualNode: true, trigger: true,
			want: []string{zone}},
		{name: "pre host upgrade", podVMV2: true, trigger: true,
			existing: []string{host}, want: []string{zone, host}},
		{name: "remove existing hostname from virtual node", podVMV2: true, virtualNode: true, isolation: true,
			trigger: true, existing: []string{zone, host}, want: []string{zone}},
		{name: "virtual node already registered", podVMV2: true, virtualNode: true, trigger: true,
			existing: []string{zone}, want: []string{zone}},
		{name: "new physical node", podVMV2: true, trigger: true,
			want: []string{zone, host}},
		{name: "physical node retains both keys", podVMV2: true, isolation: true, trigger: true,
			existing: []string{zone, host}, want: []string{zone, host}},
		{name: "restore physical node hostname", podVMV2: true, trigger: true,
			existing: []string{zone}, want: []string{zone, host}},
		{name: "virtual naming with feature disabled", virtualNode: true, isolation: true, trigger: true,
			want: []string{zone, host}},
		{name: "legacy single cluster", trigger: true, want: []string{host}},
		{name: "legacy stretched", stretched: true, trigger: true, want: []string{zone, host}},
		{name: "legacy isolation", isolation: true, trigger: true, want: []string{zone, host}},
		{name: "registration trigger absent", podVMV2: true},
		{name: "legacy prefix is not identity", nodeName: "virtual-node-domain-c1", podVMV2: true, trigger: true,
			want: []string{zone, host}},
		{name: "unknown future node type", nodeType: "entity-group", podVMV2: true, trigger: true},
		{name: "virtual zone pending", virtualNode: true, podVMV2: true, trigger: true, missingZone: true},
	} {
		for _, hostLocal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hostLocal=%t", tc.name, hostLocal), func(t *testing.T) {
				WcpCapabilitiesMap = &sync.Map{}
				WcpCapabilitiesMap.Store(common.HostLocalStorageSupport, hostLocal)
				// Omitted capabilities model older Supervisors without invoking a late-enable refresh.
				if tc.podVMV2 {
					WcpCapabilitiesMap.Store(common.PodVMV2Support, true)
				}
				if tc.isolation {
					WcpCapabilitiesMap.Store(common.WorkloadDomainIsolation, true)
				}
				clusterComputeResourceMoIds = []string{"domain-c1"}
				if tc.stretched {
					clusterComputeResourceMoIds = append(clusterComputeResourceMoIds, "domain-c2")
				}
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name: "compute-node", UID: "node-uid", Annotations: map[string]string{},
					Labels: map[string]string{corev1.LabelTopologyZone: "zone-1"},
				}}
				if tc.virtualNode {
					node.Annotations[podvmv2.NodeTypeAnnotation] = podvmv2.VirtualNodeType
				} else {
					node.Labels[corev1.LabelHostname] = node.Name
				}
				if tc.trigger {
					node.Annotations[common.CreateCSINodeAnnotation] = "true"
				}
				if tc.nodeName != "" {
					node.Name = tc.nodeName
				}
				if tc.nodeType != "" {
					node.Annotations[podvmv2.NodeTypeAnnotation] = tc.nodeType
				}
				if tc.missingZone {
					delete(node.Labels, corev1.LabelTopologyZone)
				}
				var objects []runtime.Object
				if tc.existing != nil {
					objects = append(objects, &storagev1.CSINode{
						ObjectMeta: metav1.ObjectMeta{Name: node.Name},
						Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
							Name: common.VSphereCSIDriverName, NodeID: node.Name, TopologyKeys: tc.existing,
						}}},
					})
				}
				client := fake.NewSimpleClientset(objects...)
				c := &K8sOrchestrator{k8sClient: client, clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
				c.createCSINode(node)
				result, err := client.StorageV1().CSINodes().Get(context.Background(), node.Name, metav1.GetOptions{})
				if tc.want == nil {
					if err == nil {
						t.Fatal("created CSINode without required supported metadata")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				driver := result.Spec.Drivers[0]
				if driver.NodeID != node.Name || !reflect.DeepEqual(driver.TopologyKeys, tc.want) {
					t.Fatalf("driver = %+v, want NodeID %q and topology %v", driver, node.Name, tc.want)
				}
				if tc.existing == nil || !reflect.DeepEqual(tc.existing, tc.want) {
					if len(result.OwnerReferences) != 1 || result.OwnerReferences[0].UID != node.UID {
						t.Fatal("CSINode must be owned by its Kubernetes Node")
					}
				}
				// Repeated registration must not delete or create an already-correct CSINode.
				client.ClearActions()
				c.createCSINode(node)
				for _, action := range client.Actions() {
					if action.GetVerb() != "get" {
						t.Fatalf("non-idempotent registration: %s", action.GetVerb())
					}
				}
			})
		}
	}
}

func TestCSINodeRegistrationMetadataArrivesLater(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	WcpCapabilitiesMap = &sync.Map{}
	WcpCapabilitiesMap.Store(common.PodVMV2Support, true)
	client := fake.NewSimpleClientset()
	c := &K8sOrchestrator{k8sClient: client, clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "cluster-node", UID: "uid",
		Annotations: map[string]string{podvmv2.NodeTypeAnnotation: podvmv2.VirtualNodeType}}}
	c.createCSINode(node)
	triggered := node.DeepCopy()
	triggered.Annotations[common.CreateCSINodeAnnotation] = "true"
	c.updateCSINode(node, triggered)
	if len(client.Actions()) != 0 {
		t.Fatal("registration must wait for zone")
	}
	zoned := triggered.DeepCopy()
	zoned.Labels = map[string]string{corev1.LabelTopologyZone: "zone-1"}
	c.updateCSINode(triggered, zoned)
	registered, err := client.StorageV1().CSINodes().Get(context.Background(), node.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(registered.Spec.Drivers[0].TopologyKeys, []string{corev1.LabelTopologyZone}) {
		t.Fatal("expected zone-only registration")
	}
	client.ClearActions()
	c.updateCSINode(zoned, zoned.DeepCopy())
	if len(client.Actions()) != 0 {
		t.Fatal("unrelated updates must not re-register")
	}
}
