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
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

const (
	testCSINodeName     = "test-node"
	otherCSIDriverName  = "other.csi.example.com"
	otherCSIDriverTopoK = "example.com/zone"
)

// newWCPNodeTestOrchestrator returns a Supervisor flavor orchestrator with the WorkloadDomainIsolation
// capability enabled, plus a fake clientset pre-populated with the given CSINode objects.
func newWCPNodeTestOrchestrator(t *testing.T, objs ...*storagev1.CSINode) (*K8sOrchestrator,
	*k8sfake.Clientset) {
	t.Helper()
	origCapabilities := WcpCapabilitiesMap
	WcpCapabilitiesMap = &sync.Map{}
	WcpCapabilitiesMap.Store(common.WorkloadDomainIsolation, true)
	t.Cleanup(func() { WcpCapabilitiesMap = origCapabilities })

	client := k8sfake.NewSimpleClientset()
	for _, o := range objs {
		if _, err := client.StorageV1().CSINodes().Create(context.Background(), o,
			metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to seed CSINode %q: %v", o.Name, err)
		}
	}
	client.ClearActions()
	return &K8sOrchestrator{
		k8sClient:     client,
		clusterFlavor: cnstypes.CnsClusterFlavorWorkload,
	}, client
}

// newAnnotatedNode returns a node carrying the annotation that asks CSI to reconcile its CSINode.
func newAnnotatedNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testCSINodeName,
			UID:         "test-node-uid",
			Annotations: map[string]string{common.CreateCSINodeAnnotation: "true"},
		},
	}
}

func newExistingCSINode(drivers ...storagev1.CSINodeDriver) *storagev1.CSINode {
	return &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: testCSINodeName},
		Spec:       storagev1.CSINodeSpec{Drivers: drivers},
	}
}

func otherDriver() storagev1.CSINodeDriver {
	return storagev1.CSINodeDriver{
		Name:         otherCSIDriverName,
		NodeID:       testCSINodeName,
		TopologyKeys: []string{otherCSIDriverTopoK},
	}
}

func getCSINode(t *testing.T, client *k8sfake.Clientset) *storagev1.CSINode {
	t.Helper()
	csinode, err := client.StorageV1().CSINodes().Get(context.Background(), testCSINodeName,
		metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get CSINode: %v", err)
	}
	return csinode
}

func findDriver(csinode *storagev1.CSINode, name string) *storagev1.CSINodeDriver {
	for i := range csinode.Spec.Drivers {
		if csinode.Spec.Drivers[i].Name == name {
			return &csinode.Spec.Drivers[i]
		}
	}
	return nil
}

// assertNoMutation fails if the orchestrator issued anything other than reads.
func assertNoMutation(t *testing.T, client *k8sfake.Clientset) {
	t.Helper()
	for _, a := range client.Actions() {
		switch a.GetVerb() {
		case "get", "list", "watch":
		default:
			t.Fatalf("expected no mutating API call, got %s on %s", a.GetVerb(), a.GetResource().Resource)
		}
	}
}

// TestCreateCSINode_ExistingWithEmptyDrivers covers the reported panic: kubelet creates the CSINode with an
// empty drivers list before any node plugin registers, and the node is then annotated. The driver entry must
// not be fabricated, because no CSI node daemonset runs on such a node.
func TestCreateCSINode_ExistingWithEmptyDrivers(t *testing.T) {
	c, client := newWCPNodeTestOrchestrator(t, newExistingCSINode())

	c.createCSINode(newAnnotatedNode())

	assertNoMutation(t, client)
	if drivers := getCSINode(t, client).Spec.Drivers; len(drivers) != 0 {
		t.Fatalf("expected the CSINode to be left with no drivers, got %+v", drivers)
	}
}

// TestCreateCSINode_ExistingWithOnlyOtherDriver checks that an unrelated driver's registration is neither
// used as if it were ours nor disturbed.
func TestCreateCSINode_ExistingWithOnlyOtherDriver(t *testing.T) {
	c, client := newWCPNodeTestOrchestrator(t, newExistingCSINode(otherDriver()))

	c.createCSINode(newAnnotatedNode())

	assertNoMutation(t, client)
	csinode := getCSINode(t, client)
	if len(csinode.Spec.Drivers) != 1 || csinode.Spec.Drivers[0].Name != otherCSIDriverName {
		t.Fatalf("expected only driver %q to remain, got %+v", otherCSIDriverName, csinode.Spec.Drivers)
	}
}

// TestCreateCSINode_ExistingVSphereDriverWithoutZoneKey lists the vSphere entry second so that a lookup by
// index rather than by name would inspect the wrong driver.
func TestCreateCSINode_ExistingVSphereDriverWithoutZoneKey(t *testing.T) {
	vsphereNoZone := storagev1.CSINodeDriver{
		Name:         common.VSphereCSIDriverName,
		NodeID:       testCSINodeName,
		TopologyKeys: []string{corev1.LabelHostname},
	}
	c, client := newWCPNodeTestOrchestrator(t, newExistingCSINode(otherDriver(), vsphereNoZone))

	c.createCSINode(newAnnotatedNode())

	driver := findDriver(getCSINode(t, client), common.VSphereCSIDriverName)
	if driver == nil {
		t.Fatalf("expected driver %q on the re-created CSINode", common.VSphereCSIDriverName)
	}
	want := []string{corev1.LabelTopologyZone, corev1.LabelHostname}
	if len(driver.TopologyKeys) != len(want) || driver.TopologyKeys[0] != want[0] ||
		driver.TopologyKeys[1] != want[1] {
		t.Fatalf("expected topology keys %v, got %v", want, driver.TopologyKeys)
	}
}

// TestCreateCSINode_ExistingVSphereDriverWithZoneKeyUnchanged is the steady state: nothing to do.
func TestCreateCSINode_ExistingVSphereDriverWithZoneKeyUnchanged(t *testing.T) {
	vsphereWithZone := storagev1.CSINodeDriver{
		Name:         common.VSphereCSIDriverName,
		NodeID:       testCSINodeName,
		TopologyKeys: []string{corev1.LabelTopologyZone, corev1.LabelHostname},
	}
	c, client := newWCPNodeTestOrchestrator(t, newExistingCSINode(vsphereWithZone))

	c.createCSINode(newAnnotatedNode())

	assertNoMutation(t, client)
}

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
				client := k8sfake.NewSimpleClientset(objects...)
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
	client := k8sfake.NewSimpleClientset()
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

// A Node recreated under the same name gets a new UID. Its CSINode still carries the previous
// owner, so garbage collection will remove it; matching topology keys must not stop CSI from
// re-creating it under the current Node. A CSINode with no Node owner is left alone: nothing
// will collect it, and re-creating it would drop the driver registration for no reason.
func TestCreateCSINodeStaleOwner(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	for _, tc := range []struct {
		name          string
		owners        []metav1.OwnerReference
		wantRecreated bool
	}{
		{name: "stale node owner", wantRecreated: true, owners: []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "Node", Name: "compute-node", UID: "previous-uid"}}},
		{name: "current node owner", owners: []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "Node", Name: "compute-node", UID: "node-uid"}}},
		{name: "no owner"},
		{name: "unrelated owner", owners: []metav1.OwnerReference{
			{APIVersion: "v1", Kind: "Pod", Name: "other", UID: "other-uid"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			WcpCapabilitiesMap = &sync.Map{}
			WcpCapabilitiesMap.Store(common.PodVMV2Support, true)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: "compute-node", UID: "node-uid",
				Annotations: map[string]string{
					podvmv2.NodeTypeAnnotation:     podvmv2.VirtualNodeType,
					common.CreateCSINodeAnnotation: "true",
				},
				Labels: map[string]string{corev1.LabelTopologyZone: "zone-1"},
			}}
			client := k8sfake.NewSimpleClientset(&storagev1.CSINode{
				ObjectMeta: metav1.ObjectMeta{Name: node.Name, OwnerReferences: tc.owners},
				Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
					Name: common.VSphereCSIDriverName, NodeID: node.Name,
					TopologyKeys: []string{corev1.LabelTopologyZone},
				}}},
			})
			c := &K8sOrchestrator{k8sClient: client, clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
			c.createCSINode(node)
			recreated := false
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					recreated = true
				}
			}
			if recreated != tc.wantRecreated {
				t.Fatalf("CSINode re-created = %t, want %t", recreated, tc.wantRecreated)
			}
			result, err := client.StorageV1().CSINodes().Get(context.Background(), node.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Spec.Drivers[0].TopologyKeys, []string{corev1.LabelTopologyZone}) {
				t.Fatalf("topology keys = %v", result.Spec.Drivers[0].TopologyKeys)
			}
			if tc.wantRecreated && (len(result.OwnerReferences) != 1 || result.OwnerReferences[0].UID != node.UID) {
				t.Fatalf("owner references = %+v, want the current Node UID", result.OwnerReferences)
			}
		})
	}
}

// The "no vSphere driver entry" rule holds for PodVM v2 too. A Node whose CSI node plugin never
// registered cannot serve a volume whatever its type, so CSI must not re-create its CSINode to
// advertise the driver there, even though the topology keys do not match the Node type.
func TestCreateCSINodeWithoutVSphereDriverEntryPodVMV2(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	for _, virtual := range []bool{false, true} {
		t.Run(fmt.Sprintf("virtualNode=%t", virtual), func(t *testing.T) {
			WcpCapabilitiesMap = &sync.Map{}
			WcpCapabilitiesMap.Store(common.PodVMV2Support, true)
			node := newAnnotatedNode()
			node.Labels = map[string]string{corev1.LabelTopologyZone: "zone-1"}
			if virtual {
				node.Annotations[podvmv2.NodeTypeAnnotation] = podvmv2.VirtualNodeType
			}
			c, client := newWCPNodeTestOrchestrator(t, newExistingCSINode(otherDriver()))
			c.createCSINode(node)
			assertNoMutation(t, client)
			require.Nil(t, findDriver(getCSINode(t, client), common.VSphereCSIDriverName))
		})
	}
}
