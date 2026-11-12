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

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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
