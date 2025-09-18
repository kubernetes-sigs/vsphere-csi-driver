package k8sorchestrator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	csinodetopologyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/csinodetopology/v1alpha1"
)

func TestPatchNodeTopology(t *testing.T) {
	csiNodeTopology := &csinodetopologyv1alpha1.CSINodeTopology{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "simple-topo",
			ResourceVersion: "100",
		},
		Spec: csinodetopologyv1alpha1.CSINodeTopologySpec{
			NodeID:   "foobar",
			NodeUUID: "foobar123",
		},
		Status: csinodetopologyv1alpha1.CSINodeTopologyStatus{
			Status: csinodetopologyv1alpha1.CSINodeTopologySuccess,
		},
	}

	newTopology := csiNodeTopology.DeepCopy()
	newTopology.Status.Status = ""

	patch, err := getCSINodePatchData(csiNodeTopology, newTopology, true)
	if err != nil {
		t.Errorf("error creating patch object: %v", err)
	}
	var patchMap map[string]interface{}
	err = json.Unmarshal(patch, &patchMap)
	if err != nil {
		t.Errorf("failed to unmarshal patch: %v", err)
	}
	metadata, exist := patchMap["metadata"].(map[string]interface{})
	if !exist {
		t.Errorf("ResourceVersion should exist in patch data")
	}
	resourceVersion := metadata["resourceVersion"].(string)
	if resourceVersion != csiNodeTopology.ResourceVersion {
		t.Errorf("ResourceVersion should be %s, got %s", csiNodeTopology.ResourceVersion, resourceVersion)
	}
}

// ---------------------- Fake Informer ----------------------
type fakeInformer struct {
	cache.SharedIndexInformer
	store cache.Store
}

func (f *fakeInformer) GetStore() cache.Store {
	return f.store
}

// ---------------------- Test Helpers ----------------------
func makeZone(name, ns string, deleted bool, clusterMoIDs []string, causeNestedError bool) *unstructured.Unstructured {
	zone := &unstructured.Unstructured{}
	zone.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cns.vmware.com",
		Version: "v1alpha1",
		Kind:    "Zone",
	})
	zone.SetName(name)
	zone.SetNamespace(ns)

	if deleted {
		now := metav1.NewTime(time.Now())
		zone.SetDeletionTimestamp(&now)
	}

	if causeNestedError {
		zone.Object["spec"] = "not-a-map"
	} else if clusterMoIDs != nil {
		_ = unstructured.SetNestedStringSlice(zone.Object, clusterMoIDs, "spec", "namespace", "clusterMoIDs")
	}

	return zone
}

func setupFakeStore(zones ...*unstructured.Unstructured) cache.Store {
	store := cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	for _, z := range zones {
		_ = store.Add(z)
	}
	return store
}

// ---------------------- Tests ----------------------
var _ = Describe("GetActiveClustersForNamespaceInRequestedZones", func() {
	var (
		ctx  context.Context
		orch *K8sOrchestrator
	)

	BeforeEach(func() {
		ctx = context.Background()
		orch = &K8sOrchestrator{}
	})

	It("returns clusters when valid zones exist", func() {
		z1 := makeZone("zone-A", "ns1", false, []string{"cluster-1"}, false)
		zoneInformer = &fakeInformer{store: setupFakeStore(z1)}

		clusters, err := orch.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"zone-A"})
		Expect(err).NotTo(HaveOccurred())
		Expect(clusters).To(ConsistOf("cluster-1"))
	})

	It("skips zones with deletion timestamp", func() {
		z := makeZone("zone-A", "ns1", true, []string{"cluster-1"}, false)
		zoneInformer = &fakeInformer{store: setupFakeStore(z)}

		clusters, err := orch.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"zone-A"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("could not find active cluster"))
		Expect(clusters).To(BeNil())
	})

	It("skips zones not in requestedZones", func() {
		z := makeZone("zone-A", "ns1", false, []string{"cluster-1"}, false)
		zoneInformer = &fakeInformer{store: setupFakeStore(z)}

		clusters, err := orch.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"zone-B"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("could not find active cluster"))
		Expect(clusters).To(BeNil())
	})

	It("returns error when clusterMoIDs not found", func() {
		z := makeZone("zone-A", "ns1", false, nil, false)
		zoneInformer = &fakeInformer{store: setupFakeStore(z)}

		clusters, err := orch.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"zone-A"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("clusterMoIDs not found"))
		Expect(clusters).To(BeNil())
	})

	It("returns error when NestedStringSlice returns error", func() {
		z := makeZone("zone-A", "ns1", false, nil, true)
		zoneInformer = &fakeInformer{store: setupFakeStore(z)}

		clusters, err := orch.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"zone-A"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to get clusterMoIDs"))
		Expect(clusters).To(BeNil())
	})

	It("aggregates clusters from multiple zones", func() {
		z1 := makeZone("zone-A", "ns1", false, []string{"cluster-1"}, false)
		z2 := makeZone("zone-B", "ns1", false, []string{"cluster-2", "cluster-3"}, false)
		zoneInformer = &fakeInformer{store: setupFakeStore(z1, z2)}

		clusters, err := orch.GetActiveClustersForNamespaceInRequestedZones(ctx, "ns1", []string{"zone-A", "zone-B"})
		Expect(err).NotTo(HaveOccurred())
		Expect(clusters).To(ConsistOf("cluster-1", "cluster-2", "cluster-3"))
	})
})

// ---------------------- Ginkgo Entry ----------------------
func TestGetActiveClusters_Ginkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "K8sOrchestrator GetActiveClusters Suite")
}
