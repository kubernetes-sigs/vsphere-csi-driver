package k8sorchestrator

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
