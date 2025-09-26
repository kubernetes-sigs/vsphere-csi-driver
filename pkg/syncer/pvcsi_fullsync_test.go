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
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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

func TestAddNodeAffinityRulesOnPVTopologyAnnotationPresent(t *testing.T) {
	ctx := context.Background()

	// Create supervisor PVC with topology annotation
	supPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "volume-1",
			Namespace: "sv-namespace",
			Annotations: map[string]string{
				common.AnnVolumeAccessibleTopology: `[{"zone":"zone-a"}]`,
			},
		},
	}
	// Create guest PV without node affinity
	guestPV := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "volume-1",
				},
			},
		},
	}

	// Setup supervisor client with PVC
	supervisorClient := k8sfake.NewSimpleClientset(supPVC)
	// Setup guest client with PV
	guestClient := k8sfake.NewSimpleClientset(guestPV)

	// Setup metadataSyncer
	metadataSyncer := &metadataSyncInformer{
		supervisorClient: supervisorClient,
	}
	metadataSyncer.configInfo = &cnsconfig.ConfigurationInfo{
		Cfg: &cnsconfig.Config{
			GC: cnsconfig.GCConfig{
				Endpoint: "endpoint",
				Port:     "443",
			},
		},
	}

	// Patch k8sNewClient to return our guestClient
	origK8sClient := k8sNewClient
	defer func() {
		k8sNewClient = origK8sClient
	}()
	k8sNewClient = func(ctx context.Context) (clientset.Interface, error) {
		return guestClient, nil
	}

	// Patch getPVsInBoundAvailableOrReleased to return our PV
	origGetPVs := getPVsInBoundAvailableOrReleased
	defer func() {
		getPVsInBoundAvailableOrReleased = origGetPVs
	}()
	getPVsInBoundAvailableOrReleased = func(ctx context.Context,
		syncer *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
		return []*v1.PersistentVolume{guestPV}, nil
	}

	// Patch cnsconfig.GetSupervisorNamespace to return our namespace
	origGetSuperNS := cnsconfigGetSupervisorNamespace
	defer func() {
		cnsconfigGetSupervisorNamespace = origGetSuperNS
	}()
	cnsconfigGetSupervisorNamespace = func(ctx context.Context) (string, error) {
		return "sv-namespace", nil
	}

	// Run function
	AddNodeAffinityRulesOnPV(ctx, metadataSyncer)

	// Verify node affinity added
	gotPV, err := guestClient.CoreV1().PersistentVolumes().Get(ctx, "pv-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PV not found: %v", err)
	}
	if gotPV.Spec.NodeAffinity == nil || len(gotPV.Spec.NodeAffinity.Required.NodeSelectorTerms) == 0 {
		t.Errorf("Expected node affinity to be set on PV when supervisor PVC has topology annotation")
	}
}

func TestAddNodeAffinityRulesOnPVTopologyAnnotationAbsent(t *testing.T) {
	ctx := context.Background()

	// Create supervisor PVC without annotation
	supPVC := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "volume-2",
			Namespace:   "sv-namespace",
			Annotations: map[string]string{},
		},
	}
	// Create guest PV without node affinity
	guestPV := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv-2",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "volume-2",
				},
			},
		},
	}

	supervisorClient := k8sfake.NewSimpleClientset(supPVC)
	guestClient := k8sfake.NewSimpleClientset(guestPV)

	// Setup metadataSyncer
	metadataSyncer := &metadataSyncInformer{
		supervisorClient: supervisorClient,
	}
	metadataSyncer.configInfo = &cnsconfig.ConfigurationInfo{
		Cfg: &cnsconfig.Config{
			GC: cnsconfig.GCConfig{
				Endpoint: "endpoint",
				Port:     "443",
			},
		},
	}

	// Patch k8sNewClient to return our guestClient
	origK8sClient := k8sNewClient
	defer func() {
		k8sNewClient = origK8sClient
	}()
	k8sNewClient = func(ctx context.Context) (clientset.Interface, error) {
		return guestClient, nil
	}

	// Patch getPVsInBoundAvailableOrReleased to return our PV
	origGetPVs := getPVsInBoundAvailableOrReleased
	defer func() {
		getPVsInBoundAvailableOrReleased = origGetPVs
	}()
	getPVsInBoundAvailableOrReleased = func(ctx context.Context,
		syncer *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
		return []*v1.PersistentVolume{guestPV}, nil
	}

	// Patch cnsconfig.GetSupervisorNamespace to return our namespace
	origGetSuperNS := cnsconfigGetSupervisorNamespace
	defer func() {
		cnsconfigGetSupervisorNamespace = origGetSuperNS
	}()
	cnsconfigGetSupervisorNamespace = func(ctx context.Context) (string, error) {
		return "sv-namespace", nil
	}

	// Reduce timeout value used in code for testing
	origTimeout := timeoutAddNodeAffinityOnPVs
	defer func() {
		timeoutAddNodeAffinityOnPVs = origTimeout
	}()
	timeoutAddNodeAffinityOnPVs = 15 * time.Second

	// Run function
	AddNodeAffinityRulesOnPV(ctx, metadataSyncer)

	// Verify node affinity NOT added as supervisor PVC doesn't have topology annotation
	gotPV, err := guestClient.CoreV1().PersistentVolumes().Get(ctx, "pv-2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PV not found: %v", err)
	}
	if gotPV.Spec.NodeAffinity != nil {
		t.Errorf("Expected node affinity NOT to be set on PV when supervisor PVC has no topology annotation")
	}
}
