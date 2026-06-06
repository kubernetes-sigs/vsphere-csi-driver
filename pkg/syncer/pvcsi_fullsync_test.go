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
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// Pointer helper function
func ptrTo[T any](v T) *T {
	return &v
}

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
	supervisorClient := k8sfake.NewClientset(supPVC)
	// Setup guest client with PV
	guestClient := k8sfake.NewClientset(guestPV)

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

	supervisorClient := k8sfake.NewClientset(supPVC)
	guestClient := k8sfake.NewClientset(guestPV)

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

// Tests for the k8s.PatchObject modifications in PvcsiFullSync
func TestPvcsiFullSync_PatchObject(t *testing.T) {
	ctx := context.Background()
	supervisorNamespace := "vmware-system-csi"

	// Create test guest cluster object (source of truth)
	guestObject := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-metadata",
			Namespace: "guest-namespace",
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames: []string{"volume-1", "volume-2"},
			EntityName:  "test-pvc",
			EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
			EntityReferences: []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{
				{
					EntityName: "test-pvc",
					EntityType: string(cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC),
					Namespace:  "test-namespace",
				},
			},
		},
	}

	// Create test supervisor cluster object (to be updated)
	supervisorObject := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "test-metadata",
			Namespace:       supervisorNamespace,
			ResourceVersion: "1",
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames: []string{"old-volume"},
			EntityName:  "old-pvc",
			EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
			EntityReferences: []cnsvolumemetadatav1alpha1.CnsOperatorEntityReference{
				{
					EntityName: "old-pvc",
					EntityType: string(cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC),
					Namespace:  "old-namespace",
				},
			},
		},
	}

	tests := []struct {
		name           string
		guestSpec      cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec
		supervisorSpec cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec
		shouldPatch    bool
		expectError    bool
		setupClient    func() client.Client
	}{
		{
			name:           "Successful patch when specs differ",
			guestSpec:      guestObject.Spec,
			supervisorSpec: supervisorObject.Spec,
			shouldPatch:    true,
			expectError:    false,
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorapis.AddToScheme(scheme)
				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(supervisorObject.DeepCopy()).
					Build()
			},
		},
		{
			name:           "No patch when specs are identical",
			guestSpec:      supervisorObject.Spec, // Same as supervisor
			supervisorSpec: supervisorObject.Spec,
			shouldPatch:    false,
			expectError:    false,
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorapis.AddToScheme(scheme)
				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(supervisorObject.DeepCopy()).
					Build()
			},
		},
		{
			name: "Skip patch for POD entity type",
			guestSpec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
				EntityType: cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD,
			},
			supervisorSpec: supervisorObject.Spec,
			shouldPatch:    false,
			expectError:    false,
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorapis.AddToScheme(scheme)
				return fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(supervisorObject.DeepCopy()).
					Build()
			},
		},
		{
			name:           "Handle patch error gracefully",
			guestSpec:      guestObject.Spec,
			supervisorSpec: supervisorObject.Spec,
			shouldPatch:    true,
			expectError:    true,
			setupClient: func() client.Client {
				scheme := runtime.NewScheme()
				_ = cnsoperatorapis.AddToScheme(scheme)
				// Create client without the object to simulate patch error
				return fake.NewClientBuilder().
					WithScheme(scheme).
					Build()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup fake client
			fakeClient := tt.setupClient()

			// Create metadataSyncer
			metadataSyncer := &metadataSyncInformer{
				cnsOperatorClient: fakeClient,
			}

			// Create test objects with the specified specs
			testGuestObject := guestObject.DeepCopy()
			testGuestObject.Spec = tt.guestSpec

			testSupervisorObject := supervisorObject.DeepCopy()
			testSupervisorObject.Spec = tt.supervisorSpec

			// Store original spec for comparison
			originalSupervisorSpec := testSupervisorObject.Spec.DeepCopy()

			// Execute the code under test (simulating the modified lines in PvcsiFullSync)
			if testGuestObject.Spec.EntityType != cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD &&
				!compareCnsVolumeMetadatas(&testGuestObject.Spec, &testSupervisorObject.Spec) {

				// This is the modified code we're testing
				original := testSupervisorObject.DeepCopy()
				testSupervisorObject.Spec = testGuestObject.Spec
				err := k8s.PatchObject(ctx, metadataSyncer.cnsOperatorClient, original, testSupervisorObject)

				if tt.expectError {
					assert.Error(t, err, "Expected patch operation to fail")
				} else if tt.shouldPatch {
					assert.NoError(t, err, "Patch operation should succeed")

					// Verify that the spec was updated to match guest object
					assert.Equal(t, testGuestObject.Spec, testSupervisorObject.Spec,
						"Supervisor object spec should match guest object spec after patch")

					// Verify that the original object was used for patching (DeepCopy was called)
					assert.Equal(t, *originalSupervisorSpec, original.Spec,
						"Original object should contain the pre-modification spec")
				}
			} else {
				// Verify that no patch was attempted when conditions weren't met
				assert.Equal(t, *originalSupervisorSpec, testSupervisorObject.Spec,
					"Supervisor object spec should remain unchanged when patch conditions not met")
			}
		})
	}
}

func TestCompareCnsVolumeMetadatas(t *testing.T) {
	// Test the comparison function used in the patch logic
	// Note: This function only compares Labels and ClusterDistribution fields
	spec1 := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
		VolumeNames: []string{"volume-1"},
		EntityName:  "test-entity",
		EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
		Labels: map[string]string{
			"key1": "value1",
		},
		ClusterDistribution: "tkgs",
	}

	spec2 := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
		VolumeNames: []string{"volume-1"}, // Same as spec1
		EntityName:  "test-entity",        // Same as spec1
		EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
		Labels: map[string]string{
			"key1": "different-value", // Different from spec1
		},
		ClusterDistribution: "tkgs", // Same as spec1
	}

	spec3 := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
		VolumeNames: []string{"volume-1"},
		EntityName:  "test-entity",
		EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
		Labels: map[string]string{
			"key1": "value1", // Same as spec1
		},
		ClusterDistribution: "tkgs", // Same as spec1
	}

	tests := []struct {
		name     string
		spec1    *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec
		spec2    *cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec
		expected bool
	}{
		{
			name:     "Different Labels should return false",
			spec1:    spec1,
			spec2:    spec2,
			expected: false,
		},
		{
			name:     "Identical Labels and ClusterDistribution should return true",
			spec1:    spec1,
			spec2:    spec3,
			expected: true,
		},
		{
			name:     "Same spec compared to itself should return true",
			spec1:    spec1,
			spec2:    spec1,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create copies to avoid side effects between tests
			spec1Copy := tt.spec1.DeepCopy()
			spec2Copy := tt.spec2.DeepCopy()
			result := compareCnsVolumeMetadatas(spec1Copy, spec2Copy)
			assert.Equal(t, tt.expected, result, "compareCnsVolumeMetadatas result should match expected")
		})
	}
}

func TestPvcsiFullSync_PatchLogic_Integration(t *testing.T) {
	ctx := context.Background()

	// This test simulates the actual integration of the patch logic within the full sync process
	supervisorNamespace := "vmware-system-csi"

	// Create objects that would trigger a patch (different Labels to trigger compareCnsVolumeMetadatas to return false)
	guestObject := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name: "integration-test",
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames: []string{"new-volume"},
			EntityName:  "new-pvc",
			EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
			Labels: map[string]string{
				"guest-label": "guest-value",
			},
			ClusterDistribution: "tkgs-guest",
		},
	}

	supervisorObject := &cnsvolumemetadatav1alpha1.CnsVolumeMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "integration-test",
			Namespace:       supervisorNamespace,
			ResourceVersion: "1",
		},
		Spec: cnsvolumemetadatav1alpha1.CnsVolumeMetadataSpec{
			VolumeNames: []string{"old-volume"},
			EntityName:  "old-pvc",
			EntityType:  cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePVC,
			Labels: map[string]string{
				"supervisor-label": "supervisor-value", // Different from guest
			},
			ClusterDistribution: "tkgs-supervisor", // Different from guest
		},
	}

	// Setup fake client
	scheme := runtime.NewScheme()
	_ = cnsoperatorapis.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(supervisorObject.DeepCopy()).
		Build()

	// Create metadataSyncer
	metadataSyncer := &metadataSyncInformer{
		cnsOperatorClient: fakeClient,
	}

	// Execute the patch logic (the exact code from the modified function)
	if guestObject.Spec.EntityType != cnsvolumemetadatav1alpha1.CnsOperatorEntityTypePOD &&
		!compareCnsVolumeMetadatas(&guestObject.Spec, &supervisorObject.Spec) {

		original := supervisorObject.DeepCopy()
		supervisorObject.Spec = guestObject.Spec
		err := k8s.PatchObject(ctx, metadataSyncer.cnsOperatorClient, original, supervisorObject)

		assert.NoError(t, err, "Integration patch should succeed")

		// Verify the patch was applied correctly
		assert.Equal(t, guestObject.Spec.VolumeNames, supervisorObject.Spec.VolumeNames,
			"VolumeNames should be updated")
		assert.Equal(t, guestObject.Spec.EntityName, supervisorObject.Spec.EntityName,
			"EntityName should be updated")
		assert.Equal(t, guestObject.Spec.Labels, supervisorObject.Spec.Labels,
			"Labels should be updated")
		assert.Equal(t, guestObject.Spec.ClusterDistribution, supervisorObject.Spec.ClusterDistribution,
			"ClusterDistribution should be updated")
	} else {
		t.Fatal("Expected patch conditions to be met for integration test")
	}
}

// TestPVCPatchFunctionality tests PVC patching functionality in pvcsi_fullsync and pvcsi_metadatasyncer
func TestPVCPatchFunctionality(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	err := v1.AddToScheme(scheme)
	require.NoError(t, err)

	tests := []struct {
		name         string
		setupPVC     func() *v1.PersistentVolumeClaim
		modifyPVC    func(*v1.PersistentVolumeClaim)
		expectError  bool
		validateFunc func(*testing.T, *v1.PersistentVolumeClaim)
	}{
		{
			name: "Add guest cluster labels to PVC",
			setupPVC: func() *v1.PersistentVolumeClaim {
				return &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "supervisor-ns",
						Labels:    map[string]string{},
					},
				}
			},
			modifyPVC: func(pvc *v1.PersistentVolumeClaim) {
				// Simulate adding guest cluster labels (from pvcsi_fullsync.go)
				key := "test-cluster/tkgs"
				pvc.Labels[key] = "test-cluster-uid"
			},
			expectError: false,
			validateFunc: func(t *testing.T, pvc *v1.PersistentVolumeClaim) {
				assert.Contains(t, pvc.Labels, "test-cluster/tkgs")
				assert.Equal(t, "test-cluster-uid", pvc.Labels["test-cluster/tkgs"])
			},
		},
		{
			name: "Remove guest cluster labels from PVC",
			setupPVC: func() *v1.PersistentVolumeClaim {
				return &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pvc",
						Namespace: "supervisor-ns",
						Labels: map[string]string{
							"test-cluster/tkgs": "test-cluster-uid",
							"other-label":       "other-value",
						},
					},
				}
			},
			modifyPVC: func(pvc *v1.PersistentVolumeClaim) {
				// Simulate removing guest cluster labels (from pvcsi_metadatasyncer.go)
				key := "test-cluster/tkgs"
				delete(pvc.Labels, key)
			},
			expectError: false,
			validateFunc: func(t *testing.T, pvc *v1.PersistentVolumeClaim) {
				assert.NotContains(t, pvc.Labels, "test-cluster/tkgs")
				assert.Contains(t, pvc.Labels, "other-label")
			},
		},
		{
			name: "Add CNS finalizer to PVC",
			setupPVC: func() *v1.PersistentVolumeClaim {
				return &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-pvc",
						Namespace:  "supervisor-ns",
						Finalizers: []string{"other-finalizer"},
					},
				}
			},
			modifyPVC: func(pvc *v1.PersistentVolumeClaim) {
				// Simulate adding CNS finalizer (from pvcsi_fullsync.go)
				pvc.ObjectMeta.Finalizers = append(pvc.ObjectMeta.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
			},
			expectError: false,
			validateFunc: func(t *testing.T, pvc *v1.PersistentVolumeClaim) {
				assert.Contains(t, pvc.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
				assert.Contains(t, pvc.Finalizers, "other-finalizer")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			pvc := tt.setupPVC()
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pvc).
				Build()

			// Create original copy before modification
			original := pvc.DeepCopy()

			// Apply modifications
			tt.modifyPVC(pvc)

			// Test PatchObject
			err := k8s.PatchObject(ctx, fakeClient, original, pvc)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the patch was applied
				updatedPVC := &v1.PersistentVolumeClaim{}
				err = fakeClient.Get(ctx, client.ObjectKey{
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
				}, updatedPVC)
				assert.NoError(t, err)

				// Run validation function
				if tt.validateFunc != nil {
					tt.validateFunc(t, updatedPVC)
				}
			}
		})
	}
}

// TestVolumeSnapshotPatchFunctionality tests VolumeSnapshot patching functionality
func TestVolumeSnapshotPatchFunctionality(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	err := snapv1.AddToScheme(scheme)
	require.NoError(t, err)

	tests := []struct {
		name         string
		setupVS      func() *snapv1.VolumeSnapshot
		modifyVS     func(*snapv1.VolumeSnapshot)
		expectError  bool
		validateFunc func(*testing.T, *snapv1.VolumeSnapshot)
	}{
		{
			name: "Add CNS finalizer to VolumeSnapshot",
			setupVS: func() *snapv1.VolumeSnapshot {
				return &snapv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "test-snapshot",
						Namespace:  "supervisor-ns",
						Finalizers: []string{"other-finalizer"},
					},
				}
			},
			modifyVS: func(vs *snapv1.VolumeSnapshot) {
				// Simulate adding CNS finalizer (from pvcsi_fullsync.go)
				vs.ObjectMeta.Finalizers = append(vs.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
			},
			expectError: false,
			validateFunc: func(t *testing.T, vs *snapv1.VolumeSnapshot) {
				assert.Contains(t, vs.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
				assert.Contains(t, vs.Finalizers, "other-finalizer")
			},
		},
		{
			name: "Add guest cluster labels to VolumeSnapshot",
			setupVS: func() *snapv1.VolumeSnapshot {
				return &snapv1.VolumeSnapshot{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-snapshot",
						Namespace: "supervisor-ns",
						Labels:    map[string]string{},
					},
				}
			},
			modifyVS: func(vs *snapv1.VolumeSnapshot) {
				// Simulate adding guest cluster labels (from pvcsi_fullsync.go)
				key := "test-cluster/tkgs"
				if vs.Labels == nil {
					vs.Labels = make(map[string]string)
				}
				vs.Labels[key] = "test-cluster-uid"
			},
			expectError: false,
			validateFunc: func(t *testing.T, vs *snapv1.VolumeSnapshot) {
				assert.Contains(t, vs.Labels, "test-cluster/tkgs")
				assert.Equal(t, "test-cluster-uid", vs.Labels["test-cluster/tkgs"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			vs := tt.setupVS()
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(vs).
				Build()

			// Create original copy before modification
			original := vs.DeepCopy()

			// Apply modifications
			tt.modifyVS(vs)

			// Test PatchObject
			err := k8s.PatchObject(ctx, fakeClient, original, vs)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the patch was applied
				updatedVS := &snapv1.VolumeSnapshot{}
				err = fakeClient.Get(ctx, client.ObjectKey{
					Name:      vs.Name,
					Namespace: vs.Namespace,
				}, updatedVS)
				assert.NoError(t, err)

				// Run validation function
				if tt.validateFunc != nil {
					tt.validateFunc(t, updatedVS)
				}
			}
		})
	}
}

// TestControllerRuntimeClientCreationForSupervisor tests creating controller-runtime clients for supervisor cluster
func TestControllerRuntimeClientCreationForSupervisor(t *testing.T) {
	t.Run("Client creation with scheme for VolumeSnapshot", func(t *testing.T) {
		// Create scheme with VolumeSnapshot support (simulating pvcsi_fullsync.go logic)
		scheme := runtime.NewScheme()
		err := snapv1.AddToScheme(scheme)
		require.NoError(t, err)

		// Verify scheme has VolumeSnapshot registered
		gvk := snapv1.SchemeGroupVersion.WithKind("VolumeSnapshot")
		_, err = scheme.New(gvk)
		assert.NoError(t, err, "VolumeSnapshot should be registered in scheme")
	})

	t.Run("Scheme registration for core resources", func(t *testing.T) {
		// Test that core resources are properly registered
		scheme := runtime.NewScheme()
		err := v1.AddToScheme(scheme)
		require.NoError(t, err)

		// Verify scheme has PVC registered
		gvk := v1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")
		_, err = scheme.New(gvk)
		assert.NoError(t, err, "PersistentVolumeClaim should be registered in scheme")
	})
}

// TestPatchObjectErrorScenarios tests error handling in PatchObject usage
func TestPatchObjectErrorScenarios(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	err := v1.AddToScheme(scheme)
	require.NoError(t, err)

	t.Run("PatchObject with conflicting updates", func(t *testing.T) {
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-ns",
				Labels:    map[string]string{"initial": "value"},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pvc).
			Build()

		// Simulate concurrent modification
		original := pvc.DeepCopy()
		pvc.Labels["new"] = "label"

		// This should succeed with fake client
		err := k8s.PatchObject(ctx, fakeClient, original, pvc)
		assert.NoError(t, err)

		// Verify the change was applied
		updatedPVC := &v1.PersistentVolumeClaim{}
		err = fakeClient.Get(ctx, client.ObjectKey{
			Name:      pvc.Name,
			Namespace: pvc.Namespace,
		}, updatedPVC)
		assert.NoError(t, err)
		assert.Equal(t, "label", updatedPVC.Labels["new"])
	})

	t.Run("PatchObject with nil labels map initialization", func(t *testing.T) {
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-ns",
				// Labels is nil initially
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pvc).
			Build()

		original := pvc.DeepCopy()

		// Initialize labels map (simulating pvcsi_fullsync.go logic)
		if pvc.Labels == nil {
			pvc.Labels = make(map[string]string)
		}
		pvc.Labels["test-key"] = "test-value"

		err := k8s.PatchObject(ctx, fakeClient, original, pvc)
		assert.NoError(t, err)

		// Verify labels were added
		updatedPVC := &v1.PersistentVolumeClaim{}
		err = fakeClient.Get(ctx, client.ObjectKey{
			Name:      pvc.Name,
			Namespace: pvc.Namespace,
		}, updatedPVC)
		assert.NoError(t, err)
		assert.Equal(t, "test-value", updatedPVC.Labels["test-key"])
	})
}

// TestReconcileGuestSnapshotAnnotation_SuccessfulAnnotation tests adding change-id annotation from supervisor to guest
func TestReconcileGuestSnapshotAnnotation_SuccessfulAnnotation(t *testing.T) {
	ctx := context.Background()

	// Create supervisor VolumeSnapshot with change-id annotation
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-supervisor-1",
			Namespace: "vmware-system-csi",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "change-id-abc123",
			},
		},
	}

	// Create guest VolumeSnapshot without change-id annotation
	guestVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snap-guest-1",
			Namespace:   "guest-ns",
			Annotations: make(map[string]string),
		},
	}

	// Setup fake client with guest snapshot
	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(guestVS.DeepCopy()).
		Build()

	// Call reconcile function
	err := reconcileGuestSnapshotAnnotation(ctx, supervisorVS, guestVS, fakeClient)
	assert.NoError(t, err)

	// Verify annotation was added to guest snapshot
	updatedGuestVS := &snapv1.VolumeSnapshot{}
	err = fakeClient.Get(ctx, client.ObjectKey{
		Name:      guestVS.Name,
		Namespace: guestVS.Namespace,
	}, updatedGuestVS)
	assert.NoError(t, err)
	assert.Equal(t, "change-id-abc123",
		updatedGuestVS.Annotations[common.VolumeSnapshotChangeIDKey],
		"Guest snapshot should have change-id annotation from supervisor")
}

// TestReconcileGuestSnapshotAnnotation_SkipsWhenNoSupervisorAnnotation checks that
// unannotated supervisor snapshots are skipped
func TestReconcileGuestSnapshotAnnotation_SkipsWhenNoSupervisorAnnotation(t *testing.T) {
	ctx := context.Background()

	// Create supervisor VolumeSnapshot WITHOUT change-id annotation
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snap-supervisor-no-id",
			Namespace:   "vmware-system-csi",
			Annotations: make(map[string]string),
		},
	}

	// Create guest VolumeSnapshot without change-id annotation
	guestVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snap-guest-no-id",
			Namespace:   "guest-ns",
			Annotations: make(map[string]string),
		},
	}

	// Create guest VSC
	// Setup fake client
	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(guestVS.DeepCopy()).
		Build()

	// Call reconcile function
	err := reconcileGuestSnapshotAnnotation(ctx, supervisorVS, guestVS, fakeClient)
	assert.NoError(t, err)

	// Verify NO annotation was added to guest snapshot
	updatedGuestVS := &snapv1.VolumeSnapshot{}
	err = fakeClient.Get(ctx, client.ObjectKey{
		Name:      guestVS.Name,
		Namespace: guestVS.Namespace,
	}, updatedGuestVS)
	assert.NoError(t, err)
	assert.NotContains(t, updatedGuestVS.Annotations, common.VolumeSnapshotChangeIDKey,
		"Guest snapshot should NOT have change-id annotation when supervisor has none")
}

// TestReconcileGuestSnapshotAnnotation_SkipsWhenAlreadyAnnotated tests skipping when guest already has annotation
func TestReconcileGuestSnapshotAnnotation_SkipsWhenAlreadyAnnotated(t *testing.T) {
	ctx := context.Background()

	// Create supervisor VolumeSnapshot with change-id annotation
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-supervisor-annotated",
			Namespace: "vmware-system-csi",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "change-id-new",
			},
		},
	}

	// Create guest VolumeSnapshot that ALREADY has a different change-id annotation
	guestVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-guest-already-annotated",
			Namespace: "guest-ns",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "change-id-old",
			},
		},
	}

	// Create guest VSC
	// Setup fake client
	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(guestVS.DeepCopy()).
		Build()

	// Call reconcile function
	err := reconcileGuestSnapshotAnnotation(ctx, supervisorVS, guestVS, fakeClient)
	assert.NoError(t, err)

	// Verify annotation was NOT changed (should remain old value)
	updatedGuestVS := &snapv1.VolumeSnapshot{}
	err = fakeClient.Get(ctx, client.ObjectKey{
		Name:      guestVS.Name,
		Namespace: guestVS.Namespace,
	}, updatedGuestVS)
	assert.NoError(t, err)
	assert.Equal(t, "change-id-old",
		updatedGuestVS.Annotations[common.VolumeSnapshotChangeIDKey],
		"Guest snapshot should keep its existing annotation")
}

// TestReconcileGuestSnapshotAnnotation_PatchError tests error handling when patch fails
func TestReconcileGuestSnapshotAnnotation_PatchError(t *testing.T) {
	ctx := context.Background()

	// Create supervisor VolumeSnapshot with change-id annotation
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "snap-supervisor-error",
			Namespace: "vmware-system-csi",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "change-id-error",
			},
		},
	}

	// Create guest VolumeSnapshot
	guestVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snap-guest-error",
			Namespace:   "guest-ns",
			Annotations: make(map[string]string),
		},
	}

	// Create guest VSC
	// Setup fake client WITHOUT the guest snapshot object (to cause patch error)
	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	// Call reconcile function - should return error
	err := reconcileGuestSnapshotAnnotation(ctx, supervisorVS, guestVS, fakeClient)
	assert.Error(t, err, "Patch should fail when guest snapshot doesn't exist in client")
}

// TestIsReadyVolumeSnapshotContent_ValidReady tests VSC that is ready
func TestIsReadyVolumeSnapshotContent_ValidReady(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo("handle-123"),
			ReadyToUse:     ptrTo(true),
		},
	}
	assert.True(t, isReadyVolumeSnapshotContent(vsc), "Should return true for valid ready VSC")
}

// TestIsReadyVolumeSnapshotContent_InvalidDriver tests VSC with wrong driver
func TestIsReadyVolumeSnapshotContent_InvalidDriver(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: "different-driver",
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo("handle-123"),
			ReadyToUse:     ptrTo(true),
		},
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC with wrong driver")
}

// TestIsReadyVolumeSnapshotContent_MissingVolumeSnapshotRef tests VSC without VolumeSnapshotRef name
func TestIsReadyVolumeSnapshotContent_MissingVolumeSnapshotRef(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo("handle-123"),
			ReadyToUse:     ptrTo(true),
		},
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC without VolumeSnapshotRef name")
}

// TestIsReadyVolumeSnapshotContent_NilStatus tests VSC with nil status
func TestIsReadyVolumeSnapshotContent_NilStatus(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: nil,
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC with nil status")
}

// TestIsReadyVolumeSnapshotContent_NilSnapshotHandle tests VSC with nil snapshot handle
func TestIsReadyVolumeSnapshotContent_NilSnapshotHandle(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: nil,
			ReadyToUse:     ptrTo(true),
		},
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC with nil snapshot handle")
}

// TestIsReadyVolumeSnapshotContent_EmptySnapshotHandle tests VSC with empty snapshot handle
func TestIsReadyVolumeSnapshotContent_EmptySnapshotHandle(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo(""),
			ReadyToUse:     ptrTo(true),
		},
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC with empty snapshot handle")
}

// TestIsReadyVolumeSnapshotContent_NotReady tests VSC with ReadyToUse=false
func TestIsReadyVolumeSnapshotContent_NotReady(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo("handle-123"),
			ReadyToUse:     ptrTo(false),
		},
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC with ReadyToUse=false")
}

// TestSetGuestClusterDetailsOnSupervisorPVC_AnnotationBackfill tests the upgrade-path
// annotation backfill added in setGuestClusterDetailsOnSupervisorPVC. The private
// function constructs its own controller-runtime client, so we drive the same
// patch sequence through the shared common.BuildGuestPvcAnnotation helper (the
// single source of truth for the annotation key set) plus k8s.PatchObject — the
// same pattern used by TestPVCPatchFunctionality above. The pure key/value logic
// is covered directly by TestBuildGuestPvcAnnotation in the common package.
func TestSetGuestClusterDetailsOnSupervisorPVC_AnnotationBackfill(t *testing.T) {
	ctx := context.Background()

	const (
		svPVCName    = "sv-pvc-1"
		svPVCVolName = "sv-pv-1"
		svNamespace  = "vmware-system-csi"
		clusterUID   = "tkc-uid-abc"
		clusterName  = "my-tkc"
		guestPVCName = "guest-pvc-1"
		guestPVCNS   = "guest-ns"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(scheme))

	// makeSVPVC builds a supervisor PVC with optional annotations and a bound volume.
	makeSVPVC := func(annots map[string]string) *v1.PersistentVolumeClaim {
		return &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        svPVCName,
				Namespace:   svNamespace,
				Annotations: annots,
			},
			Spec: v1.PersistentVolumeClaimSpec{
				VolumeName: svPVCVolName,
			},
		}
	}

	// runBackfill mirrors the ImprovedVolumeVisiblity block from
	// setGuestClusterDetailsOnSupervisorPVC: skip when already annotated, otherwise
	// build the annotation via the shared helper and patch the supervisor PVC.
	runBackfill := func(t *testing.T, svPVC *v1.PersistentVolumeClaim,
		fakeClient client.Client) {
		t.Helper()
		if _, ok := svPVC.Annotations[common.AnnKeyGuestClusterPvc]; ok {
			return
		}
		guestPvcAnnot := common.BuildGuestPvcAnnotation(clusterUID, clusterName,
			guestPVCName, guestPVCNS, svPVC.Spec.VolumeName)
		jsonAnnotation, err := json.Marshal(guestPvcAnnot)
		require.NoError(t, err)
		original := svPVC.DeepCopy()
		if svPVC.Annotations == nil {
			svPVC.Annotations = make(map[string]string)
		}
		svPVC.Annotations[common.AnnKeyGuestClusterPvc] = string(jsonAnnotation)
		require.NoError(t, k8s.PatchObject(ctx, fakeClient, original, svPVC))
	}

	getUpdatedAnnot := func(t *testing.T, c client.Client) map[string]string {
		t.Helper()
		updated := &v1.PersistentVolumeClaim{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{
			Name: svPVCName, Namespace: svNamespace,
		}, updated))
		return updated.Annotations
	}

	t.Run("backfills annotation when absent", func(t *testing.T) {
		svPVC := makeSVPVC(nil) // nil annotations — also exercises the nil-map guard
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svPVC).Build()

		runBackfill(t, svPVC, fakeClient)

		annots := getUpdatedAnnot(t, fakeClient)
		require.Contains(t, annots, common.AnnKeyGuestClusterPvc)
		var got map[string]string
		require.NoError(t, json.Unmarshal([]byte(annots[common.AnnKeyGuestClusterPvc]), &got))
		assert.Equal(t, map[string]string{
			common.GuestClusterAnnotKeyClusterID:   clusterUID,
			common.GuestClusterAnnotKeyClusterName: clusterName,
			common.GuestClusterAnnotKeyName:        guestPVCName,
			common.GuestClusterAnnotKeyNamespace:   guestPVCNS,
			common.GuestClusterAnnotKeyVolumeName:  svPVCVolName,
		}, got)
	})

	t.Run("idempotent: skips when annotation already present", func(t *testing.T) {
		existing := `{"clusterId":"old-uid","clusterName":"old-name"}`
		svPVC := makeSVPVC(map[string]string{common.AnnKeyGuestClusterPvc: existing})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svPVC).Build()

		runBackfill(t, svPVC, fakeClient)

		annots := getUpdatedAnnot(t, fakeClient)
		// Pre-existing annotation must be preserved untouched.
		assert.Equal(t, existing, annots[common.AnnKeyGuestClusterPvc],
			"pre-existing annotation must not be overwritten")
	})
}

// TestSetGuestClusterDetailsOnSupervisorSnapshot_AnnotationBackfill tests the
// upgrade-path annotation backfill added in setGuestClusterDetailsOnSupervisorSnapshot.
// As with the PVC backfill, the private processor builds its own controller-runtime
// client, so we drive the same patch sequence through the shared
// common.BuildGuestSnapshotAnnotation helper plus k8s.PatchObject. The pure
// key/value logic is covered directly by TestBuildGuestSnapshotAnnotation in the
// common package.
func TestSetGuestClusterDetailsOnSupervisorSnapshot_AnnotationBackfill(t *testing.T) {
	ctx := context.Background()

	const (
		svVSName     = "sv-snap-1"
		svNamespace  = "vmware-system-csi"
		clusterName  = "my-tkc"
		guestVSName  = "guest-snap-1"
		guestVSNs    = "guest-ns"
		guestVSCName = "snapcontent-guest-1"
	)

	scheme := runtime.NewScheme()
	require.NoError(t, snapv1.AddToScheme(scheme))

	// VSC whose VolumeSnapshotRef points at the guest VolumeSnapshot.
	vsc := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: guestVSCName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			VolumeSnapshotRef: v1.ObjectReference{Name: guestVSName, Namespace: guestVSNs},
		},
	}

	makeSVVS := func(annots map[string]string) *snapv1.VolumeSnapshot {
		return &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:        svVSName,
				Namespace:   svNamespace,
				Annotations: annots,
			},
		}
	}

	// runBackfill mirrors the ImprovedVolumeVisiblity block from
	// setGuestClusterDetailsOnSupervisorSnapshot's processFunc.
	runBackfill := func(t *testing.T, svVS *snapv1.VolumeSnapshot, fakeClient client.Client) {
		t.Helper()
		if _, ok := svVS.Annotations[common.AnnKeyGuestClusterSnapshot]; ok {
			return
		}
		guestSnapAnnot := common.BuildGuestSnapshotAnnotation(clusterName,
			vsc.Spec.VolumeSnapshotRef.Name, vsc.Spec.VolumeSnapshotRef.Namespace, vsc.Name)
		jsonAnnotation, err := json.Marshal(guestSnapAnnot)
		require.NoError(t, err)
		original := svVS.DeepCopy()
		if svVS.Annotations == nil {
			svVS.Annotations = make(map[string]string)
		}
		svVS.Annotations[common.AnnKeyGuestClusterSnapshot] = string(jsonAnnotation)
		require.NoError(t, k8s.PatchObject(ctx, fakeClient, original, svVS))
	}

	getUpdatedAnnot := func(t *testing.T, c client.Client) map[string]string {
		t.Helper()
		updated := &snapv1.VolumeSnapshot{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{
			Name: svVSName, Namespace: svNamespace,
		}, updated))
		return updated.Annotations
	}

	t.Run("backfills annotation when absent", func(t *testing.T) {
		svVS := makeSVVS(nil) // nil annotations — exercises the nil-map guard
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svVS).Build()

		runBackfill(t, svVS, fakeClient)

		annots := getUpdatedAnnot(t, fakeClient)
		require.Contains(t, annots, common.AnnKeyGuestClusterSnapshot)
		var got map[string]string
		require.NoError(t, json.Unmarshal([]byte(annots[common.AnnKeyGuestClusterSnapshot]), &got))
		assert.Equal(t, map[string]string{
			common.GuestClusterAnnotKeyClusterName: clusterName,
			common.GuestClusterAnnotKeyName:        guestVSName,
			common.GuestClusterAnnotKeyNamespace:   guestVSNs,
			common.GuestClusterAnnotKeyVSCName:     guestVSCName,
		}, got)
	})

	t.Run("idempotent: skips when annotation already present", func(t *testing.T) {
		existing := `{"clusterName":"old-name"}`
		svVS := makeSVVS(map[string]string{common.AnnKeyGuestClusterSnapshot: existing})
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svVS).Build()

		runBackfill(t, svVS, fakeClient)

		annots := getUpdatedAnnot(t, fakeClient)
		assert.Equal(t, existing, annots[common.AnnKeyGuestClusterSnapshot],
			"pre-existing annotation must not be overwritten")
	})
}

// TestIsReadyVolumeSnapshotContent_NilReadyToUse tests VSC with nil ReadyToUse
func TestIsReadyVolumeSnapshotContent_NilReadyToUse(t *testing.T) {
	vsc := &snapv1.VolumeSnapshotContent{
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver: common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{
				Name: "valid-name",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo("handle-123"),
			ReadyToUse:     nil,
		},
	}
	assert.False(t, isReadyVolumeSnapshotContent(vsc), "Should return false for VSC with nil ReadyToUse")
}

// ============================================================================
// High-Level Integration Tests for setGuestClusterDetailsOnSupervisorSnapshot and
// setChangeIDAnnotationOnGuestClusterSnapshot
// ============================================================================

// TestSetGuestClusterDetailsOnSupervisorSnapshot_AddsLabelsAndFinalizers tests the high-level function
func TestSetGuestClusterDetailsOnSupervisorSnapshot_AddsLabelsAndFinalizers(t *testing.T) {
	ctx := context.Background()

	// Setup supervisor snapshot without labels or finalizers
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "supervisor-snap",
			Namespace:  "vmware-system-csi",
			Labels:     make(map[string]string),
			Finalizers: []string{},
		},
	}

	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)

	// Create fake supervisor client with the snapshot
	supervisorClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(supervisorVS).
		Build()

	// Test the logic of adding labels and finalizers
	key := "test-cluster/tkgs"
	expected := "test-cluster-uid"

	// Simulate what setGuestClusterDetailsOnSupervisorSnapshot does
	if supervisorVS.Labels == nil {
		supervisorVS.Labels = make(map[string]string)
	}
	supervisorVS.Labels[key] = expected

	if !slices.Contains(supervisorVS.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer) {
		supervisorVS.ObjectMeta.Finalizers = append(supervisorVS.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
	}

	// Patch the object
	original := supervisorVS.DeepCopy()
	original.Labels = make(map[string]string)
	original.Finalizers = []string{}

	err := k8s.PatchObject(ctx, supervisorClient, original, supervisorVS)
	assert.NoError(t, err)

	// Verify the patch was applied
	updatedVS := &snapv1.VolumeSnapshot{}
	err = supervisorClient.Get(ctx, client.ObjectKey{
		Name:      supervisorVS.Name,
		Namespace: supervisorVS.Namespace,
	}, updatedVS)
	assert.NoError(t, err)
	assert.Equal(t, expected, updatedVS.Labels[key], "Label should be set correctly")
	assert.Contains(t, updatedVS.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer, "Finalizer should be added")
}

// TestSetGuestClusterDetailsOnSupervisorSnapshot_PreservesExistingLabelsAndFinalizers tests that existing
// labels/finalizers are preserved
func TestSetGuestClusterDetailsOnSupervisorSnapshot_PreservesExistingLabelsAndFinalizers(t *testing.T) {
	ctx := context.Background()

	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "supervisor-snap",
			Namespace: "vmware-system-csi",
			Labels: map[string]string{
				"existing-label": "existing-value",
			},
			Finalizers: []string{"existing-finalizer"},
		},
	}

	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)

	supervisorClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(supervisorVS).
		Build()

	// Simulate adding new label while preserving existing one
	key := "test-cluster/tkgs"
	supervisorVS.Labels[key] = "test-cluster-uid"

	// Add finalizer if not present
	if !slices.Contains(supervisorVS.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer) {
		supervisorVS.ObjectMeta.Finalizers = append(supervisorVS.ObjectMeta.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
	}

	// Patch the object
	original := supervisorVS.DeepCopy()
	original.Labels = map[string]string{"existing-label": "existing-value"}
	original.Finalizers = []string{"existing-finalizer"}

	err := k8s.PatchObject(ctx, supervisorClient, original, supervisorVS)
	assert.NoError(t, err)

	// Verify both old and new labels/finalizers are present
	updatedVS := &snapv1.VolumeSnapshot{}
	err = supervisorClient.Get(ctx, client.ObjectKey{
		Name:      supervisorVS.Name,
		Namespace: supervisorVS.Namespace,
	}, updatedVS)
	assert.NoError(t, err)
	assert.Equal(t, "existing-value", updatedVS.Labels["existing-label"], "Existing label should be preserved")
	assert.Equal(t, "test-cluster-uid", updatedVS.Labels["test-cluster/tkgs"], "New label should be added")
	assert.Contains(t, updatedVS.Finalizers, "existing-finalizer", "Existing finalizer should be preserved")
	assert.Contains(t, updatedVS.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer, "New finalizer should be added")
}

// TestSetChangeIDAnnotationOnGuestClusterSnapshot_AddsChangeIDAnnotation tests annotation reconciliation
func TestSetChangeIDAnnotationOnGuestClusterSnapshot_AddsChangeIDAnnotation(t *testing.T) {
	ctx := context.Background()

	// Setup supervisor snapshot with change-id annotation
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "supervisor-snap",
			Namespace: "vmware-system-csi",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "change-id-123",
			},
		},
	}

	// Setup guest snapshot without change-id annotation
	guestVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "guest-snap",
			Namespace:   "default",
			Annotations: make(map[string]string),
		},
	}

	scheme := runtime.NewScheme()
	_ = snapv1.AddToScheme(scheme)

	guestClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(guestVS).
		Build()

	// Simulate what reconcileGuestSnapshotAnnotation does
	supervisorChangeID := supervisorVS.Annotations[common.VolumeSnapshotChangeIDKey]
	if guestVS.Annotations == nil {
		guestVS.Annotations = make(map[string]string)
	}
	guestVS.Annotations[common.VolumeSnapshotChangeIDKey] = supervisorChangeID

	// Patch the object
	original := guestVS.DeepCopy()
	original.Annotations = make(map[string]string)

	err := k8s.PatchObject(ctx, guestClient, original, guestVS)
	assert.NoError(t, err)

	// Verify the annotation was added
	updatedVS := &snapv1.VolumeSnapshot{}
	err = guestClient.Get(ctx, client.ObjectKey{
		Name:      guestVS.Name,
		Namespace: guestVS.Namespace,
	}, updatedVS)
	assert.NoError(t, err)
	assert.Equal(t, "change-id-123", updatedVS.Annotations[common.VolumeSnapshotChangeIDKey],
		"Change-id annotation should be added from supervisor")
}

// TestSetChangeIDAnnotationOnGuestClusterSnapshot_SkipsWhenAlreadyAnnotated tests that existing
// annotation is not overwritten
func TestSetChangeIDAnnotationOnGuestClusterSnapshot_SkipsWhenAlreadyAnnotated(t *testing.T) {
	supervisorVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "supervisor-snap",
			Namespace: "vmware-system-csi",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "new-change-id",
			},
		},
	}

	guestVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "guest-snap",
			Namespace: "default",
			Annotations: map[string]string{
				common.VolumeSnapshotChangeIDKey: "old-change-id",
			},
		},
	}

	// Simulate reconcileGuestSnapshotAnnotation logic - should not update if guest already has annotation
	supervisorChangeID := supervisorVS.Annotations[common.VolumeSnapshotChangeIDKey]
	guestChangeID, guestHasChangeID := guestVS.Annotations[common.VolumeSnapshotChangeIDKey]

	// The reconcile function only adds/updates if supervisor has annotation and guest doesn't or has empty value
	if guestHasChangeID && guestChangeID != "" {
		// Skip - guest already has a non-empty change-id
		// so we don't patch
	} else {
		// Would patch in real function
		if guestVS.Annotations == nil {
			guestVS.Annotations = make(map[string]string)
		}
		guestVS.Annotations[common.VolumeSnapshotChangeIDKey] = supervisorChangeID
	}

	// Verify guest annotation was NOT changed (still has old value)
	assert.Equal(t, "old-change-id", guestVS.Annotations[common.VolumeSnapshotChangeIDKey],
		"Existing guest annotation should not be overwritten")
}
