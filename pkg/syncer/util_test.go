package syncer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
)

var (
	validMigratedPVCMetadata                            metav1.ObjectMeta
	validMigratedPVCMetadataWithGAStorageProvisionerAnn metav1.ObjectMeta
	validLegacyPVCMetadata                              metav1.ObjectMeta
	validLegacyPVCMetadataWithGAStorageProvisionerAnn   metav1.ObjectMeta
	invalidMigratedPVCMetadata                          metav1.ObjectMeta
	validMigratedPV                                     *corev1.PersistentVolume
	validLegacyPV                                       *corev1.PersistentVolume
	validLegacyStaticPV                                 *corev1.PersistentVolume
	invalidMigratedPV                                   *corev1.PersistentVolume
)

func init() {
	validMigratedPVCMetadata = metav1.ObjectMeta{
		Name: "migrated-vcppvc",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":                  "csi.vsphere.vmware.com",
			"volume.beta.kubernetes.io/storage-provisioner": "kubernetes.io/vsphere-volume",
		},
	}
	validMigratedPVCMetadataWithGAStorageProvisionerAnn = metav1.ObjectMeta{
		Name: "migrated-vcppvc",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":             "csi.vsphere.vmware.com",
			"volume.kubernetes.io/storage-provisioner": "kubernetes.io/vsphere-volume",
		},
	}
	validLegacyPVCMetadata = metav1.ObjectMeta{
		Name: "vcppvcProvisionedByCSI",
		Annotations: map[string]string{
			"volume.beta.kubernetes.io/storage-provisioner": "csi.vsphere.vmware.com",
		},
	}
	validLegacyPVCMetadataWithGAStorageProvisionerAnn = metav1.ObjectMeta{
		Name: "vcppvcProvisionedByCSI",
		Annotations: map[string]string{
			"volume.kubernetes.io/storage-provisioner": "csi.vsphere.vmware.com",
		},
	}
	validMigratedPV = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: "migrated-vcppv",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":    "csi.vsphere.vmware.com",
			"pv.kubernetes.io/provisioned-by": "kubernetes.io/vsphere-volume",
		},
	}}
	validLegacyPV = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: "vcppvProvisionedByCSI",
		Annotations: map[string]string{
			"pv.kubernetes.io/provisioned-by": "csi.vsphere.vmware.com",
		},
	}}
	validLegacyStaticPV = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: "vcppvProvisionedByCSI",
	},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{VsphereVolume: &corev1.VsphereVirtualDiskVolumeSource{
				VolumePath: "[vsanDatastore] 80f3bf62-3d22-5158-e245-020049aca941/e7dcb90169ff44a4871712e809e9e86e.vmdk",
			}},
		},
	}
	invalidMigratedPVCMetadata = metav1.ObjectMeta{
		Name: "migrated-invalid-vcppvc",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":                  "unknown.csi.driver",
			"volume.beta.kubernetes.io/storage-provisioner": "kubernetes.io/vsphere-volume",
		},
	}
	invalidMigratedPV = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{
		Name: "migrated-invalid-vcppv",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":    "unknown.csi.driver",
			"pv.kubernetes.io/provisioned-by": "kubernetes.io/vsphere-volume",
		},
	}}

}
func TestValidMigratedAndLegacyVolume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !isValidvSphereVolumeClaim(ctx, validMigratedPVCMetadata) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return True\n Actual: isValidvSphereVolumeClaim returned False")
	}
	if !isValidvSphereVolumeClaim(ctx, validMigratedPVCMetadataWithGAStorageProvisionerAnn) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return True\n Actual: isValidvSphereVolumeClaim returned False")
	}
	if !isValidvSphereVolumeClaim(ctx, validLegacyPVCMetadata) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return True\n Actual: isValidvSphereVolumeClaim returned False")
	}
	if !isValidvSphereVolumeClaim(ctx, validLegacyPVCMetadataWithGAStorageProvisionerAnn) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return True\n Actual: isValidvSphereVolumeClaim returned False")
	}
	if isValidvSphereVolumeClaim(ctx, invalidMigratedPVCMetadata) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return False\n Actual: isValidvSphereVolumeClaim returned True")
	}
	if !isValidvSphereVolume(ctx, validMigratedPV) {
		t.Errorf("Expected: isValidvSphereVolume to return True\n Actual: isValidvSphereVolume returned False")
	}
	if !isValidvSphereVolume(ctx, validLegacyPV) {
		t.Errorf("Expected: isValidvSphereVolume to return True\n Actual: isValidvSphereVolume returned False")
	}
	if !isValidvSphereVolume(ctx, validLegacyStaticPV) {
		t.Errorf("Expected: isValidvSphereVolume to return True\n Actual: isValidvSphereVolume returned False")
	}
	if isValidvSphereVolume(ctx, invalidMigratedPV) {
		t.Errorf("Expected: isValidvSphereVolume to return Fale\n Actual: isValidvSphereVolume returned True")
	}
}

// This test verifies the correctness of GetSCNameFromPVC in following scenarios
// where SC name is provided through:
//  1. Only Spec.StorageClassName
//  2. Only Metadata.Annotation
//  3. Both Spec.StorageClassName and Metadata.Annotation
//  4. Neither Spec.StorageClassName nor Metadata.Annotation
func TestGetSCNameFromPVC(t *testing.T) {
	// Create context.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	t.Log("testGetSCNameFromPVC: start")
	namespace := testNamespace
	specSCName := testSCName + "-" + uuid.New().String()
	annotatedSCName := testSCName + "-" + uuid.New().String()
	pvcLabel := make(map[string]string)
	verifySCName := func(recvSCName string, recvErr error, expSCName string, expErr error) error {
		if !reflect.DeepEqual(expErr, recvErr) || expSCName != recvSCName {
			return fmt.Errorf("expected error: %v and %v as storage class name "+
				"but got error: %v and %v as storage class name", expErr, expSCName, recvErr, recvSCName)
		}
		return nil
	}

	// Scenario 1.
	t.Log("Verifying GetSCNameFromPVC for case where SC name is provided through only Spec.StorageClassName")
	pvcName := testPVCName + "-" + uuid.New().String()
	pvc := getPersistentVolumeClaimSpec(pvcName, namespace, pvcLabel, "", specSCName)
	scName, err := k8scloudoperator.GetSCNameFromPVC(pvc)
	err = verifySCName(scName, err, specSCName, nil)
	if err != nil {
		t.Error(err)
	}

	// Scenario 2.
	t.Log("Verifying GetSCNameFromPVC for case where SC name is provided through only Metadata.Annotation")
	pvcName = testPVCName + "-" + uuid.New().String()
	pvc = getPersistentVolumeClaimSpec(pvcName, namespace, pvcLabel, "", "")
	pvc.Annotations = map[string]string{k8scloudoperator.ScNameAnnotationKey: annotatedSCName}
	scName, err = k8scloudoperator.GetSCNameFromPVC(pvc)
	err = verifySCName(scName, err, annotatedSCName, nil)
	if err != nil {
		t.Error(err)
	}

	// Scenario 3.
	t.Log("Verifying GetSCNameFromPVC for case where SC name is provided through " +
		"both Spec.StorageClassName and Metadata.Annotation")
	pvcName = testPVCName + "-" + uuid.New().String()
	pvc = getPersistentVolumeClaimSpec(pvcName, namespace, pvcLabel, "", specSCName)
	pvc.Annotations = map[string]string{k8scloudoperator.ScNameAnnotationKey: annotatedSCName}
	scName, err = k8scloudoperator.GetSCNameFromPVC(pvc)
	err = verifySCName(scName, err, specSCName, nil)
	if err != nil {
		t.Error(err)
	}

	// Scenario 4.
	t.Log("Verifying GetSCNameFromPVC for case where SC name is provided through " +
		"neither Spec.StorageClassName nor Metadata.Annotation")
	pvcName = testPVCName + "-" + uuid.New().String()
	pvc = getPersistentVolumeClaimSpec(pvcName, namespace, pvcLabel, "", "")
	expError := fmt.Errorf("storage class name not specified in PVC %q", pvcName)
	scName, err = k8scloudoperator.GetSCNameFromPVC(pvc)
	err = verifySCName(scName, err, "", expError)
	if err != nil {
		t.Error(err)
	}
	t.Log("testGetSCNameFromPVC: end")
}

func TestGetTopologySegmentsFromNodeAffinityRulesSingleMatchExpression(t *testing.T) {
	// Create context.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	t.Log("getTopologySegmentsFromNodeAffinityRules with a single match expression")

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testPv",
		},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.csi.vmware.com/k8s-zone",
									Operator: "In",
									Values:   []string{"zone-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-building",
									Operator: "In",
									Values:   []string{"building-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-level",
									Operator: "In",
									Values:   []string{"level-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-rack",
									Operator: "In",
									Values:   []string{"rack-1", "rack-2"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-region",
									Operator: "In",
									Values:   []string{"region-1"},
								},
							},
						},
					},
				},
			},
		},
	}

	topologySegments := getTopologySegmentsFromNodeAffinityRules(ctx, pv)

	expectedTopologySegments := []map[string][]string{
		{
			"topology.csi.vmware.com/k8s-zone":     {"zone-1"},
			"topology.csi.vmware.com/k8s-building": {"building-1"},
			"topology.csi.vmware.com/k8s-level":    {"level-1"},
			"topology.csi.vmware.com/k8s-rack":     {"rack-1", "rack-2"},
			"topology.csi.vmware.com/k8s-region":   {"region-1"},
		},
	}

	if len(topologySegments) != len(expectedTopologySegments) {
		t.Errorf("Unequal number of topology segments. Expected: %d, Received: %d",
			len(expectedTopologySegments), len(topologySegments))
	}

	for _, topology := range topologySegments {
		foundMatch := false
		for _, expectedTopology := range expectedTopologySegments {
			if reflect.DeepEqual(expectedTopology, topology) {
				foundMatch = true
				break
			}
		}
		if foundMatch == false {
			t.Errorf("Mismatch in topology segments")
		}
	}
}

func TestGetTopologySegmentsFromNodeAffinityRulesMultipleMatchExpressions(t *testing.T) {
	// Create context.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	t.Log("getTopologySegmentsFromNodeAffinityRules with multiple match expressions")

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testPv",
		},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.csi.vmware.com/k8s-zone",
									Operator: "In",
									Values:   []string{"zone-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-building",
									Operator: "In",
									Values:   []string{"building-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-level",
									Operator: "In",
									Values:   []string{"level-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-rack",
									Operator: "In",
									Values:   []string{"rack-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-region",
									Operator: "In",
									Values:   []string{"region-1"},
								},
							},
						},
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "topology.csi.vmware.com/k8s-zone",
									Operator: "In",
									Values:   []string{"zone-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-building",
									Operator: "In",
									Values:   []string{"building-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-level",
									Operator: "In",
									Values:   []string{"level-1"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-rack",
									Operator: "In",
									Values:   []string{"rack-2"},
								},
								{
									Key:      "topology.csi.vmware.com/k8s-region",
									Operator: "In",
									Values:   []string{"region-1"},
								},
							},
						},
					},
				},
			},
		},
	}

	topologySegments := getTopologySegmentsFromNodeAffinityRules(ctx, pv)

	expectedTopologySegments := []map[string][]string{
		{
			"topology.csi.vmware.com/k8s-zone":     {"zone-1"},
			"topology.csi.vmware.com/k8s-building": {"building-1"},
			"topology.csi.vmware.com/k8s-level":    {"level-1"},
			"topology.csi.vmware.com/k8s-rack":     {"rack-1"},
			"topology.csi.vmware.com/k8s-region":   {"region-1"},
		},
		{
			"topology.csi.vmware.com/k8s-zone":     {"zone-1"},
			"topology.csi.vmware.com/k8s-building": {"building-1"},
			"topology.csi.vmware.com/k8s-level":    {"level-1"},
			"topology.csi.vmware.com/k8s-rack":     {"rack-2"},
			"topology.csi.vmware.com/k8s-region":   {"region-1"},
		},
	}

	if len(topologySegments) != len(expectedTopologySegments) {
		t.Errorf("Unequal number of topology segments. Expected: %d, Received: %d",
			len(expectedTopologySegments), len(topologySegments))
	}

	for _, topology := range topologySegments {
		foundMatch := false
		for _, expectedTopology := range expectedTopologySegments {
			if reflect.DeepEqual(expectedTopology, topology) {
				foundMatch = true
				break
			}
		}
		if foundMatch == false {
			t.Errorf("Mismatch in topology segments")
		}
	}
}

func TestHasClusterDistributionSet(t *testing.T) {
	tests := []struct {
		name                        string
		volume                      cnstypes.CnsVolume
		clusterIDforVolumeMetadata  string
		expectedClusterDistribution string
		expectedResult              bool
	}{
		{
			name: "Cluster distribution matches",
			volume: cnstypes.CnsVolume{
				VolumeId: cnstypes.CnsVolumeId{
					Id: "volumeID",
				},
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerClusterArray: []cnstypes.CnsContainerCluster{
						{ClusterId: "cluster-1", ClusterDistribution: "SupervisorCluster"},
						{ClusterId: "cluster-2", ClusterDistribution: "TKGService"},
					},
				},
			},
			clusterIDforVolumeMetadata:  "cluster-1",
			expectedClusterDistribution: "SupervisorCluster",
			expectedResult:              true,
		},
		{
			name: "Cluster distribution does not set",
			volume: cnstypes.CnsVolume{
				VolumeId: cnstypes.CnsVolumeId{
					Id: "volumeID",
				},
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerClusterArray: []cnstypes.CnsContainerCluster{
						{ClusterId: "cluster-1", ClusterDistribution: ""},
						{ClusterId: "cluster-2", ClusterDistribution: "TKGService"},
					},
				},
			},
			clusterIDforVolumeMetadata:  "cluster-1",
			expectedClusterDistribution: "SupervisorCluster",
			expectedResult:              false,
		},
		{
			volume: cnstypes.CnsVolume{
				VolumeId: cnstypes.CnsVolumeId{
					Id: "volumeID",
				},
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerClusterArray: []cnstypes.CnsContainerCluster{
						{ClusterId: "cluster-1", ClusterDistribution: "SupervisorCluster"},
						{ClusterId: "cluster-2", ClusterDistribution: "TKGService"},
					},
				},
			},
			clusterIDforVolumeMetadata:  "cluster-3",
			expectedClusterDistribution: "SupervisorCluster",
			expectedResult:              false,
		},
		{
			name: "No container clusters in metadata",
			volume: cnstypes.CnsVolume{
				VolumeId: cnstypes.CnsVolumeId{
					Id: "volumeID",
				},
				Metadata: cnstypes.CnsVolumeMetadata{
					ContainerClusterArray: []cnstypes.CnsContainerCluster{},
				},
			},
			clusterIDforVolumeMetadata:  "cluster-1",
			expectedClusterDistribution: "SupervisorCluster",
			expectedResult:              false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := hasClusterDistributionSet(context.Background(),
				test.volume, test.clusterIDforVolumeMetadata, test.expectedClusterDistribution)
			assert.Equal(t, test.expectedResult, result)
		})
	}
}

func TestHasVMOwnerRef(t *testing.T) {
	tests := []struct {
		name     string
		pvc      *corev1.PersistentVolumeClaim
		expected bool
	}{
		{
			name:     "nil PVC",
			pvc:      nil,
			expected: false,
		},
		{
			name: "PVC with no owner references",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pvc",
					Namespace:       "test-ns",
					OwnerReferences: nil,
				},
			},
			expected: false,
		},
		{
			name: "PVC with empty owner references",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pvc",
					Namespace:       "test-ns",
					OwnerReferences: []metav1.OwnerReference{},
				},
			},
			expected: false,
		},
		{
			name: "PVC with VM ownerRef (v1alpha3)",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "vmoperator.vmware.com/v1alpha3",
							Kind:       "VirtualMachine",
							Name:       "my-vm",
							UID:        "vm-uid-123",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "PVC with VM ownerRef (v1alpha5)",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "vmoperator.vmware.com/v1alpha5",
							Kind:       "VirtualMachine",
							Name:       "my-vm",
							UID:        "vm-uid-123",
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "PVC with non-VM ownerRef",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "my-sts",
							UID:        "sts-uid-123",
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "PVC with VirtualMachine kind but wrong API group",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "other.vmware.com/v1",
							Kind:       "VirtualMachine",
							Name:       "my-vm",
							UID:        "vm-uid-123",
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "PVC with multiple ownerRefs including VM",
			pvc: &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvc",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "my-sts",
							UID:        "sts-uid-123",
						},
						{
							APIVersion: "vmoperator.vmware.com/v1alpha5",
							Kind:       "VirtualMachine",
							Name:       "my-vm",
							UID:        "vm-uid-456",
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := hasVMOwnerRef(tc.pvc)
			assert.Equal(t, tc.expected, result, "hasVMOwnerRef result mismatch")
		})
	}
}

// vmOwnerRefTestVolumeManager is a mock volume manager for testing handleVMOwnerRefChange.
// It tracks calls to ProtectVolumeFromVMDeletion and UnprotectVolumeFromVMDeletion.
type vmOwnerRefTestVolumeManager struct {
	*unittestcommon.MockVolumeManager

	mu             sync.Mutex
	protectCalls   []string
	unprotectCalls []string
	protectErr     error
	unprotectErr   error
}

func (m *vmOwnerRefTestVolumeManager) ProtectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.protectCalls = append(m.protectCalls, volumeID)
	return m.protectErr
}

func (m *vmOwnerRefTestVolumeManager) UnprotectVolumeFromVMDeletion(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unprotectCalls = append(m.unprotectCalls, volumeID)
	return m.unprotectErr
}

func (m *vmOwnerRefTestVolumeManager) getProtectCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.protectCalls...)
}

func (m *vmOwnerRefTestVolumeManager) getUnprotectCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.unprotectCalls...)
}

func TestHandleVMOwnerRefChange(t *testing.T) {
	ctx := context.Background()
	volumeID := "test-volume-id"

	basePVC := func() *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-ns",
			},
		}
	}

	vmOwnerRef := metav1.OwnerReference{
		APIVersion: "vmoperator.vmware.com/v1alpha5",
		Kind:       "VirtualMachine",
		Name:       "my-vm",
		UID:        "vm-uid-123",
	}

	tests := []struct {
		name            string
		oldPvc          *corev1.PersistentVolumeClaim
		newPvc          *corev1.PersistentVolumeClaim
		expectProtect   bool
		expectUnprotect bool
	}{
		{
			name:            "No change - neither has VM ownerRef",
			oldPvc:          basePVC(),
			newPvc:          basePVC(),
			expectProtect:   false,
			expectUnprotect: false,
		},
		{
			name: "No change - both have VM ownerRef",
			oldPvc: func() *corev1.PersistentVolumeClaim {
				p := basePVC()
				p.OwnerReferences = []metav1.OwnerReference{vmOwnerRef}
				return p
			}(),
			newPvc: func() *corev1.PersistentVolumeClaim {
				p := basePVC()
				p.OwnerReferences = []metav1.OwnerReference{vmOwnerRef}
				return p
			}(),
			expectProtect:   false,
			expectUnprotect: false,
		},
		{
			name:   "VM ownerRef added - should unprotect (clear keepAfterDeleteVm)",
			oldPvc: basePVC(),
			newPvc: func() *corev1.PersistentVolumeClaim {
				p := basePVC()
				p.OwnerReferences = []metav1.OwnerReference{vmOwnerRef}
				return p
			}(),
			expectProtect:   false,
			expectUnprotect: true,
		},
		{
			name: "VM ownerRef removed - should protect (set keepAfterDeleteVm)",
			oldPvc: func() *corev1.PersistentVolumeClaim {
				p := basePVC()
				p.OwnerReferences = []metav1.OwnerReference{vmOwnerRef}
				return p
			}(),
			newPvc:          basePVC(),
			expectProtect:   true,
			expectUnprotect: false,
		},
		{
			name: "VM ownerRef removed but PVC has deletion timestamp - skip protect",
			oldPvc: func() *corev1.PersistentVolumeClaim {
				p := basePVC()
				p.OwnerReferences = []metav1.OwnerReference{vmOwnerRef}
				return p
			}(),
			newPvc: func() *corev1.PersistentVolumeClaim {
				p := basePVC()
				now := metav1.NewTime(time.Now())
				p.DeletionTimestamp = &now
				return p
			}(),
			expectProtect:   false,
			expectUnprotect: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockMgr := &vmOwnerRefTestVolumeManager{
				MockVolumeManager: &unittestcommon.MockVolumeManager{},
			}

			// Pass nil for k8sClient since we're only testing the protect/unprotect logic,
			// not the annotation update (which requires a real k8s client).
			handleVMOwnerRefChange(ctx, tc.oldPvc, tc.newPvc, volumeID, mockMgr, nil)

			protectCalls := mockMgr.getProtectCalls()
			unprotectCalls := mockMgr.getUnprotectCalls()

			if tc.expectProtect {
				assert.Len(t, protectCalls, 1, "Expected exactly 1 protect call")
				assert.Equal(t, volumeID, protectCalls[0], "Protect called with wrong volumeID")
			} else {
				assert.Empty(t, protectCalls, "Expected no protect calls")
			}

			if tc.expectUnprotect {
				assert.Len(t, unprotectCalls, 1, "Expected exactly 1 unprotect call")
				assert.Equal(t, volumeID, unprotectCalls[0], "Unprotect called with wrong volumeID")
			} else {
				assert.Empty(t, unprotectCalls, "Expected no unprotect calls")
			}
		})
	}
}

func TestHandleVMOwnerRefChange_ErrorCases(t *testing.T) {
	ctx := context.Background()
	volumeID := "test-volume-id"

	vmOwnerRef := metav1.OwnerReference{
		APIVersion: "vmoperator.vmware.com/v1alpha5",
		Kind:       "VirtualMachine",
		Name:       "my-vm",
		UID:        "vm-uid-123",
	}

	t.Run("Protect error is logged but does not panic", func(t *testing.T) {
		oldPvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "test-pvc",
				Namespace:       "test-ns",
				OwnerReferences: []metav1.OwnerReference{vmOwnerRef},
			},
		}
		newPvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-ns",
			},
		}

		mockMgr := &vmOwnerRefTestVolumeManager{
			MockVolumeManager: &unittestcommon.MockVolumeManager{},
			protectErr:        errors.New("protect failed"),
		}

		// Should not panic (pass nil for k8sClient)
		handleVMOwnerRefChange(ctx, oldPvc, newPvc, volumeID, mockMgr, nil)
		assert.Len(t, mockMgr.getProtectCalls(), 1, "Protect should have been called")
	})

	t.Run("Unprotect error is logged but does not panic", func(t *testing.T) {
		oldPvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: "test-ns",
			},
		}
		newPvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "test-pvc",
				Namespace:       "test-ns",
				OwnerReferences: []metav1.OwnerReference{vmOwnerRef},
			},
		}

		mockMgr := &vmOwnerRefTestVolumeManager{
			MockVolumeManager: &unittestcommon.MockVolumeManager{},
			unprotectErr:      errors.New("unprotect failed"),
		}

		// Should not panic (pass nil for k8sClient)
		handleVMOwnerRefChange(ctx, oldPvc, newPvc, volumeID, mockMgr, nil)
		assert.Len(t, mockMgr.getUnprotectCalls(), 1, "Unprotect should have been called")
	})
}
