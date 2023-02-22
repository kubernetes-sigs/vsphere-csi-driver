package syncer

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
