package syncer

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/syncer/k8scloudoperator"
)

var (
	validMigratedPVCMetadata   metav1.ObjectMeta
	validMigratedPVMetadata    metav1.ObjectMeta
	validLegacyPVCMetadata     metav1.ObjectMeta
	validLegacyPVMetadata      metav1.ObjectMeta
	invalidMigratedPVCMetadata metav1.ObjectMeta
	invalidMigratedPVMetadata  metav1.ObjectMeta
)

func init() {
	validMigratedPVCMetadata = metav1.ObjectMeta{
		Name: "migrated-vcppvc",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":                  "csi.vsphere.vmware.com",
			"volume.beta.kubernetes.io/storage-provisioner": "kubernetes.io/vsphere-volume",
		},
	}
	validLegacyPVCMetadata = metav1.ObjectMeta{
		Name: "vcppvcProvisionedByCSI",
		Annotations: map[string]string{
			"volume.beta.kubernetes.io/storage-provisioner": "csi.vsphere.vmware.com",
		},
	}
	validMigratedPVMetadata = metav1.ObjectMeta{
		Name: "migrated-vcppv",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":    "csi.vsphere.vmware.com",
			"pv.kubernetes.io/provisioned-by": "kubernetes.io/vsphere-volume",
		},
	}
	validLegacyPVMetadata = metav1.ObjectMeta{
		Name: "vcppvProvisionedByCSI",
		Annotations: map[string]string{
			"pv.kubernetes.io/provisioned-by": "csi.vsphere.vmware.com",
		},
	}
	invalidMigratedPVCMetadata = metav1.ObjectMeta{
		Name: "migrated-invalid-vcppvc",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":                  "unknown.csi.driver",
			"volume.beta.kubernetes.io/storage-provisioner": "kubernetes.io/vsphere-volume",
		},
	}
	invalidMigratedPVCMetadata = metav1.ObjectMeta{
		Name: "migrated-invalid-vcppv",
		Annotations: map[string]string{
			"pv.kubernetes.io/migrated-to":    "unknown.csi.driver",
			"pv.kubernetes.io/provisioned-by": "kubernetes.io/vsphere-volume",
		},
	}

}
func TestValidMigratedAndLegacyVolume(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !isValidvSphereVolumeClaim(ctx, validMigratedPVCMetadata) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return True\n Actual: isValidvSphereVolumeClaim returned False")
	}
	if !isValidvSphereVolumeClaim(ctx, validLegacyPVCMetadata) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return True\n Actual: isValidvSphereVolumeClaim returned False")
	}
	if isValidvSphereVolumeClaim(ctx, invalidMigratedPVCMetadata) {
		t.Errorf("Expected: isValidvSphereVolumeClaim to return False\n Actual: isValidvSphereVolumeClaim returned True")
	}
	if !isValidvSphereVolume(ctx, validMigratedPVMetadata) {
		t.Errorf("Expected: isValidvSphereVolume to return True\n Actual: isValidvSphereVolume returned False")
	}
	if !isValidvSphereVolume(ctx, validLegacyPVMetadata) {
		t.Errorf("Expected: isValidvSphereVolume to return True\n Actual: isValidvSphereVolume returned False")
	}
	if isValidvSphereVolume(ctx, invalidMigratedPVMetadata) {
		t.Errorf("Expected: isValidvSphereVolume to return Fale\n Actual: isValidvSphereVolume returned True")
	}
}

// This test verifies the correctness of GetSCNameFromPVC in following scenarios
// where SC name is provided through:
//    1. Only Spec.StorageClassName
//    2. Only Metadata.Annotation
//    3. Both Spec.StorageClassName and Metadata.Annotation
//    4. Neither Spec.StorageClassName nor Metadata.Annotation
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
