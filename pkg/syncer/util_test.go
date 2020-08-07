package syncer

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
