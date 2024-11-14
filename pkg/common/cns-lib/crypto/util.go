package crypto

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto/internal"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

func GetEncryptionClassNameForPVC(pvc *corev1.PersistentVolumeClaim) string {
	annotations := pvc.GetAnnotations()
	if annotations == nil {
		return ""
	}
	return annotations[PVCEncryptionClassAnnotationName]
}

func SetEncryptionClassNameForPVC(pvc *corev1.PersistentVolumeClaim, encClassName string) {
	annotations := pvc.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	if encClassName != "" {
		annotations[PVCEncryptionClassAnnotationName] = encClassName
	} else {
		delete(annotations, PVCEncryptionClassAnnotationName)
	}

	pvc.SetAnnotations(annotations)

}

func GetStoragePolicyID(sc *storagev1.StorageClass) string {
	if sc.Provisioner != types.Name {
		return ""
	}
	// vSphere CSI supports case insensitive parameters.
	for key, value := range sc.Parameters {
		if strings.ToLower(key) == internal.StorageClassAttributeStoragePolicyID {
			return value
		}
	}
	return ""
}
