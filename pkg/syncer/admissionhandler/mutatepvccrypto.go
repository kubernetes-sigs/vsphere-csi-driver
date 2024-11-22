package admissionhandler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
)

// SetDefaultEncryptionClass assigns spec.crypto.encryptionClassName to the
// namespace's default EncryptionClass when creating a VM if spec.crypto is
// nil or spec.crypto.encryptionClassName is empty.
func setDefaultEncryptionClass(
	ctx context.Context,
	cryptoClient crypto.Client,
	pvc *corev1.PersistentVolumeClaim) (bool, error) {

	// Do not handle encryption if the PVC does not specify a StorageClass.
	if pvc.Spec.StorageClassName == nil {
		return false, nil
	}

	// Return early if the PVC already specifies an EncryptionClass.
	if encClassName := crypto.GetEncryptionClassNameForPVC(pvc); encClassName != "" {
		return false, nil
	}

	if ok, _, err := cryptoClient.IsEncryptedStorageClass(ctx, *pvc.Spec.StorageClassName); err != nil {
		return false, err
	} else if !ok {
		// The StorageClass does not support encryption, so we do not need to set
		// a default EncryptionClass for the PVC.
		return false, nil
	}

	defaultEncClass, err := cryptoClient.GetDefaultEncryptionClass(ctx, pvc.Namespace)
	if err != nil {
		if err == crypto.ErrDefaultEncryptionClassNotFound {
			return false, nil
		}
		return false, err
	}

	crypto.SetEncryptionClassNameForPVC(pvc, defaultEncClass.Name)

	return true, nil
}
