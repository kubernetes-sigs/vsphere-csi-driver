package internal

import (
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// StorageClassKind is the kind for a StorageClass resource.
	StorageClassKind = "StorageClass"

	// StorageClassGroup is the API group to which a StorageClass resource
	// belongs.
	StorageClassGroup = storagev1.GroupName

	StorageClassAttributeStoragePolicyID = "storagepolicyid"

	// StorageClassGroupVersion is the API group and version version for a
	// StorageClass resource.
	StorageClassGroupVersion = StorageClassGroup + "/v1"

	// EncryptedStorageClassNamesConfigMapName is the name of the ConfigMap in
	// the VM Operator pod's namespace that indicates which StorageClasses
	// support encryption by virtue of the OwnerRefs set on the ConfigMap.
	EncryptedStorageClassNamesConfigMapName = "encrypted-storage-class-names"
)

// GetOwnerRefForStorageClass returns an OwnerRef for the provided StorageClass.
func GetOwnerRefForStorageClass(storageClass *storagev1.StorageClass) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: StorageClassGroupVersion,
		Kind:       StorageClassKind,
		Name:       storageClass.Name,
		UID:        storageClass.UID,
	}
}
