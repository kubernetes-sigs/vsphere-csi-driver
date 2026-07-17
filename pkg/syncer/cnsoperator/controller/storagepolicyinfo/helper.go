/*
Copyright 2026 The Kubernetes Authors.

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

package storagepolicyinfo

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	spiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicyinfo/v1alpha1"
)

// ownerReferenceKey returns an OwnerReference key concatenated from APIVersion, Kind and Name.
func ownerReferenceKey(ref metav1.OwnerReference) string {
	return ref.APIVersion + "/" + ref.Kind + "/" + ref.Name
}

// mergeOwnerReference merges add into refs:
//   - if a reference with the same (APIVersion, Kind, Name) already exists and has
//     the same UID, refs is returned unchanged;
//   - if it exists with a different UID, the entry is replaced;
//   - otherwise add is appended.
func mergeOwnerReference(refs []metav1.OwnerReference, add metav1.OwnerReference) []metav1.OwnerReference {
	key := ownerReferenceKey(add)
	for i := range refs {
		if ownerReferenceKey(refs[i]) == key {
			if refs[i].UID == add.UID {
				return refs
			}
			out := make([]metav1.OwnerReference, len(refs))
			copy(out, refs)
			out[i] = add
			return out
		}
	}
	out := make([]metav1.OwnerReference, len(refs), len(refs)+1)
	copy(out, refs)
	return append(out, add)
}

// generateOwnerReference returns an OwnerReference for the given client.Object.
func generateOwnerReference(scheme *runtime.Scheme, owner client.Object) (metav1.OwnerReference, error) {
	gvk, err := apiutil.GVKForObject(owner, scheme)
	if err != nil {
		return metav1.OwnerReference{}, err
	}
	controllerRef := false
	block := false
	return metav1.OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		Controller:         &controllerRef,
		BlockOwnerDeletion: &block,
	}, nil
}

// buildClusterSPIRef returns a StoragePolicyInfoSpec ClusterStoragePolicyInfoReference pointing at
// the ClusterStoragePolicyInfo that shares the given storage policy name. StoragePolicyInfo,
// InfraStoragePolicyInfo and ClusterStoragePolicyInfo all use the same K8sCompliantName as their
// object name, so the reference can be built without fetching the ClusterStoragePolicyInfo object.
func buildClusterSPIRef(scheme *runtime.Scheme, name string) (spiv1alpha1.ClusterStoragePolicyInfoReference, error) {
	gvk, err := apiutil.GVKForObject(&clusterspiv1alpha1.ClusterStoragePolicyInfo{}, scheme)
	if err != nil {
		return spiv1alpha1.ClusterStoragePolicyInfoReference{}, err
	}
	return spiv1alpha1.ClusterStoragePolicyInfoReference{
		Name:     name,
		Kind:     gvk.Kind,
		APIGroup: gvk.GroupVersion().String(),
	}, nil
}
