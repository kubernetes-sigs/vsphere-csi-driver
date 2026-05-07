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

package clusterstoragepolicyinfo

import (
	"context"
	"fmt"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// ownerReferenceKey returns OwnerReference key which is concatenated from the APIVersion, Kind and Name.
func ownerReferenceKey(ref metav1.OwnerReference) string {
	return ref.APIVersion + "/" + ref.Kind + "/" + ref.Name
}

// mergeOwnerReference merges the OwnerReferences slice with the new OwnerReference.
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

// volumeAttributesClassAPIAvailable reports whether the apiserver exposes VolumeAttributesClass
// (storage.k8s.io/v1). VAC is supportted from K8s version 1.34 onwards.
func volumeAttributesClassAPIAvailable(mgr manager.Manager) (bool, error) {
	cfg := mgr.GetConfig()
	if cfg == nil {
		return false, fmt.Errorf("manager REST config is nil")
	}
	return volumeAttributesClassAPIAvailableFromRESTConfig(cfg)
}

// volumeAttributesClassAPIAvailableFromRESTConfig is the REST/discovery implementation used by
// volumeAttributesClassAPIAvailable (also exercised directly in unit tests).
func volumeAttributesClassAPIAvailableFromRESTConfig(cfg *rest.Config) (bool, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return false, err
	}
	_, lists, err := dc.ServerGroupsAndResources()
	if lists == nil && err != nil {
		return false, err
	}
	gv := storagev1.SchemeGroupVersion.String()
	for _, list := range lists {
		if list.GroupVersion != gv {
			continue
		}
		for i := range list.APIResources {
			if list.APIResources[i].Name == "volumeattributesclasses" {
				return true, nil
			}
		}
	}
	return false, nil
}

// generateOwnerReference returns an OwnerReference for the given client.Object.
func generateOwnerReference(scheme *runtime.Scheme, owner client.Object) (metav1.OwnerReference, error) {
	gvk, err := apiutil.GVKForObject(owner, scheme)
	if err != nil {
		return metav1.OwnerReference{}, err
	}
	controller := false
	block := false
	return metav1.OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               owner.GetName(),
		UID:                owner.GetUID(),
		Controller:         &controller,
		BlockOwnerDeletion: &block,
	}, nil
}

// buildOwnerReferences builds owner references for StorageClass and VolumeAttributesClass if they exist.
func buildOwnerReferences(ctx context.Context, scheme *runtime.Scheme, name string,
	sc *storagev1.StorageClass, vac *storagev1.VolumeAttributesClass) []metav1.OwnerReference {
	log := logger.GetLogger(ctx)
	ownerRefs := make([]metav1.OwnerReference, 0)

	if sc != nil {
		ownerRef, err := generateOwnerReference(scheme, sc)
		if err != nil {
			log.Errorf("Failed to generate ownerReference for StorageClass %q: %v", name, err)
		} else {
			ownerRefs = append(ownerRefs, ownerRef)
		}
	}

	if vac != nil {
		ownerRef, err := generateOwnerReference(scheme, vac)
		if err != nil {
			log.Errorf("Failed to generate ownerReference for VolumeAttributesClass %q: %v", name, err)
		} else {
			ownerRefs = append(ownerRefs, ownerRef)
		}
	}

	return ownerRefs
}

// storageClassIsWaitForFirstConsumer indicates whether the StorageClass has a WaitForFirstConsumer volumeBindingMode.
func storageClassIsWaitForFirstConsumer(sc *storagev1.StorageClass) bool {
	return sc.VolumeBindingMode != nil &&
		*sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer
}
