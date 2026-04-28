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
	"strconv"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
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

// storageClassIsWaitForFirstConsumer indicates whether the StorageClass has a WaitForFirstConsumer volumeBindingMode.
func storageClassIsWaitForFirstConsumer(sc *storagev1.StorageClass) bool {
	return sc.VolumeBindingMode != nil &&
		*sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer
}

// checkVsanEncryption checks if a storage policy has vSAN encryption enabled
// by examining policy rules for dataAtRestEncryption capability.
func checkVsanEncryption(policyContent []cnsvsphere.SpbmPolicyContent) bool {
	for _, policy := range policyContent {
		for _, subProfile := range policy.Profiles {
			for _, rule := range subProfile.Rules {
				if rule.PropID == vsanEncryptionPropID {
					return true
				}
			}
		}
	}
	return false
}

// hasVmEncryptionRule reports whether any rule in policyContent signals VM encryption:
// namespace == vmwarevmcrypt and CapID == vmwarevmcrypt@ENCRYPTION.
func hasVmEncryptionRule(policyContent []cnsvsphere.SpbmPolicyContent) bool {
	for _, policy := range policyContent {
		for _, subProfile := range policy.Profiles {
			for _, rule := range subProfile.Rules {
				if rule.Ns == vmEncryptionNs && rule.CapID == vmEncryptionCapID {
					return true
				}
			}
		}
	}
	return false
}

// checkVmEncryption checks if a storage policy has VM encryption enabled.
//
// Two passes are performed:
//  1. Direct: looks for vmwarevmcrypt@ENCRYPTION capability in the vmwarevmcrypt namespace.
//  2. Indirect: if a rule with namespace com.vmware.storageprofile.dataservice is found,
//     its CapID is used to fetch the referenced policy content, which is then re-checked
//     for VM encryption.
//
// retrieveContent abstracts the PbmRetrieveContent call so the function is testable
// without a live vCenter connection.
func checkVmEncryption(ctx context.Context,
	retrieveContent func(context.Context, []string) ([]cnsvsphere.SpbmPolicyContent, error),
	policyContent []cnsvsphere.SpbmPolicyContent) (bool, error) {
	log := logger.GetLogger(ctx)

	// Direct check: VM encryption rule present in the policy itself.
	if hasVmEncryptionRule(policyContent) {
		return true, nil
	}

	// Indirect check: follow any data-service reference and repeat the lookup.
	for _, policy := range policyContent {
		for _, subProfile := range policy.Profiles {
			for _, rule := range subProfile.Rules {
				if rule.Ns != dataserviceNs || rule.CapID == "" {
					continue
				}
				log.Infof("Checking referenced data service policy %q for VM encryption", rule.CapID)
				refContent, err := retrieveContent(ctx, []string{rule.CapID})
				if err != nil {
					return false, fmt.Errorf(
						"failed to retrieve referenced policy %q: %w", rule.CapID, err)
				}
				if hasVmEncryptionRule(refContent) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// extractIopsLimit searches the policy content for a vSAN IOPS limit rule and returns
// its value. Returns (nil, nil) if no IOPS limit rule is present, or an error if the
// rule value cannot be parsed as an integer.
func extractIopsLimit(policyContent []cnsvsphere.SpbmPolicyContent) (*int64, error) {
	for _, policy := range policyContent {
		for _, subProfile := range policy.Profiles {
			for _, rule := range subProfile.Rules {
				if rule.Ns == vsanIopsLimitNs && rule.PropID == vsanIopsLimitPropID {
					iops, err := strconv.ParseInt(rule.Value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("failed to parse IOPS limit value %q: %w", rule.Value, err)
					}
					return &iops, nil
				}
			}
		}
	}
	return nil, nil
}

// populatePerformanceCapabilities inspects the policy content for vSAN performance
// attributes and populates the Performance status in the ClusterStoragePolicyInfo.
// Returns an error if the IOPS limit value cannot be parsed.
func populatePerformanceCapabilities(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, profileID string,
	policyContent []cnsvsphere.SpbmPolicyContent) error {
	log := logger.GetLogger(ctx)

	iopsLimit, err := extractIopsLimit(policyContent)
	if err != nil {
		instance.Status.Performance = nil
		return fmt.Errorf("storage policy %s has invalid IOPS limit: %w", profileID, err)
	}
	if iopsLimit == nil {
		log.Infof("Storage policy %s has no IOPS limit", profileID)
		instance.Status.Performance = nil
		return nil
	}

	log.Infof("Storage policy %s has IOPS limit: %d", profileID, *iopsLimit)
	instance.Status.Performance = &clusterspiv1alpha1.Performance{
		IopsLimit: iopsLimit,
	}
	return nil
}

// populateEncryptionCapabilities analyzes the storage policy for all encryption
// capabilities (vSAN and VM) and populates the encryption status in the
// ClusterStoragePolicyInfo. Returns an error if the VM encryption check fails.
func populateEncryptionCapabilities(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, profileID string,
	vc *cnsvsphere.VirtualCenter,
	policyContent []cnsvsphere.SpbmPolicyContent) error {
	log := logger.GetLogger(ctx)

	encryptionStatus := &clusterspiv1alpha1.Encryption{
		SupportsEncryption: false,
		EncryptionTypes:    []clusterspiv1alpha1.EncryptionType{},
	}

	if checkVsanEncryption(policyContent) {
		encryptionStatus.SupportsEncryption = true
		encryptionStatus.EncryptionTypes = append(encryptionStatus.EncryptionTypes, "vsan-encryption")
		log.Infof("Storage policy %s supports vSAN encryption", profileID)
	} else {
		log.Infof("Storage policy %s does not support vSAN encryption", profileID)
	}

	hasVmEncrypt, err := checkVmEncryption(ctx, vc.PbmRetrieveContent, policyContent)
	if err != nil {
		instance.Status.Encryption = encryptionStatus
		return fmt.Errorf("failed to check VM encryption for profile %s: %w", profileID, err)
	}
	if hasVmEncrypt {
		encryptionStatus.SupportsEncryption = true
		encryptionStatus.EncryptionTypes = append(encryptionStatus.EncryptionTypes, "vm-encryption")
		log.Infof("Storage policy %s supports VM encryption", profileID)
	}

	instance.Status.Encryption = encryptionStatus
	return nil
}
