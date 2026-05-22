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
	"strings"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	commoncotypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/types"
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

	hasVsanEncryption := checkVsanEncryption(policyContent)
	if hasVsanEncryption {
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

	if !hasVmEncrypt && !hasVsanEncryption {
		encryptionStatus.SupportsEncryption = false
		log.Infof("Storage policy %s does not support any encryption", profileID)
	}

	instance.Status.Encryption = encryptionStatus
	return nil
}

// getStorageClassForPolicy returns the StorageClass that references the given storage policy ID.
// Returns error if no StorageClass is found or if multiple StorageClasses reference the same policy.
func getStorageClassForPolicy(ctx context.Context, k8sClient client.Client,
	profileID string) (*storagev1.StorageClass, error) {
	log := logger.GetLogger(ctx)

	// List all StorageClasses
	scList := &storagev1.StorageClassList{}
	err := k8sClient.List(ctx, scList)
	if err != nil {
		return nil, fmt.Errorf("failed to list StorageClasses: %w", err)
	}

	var matchingSCs []*storagev1.StorageClass
	for i := range scList.Items {
		sc := &scList.Items[i]

		// Skip WFFC StorageClasses
		if storageClassIsWaitForFirstConsumer(sc) {
			log.Debugf("Skipping StorageClass %q with WaitForFirstConsumer volume binding mode", sc.Name)
			continue
		}

		// Check if this StorageClass references our storage policy by policy ID
		if sc.Parameters != nil {
			for paramKey, paramValue := range sc.Parameters {
				// Convert parameter key to lowercase for case-insensitive comparison
				if strings.ToLower(paramKey) == "storagepolicyid" && paramValue == profileID {
					matchingSCs = append(matchingSCs, sc)
					break // Found match, no need to check other parameters
				}
			}
		}
	}

	// Validate exactly one StorageClass found
	switch len(matchingSCs) {
	case 0:
		return nil, fmt.Errorf("no StorageClass found referencing storage policy ID %q", profileID)
	case 1:
		log.Infof("Found StorageClass %q referencing storage policy ID %q", matchingSCs[0].Name, profileID)
		return matchingSCs[0], nil
	default:
		var scNames []string
		for _, sc := range matchingSCs {
			scNames = append(scNames, sc.Name)
		}
		return nil, fmt.Errorf("multiple StorageClasses (%v) found referencing storage policy ID %q, expected exactly one",
			scNames, profileID)
	}
}

// isZonalTopologyPolicy checks if the StorageClass has zonal topology configured
func getStorageTopologyType(ctx context.Context, sc *storagev1.StorageClass) (string, error) {
	log := logger.GetLogger(ctx)

	if sc == nil {
		return "", fmt.Errorf("StorageClass is nil")
	}

	if sc.Parameters != nil {
		// Check for StorageTopologyType parameter with case-insensitive key comparison
		for paramKey, paramValue := range sc.Parameters {
			if strings.EqualFold(paramKey, common.AttributeStorageTopologyType) {
				log.Debugf("StorageClass %q has topology type: %q (key: %q)", sc.Name, paramValue, paramKey)

				// Validate that the parameter value is one of the allowed values
				normalizedValue := strings.ToLower(paramValue)
				if paramValue != "" && normalizedValue != "zonal" {
					return "", fmt.Errorf(
						"invalid StorageTopologyType value %q in StorageClass %q, must be an empty string or \"zonal\"",
						paramValue, sc.Name)
				}

				// Return the normalized value (empty string stays empty, others become lowercase)
				return normalizedValue, nil
			}
		}
	}

	// StorageTopologyType parameter not found, return empty string (no topology)
	log.Debugf("StorageTopologyType parameter not found in StorageClass %q, defaulting to no topology", sc.Name)
	return "", nil
}

// getAccessibleZonesForPolicy determines which zones can access datastores compatible with the given storage policy
func getAccessibleZonesForPolicy(ctx context.Context, topologyMgr commoncotypes.ControllerTopologyService,
	vc *cnsvsphere.VirtualCenter, profileID string) ([]string, error) {
	log := logger.GetLogger(ctx)

	// If topology manager is not available, fall back to basic implementation
	if topologyMgr == nil {
		log.Warnf("Topology manager not available, cannot determine accessible zones for policy")
		return []string{}, nil
	}

	// Get all zones from the topology service
	azClustersMap := topologyMgr.GetAZClustersMap(ctx)
	if len(azClustersMap) == 0 {
		log.Warnf("No zones found in topology service")
		return []string{}, nil
	}

	// Get all datastores compatible with the policy directly (single efficient PBM call)
	compatibleHubs, err := vc.PbmQueryMatchingHub(ctx, profileID)
	if err != nil {
		return nil, fmt.Errorf("failed to query compatible datastores for policy: %w", err)
	}

	if len(compatibleHubs) == 0 {
		log.Warnf("No compatible datastores found for storage policy %s", profileID)
		return []string{}, nil
	}

	// Build map of compatible datastore IDs for quick lookup
	compatibleDSIDs := make(map[string]struct{})
	for _, hub := range compatibleHubs {
		compatibleDSIDs[hub.HubId] = struct{}{}
	}

	log.Infof("Found %d compatible datastores for policy %s", len(compatibleHubs), profileID)

	// Cache for cluster datastore lookups to avoid redundant calls
	clusterToDatastoresCache := make(map[string][]*cnsvsphere.DatastoreInfo)

	accessibleZones := make(map[string]struct{})

	// For each zone, check intersection with compatible datastores
	for zone, clusters := range azClustersMap {
		log.Debugf("Checking zone %s with clusters %v", zone, clusters)

		zoneHasCompatibleDS := false

		// Get datastores for this zone's clusters and check intersection
		for _, clusterMoref := range clusters {
			var clusterDSes []*cnsvsphere.DatastoreInfo
			var err error

			// Check cache first
			if cachedDSes, exists := clusterToDatastoresCache[clusterMoref]; exists {
				clusterDSes = cachedDSes
				log.Debugf("Using cached datastores for cluster %s", clusterMoref)
			} else {
				// Cache miss - fetch from vCenter and cache the result
				clusterDSes, _, err = cnsvsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterMoref, false)
				if err != nil {
					log.Warnf("Failed to get datastores for cluster %s in zone %s: %v", clusterMoref, zone, err)
					continue
				}
				clusterToDatastoresCache[clusterMoref] = clusterDSes
				log.Debugf("Cached %d datastores for cluster %s", len(clusterDSes), clusterMoref)
			}

			// Check if any datastores in this cluster are in the compatible list
			for _, ds := range clusterDSes {
				if _, isCompatible := compatibleDSIDs[ds.Reference().Value]; isCompatible {
					zoneHasCompatibleDS = true
					break
				}
			}

			if zoneHasCompatibleDS {
				break // Found compatible datastore in this zone, no need to check more clusters
			}
		}

		if zoneHasCompatibleDS {
			accessibleZones[zone] = struct{}{}
			log.Debugf("Zone %s has compatible datastores for policy %s", zone, profileID)
		}
	}

	// Convert map to slice
	var zones []string
	for zone := range accessibleZones {
		zones = append(zones, zone)
	}

	if len(zones) == 0 {
		log.Warnf("No accessible zones found for storage policy %s", profileID)
	} else {
		log.Infof("Storage policy %s is accessible from zones: %v", profileID, zones)
	}

	return zones, nil
}

// findStoragePolicyProfile finds a storage policy profile by K8s compliant name.
// Returns (profile, policyDeleted, error) where:
// - profile is the found profile (nil if policy deleted or error)
// - policyDeleted is true if policy was deleted from vCenter (expected scenario)
// - error is non-nil for unexpected errors (like vCenter connection issues)
func findStoragePolicyProfile(ctx context.Context,
	instance *clusterspiv1alpha1.ClusterStoragePolicyInfo, vc *cnsvsphere.VirtualCenter) (
	*cnsvsphere.ProfileDetail, bool, error) {
	log := logger.GetLogger(ctx)

	k8sCompliantName := instance.Name
	log.Infof("Looking up storage policy for K8s compliant name %q", k8sCompliantName)

	profile, faultType, err := vc.FindProfileByK8sCompliantName(ctx, k8sCompliantName)
	if err != nil {
		if faultType == fault.CSINotFoundFault {
			// Profile not found - this is expected when policy is deleted
			log.Warnf("Storage policy with K8s compliant name %q not found in vCenter: %v", k8sCompliantName, err)
			instance.Status.StoragePolicyDeleted = true
			return nil, true, nil // policyDeleted=true, no error
		} else {
			// Other errors (like internal errors) should be returned as failures
			log.Errorf("Failed to query storage policy with K8s compliant name %q (fault: %s): %v",
				k8sCompliantName, faultType, err)
			return nil, false, fmt.Errorf("failed to query storage policy: %w", err)
		}
	}

	// Profile found - policy exists
	log.Infof("Storage policy found with K8s compliant name %q: ID=%s, Name=%s",
		k8sCompliantName, profile.ID, profile.Name)
	instance.Status.StoragePolicyDeleted = false

	return profile, false, nil // profile found, not deleted, no error
}
