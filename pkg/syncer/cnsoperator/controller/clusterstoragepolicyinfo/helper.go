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

	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/mo"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clusterspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/clusterstoragepolicyinfo/v1alpha1"
	infraspiv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/infrastoragepolicyinfo/v1alpha1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/vsphereinfra"
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

// buildClusterSPIRef returns an InfraStoragePolicyInfoSpec ClusterStoragePolicyInfoReference
// pointing at the given ClusterStoragePolicyInfo.
func buildClusterSPIRef(scheme *runtime.Scheme,
	clusterSPI *clusterspiv1alpha1.ClusterStoragePolicyInfo) (infraspiv1alpha1.ClusterStoragePolicyInfoReference, error) {
	gvk, err := apiutil.GVKForObject(clusterSPI, scheme)
	if err != nil {
		return infraspiv1alpha1.ClusterStoragePolicyInfoReference{}, err
	}
	return infraspiv1alpha1.ClusterStoragePolicyInfoReference{
		Name:     clusterSPI.Name,
		Kind:     gvk.Kind,
		APIGroup: gvk.GroupVersion().String(),
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
// Returns (nil, nil) if no StorageClass references the policy yet — this is an expected state for
// policies whose ClusterStoragePolicyInfo/InfraStoragePolicyInfo CRs were created ahead of any
// StorageClass (see full sync). Returns an error only if multiple StorageClasses reference the
// same policy, which is a genuine misconfiguration.
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
		log.Debugf("No StorageClass found referencing storage policy ID %q", profileID)
		return nil, nil
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

// PBM capability that carries a storage policy's zone-availability topology, independent of any
// StorageClass. vSphere encodes this directly on the policy via the consumption-domain namespace;
// this mirrors the supervisor (wcpsvc) PolicyTopology helper so we can derive the topology type
// when no StorageClass references the policy yet.
const (
	pbmTopologyNamespace    = "com.vmware.storage.consumptiondomain"
	pbmTopologyCapabilityID = "StorageTopology"
	pbmTopologyPropertyID   = "StorageTopologyType"

	// PBM StorageTopologyType values. Both Zonal and CrossZonal are zone-aware and map to the
	// InfraStoragePolicyInfo "zonal" TopologyType; anything else (including HostLocal or an absent
	// capability) maps to the empty "no topology" value.
	pbmTopologyValueZonal      = "Zonal"
	pbmTopologyValueCrossZonal = "CrossZonal"
)

// getStorageTopologyTypeFromPolicy derives the topology type ("zonal" or "" for no topology)
// directly from the storage policy's PBM content, without requiring a StorageClass. It looks for
// the consumption-domain StorageTopology capability on any sub-profile and returns "zonal" when the
// policy declares Zonal or CrossZonal availability. This is the vCenter/PBM-native source of truth
// and is used when no StorageClass references the policy (e.g. CRs pre-created by full sync).
func getStorageTopologyTypeFromPolicy(ctx context.Context, policyContent []cnsvsphere.SpbmPolicyContent) string {
	log := logger.GetLogger(ctx)

	for _, content := range policyContent {
		for _, subProfile := range content.Profiles {
			for _, rule := range subProfile.Rules {
				if rule.Ns != pbmTopologyNamespace || rule.CapID != pbmTopologyCapabilityID ||
					rule.PropID != pbmTopologyPropertyID {
					continue
				}
				log.Debugf("Storage policy %s declares PBM StorageTopologyType %q", content.ID, rule.Value)
				// CrossZonal is not currently supported as a distinct topology; it is treated the
				// same as Zonal until cross-zonal provisioning is implemented.
				if strings.EqualFold(rule.Value, pbmTopologyValueZonal) ||
					strings.EqualFold(rule.Value, pbmTopologyValueCrossZonal) {
					return "zonal"
				}
				// Any other declared value (e.g. HostLocal) is not zone-aware.
				return ""
			}
		}
	}

	log.Debugf("No PBM StorageTopology capability found in policy content; defaulting to no topology")
	return ""
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

// populateVolumeCapabilities computes volume capabilities for the given storage policy and
// writes them into infraSPI.Status.VolumeCapabilities.
//
// SupportsVolumeModeFilesystem is always true.
//
// SupportsVolumeModeBlock is always true except when the policy is a marker policy
// (k8scompliantname is "vsan-file-service-policy").
// For marker policies, SupportsHighPerformanceLinkedClone and SupportsLinkedClone are also false.
//
// SupportsLinkedClone is true only if every zone with compatible datastores has at least one
// mounting host running ESXi 9.1 or above.
//
// SupportsHighPerformanceLinkedClone is true only if SupportsLinkedClone is true AND every zone
// has at least one ESXi 9.1+ host in a cluster with vSAN-ESA enabled.
//
// This function accepts an optional cache from topology calculation to avoid redundant vCenter calls.
func populateVolumeCapabilities(ctx context.Context,
	infraSPI *infraspiv1alpha1.InfraStoragePolicyInfo,
	vc *cnsvsphere.VirtualCenter, profileID string,
	zoneCompatibleDS map[string][]*cnsvsphere.DatastoreInfo) error {
	log := logger.GetLogger(ctx)

	caps := map[infraspiv1alpha1.VolumeCapability]bool{
		infraspiv1alpha1.SupportsVolumeModeFilesystem: true,
	}

	// Check if this is a marker policy
	// A marker policy is one where k8scompliantname is "vsan-file-service-policy"
	k8sCompliantName := infraSPI.Name
	isMarkerPolicy := k8sCompliantName == common.StorageClassVsanFileServicePolicy

	// SupportsVolumeModeBlock is always true except when policy is marker policy
	caps[infraspiv1alpha1.SupportsVolumeModeBlock] = !isMarkerPolicy
	log.Infof("Storage policy %s SupportsVolumeModeBlock=%v (isMarkerPolicy=%v)",
		profileID, !isMarkerPolicy, isMarkerPolicy)

	// For marker policies, linked clone capabilities are always false
	if isMarkerPolicy {
		caps[infraspiv1alpha1.SupportsHighPerformanceLinkedClone] = false
		caps[infraspiv1alpha1.SupportsLinkedClone] = false
		log.Infof("Storage policy %s is a marker policy - SupportsHighPerformanceLinkedClone=false, "+
			"SupportsLinkedClone=false", profileID)

		infraSPI.Status.VolumeCapabilities = caps
		return nil
	}

	lc, esxi91HostsPerZone, err := checkLinkedClone(ctx, vc, profileID, zoneCompatibleDS)
	if err != nil {
		log.Errorf("Failed to check SupportsLinkedClone for policy %s: %v", profileID, err)
		caps[infraspiv1alpha1.SupportsHighPerformanceLinkedClone] = false
		caps[infraspiv1alpha1.SupportsLinkedClone] = false
		infraSPI.Status.VolumeCapabilities = caps
		return err
	}

	caps[infraspiv1alpha1.SupportsLinkedClone] = lc

	// If LinkedClone is not supported, then HighPerformanceLinkedClone cannot be supported either.
	var hplc bool
	if !lc {
		hplc = false
		log.Infof("Storage policy %s does not support LinkedClone or HighPerformanceLinkedClone", profileID)
	} else {
		// HPLC is supported only when every zone has at least one ESXi 9.1+ host in a vSAN-ESA cluster.
		var err error
		hplc, err = checkHighPerformanceLinkedClone(ctx, vc, esxi91HostsPerZone)
		if err != nil {
			log.Errorf("Failed to check SupportsHighPerformanceLinkedClone for policy %s: %v", profileID, err)
			caps[infraspiv1alpha1.SupportsHighPerformanceLinkedClone] = false
			infraSPI.Status.VolumeCapabilities = caps
			return err
		}
	}

	caps[infraspiv1alpha1.SupportsHighPerformanceLinkedClone] = hplc
	log.Infof("Storage policy %s SupportsLinkedClone=%v, SupportsHighPerformanceLinkedClone=%v",
		profileID, lc, hplc)

	infraSPI.Status.VolumeCapabilities = caps
	return nil
}

// checkLinkedClone returns whether LinkedClone is supported across ALL zones and the per-zone
// ESXi 9.1+ hosts. LC is true only when every zone that has compatible datastores also has at
// least one ESXi 9.1+ host mounting those datastores.
func checkLinkedClone(ctx context.Context,
	vc *cnsvsphere.VirtualCenter, profileID string,
	zoneCompatibleDS map[string][]*cnsvsphere.DatastoreInfo,
) (bool, map[string]map[string]vimtypes.ManagedObjectReference, error) {
	log := logger.GetLogger(ctx)

	if vc == nil || vc.Client == nil {
		return false, nil, fmt.Errorf("virtual center client is not available")
	}

	if len(zoneCompatibleDS) == 0 {
		log.Infof("Storage policy %s has no zones with compatible datastores; SupportsLinkedClone=false", profileID)
		return false, make(map[string]map[string]vimtypes.ManagedObjectReference), nil
	}
	pc := property.DefaultCollector(vc.Client.Client)
	// esxi91HostsPerZone holds, per zone, the ESXi 9.1+ host refs found via that zone's datastores.
	esxi91HostsPerZone := make(map[string]map[string]vimtypes.ManagedObjectReference)
	// checkedHosts records whether each evaluated host is ESXi 9.1+ (true) or not (false).
	// All evaluated hosts are stored so that a host mounting multiple datastores is only
	// queried from vCenter once.
	checkedHosts := make(map[string]bool)
	// datastoreESXi91HostsCache caches the ESXi 9.1+ hosts for a given datastore so that when
	// the same datastore appears in multiple zones we reuse the result.
	datastoreESXi91HostsCache := make(map[string][]vimtypes.ManagedObjectReference)

	// relevantZones counts zones that have at least one compatible datastore for the policy.
	// Zones with no compatible datastores are excluded: the policy cannot be provisioned there,
	// so they do not contribute to the LC/HPLC determination.
	relevantZones := 0

	// Check each zone for ESXi 9.1+ hosts.
	for zone, compatibleDatastores := range zoneCompatibleDS {
		if len(compatibleDatastores) == 0 {
			continue
		}
		relevantZones++

		esxi91HostsPerZone[zone] = make(map[string]vimtypes.ManagedObjectReference)

		for _, ds := range compatibleDatastores {
			dsHosts, err := getOrFetchESXi91HostsForDS(ctx, pc, ds, checkedHosts, datastoreESXi91HostsCache)
			if err != nil {
				return false, nil, err
			}
			for _, hostRef := range dsHosts {
				esxi91HostsPerZone[zone][hostRef.Value] = hostRef
				log.Debugf("Attributed ESXi 9.1+ host %s to zone %s via datastore %s",
					hostRef.Value, zone, ds.Reference().Value)
			}
		}
	}

	if relevantZones == 0 {
		log.Infof("Storage policy %s has no zones with compatible datastores; SupportsLinkedClone=false", profileID)
		return false, make(map[string]map[string]vimtypes.ManagedObjectReference), nil
	}

	// LC is supported only when every relevant zone has at least one ESXi 9.1+ host.
	zonesWithLC := 0
	for _, hosts := range esxi91HostsPerZone {
		if len(hosts) > 0 {
			zonesWithLC++
		}
	}
	linkedCloneSupported := zonesWithLC == relevantZones
	if linkedCloneSupported {
		log.Infof("Storage policy %s supports LinkedClone: all %d zones have ESXi 9.1+ hosts", profileID, relevantZones)
	} else {
		log.Infof("Storage policy %s does not support LinkedClone: only %d/%d zones have ESXi 9.1+ hosts",
			profileID, zonesWithLC, relevantZones)
	}

	return linkedCloneSupported, esxi91HostsPerZone, nil
}

// getOrFetchESXi91HostsForDS returns the ESXi 9.1+ hosts for a datastore, using cache when
// available. On a cache miss the hosts are fetched and stored. On error the result is not cached
// so the next zone can retry.
func getOrFetchESXi91HostsForDS(ctx context.Context, pc *property.Collector,
	ds *cnsvsphere.DatastoreInfo, checkedHosts map[string]bool,
	dsHostsCache map[string][]vimtypes.ManagedObjectReference,
) ([]vimtypes.ManagedObjectReference, error) {
	datastoreValue := ds.Reference().Value
	if _, fetched := dsHostsCache[datastoreValue]; fetched {
		return dsHostsCache[datastoreValue], nil
	}
	hosts, err := fetchESXi91HostsForDatastore(ctx, pc, ds, checkedHosts)
	if err != nil {
		return nil, fmt.Errorf("fetch ESXi 9.1+ hosts for datastore %s: %w", datastoreValue, err)
	}
	dsHostsCache[datastoreValue] = hosts
	return hosts, nil
}

// fetchESXi91HostsForDatastore returns all ESXi 9.1+ hosts that mount the given datastore and
// belong to a ClusterComputeResource. checkedHosts is a shared cache (hostValue → bool) storing
// all evaluated hosts — true if ESXi 9.1+, false otherwise — to avoid redundant vCenter calls
// for hosts shared across datastores.
func fetchESXi91HostsForDatastore(ctx context.Context, pc *property.Collector,
	ds *cnsvsphere.DatastoreInfo, checkedHosts map[string]bool,
) ([]vimtypes.ManagedObjectReference, error) {
	datastoreValue := ds.Reference().Value

	var dsMO mo.Datastore
	if err := pc.RetrieveOne(ctx, ds.Reference(), []string{"host"}, &dsMO); err != nil {
		return nil, fmt.Errorf("retrieve host mounts for datastore %s: %w", datastoreValue, err)
	}

	hostRefs := make([]vimtypes.ManagedObjectReference, 0, len(dsMO.Host))
	for _, mount := range dsMO.Host {
		hostRefs = append(hostRefs, mount.Key)
	}
	if len(hostRefs) == 0 {
		return nil, nil
	}

	var esxi91Hosts []vimtypes.ManagedObjectReference
	for _, ref := range hostRefs {
		if isESXi91, checked := checkedHosts[ref.Value]; checked {
			if isESXi91 {
				esxi91Hosts = append(esxi91Hosts, ref)
			}
			continue
		}
		var hostMO mo.HostSystem
		if err := pc.RetrieveOne(ctx, ref, []string{"parent", "config.product"}, &hostMO); err != nil {
			return nil, fmt.Errorf("retrieve properties for host %s: %w", ref.Value, err)
		}
		if hostMO.Parent == nil || hostMO.Parent.Type != "ClusterComputeResource" || hostMO.Config == nil {
			checkedHosts[ref.Value] = false
			continue
		}
		is91, err := cnsvsphere.IsvSphereVersion91orAbove(ctx, hostMO.Config.Product)
		if err != nil {
			return nil, fmt.Errorf("parse ESXi version for host %s: %w", ref.Value, err)
		}
		checkedHosts[ref.Value] = is91
		if is91 {
			esxi91Hosts = append(esxi91Hosts, ref)
		}
	}
	return esxi91Hosts, nil
}

// checkHighPerformanceLinkedClone returns true only when every zone in esxi91HostsPerZone has at
// least one ESXi 9.1+ host in a vSAN-ESA enabled cluster. A shared checkedClusters cache avoids
// duplicate vCenter calls for clusters that span multiple zones.
func checkHighPerformanceLinkedClone(ctx context.Context,
	vc *cnsvsphere.VirtualCenter,
	esxi91HostsPerZone map[string]map[string]vimtypes.ManagedObjectReference,
) (bool, error) {
	log := logger.GetLogger(ctx)

	if len(esxi91HostsPerZone) == 0 {
		return false, nil
	}

	// Any zone with no ESXi 9.1+ hosts can never satisfy the ESA requirement.
	for zone, hosts := range esxi91HostsPerZone {
		if len(hosts) == 0 {
			log.Infof("Zone %s: no ESXi 9.1+ hosts; SupportsHighPerformanceLinkedClone=false", zone)
			return false, nil
		}
	}

	if vc == nil || vc.Client == nil {
		return false, fmt.Errorf("virtual center client is not available")
	}
	pc := property.DefaultCollector(vc.Client.Client)
	// checkedClusters caches vSAN-ESA state per cluster. A key present in the map (even false)
	// means the cluster was already fetched; absence means not yet fetched (retry allowed).
	checkedClusters := make(map[string]bool)
	// hostClusterCache caches the parent cluster ref for each host so that hosts appearing in
	// multiple zones are only fetched from vCenter once.
	hostClusterCache := make(map[string]vimtypes.ManagedObjectReference)

	for zone, esxi91Hosts := range esxi91HostsPerZone {
		hplc, err := zoneHasHPLC(ctx, pc, zone, esxi91Hosts, checkedClusters, hostClusterCache)
		if err != nil {
			return false, err
		}
		if !hplc {
			log.Infof("Zone %s: no ESXi 9.1+ host found in a vSAN-ESA enabled cluster; "+
				"SupportsHighPerformanceLinkedClone=false", zone)
			return false, nil
		}
	}

	return true, nil
}

// zoneHasHPLC returns true when at least one ESXi 9.1+ host in the zone belongs to a
// vSAN-ESA enabled cluster. checkedClusters and hostClusterCache are shared caches across zones.
func zoneHasHPLC(ctx context.Context, pc *property.Collector, zone string,
	esxi91Hosts map[string]vimtypes.ManagedObjectReference,
	checkedClusters map[string]bool,
	hostClusterCache map[string]vimtypes.ManagedObjectReference,
) (bool, error) {
	log := logger.GetLogger(ctx)
	for _, hostRef := range esxi91Hosts {
		clusterRef, cached := hostClusterCache[hostRef.Value]
		if !cached {
			var hostMO mo.HostSystem
			if err := pc.RetrieveOne(ctx, hostRef, []string{"parent"}, &hostMO); err != nil {
				return false, fmt.Errorf("retrieve parent cluster for host %s: %w", hostRef.Value, err)
			}
			if hostMO.Parent == nil {
				log.Warnf("Host %s has no parent cluster", hostRef.Value)
				continue
			}
			clusterRef = *hostMO.Parent
			hostClusterCache[hostRef.Value] = clusterRef
		}
		esa, err := isClusterESAEnabled(ctx, pc, clusterRef, checkedClusters)
		if err != nil {
			return false, err
		}
		if esa {
			log.Infof("Zone %s: host %s in cluster %s has vSAN-ESA enabled",
				zone, hostRef.Value, clusterRef.Value)
			return true, nil
		}
	}
	return false, nil
}

// isClusterESAEnabled returns whether the given cluster has vSAN-ESA enabled, using
// checkedClusters as a cache. On a retrieval error the result is not cached so the next
// call can retry.
func isClusterESAEnabled(ctx context.Context, pc *property.Collector,
	clusterRef vimtypes.ManagedObjectReference, checkedClusters map[string]bool,
) (bool, error) {
	clusterValue := clusterRef.Value
	if _, fetched := checkedClusters[clusterValue]; fetched {
		return checkedClusters[clusterValue], nil
	}
	var clusterMO mo.ClusterComputeResource
	if err := pc.RetrieveOne(ctx, clusterRef, []string{"configurationEx"}, &clusterMO); err != nil {
		return false, fmt.Errorf("retrieve configurationEx for cluster %s: %w", clusterValue, err)
	}
	cfgEx, ok := clusterMO.ConfigurationEx.(*vimtypes.ClusterConfigInfoEx)
	esa := ok && cfgEx.VsanConfigInfo != nil &&
		cfgEx.VsanConfigInfo.VsanEsaEnabled != nil && *cfgEx.VsanConfigInfo.VsanEsaEnabled
	checkedClusters[clusterValue] = esa
	vsphereinfra.GetCache().SetClusterESAEnabled(clusterValue, esa)
	return esa, nil
}
