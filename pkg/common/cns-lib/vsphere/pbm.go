/*
Copyright 2019 The Kubernetes Authors.

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

package vsphere

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi/pbm"
	pbmmethods "github.com/vmware/govmomi/pbm/methods"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// SpbmPolicyRule is an individual policy rule.
// Not all providers use Ns, CapID, PropID in the same way,
// so one needs to look at each one individually.
// Ns + CapID + PropID together are a unique key in all cases
type SpbmPolicyRule struct {
	Ns     string `json:"ns,omitempty"`
	CapID  string `json:"capId,omitempty"`
	PropID string `json:"propId,omitempty"`
	Value  string `json:"value,omitempty"`
}

// SpbmPolicySubProfile is a combination of rules, which are ANDed to form
// a sub profile.
type SpbmPolicySubProfile struct {
	Rules []SpbmPolicyRule `json:"rules"`
}

// ProfileDetail represents a storage profile with its basic information.
type ProfileDetail struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	K8sCompliantName string `json:"k8sCompliantName,omitempty"`
	Description      string `json:"description,omitempty"`
	Category         string `json:"category,omitempty"`
}

// SpbmPolicyContent corresponds to a single VC SPBM policy.
// The various sub profilles are ORed. For vSAN there should only
// be a single sub profile.
type SpbmPolicyContent struct {
	ID       string                 `json:"id,omitempty"`
	Profiles []SpbmPolicySubProfile `json:"profiles"`
}

// ConnectPbm creates a PBM client for the virtual center.
func (vc *VirtualCenter) ConnectPbm(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	var err = vc.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to Virtual Center %q with err: %v", vc.Config.Host, err)
		return err
	}
	if vc.PbmClient == nil {
		if vc.PbmClient, err = pbm.NewClient(ctx, vc.Client.Client); err != nil {
			log.Errorf("failed to create pbm client with err: %v", err)
			return err
		}
	}
	return nil
}

// DisconnectPbm destroys the PBM client for the virtual center.
func (vc *VirtualCenter) DisconnectPbm(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	if vc.PbmClient == nil {
		log.Info("PbmClient wasn't connected, ignoring")
	} else {
		vc.PbmClient = nil
	}
	return nil
}

// GetStoragePolicyIDByName gets storage policy ID by name.
func (vc *VirtualCenter) GetStoragePolicyIDByName(ctx context.Context, storagePolicyName string) (string, error) {
	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return "", err
	}
	storagePolicyID, err := vc.PbmClient.ProfileIDByName(ctx, storagePolicyName)
	if err != nil {
		log.Errorf("failed to get StoragePolicyID from StoragePolicyName %s with err: %v", storagePolicyName, err)
		return "", err
	}
	return storagePolicyID, nil
}

// PbmCheckCompatibility performs a compatibility check for the given profileID
// with the given datastores.
func (vc *VirtualCenter) PbmCheckCompatibility(ctx context.Context,
	datastores []vimtypes.ManagedObjectReference, profileID string) (pbm.PlacementCompatibilityResult, error) {

	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return nil, err
	}

	hubs := make([]pbmtypes.PbmPlacementHub, 0)
	for _, ds := range datastores {
		hubs = append(hubs, pbmtypes.PbmPlacementHub{
			HubType: ds.Type,
			HubId:   ds.Value,
		})
	}
	req := pbmtypes.PbmCheckCompatibility{
		This:         vc.PbmClient.ServiceContent.PlacementSolver,
		HubsToSearch: hubs,
		Profile: pbmtypes.PbmProfileId{
			UniqueId: profileID,
		},
	}

	res, err := pbmmethods.PbmCheckCompatibility(ctx, vc.PbmClient, &req)
	if err != nil {
		return nil, err
	}

	return res.Returnval, nil
}

// PbmQueryMatchingHub gets all datastores that are compatible with the given storage policy.
func (vc *VirtualCenter) PbmQueryMatchingHub(ctx context.Context,
	profileID string) ([]pbmtypes.PbmPlacementHub, error) {
	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return nil, err
	}

	req := pbmtypes.PbmQueryMatchingHub{
		This: vc.PbmClient.ServiceContent.PlacementSolver,
		Profile: pbmtypes.PbmProfileId{
			UniqueId: profileID,
		},
	}

	res, err := pbmmethods.PbmQueryMatchingHub(ctx, vc.PbmClient, &req)
	if err != nil {
		log.Errorf("failed to query matching hubs for profile %s: %v", profileID, err)
		return nil, err
	}

	log.Infof("Found %d compatible datastores for policy %s", len(res.Returnval), profileID)
	return res.Returnval, nil
}

// PbmCheckRequirementsForZoneTopology performs an SPBM placement compatibility check for
// profileID using a PbmPlacementZoneTopologyRequirement scoped to clusterMoIDs.
// For each datastore that is both policy-compatible and
// mounted on at least one of the clusterMoIDs, the result's
// HubInfo.ZoneClusters field indicates which of clusterMoIDs it is mounted on.
func (vc *VirtualCenter) PbmCheckRequirementsForZoneTopology(ctx context.Context,
	profileID string, clusterMoIDs []string) (pbm.PlacementCompatibilityResult, error) {
	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return nil, err
	}

	clusters := make([]pbmtypes.PbmServerObjectRef, 0, len(clusterMoIDs))
	for _, moID := range clusterMoIDs {
		clusters = append(clusters, pbmtypes.PbmServerObjectRef{
			ObjectType: string(pbmtypes.PbmObjectTypeCluster),
			Key:        moID,
		})
	}

	requirement := &pbmtypes.PbmPlacementZoneTopologyRequirement{
		PbmPlacementCapabilityProfileRequirement: pbmtypes.PbmPlacementCapabilityProfileRequirement{
			ProfileId: pbmtypes.PbmProfileId{UniqueId: profileID},
		},
		Clusters: clusters,
	}

	res, err := vc.PbmClient.CheckRequirements(ctx, nil, nil,
		[]pbmtypes.BasePbmPlacementRequirement{requirement})
	if err != nil {
		log.Errorf("failed to check zone topology placement requirements for profile %s: %v", profileID, err)
		return nil, err
	}

	log.Infof("Found %d placement results for profile %s across %d clusters", len(res), profileID, len(clusterMoIDs))
	return res, nil
}

// PbmRetrieveContent fetches the policy content of all given policies from SPBM.
func (vc *VirtualCenter) PbmRetrieveContent(ctx context.Context, policyIds []string) ([]SpbmPolicyContent, error) {

	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return nil, err
	}
	pbmPolicyIds := make([]pbmtypes.PbmProfileId, 0)
	for _, policyID := range policyIds {
		pbmPolicyIds = append(pbmPolicyIds, pbmtypes.PbmProfileId{
			UniqueId: policyID,
		})
	}
	profiles, err := vc.PbmClient.RetrieveContent(ctx, pbmPolicyIds)

	return simplifyProfileStructs(ctx, profiles), err
}

const (
	// hostLocalStorageNamespace and hostLocalStorageCapabilityID identify the SPBM capability
	// that marks a storage policy as host-local storage. This mirrors the check used by the WCP
	// control plane; the hostlocalstorage namespace defines exactly one capability
	// (hostLocalStorage) and it carries no constraint/property instances, so it must be matched
	// directly against the capability id rather than via PbmRetrieveContent/SpbmPolicyContent,
	// which only emits a rule per PropertyInstance and would silently drop a property-less
	// capability like this one. This does NOT match vSAN Direct or vSAN locality/SNA policies.
	hostLocalStorageNamespace    = "com.vmware.storage.hostlocalstorage"
	hostLocalStorageCapabilityID = "hostLocalStorage"
)

// IsHostLocalStorageCapabilityPolicy reports whether the SPBM capability subprofile constraints
// contain the com.vmware.storage.hostlocalstorage/hostLocalStorage capability.
func IsHostLocalStorageCapabilityPolicy(subprofiles *pbmtypes.PbmCapabilitySubProfileConstraints) bool {
	if subprofiles == nil {
		return false
	}
	for _, subprofile := range subprofiles.SubProfiles {
		for _, capIns := range subprofile.Capability {
			if capIns.Id.Namespace == hostLocalStorageNamespace && capIns.Id.Id == hostLocalStorageCapabilityID {
				return true
			}
		}
	}
	return false
}

// IsHostLocalStoragePolicy fetches the SPBM profile for the given policyID and reports whether
// it carries the host-local storage capability. An empty policyID returns (false, nil).
func (vc *VirtualCenter) IsHostLocalStoragePolicy(ctx context.Context, policyID string) (bool, error) {
	log := logger.GetLogger(ctx)
	if policyID == "" {
		return false, nil
	}
	if err := vc.ConnectPbm(ctx); err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return false, err
	}
	profiles, err := vc.PbmClient.RetrieveContent(ctx, []pbmtypes.PbmProfileId{{UniqueId: policyID}})
	if err != nil {
		log.Errorf("failed to retrieve SPBM profile for policy %q: %v", policyID, err)
		return false, err
	}
	for _, baseProfile := range profiles {
		profile, ok := baseProfile.(*pbmtypes.PbmCapabilityProfile)
		if !ok {
			continue
		}
		constraints, ok := profile.Constraints.(*pbmtypes.PbmCapabilitySubProfileConstraints)
		if !ok {
			continue
		}
		if IsHostLocalStorageCapabilityPolicy(constraints) {
			return true, nil
		}
	}
	log.Infof("storage policy %q does not carry the host-local storage capability", policyID)
	return false, nil
}

// QueryAllProfileDetails queries all profiles of a specific category from vCenter SPBM.
// This uses the queryProfileDetails API to get all profiles in the specified category.
func (vc *VirtualCenter) QueryAllProfileDetails(ctx context.Context, profileCategory string,
	fetchAllFields bool) ([]ProfileDetail, error) {
	log := logger.GetLogger(ctx)
	err := vc.ConnectPbm(ctx)
	if err != nil {
		log.Errorf("Error occurred while connecting to PBM, err: %+v", err)
		return nil, err
	}

	// Call the queryProfileDetails API
	req := pbmtypes.PbmQueryProfileDetails{
		This:            vc.PbmClient.ServiceContent.ProfileManager,
		ProfileCategory: profileCategory,
		FetchAllFields:  fetchAllFields,
	}

	res, err := pbmmethods.PbmQueryProfileDetails(ctx, vc.PbmClient, &req)
	if err != nil {
		log.Errorf("failed to query profile details for category %s: %v", profileCategory, err)
		return nil, err
	}

	// Convert response to our ProfileDetail structure
	profiles := make([]ProfileDetail, 0)
	if res.Returnval != nil {
		for _, profileDetail := range res.Returnval {
			if profileDetail.Profile != nil {
				// Cast to PbmCapabilityProfile to access fields
				if capProfile, ok := profileDetail.Profile.(*pbmtypes.PbmCapabilityProfile); ok {
					profile := ProfileDetail{
						ID:               capProfile.ProfileId.UniqueId,
						Name:             capProfile.Name,
						K8sCompliantName: capProfile.K8sCompliantName,
						Description:      capProfile.Description,
						Category:         capProfile.ProfileCategory,
					}
					profiles = append(profiles, profile)
				} else {
					log.Debugf("Failed to cast profile to PbmCapabilityProfile: %T", profileDetail.Profile)
				}
			}
		}
	}

	log.Debugf("Found %d profiles for category %s", len(profiles), profileCategory)
	return profiles, nil
}

// FindProfileByK8sCompliantName searches for a storage policy by its K8s compliant name.
// This is more flexible than ProfileIDByName as it searches through all profiles.
// Returns the matching profile, fault type, and error if not found.
func (vc *VirtualCenter) FindProfileByK8sCompliantName(ctx context.Context,
	k8sCompliantName string) (*ProfileDetail, string, error) {
	log := logger.GetLogger(ctx)

	// Query all REQUIREMENT profiles (fetchAllFields=false for better performance)
	profiles, err := vc.QueryAllProfileDetails(ctx, "REQUIREMENT", false)
	if err != nil {
		log.Errorf("failed to query profile details: %v", err)
		return nil, fault.CSIInternalFault, err
	}

	// Search for profile with matching K8s compliant name
	for _, profile := range profiles {
		if profile.K8sCompliantName == k8sCompliantName {
			log.Infof("Found storage policy with K8s compliant name %s: ID=%s, Name=%s",
				k8sCompliantName, profile.ID, profile.Name)
			return &profile, "", nil
		}
	}

	return nil, fault.CSINotFoundFault, fmt.Errorf("storage policy with K8s compliant name %s not found", k8sCompliantName)
}

func simplifyProfileStructs(ctx context.Context, profiles []pbmtypes.BasePbmProfile) []SpbmPolicyContent {
	log := logger.GetLogger(ctx)
	out := make([]SpbmPolicyContent, 0)
	for _, _profile := range profiles {
		p, ok := _profile.(*pbmtypes.PbmCapabilityProfile)
		if !ok {
			log.Infof("Failed to cast PbmProfile: %T", _profile)
			continue
		}

		c, ok := p.Constraints.(*pbmtypes.PbmCapabilitySubProfileConstraints)
		if !ok {
			log.Infof("Failed to cast Pbm Constraints: %T", p.Constraints)
			continue
		}

		k8sPolicy := SpbmPolicyContent{
			Profiles: make([]SpbmPolicySubProfile, 0),
			ID:       p.ProfileId.UniqueId,
		}
		for _, s := range c.SubProfiles {
			k8sCap := SpbmPolicySubProfile{
				Rules: make([]SpbmPolicyRule, 0),
			}
			for _, cap := range s.Capability {
				for _, con := range cap.Constraint {
					for _, pi := range con.PropertyInstance {
						k8sCap.Rules = append(k8sCap.Rules, SpbmPolicyRule{
							Ns:     cap.Id.Namespace,
							CapID:  cap.Id.Id,
							PropID: pi.Id,
							Value:  fmt.Sprintf("%v", pi.Value),
						})
					}
				}
			}
			k8sPolicy.Profiles = append(k8sPolicy.Profiles, k8sCap)
		}
		out = append(out, k8sPolicy)
	}
	return out
}
