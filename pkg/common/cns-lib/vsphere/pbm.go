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
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
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

// PbmRetrieveContent fetches the policy content of all given policies from SPBM.
func (vc *VirtualCenter) PbmRetrieveContent(ctx context.Context, policyIds []string) ([]SpbmPolicyContent, error) {
	pbmPolicyIds := make([]pbmtypes.PbmProfileId, 0)
	for _, policyID := range policyIds {
		pbmPolicyIds = append(pbmPolicyIds, pbmtypes.PbmProfileId{
			UniqueId: policyID,
		})
	}
	profiles, err := vc.PbmClient.RetrieveContent(ctx, pbmPolicyIds)

	return simplifyProfileStructs(ctx, profiles), err
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
							Value:  fmt.Sprintf("%s", pi.Value),
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
