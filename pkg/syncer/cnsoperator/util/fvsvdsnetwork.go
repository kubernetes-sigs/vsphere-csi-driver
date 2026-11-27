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

package util

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// This file resolves the vSphere Distributed Switch (VDS) identity of a supervisor namespace's
// network so that FileVolumeService (FVS) instance namespaces can be matched to a consumer
// namespace on VDS-backed supervisors, where there is no NSX VPC path to compare.
//
// Resolution chain (all netoperator.vmware.com/v1alpha1):
//
//	Namespace
//	  └─ Network (namespaced, spec.type=vsphere-distributed)
//	       └─ spec.providerRef ──► VSphereDistributedNetwork (cluster-scoped)
//	                                 ├─ spec.portGroupID                       → portgroup identity
//	                                 └─ annotations["vmware-system-dvs-uuid"]  → VDS identity

var (
	// NetworkGVR is the namespaced net-operator Network resource.
	NetworkGVR = schema.GroupVersionResource{
		Group:    "netoperator.vmware.com",
		Version:  "v1alpha1",
		Resource: "networks",
	}
	// VSphereDistributedNetworkGVR is the cluster-scoped net-operator VSphereDistributedNetwork resource.
	VSphereDistributedNetworkGVR = schema.GroupVersionResource{
		Group:    "netoperator.vmware.com",
		Version:  "v1alpha1",
		Resource: "vspheredistributednetworks",
	}
)

const (
	// NetworkTypeVSphereDistributed is the Network spec.type value for VDS-backed networks.
	NetworkTypeVSphereDistributed = "vsphere-distributed"
	// kindVSphereDistributedNetwork is the Network spec.providerRef.kind for VDS-backed networks.
	kindVSphereDistributedNetwork = "VSphereDistributedNetwork"
	// LabelDefaultNetwork marks the namespace's default Network; FVS Protocol Pods attach to it.
	LabelDefaultNetwork = "vmware.com/is-default-network"
	// AnnotationDVSUUID on a VSphereDistributedNetwork holds the UUID of the backing VDS.
	AnnotationDVSUUID = "vmware-system-dvs-uuid"
)

// VDSNetworkInfo is the resolved VDS identity of one namespace Network.
type VDSNetworkInfo struct {
	// NetworkName is the namespaced Network name.
	NetworkName string
	// VDNName is the cluster-scoped VSphereDistributedNetwork name referenced by the Network.
	VDNName string
	// PortGroupID is the distributed portgroup moref (e.g. "dvportgroup-117").
	PortGroupID string
	// DVSUUID is the normalized (trimmed, lower-cased) UUID of the VDS backing the portgroup.
	DVSUUID string
}

// String renders the network for logs and error messages.
func (n VDSNetworkInfo) String() string {
	return fmt.Sprintf("%s(portgroup=%s, vds=%q)", n.NetworkName, n.PortGroupID, n.DVSUUID)
}

// FVSNetworkMatchTier ranks how well an FVS instance namespace's network matches a consumer
// namespace's networks on VDS. Lower non-zero values are preferred.
type FVSNetworkMatchTier int

const (
	// FVSNetworkNoMatch means the instance namespace shares neither portgroup nor VDS with the consumer.
	FVSNetworkNoMatch FVSNetworkMatchTier = iota
	// FVSNetworkTierPortgroup (Tier 1) means the instance namespace is on the same portgroup (L2 adjacent).
	FVSNetworkTierPortgroup
	// FVSNetworkTierVDS (Tier 2) means the instance namespace is on a different portgroup of the same VDS.
	FVSNetworkTierVDS
)

// String renders the tier for logs.
func (t FVSNetworkMatchTier) String() string {
	switch t {
	case FVSNetworkTierPortgroup:
		return "Tier1(same-portgroup)"
	case FVSNetworkTierVDS:
		return "Tier2(same-VDS)"
	default:
		return "NoMatch"
	}
}

// normalizeDVSUUID trims and lower-cases a VDS UUID so values that differ only in case or
// surrounding whitespace compare equal.
func normalizeDVSUUID(u string) string {
	return strings.ToLower(strings.TrimSpace(u))
}

// GetVDSNetworksForNamespace returns the VDS identity of the vsphere-distributed Networks in the
// given namespace.
//
// When defaultOnly is true only the Network labeled vmware.com/is-default-network=true is
// returned; if no VDS Network carries that label but the namespace has exactly one VDS Network,
// that one is returned (it is the de-facto default).
//
// Networks whose VSphereDistributedNetwork is not found or has no spec.portGroupID are skipped.
// Any other API error is returned so the caller can retry.
func GetVDSNetworksForNamespace(ctx context.Context, dc dynamic.Interface, namespace string,
	defaultOnly bool) ([]VDSNetworkInfo, error) {
	log := logger.GetLogger(ctx)
	if dc == nil {
		return nil, fmt.Errorf("dynamic client is required to resolve VDS networks for namespace %q", namespace)
	}

	list, err := dc.Resource(NetworkGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list Networks in namespace %q: %w", namespace, err)
	}

	type vdsNetworkRef struct {
		networkName string
		vdnName     string
		isDefault   bool
	}
	var refs []vdsNetworkRef
	for i := range list.Items {
		obj := &list.Items[i]
		netType, _, _ := unstructured.NestedString(obj.Object, "spec", "type")
		kind, _, _ := unstructured.NestedString(obj.Object, "spec", "providerRef", "kind")
		vdnName, _, _ := unstructured.NestedString(obj.Object, "spec", "providerRef", "name")
		if netType != NetworkTypeVSphereDistributed || kind != kindVSphereDistributedNetwork || vdnName == "" {
			log.Debugf("GetVDSNetworksForNamespace: skipping Network %s/%s (type=%q, providerRef.kind=%q, "+
				"providerRef.name=%q)", namespace, obj.GetName(), netType, kind, vdnName)
			continue
		}
		refs = append(refs, vdsNetworkRef{
			networkName: obj.GetName(),
			vdnName:     vdnName,
			isDefault:   strings.EqualFold(obj.GetLabels()[LabelDefaultNetwork], "true"),
		})
	}

	if defaultOnly {
		var defaults []vdsNetworkRef
		for _, r := range refs {
			if r.isDefault {
				defaults = append(defaults, r)
			}
		}
		switch {
		case len(defaults) > 0:
			refs = defaults
		case len(refs) == 1:
			log.Debugf("GetVDSNetworksForNamespace: namespace %q has no Network labeled %s=true; "+
				"using its only VDS Network %q", namespace, LabelDefaultNetwork, refs[0].networkName)
		default:
			log.Debugf("GetVDSNetworksForNamespace: namespace %q has %d VDS Networks and none labeled %s=true",
				namespace, len(refs), LabelDefaultNetwork)
			refs = nil
		}
	}

	vdnCache := make(map[string]*unstructured.Unstructured)
	out := make([]VDSNetworkInfo, 0, len(refs))
	for _, r := range refs {
		vdn, cached := vdnCache[r.vdnName]
		if !cached {
			vdn, err = dc.Resource(VSphereDistributedNetworkGVR).Get(ctx, r.vdnName, metav1.GetOptions{})
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("get VSphereDistributedNetwork %q for Network %s/%s: %w",
						r.vdnName, namespace, r.networkName, err)
				}
				vdn = nil
			}
			vdnCache[r.vdnName] = vdn
		}
		if vdn == nil {
			log.Debugf("GetVDSNetworksForNamespace: skipping Network %s/%s: VSphereDistributedNetwork %q not found",
				namespace, r.networkName, r.vdnName)
			continue
		}
		portGroupID, _, _ := unstructured.NestedString(vdn.Object, "spec", "portGroupID")
		if portGroupID == "" {
			log.Debugf("GetVDSNetworksForNamespace: skipping Network %s/%s: VSphereDistributedNetwork %q "+
				"has no spec.portGroupID", namespace, r.networkName, r.vdnName)
			continue
		}
		out = append(out, VDSNetworkInfo{
			NetworkName: r.networkName,
			VDNName:     r.vdnName,
			PortGroupID: portGroupID,
			DVSUUID:     normalizeDVSUUID(vdn.GetAnnotations()[AnnotationDVSUUID]),
		})
	}
	return out, nil
}

// MatchVDSNetworks returns the best tier over every (consumer, instance) network pair:
// Tier 1 when both are on the same portgroup, Tier 2 when they are on the same VDS, and
// FVSNetworkNoMatch otherwise. Empty identifiers never match.
func MatchVDSNetworks(consumer, instance []VDSNetworkInfo) FVSNetworkMatchTier {
	best := FVSNetworkNoMatch
	for _, c := range consumer {
		for _, i := range instance {
			if c.PortGroupID != "" && c.PortGroupID == i.PortGroupID {
				return FVSNetworkTierPortgroup
			}
			if c.DVSUUID != "" && c.DVSUUID == i.DVSUUID {
				best = FVSNetworkTierVDS
			}
		}
	}
	return best
}
