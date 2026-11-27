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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// testVDNObj builds a cluster-scoped VSphereDistributedNetwork. An empty dvsUUID omits the annotation.
func testVDNObj(name, portGroupID, dvsUUID string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "netoperator.vmware.com/v1alpha1",
		"kind":       "VSphereDistributedNetwork",
		"metadata":   map[string]interface{}{"name": name},
		"spec":       map[string]interface{}{},
	}}
	if portGroupID != "" {
		_ = unstructured.SetNestedField(obj.Object, portGroupID, "spec", "portGroupID")
	}
	if dvsUUID != "" {
		obj.SetAnnotations(map[string]string{AnnotationDVSUUID: dvsUUID})
	}
	return obj
}

// testNetworkObj builds a namespaced net-operator Network referencing a VSphereDistributedNetwork.
func testNetworkObj(namespace, name, netType, vdnName string, isDefault bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "netoperator.vmware.com/v1alpha1",
		"kind":       "Network",
		"metadata":   map[string]interface{}{"name": name, "namespace": namespace},
		"spec": map[string]interface{}{
			"type": netType,
			"providerRef": map[string]interface{}{
				"apiGroup": "netoperator.vmware.com",
				"kind":     kindVSphereDistributedNetwork,
				"name":     vdnName,
			},
		},
	}}
	if isDefault {
		obj.SetLabels(map[string]string{LabelDefaultNetwork: "true"})
	}
	return obj
}

func TestGetVDSNetworksForNamespace(t *testing.T) {
	ctx := context.Background()
	const dvs = "50 24 81 df 8a 5a 0a 1d-54 0b 76 18 90 6f 16 e4"

	tests := []struct {
		name        string
		objs        []runtime.Object
		defaultOnly bool
		want        []VDSNetworkInfo
	}{
		{
			name: "resolves portgroup and normalized VDS UUID",
			objs: []runtime.Object{
				testVDNObj("vdn-1", "dvportgroup-117", "  "+"50 24 81 DF 8A 5A 0A 1D-54 0B 76 18 90 6F 16 E4"+" "),
				testNetworkObj("ns", "net-1", NetworkTypeVSphereDistributed, "vdn-1", true),
			},
			want: []VDSNetworkInfo{{NetworkName: "net-1", VDNName: "vdn-1", PortGroupID: "dvportgroup-117",
				DVSUUID: dvs}},
		},
		{
			name: "all VDS networks when defaultOnly=false; non-VDS types skipped",
			objs: []runtime.Object{
				testVDNObj("vdn-1", "dvportgroup-1", dvs),
				testVDNObj("vdn-2", "dvportgroup-2", dvs),
				testNetworkObj("ns", "net-1", NetworkTypeVSphereDistributed, "vdn-1", true),
				testNetworkObj("ns", "net-2", NetworkTypeVSphereDistributed, "vdn-2", false),
				testNetworkObj("ns", "net-nsx", "nsx-t", "vdn-1", false),
			},
			want: []VDSNetworkInfo{
				{NetworkName: "net-1", VDNName: "vdn-1", PortGroupID: "dvportgroup-1", DVSUUID: dvs},
				{NetworkName: "net-2", VDNName: "vdn-2", PortGroupID: "dvportgroup-2", DVSUUID: dvs},
			},
		},
		{
			name:        "defaultOnly returns only the default network",
			defaultOnly: true,
			objs: []runtime.Object{
				testVDNObj("vdn-1", "dvportgroup-1", dvs),
				testVDNObj("vdn-2", "dvportgroup-2", dvs),
				testNetworkObj("ns", "net-1", NetworkTypeVSphereDistributed, "vdn-1", false),
				testNetworkObj("ns", "net-2", NetworkTypeVSphereDistributed, "vdn-2", true),
			},
			want: []VDSNetworkInfo{{NetworkName: "net-2", VDNName: "vdn-2", PortGroupID: "dvportgroup-2",
				DVSUUID: dvs}},
		},
		{
			name:        "defaultOnly falls back to the only VDS network when none is labeled default",
			defaultOnly: true,
			objs: []runtime.Object{
				testVDNObj("vdn-1", "dvportgroup-1", dvs),
				testNetworkObj("ns", "net-1", NetworkTypeVSphereDistributed, "vdn-1", false),
			},
			want: []VDSNetworkInfo{{NetworkName: "net-1", VDNName: "vdn-1", PortGroupID: "dvportgroup-1",
				DVSUUID: dvs}},
		},
		{
			name:        "defaultOnly with several unlabeled VDS networks returns none",
			defaultOnly: true,
			objs: []runtime.Object{
				testVDNObj("vdn-1", "dvportgroup-1", dvs),
				testVDNObj("vdn-2", "dvportgroup-2", dvs),
				testNetworkObj("ns", "net-1", NetworkTypeVSphereDistributed, "vdn-1", false),
				testNetworkObj("ns", "net-2", NetworkTypeVSphereDistributed, "vdn-2", false),
			},
			want: []VDSNetworkInfo{},
		},
		{
			name: "missing VDN and VDN without portGroupID are skipped",
			objs: []runtime.Object{
				testVDNObj("vdn-nopg", "", dvs),
				testNetworkObj("ns", "net-gone", NetworkTypeVSphereDistributed, "vdn-gone", false),
				testNetworkObj("ns", "net-nopg", NetworkTypeVSphereDistributed, "vdn-nopg", false),
			},
			want: []VDSNetworkInfo{},
		},
		{
			name: "networks in other namespaces are ignored",
			objs: []runtime.Object{
				testVDNObj("vdn-1", "dvportgroup-1", dvs),
				testNetworkObj("other-ns", "net-1", NetworkTypeVSphereDistributed, "vdn-1", true),
			},
			want: []VDSNetworkInfo{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dc := newFakeDynClient(nil, tt.objs...)
			got, err := GetVDSNetworksForNamespace(ctx, dc, "ns", tt.defaultOnly)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}

	t.Run("nil dynamic client is an error", func(t *testing.T) {
		_, err := GetVDSNetworksForNamespace(ctx, nil, "ns", false)
		require.Error(t, err)
	})
}

func TestMatchVDSNetworks(t *testing.T) {
	pg1A := VDSNetworkInfo{NetworkName: "a", PortGroupID: "dvportgroup-1", DVSUUID: "dvs-a"}
	pg2A := VDSNetworkInfo{NetworkName: "b", PortGroupID: "dvportgroup-2", DVSUUID: "dvs-a"}
	pg3C := VDSNetworkInfo{NetworkName: "c", PortGroupID: "dvportgroup-3", DVSUUID: "dvs-c"}
	pg4NoDVS := VDSNetworkInfo{NetworkName: "d", PortGroupID: "dvportgroup-4"}
	pg5NoDVS := VDSNetworkInfo{NetworkName: "e", PortGroupID: "dvportgroup-5"}

	tests := []struct {
		name     string
		consumer []VDSNetworkInfo
		instance []VDSNetworkInfo
		want     FVSNetworkMatchTier
	}{
		{"same portgroup", []VDSNetworkInfo{pg1A}, []VDSNetworkInfo{pg1A}, FVSNetworkTierPortgroup},
		{"same VDS, different portgroup", []VDSNetworkInfo{pg1A}, []VDSNetworkInfo{pg2A}, FVSNetworkTierVDS},
		{"different VDS", []VDSNetworkInfo{pg1A}, []VDSNetworkInfo{pg3C}, FVSNetworkNoMatch},
		{"empty DVS UUIDs never match", []VDSNetworkInfo{pg4NoDVS}, []VDSNetworkInfo{pg5NoDVS}, FVSNetworkNoMatch},
		{"empty consumer", nil, []VDSNetworkInfo{pg1A}, FVSNetworkNoMatch},
		{"empty instance", []VDSNetworkInfo{pg1A}, nil, FVSNetworkNoMatch},
		{"best tier wins across several consumer networks",
			[]VDSNetworkInfo{pg2A, pg3C}, []VDSNetworkInfo{pg3C}, FVSNetworkTierPortgroup},
		{"Tier 2 via secondary consumer network",
			[]VDSNetworkInfo{pg3C, pg2A}, []VDSNetworkInfo{pg1A}, FVSNetworkTierVDS},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, MatchVDSNetworks(tt.consumer, tt.instance))
		})
	}
	assert.Equal(t, "Tier1(same-portgroup)", FVSNetworkTierPortgroup.String())
	assert.Equal(t, "Tier2(same-VDS)", FVSNetworkTierVDS.String())
	assert.Equal(t, "NoMatch", FVSNetworkNoMatch.String())
}
