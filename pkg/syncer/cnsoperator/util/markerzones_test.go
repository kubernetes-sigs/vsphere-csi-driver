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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// vpcCRPair holds a VPCNetworkConfiguration CR name and its resolved VPC path.
type vpcCRPair struct {
	name    string
	vpcPath string
}

// newFakeDynClientWithVPCCRs builds a fake dynamic client pre-populated with one
// VPCNetworkConfiguration object per pair, each carrying status.vpcs[0].vpcPath.
func newFakeDynClientWithVPCCRs(pairs ...vpcCRPair) *dynfake.FakeDynamicClient {
	dynScheme := runtime.NewScheme()
	dynScheme.AddKnownTypeWithName(
		vpcNetworkConfigurationGVR.GroupVersion().WithKind("VPCNetworkConfiguration"),
		&unstructured.Unstructured{},
	)
	dynScheme.AddKnownTypeWithName(
		vpcNetworkConfigurationGVR.GroupVersion().WithKind("VPCNetworkConfigurationList"),
		&unstructured.UnstructuredList{},
	)

	objs := make([]runtime.Object, 0, len(pairs))
	for _, p := range pairs {
		cr := &unstructured.Unstructured{}
		cr.SetGroupVersionKind(vpcNetworkConfigurationGVR.GroupVersion().WithKind("VPCNetworkConfiguration"))
		cr.SetName(p.name)
		_ = unstructured.SetNestedSlice(cr.Object, []interface{}{
			map[string]interface{}{"vpcPath": p.vpcPath},
		}, "status", "vpcs")
		objs = append(objs, cr)
	}

	return dynfake.NewSimpleDynamicClient(dynScheme, objs...)
}

// fvsNamespace returns a Namespace labelled as an FVS instance namespace with
// the given VPC network config annotation. An empty vpcAnnotation omits the
// annotation entirely.
func fvsNamespace(name, vpcAnnotation string) *v1.Namespace {
	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{NamespaceLabelFVSInstance: "true"},
	}}
	if vpcAnnotation != "" {
		ns.Annotations = map[string]string{AnnotationVPCNetworkConfig: vpcAnnotation}
	}
	return ns
}

// zonesFnFromMap returns a zonesFn that looks up the given namespace's zones in m.
func zonesFnFromMap(m map[string][]string) func(string) map[string]struct{} {
	return func(ns string) map[string]struct{} {
		out := make(map[string]struct{})
		for _, z := range m[ns] {
			out[z] = struct{}{}
		}
		return out
	}
}

// TestVPCPathForNamespace tests the VPCPathForNamespace function.
func TestVPCPathForNamespace(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		namespace   *v1.Namespace
		vpcCRs      []vpcCRPair
		lookupNS    string
		expectedVPC string
		expectError bool
	}{
		{
			name:        "happy path",
			namespace:   fvsNamespace("ns1", "vpc-cfg-a"),
			vpcCRs:      []vpcCRPair{{name: "vpc-cfg-a", vpcPath: "/org/proj/vpc-a"}},
			lookupNS:    "ns1",
			expectedVPC: "/org/proj/vpc-a",
		},
		{
			name:        "namespace not found",
			namespace:   nil,
			lookupNS:    "missing-ns",
			expectError: true,
		},
		{
			name:        "missing VPC network config annotation",
			namespace:   fvsNamespace("ns1", ""),
			lookupNS:    "ns1",
			expectError: true,
		},
		{
			name:        "VPCNetworkConfiguration CR not found",
			namespace:   fvsNamespace("ns1", "vpc-cfg-missing"),
			lookupNS:    "ns1",
			expectError: true,
		},
		{
			name:        "CR has no vpcPath in status",
			namespace:   fvsNamespace("ns1", "vpc-cfg-a"),
			vpcCRs:      []vpcCRPair{{name: "vpc-cfg-a", vpcPath: ""}},
			lookupNS:    "ns1",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var k8sObjs []runtime.Object
			if tt.namespace != nil {
				k8sObjs = append(k8sObjs, tt.namespace)
			}
			k8sClient := k8sfake.NewClientset(k8sObjs...)
			dc := newFakeDynClientWithVPCCRs(tt.vpcCRs...)

			path, err := VPCPathForNamespace(ctx, dc, k8sClient, tt.lookupNS)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedVPC, path)
		})
	}
}

// TestGetFVSZones tests the GetFVSZones function.
func TestGetFVSZones(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		namespaces    []*v1.Namespace
		vpcCRs        []vpcCRPair
		matchVPCPath  string
		zonesByNS     map[string][]string
		expectedZones []string
	}{
		{
			name:          "no FVS namespaces",
			expectedZones: []string{},
		},
		{
			name: "union of zones across namespaces, no VPC filter",
			namespaces: []*v1.Namespace{
				fvsNamespace("ns-a", "vpc-cfg-a"),
				fvsNamespace("ns-b", "vpc-cfg-b"),
				{ObjectMeta: metav1.ObjectMeta{Name: "regular-ns"}},
			},
			vpcCRs: []vpcCRPair{
				{name: "vpc-cfg-a", vpcPath: "/org/proj/vpc-a"},
				{name: "vpc-cfg-b", vpcPath: "/org/proj/vpc-b"},
			},
			zonesByNS: map[string][]string{
				"ns-a": {"zone-1", "zone-2"},
				"ns-b": {"zone-2", "zone-3"},
			},
			expectedZones: []string{"zone-1", "zone-2", "zone-3"},
		},
		{
			name: "filters namespaces by matchVPCPath",
			namespaces: []*v1.Namespace{
				fvsNamespace("ns-a", "vpc-cfg-a"),
				fvsNamespace("ns-b", "vpc-cfg-b"),
			},
			vpcCRs: []vpcCRPair{
				{name: "vpc-cfg-a", vpcPath: "/org/proj/vpc-a"},
				{name: "vpc-cfg-b", vpcPath: "/org/proj/vpc-b"},
			},
			matchVPCPath: "/org/proj/vpc-a",
			zonesByNS: map[string][]string{
				"ns-a": {"zone-1"},
				"ns-b": {"zone-2"},
			},
			expectedZones: []string{"zone-1"},
		},
		{
			name: "skips namespace missing the VPC network config annotation",
			namespaces: []*v1.Namespace{
				fvsNamespace("ns-no-annot", ""),
			},
			zonesByNS:     map[string][]string{"ns-no-annot": {"zone-1"}},
			expectedZones: []string{},
		},
		{
			name: "skips namespace whose VPCNetworkConfiguration CR is not found when filtering",
			namespaces: []*v1.Namespace{
				fvsNamespace("ns-dangling", "vpc-cfg-missing"),
			},
			matchVPCPath:  "/org/proj/vpc-a",
			zonesByNS:     map[string][]string{"ns-dangling": {"zone-1"}},
			expectedZones: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sObjs := make([]runtime.Object, len(tt.namespaces))
			for i, ns := range tt.namespaces {
				k8sObjs[i] = ns
			}
			k8sClient := k8sfake.NewClientset(k8sObjs...)
			dc := newFakeDynClientWithVPCCRs(tt.vpcCRs...)

			zones, err := GetFVSZones(ctx, dc, k8sClient, tt.matchVPCPath, zonesFnFromMap(tt.zonesByNS))
			require.NoError(t, err)
			sort.Strings(zones)
			sort.Strings(tt.expectedZones)
			assert.Equal(t, tt.expectedZones, zones)
		})
	}
}

// TestGetZonesForvSANFileServiceMarkerPolicy tests the GetZonesForvSANFileServiceMarkerPolicy function.
func TestGetZonesForvSANFileServiceMarkerPolicy(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		namespaces    []*v1.Namespace
		zonesByNS     map[string][]string
		expectedZones []string
	}{
		{
			name: "aggregates zones across all FVS namespaces regardless of VPC",
			namespaces: []*v1.Namespace{
				fvsNamespace("ns-a", "vpc-cfg-a"),
				fvsNamespace("ns-b", "vpc-cfg-b"),
			},
			zonesByNS: map[string][]string{
				"ns-a": {"zone-1"},
				"ns-b": {"zone-2"},
			},
			expectedZones: []string{"zone-1", "zone-2"},
		},
		{
			name:          "no FVS namespaces returns empty zones",
			expectedZones: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sObjs := make([]runtime.Object, len(tt.namespaces))
			for i, ns := range tt.namespaces {
				k8sObjs[i] = ns
			}
			k8sClient := k8sfake.NewClientset(k8sObjs...)

			zones, err := GetZonesForvSANFileServiceMarkerPolicy(ctx, k8sClient, zonesFnFromMap(tt.zonesByNS))
			require.NoError(t, err)
			sort.Strings(zones)
			sort.Strings(tt.expectedZones)
			assert.Equal(t, tt.expectedZones, zones)
		})
	}
}

// TestGetZonesForvSANFileServiceMarkerPolicyByNamespace tests the
// GetZonesForvSANFileServiceMarkerPolicyByNamespace function.
func TestGetZonesForvSANFileServiceMarkerPolicyByNamespace(t *testing.T) {
	ctx := context.Background()

	consumerNSWithVPC := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:        "consumer-ns",
		Annotations: map[string]string{AnnotationVPCNetworkConfig: "vpc-cfg-a"},
	}}
	consumerNSNoVPC := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "consumer-ns"}}

	tests := []struct {
		name          string
		namespaces    []*v1.Namespace
		vpcCRs        []vpcCRPair
		lookupNS      string
		zonesByNS     map[string][]string
		expectedZones []string
		expectError   bool
	}{
		{
			name: "only zones from FVS namespaces sharing the consumer's VPC path",
			namespaces: []*v1.Namespace{
				consumerNSWithVPC,
				fvsNamespace("fvs-same-vpc", "vpc-cfg-a"),
				fvsNamespace("fvs-other-vpc", "vpc-cfg-b"),
			},
			vpcCRs: []vpcCRPair{
				{name: "vpc-cfg-a", vpcPath: "/org/proj/vpc-a"},
				{name: "vpc-cfg-b", vpcPath: "/org/proj/vpc-b"},
			},
			lookupNS: "consumer-ns",
			zonesByNS: map[string][]string{
				"fvs-same-vpc":  {"zone-1"},
				"fvs-other-vpc": {"zone-2"},
			},
			expectedZones: []string{"zone-1"},
		},
		{
			name:        "consumer namespace missing VPC annotation returns error",
			namespaces:  []*v1.Namespace{consumerNSNoVPC},
			lookupNS:    "consumer-ns",
			expectError: true,
		},
		{
			name:        "consumer namespace not found returns error",
			lookupNS:    "missing-ns",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k8sObjs := make([]runtime.Object, len(tt.namespaces))
			for i, ns := range tt.namespaces {
				k8sObjs[i] = ns
			}
			k8sClient := k8sfake.NewClientset(k8sObjs...)
			dc := newFakeDynClientWithVPCCRs(tt.vpcCRs...)

			zones, err := GetZonesForvSANFileServiceMarkerPolicyByNamespace(
				ctx, dc, k8sClient, tt.lookupNS, zonesFnFromMap(tt.zonesByNS))
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			sort.Strings(zones)
			sort.Strings(tt.expectedZones)
			assert.Equal(t, tt.expectedZones, zones)
		})
	}
}
