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
	"sort"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

var vpcNetworkConfigurationGVR = schema.GroupVersionResource{
	Group:    "crd.nsx.vmware.com",
	Version:  "v1alpha1",
	Resource: "vpcnetworkconfigurations",
}

const (
	// AnnotationVPCNetworkConfig is set on supervisor namespaces; value is the VPCNetworkConfiguration CR name.
	AnnotationVPCNetworkConfig = "nsx.vmware.com/vpc_network_config"
	// NamespaceLabelFVSInstance marks FVS instance namespaces (supervisor namespaces that host FileVolume CRs).
	// Expected label value is "true".
	NamespaceLabelFVSInstance = "fvs_instance_namespace"
)

// vpcPathFromVPCNetworkConfiguration returns status.vpcs[0].vpcPath from a VPCNetworkConfiguration CR.
func vpcPathFromVPCNetworkConfiguration(obj *unstructured.Unstructured) string {
	vpcs, found, err := unstructured.NestedSlice(obj.Object, "status", "vpcs")
	if err == nil && found && len(vpcs) > 0 {
		if vpc, ok := vpcs[0].(map[string]interface{}); ok {
			if path, ok := vpc["vpcPath"].(string); ok && path != "" {
				return path
			}
		}
	}
	return ""
}

// VPCPathForNamespace reads the AnnotationVPCNetworkConfig annotation off the given namespace,
// fetches the cluster-scoped VPCNetworkConfiguration CR, and returns status.vpcs[0].vpcPath.
func VPCPathForNamespace(ctx context.Context, dc dynamic.Interface,
	k8sClient kubernetes.Interface, namespace string) (string, error) {
	log := logger.GetLogger(ctx)

	ns, err := k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get namespace %q: %w", namespace, err)
	}

	vpcConfigName := ns.Annotations[AnnotationVPCNetworkConfig]
	if vpcConfigName == "" {
		return "", fmt.Errorf("namespace %q has no %q annotation", namespace, AnnotationVPCNetworkConfig)
	}

	vpcConfig, err := dc.Resource(vpcNetworkConfigurationGVR).Get(ctx, vpcConfigName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get VPCNetworkConfiguration %q: %w", vpcConfigName, err)
	}

	path := vpcPathFromVPCNetworkConfiguration(vpcConfig)
	if path == "" {
		return "", fmt.Errorf("no VPC path in status.vpcs for VPCNetworkConfiguration %q (namespace %q)",
			vpcConfigName, namespace)
	}

	log.Debugf("Namespace %q uses VPC path %q (VPCNetworkConfiguration %q)", namespace, path, vpcConfigName)
	return path, nil
}

// GetFVSZones iterates over all fvs_instance_namespace=true namespaces and calls zonesFn on
// each one whose VPC path satisfies matchVPCPath (empty string means accept all VPC paths).
// Returns the sorted union of all zone sets returned by zonesFn.
func GetFVSZones(ctx context.Context, dc dynamic.Interface,
	k8sClient kubernetes.Interface, matchVPCPath string,
	zonesFn func(string) map[string]struct{}) ([]string, error) {
	log := logger.GetLogger(ctx)

	nsList, err := k8sClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: NamespaceLabelFVSInstance + "=true",
	})
	if err != nil {
		return nil, fmt.Errorf("list fvs_instance_namespace namespaces: %w", err)
	}

	zonesSet := make(map[string]struct{})
	for i := range nsList.Items {
		ns := &nsList.Items[i]
		vpcConfigName := ns.Annotations[AnnotationVPCNetworkConfig]
		if vpcConfigName == "" {
			log.Debugf("Skipping namespace %q: missing %q annotation", ns.Name, AnnotationVPCNetworkConfig)
			continue
		}

		if matchVPCPath != "" {
			vpcConfig, err := dc.Resource(vpcNetworkConfigurationGVR).Get(ctx, vpcConfigName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Debugf("VPCNetworkConfiguration %q for namespace %q not found; skipping",
						vpcConfigName, ns.Name)
					continue
				}
				return nil, fmt.Errorf("get VPCNetworkConfiguration %q for namespace %q: %w",
					vpcConfigName, ns.Name, err)
			}
			if vpcPathFromVPCNetworkConfiguration(vpcConfig) != matchVPCPath {
				continue
			}
		}

		nsZones := zonesFn(ns.Name)
		log.Debugf("Namespace %q contributes zones: %v", ns.Name, nsZones)
		for z := range nsZones {
			zonesSet[z] = struct{}{}
		}
	}

	zones := make([]string, 0, len(zonesSet))
	for z := range zonesSet {
		zones = append(zones, z)
	}
	return zones, nil
}

// GetZonesForvSANFileServiceMarkerPolicy returns the union of zones (via zonesFn) across ALL
// fvs_instance_namespace=true namespaces, regardless of their network type (NSX VPC or VDS).
// This is the cluster-wide accessible zone algorithm for the vSAN File Service marker policy,
// used to populate InfraStoragePolicyInfo.
func GetZonesForvSANFileServiceMarkerPolicy(ctx context.Context,
	k8sClient kubernetes.Interface,
	zonesFn func(string) map[string]struct{}) ([]string, error) {
	log := logger.GetLogger(ctx)

	nsList, err := listFVSInstanceNamespaces(ctx, k8sClient)
	if err != nil {
		return nil, err
	}
	zonesSet := make(map[string]struct{})
	for i := range nsList {
		addNamespaceZones(ctx, nsList[i].Name, zonesFn, zonesSet)
	}
	zones := sortedZoneList(zonesSet)
	log.Infof("Accessible zones for vSAN File Service: %v", zones)
	return zones, nil
}

// GetZonesForvSANFileServiceMarkerPolicyByNamespace returns the union of zones (via zonesFn)
// across the fvs_instance_namespace=true namespaces reachable from the consumer namespace.
// This is the per-namespace accessible zone algorithm for the vSAN File Service marker policy,
// used to populate namespace-level StoragePolicyInfo.
//
//   - NSX VPC (consumer carries AnnotationVPCNetworkConfig): instance namespaces whose VPC path
//     matches the consumer's VPC path.
//   - VDS (no VPC annotation): instance namespaces whose default VDS Network shares a portgroup
//     (Tier 1) or a VDS (Tier 2) with any of the consumer's VDS Networks. This is the same
//     reachability rule the CSI controller uses to place FileVolume CRs.
func GetZonesForvSANFileServiceMarkerPolicyByNamespace(ctx context.Context, dc dynamic.Interface,
	k8sClient kubernetes.Interface, namespace string,
	zonesFn func(string) map[string]struct{}) ([]string, error) {
	log := logger.GetLogger(ctx)

	ns, err := k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get namespace %q: %w", namespace, err)
	}
	if ns.Annotations[AnnotationVPCNetworkConfig] == "" {
		return getZonesForvSANFileServiceMarkerPolicyByVDSNamespace(ctx, dc, k8sClient, namespace, zonesFn)
	}

	consumerVPCPath, err := VPCPathForNamespace(ctx, dc, k8sClient, namespace)
	if err != nil {
		return nil, fmt.Errorf("resolve VPC path for namespace %q: %w", namespace, err)
	}
	log.Infof("Namespace %q has VPC path %q", namespace, consumerVPCPath)

	zones, err := GetFVSZones(ctx, dc, k8sClient, consumerVPCPath, zonesFn)
	if err != nil {
		return nil, err
	}
	log.Infof("Namespace %q marker-policy accessible zones: %v", namespace, zones)
	return zones, nil
}

// getZonesForvSANFileServiceMarkerPolicyByVDSNamespace is the VDS branch of
// GetZonesForvSANFileServiceMarkerPolicyByNamespace.
func getZonesForvSANFileServiceMarkerPolicyByVDSNamespace(ctx context.Context, dc dynamic.Interface,
	k8sClient kubernetes.Interface, namespace string,
	zonesFn func(string) map[string]struct{}) ([]string, error) {
	log := logger.GetLogger(ctx)

	consumerNets, err := GetVDSNetworksForNamespace(ctx, dc, namespace, false)
	if err != nil {
		return nil, fmt.Errorf("resolve VDS networks for namespace %q: %w", namespace, err)
	}
	if len(consumerNets) == 0 {
		return nil, fmt.Errorf("namespace %q has neither the %q annotation nor a %s Network",
			namespace, AnnotationVPCNetworkConfig, NetworkTypeVSphereDistributed)
	}
	log.Infof("Namespace %q has VDS networks %v", namespace, consumerNets)

	nsList, err := listFVSInstanceNamespaces(ctx, k8sClient)
	if err != nil {
		return nil, err
	}
	zonesSet := make(map[string]struct{})
	for i := range nsList {
		instanceNS := nsList[i].Name
		if instanceNS == namespace {
			continue
		}
		instanceNets, err := GetVDSNetworksForNamespace(ctx, dc, instanceNS, true)
		if err != nil {
			return nil, err
		}
		tier := MatchVDSNetworks(consumerNets, instanceNets)
		if tier == FVSNetworkNoMatch {
			log.Debugf("Skipping FVS instance namespace %q: networks %v not reachable from %q",
				instanceNS, instanceNets, namespace)
			continue
		}
		log.Debugf("FVS instance namespace %q matches namespace %q at %s", instanceNS, namespace, tier)
		addNamespaceZones(ctx, instanceNS, zonesFn, zonesSet)
	}
	zones := sortedZoneList(zonesSet)
	log.Infof("Namespace %q marker-policy accessible zones (VDS): %v", namespace, zones)
	return zones, nil
}

// listFVSInstanceNamespaces lists the fvs_instance_namespace=true namespaces.
func listFVSInstanceNamespaces(ctx context.Context, k8sClient kubernetes.Interface) ([]v1.Namespace, error) {
	nsList, err := k8sClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: NamespaceLabelFVSInstance + "=true",
	})
	if err != nil {
		return nil, fmt.Errorf("list fvs_instance_namespace namespaces: %w", err)
	}
	return nsList.Items, nil
}

// addNamespaceZones adds the zones zonesFn reports for namespace into zonesSet.
func addNamespaceZones(ctx context.Context, namespace string, zonesFn func(string) map[string]struct{},
	zonesSet map[string]struct{}) {
	nsZones := zonesFn(namespace)
	logger.GetLogger(ctx).Debugf("Namespace %q contributes zones: %v", namespace, nsZones)
	for z := range nsZones {
		zonesSet[z] = struct{}{}
	}
}

// sortedZoneList returns the zones in zonesSet as a sorted slice (never nil).
func sortedZoneList(zonesSet map[string]struct{}) []string {
	zones := make([]string, 0, len(zonesSet))
	for z := range zonesSet {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	return zones
}
