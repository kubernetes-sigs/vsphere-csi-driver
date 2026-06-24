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

	crName := ns.Annotations[AnnotationVPCNetworkConfig]
	if crName == "" {
		return "", fmt.Errorf("namespace %q has no %q annotation", namespace, AnnotationVPCNetworkConfig)
	}

	cr, err := dc.Resource(vpcNetworkConfigurationGVR).Get(ctx, crName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get VPCNetworkConfiguration %q: %w", crName, err)
	}

	path := vpcPathFromVPCNetworkConfiguration(cr)
	if path == "" {
		return "", fmt.Errorf("no VPC path in status.vpcs for VPCNetworkConfiguration %q (namespace %q)",
			crName, namespace)
	}

	log.Debugf("Namespace %q uses VPC path %q (VPCNetworkConfiguration %q)", namespace, path, crName)
	return path, nil
}

// collectFVSZones iterates over all fvs_instance_namespace=true namespaces and calls zonesFn on
// each one whose VPC path satisfies matchVPCPath (empty string means accept all VPC paths).
// Returns the sorted union of all zone sets returned by zonesFn.
func collectFVSZones(ctx context.Context, dc dynamic.Interface,
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
		crName := ns.Annotations[AnnotationVPCNetworkConfig]
		if crName == "" {
			log.Debugf("Skipping namespace %q: missing %q annotation", ns.Name, AnnotationVPCNetworkConfig)
			continue
		}

		if matchVPCPath != "" {
			cr, err := dc.Resource(vpcNetworkConfigurationGVR).Get(ctx, crName, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Debugf("VPCNetworkConfiguration %q for namespace %q not found; skipping",
						crName, ns.Name)
					continue
				}
				return nil, fmt.Errorf("get VPCNetworkConfiguration %q for namespace %q: %w",
					crName, ns.Name, err)
			}
			if vpcPathFromVPCNetworkConfiguration(cr) != matchVPCPath {
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
// fvs_instance_namespace=true namespaces that carry the AnnotationVPCNetworkConfig annotation.
// This is the cluster-wide accessible zone algorithm for the vSAN File Service marker policy,
// used to populate InfraStoragePolicyInfo.
func GetZonesForvSANFileServiceMarkerPolicy(ctx context.Context,
	k8sClient kubernetes.Interface,
	zonesFn func(string) map[string]struct{}) ([]string, error) {
	log := logger.GetLogger(ctx)

	zones, err := collectFVSZones(ctx, nil, k8sClient, "", zonesFn)
	if err != nil {
		return nil, err
	}
	log.Infof("Accessible zones for vSAN File Service: %v", zones)
	return zones, nil
}

// GetZonesForvSANFileServiceMarkerPolicyByNamespace returns the union of zones (via zonesFn)
// across all fvs_instance_namespace=true namespaces whose VPC path matches the consumer
// namespace's VPC path.
// This is the per-namespace accessible zone algorithm for the vSAN File Service marker policy,
// used to populate namespace-level StoragePolicyInfo.
func GetZonesForvSANFileServiceMarkerPolicyByNamespace(ctx context.Context, dc dynamic.Interface,
	k8sClient kubernetes.Interface, namespace string,
	zonesFn func(string) map[string]struct{}) ([]string, error) {
	log := logger.GetLogger(ctx)

	consumerVPCPath, err := VPCPathForNamespace(ctx, dc, k8sClient, namespace)
	if err != nil {
		return nil, fmt.Errorf("resolve VPC path for namespace %q: %w", namespace, err)
	}
	log.Infof("Namespace %q has VPC path %q", namespace, consumerVPCPath)

	zones, err := collectFVSZones(ctx, dc, k8sClient, consumerVPCPath, zonesFn)
	if err != nil {
		return nil, err
	}
	log.Infof("Namespace %q marker-policy accessible zones: %v", namespace, zones)
	return zones, nil
}
