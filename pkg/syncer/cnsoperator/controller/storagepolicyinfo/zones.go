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

package storagepolicyinfo

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// cacheZonesProvider implements zonesProvider by reading Zone CRs from the controller-runtime
// manager's informer cache.
type cacheZonesProvider struct {
	reader client.Reader
}

var _ zonesProvider = &cacheZonesProvider{}

// newCacheZonesProvider returns a zonesProvider that reads Zone CRs through reader, which in
// production must be the manager's cache (mgr.GetCache()).
func newCacheZonesProvider(reader client.Reader) *cacheZonesProvider {
	return &cacheZonesProvider{reader: reader}
}

// listZones returns the Zone CRs in namespace that are not being deleted.
func (p *cacheZonesProvider) listZones(ctx context.Context, namespace string) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(zoneGVK.GroupVersion().WithKind(zoneGVK.Kind + "List"))
	if err := p.reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list Zones in namespace %q: %w", namespace, err)
	}
	zones := make([]unstructured.Unstructured, 0, len(list.Items))
	for _, zone := range list.Items {
		if zone.GetDeletionTimestamp() == nil {
			zones = append(zones, zone)
		}
	}
	return zones, nil
}

// GetZonesForNamespace returns the names of the Zone CRs in namespace that are not being deleted.
func (p *cacheZonesProvider) GetZonesForNamespace(ctx context.Context,
	namespace string) (map[string]struct{}, error) {
	zones, err := p.listZones(ctx, namespace)
	if err != nil {
		return nil, err
	}
	names := make(map[string]struct{}, len(zones))
	for i := range zones {
		names[zones[i].GetName()] = struct{}{}
	}
	return names, nil
}

// GetActiveClustersForNamespaceInRequestedZones returns the spec.namespace.clusterMoIDs of the
// Zone CRs in namespace named in requestedZones that are not being deleted. Like the commonco
// implementation, it returns an error if one of those Zones has no spec.namespace.clusterMoIDs
// yet, or if none of the requested zones has an active cluster for the namespace.
func (p *cacheZonesProvider) GetActiveClustersForNamespaceInRequestedZones(ctx context.Context,
	namespace string, requestedZones []string) ([]string, error) {
	zones, err := p.listZones(ctx, namespace)
	if err != nil {
		return nil, err
	}
	var activeClusters []string
	for i := range zones {
		if !slices.Contains(requestedZones, zones[i].GetName()) {
			continue
		}
		clusters, found, err := unstructured.NestedStringSlice(zones[i].Object,
			"spec", "namespace", "clusterMoIDs")
		if err != nil {
			return nil, fmt.Errorf("failed to get spec.namespace.clusterMoIDs from Zone %s/%s: %w",
				namespace, zones[i].GetName(), err)
		}
		if !found {
			return nil, fmt.Errorf("spec.namespace.clusterMoIDs not found in Zone %s/%s",
				namespace, zones[i].GetName())
		}
		activeClusters = append(activeClusters, clusters...)
	}
	if len(activeClusters) == 0 {
		return nil, fmt.Errorf("could not find active cluster for the namespace %q in requested zones: %v",
			namespace, requestedZones)
	}
	return activeClusters, nil
}
