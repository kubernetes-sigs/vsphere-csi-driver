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

package common

import (
	"encoding/json"
	"fmt"
	"maps"

	v1 "k8s.io/api/core/v1"
)

// GuestAccessibleTopology builds the guest topology of a Supervisor PVC for the
// HostLocalStorage + PodVM v2 behavior. It uses the committed backing-host mirror
// (AttributeESXiHostMoID), never the Supervisor Node name, for the guest's
// physical-host topology: {zone, topology.csi.vmware.com/host: <host MoID>}. A PVC
// without the mirror returns zone-only topology; one that still carries
// kubernetes.io/hostname without the mirror is rejected.
//
// HostLocalStorage volumes (no mirror, zone + hostname topology) do not use this
// function: PVCSI renames their kubernetes.io/hostname key to the VKS host key instead.
func GuestAccessibleTopology(pvc *v1.PersistentVolumeClaim) ([]map[string]string, error) {
	var topologies []map[string]string
	if err := json.Unmarshal([]byte(pvc.Annotations[AnnVolumeAccessibleTopology]), &topologies); err != nil {
		return nil, fmt.Errorf("invalid accessible topology on PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}
	if len(topologies) == 0 {
		return nil, fmt.Errorf("PVC %s/%s has no accessible topology", pvc.Namespace, pvc.Name)
	}
	host := pvc.Annotations[AttributeESXiHostMoID]
	result := make([]map[string]string, 0, len(topologies))
	for _, topology := range topologies {
		if len(topology) == 0 {
			return nil, fmt.Errorf("PVC %s/%s has an empty accessible topology segment", pvc.Namespace, pvc.Name)
		}
		if topology[v1.LabelHostname] != "" && host == "" {
			return nil, fmt.Errorf("host-local PVC %s/%s has no backing-host metadata", pvc.Namespace, pvc.Name)
		}
		segment := maps.Clone(topology)
		delete(segment, v1.LabelHostname)
		if host != "" {
			if segment[v1.LabelTopologyZone] == "" {
				return nil, fmt.Errorf("host-local PVC %s/%s has no accessible zone", pvc.Namespace, pvc.Name)
			}
			segment[GuestClusterTopologyLabelHost] = host
		}
		result = append(result, segment)
	}
	return result, nil
}
