/*
Copyright 2021 The Kubernetes Authors.

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

package types

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
)

// NodeInfo contains the information required for the TopologyService to
// identify a node and retrieve its topology information.
type NodeInfo struct {
	// NodeName uniquely identifies the node in kubernetes.
	NodeName string
	// NodeID is a unique identifier of the NodeVM in vSphere.
	NodeID string
}

// ControllerTopologyService is an interface which exposes functionality
// related to topology aware clusters in the controller mode.
type ControllerTopologyService interface {
	// GetSharedDatastoresInTopology gets the list of shared datastores which adhere to the topology
	// requirement given as a parameter.
	GetSharedDatastoresInTopology(ctx context.Context, topologyRequirement *csi.TopologyRequirement) (
		[]*cnsvsphere.DatastoreInfo, map[string][]map[string]string, error)
}

// NodeTopologyService is an interface which exposes functionality related to
// topology aware clusters in the nodes.
type NodeTopologyService interface {
	// GetNodeTopologyLabels fetches the topology labels of a NodeVM given the NodeInfo.
	GetNodeTopologyLabels(ctx context.Context, info *NodeInfo) (map[string]string, error)
}
