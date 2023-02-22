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

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

// NodeInfo contains the information required for the TopologyService to
// identify a node and retrieve its topology information.
type NodeInfo struct {
	// NodeName uniquely identifies the node in kubernetes.
	NodeName string
	// NodeID is a unique identifier of the NodeVM in vSphere.
	NodeID string
}

// VanillaTopologyFetchDSParams represents the params required to call
// GetSharedDatastoresInTopology in vanilla cluster.
type VanillaTopologyFetchDSParams struct {
	// TopologyRequirement represents the topology conditions
	// which need to be satisfied during volume provisioning.
	TopologyRequirement *csi.TopologyRequirement
	// Vc is the vcenter instance using which storage policy
	// compatibility will be calculated.
	Vc *cnsvsphere.VirtualCenter
	// StoragePolicyName is the policy name specified in the storage class
	// to which the volume should be compatible.
	StoragePolicyName string
}

// WCPTopologyFetchDSParams represents the params required to call
// GetSharedDatastoresInTopology in workload cluster.
type WCPTopologyFetchDSParams struct {
	// TopologyRequirement represents the topology conditions
	// which need to be satisfied during volume provisioning.
	TopologyRequirement *csi.TopologyRequirement
	// Vc is the vcenter instance using which the potential
	// datastores will be calculated.
	Vc *cnsvsphere.VirtualCenter
}

// VanillaRetrieveTopologyInfoParams represents the params
// required to be able to call GetTopologyInfoFromNodes in
// vanilla cluster.
type VanillaRetrieveTopologyInfoParams struct {
	// NodeNames is the list of node names which have
	// access to the selected datastore.
	NodeNames []string
	// DatastoreURL is the selected datastore for which the topology
	// information needs to be retrieved.
	DatastoreURL string
	// TopologyRequirement represents the topology conditions
	// which need to be satisfied during volume provisioning.
	TopologyRequirement *csi.TopologyRequirement
}

// WCPRetrieveTopologyInfoParams represents the params required to call
// GetTopologyInfoFromNodes in workload cluster.
type WCPRetrieveTopologyInfoParams struct {
	// DatastoreURL is the selected datastore for which the topology
	// information needs to be retrieved.
	DatastoreURL string
	// StorageTopologyType is a storageClass parameter.
	// It represents a zonal or a crossZonal volume provisioning.
	StorageTopologyType string
	// TopologyRequirement represents the topology conditions
	// which need to be satisfied during volume provisioning.
	TopologyRequirement *csi.TopologyRequirement
	// Vc is the vcenter instance using which the potential
	// datastores will be calculated.
	Vc *cnsvsphere.VirtualCenter
}

// ControllerTopologyService is an interface which exposes functionality
// related to topology aware clusters in the controller mode.
type ControllerTopologyService interface {
	// GetSharedDatastoresInTopology gets the list of shared datastores which adhere to the topology
	// requirement given in the CreateVolume request.
	GetSharedDatastoresInTopology(ctx context.Context, topologyFetchDSParams interface{}) ([]*cnsvsphere.DatastoreInfo,
		error)
	// GetTopologyInfoFromNodes retrieves the topology information of the nodes after the datastore has been
	// selected for volume provisioning.
	GetTopologyInfoFromNodes(ctx context.Context, retrieveTopologyInfoParams interface{}) ([]map[string]string, error)
}

// NodeTopologyService is an interface which exposes functionality related to
// topology aware clusters in the nodes.
type NodeTopologyService interface {
	// GetNodeTopologyLabels fetches the topology labels of a NodeVM given the NodeInfo.
	GetNodeTopologyLabels(ctx context.Context, info *NodeInfo) (map[string]string, error)
}
