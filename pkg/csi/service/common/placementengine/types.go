package placementengine

import cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"

// VanillaRetrieveTopologyInfoParams represents the params
// required to be able to call GetTopologyInfoFromNodes in
// vanilla cluster.
type VanillaRetrieveTopologyInfoParams struct {
	// VCHost is the vCenter IP/FQDN in which volume
	// provisioning is being attempted.
	VCHost string
	// NodeNames is the list of node names which have
	// access to the selected datastore.
	NodeNames []string
	// DatastoreURL is the selected datastore for which the topology
	// information needs to be retrieved.
	DatastoreURL string
	// RequestedTopologySegments represents the topology segments
	// which need to be satisfied during volume provisioning for a particular VC.
	RequestedTopologySegments []map[string]string
	// IsTopologyPreferentialDatastoresFSSEnabled determines the
	// state of topology-preferential-datastores FSS.
	IsTopologyPreferentialDatastoresFSSEnabled bool
}

// VanillaSharedDatastoresParams represents the params
// required to be able to call GetSharedDatastores function.
type VanillaSharedDatastoresParams struct {
	// Vcenter holds the client connection to the VC on
	// which volume provisioning is being attempted.
	Vcenter *cnsvsphere.VirtualCenter
	// TopologySegmentsList is the list of topology segments
	// which represent the accessibility requirements for the volume provisioning request.
	TopologySegmentsList []map[string]string
	// StoragePolicyID represents the unique ID of the storage policy
	// name given in the Storage Class on the attempted VC.
	StoragePolicyID string
}
