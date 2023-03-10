/*
Copyright 2019 The Kubernetes Authors.

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

package config

import vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"

// Config is used to read and store information from the cloud configuration file
type Config struct {
	Global struct {
		//vCenter IP address or FQDN
		VCenterIP string
		// Kubernetes Cluster ID
		ClusterID string `gcfg:"cluster-id"`
		// SupervisorID is the UUID representing Supervisor Cluster. ClusterID is being deprecated
		// and SupervisorID is the replacement ID we need to use for VolumeMetadata and datastore lookup.
		SupervisorID string `gcfg:"supervisor-id"`
		// vCenter username.
		User string `gcfg:"user"`
		// vCenter password in clear text.
		Password string `gcfg:"password"`
		// vCenter port.
		VCenterPort string `gcfg:"port"`
		// Specifies whether to verify the server's certificate chain. Set to true to
		// skip verification.
		InsecureFlag bool `gcfg:"insecure-flag"`
		// Specifies the path to a CA certificate in PEM format. This has no effect if
		// InsecureFlag is enabled. Optional; if not configured, the system's CA
		// certificates will be used.
		CAFile string `gcfg:"ca-file"`
		// Thumbprint specifies the certificate thumbprint to use
		// This has no effect if InsecureFlag is enabled.
		Thumbprint string `gcfg:"thumbprint"`
		// Datacenter in which Node VMs are located.
		Datacenters string `gcfg:"datacenters"`
		// CnsRegisterVolumesCleanupIntervalInMin specifies the interval after which
		// successful CnsRegisterVolumes will be cleaned up.
		CnsRegisterVolumesCleanupIntervalInMin int `gcfg:"cnsregistervolumes-cleanup-intervalinmin"`
		// VolumeMigrationCRCleanupIntervalInMin specifies the interval after which
		// stale CnsVSphereVolumeMigration CRs will be cleaned up.
		VolumeMigrationCRCleanupIntervalInMin int `gcfg:"volumemigration-cr-cleanup-intervalinmin"`
		// VCClientTimeout specifies a time limit in minutes for requests made by client
		// If not set, default will be 5 minutes
		VCClientTimeout int `gcfg:"vc-client-timeout"`
		// Cluster Distribution Name
		ClusterDistribution string `gcfg:"cluster-distribution"`

		//CSIAuthCheckIntervalInMin specifies the interval that the auth check for datastores will be trigger
		CSIAuthCheckIntervalInMin int `gcfg:"csi-auth-check-intervalinmin"`
		// CnsVolumeOperationRequestCleanupIntervalInMin specifies the interval after which
		// stale CnsVolumeOperationRequest instances will be cleaned up.
		CnsVolumeOperationRequestCleanupIntervalInMin int `gcfg:"cnsvolumeoperationrequest-cleanup-intervalinmin"`
		// CSIFetchPreferredDatastoresIntervalInMin specifies the interval
		// after which the preferred datastores cache is refreshed in the driver.
		CSIFetchPreferredDatastoresIntervalInMin int `gcfg:"csi-fetch-preferred-datastores-intervalinmin"`

		// QueryLimit specifies the number of volumes that can be fetched by CNS QueryAll API at a time
		QueryLimit int `gcfg:"query-limit"`
		// ListVolumeThreshold specifies the maximum number of differences in volume that can exist between CNS
		// and kubernetes
		ListVolumeThreshold int `gcfg:"list-volume-threshold"`
	}

	// Multiple sets of Net Permissions applied to all file shares
	// The string can uniquely represent each Net Permissions config
	NetPermissions map[string]*NetPermissionConfig

	// Virtual Center configurations
	VirtualCenter map[string]*VirtualCenterConfig

	// Snapshot configurations.
	Snapshot SnapshotConfig

	// Guest Cluster configurations, only used by GC
	GC GCConfig

	// Labels will list the topology domains the CSI driver is expected
	// to pick up from the inventory. This info will later be used while provisioning volumes.
	Labels struct {
		// Zone and Region correspond to the vSphere categories
		// created to tag specific topology domains in the inventory.
		Zone   string `gcfg:"zone"`   // Deprecated
		Region string `gcfg:"region"` // Deprecated
		// TopologyCategories is a comma separated string of topology domains
		// which will correspond to the `Categories` the vSphere admin will
		// create in the inventory using the UI.
		// Maximum number of categories allowed is 5.
		TopologyCategories string `gcfg:"topology-categories"`
	}

	TopologyCategory map[string]*TopologyCategoryInfo
}

// ConfigurationInfo is a struct that used to capture config param details
type ConfigurationInfo struct {
	Cfg *Config
}

// FeatureStatesConfigInfo contains the details about feature states configmap
type FeatureStatesConfigInfo struct {
	Name      string
	Namespace string
}

// TopologyCategoryInfo contains metadata for the Zone and Region parameters under Labels section.
type TopologyCategoryInfo struct {
	Label string `gcfg:"label"`
}

// NetPermissionConfig consists of information used to restrict the
// network permissions set on file share volumes
type NetPermissionConfig struct {
	// Client IP address, IP range or IP subnet. Example: "10.20.30.0/24"; defaults to "*" if not specified
	Ips string `gcfg:"ips"`
	// Is it READ_ONLY, READ_WRITE or NO_ACCESS. Defaults to "READ_WRITE" if not specified
	Permissions vsanfstypes.VsanFileShareAccessType `gcfg:"permissions"`
	// Disallow root access for this IP range. Defaults to "false" if not specified
	RootSquash bool `gcfg:"rootsquash"`
}

// VirtualCenterConfig contains information used to access a remote vCenter
// endpoint.
type VirtualCenterConfig struct {
	// vCenter username.
	User string `gcfg:"user"`
	// vCenter password in clear text.
	Password string `gcfg:"password"`
	// vCenter port.
	VCenterPort string `gcfg:"port"`
	// True if vCenter uses self-signed cert.
	InsecureFlag bool `gcfg:"insecure-flag"`
	// Datacenter in which VMs are located.
	Datacenters string `gcfg:"datacenters"`
	// Target datastore urls for provisioning file volumes.
	TargetvSANFileShareDatastoreURLs string `gcfg:"targetvSANFileShareDatastoreURLs"`
	// TargetvSANFileShareClusters represents file service enabled vSAN clusters on which file volumes can be created.
	TargetvSANFileShareClusters string `gcfg:"targetvSANFileShareClusters"`
	// MigrationDataStore specifies datastore which is set as default datastore in legacy cloud-config
	// and hence should be used as default datastore.
	MigrationDataStoreURL string `gcfg:"migration-datastore-url"`
}

// GCConfig contains information used by guest cluster to access a supervisor
// cluster endpoint
type GCConfig struct {
	// Supervisor Cluster server IP
	Endpoint string `gcfg:"endpoint"`
	// Supervisor Cluster server port
	Port string `gcfg:"port"`
	// Guest Cluster UID
	TanzuKubernetesClusterUID string `gcfg:"tanzukubernetescluster-uid"`
	// Guest Cluster Name
	TanzuKubernetesClusterName string `gcfg:"tanzukubernetescluster-name"`
	// Cluster Distribution Name
	ClusterDistribution string `gcfg:"cluster-distribution"`
	// ClusterAPIVersion refers to the API version of the object guest cluster is created from.
	ClusterAPIVersion string `gcfg:"cluster-api-version"`
	// ClusterKind refers to the kind of object guest cluster is created from.
	ClusterKind string `gcfg:"cluster-kind"`
}

// SnapshotConfig contains snapshot configuration.
type SnapshotConfig struct {
	// GlobalMaxSnapshotsPerBlockVolume specifies the maximum number of block volume snapshots per volume.
	GlobalMaxSnapshotsPerBlockVolume int `gcfg:"global-max-snapshots-per-block-volume"`
	// GranularMaxSnapshotsPerBlockVolumeInVSAN specifies the maximum number of block volume snapshots
	// per volume in VSAN datastores.
	GranularMaxSnapshotsPerBlockVolumeInVSAN int `gcfg:"granular-max-snapshots-per-block-volume-vsan"`
	// GranularMaxSnapshotsPerBlockVolumeInVVOL specifies the maximum number of block volume snapshots
	// per volume in VVOL datastores.
	GranularMaxSnapshotsPerBlockVolumeInVVOL int `gcfg:"granular-max-snapshots-per-block-volume-vvol"`
}

// EnvClusterFlavor is the k8s cluster type on which CSI Driver is being deployed
const EnvClusterFlavor = "CLUSTER_FLAVOR"
