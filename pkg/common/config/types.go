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
		// Datacenter in which Node VMs are located.
		Datacenters string `gcfg:"datacenters"`
	}

	// Multiple sets of Net Permissions applied to all file shares
	// The string can uniquely represent each Net Permissions config
	NetPermissions map[string]*NetPermissionConfig

	// Virtual Center configurations
	VirtualCenter map[string]*VirtualCenterConfig

	// Guest Cluster configurations, only used by GC
	GC GCConfig

	// Tag categories and tags which correspond to "built-in node labels: zones and region"
	Labels struct {
		Zone   string `gcfg:"zone"`
		Region string `gcfg:"region"`
	}
	// Set of features with state flags
	FeatureStates FeatureStateSwitches
}

// FeatureStateSwitches contains flags to enable/disable features
type FeatureStateSwitches struct {
	CSIMigration bool `gcfg:"csi-migration"`
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
}
