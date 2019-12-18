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

	// Virtual Center configurations
	VirtualCenter map[string]*VirtualCenterConfig

	// Tag categories and tags which correspond to "built-in node labels: zones and region"
	Labels struct {
		Zone   string `gcfg:"zone"`
		Region string `gcfg:"region"`
	}
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
}
