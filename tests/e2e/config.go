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

package e2e

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/gcfg.v1"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
)

// ENV variable to specify path of the E2E test config file
const e2eTestConfFileEnvVar = "E2E_TEST_CONF_FILE"

// e2eTestConfig contains vSphere connection detail and kubernetes cluster-id
type e2eTestConfig struct {
	Global struct {
		// Kubernetes Cluster-ID
		ClusterID string `gcfg:"cluster-id"`
		// vCenter username.
		User string `gcfg:"user"`
		// vCenter password in clear text.
		Password string `gcfg:"password"`
		// vCenter Hostname.
		VCenterHostname string `gcfg:"hostname"`
		// vCenter port.
		VCenterPort string `gcfg:"port"`
		// True if vCenter uses self-signed cert.
		InsecureFlag bool `gcfg:"insecure-flag"`
		// Datacenter in which VMs are located.
		Datacenters string `gcfg:"datacenters"`
		// Target datastore urls for provisioning file volumes.
		TargetvSANFileShareDatastoreURLs string `gcfg:"targetvSANFileShareDatastoreURLs"`
		// CnsRegisterVolumesCleanupIntervalInMin specifies the interval after which
		// successful CnsRegisterVolumes will be cleaned up.
		CnsRegisterVolumesCleanupIntervalInMin int `gcfg:"cnsregistervolumes-cleanup-intervalinmin"`
	}
	// Multiple sets of Net Permissions applied to all file shares
	// The string can uniquely represent each Net Permissions config
	NetPermissions map[string]*NetPermissionConfig
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

// getConfig returns e2eTestConfig struct for e2e tests to help establish vSphere connection.
func getConfig() (*e2eTestConfig, error) {
	var confFileLocation = os.Getenv(e2eTestConfFileEnvVar)
	if confFileLocation == "" {
		return nil, fmt.Errorf("environment variable 'E2E_TEST_CONF_FILE' is not set")
	}
	confFile, err := os.Open(confFileLocation)
	if err != nil {
		return nil, err
	}
	defer confFile.Close()
	cfg, err := readConfig(confFile)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// readConfig parses e2e tests config file into Config struct.
func readConfig(config io.Reader) (e2eTestConfig, error) {
	if config == nil {
		err := fmt.Errorf("no config file given")
		return e2eTestConfig{}, err
	}
	var cfg e2eTestConfig
	err := gcfg.ReadInto(&cfg, config)
	return cfg, err
}
