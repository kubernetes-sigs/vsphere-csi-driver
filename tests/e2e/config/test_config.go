/*
Copyright 2025 The Kubernetes Authors.

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

import (
	"fmt"
	"io"
	"os"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	"gopkg.in/gcfg.v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/clients/cns"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
)

// E2eTestConfig contains all test related data to be provided by user
type E2eTestConfig struct {
	TestInput  *TestInputData
	VcClient   *govmomi.Client
	CnsClient  *cns.CnsClient
	RestConfig *rest.Config
}

// TestInputData contains vSphere connection detail and kubernetes cluster-id
type TestInputData struct {
	Global struct {
		// Kubernetes Cluster-ID
		ClusterID string `gcfg:"cluster-id"`
		// Kubernetes Cluster-Distribution
		ClusterDistribution string `gcfg:"cluster-distribution"`
		// vCenter username.
		User string `gcfg:"user"`
		// vCenter password in clear text.
		Password string `gcfg:"password"`
		// vmc vCentre cloudadmin username
		VmcCloudUser string `gcfg:"vmc-cloudadminuser"`
		// vmc vCentre cloudadmin password
		VmcCloudPassword string `gcfg:"cloudadminpassword"`
		// vmc vCentre Devops username
		VmcDevopsUser string `gcfg:"vmc-devopsuser"`
		// vmc vCentre Devops password
		VmcDevopsPassword string `gcfg:"vmc-devopspassword"`
		// vCenter Hostname.
		VCenterHostname string `gcfg:"hostname"`
		// vCenter port.
		VCenterPort string `gcfg:"port"`
		// True if vCenter uses self-signed cert.
		InsecureFlag bool `gcfg:"insecure-flag"`
		// Datacenter in which VMs are located.
		Datacenters string `gcfg:"datacenters"`
		// CnsRegisterVolumesCleanupIntervalInMin specifies the interval after which
		// successful CnsRegisterVolumes will be cleaned up.
		CnsRegisterVolumesCleanupIntervalInMin int `gcfg:"cnsregistervolumes-cleanup-intervalinmin"`
		// preferential topology
		CSIFetchPreferredDatastoresIntervalInMin int `gcfg:"csi-fetch-preferred-datastores-intervalinmin"`
		// QueryLimit specifies the number of volumes that can be fetched by CNS QueryAll API at a time
		QueryLimit int `gcfg:"query-limit"`
		// ListVolumeThreshold specifies the maximum number of differences in volume that can exist between CNS
		// and kubernetes
		ListVolumeThreshold int `gcfg:"list-volume-threshold"`
		// CA file
		CaFile string `gcfg:"ca-file"`
		// Supervisor-id
		SupervisorID string `gcfg:"supervisor-id"`
		// targetvSANFileShareClusters
		TargetVsanFileShareClusters string `gcfg:"targetvSANFileShareClusters"`
		// fileVolumeActivated
		FileVolumeActivated bool `gcfg:"fileVolumeActivated"`
	}
	// Multiple sets of Net Permissions applied to all file shares
	// The string can uniquely represent each Net Permissions config
	NetPermissions map[string]*NetPermissionConfig

	// Snapshot configurations.
	Snapshot SnapshotConfig

	// Topology Level-5
	Labels TopologyLevel5Config

	// Testbed related information like various Esx/VcIps
	TestBedInfo   TestBedConfig
	ClusterFlavor ClusterFlavor
	SVCType       SVCType
	VMMor         VMMor
	BusyBoxGcr    BusyBoxGcr
	WindowsOnMcr  WindowsOnMcr
}

// This struct has all the testbed related information
type TestBedConfig struct {
	EsxIp1              string
	EsxIp2              string
	EsxIp3              string
	EsxIp4              string
	EsxIp5              string
	EsxIp6              string
	EsxIp7              string
	EsxIp8              string
	EsxIp9              string
	EsxIp10             string
	VcAddress           string
	VcAddress2          string
	VcAddress3          string
	MasterIP1           string
	MasterIP2           string
	MasterIP3           string
	IpPortMap           map[string]string
	MissingEnvVars      []string
	DefaultlocalhostIP  string //= "127.0.0.1"
	VcIp2SshPortNum     string
	VcIp3SshPortNum     string
	RwxAccessMode       bool
	Vcptocsi            bool
	MultipleSvc         bool
	WindowsEnv          bool
	Multivc             bool
	StretchedSVC        bool
	IsPrivateNetwork    bool
	LinkedClone         bool
	K8sMasterIp1PortNum string
	K8sMasterIp2PortNum string
	K8sMasterIp3PortNum string
	VcIp1SshPortNum     string
	EsxIp1PortNum       string
	EsxIp2PortNum       string
	EsxIp3PortNum       string
	EsxIp4PortNum       string
	EsxIp5PortNum       string
	EsxIp6PortNum       string
	EsxIp7PortNum       string
	EsxIp8PortNum       string
	EsxIp9PortNum       string
	EsxIp10PortNum      string
	VcVersion           string
	DefaultCluster      *object.ClusterComputeResource
	DefaultDatastore    *object.Datastore
	Name                string `default:"worker"`
	User                string
	Location            string
	VcIp                string
	VcVmName            string
	EsxHosts            []map[string]string
	Podname             string
	Datastores          []map[string]string
}

// NetPermissionConfig consists of information used to restrict the
// network permissions set on file share volumes
type ClusterFlavor struct {
	SupervisorCluster bool
	VanillaCluster    bool
	GuestCluster      bool
}
type SVCType struct {
	StretchedSVC bool
}
type VMMor struct {
	VmIp2MoMap map[string]vim25types.ManagedObjectReference
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
type BusyBoxGcr struct {
	Image string
}
type WindowsOnMcr struct {
	Image string
}

// TopologyLevel5Config contains topology categories
type TopologyLevel5Config struct {
	TopologyCategories string `gcfg:"topology-categories"`
}

// getConfig returns e2eTestConfig struct for e2e tests to help establish vSphere connection.
func GetConfig() (*TestInputData, error) {
	var confFileLocation = os.Getenv(constants.E2eTestConfFileEnvVar)
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
func readConfig(config io.Reader) (TestInputData, error) {
	if config == nil {
		err := fmt.Errorf("no config file given")
		return TestInputData{}, err
	}
	var cfg TestInputData
	err := gcfg.ReadInto(&cfg, config)
	return cfg, err
}
