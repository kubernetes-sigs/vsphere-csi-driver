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

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"gopkg.in/gcfg.v1"

	cnstypes "github.com/vmware/govmomi/cns/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

const (
	// DefaultVCenterPort is the default port used to access vCenter.
	DefaultVCenterPort string = "443"
	// DefaultGCPort is the default port used to access Supervisor Cluster.
	DefaultGCPort string = "6443"
	// DefaultCloudConfigPath is the default path of csi config file.
	DefaultCloudConfigPath = "/etc/cloud/csi-vsphere.conf"
	// DefaultGCConfigPath is the default path of GC config file.
	DefaultGCConfigPath = "/etc/cloud/pvcsi-config/cns-csi.conf"
	// SupervisorCAFilePath is the file path of certificate in Supervisor
	// Clusters. This is needed to establish VC connection.
	SupervisorCAFilePath = "/etc/vmware/wcp/tls/vmca.pem"
	// EnvVSphereCSIConfig contains the path to the CSI vSphere Config.
	EnvVSphereCSIConfig = "VSPHERE_CSI_CONFIG"
	// EnvGCConfig contains the path to the CSI GC Config.
	EnvGCConfig = "GC_CONFIG"
	// DefaultpvCSIProviderPath is the default path of pvCSI provider config.
	DefaultpvCSIProviderPath = "/etc/cloud/pvcsi-provider"
	// DefaultSupervisorFSSConfigMapName is the default name of Feature states
	// config map in Supervisor cluster. This configmap is also replicated by
	// the supervisor unto any TKGS deployed on it.
	DefaultSupervisorFSSConfigMapName = "csi-feature-states"
	// DefaultInternalFSSConfigMapName is the default name of feature states
	// config map used in pvCSI and Vanilla drivers.
	DefaultInternalFSSConfigMapName = "internal-feature-states.csi.vsphere.vmware.com"
	// DefaultCSINamespace is the default namespace for CNS-CSI and pvCSI drivers.
	DefaultCSINamespace = "vmware-system-csi"
	// DefaultCnsRegisterVolumesCleanupIntervalInMin is the default time
	// interval after which successful CnsRegisterVolumes will be cleaned up.
	// Current default value is set to 12 hours
	DefaultCnsRegisterVolumesCleanupIntervalInMin = 720
	// DefaultVolumeMigrationCRCleanupIntervalInMin is the default time interval
	// after which stale CnsVSphereVolumeMigration CRs will be cleaned up.
	// Current default value is set to 2 hours.
	DefaultVolumeMigrationCRCleanupIntervalInMin = 120
	// DefaultCSIAuthCheckIntervalInMin is the default time interval to refresh
	// DatastoreMap.
	DefaultCSIAuthCheckIntervalInMin = 5
	// DefaultCnsVolumeOperationRequestCleanupIntervalInMin is the default time
	// interval after which stale CnsVSphereVolumeMigration CRs will be cleaned up.
	// Current default value is set to 24 hours.
	DefaultCnsVolumeOperationRequestCleanupIntervalInMin = 1440
	// DefaultGlobalMaxSnapshotsPerBlockVolume is the default maximum number of block volume snapshots per volume.
	DefaultGlobalMaxSnapshotsPerBlockVolume = 3
	// MaxNumberOfTopologyCategories is the max number of topology domains/categories allowed.
	MaxNumberOfTopologyCategories = 5
)

// Errors
var (
	// ErrUsernameMissing is returned when the provided username is empty.
	ErrUsernameMissing = errors.New("username is missing")

	// ErrPasswordMissing is returned when the provided password is empty.
	ErrPasswordMissing = errors.New("password is missing")

	// ErrInvalidVCenterIP is returned when the provided vCenter IP address is
	// missing from the provided configuration.
	ErrInvalidVCenterIP = errors.New("vsphere.conf does not have the VirtualCenter IP address specified")

	// ErrMissingVCenter is returned when the provided configuration does not
	// define any vCenters.
	ErrMissingVCenter = errors.New("no Virtual Center hosts defined")

	// ErrClusterIdCharLimit is returned when the provided cluster id is more
	// than 64 characters.
	ErrClusterIDCharLimit = errors.New("cluster id must not exceed 64 characters")

	// ErrMissingEndpoint is returned when the provided configuration does not
	// define any endpoints.
	ErrMissingEndpoint = errors.New("no Supervisor Cluster endpoint defined in Guest Cluster config")

	// ErrMissingTanzuKubernetesClusterUID is returned when the provided
	// configuration does not define any TanzuKubernetesClusterUID.
	ErrMissingTanzuKubernetesClusterUID = errors.New("no Tanzu Kubernetes Cluster UID defined in Guest Cluster config")

	// ErrInvalidNetPermission is returned when the value of Permission in
	// NetPermissions is not among the ones listed.
	ErrInvalidNetPermission = errors.New("invalid value for Permissions under NetPermission Config")
)

func getEnvKeyValue(match string, partial bool) (string, string, error) {
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")
		if len(pair) != 2 {
			continue
		}

		key := pair[0]
		value := pair[1]

		if partial && strings.Contains(key, match) {
			return key, value, nil
		}

		if strings.Compare(key, match) == 0 {
			return key, value, nil
		}
	}

	matchType := "match"
	if partial {
		matchType = "partial match"
	}

	return "", "", fmt.Errorf("failed to find %s with %s", matchType, match)
}

// FromEnv initializes the provided configuration object with values
// obtained from environment variables. If an environment variable is set
// for a property that's already initialized, the environment variable's value
// takes precedence.
func FromEnv(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config object cannot be nil")
	}
	log := logger.GetLogger(ctx)
	// Init.
	if cfg.VirtualCenter == nil {
		cfg.VirtualCenter = make(map[string]*VirtualCenterConfig)
	}

	// Globals.
	if v := os.Getenv("VSPHERE_VCENTER"); v != "" {
		cfg.Global.VCenterIP = v
	}
	if v := os.Getenv("VSPHERE_VCENTER_PORT"); v != "" {
		cfg.Global.VCenterPort = v
	}
	if v := os.Getenv("VSPHERE_USER"); v != "" {
		cfg.Global.User = v
	}
	if v := os.Getenv("VSPHERE_PASSWORD"); v != "" {
		cfg.Global.Password = v
	}
	if v := os.Getenv("VSPHERE_DATACENTER"); v != "" {
		cfg.Global.Datacenters = v
	}
	if v := os.Getenv("VSPHERE_INSECURE"); v != "" {
		InsecureFlag, err := strconv.ParseBool(v)
		if err != nil {
			log.Errorf("failed to parse VSPHERE_INSECURE: %s", err)
		} else {
			cfg.Global.InsecureFlag = InsecureFlag
		}
	}
	if v := os.Getenv("VSPHERE_LABEL_REGION"); v != "" {
		cfg.Labels.Region = v
	}
	if v := os.Getenv("VSPHERE_LABEL_ZONE"); v != "" {
		cfg.Labels.Zone = v
	}
	if v := os.Getenv("GLOBAL_MAX_SNAPSHOTS_PER_BLOCK_VOLUME"); v != "" {
		maxSnaps, err := strconv.Atoi(v)
		if err != nil {
			log.Errorf("failed to parse GLOBAL_MAX_SNAPSHOTS_PER_BLOCK_VOLUME: %s", err)
		} else {
			cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume = maxSnaps
		}
	}
	if v := os.Getenv("GRANULAR_MAX_SNAPSHOTS_PER_BLOCK_VOLUME_VSAN"); v != "" {
		maxSnaps, err := strconv.Atoi(v)
		if err != nil {
			log.Errorf("failed to parse GRANULAR_MAX_SNAPSHOTS_PER_BLOCK_VOLUME_VSAN: %s", err)
		} else {
			cfg.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVSAN = maxSnaps
		}
	}
	if v := os.Getenv("GRANULAR_MAX_SNAPSHOTS_PER_BLOCK_VOLUME_VVOL"); v != "" {
		maxSnaps, err := strconv.Atoi(v)
		if err != nil {
			log.Errorf("failed to parse GRANULAR_MAX_SNAPSHOTS_PER_BLOCK_VOLUME_VVOL: %s", err)
		} else {
			cfg.Snapshot.GranularMaxSnapshotsPerBlockVolumeInVVOL = maxSnaps
		}
	}
	// Build VirtualCenter from ENVs.
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")

		if len(pair) != 2 {
			continue
		}

		key := pair[0]
		value := pair[1]

		if strings.HasPrefix(key, "VSPHERE_VCENTER_") && len(value) > 0 {
			id := strings.TrimPrefix(key, "VSPHERE_VCENTER_")
			vcenter := value

			_, username, errUsername := getEnvKeyValue("VCENTER_"+id+"_USERNAME", false)
			if errUsername != nil {
				username = cfg.Global.User
			}
			_, password, errPassword := getEnvKeyValue("VCENTER_"+id+"_PASSWORD", false)
			if errPassword != nil {
				password = cfg.Global.Password
			}
			_, port, errPort := getEnvKeyValue("VCENTER_"+id+"_PORT", false)
			if errPort != nil {
				port = cfg.Global.VCenterPort
			}
			insecureFlag := false
			_, insecureTmp, errInsecure := getEnvKeyValue("VCENTER_"+id+"_INSECURE", false)
			if errInsecure != nil {
				insecureFlagTmp, errTmp := strconv.ParseBool(insecureTmp)
				if errTmp == nil {
					insecureFlag = insecureFlagTmp
				}
			}
			_, datacenters, errDatacenters := getEnvKeyValue("VCENTER_"+id+"_DATACENTERS", false)
			if errDatacenters != nil {
				datacenters = cfg.Global.Datacenters
			}
			_, targetFSDatastores, errDatastores := getEnvKeyValue("VCENTER_"+id+"_TARGETFSDATASTORES", false)
			if errDatastores != nil {
				targetFSDatastores = ""
			}
			cfg.VirtualCenter[vcenter] = &VirtualCenterConfig{
				User:                             username,
				Password:                         password,
				VCenterPort:                      port,
				InsecureFlag:                     insecureFlag,
				Datacenters:                      datacenters,
				TargetvSANFileShareDatastoreURLs: targetFSDatastores,
			}
		}
	}
	if cfg.Global.VCenterIP != "" && cfg.VirtualCenter[cfg.Global.VCenterIP] == nil {
		cfg.VirtualCenter[cfg.Global.VCenterIP] = &VirtualCenterConfig{
			User:         cfg.Global.User,
			Password:     cfg.Global.Password,
			VCenterPort:  cfg.Global.VCenterPort,
			InsecureFlag: cfg.Global.InsecureFlag,
			Datacenters:  cfg.Global.Datacenters,
		}
	}
	err := validateConfig(ctx, cfg)
	if err != nil {
		return err
	}

	return nil
}

func validateConfig(ctx context.Context, cfg *Config) error {
	log := logger.GetLogger(ctx)
	// Fix default global values.
	if cfg.Global.VCenterPort == "" {
		cfg.Global.VCenterPort = DefaultVCenterPort
	}
	// Must have at least one vCenter defined.
	if len(cfg.VirtualCenter) == 0 {
		log.Error(ErrMissingVCenter)
		return ErrMissingVCenter
	}
	// Cluster ID should not exceed 64 characters.
	if len(cfg.Global.ClusterID) > 64 {
		log.Error(ErrClusterIDCharLimit)
		return ErrClusterIDCharLimit
	}
	for vcServer, vcConfig := range cfg.VirtualCenter {
		log.Debugf("Initializing vc server %s", vcServer)
		if vcServer == "" {
			log.Error(ErrInvalidVCenterIP)
			return ErrInvalidVCenterIP
		}

		if vcConfig.User == "" {
			vcConfig.User = cfg.Global.User
			if vcConfig.User == "" {
				log.Errorf("vcConfig.User is empty for vc %s!", vcServer)
				return ErrUsernameMissing
			}
		}
		if vcConfig.Password == "" {
			vcConfig.Password = cfg.Global.Password
			if vcConfig.Password == "" {
				log.Errorf("vcConfig.Password is empty for vc %s!", vcServer)
				return ErrPasswordMissing
			}
		}
		if vcConfig.VCenterPort == "" {
			vcConfig.VCenterPort = cfg.Global.VCenterPort
		}
		if vcConfig.Datacenters == "" {
			if cfg.Global.Datacenters != "" {
				vcConfig.Datacenters = cfg.Global.Datacenters
			}
		}
		insecure := vcConfig.InsecureFlag
		if !insecure {
			vcConfig.InsecureFlag = cfg.Global.InsecureFlag
		}
	}
	clusterFlavor, err := GetClusterFlavor(ctx)
	if err != nil {
		return err
	}
	if cfg.NetPermissions == nil {
		// If no net permissions are given, assume default.
		log.Info("No Net Permissions given in Config. Using default permissions.")
		if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
			cfg.NetPermissions = map[string]*NetPermissionConfig{"#": GetDefaultNetPermission()}
		}
	} else {
		for key, netPerm := range cfg.NetPermissions {
			if netPerm.Permissions == "" {
				netPerm.Permissions = vsanfstypes.VsanFileShareAccessTypeREAD_WRITE
			} else if netPerm.Permissions != vsanfstypes.VsanFileShareAccessTypeNO_ACCESS &&
				netPerm.Permissions != vsanfstypes.VsanFileShareAccessTypeREAD_ONLY &&
				netPerm.Permissions != vsanfstypes.VsanFileShareAccessTypeREAD_WRITE {
				log.Errorf("Invalid value %s for Permissions under NetPermission Config %s", netPerm.Permissions, key)
				return ErrInvalidNetPermission
			}
			if netPerm.Ips == "" {
				netPerm.Ips = "*"
			}
		}
	}

	if cfg.Global.CnsRegisterVolumesCleanupIntervalInMin == 0 {
		cfg.Global.CnsRegisterVolumesCleanupIntervalInMin = DefaultCnsRegisterVolumesCleanupIntervalInMin
	}
	if cfg.Global.VolumeMigrationCRCleanupIntervalInMin == 0 {
		cfg.Global.VolumeMigrationCRCleanupIntervalInMin = DefaultVolumeMigrationCRCleanupIntervalInMin
	}
	if cfg.Global.CSIAuthCheckIntervalInMin == 0 {
		cfg.Global.CSIAuthCheckIntervalInMin = DefaultCSIAuthCheckIntervalInMin
	}
	if cfg.Global.CnsVolumeOperationRequestCleanupIntervalInMin == 0 {
		cfg.Global.CnsVolumeOperationRequestCleanupIntervalInMin =
			DefaultCnsVolumeOperationRequestCleanupIntervalInMin
	}
	if cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume == 0 {
		cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume = DefaultGlobalMaxSnapshotsPerBlockVolume
	}

	// Labels section validation - the customer can either provide topology
	// domain info using zone,region parameters or by using the topologyCategories
	// parameter. Specifying all the 3 parameters is not allowed.
	if strings.TrimSpace(cfg.Labels.TopologyCategories) != "" &&
		(strings.TrimSpace(cfg.Labels.Zone) != "" || strings.TrimSpace(cfg.Labels.Region) != "") {
		return logger.LogNewErrorf(log,
			"zone and region parameters should be skipped when topologyCategories is specified.")
	}

	// Validate length of topologyCategories in Labels section
	if strings.TrimSpace(cfg.Labels.TopologyCategories) != "" {
		if len(strings.Split(cfg.Labels.TopologyCategories, ",")) > MaxNumberOfTopologyCategories {
			return logger.LogNewErrorf(log, "maximum limit of topology categories exceeded. Only %d allowed.",
				MaxNumberOfTopologyCategories)
		}
	}

	return nil
}

// ReadConfig parses vSphere cloud config file and stores it into VSphereConfig.
// Environment variables are also checked.
func ReadConfig(ctx context.Context, config io.Reader) (*Config, error) {
	log := logger.GetLogger(ctx)
	if config == nil {
		return nil, fmt.Errorf("no vSphere CSI driver config file given")
	}
	cfg := &Config{}
	if err := gcfg.FatalOnly(gcfg.ReadInto(cfg, config)); err != nil {
		log.Errorf("error while reading config file: %+v", err)
		return nil, err
	}
	// Env Vars should override config file entries if present.
	if err := FromEnv(ctx, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// GetCnsconfig returns Config from specified config file path.
func GetCnsconfig(ctx context.Context, cfgPath string) (*Config, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("GetCnsconfig called with cfgPath: %s", cfgPath)
	var cfg *Config
	// Read in the vsphere.conf if it exists.
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		log.Infof("Could not stat %s, reading config params from env", cfgPath)
		// Config from Env var only.
		cfg = &Config{}
		if fromEnvErr := FromEnv(ctx, cfg); fromEnvErr != nil {
			log.Errorf("Failed to get config params from env. Err: %v", fromEnvErr)
			return cfg, err
		}
	} else {
		config, err := os.Open(cfgPath)
		if err != nil {
			log.Errorf("failed to open %s. Err: %v", cfgPath, err)
			return cfg, err
		}
		cfg, err = ReadConfig(ctx, config)
		if err != nil {
			log.Errorf("failed to parse config. Err: %v", err)
			return cfg, err
		}
	}
	return cfg, nil
}

// GetDefaultNetPermission returns the default file share net permission.
func GetDefaultNetPermission() *NetPermissionConfig {
	return &NetPermissionConfig{
		RootSquash:  false,
		Permissions: vsanfstypes.VsanFileShareAccessTypeREAD_WRITE,
		Ips:         "*",
	}
}

// FromEnvToGC initializes the provided configuration object with values
// obtained from environment variables. If an environment variable is set
// for a property that's already initialized, the environment variable's value
// takes precedence.
func FromEnvToGC(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config object cannot be nil")
	}
	if v := os.Getenv("WCP_ENDPOINT"); v != "" {
		cfg.GC.Endpoint = v
	}
	if v := os.Getenv("WCP_PORT"); v != "" {
		cfg.GC.Port = v
	}
	if v := os.Getenv("WCP_TanzuKubernetesClusterUID"); v != "" {
		cfg.GC.TanzuKubernetesClusterUID = v
	}

	err := validateGCConfig(ctx, cfg)
	if err != nil {
		return err
	}
	return nil
}

// ReadGCConfig parses gc config file and stores it into GCConfig.
// Environment variables are also checked.
func ReadGCConfig(ctx context.Context, config io.Reader) (*Config, error) {
	if config == nil {
		return nil, fmt.Errorf("guest cluster config file is not present")
	}
	cfg := &Config{}
	if err := gcfg.FatalOnly(gcfg.ReadInto(cfg, config)); err != nil {
		return nil, err
	}
	// Env Vars should override config file entries if present.
	if err := FromEnvToGC(ctx, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// GetGCconfig returns Config from specified config file path.
func GetGCconfig(ctx context.Context, cfgPath string) (*Config, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("Get Guest Cluster config called with cfgPath: %s", cfgPath)
	var cfg *Config
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		// Config from Env var only.
		cfg = &Config{}
		if err := FromEnvToGC(ctx, cfg); err != nil {
			log.Errorf("Error reading guest cluster configuration file. Err: %v", err)
			return cfg, err
		}
	} else {
		config, err := os.Open(cfgPath)
		if err != nil {
			log.Errorf("failed to open %s. Err: %v", cfgPath, err)
			return cfg, err
		}
		cfg, err = ReadGCConfig(ctx, config)
		if err != nil {
			log.Errorf("failed to parse config. Err: %v", err)
			return cfg, err
		}
	}
	// Set default GCPort if Port is still empty.
	if cfg.GC.Port == "" {
		cfg.GC.Port = DefaultGCPort
	}
	return cfg, nil
}

// validateGCConfig validates that Guest Cluster config contains all the
// necessary fields.
func validateGCConfig(ctx context.Context, cfg *Config) error {
	log := logger.GetLogger(ctx)
	if cfg.GC.Endpoint == "" {
		log.Error(ErrMissingEndpoint)
		return ErrMissingEndpoint
	}
	if cfg.GC.TanzuKubernetesClusterUID == "" {
		log.Error(ErrMissingTanzuKubernetesClusterUID)
		return ErrMissingTanzuKubernetesClusterUID
	}
	return nil
}

// GetSupervisorNamespace returns the supervisor namespace in which this guest
// cluster is deployed.
func GetSupervisorNamespace(ctx context.Context) (string, error) {
	log := logger.GetLogger(ctx)
	const (
		namespaceFile = DefaultpvCSIProviderPath + "/namespace"
	)
	namespace, err := ioutil.ReadFile(namespaceFile)
	if err != nil {
		log.Errorf("Expected to load namespace from %s, but got err: %v", namespaceFile, err)
		return "", err
	}
	return string(namespace), nil
}

// GetClusterFlavor returns the cluster flavor based on the env variable set in
// the driver deployment file.
func GetClusterFlavor(ctx context.Context) (cnstypes.CnsClusterFlavor, error) {
	log := logger.GetLogger(ctx)
	// CLUSTER_FLAVOR is defined only in Supervisor and Guest cluster deployments.
	// If it is empty, it is implied that cluster flavor is Vanilla K8S.
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv("CLUSTER_FLAVOR"))
	if strings.TrimSpace(string(clusterFlavor)) == "" {
		return cnstypes.CnsClusterFlavorVanilla, nil
	} else if clusterFlavor == cnstypes.CnsClusterFlavorGuest ||
		clusterFlavor == cnstypes.CnsClusterFlavorWorkload ||
		clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
		return clusterFlavor, nil
	}
	errMsg := "unrecognized value set for CLUSTER_FLAVOR"
	log.Error(errMsg)
	return "", fmt.Errorf(errMsg)
}
