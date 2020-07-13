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

package common

import (
	"fmt"
	"os"
	"strings"

	csictx "github.com/rexray/gocsi/context"
	cnstypes "github.com/vmware/govmomi/cns/types"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"

	"github.com/akutz/gofsutil"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/vmware/govmomi/vim25/types"
	"golang.org/x/net/context"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
)

// GetVCenter returns VirtualCenter object from specified Manager object.
// Before returning VirtualCenter object, vcenter connection is established if session doesn't exist.
func GetVCenter(ctx context.Context, manager *Manager) (*cnsvsphere.VirtualCenter, error) {
	var err error
	log := logger.GetLogger(ctx)
	vcenter, err := manager.VcenterManager.GetVirtualCenter(ctx, manager.VcenterConfig.Host)
	if err != nil {
		log.Errorf("failed to get VirtualCenter instance for host: %q. err=%v", manager.VcenterConfig.Host, err)
		return nil, err
	}
	err = vcenter.Connect(ctx)
	if err != nil {
		log.Errorf("failed to connect to VirtualCenter host: %q. err=%v", manager.VcenterConfig.Host, err)
		return nil, err
	}
	return vcenter, nil
}

// GetUUIDFromProviderID Returns VM UUID from Node's providerID
func GetUUIDFromProviderID(providerID string) string {
	return strings.TrimPrefix(providerID, ProviderPrefix)
}

// FormatDiskUUID removes any spaces and hyphens in UUID
// Example UUID input is 42375390-71f9-43a3-a770-56803bcd7baa and output after format is 4237539071f943a3a77056803bcd7baa
func FormatDiskUUID(uuid string) string {
	uuidwithNoSpace := strings.Replace(uuid, " ", "", -1)
	uuidWithNoHypens := strings.Replace(uuidwithNoSpace, "-", "", -1)
	return strings.ToLower(uuidWithNoHypens)
}

// RoundUpSize calculates how many allocation units are needed to accommodate
// a volume of given size.
func RoundUpSize(volumeSizeBytes int64, allocationUnitBytes int64) int64 {
	roundedUp := volumeSizeBytes / allocationUnitBytes
	if volumeSizeBytes%allocationUnitBytes > 0 {
		roundedUp++
	}
	return roundedUp
}

// GetLabelsMapFromKeyValue creates a  map object from given parameter
func GetLabelsMapFromKeyValue(labels []types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// IsFileVolumeRequest checks whether the request is to create a CNS file volume.
func IsFileVolumeRequest(ctx context.Context, capabilities []*csi.VolumeCapability) bool {
	for _, capability := range capabilities {
		if capability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
			capability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER ||
			capability.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			return true
		}
	}
	return false
}

// GetVolumeCapabilityFsType retrieves fstype from VolumeCapability.
// Defaults to nfs4 for file volume and ext4 for block volume when empty string is observed.
// This function also ignores default ext4 fstype supplied by external-provisioner when none is
// specified in the StorageClass
func GetVolumeCapabilityFsType(ctx context.Context, capability *csi.VolumeCapability) string {
	log := logger.GetLogger(ctx)
	fsType := strings.ToLower(capability.GetMount().GetFsType())
	log.Debugf("FsType received from Volume Capability: %q", fsType)
	isFileVolume := IsFileVolumeRequest(ctx, []*csi.VolumeCapability{capability})
	if isFileVolume && (fsType == "" || fsType == "ext4") {
		log.Infof("empty string or ext4 fstype observed for file volume. Defaulting to: %s", NfsV4FsType)
		fsType = NfsV4FsType
	} else if !isFileVolume && fsType == "" {
		log.Infof("empty string fstype observed for block volume. Defaulting to: %s", Ext4FsType)
		fsType = Ext4FsType
	}
	return fsType
}

// IsVolumeReadOnly checks the access mode in Volume Capability and decides if volume is readonly or not
func IsVolumeReadOnly(capability *csi.VolumeCapability) bool {
	accMode := capability.GetAccessMode().GetMode()
	ro := false
	if accMode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY ||
		accMode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		ro = true
	}
	return ro
}

// validateVolumeCapabilities validates the access mode in given volume capabilities in validAccessModes.
func validateVolumeCapabilities(volCaps []*csi.VolumeCapability, validAccessModes []csi.VolumeCapability_AccessMode) bool {
	// Validate if all capabilities of the volume
	// are supported.
	for _, volCap := range volCaps {
		found := false
		for _, validAccessMode := range validAccessModes {
			if volCap.AccessMode.GetMode() == validAccessMode.GetMode() {
				found = true
				break
			}
		}
		if !found {
			return false
		}
		if volCap.AccessMode.Mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			if volCap.GetMount() != nil && (volCap.GetMount().FsType == NfsV4FsType || volCap.GetMount().FsType == NfsFsType) {
				return false
			}
		}
	}
	return true
}

// IsValidVolumeCapabilities helps validate the given volume capabilities based on volume type.
func IsValidVolumeCapabilities(ctx context.Context, volCaps []*csi.VolumeCapability) bool {
	if IsFileVolumeRequest(ctx, volCaps) {
		return validateVolumeCapabilities(volCaps, FileVolumeCaps)
	}
	return validateVolumeCapabilities(volCaps, BlockVolumeCaps)
}

// IsFileVolumeMount loops through the list of mount points and
// checks if the target path mount point is a file volume type or not
// Returns an error if the target path is not found in the mount points
func IsFileVolumeMount(ctx context.Context, target string, mnts []gofsutil.Info) (bool, error) {
	log := logger.GetLogger(ctx)
	for _, m := range mnts {
		if m.Path == target {
			if m.Type == NfsFsType || m.Type == NfsV4FsType {
				log.Debug("IsFileVolumeMount: Found file volume")
				return true, nil
			}
			log.Debug("IsFileVolumeMount: Found block volume")
			return false, nil
		}
	}
	// Target path mount point not found in list of mounts
	return false, fmt.Errorf("could not find target path %q in list of mounts", target)
}

// IsTargetInMounts checks if the given target path is present in list of mount points
func IsTargetInMounts(ctx context.Context, target string, mnts []gofsutil.Info) bool {
	log := logger.GetLogger(ctx)
	for _, m := range mnts {
		if m.Path == target {
			log.Debugf("Found target %q in list of mounts", target)
			return true
		}
	}
	log.Debugf("Target %q not found in list of mounts", target)
	return false
}

// ParseStorageClassParams parses the params in the CSI CreateVolumeRequest API call back
// to StorageClassParams structure.
func ParseStorageClassParams(ctx context.Context, params map[string]string) (*StorageClassParams, error) {
	log := logger.GetLogger(ctx)
	scParams := &StorageClassParams{
		DatastoreURL:      "",
		StoragePolicyName: "",
	}
	if !CSIMigrationFeatureEnabled {
		for param, value := range params {
			param = strings.ToLower(param)
			if param == AttributeDatastoreURL {
				scParams.DatastoreURL = value
			} else if param == AttributeStoragePolicyName {
				scParams.StoragePolicyName = value
			} else if param == AttributeFsType {
				log.Warnf("param 'fstype' is deprecated, please use 'csi.storage.k8s.io/fstype' instead")
			} else {
				return nil, fmt.Errorf("Invalid param: %q and value: %q", param, value)
			}
		}
	} else {
		otherParams := make(map[string]string)
		for param, value := range params {
			param = strings.ToLower(param)
			if param == AttributeDatastoreURL {
				scParams.DatastoreURL = value
			} else if param == AttributeStoragePolicyName {
				scParams.StoragePolicyName = value
			} else if param == AttributeFsType {
				log.Warnf("param 'fstype' is deprecated, please use 'csi.storage.k8s.io/fstype' instead")
			} else if param == CSIMigrationParams {
				scParams.CSIMigration = value
				log.Infof("vSphere CSI Driver supports StoragePolicyName, Datastore and fsType parameters supplied from legacy in-tree provisioner. All other parameters supplied in csimigrationparams will be dropped.")
			} else {
				otherParams[param] = value
			}
		}

		// check otherParams belongs to in-tree migrated Parameters
		if scParams.CSIMigration == "true" {
			for param, value := range otherParams {
				param = strings.ToLower(param)
				if param == DatastoreMigrationParam {
					scParams.Datastore = value
				} else if param != DiskFormatMigrationParam && param != HostFailuresToTolerateMigrationParam &&
					param != ForceProvisioningMigrationParam && param != CacheReservationMigrationParam &&
					param != DiskstripesMigrationParam && param != ObjectspacereservationMigrationParam &&
					param != IopslimitMigrationParam {
					return nil, fmt.Errorf("Invalid parameter key:%v, value:%v", param, value)
				}
			}
		} else {
			if len(otherParams) != 0 {
				return nil, fmt.Errorf("Invalid parameters :%v", otherParams)
			}
		}
	}
	return scParams, nil
}

// GetConfigPath returns ConfigPath depending on the environment variable specified and the cluster flavor set
func GetConfigPath(ctx context.Context) string {
	var cfgPath string
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	if strings.TrimSpace(string(clusterFlavor)) == "" {
		clusterFlavor = cnstypes.CnsClusterFlavorVanilla
	}
	if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Config path for Guest Cluster
		cfgPath = csictx.Getenv(ctx, cnsconfig.EnvGCConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultGCConfigPath
		}
	} else {
		// Config path for SuperVisor and Vanilla Cluster
		cfgPath = csictx.Getenv(ctx, cnsconfig.EnvVSphereCSIConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultCloudConfigPath
		}
	}
	return cfgPath
}

// GetFeatureStatesConfigPath returns CSI feature states config path depending on the environment variable specified and the cluster flavor set
func GetFeatureStatesConfigPath(ctx context.Context) string {
	var featureStatesCfgPath string
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	featureStatesCfgPath = csictx.Getenv(ctx, cnsconfig.EnvFeatureStates)
	if clusterFlavor == cnstypes.CnsClusterFlavorVanilla || clusterFlavor == "" {
		if featureStatesCfgPath == "" {
			featureStatesCfgPath = cnsconfig.DefaultVanillaFeatureStateConfigPath
		}
	} else {
		return ""
	}
	return featureStatesCfgPath
}

// GetFeatureStates loads feature states information from csi-feature-states configmap
func GetFeatureStates(ctx context.Context, cfg *cnsconfig.Config) error {
	log := logger.GetLogger(ctx)
	featureStatesCfgPath := GetFeatureStatesConfigPath(ctx)
	err := cnsconfig.GetFeatureStatesConfig(ctx, featureStatesCfgPath, cfg)
	if err != nil {
		log.Errorf("could not load the feature states from %s with err: %+v", featureStatesCfgPath, err)
		return err
	}
	return err
}

// GetConfig loads configuration from secret and returns config object
func GetConfig(ctx context.Context) (*cnsconfig.Config, error) {
	log := logger.GetLogger(ctx)
	var cfg *cnsconfig.Config
	var err error
	cfgPath := GetConfigPath(ctx)
	if cfgPath == cnsconfig.DefaultGCConfigPath {
		cfg, err = cnsconfig.GetGCconfig(ctx, cfgPath)
	} else {
		cfg, err = cnsconfig.GetCnsconfig(ctx, cfgPath)
	}
	// Reading feature states information
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	if clusterFlavor == cnstypes.CnsClusterFlavorVanilla || clusterFlavor == "" {
		err := GetFeatureStates(ctx, cfg)
		if err != nil {
			log.Errorf("error while reading the feature states. Error: %+v", err)
			return cfg, err
		}
	}
	return cfg, err
}
