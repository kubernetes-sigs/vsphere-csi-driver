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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/akutz/gofsutil"
	"github.com/container-storage-interface/spec/lib/go/csi"
	cnstypes "github.com/vmware/govmomi/cns/types"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/vim25/types"
	"golang.org/x/net/context"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
)

const (
	defaultK8sCloudOperatorServicePort = 10000
)

// GetVCenter returns VirtualCenter object from specified Manager object.
// Before returning VirtualCenter object, vcenter connection is established if
// session doesn't exist.
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

// GetUUIDFromProviderID Returns VM UUID from Node's providerID.
func GetUUIDFromProviderID(providerID string) string {
	return strings.TrimPrefix(providerID, ProviderPrefix)
}

// FormatDiskUUID removes any spaces and hyphens in UUID.
// Example UUID input is 42375390-71f9-43a3-a770-56803bcd7baa and output after
// format is 4237539071f943a3a77056803bcd7baa.
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

// GetLabelsMapFromKeyValue creates a  map object from given parameter.
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
// Defaults to nfs4 for file volume and ext4 for block volume when empty string
// is observed. This function also ignores default ext4 fstype supplied by
// external-provisioner when none is specified in the StorageClass
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

// IsVolumeReadOnly checks the access mode in Volume Capability and decides
// if volume is readonly or not.
func IsVolumeReadOnly(capability *csi.VolumeCapability) bool {
	accMode := capability.GetAccessMode().GetMode()
	ro := false
	if accMode == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY ||
		accMode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		ro = true
	}
	return ro
}

// validateVolumeCapabilities validates the access mode in given volume
// capabilities in validAccessModes.
func validateVolumeCapabilities(volCaps []*csi.VolumeCapability,
	validAccessModes []csi.VolumeCapability_AccessMode, volumeType string) error {
	// Validate if all capabilities of the volume are supported.
	for _, volCap := range volCaps {
		found := false
		for _, validAccessMode := range validAccessModes {
			if volCap.AccessMode.GetMode() == validAccessMode.GetMode() {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%s access mode is not supported for %q volumes",
				csi.VolumeCapability_AccessMode_Mode_name[int32(volCap.AccessMode.GetMode())], volumeType)
		}
		if volCap.AccessMode.Mode == csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER {
			if volCap.GetMount() != nil && (volCap.GetMount().FsType == NfsV4FsType ||
				volCap.GetMount().FsType == NfsFsType) {
				return fmt.Errorf("NFS fstype not supported for ReadWriteOnce volume creation")
			}
		}
	}
	return nil
}

// IsValidVolumeCapabilities helps validate the given volume capabilities
// based on volume type.
func IsValidVolumeCapabilities(ctx context.Context, volCaps []*csi.VolumeCapability) error {
	if IsFileVolumeRequest(ctx, volCaps) {
		return validateVolumeCapabilities(volCaps, FileVolumeCaps, FileVolumeType)
	}
	return validateVolumeCapabilities(volCaps, BlockVolumeCaps, BlockVolumeType)
}

// IsFileVolumeMount loops through the list of mount points and
// checks if the target path mount point is a file volume type or not.
// Returns an error if the target path is not found in the mount points.
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
	// Target path mount point not found in list of mounts.
	return false, fmt.Errorf("could not find target path %q in list of mounts", target)
}

// IsTargetInMounts checks if the given target path is present in list of
// mount points.
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

// ParseStorageClassParams parses the params in the CSI CreateVolumeRequest API
// call back to StorageClassParams structure.
func ParseStorageClassParams(ctx context.Context, params map[string]string,
	csiMigrationFeatureState bool) (*StorageClassParams, error) {
	log := logger.GetLogger(ctx)
	scParams := &StorageClassParams{
		DatastoreURL:      "",
		StoragePolicyName: "",
	}
	if !csiMigrationFeatureState {
		for param, value := range params {
			param = strings.ToLower(param)
			if param == AttributeDatastoreURL {
				scParams.DatastoreURL = value
			} else if param == AttributeStoragePolicyName {
				scParams.StoragePolicyName = value
			} else if param == AttributeFsType {
				log.Warnf("param 'fstype' is deprecated, please use 'csi.storage.k8s.io/fstype' instead")
			} else {
				return nil, fmt.Errorf("invalid param: %q and value: %q", param, value)
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
			} else {
				otherParams[param] = value
			}
		}
		// check otherParams belongs to in-tree migrated Parameters.
		if scParams.CSIMigration == "true" {
			for param, value := range otherParams {
				param = strings.ToLower(param)
				if param == DatastoreMigrationParam {
					scParams.Datastore = value
				} else if param == DiskFormatMigrationParam && value == "thin" {
					continue
				} else if param == HostFailuresToTolerateMigrationParam ||
					param == ForceProvisioningMigrationParam || param == CacheReservationMigrationParam ||
					param == DiskstripesMigrationParam || param == ObjectspacereservationMigrationParam ||
					param == IopslimitMigrationParam {
					return nil, fmt.Errorf("vSphere CSI driver does not support creating volume using "+
						"in-tree vSphere volume plugin parameter key:%v, value:%v", param, value)
				} else {
					return nil, fmt.Errorf("invalid parameter. key:%v, value:%v", param, value)
				}
			}
		} else {
			if len(otherParams) != 0 {
				return nil, fmt.Errorf("invalid parameters :%v", otherParams)
			}
		}
	}
	return scParams, nil
}

// GetConfigPath returns ConfigPath depending on the environment variable
// specified and the cluster flavor set.
func GetConfigPath(ctx context.Context) string {
	var cfgPath string
	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	if strings.TrimSpace(string(clusterFlavor)) == "" {
		clusterFlavor = cnstypes.CnsClusterFlavorVanilla
	}
	if clusterFlavor == cnstypes.CnsClusterFlavorGuest {
		// Config path for Guest Cluster.
		cfgPath = os.Getenv(cnsconfig.EnvGCConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultGCConfigPath
		}
	} else {
		// Config path for SuperVisor and Vanilla Cluster.
		cfgPath = os.Getenv(cnsconfig.EnvVSphereCSIConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultCloudConfigPath
		}
	}
	return cfgPath
}

// GetConfig loads configuration from secret and returns config object.
func GetConfig(ctx context.Context) (*cnsconfig.Config, error) {
	var cfg *cnsconfig.Config
	var err error
	cfgPath := GetConfigPath(ctx)
	if cfgPath == cnsconfig.DefaultGCConfigPath {
		cfg, err = cnsconfig.GetGCconfig(ctx, cfgPath)
		if err != nil {
			return cfg, err
		}
	} else {
		cfg, err = cnsconfig.GetCnsconfig(ctx, cfgPath)
		if err != nil {
			return cfg, err
		}
	}
	return cfg, err
}

// InitConfigInfo initializes the ConfigurationInfo struct.
func InitConfigInfo(ctx context.Context) (*cnsconfig.ConfigurationInfo, error) {
	log := logger.GetLogger(ctx)
	cfg, err := GetConfig(ctx)
	if err != nil {
		log.Errorf("failed to read config. Error: %+v", err)
		return nil, err
	}
	configInfo := &cnsconfig.ConfigurationInfo{
		Cfg: cfg,
	}
	return configInfo, nil
}

// GetK8sCloudOperatorServicePort return the port to connect the
// K8sCloudOperator gRPC service.
// If environment variable POD_LISTENER_SERVICE_PORT is set and valid,
// return the interval value read from environment variable.
// Otherwise, use the default port.
func GetK8sCloudOperatorServicePort(ctx context.Context) int {
	k8sCloudOperatorServicePort := defaultK8sCloudOperatorServicePort
	log := logger.GetLogger(ctx)
	if v := os.Getenv("POD_LISTENER_SERVICE_PORT"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Connecting to K8s Cloud Operator Service on port set in env variable "+
					"POD_LISTENER_SERVICE_PORT %s is equal or less than 0, will use the default port %d",
					v, defaultK8sCloudOperatorServicePort)
			} else {
				k8sCloudOperatorServicePort = value
				log.Infof("Connecting to K8s Cloud Operator Service on port %d", k8sCloudOperatorServicePort)
			}
		} else {
			log.Warnf("Connecting to K8s Cloud Operator Service on port set in env variable "+
				"POD_LISTENER_SERVICE_PORT %s is invalid, will use the default port %d",
				v, defaultK8sCloudOperatorServicePort)
		}
	}
	return k8sCloudOperatorServicePort
}

// ConvertVolumeHealthStatus convert the volume health status into
// accessible/inaccessible status.
func ConvertVolumeHealthStatus(ctx context.Context, volID string, volHealthStatus string) (string, error) {
	log := logger.GetLogger(ctx)
	switch volHealthStatus {
	case string(pbmtypes.PbmHealthStatusForEntityRed):
		return VolHealthStatusInaccessible, nil
	case string(pbmtypes.PbmHealthStatusForEntityGreen):
		return VolHealthStatusAccessible, nil
	case string(pbmtypes.PbmHealthStatusForEntityYellow):
		return VolHealthStatusAccessible, nil
	case string(pbmtypes.PbmHealthStatusForEntityUnknown):
		return string(pbmtypes.PbmHealthStatusForEntityUnknown), nil
	default:
		// NOTE: volHealthStatus is not set by SPBM in this case.
		// This implies the volume does not exist any more.
		// Set health annotation to "Inaccessible" so that the caller
		// can make appropriate reactions based on this status.
		log.Debugf("Volume health is not set for volume: %s", volID)
		return VolHealthStatusInaccessible, nil
	}
}

// ParseCSISnapshotID parses the SnapshotID from CSI RPC such as DeleteSnapshot, CreateVolume from snapshot
// into a pair of CNS VolumeID and CNS SnapshotID.
func ParseCSISnapshotID(csiSnapshotID string) (string, string, error) {
	if csiSnapshotID == "" {
		return "", "", errors.New("csiSnapshotID from the input is empty")
	}

	// The expected format of the SnapshotId in the DeleteSnapshotRequest is,
	// a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+" sign.
	// That is, a string of "<UUID>+<UUID>". Decompose csiSnapshotID based on the expected format.
	IDs := strings.Split(csiSnapshotID, VSphereCSISnapshotIdDelimiter)
	if len(IDs) != 2 {
		return "", "", fmt.Errorf("unexpected format in csiSnapshotID: %v", csiSnapshotID)
	}

	cnsVolumeID := IDs[0]
	cnsSnapshotID := IDs[1]

	return cnsVolumeID, cnsSnapshotID, nil
}
