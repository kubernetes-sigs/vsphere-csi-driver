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
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/vim25/types"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	defaultK8sCloudOperatorServicePort = 10000
	MissingSnapshotAggregatedCapacity  = "csi.vsphere.missing-snapshot-aggregated-capacity"
)

var ErrAvailabilityZoneCRNotRegistered = errors.New("AvailabilityZone custom resource not registered")

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

// GetVCenters returns VirtualCenter object from specified Managers object.
// Before returning VirtualCenter objects, vcenter connection is established if
// session doesn't exist.
func GetVCenters(ctx context.Context, managers *Managers) ([]*cnsvsphere.VirtualCenter, error) {
	vcenters := make([]*cnsvsphere.VirtualCenter, 0)
	log := logger.GetLogger(ctx)
	for _, vcconfig := range managers.VcenterConfigs {
		vcenter, err := managers.VcenterManager.GetVirtualCenter(ctx, vcconfig.Host)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to get VirtualCenter instance for host: %q. err=%v", vcconfig.Host, err)
		}
		err = vcenter.Connect(ctx)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to connect to VirtualCenter host: %q. err=%v", vcconfig.Host, err)
		}
		vcenters = append(vcenters, vcenter)
	}
	return vcenters, nil
}

// GetVCenterFromVCHost returns VirtualCenter object from specified VC host.
// Before returning VirtualCenter objects, vcenter connection is established if
// session doesn't exist.
func GetVCenterFromVCHost(ctx context.Context, vCenterManager cnsvsphere.VirtualCenterManager,
	vCenterHost string) (*cnsvsphere.VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	vcenter, err := vCenterManager.GetVirtualCenter(ctx, vCenterHost)
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to get VirtualCenter instance for VC host: %q. Error: %v", vCenterHost, err)
	}
	err = vcenter.Connect(ctx)
	if err != nil {
		return nil, logger.LogNewErrorf(log,
			"failed to connect to VirtualCenter host: %q. Error: %v", vCenterHost, err)
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
			// For ReadWriteOnce access mode we only support following filesystems:
			// ext3, ext4, xfs for Linux and ntfs for Windows.
			if volCap.GetMount() != nil && !(volCap.GetMount().FsType == Ext4FsType ||
				volCap.GetMount().FsType == Ext3FsType || volCap.GetMount().FsType == XFSType ||
				strings.ToLower(volCap.GetMount().FsType) == NTFSFsType || volCap.GetMount().FsType == "") {
				return fmt.Errorf("fstype %s not supported for ReadWriteOnce volume creation",
					volCap.GetMount().FsType)
			}
		} else if volCap.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY ||
			volCap.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER ||
			volCap.AccessMode.Mode == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			// For ReadWriteMany or ReadOnlyMany access modes we only support nfs or nfs4 filesystem.
			// external-provisioner sets default fstype as ext4 when none is specified in StorageClass,
			// but we overwrite it to nfs4 while mounting the volume.
			if volCap.GetMount() != nil && !(volCap.GetMount().FsType == NfsV4FsType ||
				volCap.GetMount().FsType == NfsFsType || volCap.GetMount().FsType == Ext4FsType ||
				volCap.GetMount().FsType == "") {
				return fmt.Errorf("fstype %s not supported for ReadWriteMany or ReadOnlyMany volume creation",
					volCap.GetMount().FsType)
			} else if volCap.GetBlock() != nil {
				// Raw Block volumes are not supported with ReadWriteMany or ReadOnlyMany access modes,
				return fmt.Errorf("block volume mode is not supported for ReadWriteMany or ReadOnlyMany " +
					"volume creation")
			}
		}
	}
	return nil
}

// IsValidVolumeCapabilities helps validate the given volume capabilities
// based on volume type.
func IsValidVolumeCapabilities(ctx context.Context, volCaps []*csi.VolumeCapability) error {
	if IsFileVolumeRequest(ctx, volCaps) {
		return validateVolumeCapabilities(volCaps, MultiNodeVolumeCaps, FileVolumeType)
	}
	return validateVolumeCapabilities(volCaps, BlockVolumeCaps, BlockVolumeType)
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

// Contains check if item exist in list
func Contains(list []string, item string) bool {
	for _, x := range list {
		if x == item {
			return true
		}
	}
	return false
}

// GetClusterComputeResourceMoIds helps find ClusterComputeResourceMoIds from
// AvailabilityZone CRs on the supervisor cluster.
func GetClusterComputeResourceMoIds(ctx context.Context) ([]string, error) {
	log := logger.GetLogger(ctx)
	// Get a config to talk to the apiserver.
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes config. Err: %+v", err)
	}

	// Create a new AvailabilityZone client.
	azClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create AvailabilityZone client using config. Err: %+v", err)
	}
	azResource := schema.GroupVersionResource{
		Group: "topology.tanzu.vmware.com", Version: "v1alpha1", Resource: "availabilityzones"}
	// Get AvailabilityZone list.
	azList, err := azClient.Resource(azResource).List(ctx, metav1.ListOptions{})
	if err != nil {
		// If the AvailabilityZone CR is not registered in the
		// supervisor cluster, we receive NoKindMatchError. In such cases
		// return nil array for ClusterComputeResourceMoIds with no error.
		// AvailabilityZone CR is not registered when supervisor cluster was installed prior to vSphere 8.0.
		// Upgrading vSphere to 8.0 does not create zone CRs on existing supervisor clusters.
		_, ok := err.(*apiMeta.NoKindMatchError)
		if ok {
			log.Infof("AvailabilityZone CR is not registered on the cluster")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get AvailabilityZone lists. err: %+v", err)
	}
	if len(azList.Items) == 0 {
		return nil, fmt.Errorf("could not find any AvailabilityZone")
	}

	var (
		clusterComputeResourceMoIds []string
	)
	for _, az := range azList.Items {
		clusterComputeResourceMoIdSlice, found, err := unstructured.NestedStringSlice(az.Object, "spec",
			"clusterComputeResourceMoIDs")
		if !found || err != nil {
			return nil, fmt.Errorf("failed to get ClusterComputeResourceMoIDs "+
				"from AvailabilityZone instance: %+v, err:%+v", az.Object, err)
		}
		clusterComputeResourceMoIds = append(clusterComputeResourceMoIds, clusterComputeResourceMoIdSlice...)
	}
	return clusterComputeResourceMoIds, nil
}

// MergeMaps merges two maps to create a new one, the key-value pair from first
// are replaced with key-value pair of second
func MergeMaps(first map[string]string, second map[string]string) map[string]string {
	merged := make(map[string]string)
	for key, val := range first {
		merged[key] = val
	}
	for key, val := range second {
		merged[key] = val
	}
	return merged
}

// GetCSINamespace returns the namespace in which CSI driver is installed
func GetCSINamespace() string {
	return cnsconfig.GetCSINamespace()
}

func GetValidatedCNSVolumeInfoPatch(ctx context.Context,
	cnsSnapshotInfo *cnsvolume.CnsSnapshotInfo) (map[string]interface{}, error) {
	log := logger.GetLogger(ctx)
	var patch map[string]interface{}
	if cnsSnapshotInfo == nil || cnsSnapshotInfo.SourceVolumeID == "" {
		log.Errorf("VolumeID %q values cannot be empty", cnsSnapshotInfo.SourceVolumeID)
		return nil, logger.LogNewErrorf(log, "VolumeID values cannot be empty")
	}
	if cnsSnapshotInfo.AggregatedSnapshotCapacityInMb == -1 {
		log.Infof("Couldn't retrieve aggregated snapshot capacity for volume %q", cnsSnapshotInfo.SourceVolumeID)
		patch = map[string]interface{}{
			"spec": map[string]interface{}{
				"validaggregatedsnapshotsize": false,
			},
		}
	} else {
		aggregatedSnapshotSizeBytes := cnsSnapshotInfo.AggregatedSnapshotCapacityInMb * MbInBytes
		log.Infof("retrieved aggregated snapshot capacity %d for volume %q",
			cnsSnapshotInfo.AggregatedSnapshotCapacityInMb, cnsSnapshotInfo.SourceVolumeID)
		patch = map[string]interface{}{
			"spec": map[string]interface{}{
				"validaggregatedsnapshotsize": true,
				"aggregatedsnapshotsize": resource.NewQuantity(
					aggregatedSnapshotSizeBytes, resource.BinarySI),
				"snapshotlatestoperationcompletetime": &metav1.Time{
					Time: cnsSnapshotInfo.SnapshotLatestOperationCompleteTime},
			},
		}
	}
	return patch, nil
}

// ExtractVolumeIDFromPVName parses the VolumeID UUID from a volumeName
// generated by the external-provisioner (e.g., "pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx").
// Returns the UUID string if found, or an error otherwise.
func ExtractVolumeIDFromPVName(ctx context.Context, volumeName string) (string, error) {
	log := logger.GetLogger(ctx)

	re := regexp.MustCompile(`pvc-([0-9a-fA-F\-]+)`)
	matches := re.FindStringSubmatch(volumeName)

	if len(matches) != 2 {
		return "", logger.LogNewErrorf(log, "failed to extract UID from volumeName name: %q", volumeName)
	}
	return matches[1], nil
}
