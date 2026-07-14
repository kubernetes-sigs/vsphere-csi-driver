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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pbmtypes "github.com/vmware/govmomi/pbm/types"
	"github.com/vmware/govmomi/vim25/types"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
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

// getVCenterInternal is the internal implementation that can be overridden for testing
var getVCenterInternal = func(ctx context.Context, manager *Manager) (*cnsvsphere.VirtualCenter, error) {
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

// getAvailabilityZoneClient returns AZ Client that can be overridden for testing
var getAvailabilityZoneClient = func() (dynamic.Interface, error) {
	cfg, err := crconfig.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes config. Err: %+v", err)
	}
	azClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create AvailabilityZone client using config. Err: %+v", err)
	}
	return azClient, nil
}

// GetVCenter returns VirtualCenter object from specified Manager object.
// Before returning VirtualCenter object, vcenter connection is established if
// session doesn't exist.
func GetVCenter(ctx context.Context, manager *Manager) (*cnsvsphere.VirtualCenter, error) {
	return getVCenterInternal(ctx, manager)
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
	validAccessModes []csi.VolumeCapability_AccessMode_Mode, volumeType string) error {
	// Validate if all capabilities of the volume are supported.
	for _, volCap := range volCaps {
		found := false
		for _, validAccessMode := range validAccessModes {
			if volCap.AccessMode.GetMode() == validAccessMode {
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
func ParseStorageClassParams(ctx context.Context, params map[string]string) (*StorageClassParams, error) {
	log := logger.GetLogger(ctx)
	scParams := &StorageClassParams{
		DatastoreURL:      "",
		StoragePolicyName: "",
	}
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
// returns true if any AZ in the supervisor has multiple clusters
func GetClusterComputeResourceMoIds(ctx context.Context) ([]string, bool, error) {
	log := logger.GetLogger(ctx)
	azClient, err := getAvailabilityZoneClient()
	if err != nil {
		return nil, false, fmt.Errorf("failed to create AvailabilityZone client using config. Err: %+v", err)
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
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get AvailabilityZone lists. err: %+v", err)
	}
	if len(azList.Items) == 0 {
		return nil, false, fmt.Errorf("could not find any AvailabilityZone")
	}

	var (
		clusterComputeResourceMoIds []string
		multipleClustersPerAZ       bool
	)
	for _, az := range azList.Items {
		clusterComputeResourceMoIdSlice, found, err := unstructured.NestedStringSlice(az.Object, "spec",
			"clusterComputeResourceMoIDs")
		if !found || err != nil {
			return nil, multipleClustersPerAZ, fmt.Errorf("failed to get ClusterComputeResourceMoIDs "+
				"from AvailabilityZone instance: %+v, err:%+v", az.Object, err)
		}
		if len(clusterComputeResourceMoIdSlice) > 1 {
			multipleClustersPerAZ = true
		}
		clusterComputeResourceMoIds = append(clusterComputeResourceMoIds, clusterComputeResourceMoIdSlice...)
	}
	return clusterComputeResourceMoIds, multipleClustersPerAZ, nil
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

func GetCNSVolumeInfoPatch(ctx context.Context, CapacityInMb int64, volumeId string) (map[string]interface{}, error) {
	log := logger.GetLogger(ctx)
	var patch map[string]interface{}
	if volumeId == "" {
		log.Errorf("VolumeID %q values cannot be empty", volumeId)
		return nil, logger.LogNewErrorf(log, "VolumeID values cannot be empty")
	}
	if CapacityInMb == -1 {
		log.Infof("Couldn't retrieve aggregated snapshot capacity for volume %q", volumeId)
		patch = map[string]interface{}{
			"spec": map[string]interface{}{
				"validaggregatedsnapshotsize": false,
			},
		}
	} else {
		aggregatedSnapshotSizeBytes := CapacityInMb * MbInBytes
		log.Infof("retrieved aggregated snapshot capacity %d for volume %q",
			CapacityInMb, volumeId)
		// In cases of concurrent request for vmsnapshot and volumesnapshot
		// we compare the snapshotlatestoperationcompletetime and based on that allow update to cnsvolumeinfo.
		// In cases where there is existing aggregatedSnapshotSizeBytes value and a new one is received
		// on patching CNSVolumeInfo, in cnsVolumeInfoCRUpdated() compares the old value and new value
		// based on result the exact differece is added to spu.
		patch = map[string]interface{}{
			"spec": map[string]interface{}{
				"validaggregatedsnapshotsize": true,
				"aggregatedsnapshotsize": resource.NewQuantity(
					aggregatedSnapshotSizeBytes, resource.BinarySI),
				"snapshotlatestoperationcompletetime": &metav1.Time{
					Time: time.Now(),
				},
			},
		}
	}
	return patch, nil
}

// CBTStateForNamespace reports the CBT state for the given namespace by
// inspecting the first CBTConfig CR (namespace-scoped singleton).
//
// Return values:
//   - active: true when the CBTConfig CR is configured and status.state is Active.
//   - configured: true when a CBTConfig CR exists in the namespace and its
//     status.state field has been set by the operator, meaning the config is
//     actively taking effect. False when no CR exists or the operator has not
//     yet reconciled status.state (nil).
//   - err: non-nil only when the API List call fails. A missing CR or an unset
//     status.state are not errors.
func CBTStateForNamespace(
	ctx context.Context,
	cbtClient ctrlclient.Client,
	pvcNamespace string,
) (active bool, configured bool, err error) {
	log := logger.GetLogger(ctx)

	var list cbtconfigv1alpha1.CBTConfigList
	if err = cbtClient.List(ctx, &list, ctrlclient.InNamespace(pvcNamespace)); err != nil {
		// CRD not yet installed or API version absent from discovery — treat as not configured.
		if apiMeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			log.Debugf("CBTConfig API unavailable in namespace %s, treating as not configured", pvcNamespace)
			return false, false, nil
		}
		return false, false, fmt.Errorf("failed to list CBTConfig CRs in namespace %s: %w", pvcNamespace, err)
	}

	if len(list.Items) == 0 {
		log.Debugf("No CBTConfig CRs found in namespace %s", pvcNamespace)
		return false, false, nil
	}

	// CBTConfig CR is implemented with namespace-scoped singleton pattern.
	if len(list.Items) > 1 {
		log.Warnf("found multiple CBTConfig CRs.")
	}

	for _, cbtConfig := range list.Items {
		if cbtConfig.Status != nil && cbtConfig.Status.State != nil {
			active = *cbtConfig.Status.State == cbtconfigv1alpha1.CBTStateActive
			log.Debugf("CBT active=%t (configured) for namespace %s", active, pvcNamespace)
			return active, true, nil
		}
	}

	log.Debugf("CBTConfig Status.State is not set for namespace %s", pvcNamespace)
	return false, false, nil
}

// SyncVolumeCBTState resolves the CBT intent for pvcNamespace via CBTStateForNamespace
// and applies the resulting active/inactive operation to every volume in volumeIDs.
// Both SetVolumeControlFlags and ClearVolumeControlFlags are idempotent, so the
// call is safe to repeat without querying current volume state.
//
// The function returns early (with debug logging) when the namespace has no
// active CBTConfig. Outcomes per volume are logged at Warnf on failure and
// Infof on success; callers do not need additional log lines around this call.
func SyncVolumeCBTState(
	ctx context.Context,
	cbtClient ctrlclient.Client,
	pvcNamespace string,
	volumeManager cnsvolume.Manager,
	volumeIDs ...string,
) {
	log := logger.GetLogger(ctx)

	active, configured, err := CBTStateForNamespace(ctx, cbtClient, pvcNamespace)
	if err != nil {
		log.Warnf("failed to get CBTConfig for namespace %s: %+v", pvcNamespace, err)
		return
	}
	if !configured {
		log.Debugf("CBTConfig not yet configured for namespace %s, skipping CBT flag change for volumes %v",
			pvcNamespace, volumeIDs)
		return
	}

	for _, volumeID := range volumeIDs {
		var applyErr error
		if active {
			applyErr = SetVolumeCbtFlagsUtil(ctx, volumeManager, volumeID)
		} else {
			applyErr = ClearVolumeCbtFlagsUtil(ctx, volumeManager, volumeID)
		}
		if applyErr != nil {
			log.Warnf("failed to set CBT %t for volume %s: %v", active, volumeID, applyErr)
		} else {
			log.Infof("Successfully set CBT %t for volume %s", active, volumeID)
		}
	}
}

// IsFVSVolumeHandle returns true if the supplied CSI volume handle was minted by
// the vSAN FileVolumeService (FVS) workflow on the supervisor. FVS volume IDs are
// formatted as "fv:<instance-namespace>:<filevolume-name>" (see FVSVolumeIDPrefix).
//
// This is the supervisor-side identification helper. The guest cluster cannot rely
// on the volume handle (the supervisor-side volume id is opaque on the guest) and
// must use IsFVSStorageClassName instead.
func IsFVSVolumeHandle(volumeHandle string) bool {
	return strings.HasPrefix(volumeHandle, FVSVolumeIDPrefix)
}

// ParseFVSVolumeHandle parses a CSI volume handle minted by the FVS workflow
// (formatted as "fv:<instance-namespace>:<filevolume-name>", see FVSVolumeIDPrefix)
// and returns the FileVolume CR's namespace and name. Returns an error if the
// volume handle does not carry the FVS prefix or is malformed (missing namespace
// or name component).
func ParseFVSVolumeHandle(volumeHandle string) (namespace, name string, err error) {
	if !IsFVSVolumeHandle(volumeHandle) {
		return "", "", fmt.Errorf("volume handle %q is not an FVS volume handle (missing %q prefix)",
			volumeHandle, FVSVolumeIDPrefix)
	}
	rest := strings.TrimPrefix(volumeHandle, FVSVolumeIDPrefix)
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("malformed FVS volume handle %q; expected %q<namespace>:<name>",
			volumeHandle, FVSVolumeIDPrefix)
	}
	return parts[0], parts[1], nil
}

// IsFVSStorageClassName returns true if the supplied storage class name is one of
// the vSAN file service storage classes that route provisioning through FVS on the
// supervisor (StorageClassVsanFileServicePolicy or
// StorageClassVsanFileServicePolicyLateBinding).
//
// This is the guest-cluster identification helper. The guest sees the supervisor
// PVC name as the volume handle (no FVS prefix) so it must look at the storage
// class to decide whether the volume is FVS-backed.
func IsFVSStorageClassName(storageClassName string) bool {
	switch storageClassName {
	case StorageClassVsanFileServicePolicy,
		StorageClassVsanFileServicePolicyLateBinding:
		return true
	default:
		return false
	}
}

// IsFVSPersistentVolumeClaim returns true when the PVC's storage class is one
// of the vSAN file service storage classes used for the new FileVolumeService
// workflow (see IsFVSStorageClassName). Applies to guest or supervisor PVC
// objects wherever the FVS storage class names are set.
func IsFVSPersistentVolumeClaim(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc == nil || pvc.Spec.StorageClassName == nil {
		return false
	}
	return IsFVSStorageClassName(*pvc.Spec.StorageClassName)
}

// BuildGuestPvcAnnotation constructs the value map stored on a supervisor PVC
// under the AnnKeyGuestClusterPvc annotation. It records the originating guest
// cluster identity (clusterId, clusterName) and, when known, the guest PVC name,
// namespace, and the bound supervisor volume name. Empty optional fields are
// omitted so the serialized annotation never carries blank values.
//
// This is the single source of truth for the annotation's key set; both the
// CreateVolume provisioning path and the full-sync upgrade backfill build their
// annotation through this helper so the two cannot drift.
func BuildGuestPvcAnnotation(clusterID, clusterName, pvcName, pvcNamespace, volumeName string) map[string]string {
	annot := map[string]string{
		GuestClusterAnnotKeyClusterID:   clusterID,
		GuestClusterAnnotKeyClusterName: clusterName,
	}
	if pvcName != "" {
		annot[GuestClusterAnnotKeyName] = pvcName
	}
	if pvcNamespace != "" {
		annot[GuestClusterAnnotKeyNamespace] = pvcNamespace
	}
	if volumeName != "" {
		annot[GuestClusterAnnotKeyVolumeName] = volumeName
	}
	return annot
}

// BuildGuestSnapshotAnnotation constructs the value map stored on a supervisor
// VolumeSnapshot under the AnnKeyGuestClusterSnapshot annotation. It records the
// originating guest cluster name and, when known, the guest VolumeSnapshot name,
// namespace, and the guest VolumeSnapshotContent name. Empty optional fields are
// omitted so the serialized annotation never carries blank values.
//
// This is the single source of truth for the annotation's key set; both the
// CreateSnapshot provisioning path and the full-sync upgrade backfill build their
// annotation through this helper so the two cannot drift.
func BuildGuestSnapshotAnnotation(clusterName, snapshotName, snapshotNamespace, vscName string) map[string]string {
	annot := map[string]string{
		GuestClusterAnnotKeyClusterName: clusterName,
	}
	if snapshotName != "" {
		annot[GuestClusterAnnotKeyName] = snapshotName
	}
	if snapshotNamespace != "" {
		annot[GuestClusterAnnotKeyNamespace] = snapshotNamespace
	}
	if vscName != "" {
		annot[GuestClusterAnnotKeyVSCName] = vscName
	}
	return annot
}

// IsvSANFileServiceMarkerPolicyName reports whether name is the K8s-compliant storage policy
// name for the vSAN File Service marker policy. Both ClusterStoragePolicyInfo.Name and
// StoragePolicyInfo.Name are K8s-compliant name, so obj.GetName() on either CR is a
// valid argument. Topology for this policy is derived from FVS instance namespaces rather
// than datastore/PBM compatibility.
func IsvSANFileServiceMarkerPolicyName(name string) bool {
	return name == StorageClassVsanFileServicePolicy
}

// changeIDPattern matches a well-formed vSphere CBT change-id: <change-epic-uuid>/<epoch-number>.
// Example: "52 05 a2 ce 6d 6d c9 59-d3 2e ad 19 e3 96 73 6d/5"
var changeIDPattern = regexp.MustCompile(`^[0-9a-f]{2}(?: [0-9a-f]{2}){7}-[0-9a-f]{2}(?: [0-9a-f]{2}){7}/\d+$`)

// IsValidChangeId reports whether changeID is a well-formed vSphere CBT
// change-id in the form <change-epic-uuid>/<epoch-number> expected at the CSI API boundary.
func IsValidChangeId(changeID string) bool {
	return changeIDPattern.MatchString(changeID)
}
