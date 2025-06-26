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

import "time"

const (
	// MbInBytes is the number of bytes in one mebibyte.
	MbInBytes = int64(1024 * 1024)

	// GbInBytes is the number of bytes in one gibibyte.
	GbInBytes = int64(1024 * 1024 * 1024)

	// DefaultGbDiskSize is the default disk size in gibibytes.
	// TODO: will make the DefaultGbDiskSize configurable in the future.
	DefaultGbDiskSize = int64(10)

	// DiskTypeBlockVolume is the value for PersistentVolume's attribute "type".
	DiskTypeBlockVolume = "vSphere CNS Block Volume"

	// DiskTypeFileVolume is the value for PersistentVolume's attribute "type".
	DiskTypeFileVolume = "vSphere CNS File Volume"

	// AttributeDiskType is a PersistentVolume's attribute.
	AttributeDiskType = "type"

	// AttributeDatastoreURL represents URL of the datastore in the StorageClass.
	// For Example: DatastoreURL: "ds:///vmfs/volumes/5c9bb20e-009c1e46-4b85-0200483b2a97/".
	AttributeDatastoreURL = "datastoreurl"

	// AttributeStoragePolicyName represents name of the Storage Policy in the
	// Storage Class.
	// For Example: StoragePolicy: "vSAN Default Storage Policy".
	AttributeStoragePolicyName = "storagepolicyname"

	// AttributeStoragePolicyID represents Storage Policy Id in the Storage Classs.
	// For Example: StoragePolicyId: "251bce41-cb24-41df-b46b-7c75aed3c4ee".
	AttributeStoragePolicyID = "storagepolicyid"

	// AttributeSupervisorStorageClass represents name of the Storage Class.
	// For example: StorageClassName: "silver".
	AttributeSupervisorStorageClass = "svstorageclass"

	// AttributeStorageTopologyType is a storageClass parameter.
	// It represents a zonal or a crossZonal volume provisioning.
	// For example: StorageTopologyType: "zonal"
	AttributeStorageTopologyType = "storagetopologytype"

	// AttributeFsType represents filesystem type in the Storage Classs.
	// For Example: FsType: "ext4".
	AttributeFsType = "fstype"

	// AttributeStoragePool represents name of the StoragePool on which to place
	// the PVC. For example: StoragePool: "storagepool-vsandatastore".
	AttributeStoragePool = "storagepool"

	// AttributeHostLocal represents the presence of HostLocal functionality in
	// the given storage policy. For Example: HostLocal: "True".
	AttributeHostLocal = "hostlocal"

	// AttributePvName represents the name of the PV
	AttributePvName = "csi.storage.k8s.io/pv/name"

	// AttributePvcName represents the name of the PVC
	AttributePvcName = "csi.storage.k8s.io/pvc/name"

	// AttributePvcNamespace represents the namespace of the PVC
	AttributePvcNamespace = "csi.storage.k8s.io/pvc/namespace"

	// AttributeStorageClassName represents name of the Storage Class.
	AttributeStorageClassName = "csi.storage.k8s.io/sc/name"

	// HostMoidAnnotationKey represents the Node annotation key that has the value
	// of VC's ESX host moid of this node.
	HostMoidAnnotationKey = "vmware-system-esxi-node-moid"

	// Ext4FsType represents the default filesystem type for block volume.
	Ext4FsType = "ext4"

	// Ext3FsType represents the ext3 filesystem type for block volume.
	Ext3FsType = "ext3"

	// XFSType represents the xfs filesystem type for block volume.
	XFSType = "xfs"

	// NfsV4FsType represents nfs4 mount type.
	NfsV4FsType = "nfs4"

	// NTFSFsType represents ntfs
	NTFSFsType = "ntfs"

	// NfsFsType represents nfs mount type.
	NfsFsType = "nfs"

	// ProviderPrefix is the prefix used for the ProviderID set on the node.
	// Example: vsphere://4201794a-f26b-8914-d95a-edeb7ecc4a8f
	ProviderPrefix = "vsphere://"

	// AttributeFirstClassDiskUUID is the SCSI Disk Identifier.
	AttributeFirstClassDiskUUID = "diskUUID"

	// AttributeVmUUID is the vmUUID to which volume is attached to.
	AttributeVmUUID = "vmUUID"

	// AttributeFakeAttached is the flag that indicates if a volume is fake
	// attached.
	AttributeFakeAttached = "fake-attach"

	// BlockVolumeType is the VolumeType for CNS Volume.
	BlockVolumeType = "BLOCK"

	// FileVolumeType is the VolumeType for CNS File Share Volume.
	FileVolumeType = "FILE"

	// UnknownVolumeType is assigned to CNS volumes whose type couldn't be determined.
	UnknownVolumeType = "UNKNOWN"

	// Nfsv4AccessPointKey is the key for NFSv4 access point.
	Nfsv4AccessPointKey = "NFSv4.1"

	// Nfsv4AccessPoint is the access point of file volume.
	Nfsv4AccessPoint = "Nfsv4AccessPoint"

	// MinSupportedVCenterMajor is the minimum, major version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterMajor int = 6

	// MinSupportedVCenterMinor is the minimum, minor version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterMinor int = 7

	// MinSupportedVCenterPatch is the minimum patch version of vCenter
	// on which CNS is supported.
	MinSupportedVCenterPatch int = 3

	// SnapshotSupportedVCenterMajor is the minimum major version of vCenter
	// on which Snapshot feature is supported.
	SnapshotSupportedVCenterMajor int = 7

	// SnapshotSupportedVCenterMinor is the minimum minor version of vCenter
	// on which Snapshot feature is supported.
	SnapshotSupportedVCenterMinor int = 0

	// SnapshotSupportedVCenterPatch is the minimum patch version of vCenter
	// on which Snapshot feature is supported.
	SnapshotSupportedVCenterPatch int = 3

	// VSphere67u3Version is the minimum vSphere version to use Vslm APIs
	// to support volume migration feature.
	VSphere67u3Version string = "6.7.3"

	// VSphere7Version is the maximum vSphere version to use Vslm APIs
	// to support volume migration feature.
	VSphere7Version string = "7.0.0"

	// VSphere8VersionMajorInt indicates the major version value in integer
	VSphere8VersionMajorInt int = 8

	// VSphere67u3lBuildInfo is the build number for vCenter in 6.7 Update 3l
	// GA bits.
	VSphere67u3lBuildInfo int = 17137327

	// VsanAffinityKey is the profile param key to indicate which node the FCD
	// should be affinitized to.
	VsanAffinityKey string = "VSAN/affinity/affinity"

	// VsanAffinityMandatory is the profile param key to turn on affinity of
	// the volume to a specific ESX host.
	VsanAffinityMandatory string = "VSAN/affinityMandatory/affinityMandatory"

	// VsanMigrateForDecom is the profile param key to set the migrate mode
	// for the volume.
	VsanMigrateForDecom string = "VSAN/migrateForDecom/migrateForDecom"

	// VsanDatastoreType is the string to identify datastore type as vsan.
	VsanDatastoreType string = "vsan"

	// CSIMigrationParams helps identify if volume creation is requested by
	// in-tree storageclass or CSI storageclass.
	CSIMigrationParams = "csimigration"

	// AttributeInitialVolumeFilepath represents the path of volume where volume
	// is created.
	AttributeInitialVolumeFilepath = "initialvolumefilepath"

	// DatastoreMigrationParam is used to supply datastore name for Volume
	// provisioning.
	DatastoreMigrationParam = "datastore-migrationparam"

	// DiskFormatMigrationParam supplies disk foramt (thin, thick, zeoredthick)
	// for Volume provisioning.
	DiskFormatMigrationParam = "diskformat-migrationparam"

	// HostFailuresToTolerateMigrationParam is raw vSAN Policy Parameter.
	HostFailuresToTolerateMigrationParam = "hostfailurestotolerate-migrationparam"

	// ForceProvisioningMigrationParam is raw vSAN Policy Parameter.
	ForceProvisioningMigrationParam = "forceprovisioning-migrationparam"

	// CacheReservationMigrationParam is raw vSAN Policy Parameter.
	CacheReservationMigrationParam = "cachereservation-migrationparam"

	// DiskstripesMigrationParam is raw vSAN Policy Parameter.
	DiskstripesMigrationParam = "diskstripes-migrationparam"

	// ObjectspacereservationMigrationParam is raw vSAN Policy Parameter.
	ObjectspacereservationMigrationParam = "objectspacereservation-migrationparam"

	// IopslimitMigrationParam is raw vSAN Policy Parameter.
	IopslimitMigrationParam = "iopslimit-migrationparam"

	// AnnMigratedTo annotation is added to a PVC and PV that is supposed to be
	// provisioned/deleted by its corresponding CSI driver.
	AnnMigratedTo = "pv.kubernetes.io/migrated-to"

	// AnnBetaStorageProvisioner annotation is added to a PVC that is supposed to
	// be dynamically provisioned. Its value is name of volume plugin that is
	// supposed to provision a volume for this PVC.
	AnnBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"

	// AnnStorageProvisioner annotation is added to a PVC that is supposed to
	// be dynamically provisioned. Its value is name of volume plugin that is
	// supposed to provision a volume for this PVC.
	AnnStorageProvisioner = "volume.kubernetes.io/storage-provisioner"

	// VSphereCSIDriverName vSphere CSI driver name
	VSphereCSIDriverName = "csi.vsphere.vmware.com"

	// AnnDynamicallyProvisioned annotation is added to a PV that has been
	// dynamically provisioned by Kubernetes. Its value is name of volume plugin
	// that created the volume. It serves both user (to show where a PV comes
	// from) and Kubernetes (to recognize dynamically provisioned PVs in its
	// decisions).
	AnnDynamicallyProvisioned = "pv.kubernetes.io/provisioned-by"

	// InTreePluginName is the name of vsphere cloud provider in kubernetes.
	InTreePluginName = "kubernetes.io/vsphere-volume"

	// DsPriv is the privilege need to write on that datastore.
	DsPriv = "Datastore.FileManagement"

	// SysReadPriv is the privilege to view an entity.
	SysReadPriv = "System.Read"

	// HostConfigStoragePriv is the privilege for file volumes.
	HostConfigStoragePriv = "Host.Config.Storage"

	// AnnVolumeHealth is the key for HealthStatus annotation on volume claim.
	AnnVolumeHealth = "volumehealth.storage.kubernetes.io/health"

	// AnnFakeAttached is the key for fake attach annotation on volume claim.
	AnnFakeAttached = "csi.vmware.com/fake-attached"

	// VolHealthStatusAccessible is volume health status for accessible volume.
	VolHealthStatusAccessible = "accessible"

	// VolHealthStatusInaccessible is volume health status for inaccessible volume.
	VolHealthStatusInaccessible = "inaccessible"

	// AnnIgnoreInaccessiblePV is annotation key on volume claim to indicate
	// if inaccessible PV can be fake attached.
	AnnIgnoreInaccessiblePV = "pv.attach.kubernetes.io/ignore-if-inaccessible"

	// TriggerCsiFullSyncCRName is the instance name of TriggerCsiFullSync
	// All other names will be rejected by TriggerCsiFullSync controller.
	TriggerCsiFullSyncCRName = "csifullsync"

	// QuerySnapshotLimit is the maximum number of snapshots that can be retrieved per QuerySnapshot call.
	// The 128 size limit is specified by CNS QuerySnapshot API.
	QuerySnapshotLimit = int64(128)

	// VSphereCSISnapshotIdDelimiter is the delimiter for concatenating CNS VolumeID and CNS SnapshotID
	VSphereCSISnapshotIdDelimiter = "+"

	// TopologyLabelsDomain is the domain name used to identify user-defined
	// topology labels applied on the node by vSphere CSI driver.
	TopologyLabelsDomain = "topology.csi.vmware.com"

	// AnnGuestClusterRequestedTopology is the key for guest cluster requested topology
	AnnGuestClusterRequestedTopology = "csi.vsphere.volume-requested-topology"

	// AnnVolumeAccessibleTopology is the annotation set by the supervisor cluster on PVC
	AnnVolumeAccessibleTopology = "csi.vsphere.volume-accessible-topology"

	// PVtoBackingDiskObjectIdSupportedVCenterMajor is the minimum major version of vCenter
	// on which PV to BackingDiskObjectId mapping feature is supported.
	PVtoBackingDiskObjectIdSupportedVCenterMajor int = 7

	// PVtoBackingDiskObjectIdSupportedVCenterMinor is the minimum minor version of vCenter
	// on which PV to BackingDiskObjectId mapping feature is supported.
	PVtoBackingDiskObjectIdSupportedVCenterMinor int = 0

	// PVtoBackingDiskObjectIdSupportedVCenterPatch is the minimum patch version of vCenter
	// on which PV to BackingDiskObjectId mapping feature is supported.
	PVtoBackingDiskObjectIdSupportedVCenterPatch int = 2

	// PreferredDatastoresCategory points to the vSphere Category
	// created to tag preferred datastores in a topology-aware environment.
	PreferredDatastoresCategory = "cns.vmware.topology-preferred-datastores"

	// VolumeSnapshotNameKey represents the volumesnapshot CR name within
	// the request parameters
	VolumeSnapshotNameKey = "csi.storage.k8s.io/volumesnapshot/name"

	// VolumeSnapshotNamespaceKey represents the volumesnapshot CR namespace within
	// the request parameters
	VolumeSnapshotNamespaceKey = "csi.storage.k8s.io/volumesnapshot/namespace"

	// VolumeSnapshotInfoKey represents the annotation key of the fcd-id + snapshot-id
	// on the VolumeSnapshot CR
	VolumeSnapshotInfoKey = "csi.vsphere.volume/snapshot"

	// SupervisorVolumeSnapshotAnnotationKey represents the annotation key on VolumeSnapshot CR
	// in Supervisor cluster which is used to indicate that snapshot operation is initiated from
	// Guest cluster.
	SupervisorVolumeSnapshotAnnotationKey = "csi.vsphere.guest-initiated-csi-snapshot"

	// AttributeSupervisorVolumeSnapshotClass represents name of VolumeSnapshotClass
	AttributeSupervisorVolumeSnapshotClass = "svvolumesnapshotclass"

	// VolumeSnapshotApiGroup represents the VolumeSnapshot API Group name
	VolumeSnapshotApiGroup = "snapshot.storage.k8s.io"

	// VolumeSnapshotKind represents the VolumeSnapshot Kind name
	VolumeSnapshotKind = "VolumeSnapshot"

	// CreateCSINodeAnnotation is the annotation applied by spherelet
	// to convey to CSI driver to create a CSINode instance for each node.
	CreateCSINodeAnnotation = "vmware-system/csi-create-csinode-object"

	// KubeSystemNamespace is the namespace for system resources.
	KubeSystemNamespace = "kube-system"

	// WCPCapabilitiesCRName is the name of the CR where WCP component's capabilities are stored
	WCPCapabilitiesCRName = "supervisor-capabilities"
)

// Supported container orchestrators.
const (
	// Default container orchestrator for TKC, Supervisor Cluster and Vanilla K8s.
	Kubernetes = iota
)

// Constants related to Feature state
const (
	// Default interval to check if the feature is enabled or not.
	DefaultFeatureEnablementCheckInterval = 1 * time.Minute
	// VolumeHealth is the feature flag name for volume health.
	VolumeHealth = "volume-health"
	// VolumeExtend is feature flag name for volume expansion.
	VolumeExtend = "volume-extend"
	// OnlineVolumeExtend guards the feature for online volume expansion.
	OnlineVolumeExtend = "online-volume-extend"
	// CSIMigration is feature flag for migrating in-tree vSphere volumes to CSI.
	CSIMigration = "csi-migration"
	// AsyncQueryVolume is feature flag for using async query volume API.
	AsyncQueryVolume = "async-query-volume"
	// CSISVFeatureStateReplication is feature flag for SV feature state
	// replication feature.
	CSISVFeatureStateReplication = "csi-sv-feature-states-replication"
	// FileVolume is feature flag name for file volume support in WCP.
	FileVolume = "file-volume"
	// FakeAttach is the feature flag for fake attach support in WCP.
	FakeAttach = "fake-attach"
	// TriggerCSIFullSyync is feature flag to trigger full sync.
	TriggerCsiFullSync = "trigger-csi-fullsync"
	// CSIVolumeManagerIdempotency is the feature flag for idempotency handling
	// in CSI volume manager.
	CSIVolumeManagerIdempotency = "improved-csi-idempotency"
	// BlockVolumeSnapshot is the feature to support CSI Snapshots for block
	// volume on vSphere CSI driver.
	BlockVolumeSnapshot = "block-volume-snapshot"
	// SiblingReplicaBoundPvcCheck is the feature to check whether a PVC of
	// a given replica can be placed on a node such that it does not have PVCs
	// of any of its sibling replicas.
	SiblingReplicaBoundPvcCheck = "sibling-replica-bound-pvc-check"
	// CSIWindowsSupport is the feature to support csi block volumes for windows
	// node.
	CSIWindowsSupport = "csi-windows-support"
	// TKGsHA is the feature gate to check whether TKGS HA feature
	// is enabled.
	TKGsHA = "tkgs-ha"
	// ListVolumes is the feature to support list volumes API
	ListVolumes = "list-volumes"
	// PVtoBackingDiskObjectIdMapping is the feature to support pv to backingDiskObjectId mapping on vSphere CSI driver.
	PVtoBackingDiskObjectIdMapping = "pv-to-backingdiskobjectid-mapping"
	// Block Create Volume for datastores that are in suspended mode
	CnsMgrSuspendCreateVolume = "cnsmgr-suspend-create-volume"
	// TopologyPreferentialDatastores is the feature gate for preferential
	// datastore deployment in topology aware environments.
	TopologyPreferentialDatastores = "topology-preferential-datastores"
	// MaxPVSCSITargetsPerVM enables support for 255 volumes per node vm
	MaxPVSCSITargetsPerVM = "max-pvscsi-targets-per-vm"
	// MultiVCenterCSITopology is the feature gate for enabling multi vCenter topology support for vSphere CSI driver.
	MultiVCenterCSITopology = "multi-vcenter-csi-topology"
	// CSIInternalGeneratedClusterID enables support to generate unique cluster
	// ID internally if user doesn't provide it in vSphere config secret.
	CSIInternalGeneratedClusterID = "csi-internal-generated-cluster-id"
	// TopologyAwareFileVolume enables provisioning of file volumes in a topology enabled environment
	TopologyAwareFileVolume = "topology-aware-file-volume"
	// PodVMOnStretchedSupervisor is the WCP FSS which determines if PodVM
	// support is available on stretched supervisor cluster.
	PodVMOnStretchedSupervisor = "PodVM_On_Stretched_Supervisor_Supported"
	// StorageQuotaM2 enables support for snapshot quota feature
	StorageQuotaM2 = "storage-quota-m2"
	// VdppOnStretchedSupervisor enables support for vDPp workloads on stretched SV clusters
	VdppOnStretchedSupervisor = "vdpp-on-stretched-supervisor"
	// CSIDetachOnSupervisor enables CSI to detach the disk from the podvm in a supervisor environment
	CSIDetachOnSupervisor = "CSI_Detach_Supported"
	// CnsUnregisterVolume enables the creation of CRD and controller for CnsUnregisterVolume API.
	CnsUnregisterVolume = "cns-unregister-volume"
	// WorkloadDomainIsolation is the name of the WCP capability which determines if
	// workload domain isolation feature is available on a supervisor cluster.
	WorkloadDomainIsolation = "Workload_Domain_Isolation_Supported"
	// WorkloadDomainIsolationFSS is FSS for Workload Domain isolation feature
	// Used in PVCSI
	WorkloadDomainIsolationFSS = "workload-domain-isolation"
	// VPCCapabilitySupervisor is a supervisor capability indicating if VPC FSS is enabled
	VPCCapabilitySupervisor = "VPC_Supported"
	// WCP_VMService_BYOK_FSS enables Bring Your Own Key (BYOK) capabilities.
	WCP_VMService_BYOK = "WCP_VMService_BYOK"
	// SVPVCSnapshotProtectionFinalizer is FSS that controls add/remove
	// CNS finalizer on supervisor PVC/Snapshots from PVCSI
	SVPVCSnapshotProtectionFinalizer = "sv-pvc-snapshot-protection-finalizer"
	// FileVolumesWithVmService is an FSS to support file volumes with VM service VMs.
	FileVolumesWithVmService = "file-volume-with-vm-service"
	// SharedDiskFss is an FSS that tells whether shared disks are supported or not
	SharedDiskFss = "supports_shared_disks"
	// CSITranSactionSupport is an FSS for transaction support
	CSITranSactionSupport = "csi-transaction-support"
)

var WCPFeatureStates = map[string]struct{}{
	PodVMOnStretchedSupervisor: {},
	CSIDetachOnSupervisor:      {},
	WorkloadDomainIsolation:    {},
	VPCCapabilitySupervisor:    {},
}

// WCPFeatureStatesSupportsLateEnablement contains capabilities that can be enabled later
// after CSI upgrade
// During FSS check if driver detects that the capabilities is disabled in the cached configmap,
// it will re-fetch the configmap and update the cached configmap.
var WCPFeatureStatesSupportsLateEnablement = map[string]struct{}{
	WorkloadDomainIsolation: {},
}

// WCPFeatureAssociatedWithPVCSI contains FSS name used in PVCSI and associated WCP Capability name on a
// supervisor cluster. Add entry in this map only for PVCSI feature for which there is any associated Capability
// on supervisor cluster. If PVCSI feature is enabled, then we need to check if associated Capability is enabled
// or not on the supervisor cluster to decide if effective value of this FSS is enabled or disabled.
var WCPFeatureStateAssociatedWithPVCSI = map[string]string{
	WorkloadDomainIsolationFSS: WorkloadDomainIsolation,
}
