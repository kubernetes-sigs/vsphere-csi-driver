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

package constants

// ENV variable to specify path of the E2E test config file
const (
	E2eTestConfFileEnvVar = "E2E_TEST_CONF_FILE"
)

// For rwx
const (
	EnvVsanDsStoragePolicyCluster1           = "VSAN_DATASTORE_CLUSTER1_STORAGE_POLICY"
	EnvVsanDsStoragePolicyCluster3           = "VSAN_DATASTORE_CLUSTER3_STORAGE_POLICY"
	EnvNonVsanDsUrl                          = "NON_VSAN_DATASTOREURL"
	EnvVsanDsUrlCluster3                     = "VSAN_DATASTOREURL_CLUSTER3"
	EnvRemoteDatastoreUrl                    = "REMOTE_DATASTORE_URL"
	EnvTopologySetupType                     = "TOPOLOGY_SETUP_TYPE"
	EnvStoragePolicyNameForVsanNfsDatastores = "STORAGE_POLICY_FOR_VSAN_NFS_DATASTORES"
	EnvSharedDatastoreName                   = "SHARED_VSPHERE_DATASTORE_NAME"
	SvcMasterIP                              = "SVC_MASTER_IP"
	SvcMasterPassword                        = "SVC_MASTER_PASSWORD"
)

// For zonal storage classes
const (
	EnvZonalStoragePolicyName2    = "ZONAL2_STORAGECLASS"
	EnvZonalStoragePolicyName     = "ZONAL_STORAGECLASS"
	EnvZonalWffcStoragePolicyName = "ZONAL_WFFC_STORAGECLASS"
)

// For management workload domain isolation
const (
	EnvZonal2StoragePolicyName            = "ZONAL2_STORAGE_POLICY_IMM"
	EnvZonal2StoragePolicyNameLateBidning = "ZONAL2_STORAGE_POLICY_WFFC"
	EnvZonal1StoragePolicyName            = "ZONAL1_STORAGE_POLICY_IMM"
	EnvZonal3StoragePolicyName            = "ZONAL3_STORAGE_POLICY_IMM"
	TopologyDomainIsolation               = "Workload_Management_Isolation"
	EnvTopologyMap                        = "TOPOLOGY_MAP"
	TopologyHaMap                         = "TOPOLOGY_HA_MAP"
	TopologyFeature                       = "TOPOLOGY_FEATURE"
	TopologyTkgHaName                     = "tkgs_ha"
	EnvIsolationSharedStoragePolicyName   = "WORKLOAD_ISOLATION_SHARED_STORAGE_POLICY"
	EnvSharedZone2Zone4StoragePolicyName  = "SHARED_ZONE2_ZONE4_STORAGE_POLICY_IMM"
	EnvSharedZone2Zone4DatastoreUrl       = "SHARED_ZONE2_ZONE4_DATASTORE_URL"
	EnvTopologyType                       = "TOPOLOGY_TYPE"
)

// multi svc environment variables
const (
	EnvSupervisorType                   = "SUPERVISOR_TYPE"
	EnvStoragePolicyNameForSharedDsSvc1 = "STORAGE_POLICY_FOR_SHARED_DATASTORES_SVC1"
	EnvStoragePolicyNameForSharedDsSvc2 = "STORAGE_POLICY_FOR_SHARED_DATASTORES_SVC2"
	EnvSupervisorClusterNamespace1      = "SVC_NAMESPACE1"
	EnvNfsDatastoreName                 = "NFS_DATASTORE_NAME"
	EnvNfsDatastoreIP                   = "NFS_DATASTORE_IP"
	EnvSharedDatastoreURLVC1            = "SHARED_VSPHERE_DATASTORE_URL_VC1"
	EnvSharedDatastoreURLVC2            = "SHARED_VSPHERE_DATASTORE_URL_VC2"
	EnvStoragePolicyNameToDeleteLater   = "STORAGE_POLICY_TO_DELETE_LATER"
	EnvMultiVCSetupType                 = "MULTI_VC_SETUP_TYPE"
	EnvStoragePolicyNameVC1             = "STORAGE_POLICY_VC1"
	EnvStoragePolicyNameInVC1VC2        = "STORAGE_POLICY_NAME_COMMON_IN_VC1_VC2"
	EnvPreferredDatastoreUrlVC1         = "PREFERRED_DATASTORE_URL_VC1"
	EnvPreferredDatastoreUrlVC2         = "PREFERRED_DATASTORE_URL_VC2"
	EnvTestbedInfoJsonPathVC1           = "TESTBEDINFO_JSON_VC1"
	EnvTestbedInfoJsonPathVC2           = "TESTBEDINFO_JSON_VC2"
	EnvTestbedInfoJsonPathVC3           = "TESTBEDINFO_JSON_VC3"
)

// windows env variables
const (
	EnvWindowsUser = "WINDOWS_USER"
	EnvWindowsPwd  = "WINDOWS_PWD"
	EnvWorkerType  = "WORKER_TYPE"
)

// reading port numbers for VC, Master VM and ESXi from export variables
const (
	EnvVc1SshdPortNum       = "VC1_SSHD_PORT_NUM"
	EnvVc2SshdPortNum       = "VC2_SSHD_PORT_NUM"
	EnvVc3SshdPortNum       = "VC3_SSHD_PORT_NUM"
	EnvMasterIP1SshdPortNum = "MASTER_IP1_SSHD_PORT_NUM"
	EnvMasterIP2SshdPortNum = "MASTER_IP2_SSHD_PORT_NUM"
	EnvMasterIP3SshdPortNum = "MASTER_IP3_SSHD_PORT_NUM"
	EnvEsx1PortNum          = "ESX1_SSHD_PORT_NUM"
	EnvEsx2PortNum          = "ESX2_SSHD_PORT_NUM"
	EnvEsx3PortNum          = "ESX3_SSHD_PORT_NUM"
	EnvEsx4PortNum          = "ESX4_SSHD_PORT_NUM"
	EnvEsx5PortNum          = "ESX5_SSHD_PORT_NUM"
	EnvEsx6PortNum          = "ESX6_SSHD_PORT_NUM"
	EnvEsx7PortNum          = "ESX7_SSHD_PORT_NUM"
	EnvEsx8PortNum          = "ESX8_SSHD_PORT_NUM"
	EnvEsx9PortNum          = "ESX9_SSHD_PORT_NUM"
	EnvEsx10PortNum         = "ESX10_SSHD_PORT_NUM"

	// reading IPs for VC, Master VM and ESXi from export variables
	EnvVcIP1     = "VC_IP1"
	EnvVcIP2     = "VC_IP2"
	EnvVcIP3     = "VC_IP3"
	EnvMasterIP1 = "MASTER_IP1"
	EnvMasterIP2 = "MASTER_IP2"
	EnvMasterIP3 = "MASTER_IP3"
	EnvEsxIp1    = "ESX1_IP"
	EnvEsxIp2    = "ESX2_IP"
	EnvEsxIp3    = "ESX3_IP"
	EnvEsxIp4    = "ESX4_IP"
	EnvEsxIp5    = "ESX5_IP"
	EnvEsxIp6    = "ESX6_IP"
	EnvEsxIp7    = "ESX7_IP"
	EnvEsxIp8    = "ESX8_IP"
	EnvEsxIp9    = "ESX9_IP"
	EnvEsxIp10   = "ESX10_IP"

	// default port declaration for each IP
	VcIp1SshPortNum       = SshdPort
	VcIp2SshPortNum       = SshdPort
	VcIp3SshPortNum       = SshdPort
	EsxIp1PortNum         = SshdPort
	EsxIp2PortNum         = SshdPort
	EsxIp3PortNum         = SshdPort
	EsxIp4PortNum         = SshdPort
	EsxIp5PortNum         = SshdPort
	EsxIp6PortNum         = SshdPort
	EsxIp7PortNum         = SshdPort
	EsxIp8PortNum         = SshdPort
	EsxIp9PortNum         = SshdPort
	EsxIp10PortNum        = SshdPort
	K8sMasterIp1PortNum   = SshdPort
	K8sMasterIp2PortNum   = SshdPort
	K8sMasterIp3PortNum   = SshdPort
	DefaultVcAdminPortNum = "443"
	VirtualDiskUUID       = "virtualDiskUUID"
	ManifestPath          = "tests/e2e/testing-manifests/statefulset/nginx"
)

// VolumeSnapshotClass env variables for tkg-snapshot
const (
	EnvVolSnapClassDel = "VOLUME_SNAPSHOT_CLASS_DELETE"
)

// For vsan stretched cluster tests
const (
	EnvTestbedInfoJsonPath = "TESTBEDINFO_JSON"
	EnvStretchedSvc        = "STRETCHED_SVC"
)
const (
	EnsureAccessibilityMModeType               = "ensureObjectAccessibility"
	EnvClusterFlavor                           = "CLUSTER_FLAVOR"
	EnvDiskSizeLarge                           = "LARGE_DISK_SIZE"
	EnvCSINamespace                            = "CSI_NAMESPACE"
	EnvContentLibraryUrl                       = "CONTENT_LIB_URL"
	EnvContentLibraryUrlSslThumbprint          = "CONTENT_LIB_THUMBPRINT"
	EnvEsxHostIP                               = "ESX_TEST_HOST_IP"
	EnvFileServiceDisabledSharedDatastoreURL   = "FILE_SERVICE_DISABLED_SHARED_VSPHERE_DATASTORE_URL"
	EnvFullSyncWaitTime                        = "FULL_SYNC_WAIT_TIME"
	EnvGatewayVmIp                             = "GATEWAY_VM_IP"
	EnvGatewayVmUser                           = "GATEWAY_VM_USER"
	EnvGatewayVmPasswd                         = "GATEWAY_VM_PASSWD"
	EnvHciMountRemoteDs                        = "USE_HCI_MESH_DS"
	EnvInaccessibleZoneDatastoreURL            = "INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL"
	EnvNonSharedStorageClassDatastoreURL       = "NONSHARED_VSPHERE_DATASTORE_URL"
	EnvPandoraSyncWaitTime                     = "PANDORA_SYNC_WAIT_TIME"
	EnvK8sNodesUpWaitTime                      = "K8S_NODES_UP_WAIT_TIME"
	EnvRegionZoneWithNoSharedDS                = "TOPOLOGY_WITH_NO_SHARED_DATASTORE"
	EnvRegionZoneWithSharedDS                  = "TOPOLOGY_WITH_SHARED_DATASTORE"
	EnvRemoteHCIDsUrl                          = "REMOTE_HCI_DS_URL"
	EnvSharedDatastoreURL                      = "SHARED_VSPHERE_DATASTORE_URL"
	EnvSharedVVOLDatastoreURL                  = "SHARED_VVOL_DATASTORE_URL"
	EnvSharedNFSDatastoreURL                   = "SHARED_NFS_DATASTORE_URL"
	EnvSharedVMFSDatastoreURL                  = "SHARED_VMFS_DATASTORE_URL"
	EnvSharedVMFSDatastore2URL                 = "SHARED_VMFS_DATASTORE2_URL"
	EnvVMClass                                 = "VM_CLASS"
	EnvVsanDirectSetup                         = "USE_VSAN_DIRECT_DATASTORE_IN_WCP"
	EnvVsanDDatastoreURL                       = "SHARED_VSAND_DATASTORE_URL"
	EnvVsanDDatastore2URL                      = "SHARED_VSAND_DATASTORE2_URL"
	EnvStoragePolicyNameForNonSharedDatastores = "STORAGE_POLICY_FOR_NONSHARED_DATASTORES"
	EnvStoragePolicyNameForSharedDatastores    = "STORAGE_POLICY_FOR_SHARED_DATASTORES"
	EnvStoragePolicyNameForHCIRemoteDatastores = "STORAGE_POLICY_FOR_HCI_REMOTE_DS"
	EnvStoragePolicyNameForVsanVmfsDatastores  = "STORAGE_POLICY_FOR_VSAN_VMFS_DATASTORES"
	EnvStoragePolicyNameForSharedDatastores2   = "STORAGE_POLICY_FOR_SHARED_DATASTORES_2"
	EnvStoragePolicyNameForVmfsDatastores      = "STORAGE_POLICY_FOR_VMFS_DATASTORES"
	EnvStoragePolicyNameForNfsDatastores       = "STORAGE_POLICY_FOR_NFS_DATASTORES"
	EnvStoragePolicyNameForVvolDatastores      = "STORAGE_POLICY_FOR_VVOL_DATASTORES"
	EnvStoragePolicyNameFromInaccessibleZone   = "STORAGE_POLICY_FROM_INACCESSIBLE_ZONE"
	EnvStoragePolicyNameWithThickProvision     = "STORAGE_POLICY_WITH_THICK_PROVISIONING"
	EnvStoragePolicyNameWithEncryption         = "STORAGE_POLICY_WITH_ENCRYPTION"
	EnvKeyProvider                             = "KEY_PROVIDER"
	EnvSupervisorClusterNamespace              = "SVC_NAMESPACE"
	EnvSupervisorClusterNamespaceToDelete      = "SVC_NAMESPACE_TO_DELETE"
	EnvTopologyWithOnlyOneNode                 = "TOPOLOGY_WITH_ONLY_ONE_NODE"
	EnvTopologyWithInvalidTagInvalidCat        = "TOPOLOGY_WITH_INVALID_TAG_INVALID_CAT"
	EnvTopologyWithInvalidTagValidCat          = "TOPOLOGY_WITH_INVALID_TAG_VALID_CAT"
	EnvNumberOfGoRoutines                      = "NUMBER_OF_GO_ROUTINES"
	EnvWorkerPerRoutine                        = "WORKER_PER_ROUTINE"
	EnvVmdkDiskURL                             = "DISK_URL_PATH"
	EnvVmsvcVmImageName                        = "VMSVC_IMAGE_NAME"
	EnvVolumeOperationsScale                   = "VOLUME_OPS_SCALE"
	EnvComputeClusterName                      = "COMPUTE_CLUSTER_NAME"
	EnvTKGImage                                = "TKG_IMAGE_NAME"
	EnvVmknic4Vsan                             = "VMKNIC_FOR_VSAN"
	EnvAccessMode                              = "ACCESS_MODE"
	EnvVpToCsi                                 = "VCPTOCSI"
	EnvVsphereTKGSystemNamespace               = "VSPHERE_TKG_SYSTEM_NAMESPACE"
)
