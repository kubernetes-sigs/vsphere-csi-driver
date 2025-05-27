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
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"

	"github.com/onsi/gomega"
)

const (
	adminUser                                  = "Administrator@vsphere.local"
	apiServerIPs                               = "API_SERVER_IPS"
	attacherContainerName                      = "csi-attacher"
	nginxImage                                 = "registry.k8s.io/nginx-slim:0.26"
	nginxImage4upg                             = "registry.k8s.io/nginx-slim:0.27"
	retainClaimPolicy                          = "Retain"
	cloudInitLabel                             = "CloudInit"
	configSecret                               = "vsphere-config-secret"
	contollerClusterKubeConfig                 = "CONTROLLER_CLUSTER_KUBECONFIG"
	controlPlaneLabel                          = "node-role.kubernetes.io/control-plane"
	crdCNSNodeVMAttachment                     = "cnsnodevmattachments"
	crdCNSVolumeMetadatas                      = "cnsvolumemetadatas"
	crdCNSFileAccessConfig                     = "cnsfileaccessconfigs"
	crdtriggercsifullsyncsName                 = "csifullsync"
	crdGroup                                   = "cns.vmware.com"
	crdVersion                                 = "v1alpha1"
	crdVirtualMachineImages                    = "virtualmachineimages"
	crdVirtualMachines                         = "virtualmachines"
	crdVirtualMachineService                   = "virtualmachineservice"
	csiSystemNamespace                         = "vmware-system-csi"
	csiFssCM                                   = "internal-feature-states.csi.vsphere.vmware.com"
	csiVolAttrVolType                          = "vSphere CNS Block Volume"
	csiDriverContainerName                     = "vsphere-csi-controller"
	datacenter                                 = "DATACENTER"
	defaultFullSyncIntervalInMin               = "30"
	defaultProvisionerTimeInSec                = "300"
	defaultFullSyncWaitTime                    = 1800
	defaultPandoraSyncWaitTime                 = 90
	defaultK8sNodesUpWaitTime                  = 25
	destinationDatastoreURL                    = "DESTINATION_VSPHERE_DATASTORE_URL"
	disklibUnlinkErr                           = "DiskLib_Unlink"
	diskSize1GB                                = "1Gi"
	diskSize                                   = "2Gi"
	diskSizeSmall                              = "100Mi"
	diskSizeLarge                              = "100Gi"
	diskSizeInMb                               = int64(2048)
	diskSizeInMinMb                            = int64(200)
	e2eTestPassword                            = "E2E-test-password!23"
	e2evSphereCSIDriverName                    = "csi.vsphere.vmware.com"
	ensureAccessibilityMModeType               = "ensureObjectAccessibility"
	envClusterFlavor                           = "CLUSTER_FLAVOR"
	envDiskSizeLarge                           = "LARGE_DISK_SIZE"
	envCSINamespace                            = "CSI_NAMESPACE"
	envContentLibraryUrl                       = "CONTENT_LIB_URL"
	envContentLibraryUrlSslThumbprint          = "CONTENT_LIB_THUMBPRINT"
	envEsxHostIP                               = "ESX_TEST_HOST_IP"
	envFileServiceDisabledSharedDatastoreURL   = "FILE_SERVICE_DISABLED_SHARED_VSPHERE_DATASTORE_URL"
	envFullSyncWaitTime                        = "FULL_SYNC_WAIT_TIME"
	envGatewayVmIp                             = "GATEWAY_VM_IP"
	envGatewayVmUser                           = "GATEWAY_VM_USER"
	envGatewayVmPasswd                         = "GATEWAY_VM_PASSWD"
	envHciMountRemoteDs                        = "USE_HCI_MESH_DS"
	envInaccessibleZoneDatastoreURL            = "INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL"
	envNonSharedStorageClassDatastoreURL       = "NONSHARED_VSPHERE_DATASTORE_URL"
	envPandoraSyncWaitTime                     = "PANDORA_SYNC_WAIT_TIME"
	envK8sNodesUpWaitTime                      = "K8S_NODES_UP_WAIT_TIME"
	envRegionZoneWithNoSharedDS                = "TOPOLOGY_WITH_NO_SHARED_DATASTORE"
	envRegionZoneWithSharedDS                  = "TOPOLOGY_WITH_SHARED_DATASTORE"
	envRemoteHCIDsUrl                          = "REMOTE_HCI_DS_URL"
	envSharedDatastoreURL                      = "SHARED_VSPHERE_DATASTORE_URL"
	envSharedVVOLDatastoreURL                  = "SHARED_VVOL_DATASTORE_URL"
	envSharedNFSDatastoreURL                   = "SHARED_NFS_DATASTORE_URL"
	envSharedVMFSDatastoreURL                  = "SHARED_VMFS_DATASTORE_URL"
	envSharedVMFSDatastore2URL                 = "SHARED_VMFS_DATASTORE2_URL"
	envVMClass                                 = "VM_CLASS"
	envVsanDirectSetup                         = "USE_VSAN_DIRECT_DATASTORE_IN_WCP"
	envVsanDDatastoreURL                       = "SHARED_VSAND_DATASTORE_URL"
	envVsanDDatastore2URL                      = "SHARED_VSAND_DATASTORE2_URL"
	envStoragePolicyNameForNonSharedDatastores = "STORAGE_POLICY_FOR_NONSHARED_DATASTORES"
	envStoragePolicyNameForSharedDatastores    = "STORAGE_POLICY_FOR_SHARED_DATASTORES"
	envStoragePolicyNameForHCIRemoteDatastores = "STORAGE_POLICY_FOR_HCI_REMOTE_DS"
	envStoragePolicyNameForVsanVmfsDatastores  = "STORAGE_POLICY_FOR_VSAN_VMFS_DATASTORES"
	envStoragePolicyNameForSharedDatastores2   = "STORAGE_POLICY_FOR_SHARED_DATASTORES_2"
	envStoragePolicyNameForVmfsDatastores      = "STORAGE_POLICY_FOR_VMFS_DATASTORES"
	envStoragePolicyNameForNfsDatastores       = "STORAGE_POLICY_FOR_NFS_DATASTORES"
	envStoragePolicyNameForVvolDatastores      = "STORAGE_POLICY_FOR_VVOL_DATASTORES"
	envStoragePolicyNameFromInaccessibleZone   = "STORAGE_POLICY_FROM_INACCESSIBLE_ZONE"
	envStoragePolicyNameWithThickProvision     = "STORAGE_POLICY_WITH_THICK_PROVISIONING"
	envStoragePolicyNameWithEncryption         = "STORAGE_POLICY_WITH_ENCRYPTION"
	envKeyProvider                             = "KEY_PROVIDER"
	envSupervisorClusterNamespace              = "SVC_NAMESPACE"
	envSupervisorClusterNamespaceToDelete      = "SVC_NAMESPACE_TO_DELETE"
	envTopologyWithOnlyOneNode                 = "TOPOLOGY_WITH_ONLY_ONE_NODE"
	envTopologyWithInvalidTagInvalidCat        = "TOPOLOGY_WITH_INVALID_TAG_INVALID_CAT"
	envTopologyWithInvalidTagValidCat          = "TOPOLOGY_WITH_INVALID_TAG_VALID_CAT"
	envNumberOfGoRoutines                      = "NUMBER_OF_GO_ROUTINES"
	envWorkerPerRoutine                        = "WORKER_PER_ROUTINE"
	envVmdkDiskURL                             = "DISK_URL_PATH"
	envVmsvcVmImageName                        = "VMSVC_IMAGE_NAME"
	envVolumeOperationsScale                   = "VOLUME_OPS_SCALE"
	envComputeClusterName                      = "COMPUTE_CLUSTER_NAME"
	envTKGImage                                = "TKG_IMAGE_NAME"
	envVmknic4Vsan                             = "VMKNIC_FOR_VSAN"
	execCommand                                = "/bin/df -T /mnt/volume1 | " +
		"/bin/awk 'FNR == 2 {print $2}' > /mnt/volume1/fstype && while true ; do sleep 2 ; done"
	execRWXCommandPod = "echo 'Hello message from Pod' > /mnt/volume1/Pod.html  && " +
		"chmod o+rX /mnt /mnt/volume1/Pod.html && while true ; do sleep 2 ; done"
	execRWXCommandPod1 = "echo 'Hello message from Pod1' > /mnt/volume1/Pod1.html  && " +
		"chmod o+rX /mnt /mnt/volume1/Pod1.html && while true ; do sleep 2 ; done"
	execRWXCommandPod2 = "echo 'Hello message from Pod2' > /mnt/volume1/Pod2.html  && " +
		"chmod o+rX /mnt /mnt/volume1/Pod2.html && while true ; do sleep 2 ; done"
	ext3FSType                                = "ext3"
	ext4FSType                                = "ext4"
	xfsFSType                                 = "xfs"
	evacMModeType                             = "evacuateAllData"
	fcdName                                   = "BasicStaticFCD"
	fileSizeInMb                              = int64(2048)
	filePathPod                               = "/mnt/volume1/Pod.html"
	filePathPod1                              = "/mnt/volume1/Pod1.html"
	filePathPod2                              = "/mnt/volume1/Pod2.html"
	filePathFsType                            = "/mnt/volume1/fstype"
	fullSyncFss                               = "trigger-csi-fullsync"
	gcNodeUser                                = "vmware-system-user"
	gcKubeConfigPath                          = "GC_KUBE_CONFIG"
	gcSshKey                                  = "TEST-CLUSTER-SSH-KEY"
	healthGreen                               = "green"
	healthRed                                 = "red"
	healthStatusAccessible                    = "accessible"
	healthStatusInAccessible                  = "inaccessible"
	healthStatusWaitTime                      = 3 * time.Minute
	hostdServiceName                          = "hostd"
	invalidFSType                             = "ext10"
	k8sPodTerminationTimeOut                  = 7 * time.Minute
	k8sPodTerminationTimeOutLong              = 10 * time.Minute
	kcmManifest                               = "/etc/kubernetes/manifests/kube-controller-manager.yaml"
	kubeAPIPath                               = "/etc/kubernetes/manifests/"
	kubeAPIfile                               = "kube-apiserver.yaml"
	kubeAPIRecoveryTime                       = 1 * time.Minute
	kubeSystemNamespace                       = "kube-system"
	kubeletConfigYaml                         = "/var/lib/kubelet/config.yaml"
	nfs4FSType                                = "nfs4"
	mmStateChangeTimeout                      = 300 // int
	objOrItemNotFoundErr                      = "The object or item referred to could not be found"
	passorwdFilePath                          = "/etc/vmware/wcp/.storageUser"
	podContainerCreatingState                 = "ContainerCreating"
	poll                                      = 2 * time.Second
	pollTimeout                               = 10 * time.Minute
	pollTimeoutShort                          = 1 * time.Minute
	pollTimeoutSixMin                         = 6 * time.Minute
	healthStatusPollTimeout                   = 20 * time.Minute
	healthStatusPollInterval                  = 30 * time.Second
	psodTime                                  = "120"
	pvcHealthAnnotation                       = "volumehealth.storage.kubernetes.io/health"
	pvcHealthTimestampAnnotation              = "volumehealth.storage.kubernetes.io/health-timestamp"
	provisionerContainerName                  = "csi-provisioner"
	quotaName                                 = "cns-test-quota"
	regionKey                                 = "topology.csi.vmware.com/k8s-region"
	resizePollInterval                        = 2 * time.Second
	restartOperation                          = "restart"
	rqLimit                                   = "200Gi"
	rqLimitScaleTest                          = "900Gi"
	rootUser                                  = "root"
	defaultrqLimit                            = "20Gi"
	rqStorageType                             = ".storageclass.storage.k8s.io/requests.storage"
	resizerContainerName                      = "csi-resizer"
	scParamDatastoreURL                       = "DatastoreURL"
	scParamFsType                             = "csi.storage.k8s.io/fstype"
	scParamStoragePolicyID                    = "storagePolicyID"
	scParamStoragePolicyName                  = "StoragePolicyName"
	shortProvisionerTimeout                   = "10"
	snapshotapigroup                          = "snapshot.storage.k8s.io"
	sleepTimeOut                              = 30
	oneMinuteWaitTimeInSeconds                = 60
	spsServiceName                            = "sps"
	snapshotterContainerName                  = "csi-snapshotter"
	sshdPort                                  = "22"
	sshSecretName                             = "SSH_SECRET_NAME"
	svcRunningMessage                         = "Running"
	svcMasterIP                               = "SVC_MASTER_IP"
	svcMasterPassword                         = "SVC_MASTER_PASSWORD"
	startOperation                            = "start"
	svcStoppedMessage                         = "Stopped"
	stopOperation                             = "stop"
	statusOperation                           = "status"
	envZonalStoragePolicyName                 = "ZONAL_STORAGECLASS"
	envZonalWffcStoragePolicyName             = "ZONAL_WFFC_STORAGECLASS"
	supervisorClusterOperationsTimeout        = 3 * time.Minute
	svClusterDistribution                     = "SupervisorCluster"
	svOperationTimeout                        = 240 * time.Second
	svStorageClassName                        = "SVStorageClass"
	syncerContainerName                       = "vsphere-syncer"
	totalResizeWaitPeriod                     = 10 * time.Minute
	tkgClusterDistribution                    = "TKGService"
	vanillaClusterDistribution                = "CSI-Vanilla"
	vanillaClusterDistributionWithSpecialChar = "CSI-\tVanilla-#Test"
	vcClusterAPI                              = "/api/vcenter/namespace-management/clusters"
	vcRestSessionIdHeaderName                 = "vmware-api-session-Id"
	vpxdServiceName                           = "vpxd"
	vpxdReducedTaskTimeoutSecsInt             = 90
	vSphereCSIControllerPodNamePrefix         = "vsphere-csi-controller"
	vmUUIDLabel                               = "vmware-system-vm-uuid"
	vsanLabel                                 = "vsan"
	vsanDefaultStorageClassInSVC              = "vsan-default-storage-policy"
	vsanDefaultStoragePolicyName              = "vSAN Default Storage Policy"
	vsanHealthServiceWaitTime                 = 15
	vsanhealthServiceName                     = "vsan-health"
	vsphereCloudProviderConfiguration         = "vsphere-cloud-provider.conf"
	vsphereControllerManager                  = "vmware-system-tkg-controller-manager"
	vSphereCSIConf                            = "csi-vsphere.conf"
	vsphereTKGSystemNamespace                 = "svc-tkg-domain-c10"
	waitTimeForCNSNodeVMAttachmentReconciler  = 30 * time.Second
	wcpServiceName                            = "wcp"
	vmcWcpHost                                = "10.2.224.24" //This is the LB IP of VMC WCP and its constant
	devopsTKG                                 = "test-cluster-e2e-script"
	cloudadminTKG                             = "test-cluster-e2e-script-1"
	vmOperatorAPI                             = "/apis/vmoperator.vmware.com/v1alpha1/"
	devopsUser                                = "testuser"
	zoneKey                                   = "topology.csi.vmware.com/k8s-zone"
	tkgAPI                                    = "/apis/run.tanzu.vmware.com/v1alpha3/namespaces" +
		"/test-gc-e2e-demo-ns/tanzukubernetesclusters/"
	topologykey                                = "topology.csi.vmware.com"
	envTopologyMap                             = "TOPOLOGY_MAP"
	topologyHaMap                              = "TOPOLOGY_HA_MAP"
	topologyFeature                            = "TOPOLOGY_FEATURE"
	topologyTkgHaName                          = "tkgs_ha"
	tkgHATopologyKey                           = "topology.kubernetes.io"
	tkgHAccessibleAnnotationKey                = "csi.vsphere.volume-accessible-topology"
	tkgHARequestedAnnotationKey                = "csi.vsphere.volume-requested-topology"
	datstoreSharedBetweenClusters              = "DATASTORE_SHARED_BETWEEN_TWO_CLUSTERS"
	datastoreUrlSpecificToCluster              = "DATASTORE_URL_SPECIFIC_TO_CLUSTER"
	storagePolicyForDatastoreSpecificToCluster = "STORAGE_POLICY_FOR_DATASTORE_SPECIFIC_TO_CLUSTER"
	topologyCluster                            = "TOPOLOGY_CLUSTERS"
	topologyLength                             = 5
	tkgshaTopologyLevels                       = 1
	vmClassBestEffortSmall                     = "best-effort-small"
	vmcPrdEndpoint                             = "https://vmc.vmware.com/vmc/api/orgs/"
	vsphereClusterIdConfigMapName              = "vsphere-csi-cluster-id"
	authAPI                                    = "https://console.cloud.vmware.com/csp/gateway/am/api/auth" +
		"/api-tokens/authorize"
	storagePolicyQuota                       = "-storagepolicyquota"
	podVMOnStretchedSupervisor               = "stretched-svc"
	stretchedSVCTopologyLevels               = 1
	envZonalStoragePolicyName2               = "ZONAL2_STORAGECLASS"
	volExtensionName                         = "volume.cns.vsphere.vmware.com"
	snapshotExtensionName                    = "snapshot.cns.vsphere.vmware.com"
	vmServiceExtensionName                   = "vmservice.cns.vsphere.vmware.com"
	pvcUsage                                 = "-pvc-usage"
	snapshotUsage                            = "-snapshot-usage"
	vmUsage                                  = "-vm-usage"
	diskSize1Gi                              = int64(1024)
	storageQuotaWebhookPrefix                = "storage-quota-webhook"
	envStoragePolicyNameForVsanNfsDatastores = "STORAGE_POLICY_FOR_VSAN_NFS_DATASTORES"
	devopsKubeConf                           = "DEV_OPS_USER_KUBECONFIG"
	quotaSupportedVCVersion                  = "9.0.0"
)

/*
// test suite labels

flaky -> label include the testcases which fails intermittently
disruptive -> label include the testcases which are disruptive in nature  ex: hosts down, cluster down, datastore down
vanilla -> label include the testcases for block, file, configSecret, topology etc.
stable -> label include the testcases which do not fail
longRunning -> label include the testcases which takes longer time for completion
p0 -> label include the testcases which are P0
p1 -> label include the testcases which are P1, vcreboot, negative
p2 -> label include the testcases which are P2
semiAutomated -> label include the testcases which are semi-automated
newTests -> label include the testcases which are newly automated
core -> label include the testcases specific to block or file
level2 -> label include the level-2 topology testcases or pipeline specific
level5 -> label include the level-5 topology testcases
customPort -> label include the testcases running on vCenter custom port <VC:444>
deprecated ->label include the testcases which are no longer in execution
negative -> Negative tests, ex: service/pod down(sps, vsan-health, vpxd, hostd, csi pods)
vc70 -> Tests for vc70 features
vc80 -> Tests for vc80 features
vc80 -> Tests for vc90 features
vmServiceVm -> vmService VM related testcases
wldi -> Work-Load Domain Isolation testcases
*/
const (
	flaky                 = "flaky"
	disruptive            = "disruptive"
	wcp                   = "wcp"
	tkg                   = "tkg"
	vanilla               = "vanilla"
	preferential          = "preferential"
	vsphereConfigSecret   = "vsphereConfigSecret"
	snapshot              = "snapshot"
	stable                = "stable"
	newTest               = "newTest"
	multiVc               = "multiVc"
	block                 = "block"
	file                  = "file"
	core                  = "core"
	hci                   = "hci"
	p0                    = "p0"
	p1                    = "p1"
	p2                    = "p2"
	vsanStretch           = "vsanStretch"
	longRunning           = "longRunning"
	deprecated            = "deprecated"
	vmc                   = "vmc"
	tkgsHA                = "tkgsHA"
	thickThin             = "thickThin"
	customPort            = "customPort"
	windows               = "windows"
	semiAutomated         = "semiAutomated"
	level2                = "level2"
	level5                = "level5"
	negative              = "negative"
	listVolume            = "listVolume"
	multiSvc              = "multiSvc"
	primaryCentric        = "primaryCentric"
	controlPlaneOnPrimary = "controlPlaneOnPrimary"
	distributed           = "distributed"
	vmsvc                 = "vmsvc"
	vc90                  = "vc90"
	vc80                  = "vc80"
	vc70                  = "vc70"
	wldi                  = "wldi"
	vmServiceVm           = "vmServiceVm"
)

// The following variables are required to know cluster type to run common e2e
// tests. These variables will be set once during test suites initialization.
var (
	vanillaCluster       bool
	supervisorCluster    bool
	guestCluster         bool
	rwxAccessMode        bool
	wcpVsanDirectCluster bool
	vcptocsi             bool
	windowsEnv           bool
	multipleSvc          bool
	multivc              bool
	stretchedSVC         bool
)

// For busybox pod image
var (
	busyBoxImageOnGcr = "busybox"
)

// For VCP to CSI migration tests.
var (
	envSharedDatastoreName          = "SHARED_VSPHERE_DATASTORE_NAME"
	vcpProvisionerName              = "kubernetes.io/vsphere-volume"
	vcpScParamDatastoreName         = "datastore"
	vcpScParamPolicyName            = "storagePolicyName"
	vcpScParamFstype                = "fstype"
	migratedToAnnotation            = "pv.kubernetes.io/migrated-to"
	migratedPluginAnnotation        = "storage.alpha.kubernetes.io/migrated-plugins"
	pvcAnnotationStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	pvAnnotationProvisionedBy       = "pv.kubernetes.io/provisioned-by"
	nodeMapper                      = &NodeMapper{}
)

// For vsan stretched cluster tests
var (
	envTestbedInfoJsonPath = "TESTBEDINFO_JSON"
)

// Config secret testuser credentials
var (
	configSecretTestUser1Password = "VMware!23"
	configSecretTestUser2Password = "VMware!234"
	configSecretTestUser1         = "testuser1"
	configSecretTestUser2         = "testuser2"
)

// Nimbus generated passwords
var (
	nimbusK8sVmPwd = "NIMBUS_K8S_VM_PWD"
	nimbusEsxPwd   = "ESX_PWD"
	nimbusVcPwd    = "VC_PWD"
	vcUIPwd        = "VC_ADMIN_PWD"
)

// volume allocation types for cns volumes
var (
	thinAllocType = "Conserve space when possible"
	eztAllocType  = "Fully initialized"
	lztAllocType  = "Reserve space"
)

// For Preferential datatsore
var (
	preferredDatastoreRefreshTimeInterval = 1
	preferredDatastoreTimeOutInterval     = 1 * time.Minute
	preferredDSCat                        = "cns.vmware.topology-preferred-datastores"
	preferredTagDesc                      = "preferred datastore tag"
	nfsStoragePolicyName                  = "NFS_STORAGE_POLICY_NAME"
	nfstoragePolicyDatastoreUrl           = "NFS_STORAGE_POLICY_DATASTORE_URL"
	workerClusterMap                      = "WORKER_CLUSTER_MAP"
	datastoreClusterMap                   = "DATASTORE_CLUSTER_MAP"
)

// For multivc
var (
	envSharedDatastoreURLVC1          = "SHARED_VSPHERE_DATASTORE_URL_VC1"
	envSharedDatastoreURLVC2          = "SHARED_VSPHERE_DATASTORE_URL_VC2"
	envStoragePolicyNameToDeleteLater = "STORAGE_POLICY_TO_DELETE_LATER"
	envMultiVCSetupType               = "MULTI_VC_SETUP_TYPE"
	envStoragePolicyNameVC1           = "STORAGE_POLICY_VC1"
	envStoragePolicyNameInVC1VC2      = "STORAGE_POLICY_NAME_COMMON_IN_VC1_VC2"
	envPreferredDatastoreUrlVC1       = "PREFERRED_DATASTORE_URL_VC1"
	envPreferredDatastoreUrlVC2       = "PREFERRED_DATASTORE_URL_VC2"
	envTestbedInfoJsonPathVC1         = "TESTBEDINFO_JSON_VC1"
	envTestbedInfoJsonPathVC2         = "TESTBEDINFO_JSON_VC2"
	envTestbedInfoJsonPathVC3         = "TESTBEDINFO_JSON_VC3"
)

// VolumeSnapshotClass env variables for tkg-snapshot
var (
	envVolSnapClassDel = "VOLUME_SNAPSHOT_CLASS_DELETE"
	deletionPolicy     = "Delete"
)

// windows env variables
var (
	envWindowsUser    = "WINDOWS_USER"
	envWindowsPwd     = "WINDOWS_PWD"
	invalidNtfsFSType = "NtFs1"
	ntfsFSType        = "NTFS"
	windowsImageOnMcr = "servercore"
	windowsExecCmd    = "while (1) " +
		" { Add-Content -Encoding Ascii /mnt/volume1/fstype.txt $([System.IO.DriveInfo]::getdrives() " +
		"| Where-Object {$_.DriveType -match 'Fixed'} | Select-Object -Property DriveFormat); sleep 1 }"
	windowsExecRWXCommandPod = "while (1) " +
		" { Add-Content /mnt/volume1/Pod.html 'Hello message from Pod'; sleep 1 }"
	windowsExecRWXCommandPod1 = "while (1) " +
		" { Add-Content /mnt/volume1/Pod1.html 'Hello message from Pod1'; sleep 1 }"
)

// multiSvc env variables
var (
	vcSessionWaitTime                   = 5 * time.Minute
	envStoragePolicyNameForSharedDsSvc1 = "STORAGE_POLICY_FOR_SHARED_DATASTORES_SVC1"
	envStoragePolicyNameForSharedDsSvc2 = "STORAGE_POLICY_FOR_SHARED_DATASTORES_SVC2"
	envSupervisorClusterNamespace1      = "SVC_NAMESPACE1"
	envNfsDatastoreName                 = "NFS_DATASTORE_NAME"
	envNfsDatastoreIP                   = "NFS_DATASTORE_IP"
	pwdRotationTimeout                  = 10 * time.Minute
	roleCnsDatastore                    = "CNS-SUPERVISOR-DATASTORE"
	roleCnsSearchAndSpbm                = "CNS-SUPERVISOR-SEARCH-AND-SPBM"
	roleCnsHostConfigStorageAndCnsVm    = "CNS-SUPERVISOR-HOST-CONFIG-STORAGE-AND-CNS-VM"
)

// For rwx
var (
	envVsanDsStoragePolicyCluster1 = "VSAN_DATASTORE_CLUSTER1_STORAGE_POLICY"
	envVsanDsStoragePolicyCluster3 = "VSAN_DATASTORE_CLUSTER3_STORAGE_POLICY"
	envNonVsanDsUrl                = "NON_VSAN_DATASTOREURL"
	envVsanDsUrlCluster3           = "VSAN_DATASTOREURL_CLUSTER3"
	envRemoteDatastoreUrl          = "REMOTE_DATASTORE_URL"
	envTopologySetupType           = "TOPOLOGY_SETUP_TYPE"
)

// For management workload domain isolation
var (
	envZonal2StoragePolicyName            = "ZONAL2_STORAGE_POLICY_IMM"
	envZonal2StoragePolicyNameLateBidning = "ZONAL2_STORAGE_POLICY_WFFC"
	envZonal1StoragePolicyName            = "ZONAL1_STORAGE_POLICY_IMM"
	envZonal3StoragePolicyName            = "ZONAL3_STORAGE_POLICY_IMM"
	topologyDomainIsolation               = "Workload_Management_Isolation"
	envIsolationSharedStoragePolicyName   = "WORKLOAD_ISOLATION_SHARED_STORAGE_POLICY"
	envSharedZone2Zone4StoragePolicyName  = "SHARED_ZONE2_ZONE4_STORAGE_POLICY_IMM"
	envSharedZone2Zone4DatastoreUrl       = "SHARED_ZONE2_ZONE4_DATASTORE_URL"
)

// storage policy usages for storage quota validation
var usageSuffixes = []string{
	"-pvc-usage",
	"-latebinding-pvc-usage",
	"-snapshot-usage",
	"-latebinding-snapshot-usage",
	"-vm-usage",
	"-latebinding-vm-usage",
}

const (
	storagePolicyUsagePollInterval = 10 * time.Second
	storagePolicyUsagePollTimeout  = 1 * time.Minute
)

// GetAndExpectEnvVar returns the value of an environment variable or fails the regression if it's not set.
func GetAndExpectEnvVar(varName string) string {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := logger.GetLogger(ctx)

	varValue, exists := os.LookupEnv(varName)
	if !exists {
		log.Fatalf("Required environment variable not found: %s", varName)
	}
	return varValue
}

// GetAndExpectStringEnvVar parses a string from env variable.
func GetAndExpectStringEnvVar(varName string) string {
	varValue := os.Getenv(varName)
	gomega.Expect(varValue).NotTo(gomega.BeEmpty(), "ENV "+varName+" is not set")
	return varValue
}

// GetAndExpectIntEnvVar parses an int from env variable.
func GetAndExpectIntEnvVar(varName string) int {
	varValue := GetAndExpectStringEnvVar(varName)
	varIntValue, err := strconv.Atoi(varValue)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+varName)
	return varIntValue
}

// GetBoolEnvVarOrDefault returns the boolean value of an environment variable or return default if it's not set
func GetBoolEnvVarOrDefault(varName string, defaultVal bool) bool {
	varValue, exists := os.LookupEnv(varName)
	if !exists {
		return defaultVal
	}

	varBoolValue, err := strconv.ParseBool(varValue)
	if err != nil {
		ctx := context.Background()
		log := logger.GetLogger(ctx)
		log.Warnf("Invalid boolean value for %s: '%s'. Using default: %v", varName, varValue, defaultVal)
		return defaultVal
	}

	return varBoolValue
}

// GetStringEnvVarOrDefault returns the string value of an environment variable or return default if it's not set
func GetStringEnvVarOrDefault(varName string, defaultVal string) string {
	varValue, exists := os.LookupEnv(varName)
	if !exists || strings.TrimSpace(varValue) == "" {
		return defaultVal
	}
	return varValue
}

/*
GetorIgnoreStringEnvVar, retrieves the value of an environment variable while logging
a warning if the variable is not set.
*/
func GetorIgnoreStringEnvVar(varName string) string {
	varValue, exists := os.LookupEnv(varName)
	if !exists {
		missingEnvVars = append(missingEnvVars, varName)
	}
	return varValue
}

// setClusterFlavor sets the boolean variables w.r.t the Cluster type.
func setClusterFlavor(clusterFlavor cnstypes.CnsClusterFlavor) {
	switch clusterFlavor {
	case cnstypes.CnsClusterFlavorWorkload:
		supervisorCluster = true
	case cnstypes.CnsClusterFlavorGuest:
		guestCluster = true
	default:
		vanillaCluster = true
	}

	// Check if the access mode is set for File volume setups
	kind := os.Getenv("ACCESS_MODE")
	if strings.TrimSpace(string(kind)) == "RWX" {
		rwxAccessMode = true
	}

	// Check if its the vcptocsi tesbed
	mode := os.Getenv("VCPTOCSI")
	if strings.TrimSpace(string(mode)) == "1" {
		vcptocsi = true
	}
	//Check if its windows env
	workerNode := os.Getenv("WORKER_TYPE")
	if strings.TrimSpace(string(workerNode)) == "WINDOWS" {
		windowsEnv = true
	}

	// Check if it's multiple supervisor cluster setup
	svcType := os.Getenv("SUPERVISOR_TYPE")
	if strings.TrimSpace(string(svcType)) == "MULTI_SVC" {
		multipleSvc = true
	}

	//Check if it is multivc env
	topologyType := os.Getenv("TOPOLOGY_TYPE")
	if strings.TrimSpace(string(topologyType)) == "MULTI_VC" {
		multivc = true
	}

	//Check if its stretched SVC testbed
	testbedType := os.Getenv("STRETCHED_SVC")
	if strings.TrimSpace(string(testbedType)) == "1" {
		stretchedSVC = true
	}
}

var (
	// reading port numbers for VC, Master VM and ESXi from export variables
	envVc1SshdPortNum       = "VC1_SSHD_PORT_NUM"
	envVc2SshdPortNum       = "VC2_SSHD_PORT_NUM"
	envVc3SshdPortNum       = "VC3_SSHD_PORT_NUM"
	envMasterIP1SshdPortNum = "MASTER_IP1_SSHD_PORT_NUM"
	envMasterIP2SshdPortNum = "MASTER_IP2_SSHD_PORT_NUM"
	envMasterIP3SshdPortNum = "MASTER_IP3_SSHD_PORT_NUM"
	envEsx1PortNum          = "ESX1_SSHD_PORT_NUM"
	envEsx2PortNum          = "ESX2_SSHD_PORT_NUM"
	envEsx3PortNum          = "ESX3_SSHD_PORT_NUM"
	envEsx4PortNum          = "ESX4_SSHD_PORT_NUM"
	envEsx5PortNum          = "ESX5_SSHD_PORT_NUM"
	envEsx6PortNum          = "ESX6_SSHD_PORT_NUM"
	envEsx7PortNum          = "ESX7_SSHD_PORT_NUM"
	envEsx8PortNum          = "ESX8_SSHD_PORT_NUM"
	envEsx9PortNum          = "ESX9_SSHD_PORT_NUM"
	envEsx10PortNum         = "ESX10_SSHD_PORT_NUM"

	// reading IPs for VC, Master VM and ESXi from export variables
	envVcIP1     = "VC_IP1"
	envVcIP2     = "VC_IP2"
	envVcIP3     = "VC_IP3"
	envMasterIP1 = "MASTER_IP1"
	envMasterIP2 = "MASTER_IP2"
	envMasterIP3 = "MASTER_IP3"
	envEsxIp1    = "ESX1_IP"
	envEsxIp2    = "ESX2_IP"
	envEsxIp3    = "ESX3_IP"
	envEsxIp4    = "ESX4_IP"
	envEsxIp5    = "ESX5_IP"
	envEsxIp6    = "ESX6_IP"
	envEsxIp7    = "ESX7_IP"
	envEsxIp8    = "ESX8_IP"
	envEsxIp9    = "ESX9_IP"
	envEsxIp10   = "ESX10_IP"

	// default port declaration for each IP
	vcIp1SshPortNum       = sshdPort
	vcIp2SshPortNum       = sshdPort
	vcIp3SshPortNum       = sshdPort
	esxIp1PortNum         = sshdPort
	esxIp2PortNum         = sshdPort
	esxIp3PortNum         = sshdPort
	esxIp4PortNum         = sshdPort
	esxIp5PortNum         = sshdPort
	esxIp6PortNum         = sshdPort
	esxIp7PortNum         = sshdPort
	esxIp8PortNum         = sshdPort
	esxIp9PortNum         = sshdPort
	esxIp10PortNum        = sshdPort
	k8sMasterIp1PortNum   = sshdPort
	k8sMasterIp2PortNum   = sshdPort
	k8sMasterIp3PortNum   = sshdPort
	defaultVcAdminPortNum = "443"

	// global variables declared
	esxIp1             = ""
	esxIp2             = ""
	esxIp3             = ""
	esxIp4             = ""
	esxIp5             = ""
	esxIp6             = ""
	esxIp7             = ""
	esxIp8             = ""
	esxIp9             = ""
	esxIp10            = ""
	vcAddress          = ""
	vcAddress2         = ""
	vcAddress3         = ""
	masterIP1          = ""
	masterIP2          = ""
	masterIP3          = ""
	ipPortMap          = make(map[string]string)
	missingEnvVars     []string
	defaultlocalhostIP = "127.0.0.1"
)

/*
The setSShdPort function dynamically configures SSH port mappings for a vSphere test environment by reading
environment variables and adapting to the network type and topology.
It sets up SSH access to vCenter servers, ESXi hosts, and Kubernetes masters based on the environment configuration.
*/
func safeInsertToMap(key, value string) {
	if key != "" && value != "" {
		ipPortMap[key] = value
	}
}

func setSShdPort() {
	vcAddress = GetAndExpectEnvVar(envVcIP1)
	isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)

	if multivc {
		vcAddress2 = GetAndExpectEnvVar(envVcIP2)
		vcAddress3 = GetAndExpectEnvVar(envVcIP3)
	}

	if isPrivateNetwork {
		if multivc {
			vcIp2SshPortNum = GetorIgnoreStringEnvVar(envVc2SshdPortNum)
			vcIp3SshPortNum = GetorIgnoreStringEnvVar(envVc3SshdPortNum)

			safeInsertToMap(vcAddress2, vcIp2SshPortNum)
			safeInsertToMap(vcAddress3, vcIp3SshPortNum)
		}

		// reading masterIP and its port number
		masterIP1 = GetorIgnoreStringEnvVar(envMasterIP1)
		masterIP2 = GetorIgnoreStringEnvVar(envMasterIP2)
		masterIP3 = GetorIgnoreStringEnvVar(envMasterIP3)
		k8sMasterIp1PortNum = GetorIgnoreStringEnvVar(envMasterIP1SshdPortNum)
		k8sMasterIp2PortNum = GetorIgnoreStringEnvVar(envMasterIP2SshdPortNum)
		k8sMasterIp3PortNum = GetorIgnoreStringEnvVar(envMasterIP3SshdPortNum)

		vcIp1SshPortNum = GetorIgnoreStringEnvVar(envVc1SshdPortNum)

		// reading esxi ip and its port
		esxIp1 = GetorIgnoreStringEnvVar(envEsxIp1)
		esxIp1PortNum = GetorIgnoreStringEnvVar(envEsx1PortNum)
		esxIp2PortNum = GetorIgnoreStringEnvVar(envEsx2PortNum)
		esxIp3PortNum = GetorIgnoreStringEnvVar(envEsx3PortNum)
		esxIp4PortNum = GetorIgnoreStringEnvVar(envEsx4PortNum)
		esxIp5PortNum = GetorIgnoreStringEnvVar(envEsx5PortNum)
		esxIp6PortNum = GetorIgnoreStringEnvVar(envEsx6PortNum)
		esxIp7PortNum = GetorIgnoreStringEnvVar(envEsx7PortNum)
		esxIp8PortNum = GetorIgnoreStringEnvVar(envEsx8PortNum)
		esxIp9PortNum = GetorIgnoreStringEnvVar(envEsx9PortNum)
		esxIp10PortNum = GetorIgnoreStringEnvVar(envEsx10PortNum)

		esxIp2 = GetorIgnoreStringEnvVar(envEsxIp2)
		esxIp3 = GetorIgnoreStringEnvVar(envEsxIp3)
		esxIp4 = GetorIgnoreStringEnvVar(envEsxIp4)
		esxIp5 = GetorIgnoreStringEnvVar(envEsxIp5)
		esxIp6 = GetorIgnoreStringEnvVar(envEsxIp6)
		esxIp7 = GetorIgnoreStringEnvVar(envEsxIp7)
		esxIp8 = GetorIgnoreStringEnvVar(envEsxIp8)
		esxIp9 = GetorIgnoreStringEnvVar(envEsxIp9)
		esxIp10 = GetorIgnoreStringEnvVar(envEsxIp10)

		safeInsertToMap(vcAddress, vcIp1SshPortNum)
		safeInsertToMap(masterIP1, k8sMasterIp1PortNum)
		safeInsertToMap(masterIP2, k8sMasterIp2PortNum)
		safeInsertToMap(masterIP3, k8sMasterIp3PortNum)
		safeInsertToMap(esxIp1, esxIp1PortNum)
		safeInsertToMap(esxIp2, esxIp2PortNum)
		safeInsertToMap(esxIp3, esxIp3PortNum)
		safeInsertToMap(esxIp4, esxIp4PortNum)
		safeInsertToMap(esxIp5, esxIp5PortNum)
		safeInsertToMap(esxIp6, esxIp6PortNum)
		safeInsertToMap(esxIp7, esxIp7PortNum)
		safeInsertToMap(esxIp8, esxIp8PortNum)
		safeInsertToMap(esxIp9, esxIp9PortNum)
		safeInsertToMap(esxIp10, esxIp10PortNum)
	}

	if len(missingEnvVars) > 0 {
		ctx := context.Background()
		log := logger.GetLogger(ctx)
		log.Warnf("Missing environment variables: %v", strings.Join(missingEnvVars, ", "))
	}
}
