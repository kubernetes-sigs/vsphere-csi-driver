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
	"os"
	"strconv"
	"strings"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"

	"github.com/onsi/gomega"
)

const (
	adminUser                                  = "Administrator@vsphere.local"
	apiServerIPs                               = "API_SERVER_IPS"
	attacherContainerName                      = "csi-attacher"
	nginxImage                                 = "registry.k8s.io/nginx-slim:0.26"
	nginxImage4upg                             = "registry.k8s.io/nginx-slim:0.27"
	retainClaimPolicy                          = "Retain"
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
	diskSize                                   = "2Gi"
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
	filePathPod1                              = "/mnt/volume1/Pod1.html"
	filePathFsType                            = "/mnt/volume1/fstype"
	fullSyncFss                               = "trigger-csi-fullsync"
	gcNodeUser                                = "vmware-system-user"
	gcKubeConfigPath                          = "GC_KUBE_CONFIG"
	healthGreen                               = "green"
	healthRed                                 = "red"
	healthStatusAccessible                    = "accessible"
	healthStatusInAccessible                  = "inaccessible"
	healthStatusWaitTime                      = 2 * time.Minute
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
	pollTimeout                               = 5 * time.Minute
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
	vsphereTKGSystemNamespace                 = "vmware-system-tkg"
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
	envStoragePolicyNameForNkpEncryption       = "STORAGE_POLICY_FOR_NKP_ENCRYPTION"
	topologyCluster                            = "TOPOLOGY_CLUSTERS"
	topologyLength                             = 5
	tkgshaTopologyLevels                       = 1
	vmClassBestEffortSmall                     = "best-effort-small"
	vmcPrdEndpoint                             = "https://vmc.vmware.com/vmc/api/orgs/"
	vsphereClusterIdConfigMapName              = "vsphere-csi-cluster-id"
	authAPI                                    = "https://console.cloud.vmware.com/csp/gateway/am/api/auth" +
		"/api-tokens/authorize"
)

/*
// test suite labels

flaky -> label include the testcases which fails intermittently
disruptive -> label include the testcases which are disruptive in nature
vanilla -> label include the testcases for block, file, configSecret, topology etc.
stable -> label include the testcases which do not fail
longRunning -> label include the testcases which takes longer time for completion
p0 -> label include the testcases which are P0
p1 -> label include the testcases which are P1
p2 -> label include the testcases which are P2
semiAutomated -> label include the testcases which are semi-automated
newTests -> label include the testcases which are newly automated
core -> label include the testcases specific to block or file
level2 -> label include the level-2 topology testcases or pipeline specific
level5 -> label include the level-5 topology testcases
customPort -> label include the testcases running on vCenter custom port <VC:444>
deprecated ->label include the testcases which are no longer in execution
*/
const (
	flaky               = "flaky"
	disruptive          = "disruptive"
	wcp                 = "wcp"
	tkg                 = "tkg"
	vanilla             = "vanilla"
	preferential        = "preferential"
	vsphereConfigSecret = "vsphereConfigSecret"
	snapshot            = "snapshot"
	stable              = "stable"
	newTest             = "newTest"
	multiVc             = "multiVc"
	block               = "block"
	file                = "file"
	core                = "core"
	hci                 = "hci"
	p0                  = "p0"
	p1                  = "p1"
	p2                  = "p2"
	vsanStretch         = "vsanStretch"
	longRunning         = "longRunning"
	deprecated          = "deprecated"
	vmc                 = "vmc"
	tkgsHA              = "tkgsHA"
	thickThin           = "thickThin"
	customPort          = "customPort"
	windows             = "windows"
	semiAutomated       = "semiAutomated"
	level2              = "level2"
	level5              = "level5"
	negative            = "negative"
	listVolume          = "listVolume"
	multiSvc            = "multiSvc"
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
	roleCnsDatastore                    = "CNS-Datastore"
	roleCnsSearchAndSpbm                = "CNS-SEARCH-AND-SPBM"
	roleCnsHostConfigStorageAndCnsVm    = "CNS-HOST-CONFIG-STORAGE-AND-CNS-VM"
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

// GetAndExpectBoolEnvVar parses a boolean from env variable.
func GetAndExpectBoolEnvVar(varName string) bool {
	varValue := GetAndExpectStringEnvVar(varName)
	varBoolValue, err := strconv.ParseBool(varValue)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+varName)
	return varBoolValue
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
}
