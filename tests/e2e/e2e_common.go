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
	busyBoxImageOnGcr                          = "harbor-repo.vmware.com/csi/busybox:1.35"
	nginxImage                                 = "registry.k8s.io/nginx-slim:0.26"
	nginxImage4upg                             = "registry.k8s.io/nginx-slim:0.27"
	configSecret                               = "vsphere-config-secret"
	contollerClusterKubeConfig                 = "CONTROLLER_CLUSTER_KUBECONFIG"
	crdCNSNodeVMAttachment                     = "cnsnodevmattachments"
	crdCNSVolumeMetadatas                      = "cnsvolumemetadatas"
	crdCNSFileAccessConfig                     = "cnsfileaccessconfigs"
	crdtriggercsifullsyncsName                 = "csifullsync"
	crdGroup                                   = "cns.vmware.com"
	crdVersion                                 = "v1alpha1"
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
	envEsxHostIP                               = "ESX_TEST_HOST_IP"
	envFileServiceDisabledSharedDatastoreURL   = "FILE_SERVICE_DISABLED_SHARED_VSPHERE_DATASTORE_URL"
	envFullSyncWaitTime                        = "FULL_SYNC_WAIT_TIME"
	envInaccessibleZoneDatastoreURL            = "INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL"
	envNonSharedStorageClassDatastoreURL       = "NONSHARED_VSPHERE_DATASTORE_URL"
	envPandoraSyncWaitTime                     = "PANDORA_SYNC_WAIT_TIME"
	envK8sNodesUpWaitTime                      = "K8S_NODES_UP_WAIT_TIME"
	envRegionZoneWithNoSharedDS                = "TOPOLOGY_WITH_NO_SHARED_DATASTORE"
	envRegionZoneWithSharedDS                  = "TOPOLOGY_WITH_SHARED_DATASTORE"
	envSharedDatastoreURL                      = "SHARED_VSPHERE_DATASTORE_URL"
	envSharedVVOLDatastoreURL                  = "SHARED_VVOL_DATASTORE_URL"
	envSharedNFSDatastoreURL                   = "SHARED_NFS_DATASTORE_URL"
	envSharedVMFSDatastoreURL                  = "SHARED_VMFS_DATASTORE_URL"
	envSharedVMFSDatastore2URL                 = "SHARED_VMFS_DATASTORE2_URL"
	envStoragePolicyNameForNonSharedDatastores = "STORAGE_POLICY_FOR_NONSHARED_DATASTORES"
	envStoragePolicyNameForSharedDatastores    = "STORAGE_POLICY_FOR_SHARED_DATASTORES"
	envStoragePolicyNameForSharedDatastores2   = "STORAGE_POLICY_FOR_SHARED_DATASTORES_2"
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
	envVolumeOperationsScale                   = "VOLUME_OPS_SCALE"
	envComputeClusterName                      = "COMPUTE_CLUSTER_NAME"
	execCommand                                = "/bin/df -T /mnt/volume1 | " +
		"/bin/awk 'FNR == 2 {print $2}' > /mnt/volume1/fstype && while true ; do sleep 2 ; done"
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
	regionKey                                 = "failure-domain.beta.kubernetes.io/region"
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
	scParamStoragePolicyID                    = "StoragePolicyId"
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
	vpxdServiceName                           = "vpxd"
	vpxdReducedTaskTimeoutSecsInt             = 90
	vSphereCSIControllerPodNamePrefix         = "vsphere-csi-controller"
	vmUUIDLabel                               = "vmware-system-vm-uuid"
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
	zoneKey                                   = "failure-domain.beta.kubernetes.io/zone"
	tkgAPI                                    = "/apis/run.tanzu.vmware.com/v1alpha1/namespaces" +
		"/test-gc-e2e-demo-ns/tanzukubernetesclusters/"
	topologykey                                = "topology.csi.vmware.com"
	topologyMap                                = "TOPOLOGY_MAP"
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
	vmcPrdEndpoint                             = "https://vmc.vmware.com/vmc/api/orgs/"
	authAPI                                    = "https://console.cloud.vmware.com/csp/gateway/am/api/auth" +
		"/api-tokens/authorize"
)

// The following variables are required to know cluster type to run common e2e
// tests. These variables will be set once during test suites initialization.
var (
	vanillaCluster    bool
	supervisorCluster bool
	guestCluster      bool
	rwxAccessMode     bool
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
	scAnnotation4Statefulset        = "volume.beta.kubernetes.io/storage-class"
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

// CSI Internal FSSs
var (
	useCsiNodeID = "use-csinode-id"
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
}
