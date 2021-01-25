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
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"

	"github.com/onsi/gomega"
)

const (
	adminPassword                              = "Admin!23"
	configSecret                               = "vsphere-config-secret"
	crdCNSNodeVMAttachment                     = "cnsnodevmattachments"
	crdCNSVolumeMetadatas                      = "cnsvolumemetadatas"
	crdGroup                                   = "cns.vmware.com"
	crdVersion                                 = "v1alpha1"
	csiSystemNamespace                         = "vmware-system-csi"
	defaultFullSyncIntervalInMin               = "30"
	defaultFullSyncWaitTime                    = 1800
	defaultPandoraSyncWaitTime                 = 90
	defaultVCRebootWaitTime                    = 180
	destinationDatastoreURL                    = "DESTINATION_VSPHERE_DATASTORE_URL"
	diskSize                                   = "2Gi"
	diskSizeInMb                               = int64(2048)
	diskSizeInMinMb                            = int64(200)
	e2eTestPassword                            = "E2E-test-password!23"
	e2evSphereCSIDriverName                    = "csi.vsphere.vmware.com"
	envFileServiceDisabledSharedDatastoreURL   = "FILE_SERVICE_DISABLED_SHARED_VSPHERE_DATASTORE_URL"
	envFullSyncWaitTime                        = "FULL_SYNC_WAIT_TIME"
	envInaccessibleZoneDatastoreURL            = "INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL"
	envNonSharedStorageClassDatastoreURL       = "NONSHARED_VSPHERE_DATASTORE_URL"
	envPandoraSyncWaitTime                     = "PANDORA_SYNC_WAIT_TIME"
	envVCRebootWaitTime                        = "VC_REBOOT_WAIT_TIME"
	envRegionZoneWithNoSharedDS                = "TOPOLOGY_WITH_NO_SHARED_DATASTORE"
	envRegionZoneWithSharedDS                  = "TOPOLOGY_WITH_SHARED_DATASTORE"
	envSharedDatastoreURL                      = "SHARED_VSPHERE_DATASTORE_URL"
	envSharedVVOLDatastoreURL                  = "SHARED_VVOL_DATASTORE_URL"
	envSharedNFSDatastoreURL                   = "SHARED_NFS_DATASTORE_URL"
	envSharedVMFSDatastoreURL                  = "SHARED_VMFS_DATASTORE_URL"
	envStoragePolicyNameForNonSharedDatastores = "STORAGE_POLICY_FOR_NONSHARED_DATASTORES"
	envStoragePolicyNameForSharedDatastores    = "STORAGE_POLICY_FOR_SHARED_DATASTORES"
	envStoragePolicyNameFromInaccessibleZone   = "STORAGE_POLICY_FROM_INACCESSIBLE_ZONE"
	envStoragePolicyNameWithThickProvision     = "STORAGE_POLICY_WITH_THICK_PROVISIONING"
	envSupervisorClusterNamespace              = "SVC_NAMESPACE"
	envSupervisorClusterNamespaceToDelete      = "SVC_NAMESPACE_TO_DELETE"
	envTopologyWithOnlyOneNode                 = "TOPOLOGY_WITH_ONLY_ONE_NODE"
	envVolumeOperationsScale                   = "VOLUME_OPS_SCALE"
	esxPassword                                = "ca$hc0w"
	execCommand                                = "/bin/df -T /mnt/volume1 | /bin/awk 'FNR == 2 {print $2}' > /mnt/volume1/fstype && while true ; do sleep 2 ; done"
	ext3FSType                                 = "ext3"
	ext4FSType                                 = "ext4"
	fcdName                                    = "BasicStaticFCD"
	fileSizeInMb                               = int64(2048)
	healthGreen                                = "green"
	healthRed                                  = "red"
	healthStatusAccessible                     = "accessible"
	healthStatusInAccessible                   = "inaccessible"
	healthStatusWaitTime                       = 2 * time.Minute
	hostRecoveryTime                           = 5 * time.Minute
	invalidFSType                              = "ext10"
	k8sPodTerminationTimeOut                   = 7 * time.Minute
	k8sPodTerminationTimeOutLong               = 10 * time.Minute
	kcmManifest                                = "/etc/kubernetes/manifests/kube-controller-manager.yaml"
	kubeAPIPath                                = "/etc/kubernetes/manifests/"
	kubeAPIfile                                = "kube-apiserver.yaml"
	kubeAPIRecoveryTime                        = 1 * time.Minute
	kubeSystemNamespace                        = "kube-system"
	nfs4FSType                                 = "nfs4"
	passorwdFilePath                           = "/etc/vmware/wcp/.storageUser"
	podContainerCreatingState                  = "ContainerCreating"
	poll                                       = 2 * time.Second
	pollTimeout                                = 5 * time.Minute
	pollTimeoutShort                           = 1 * time.Minute
	psodTime                                   = "120"
	pvcHealthAnnotation                        = "volumehealth.storage.kubernetes.io/health"
	quotaName                                  = "cns-test-quota"
	regionKey                                  = "failure-domain.beta.kubernetes.io/region"
	resizePollInterval                         = 2 * time.Second
	rqLimit                                    = "200Gi"
	defaultrqLimit                             = "20Gi"
	rqStorageType                              = ".storageclass.storage.k8s.io/requests.storage"
	scParamDatastoreURL                        = "DatastoreURL"
	scParamFsType                              = "csi.storage.k8s.io/fstype"
	scParamStoragePolicyID                     = "StoragePolicyId"
	scParamStoragePolicyName                   = "StoragePolicyName"
	sleepTimeOut                               = 30
	spsServiceName                             = "sps"
	sshdPort                                   = "22"
	startOperation                             = "start"
	stopOperation                              = "stop"
	supervisorClusterOperationsTimeout         = 3 * time.Minute
	svOperationTimeout                         = 240 * time.Second
	svStorageClassName                         = "SVStorageClass"
	totalResizeWaitPeriod                      = 10 * time.Minute
	vSphereCSIControllerPodNamePrefix          = "vsphere-csi-controller"
	vmUUIDLabel                                = "vmware-system-vm-uuid"
	vsanHealthServiceWaitTime                  = 15
	vsanhealthServiceName                      = "vsan-health"
	vsphereCloudProviderConfiguration          = "vsphere-cloud-provider.conf"
	vsphereControllerManager                   = "vmware-system-tkg-controller-manager"
	vsphereTKGSystemNamespace                  = "vmware-system-tkg"
	waitTimeForCNSNodeVMAttachmentReconciler   = 30 * time.Second
	wcpServiceName                             = "wcp"
	zoneKey                                    = "failure-domain.beta.kubernetes.io/zone"
	envVmdkDiskURL                             = "DISK_URL_PATH"
	vsanDefaultStorageClassInSVC               = "vsan-default-storage-policy"
	vsanDefaultStoragePolicyName               = "vSAN Default Storage Policy"
	busyBoxImageOnGcr                          = "gcr.io/google_containers/busybox:1.27"
)

// The following variables are required to know cluster type to run common e2e tests
// These variables will be set once during test suites initialization.
var (
	vanillaCluster    bool
	supervisorCluster bool
	guestCluster      bool
)

// For VCP to CSI migration tests
var (
	envSharedDatastoreName          = "SHARED_VSPHERE_DATASTORE_NAME"
	vcpProvisionerName              = "kubernetes.io/vsphere-volume"
	vcpScParamDatastoreName         = "datastore"
	vcpScParamPolicyName            = "storagePolicyName"
	migratedToAnnotation            = "pv.kubernetes.io/migrated-to"
	pvcAnnotationStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	pvAnnotationProvisionedBy       = "pv.kubernetes.io/provisioned-by"
	nodeMapper                      = &NodeMapper{}
)

// GetAndExpectStringEnvVar parses a string from env variable
func GetAndExpectStringEnvVar(varName string) string {
	varValue := os.Getenv(varName)
	gomega.Expect(varValue).NotTo(gomega.BeEmpty(), "ENV "+varName+" is not set")
	return varValue
}

// GetAndExpectIntEnvVar parses an int from env variable
func GetAndExpectIntEnvVar(varName string) int {
	varValue := GetAndExpectStringEnvVar(varName)
	varIntValue, err := strconv.Atoi(varValue)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+varName)
	return varIntValue
}

// GetAndExpectBoolEnvVar parses a boolean from env variable
func GetAndExpectBoolEnvVar(varName string) bool {
	varValue := GetAndExpectStringEnvVar(varName)
	varBoolValue, err := strconv.ParseBool(varValue)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error Parsing "+varName)
	return varBoolValue
}

// setClusterFlavor sets the boolean variables w.r.t the Cluster type
func setClusterFlavor(clusterFlavor cnstypes.CnsClusterFlavor) {
	switch clusterFlavor {
	case cnstypes.CnsClusterFlavorWorkload:
		supervisorCluster = true
	case cnstypes.CnsClusterFlavorGuest:
		guestCluster = true
	default:
		vanillaCluster = true
	}
}

// isValueSet check whether the environment variable is set or not
func isValueSet(varName string) bool {
	varValue := os.Getenv(varName)
	if varValue == "" {
		return false
	}
	return true
}
