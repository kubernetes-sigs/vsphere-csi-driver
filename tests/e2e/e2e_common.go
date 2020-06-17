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
	envSharedDatastoreURL                      = "SHARED_VSPHERE_DATASTORE_URL"
	destinationDatastoreURL                    = "DESTINATION_VSPHERE_DATASTORE_URL"
	envNonSharedStorageClassDatastoreURL       = "NONSHARED_VSPHERE_DATASTORE_URL"
	envInaccessibleZoneDatastoreURL            = "INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL"
	envFileServiceDisabledSharedDatastoreURL   = "FILE_SERVICE_DISABLED_SHARED_VSPHERE_DATASTORE_URL"
	scParamDatastoreURL                        = "DatastoreURL"
	diskSize                                   = "2Gi"
	diskSizeInMb                               = int64(2048)
	e2evSphereCSIBlockDriverName               = "csi.vsphere.vmware.com"
	envVolumeOperationsScale                   = "VOLUME_OPS_SCALE"
	envStoragePolicyNameForSharedDatastores    = "STORAGE_POLICY_FOR_SHARED_DATASTORES"
	envStoragePolicyNameForNonSharedDatastores = "STORAGE_POLICY_FOR_NONSHARED_DATASTORES"
	envStoragePolicyNameFromInaccessibleZone   = "STORAGE_POLICY_FROM_INACCESSIBLE_ZONE"
	scParamStoragePolicyName                   = "StoragePolicyName"
	svStorageClassName                         = "SVStorageClass"
	poll                                       = 2 * time.Second
	pollTimeout                                = 5 * time.Minute
	pollTimeoutShort                           = 1 * time.Minute / 2
	scParamStoragePolicyID                     = "StoragePolicyId"
	scParamFsType                              = "csi.storage.k8s.io/fstype"
	envClusterFlavor                           = "CLUSTER_FLAVOR"
	envSupervisorClusterNamespace              = "SVC_NAMESPACE"
	envPandoraSyncWaitTime                     = "PANDORA_SYNC_WAIT_TIME"
	envFullSyncWaitTime                        = "FULL_SYNC_WAIT_TIME"
	defaultPandoraSyncWaitTime                 = 90
	defaultFullSyncWaitTime                    = 1800
	sleepTimeOut                               = 30
	k8sPodTerminationTimeOut                   = 7 * time.Minute
	supervisorClusterOperationsTimeout         = 3 * time.Minute
	k8sPodTerminationTimeOutLong               = 10 * time.Minute
	waitTimeForCNSNodeVMAttachmentReconciler   = 30 * time.Second
	healthStatusWaitTime                       = 2 * time.Minute
	healthStatusAccessible                     = "accessible"
	healthStatusInAccessible                   = "inaccessible"
	healthGreen                                = "green"
	healthYellow                               = "yellow"
	healthRed                                  = "red"
	vsanhealthServiceName                      = "vsan-health"
	spsServiceName                             = "sps"
	zoneKey                                    = "failure-domain.beta.kubernetes.io/zone"
	regionKey                                  = "failure-domain.beta.kubernetes.io/region"
	envRegionZoneWithNoSharedDS                = "TOPOLOGY_WITH_NO_SHARED_DATASTORE"
	envRegionZoneWithSharedDS                  = "TOPOLOGY_WITH_SHARED_DATASTORE"
	envTopologyWithOnlyOneNode                 = "TOPOLOGY_WITH_ONLY_ONE_NODE"
	ext4FSType                                 = "ext4"
	ext3FSType                                 = "ext3"
	invalidFSType                              = "ext10"
	execCommand                                = "/bin/df -T /mnt/volume1 | /bin/awk 'FNR == 2 {print $2}' > /mnt/volume1/fstype && while true ; do sleep 2 ; done"
	kubeSystemNamespace                        = "kube-system"
	csiSystemNamespace                         = "vmware-system-csi"
	syncerStatefulsetName                      = "vsphere-csi-metadata-syncer"
	rqStorageType                              = ".storageclass.storage.k8s.io/requests.storage"
	rqLimit                                    = "100Gi"
	vmUUIDLabel                                = "vmware-system-vm-uuid"
	quotaName                                  = "cns-test-quota"
	vSphereCSIControllerPodNamePrefix          = "vsphere-csi-controller"
	envK8SMaster1Name                          = "K8S_MASTER1_NAME"
	envK8SMaster2Name                          = "K8S_MASTER2_NAME"
	envK8SMaster3Name                          = "K8S_MASTER3_NAME"
	envK8SMaster1IP                            = "K8S_MASTER1_IP"
	envK8SMaster2IP                            = "K8S_MASTER2_IP"
	envK8SMaster3IP                            = "K8S_MASTER3_IP"
	nfs4FSType                                 = "nfs4"
	fcdName                                    = "BasicStaticFCD"
	stopOperation                              = "stop"
	startOperation                             = "start"
	sshdPort                                   = "22"
	vsanHealthServiceWaitTime                  = 15
	crdCNSNodeVMAttachment                     = "cnsnodevmattachments"
	crdCNSVolumeMetadata                       = "cnsvolumemetadatas"
	crdGroup                                   = "cns.vmware.com"
	crdVersion                                 = "v1alpha1"
	adminPassword                              = "Admin!23"
	e2eTestPassword                            = "E2E-test-password!23"
	vsphereCloudProviderConfiguration          = "vsphere-cloud-provider.conf"
)

const (
	// total time to wait for CSI controller plugin or file system resize to finish
	totalResizeWaitPeriod = 10 * time.Minute
	resizePollInterval    = 2 * time.Second
)

// The following variables are required to know cluster type to run common e2e tests
// These variables will be set once during test suites initialization.
var (
	vanillaCluster    bool
	supervisorCluster bool
	guestCluster      bool
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
