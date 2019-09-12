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

	"github.com/onsi/gomega"
)

const (
	envSharedDatastoreURL                      = "SHARED_VSPHERE_DATASTORE_URL"
	destinationDatastoreURL                    = "DESTINATION_VSPHERE_DATASTORE_URL"
	envNonSharedStorageClassDatastoreURL       = "NONSHARED_VSPHERE_DATASTORE_URL"
	envInaccessibleZoneDatastoreURL            = "INACCESSIBLE_ZONE_VSPHERE_DATASTORE_URL"
	scParamDatastoreURL                        = "DatastoreURL"
	diskSize                                   = "2Gi"
	diskSizeInMb                               = int64(2048)
	e2evSphereCSIBlockDriverName               = "csi.vsphere.vmware.com"
	envVolumeOperationsScale                   = "VOLUME_OPS_SCALE"
	envStoragePolicyNameForSharedDatastores    = "STORAGE_POLICY_FOR_SHARED_DATASTORES"
	envStoragePolicyNameForNonSharedDatastores = "STORAGE_POLICY_FOR_NONSHARED_DATASTORES"
	envStoragePolicyNameFromInaccessibleZone   = "STORAGE_POLICY_FROM_INACCESSIBLE_ZONE"
	scParamStoragePolicyName                   = "StoragePolicyName"
	poll                                       = 2 * time.Second
	pollTimeout                                = 5 * time.Minute
	pollTimeoutShort                           = 1 * time.Minute / 2
	scParamStoragePolicyID                     = "StoragePolicyId"
	envK8SVanillaTestSetup                     = "K8S_VANILLA_ENVIRONMENT"
	envSupervisorClusterNamespace              = "SVC_NAMESPACE"
	envPandoraSyncWaitTime                     = "PANDORA_SYNC_WAIT_TIME"
	envFullSyncWaitTime                        = "FULL_SYNC_WAIT_TIME"
	defaultPandoraSyncWaitTime                 = 90
	defaultFullSyncWaitTime                    = 1800
	sleepTimeOut                               = 30
	k8sPodTerminationTimeOut                   = 7 * time.Minute
	supervisorClusterOperationsTimeout         = 3 * time.Minute
	vsanhealthServiceName                      = "vsan-health"
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
	syncerStatefulsetName                      = "vsphere-csi-metadata-syncer"
	rqStorageType                              = ".storageclass.storage.k8s.io/requests.storage"
	rqLimit                                    = "10Gi"
	vmUUIDLabel                                = "vmware-system-vm-uuid"
	quotaName                                  = "cns-test-quota"
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
