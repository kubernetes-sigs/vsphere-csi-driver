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

package syncer

import (
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	volumes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	k8s "sigs.k8s.io/vsphere-csi-driver/v2/pkg/kubernetes"
)

// Version of the syncer. This should be set via ldflags.
var Version string

const (
	// default interval for csi full sync, used unless overridden by user in csi-controller YAML
	defaultFullSyncIntervalInMin = 30

	// queryVolumeLimit is the page size, which should be set in the cursor when syncer container need to
	// query many volumes using QueryVolume API
	queryVolumeLimit = int64(500)

	// key for HealthStatus annotation on PVC
	annVolumeHealth = "volumehealth.storage.kubernetes.io/health"

	// key for expressing timestamp for volume health annotation
	annVolumeHealthTS = "volumehealth.storage.kubernetes.io/health-timestamp"

	// default interval for csi volume health
	defaultVolumeHealthIntervalInMin = 5

	// default resync period for volume health reconciler
	volumeHealthResyncPeriod = 10 * time.Minute
	// default retry start interval time for volume health reconciler
	volumeHealthRetryIntervalStart = time.Second
	// default retry max interval time for volume health reconciler
	volumeHealthRetryIntervalMax = 5 * time.Minute
	// default number of threads concurrently running for volume health reconciler
	volumeHealthWorkers = 10
	// key for dynamically provisioned PV in volume attributes of PV spec
	attribCSIProvisionerID = "storage.kubernetes.io/csiProvisionerIdentity"
)

var (
	// cnsDeletionMap tracks volumes that exist in CNS but not in K8s
	// If a volume exists in this map across two fullsync cycles,
	// the volume is deleted from CNS
	cnsDeletionMap map[string]bool

	// cnsCreationMap tracks volumes that exist in K8s but not in CNS
	// If a volume exists in this map across two fullsync cycles,
	// the volume is created in CNS
	cnsCreationMap map[string]bool

	// Metadata syncer and full sync share a global lock
	// to mitigate race conditions related to
	// static provisioning of volumes
	volumeOperationsLock sync.Mutex
)

type (
	// Maps K8s PV names to respective PVC object
	pvcMap = map[string]*v1.PersistentVolumeClaim
	// Maps K8s PVC name to respective Pod object
	podMap = map[string][]*v1.Pod
	// Maps K8s PV's Spec.CSI.VolumeHandle to corresponding PVC object
	volumeHandlePVCMap = map[string]*v1.PersistentVolumeClaim
	// Maps CnsVolume's VolumeId.Id to vol.HealthStatus
	volumeIdHealthStatusMap = map[string]string
)

type metadataSyncInformer struct {
	clusterFlavor      cnstypes.CnsClusterFlavor
	volumeManager      volumes.Manager
	host               string
	cnsOperatorClient  client.Client
	supervisorClient   clientset.Interface
	configInfo         *config.ConfigurationInfo
	k8sInformerManager *k8s.InformerManager
	pvLister           corelisters.PersistentVolumeLister
	pvcLister          corelisters.PersistentVolumeClaimLister
	podLister          corelisters.PodLister
	coCommonInterface  commonco.COCommonInterface
}

const (
	// resizeResyncPeriod represents the interval between two resize reconciler syncs
	resizeResyncPeriod = 10 * time.Minute
	// resizeRetryIntervalStart represents the start retry interval of the resize reconciler
	resizeRetryIntervalStart = time.Second
	// resizeRetryIntervalMax represents the max retry interval of the resize reconciler
	resizeRetryIntervalMax = 5 * time.Minute
	// resizeWorkers represents the number of running worker threads
	resizeWorkers = 10
)
