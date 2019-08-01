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

	v1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

const (
	// default interval for csi full sync, used unless overridden by user in csi-controller YAML
	defaultFullSyncIntervalInMin = 30

	// Constants for specifying operation that needs to be performed on CNS volume
	// Create the volume on CNS
	createVolumeOperation = "createVolume"
	// Update the volume entries on CNS
	updateVolumeOperation = "updateVolume"
	// Delete the PVC entry and Pod entry (if it exists) on CNS
	updateVolumeWithDeleteClaimOperation = "updateVolumeWithDeleteClaim"
	// Delete the Pod entry on CNS
	updateVolumeWithDeletePodOperation = "updateVolumeWithDeletePod"
)

var (
	// Create a mapping of CNS volume to Pod name
	// as this mapping does not exist in K8s
	// in case a Pod entry needs to be deleted from CNS cache
	cnsVolumeToPodMap map[string]string
	// Create a mapping of CNS volume to Pvc name
	cnsVolumeToPvcMap map[string]string
	// Create a mapping of CNS volume to entity Namespace name
	// Here entity can be either PVC or Pod - both will
	// belong to the same namespace
	cnsVolumeToEntityNamespaceMap map[string]string
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
	podMap = map[string]*v1.Pod
)

type metadataSyncInformer struct {
	types.Commontypes
	k8sInformerManager   *k8s.InformerManager
	vcconfig             *cnsvsphere.VirtualCenterConfig
	virtualcentermanager cnsvsphere.VirtualCenterManager
	vcenter              *cnsvsphere.VirtualCenter
	pvLister             corelisters.PersistentVolumeLister
	pvcLister            corelisters.PersistentVolumeClaimLister
}
