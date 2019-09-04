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

	corelisters "k8s.io/client-go/listers/core/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
)

var (
	// Metadata syncer and full sync share a global lock
	// to mitigate race conditions related to
	// static provisioning of volumes
	volumeOperationsLock sync.Mutex
)

// MetadataSyncInformer is the struct for metadata sync informer
type MetadataSyncInformer struct {
	cfg                  *cnsconfig.Config
	vcconfig             *cnsvsphere.VirtualCenterConfig
	k8sInformerManager   *k8s.InformerManager
	virtualcentermanager cnsvsphere.VirtualCenterManager
	vcenter              *cnsvsphere.VirtualCenter
	pvLister             corelisters.PersistentVolumeLister
	pvcLister            corelisters.PersistentVolumeClaimLister
}
