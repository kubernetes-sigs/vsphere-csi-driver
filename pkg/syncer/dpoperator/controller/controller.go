/*
Copyright 2026 The Kubernetes Authors.

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

package controller

import (
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelistersv1 "k8s.io/client-go/listers/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
)

// AddToManagerFunc registers a single DP operator controller with mgr. The PV / PVC /
// VolumeAttachment listers come from the singleton InformerManager (shared with the metadata
// syncer and k8sorchestrator) so child controllers do not start their own copies of those
// informers via the controller-runtime cache. kubeClient is shared across all controllers
// registered under one manager so there is no second apiserver connection.
type AddToManagerFunc func(mgr manager.Manager, kubeClient clientset.Interface,
	volumeManager volume.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister) error

// AddToManagerFuncs is the list of registration funcs invoked by AddToManager.
// The DP operator only runs on the Supervisor (Workload) cluster, so cluster flavor
// is implied and not threaded through these registration funcs.
var AddToManagerFuncs []AddToManagerFunc

// AddToManager adds all Controllers to the Manager.
func AddToManager(mgr manager.Manager, kubeClient clientset.Interface,
	volumeManager volume.Manager,
	pvLister corelisters.PersistentVolumeLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vaLister storagelistersv1.VolumeAttachmentLister) error {
	for _, f := range AddToManagerFuncs {
		if err := f(mgr, kubeClient, volumeManager, pvLister, pvcLister, vaLister); err != nil {
			return err
		}
	}
	return nil
}
