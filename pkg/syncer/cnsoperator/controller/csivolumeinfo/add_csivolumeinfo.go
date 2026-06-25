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

// Package csivolumeinfo implements the CsiVolumeInfo controller that watches
// CsiVolumeInfo CRs and declaratively reconciles volume ownership transitions
// between CSI-managed (FCD) and VM-managed (VMDK) states.
package csivolumeinfo

import (
	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"

	csivolumeinfosvc "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo"
	csivolumeinfov1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/csivolumeinfo/v1alpha1"
)

// Add creates the CsiVolumeInfo controller and registers it with the manager.
// Only runs on Workload (supervisor) clusters with the VMOwnedVolumes FSS enabled.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()

	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CsiVolumeInfo Controller as its a non-WCP CSI deployment")
		return nil
	}

	coCommonInterface, err := commonco.GetContainerOrchestratorInterface(ctx,
		common.Kubernetes, clusterFlavor, &syncer.COInitParams)
	if err != nil {
		log.Errorf("failed to create CO agnostic interface. Err: %v", err)
		return err
	}

	if !coCommonInterface.IsFSSEnabled(ctx, common.VMOwnedVolumes) {
		log.Infof("Not initializing the CsiVolumeInfo Controller as VMOwnedVolumes feature is disabled")
		return nil
	}

	// Initialise Kubernetes client (used for scheme registration check and CVI service).
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}
	_ = k8sclient // referenced for side-effect: ensures connectivity at startup.

	// Initialise the CsiVolumeInfo service backed by the manager's client so that
	// the service shares the same cache-backed client as the reconciler.
	cviSvc := csivolumeinfosvc.NewCsiVolumeInfoService(mgr.GetClient())

	// Register the CsiVolumeInfo type with the manager's scheme.
	if err := csivolumeinfov1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		log.Errorf("failed to add CsiVolumeInfo to scheme. Err: %v", err)
		return err
	}

	return add(mgr, newReconciler(mgr, volumeManager, configInfo, cviSvc))
}
