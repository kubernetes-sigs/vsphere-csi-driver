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

package dpoperator

import (
	"context"
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/dpoperator/controller"
)

// NewManager creates and initializes a new controller manager for the DP operator.
// The DP operator only runs on the Supervisor (Workload) cluster, so the cluster flavor
// is implied and hard-coded internally rather than being passed in by the caller.
func NewManager(
	ctx context.Context,
	configInfo *cnsconfig.ConfigurationInfo,
) (ctrlmgr.Manager, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Initializing DP Operator")
	kubeConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	mgr, err := ctrlmgr.New(kubeConfig, ctrlmgr.Options{
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create new Data Protection(DP) operator instance. Err: %+v", err)
	}

	log.Info("Registering Components for Data Protection(DP) operator")

	if err := cbtconfigv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, fmt.Errorf("failed to set scheme for Data Protection(DP) operator for type %s. Err: %+v",
			cbtconfigv1alpha1.GroupVersion.String(), err)
	}

	vcClient, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
	if err != nil {
		return nil, err
	}

	volumeManager, err := volume.GetManager(ctx, vcClient, nil, false, false, false,
		cnstypes.CnsClusterFlavorWorkload, configInfo.Cfg.Global.SupervisorID,
		configInfo.Cfg.Global.ClusterDistribution)
	if err != nil {
		return nil, fmt.Errorf("failed to create an instance of volume manager: %w", err)
	}

	// Reuse the singleton InformerManager so the DP operator's PV/PVC/VA reads share the
	// same cluster-wide informer caches that the metadata syncer (and other syncer
	// controllers) already run in this process. Listen() starts the factory and waits for
	// the registered informers' caches to sync (idempotent across callers - the metadata
	// syncer also calls Listen on the same singleton).
	k8sClient, err := k8s.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client for dp operator: %w", err)
	}
	informerManager := k8s.NewInformer(ctx, k8sClient, true)
	informerManager.InitVolumeAttachmentInformer()
	informerManager.Listen()

	// Get listers after Listen() so caches are populated by the time the controllers start
	// reconciling.
	pvLister := informerManager.GetPVLister()
	pvcLister := informerManager.GetPVCLister()
	vaLister := informerManager.GetVolumeAttachmentLister()

	if err := controller.AddToManager(mgr, k8sClient, volumeManager, pvLister, pvcLister, vaLister); err != nil {
		return nil, err
	}

	return mgr, nil
}
