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

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	snapshotmetadatav1beta1 "github.com/kubernetes-csi/external-snapshot-metadata/client/apis/snapshotmetadataservice/v1beta1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/dpoperator/controller"
)

// NewManager creates and initializes a new controller manager for the DP operator. clusterFlavor
// and configInfo are handed to each registered controller as-is via controller.AddToManager; each
// controller decides for itself whether it applies to clusterFlavor and builds whatever
// dependencies (vCenter client, volume manager, informers, etc.) it needs from configInfo.
func NewManager(
	ctx context.Context,
	configInfo *cnsconfig.ConfigurationInfo,
	clusterFlavor cnstypes.CnsClusterFlavor,
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

	if err := certmanagerv1.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, fmt.Errorf(
			"failed to set scheme for Data Protection(DP) operator for type %s. Err: %+v",
			certmanagerv1.SchemeGroupVersion.Group, err)
	}

	if err := snapshotmetadatav1beta1.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, fmt.Errorf(
			"failed to set scheme for Data Protection(DP) operator for type %s. Err: %+v",
			snapshotmetadatav1beta1.SchemeGroupVersion.Group, err)
	}

	if err := controller.AddToManager(mgr, clusterFlavor, configInfo); err != nil {
		return nil, err
	}

	return mgr, nil
}
