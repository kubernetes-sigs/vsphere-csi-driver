/*
Copyright 2024 The Kubernetes Authors.

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

package byokoperator

import (
	"context"
	"fmt"

	cnstypes "github.com/vmware/govmomi/cns/types"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller"
	ctrlcommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
)

// NewManager creates and initializes a new controller manager for the specified
// cluster flavor and configuration.
func NewManager(
	ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *cnsconfig.ConfigurationInfo,
) (ctrlmgr.Manager, error) {
	scheme, err := crypto.NewK8sScheme()
	if err != nil {
		return nil, err
	}

	kubeConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	mgr, err := ctrl.NewManager(kubeConfig, ctrl.Options{
		Scheme: scheme,
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{
					&corev1.ConfigMap{},
					&corev1.Secret{},
				},
			},
		},
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create crypto manager: %w", err)
	}

	cryptoClient := crypto.NewClient(ctx, mgr.GetClient())

	vcClient, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
	if err != nil {
		return nil, err
	}

	volumeManager, err := volume.GetManager(ctx, vcClient, nil, false, false, false, clusterFlavor)
	if err != nil {
		return nil, fmt.Errorf("failed to create an instance of volume manager: %w", err)
	}

	if err := controller.AddToManager(ctx, mgr, ctrlcommon.Options{
		ClusterFlavor: clusterFlavor,
		VCenterClient: vcClient,
		CryptoClient:  cryptoClient,
		VolumeManager: volumeManager,
	}); err != nil {
		return nil, err
	}

	return mgr, nil
}
