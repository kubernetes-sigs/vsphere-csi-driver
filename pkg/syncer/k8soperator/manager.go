/*
Copyright 2025 The Kubernetes Authors.

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

package k8soperator

import (
	"context"
	"fmt"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8soperator/k8scontroller"
)

// NewManager creates and initializes a new controller manager for the specified
// cluster flavor.
func NewManager(
	ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor,
) (ctrlmgr.Manager, error) {
	log := logger.GetLogger(ctx)
	log.Infof("Initializing K8s Operator")
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
		return nil, fmt.Errorf("failed to create new k8soperator instance. Err: %+v", err)
	}

	log.Info("Registering Components for K8s Operator")

	// Setup Scheme for all resources
	if err := snapv1.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, fmt.Errorf("failed to set scheme for K8s operator for type %s. Err: %+v", snapv1.GroupName, err)
	}

	if err := k8scontroller.AddToManager(mgr, clusterFlavor); err != nil {
		return nil, err
	}

	return mgr, nil
}
