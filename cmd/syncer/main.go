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

package main

import (
	"context"
	"flag"
	"os"

	"github.com/kubernetes-csi/csi-lib-utils/leaderelection"
	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"
	"k8s.io/klog"
	vTypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/manager"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/podlistener"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

var (
	enableLeaderElection = flag.Bool("leader-election", false, "Enable leader election.")
)

// main is ignored when this package is built as a go plug-in.
func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// Initialize configInfo and virtualCenterTypes
	configInfo, err := types.InitConfigInfo()
	if err != nil {
		klog.Errorf("Failed to initialize the configInfo. Err: %+v", err)
		os.Exit(1)
	}
	virtualCenterTypes, err := types.InitVirtualCenterTypes(configInfo)
	if err != nil {
		klog.Errorf("Failed to initialize the virtualCenterTypes. Err: %+v", err)
		os.Exit(1)
	}

	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(vTypes.EnvClusterFlavor))
	// Initialize Pod Listener gRPC server only if WCP controller is enabled
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		go func() {
			if err := podlistener.InitPodListenerService(); err != nil {
				klog.Errorf("Error initializing Pod Listener gRPC sever. Error: %+v", err)
				os.Exit(1)
			}
		}()
		go func() {
			if err := manager.InitCnsOperator(configInfo, virtualCenterTypes); err != nil {
				klog.Errorf("Error initializing Cns Operator. Error: %+v", err)
				os.Exit(1)
			}
		}()
	}

	syncer := syncer.NewInformer()
	run := func(ctx context.Context) {
		klog.V(2).Infof("Calling InitMetadataSyncer function")
		if err := syncer.InitMetadataSyncer(configInfo, virtualCenterTypes); err != nil {
			klog.Errorf("Error initializing Metadata Syncer. Error: %+v", err)
			os.Exit(1)
		}
	}

	if !*enableLeaderElection {
		run(context.TODO())
	} else {
		k8sClient, err := k8s.NewClient()
		if err != nil {
			klog.Errorf("Creating Kubernetes client failed. Err: %v", err)
			os.Exit(1)
		}
		lockName := "vsphere-syncer"
		le := leaderelection.NewLeaderElection(k8sClient, lockName, run)

		if err := le.Run(); err != nil {
			klog.Fatalf("Error initializing leader election: %v", err)
		}
	}
}
