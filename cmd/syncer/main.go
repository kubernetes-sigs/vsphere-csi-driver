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

	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/manager"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/podlistener"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

var (
	enableLeaderElection = flag.Bool("leader-election", false, "Enable leader election.")
)

// main for vsphere syncer
func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// run will be executed if this instance is elected as the leader
	// or if leader election is not enabled
	var run func(ctx context.Context)
	var err error

	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	configInfo, err := types.InitConfigInfo(clusterFlavor)
	if err != nil {
		klog.Errorf("Failed to initialize the configInfo. Err: %+v", err)
		os.Exit(1)
	}

	// Initialize PodListener for every instance of vsphere-syncer in the Supervisor
	// Cluster, independent of whether leader election is enabled.
	// PodListener should run on every node where csi controller can run.
	if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
		go func() {
			if err := podlistener.InitPodListenerService(); err != nil {
				klog.Errorf("Error initializing Pod Listener gRPC sever. Error: %+v", err)
				os.Exit(1)
			}
		}()
	}

	// Initialize syncer components that are dependant on the outcome of leader election, if enabled.
	run = initSyncerComponents(clusterFlavor, configInfo)

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

// initSyncerComponents initializes syncer components that are dependant on the leader election algorithm.
// This function is only called by the leader instance of vsphere-syncer, if enabled.
// TODO: Change name from initSyncerComponents to init<Name>Components where <Name> will be the name of this container
func initSyncerComponents(clusterFlavor cnstypes.CnsClusterFlavor, configInfo *types.ConfigInfo) func(ctx context.Context) {
	return func(ctx context.Context) {
		// Initialize CNS Operator for Supervisor clusters
		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			go func() {
				if err := manager.InitCnsOperator(configInfo); err != nil {
					klog.Errorf("Error initializing Cns Operator. Error: %+v", err)
					os.Exit(1)
				}
			}()
		}
		syncer := syncer.NewInformer()
		if err := syncer.InitMetadataSyncer(clusterFlavor, configInfo); err != nil {
			klog.Errorf("Error initializing Metadata Syncer. Error: %+v", err)
			os.Exit(1)
		}
	}
}
