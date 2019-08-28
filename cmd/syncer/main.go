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
	"k8s.io/klog"
	vTypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/podlistener"
)

const (
	// WcpControllerType indicated WCP CSI Controller
	WcpControllerType = "WCP"
)

var (
	enableLeaderElection = flag.Bool("leader-election", false, "Enable leader election.")
)

// main is ignored when this package is built as a go plug-in.
func main() {
	klog.InitFlags(nil)
	flag.Parse()

	controllerType := os.Getenv(vTypes.EnvControllerType)
	// Initialize Pod Listener gRPC server only if WCP controller is enabled
	if controllerType == WcpControllerType {
		go func() {
			if err := podlistener.InitPodListenerService(); err != nil {
				klog.Errorf("Error initializing Pod Listener gRPC sever")
				os.Exit(1)
			}
		}()
	}

	syncer := syncer.NewInformer()
	run := func(ctx context.Context) {
		if err := syncer.InitMetadataSyncer(); err != nil {
			klog.Errorf("Error initializing Metadata Syncer")
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
