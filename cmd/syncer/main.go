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
	"fmt"
	"os"

	"github.com/kubernetes-csi/csi-lib-utils/leaderelection"
	cnstypes "github.com/vmware/govmomi/cns/types"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/admissionhandler"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/k8scloudoperator"

	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/manager"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/storagepool"
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/types"
)

// OperationModeWebHookServer starts container for webhook server
const operationModeWebHookServer = "WEBHOOK_SERVER"

// OperationModeWebHookServer starts container for metadata sync
const operationModeMetaDataSync = "METADATA_SYNC"

var (
	enableLeaderElection = flag.Bool("leader-election", false, "Enable leader election.")
	printVersion         = flag.Bool("version", false, "Print syncer version and exit")
	operationMode        = flag.String("operation-mode", operationModeMetaDataSync, "specify operation mode METADATA_SYNC or WEBHOOK_SERVER")
)

// main for vsphere syncer
func main() {
	flag.Parse()
	if *printVersion {
		fmt.Printf("%s\n", syncer.Version)
		return
	}
	logType := logger.LogLevel(os.Getenv(logger.EnvLoggerLevel))
	logger.SetLoggerLevel(logType)
	ctx, log := logger.GetNewContextWithLogger()
	log.Infof("Version : %s", syncer.Version)

	clusterFlavor := cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	if clusterFlavor == "" {
		clusterFlavor = cnstypes.CnsClusterFlavorVanilla
	}

	if *operationMode == operationModeWebHookServer {
		log.Infof("Starting container with operation mode: %v", operationModeWebHookServer)
		if webHookStartError := admissionhandler.StartWebhookServer(ctx); webHookStartError != nil {
			log.Fatalf("failed to start webhook server. err: %v", webHookStartError)
		}
	} else if *operationMode == operationModeMetaDataSync {
		log.Infof("Starting container with operation mode: %v", operationModeMetaDataSync)
		var err error
		configInfo, err := types.InitConfigInfo(ctx)
		if err != nil {
			log.Errorf("failed to initialize the configInfo. Err: %+v", err)
			os.Exit(1)
		}
		// run will be executed if this instance is elected as the leader
		// or if leader election is not enabled
		var run func(ctx context.Context)

		// Initialize K8sCloudOperator for every instance of vsphere-syncer in the Supervisor
		// Cluster, independent of whether leader election is enabled.
		// K8sCloudOperator should run on every node where csi controller can run.
		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			go func() {
				if err := k8scloudoperator.InitK8sCloudOperatorService(ctx); err != nil {
					log.Fatalf("Error initializing K8s Cloud Operator gRPC sever. Error: %+v", err)
				}
			}()
		}
		// Initialize syncer components that are dependant on the outcome of leader election, if enabled.
		run = initSyncerComponents(ctx, clusterFlavor, configInfo)

		if !*enableLeaderElection {
			run(context.TODO())
		} else {
			k8sClient, err := k8s.NewClient(ctx)
			if err != nil {
				log.Fatalf("Creating Kubernetes client failed. Err: %v", err)
			}
			lockName := "vsphere-syncer"
			le := leaderelection.NewLeaderElection(k8sClient, lockName, run)

			if err := le.Run(); err != nil {
				log.Fatalf("Error initializing leader election: %v", err)
			}
		}
	} else {
		log.Fatalf("unsupported operation mode: %v", *operationMode)
	}
}

// initSyncerComponents initializes syncer components that are dependant on the leader election algorithm.
// This function is only called by the leader instance of vsphere-syncer, if enabled.
// TODO: Change name from initSyncerComponents to init<Name>Components where <Name> will be the name of this container
func initSyncerComponents(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, configInfo *types.ConfigInfo) func(ctx context.Context) {
	return func(ctx context.Context) {
		log := logger.GetLogger(ctx)
		// Initialize CNS Operator for Supervisor clusters
		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			go func() {
				if err := storagepool.InitStoragePoolService(ctx, configInfo); err != nil {
					log.Errorf("Error initializing StoragePool Service. Error: %+v", err)
					os.Exit(1)
				}
			}()
			go func() {
				if err := manager.InitCnsOperator(configInfo); err != nil {
					log.Errorf("Error initializing Cns Operator. Error: %+v", err)
					os.Exit(1)
				}
			}()
		}
		if err := syncer.InitMetadataSyncer(ctx, clusterFlavor, configInfo); err != nil {
			log.Errorf("Error initializing Metadata Syncer. Error: %+v", err)
			os.Exit(1)
		}
	}
}
