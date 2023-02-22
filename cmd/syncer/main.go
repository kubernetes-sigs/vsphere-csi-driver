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
	"net/http"
	"os"
	"time"

	"github.com/kubernetes-csi/csi-lib-utils/leaderelection"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	cnstypes "github.com/vmware/govmomi/cns/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/admissionhandler"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/storagepool"
)

// OperationModeWebHookServer starts container for webhook server.
const operationModeWebHookServer = "WEBHOOK_SERVER"

// OperationModeWebHookServer starts container for metadata sync.
const operationModeMetaDataSync = "METADATA_SYNC"

var (
	enableLeaderElection    = flag.Bool("leader-election", false, "Enable leader election.")
	leaderElectionNamespace = flag.String("leader-election-namespace", "", "Namespace where the leader "+
		"election resource lives. Defaults to the pod namespace if not set.")
	leaderElectionLeaseDuration = flag.Duration("leader-election-lease-duration", 15*time.Second,
		"Duration, in seconds, that non-leader candidates will wait to force acquire leadership. "+
			"Defaults to 15 seconds.")
	leaderElectionRenewDeadline = flag.Duration("leader-election-renew-deadline", 10*time.Second,
		"Duration, in seconds, that the acting leader will retry refreshing leadership before giving up. "+
			"Defaults to 10 seconds.")
	leaderElectionRetryPeriod = flag.Duration("leader-election-retry-period", 5*time.Second,
		"Duration in seconds, the LeaderElector clients should wait between tries of actions. "+
			"Defaults to 5 seconds.")
	printVersion  = flag.Bool("version", false, "Print syncer version and exit")
	operationMode = flag.String("operation-mode", operationModeMetaDataSync,
		"specify operation mode METADATA_SYNC or WEBHOOK_SERVER")

	supervisorFSSName = flag.String("supervisor-fss-name", "",
		"Name of the feature state switch configmap in supervisor cluster")
	supervisorFSSNamespace = flag.String("supervisor-fss-namespace", "",
		"Namespace of the feature state switch configmap in supervisor cluster")
	internalFSSName      = flag.String("fss-name", "", "Name of the feature state switch configmap")
	internalFSSNamespace = flag.String("fss-namespace", "", "Namespace of the feature state switch configmap")
)

// main for vsphere syncer.
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

	// Set CO agnostic init params.
	clusterFlavor, err := config.GetClusterFlavor(ctx)
	if err != nil {
		log.Errorf("failed retrieving cluster flavor. Error: %v", err)
	}
	commonco.SetInitParams(ctx, clusterFlavor, &syncer.COInitParams, *supervisorFSSName, *supervisorFSSNamespace,
		*internalFSSName, *internalFSSNamespace, "")
	admissionhandler.COInitParams = &syncer.COInitParams

	if *operationMode == operationModeWebHookServer {
		log.Infof("Starting container with operation mode: %v", operationModeWebHookServer)
		if webHookStartError := admissionhandler.StartWebhookServer(ctx); webHookStartError != nil {
			log.Fatalf("failed to start webhook server. err: %v", webHookStartError)
		}
	} else if *operationMode == operationModeMetaDataSync {
		log.Infof("Starting container with operation mode: %v", operationModeMetaDataSync)
		var err error

		// run will be executed if this instance is elected as the leader
		// or if leader election is not enabled.
		var run func(ctx context.Context)

		// Initialize K8sCloudOperator for every instance of vsphere-syncer in the
		// Supervisor Cluster, independent of whether leader election is enabled.
		// K8sCloudOperator should run on every node where csi controller can run.
		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			go func() {
				if err := k8scloudoperator.InitK8sCloudOperatorService(ctx); err != nil {
					log.Fatalf("Error initializing K8s Cloud Operator gRPC sever. Error: %+v", err)
				}
			}()
		}

		// Go module to keep the metrics http server running all the time.
		go func() {
			prometheus.SyncerInfo.WithLabelValues(syncer.Version).Set(1)
			for {
				log.Info("Starting the http server to expose Prometheus metrics..")
				http.Handle("/metrics", promhttp.Handler())
				err = http.ListenAndServe(":2113", nil)
				if err != nil {
					log.Warnf("Http server that exposes the Prometheus exited with err: %+v", err)
				}
				log.Info("Restarting http server to expose Prometheus metrics..")
			}
		}()

		// Initialize syncer components that are dependant on the outcome of
		// leader election, if enabled.
		run = initSyncerComponents(ctx, clusterFlavor, &syncer.COInitParams)

		if !*enableLeaderElection {
			run(ctx)
		} else {
			k8sClient, err := k8s.NewClient(ctx)
			if err != nil {
				log.Fatalf("Creating Kubernetes client failed. Err: %v", err)
			}
			lockName := "vsphere-syncer"
			le := leaderelection.NewLeaderElection(k8sClient, lockName, run)

			if *leaderElectionNamespace != "" {
				le.WithNamespace(*leaderElectionNamespace)
			}

			le.WithLeaseDuration(*leaderElectionLeaseDuration)
			le.WithRenewDeadline(*leaderElectionRenewDeadline)
			le.WithRetryPeriod(*leaderElectionRetryPeriod)

			if err := le.Run(); err != nil {
				log.Fatalf("Error initializing leader election: %v", err)
			}
		}
	} else {
		log.Fatalf("unsupported operation mode: %v", *operationMode)
	}
}

// initSyncerComponents initializes syncer components that are dependant on
// the leader election algorithm. This function is only called by the leader
// instance of vsphere-syncer, if enabled.
// TODO: Change name from initSyncerComponents to init<Name>Components where
// <Name> will be the name of this container.
func initSyncerComponents(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	coInitParams *interface{}) func(ctx context.Context) {
	return func(ctx context.Context) {
		log := logger.GetLogger(ctx)

		if err := manager.InitCommonModules(ctx, clusterFlavor, coInitParams); err != nil {
			log.Errorf("Error initializing common modules for all flavors. Error: %+v", err)
			os.Exit(1)
		}
		var configInfo *config.ConfigurationInfo
		var err error
		if clusterFlavor == cnstypes.CnsClusterFlavorVanilla &&
			commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIInternalGeneratedClusterID) {
			configInfo, err = syncer.SyncerInitConfigInfo(ctx)
			if err != nil {
				log.Errorf("failed to initialize the configInfo. Err: %+v", err)
				os.Exit(1)
			}
		} else {
			configInfo, err = config.InitConfigInfo(ctx)
			if err != nil {
				log.Errorf("failed to initialize the configInfo. Err: %+v", err)
				os.Exit(1)
			}
		}

		// Initialize CNS Operator for Supervisor clusters.
		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload {
			go func() {
				if err := storagepool.InitStoragePoolService(ctx, configInfo, coInitParams); err != nil {
					log.Errorf("Error initializing StoragePool Service. Error: %+v", err)
					os.Exit(1)
				}
			}()
		}
		if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
			// Initialize node manager so that syncer components can
			// retrieve NodeVM using the NodeID.
			useNodeUuid := false
			if commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.UseCSINodeId) {
				useNodeUuid = true
			}
			nodeMgr := &node.Nodes{}
			err = nodeMgr.Initialize(ctx, useNodeUuid)
			if err != nil {
				log.Errorf("failed to initialize nodeManager. Error: %+v", err)
				os.Exit(1)
			}
		}
		go func() {
			if err := manager.InitCnsOperator(ctx, clusterFlavor, configInfo, coInitParams); err != nil {
				log.Errorf("Error initializing Cns Operator. Error: %+v", err)
				os.Exit(1)
			}
		}()
		if err := syncer.InitMetadataSyncer(ctx, clusterFlavor, configInfo); err != nil {
			log.Errorf("Error initializing Metadata Syncer. Error: %+v", err)
			os.Exit(1)
		}
	}
}
