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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/prometheus"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/admissionhandler"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8scloudoperator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/k8soperator"
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
	enableWebhookClientCertVerification = flag.Bool("webhook-client-cert-verification", false,
		"Enable client cert authenticate of apiserver to CSI webhook")

	supervisorFSSName = flag.String("supervisor-fss-name", "",
		"Name of the feature state switch configmap in supervisor cluster")
	supervisorFSSNamespace = flag.String("supervisor-fss-namespace", "",
		"Namespace of the feature state switch configmap in supervisor cluster")
	internalFSSName           = flag.String("fss-name", "", "Name of the feature state switch configmap")
	internalFSSNamespace      = flag.String("fss-namespace", "", "Namespace of the feature state switch configmap")
	periodicSyncIntervalInMin = flag.Duration("storagequota-sync-interval", 30*time.Minute,
		"Periodic sync interval in Minutes")
	enableProfileServer = flag.Bool("enable-profile-server", false, "Enable profiling endpoint for the syncer.")
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

	if *enableProfileServer {
		go func() {
			log.Info("Starting the http server to expose profiling metrics..")
			err := http.ListenAndServe(":9501", nil)
			if err != nil {
				log.Fatalf("Unable to start profiling server: %s", err)
			}
		}()
	}

	// Set CO agnostic init params.
	clusterFlavor, err := config.GetClusterFlavor(ctx)
	if err != nil {
		log.Errorf("failed retrieving cluster flavor. Error: %v", err)
	}
	commonco.SetInitParams(ctx, clusterFlavor, &syncer.COInitParams, *supervisorFSSName, *supervisorFSSNamespace,
		*internalFSSName, *internalFSSNamespace, "", *operationMode)
	admissionhandler.COInitParams = &syncer.COInitParams

	// Disconnect VC session on restart
	defer func() {
		log.Info("Cleaning up vc sessions")
		if r := recover(); r != nil {
			cleanupSessions(ctx, r)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	go func() {
		for {
			sig := <-ch
			if sig == syscall.SIGTERM {
				log.Info("SIGTERM signal received")
				utils.LogoutAllvCenterSessions(ctx)
				os.Exit(0)
			}
		}
	}()

	if *operationMode == operationModeWebHookServer {
		log.Infof("Starting container with operation mode: %v", operationModeWebHookServer)
		if webHookStartError := admissionhandler.StartWebhookServer(ctx,
			*enableWebhookClientCertVerification); webHookStartError != nil {
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
				defer func() {
					log.Info("Cleaning up vc sessions cloud operator service")
					if r := recover(); r != nil {
						cleanupSessions(ctx, r)
					}
				}()
				if err := k8scloudoperator.InitK8sCloudOperatorService(ctx); err != nil {
					log.Fatalf("Error initializing K8s Cloud Operator gRPC sever. Error: %+v", err)
				}
			}()
		}

		// Go module to keep the metrics http server running all the time.
		go func() {
			defer func() {
				log.Info("Cleaning up vc sessions prometheus metrics")
				if r := recover(); r != nil {
					cleanupSessions(ctx, r)
				}
			}()
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
			id, err := defaultLeaderElectionIdentity()
			if err != nil {
				log.Fatalf("error getting the default leader identity. Err: %v", err)
			}
			resourceLockConfig := rl.ResourceLockConfig{
				Identity: sanitizeName(id),
			}
			if *leaderElectionNamespace == "" {
				*leaderElectionNamespace = inClusterNamespace()
			}
			log.Debugf("identity: %v, leaderElectionNamespace: %v",
				resourceLockConfig.Identity, *leaderElectionNamespace)
			lock, err := rl.New(rl.LeasesResourceLock, *leaderElectionNamespace, lockName, k8sClient.CoreV1(),
				k8sClient.CoordinationV1(), resourceLockConfig)
			if err != nil {
				log.Fatalf("Creating lock for leader election failed. Err: %v", err)
			}
			leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
				Lock:          lock,
				LeaseDuration: *leaderElectionLeaseDuration,
				RenewDeadline: *leaderElectionRenewDeadline,
				RetryPeriod:   *leaderElectionRetryPeriod,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(_ context.Context) {
						log.Info("became leader, starting")
						run(ctx)
					},
					OnStoppedLeading: func() {
						log.Info("stopped leading. disconnecting vc session")
						utils.LogoutAllvCenterSessions(ctx)
						os.Exit(0)
					},
					OnNewLeader: func(identity string) {
						if identity == resourceLockConfig.Identity {
							//  current replica reacquired lease
							return
						}
						log.Infof("new leader detected, current leader: %s", identity)
					},
				},
				ReleaseOnCancel: true,
				Name:            lockName,
			})
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
	return func(_ context.Context) {
		log := logger.GetLogger(ctx)
		// Disconnect vCenter sessions on restart
		defer func() {
			log.Info("Cleaning up vc sessions syncer components")
			if r := recover(); r != nil {
				cleanupSessions(ctx, r)
			}
		}()
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
				defer func() {
					log.Info("Cleaning up vc sessions storage pool service")
					if r := recover(); r != nil {
						cleanupSessions(ctx, r)
					}
				}()
				if err := storagepool.InitStoragePoolService(ctx, configInfo, coInitParams); err != nil {
					log.Errorf("Error initializing StoragePool Service. Error: %+v", err)
					utils.LogoutAllvCenterSessions(ctx)
					os.Exit(0)
				}
			}()
		}

		if clusterFlavor == cnstypes.CnsClusterFlavorVanilla {
			// Initialize node manager so that syncer components can
			// retrieve NodeVM using the NodeID.
			nodeMgr := &node.Nodes{}
			err = nodeMgr.Initialize(ctx)
			if err != nil {
				log.Errorf("failed to initialize nodeManager. Error: %+v", err)
				os.Exit(1)
			}
			if configInfo.Cfg.Global.ClusterDistribution == "" {
				config, err := rest.InClusterConfig()
				if err != nil {
					log.Errorf("failed to get InClusterConfig: %v", err)
					os.Exit(1)
				}
				clientset, err := kubernetes.NewForConfig(config)
				if err != nil {
					log.Errorf("failed to create kubernetes client with err: %v", err)
					os.Exit(1)
				}

				// Get the version info for the Kubernetes API server
				versionInfo, err := clientset.Discovery().ServerVersion()
				if err != nil {
					log.Errorf("failed to fetch versionInfo with err: %v", err)
					os.Exit(1)
				}

				// Extract the version string from the version info
				version := versionInfo.GitVersion
				var ClusterDistNameToServerVersion = map[string]string{
					"gke":       "Anthos",
					"racher":    "Rancher",
					"rke":       "Rancher",
					"docker":    "DockerEE",
					"dockeree":  "DockerEE",
					"openshift": "Openshift",
					"wcp":       "Supervisor",
					"vmware":    "TanzuKubernetesCluster",
					"eks":       "EKS",
					"aks":       "AKS",
					"nativek8s": "VanillaK8S",
				}
				distributionUnknown := true
				for distServerVersion, distName := range ClusterDistNameToServerVersion {
					if strings.Contains(version, distServerVersion) {
						configInfo.Cfg.Global.ClusterDistribution = distName
						distributionUnknown = false
						break
					}
				}
				if distributionUnknown {
					configInfo.Cfg.Global.ClusterDistribution = ClusterDistNameToServerVersion["nativek8s"]
				}
			}
		}

		go func() {
			defer func() {
				log.Info("Cleaning up vc sessions cns operator")
				if r := recover(); r != nil {
					cleanupSessions(ctx, r)
				}
			}()
			if err := manager.InitCnsOperator(ctx, clusterFlavor, configInfo, coInitParams); err != nil {
				log.Errorf("Error initializing Cns Operator. Error: %+v", err)
				utils.LogoutAllvCenterSessions(ctx)
				os.Exit(0)
			}
		}()

		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
			commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.BYOKEncryption) {
			// Start BYOK Operator for Supervisor clusters.
			go func() {
				defer func() {
					log.Info("Cleaning up vc sessions BYOK operator")
					if r := recover(); r != nil {
						cleanupSessions(ctx, r)
					}
				}()
				if err := startByokOperator(ctx, clusterFlavor, configInfo); err != nil {
					log.Errorf("Error initializing BYOK Operator. Error: %+v", err)
					utils.LogoutAllvCenterSessions(ctx)
					os.Exit(0)
				}
			}()
		}

		if clusterFlavor == cnstypes.CnsClusterFlavorWorkload &&
			commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SVPVCSnapshotProtectionFinalizer) {
			// Start K8s Operator for Supervisor clusters.
			go func() {
				defer func() {
					log.Info("Cleaning up vc sessions K8s operator")
					if r := recover(); r != nil {
						cleanupSessions(ctx, r)
					}
				}()
				if err := startK8sOperator(ctx, clusterFlavor); err != nil {
					log.Errorf("Error initializing K8s Operator. Error: %+v", err)
					utils.LogoutAllvCenterSessions(ctx)
					os.Exit(0)
				}
			}()
		}

		syncer.PeriodicSyncIntervalInMin = *periodicSyncIntervalInMin
		if err := syncer.InitMetadataSyncer(ctx, clusterFlavor, configInfo); err != nil {
			log.Errorf("Error initializing Metadata Syncer. Error: %+v", err)
			utils.LogoutAllvCenterSessions(ctx)
			os.Exit(0)
		}
	}
}

func startByokOperator(ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo) error {

	mgr, err := byokoperator.NewManager(ctx, clusterFlavor, configInfo)
	if err != nil {
		return err
	}

	return mgr.Start(ctx)
}

func startK8sOperator(ctx context.Context,
	clusterFlavor cnstypes.CnsClusterFlavor) error {

	mgr, err := k8soperator.NewManager(ctx, clusterFlavor)
	if err != nil {
		return err
	}

	return mgr.Start(ctx)
}

func cleanupSessions(ctx context.Context, r interface{}) {
	log := logger.GetLogger(ctx)
	log.Errorf("Observed a panic and a restart was invoked, panic: %+v", r)
	log.Info("Recovered from panic. Disconnecting the existing vc sessions.")
	utils.LogoutAllvCenterSessions(ctx)
	os.Exit(0)
}

func defaultLeaderElectionIdentity() (string, error) {
	return os.Hostname()
}

// sanitizeName sanitizes the provided string so it can be consumed by leader election library
func sanitizeName(name string) string {
	re := regexp.MustCompile("[^a-zA-Z0-9-]")
	name = re.ReplaceAllString(name, "-")
	if name[len(name)-1] == '-' {
		// name must not end with '-'
		name = name + "X"
	}
	return name
}

// inClusterNamespace returns the namespace in which the pod is running in by checking
// the env var POD_NAMESPACE, then the file /var/run/secrets/kubernetes.io/serviceaccount/namespace.
// if neither returns a valid namespace, the "default" namespace is returned
func inClusterNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}

	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
			return ns
		}
	}

	return "default"
}
