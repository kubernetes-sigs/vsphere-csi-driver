/*
Copyright 2018 The Kubernetes Authors.

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
	"syscall"
	"time"

	csiconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/runtimeconfig"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	profile_logger "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils/profile_logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var (
	printVersion = flag.Bool("version", false, "Print driver version and exit")

	migrateFileVolumeConfig = flag.Bool("migrate-file-volume-config", false,
		"Copy the legacy file-volume activation into the runtime ConfigMap and exit. Run by the Supervisor "+
			"package before it replaces the CSI config Secret")
	fileVolumeNamespace = flag.String("file-volume-namespace", "vmware-system-csi",
		"Namespace of the CSI config Secret and the file-volume ConfigMap (used with --migrate-file-volume-config)")
	migrationSDDCName = flag.String("sddc-name", "",
		"SDDC whose file-volume activation is migrated (used with --migrate-file-volume-config)")

	supervisorFSSName = flag.String("supervisor-fss-name", "",
		"Name of the feature state switch configmap in supervisor cluster")
	supervisorFSSNamespace = flag.String("supervisor-fss-namespace", "",
		"Namespace of the feature state switch configmap in supervisor cluster")
	internalFSSName      = flag.String("fss-name", "", "Name of the feature state switch configmap")
	internalFSSNamespace = flag.String("fss-namespace", "", "Namespace of the feature state switch configmap")
	enableProfileServer  = flag.Bool("enable-profile-server", false, "Enable profiling endpoint for the controller.")
	enableProfileLogger  = flag.Bool("enable-profile-logger", true,
		"Write memory profiles to the log when the live heap nears GOMEMLIMIT.")
)

// main is ignored when this package is built as a go plug-in.
func main() {
	flag.Parse()
	if *printVersion {
		fmt.Printf("Version : %s\n", service.Version)
		return
	}
	logType := logger.LogLevel(os.Getenv(logger.EnvLoggerLevel))
	logger.SetLoggerLevel(logType)
	ctx, log := logger.GetNewContextWithLogger()
	if *migrateFileVolumeConfig {
		// A one-shot migration that needs no vCenter access, so it exits before any driver startup.
		if err := runFileVolumeMigration(ctx); err != nil {
			log.Errorf("File-volume configuration migration failed: %v", err)
			os.Exit(1)
		}
		log.Info("File-volume configuration migration completed")
		return
	}
	log.Infof("Vsphere CSI Driver BuildInfo : %s-%s-%s", service.Version, service.Branch, service.Commit)

	// Log GOMEMLIMIT if set for memory management visibility
	if goMemLimit := os.Getenv("GOMEMLIMIT"); goMemLimit != "" {
		log.Infof("GOMEMLIMIT is set to: %s", goMemLimit)
	} else {
		log.Infof("GOMEMLIMIT is not set, using Go default memory management")
	}

	if *enableProfileLogger {
		if err := profile_logger.InitProfileLogger(ctx, profile_logger.Config{}); err != nil {
			log.Errorf("profile logger is not started. Error: %+v", err)
		}
	}

	if *enableProfileServer {
		go func() {
			log.Info("Starting the http server to expose profiling metrics..")
			err := http.ListenAndServe("127.0.0.1:9500", nil)
			if err != nil {
				log.Fatalf("Unable to start profiling server: %s", err)
			}
		}()
	}

	// Set CO Init params.
	clusterFlavor, err := csiconfig.GetClusterFlavor(ctx)
	if err != nil {
		log.Errorf("failed retrieving the cluster flavor. Error: %v", err)
	}
	serviceMode := os.Getenv(csitypes.EnvVarMode)
	commonco.SetInitParams(ctx, clusterFlavor, &service.COInitParams, *supervisorFSSName, *supervisorFSSNamespace,
		*internalFSSName, *internalFSSNamespace, serviceMode, "")

	// If no endpoint is set then exit the program.
	CSIEndpoint := os.Getenv(csitypes.EnvVarEndpoint)
	if CSIEndpoint == "" {
		log.Error("CSI endpoint cannot be empty. Please set the env variable.")
		os.Exit(1)
	}
	log.Info("Enable logging off for vCenter sessions on exit")
	// Disconnect VC session on restart
	defer func() {
		if r := recover(); r != nil {
			log.Info("Cleaning up vc sessions")
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

	vSphereCSIDriver := service.NewDriver()
	vSphereCSIDriver.Run(ctx, CSIEndpoint)

}

func runFileVolumeMigration(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	client, err := k8s.NewClient(ctx)
	if err != nil {
		return err
	}
	return runtimeconfig.MigrateFileVolumeConfig(ctx, client, *fileVolumeNamespace, *migrationSDDCName)
}

func cleanupSessions(ctx context.Context, r interface{}) {
	log := logger.GetLogger(ctx)
	log.Errorf("Observed a panic and a restart was invoked, panic: %+v", r)
	log.Info("Recovered from panic. Disconnecting the existing vc sessions.")
	utils.LogoutAllvCenterSessions(ctx)
	os.Exit(0)
}
