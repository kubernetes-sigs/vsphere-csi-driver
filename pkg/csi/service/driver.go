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

package service

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rexray/gocsi"
	cnstypes "github.com/vmware/govmomi/cns/types"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/vanilla"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/wcp"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/wcpguest"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/types"
)

const (
	defaultClusterFlavor = cnstypes.CnsClusterFlavorVanilla

	// UnixSocketPrefix is the prefix before the path on disk.
	UnixSocketPrefix = "unix://"
)

var (
	// COInitParams stores the input params required for initiating the
	// CO agnostic orchestrator for the controller as well as node containers.
	COInitParams  interface{}
	clusterFlavor = defaultClusterFlavor
	cfgPath       = cnsconfig.DefaultCloudConfigPath
)

// Driver is a CSI SP and idempotency.Provider.
type Driver interface {
	csi.IdentityServer
	csi.NodeServer
	GetController() csi.ControllerServer
	BeforeServe(context.Context, *gocsi.StoragePlugin, net.Listener) error
	Run(ctx context.Context, endpoint string)
}

type vsphereCSIDriver struct {
	mode  string
	cnscs csitypes.CnsController
}

// If k8s node died unexpectedly in an earlier run, the unix socket is left
// behind. This method will clean up the sock file during initialization.
// We cannot do this in BeforeServe, because gocsi will already try to
// bind and fail if the sock file already exists.
func init() {
	sockPath := os.Getenv(csitypes.EnvVarEndpoint)
	sockPath = strings.TrimPrefix(sockPath, UnixSocketPrefix)
	if len(sockPath) > 1 { // Minimal valid path length.
		os.Remove(sockPath)
	}
}

// NewDriver returns a new Driver.
func NewDriver() Driver {
	return &vsphereCSIDriver{}
}

func (driver *vsphereCSIDriver) GetController() csi.ControllerServer {
	// Check which controller type to use.
	clusterFlavor = cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	switch clusterFlavor {
	case cnstypes.CnsClusterFlavorWorkload:
		driver.cnscs = wcp.New()
	case cnstypes.CnsClusterFlavorGuest:
		driver.cnscs = wcpguest.New()
	default:
		clusterFlavor = defaultClusterFlavor
		driver.cnscs = vanilla.New()
	}
	return driver.cnscs
}

//BeforeServe defines the tasks needed before starting the driver.
func (driver *vsphereCSIDriver) BeforeServe(
	ctx context.Context, sp *gocsi.StoragePlugin, lis net.Listener) error {
	logger.SetLoggerLevel(logger.LogLevel(os.Getenv(logger.EnvLoggerLevel)))
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	defer func() {
		log.Infof("Configured: %q with clusterFlavor: %q and mode: %q",
			csitypes.Name, clusterFlavor, driver.mode)
	}()

	var (
		err error
		cfg *cnsconfig.Config
	)

	// Initialize CO utility in Nodes.
	commonco.ContainerOrchestratorUtility, err = commonco.GetContainerOrchestratorInterface(
		ctx, common.Kubernetes, clusterFlavor, COInitParams)
	if err != nil {
		log.Errorf("Failed to create CO agnostic interface. Error: %v", err)
		return err
	}

	// Get the SP's operating mode.
	driver.mode = os.Getenv(csitypes.EnvVarMode)
	if !strings.EqualFold(driver.mode, "node") {
		// Controller service is needed.
		cfg, err = common.GetConfig(ctx)
		if err != nil {
			log.Errorf("failed to read config. Error: %+v", err)
			return err
		}
		if err := driver.cnscs.Init(cfg, Version); err != nil {
			log.Errorf("failed to init controller. Error: %+v", err)
			return err
		}
	}
	return nil
}

// Run starts a gRPC server that serves requests at the specified endpoint.
func (driver *vsphereCSIDriver) Run(ctx context.Context, endpoint string) {
	log := logger.GetLogger(ctx)
	controllerServer := driver.GetController()

	// Invoke BeforeServe function to perform any local initialization routines.
	if err := driver.BeforeServe(ctx, nil, nil); err != nil {
		log.Errorf("failed to run the driver. Err: +%v", err)
		os.Exit(1)
	}

	//Start the nonblocking GRPC
	grpc := NewNonBlockingGRPCServer()
	grpc.Start(endpoint, driver, controllerServer, driver)
}
