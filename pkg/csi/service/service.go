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

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/vanilla"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/wcp"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/wcpguest"

	cnstypes "gitlab.eng.vmware.com/hatchway/govmomi/cns/types"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rexray/gocsi"
	csictx "github.com/rexray/gocsi/context"

	csitypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

const (
	defaultClusterFlavor = cnstypes.CnsClusterFlavorVanilla

	// UnixSocketPrefix is the prefix before the path on disk
	UnixSocketPrefix = "unix://"
)

var (
	clusterFlavor = defaultClusterFlavor
	cfgPath       = cnsconfig.DefaultCloudConfigPath
)

// Service is a CSI SP and idempotency.Provider.
type Service interface {
	csi.IdentityServer
	csi.NodeServer
	GetController() csi.ControllerServer
	BeforeServe(context.Context, *gocsi.StoragePlugin, net.Listener) error
}

type service struct {
	mode  string
	cnscs csitypes.CnsController
}

// This works around a bug that if k8s node dies, this will clean up the sock file
// left behind. This can't be done in BeforeServe because gocsi will already try to
// bind and fail because the sock file already exists.
func init() {
	sockPath := os.Getenv(gocsi.EnvVarEndpoint)
	sockPath = strings.TrimPrefix(sockPath, UnixSocketPrefix)
	if len(sockPath) > 1 { // minimal valid path length
		os.Remove(sockPath)
	}
}

// New returns a new Service.
func New() Service {
	return &service{}
}

func (s *service) GetController() csi.ControllerServer {
	// check which controller type to use
	clusterFlavor = cnstypes.CnsClusterFlavor(os.Getenv(csitypes.EnvClusterFlavor))
	switch clusterFlavor {
	case cnstypes.CnsClusterFlavorWorkload:
		s.cnscs = wcp.New()
	case cnstypes.CnsClusterFlavorGuest:
		s.cnscs = wcpguest.New()
	default:
		clusterFlavor = defaultClusterFlavor
		s.cnscs = vanilla.New()
	}
	return s.cnscs
}

func (s *service) BeforeServe(
	ctx context.Context, sp *gocsi.StoragePlugin, lis net.Listener) error {
	logType := logger.LogLevel(os.Getenv(logger.EnvLoggerLevel))
	logger.SetLoggerLevel(logType)
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	defer func() {
		log.Infof("configured: %q with clusterFlavor: %q and mode: %q", csitypes.Name, clusterFlavor, s.mode)
	}()

	// Get the SP's operating mode.
	s.mode = csictx.Getenv(ctx, gocsi.EnvVarMode)
	if !strings.EqualFold(s.mode, "node") {
		// Controller service is needed
		var cfg *cnsconfig.Config
		var err error

		cfg, err = common.GetConfig(ctx)
		if err != nil {
			log.Errorf("Failed to read config. Error: %+v", err)
			return err
		}
		if err := s.cnscs.Init(cfg); err != nil {
			log.Errorf("Failed to init controller. Error: %+v", err)
			return err
		}
	}
	return nil
}
