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
	"flag"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/block/vanilla"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/block/wcp"

	"net"
	"os"
	"strings"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/pkg/common/config"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rexray/gocsi"
	csictx "github.com/rexray/gocsi/context"
	log "github.com/sirupsen/logrus"
	vTypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"
)

const (
	// Name is the name of this CSI SP.
	Name = "vsphere.csi.vmware.com"

	// VanillaK8SControllerType indicated Vanilla K8S CSI Controller
	VanillaK8SControllerType = "VANILLA"

	// WcpControllerType indicated WCP CSI Controller
	WcpControllerType = "WCP"

	defaultController = VanillaK8SControllerType
)

var (
	controllerType = defaultController
	cfgPath        = vTypes.DefaultCloudConfigPath
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
	cnscs vTypes.CnsController
}

// New returns a new Service.
func New() Service {
	return &service{}
}

func (s *service) GetController() csi.ControllerServer {
	// check which controller type to use
	controllerType = os.Getenv(vTypes.EnvControllerType)
	if controllerType == WcpControllerType {
		s.cnscs = wcp.New()
		return s.cnscs
	}
	controllerType = defaultController
	s.cnscs = vanilla.New()
	return s.cnscs
}

func (s *service) BeforeServe(
	ctx context.Context, sp *gocsi.StoragePlugin, lis net.Listener) error {

	defer func() {
		fields := map[string]interface{}{
			"controllerType": controllerType,
			"mode":           s.mode,
		}

		log.WithFields(fields).Infof("configured: %s", Name)
	}()

	// Get the SP's operating mode.
	s.mode = csictx.Getenv(ctx, gocsi.EnvVarMode)

	// Set klog level based on CSI debug being enabled
	klogLevel := "2"
	lvl := log.GetLevel()
	if lvl == log.DebugLevel {
		klogLevel = "4"
	}

	flag.Set("logtostderr", "true")
	flag.Set("stderrthreshold", "INFO")
	flag.Set("v", klogLevel)
	flag.Parse()

	if !strings.EqualFold(s.mode, "node") {
		// Controller service is needed

		cfgPath = csictx.Getenv(ctx, vTypes.EnvCloudConfig)
		if cfgPath == "" {
			cfgPath = vTypes.DefaultCloudConfigPath
		}

		var cfg *cnsconfig.Config
		//Read in the vsphere.conf if it exists
		if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
			// config from Env var only
			cfg = &cnsconfig.Config{}
			if err := cnsconfig.FromEnv(cfg); err != nil {
				return err
			}
		} else {
			config, err := os.Open(cfgPath)
			if err != nil {
				log.Errorf("Failed to open %s. Err: %v", cfgPath, err)
				return err
			}
			cfg, err = cnsconfig.ReadConfig(config)
			if err != nil {
				log.Errorf("Failed to parse config. Err: %v", err)
				return err
			}
		}
		if err := s.cnscs.Init(cfg); err != nil {
			log.WithError(err).Error("Failed to init controller")
			return err
		}
	}
	return nil
}
