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
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rexray/gocsi"
	csictx "github.com/rexray/gocsi/context"
	log "github.com/sirupsen/logrus"

	vcfg "k8s.io/cloud-provider-vsphere/pkg/common/config"
	"k8s.io/cloud-provider-vsphere/pkg/csi/service/fcd"
	vTypes "k8s.io/cloud-provider-vsphere/pkg/csi/types"
)

const (
	// Name is the name of this CSI SP.
	Name = "io.k8s.cloud-provider-vsphere.vsphere"

	// APIFCD is the FCD API
	APIFCD = "FCD"

	defaultAPI = APIFCD
)

var (
	api     = defaultAPI
	cfgPath = DefaultCloudConfigPath
)

// Service is a CSI SP and idempotency.Provider.
type Service interface {
	csi.IdentityServer
	csi.NodeServer
	GetController() csi.ControllerServer
	BeforeServe(context.Context, *gocsi.StoragePlugin, net.Listener) error
}

type service struct {
	mode string
	cs   vTypes.Controller
}

// New returns a new Service.
func New() Service {
	return &service{}
}

func (s *service) GetController() csi.ControllerServer {
	// check which API to use
	api = os.Getenv(EnvAPI)
	if api == "" {
		api = defaultAPI
	}

	if strings.EqualFold(APIFCD, api) {
		s.cs = fcd.New()
	}

	return s.cs
}

func (s *service) BeforeServe(
	ctx context.Context, sp *gocsi.StoragePlugin, lis net.Listener) error {

	defer func() {
		fields := map[string]interface{}{
			"api":  api,
			"mode": s.mode,
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
		if s.cs == nil {
			return fmt.Errorf("Invalid API: %s", api)
		}

		cfgPath = csictx.Getenv(ctx, EnvCloudConfig)
		if cfgPath == "" {
			cfgPath = DefaultCloudConfigPath
		}

		var cfg *vcfg.Config

		//Read in the vsphere.conf if it exists
		if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
			// config from Env var only
			cfg = &vcfg.Config{}
			vcfg.FromEnv(cfg)
		} else {
			config, err := os.Open(cfgPath)
			if err != nil {
				log.Errorf("Failed to open %s. Err: %v", cfgPath, err)
				return err
			}
			cfg, err = vcfg.ReadConfig(config)
			if err != nil {
				log.Errorf("Failed to parse config. Err: %v", err)
				return err
			}
		}

		if err := s.cs.Init(cfg); err != nil {
			log.WithError(err).Error("Failed to init controller")
			return err
		}

	}

	return nil
}
