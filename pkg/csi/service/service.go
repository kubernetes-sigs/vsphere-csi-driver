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
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/rexray/gocsi"
	log "github.com/sirupsen/logrus"

	vcfg "k8s.io/cloud-provider-vsphere/pkg/common/config"
	"k8s.io/cloud-provider-vsphere/pkg/csi/service/fcd"
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
	cs csi.ControllerServer
}

// New returns a new Service.
func New() Service {
	// check which API to use

	api = os.Getenv(EnvAPI)
	if api == "" {
		api = defaultAPI
	}
	cfgPath = os.Getenv(EnvCloudConfig)
	if cfgPath == "" {
		cfgPath = DefaultCloudConfigPath
	}

	//Read in the vsphere.conf
	config, err := os.Open(cfgPath)
	if err != nil {
		log.Fatalln("Failed to open", cfgPath, ". Err:", err)
	}
	cfg, err := vcfg.ReadConfig(config)
	if err != nil {
		log.Fatalln("Failed to parse config. Err:", err)
	}

	if strings.EqualFold(APIFCD, api) {
		return &service{
			cs: fcd.New(&cfg),
		}
	}
	return &service{}
}

func (s *service) GetController() csi.ControllerServer {
	return s.cs
}

func (s *service) BeforeServe(
	ctx context.Context, sp *gocsi.StoragePlugin, lis net.Listener) error {

	defer func() {
		fields := map[string]interface{}{
			"api": api,
		}

		log.WithFields(fields).Infof("configured: %s", Name)
	}()

	if s.cs == nil {
		return fmt.Errorf("Invalid API: %s", api)
	}

	return nil
}
