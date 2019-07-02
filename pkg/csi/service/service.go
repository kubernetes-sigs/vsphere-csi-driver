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

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/fcd"
	vTypes "sigs.k8s.io/vsphere-csi-driver/pkg/csi/types"

	"k8s.io/klog"
)

const (
	// Name is the name of this CSI SP.
	Name = "vsphere.csi.vmware.com"

	// UnixSocketPrefix is the prefix before the path on disk
	UnixSocketPrefix = "unix://"

	// APIFCD is the FCD API
	APIFCD = "FCD"

	defaultAPI = APIFCD
)

var (
	api     = defaultAPI
	cfgPath = vTypes.DefaultCloudConfigPath
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

// This works around a bug that if k8s node dies, this will clean up the sock file
// left behind. This can't be done in BeforeServe because gocsi will already try to
// bind and fail because the sock file already exists.
func init() {
	sockPath := getSocketPath(gocsi.EnvVarEndpoint)
	if len(sockPath) > 1 { //minimal valid path
		os.Remove(sockPath)
	}
}

// New returns a new Service.
func New() Service {
	return &service{}
}

func (s *service) GetController() csi.ControllerServer {
	// check which API to use
	api = os.Getenv(vTypes.EnvAPI)
	if api == "" {
		api = defaultAPI
	}

	if strings.EqualFold(APIFCD, api) {
		s.cs = fcd.New()
	}

	return s.cs
}

func getSocketPath(socketPath string) string {
	return strings.TrimPrefix(socketPath, UnixSocketPrefix)
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
		klogLevel = "6"
	}

	flag.Set("logtostderr", "true")
	flag.Set("stderrthreshold", "INFO")
	flag.Set("alsologtostderr", "true")

	flag.Set("v", klogLevel)
	flag.Parse()

	// This is a temporary hack to enable proper logging until upstream dependencies
	// are migrated to fully utilize klog instead of glog.
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)

	// Sync the glog and klog flags.
	flag.VisitAll(func(f1 *flag.Flag) {
		f2 := klogFlags.Lookup(f1.Name)
		if f2 != nil {
			value := f1.Value.String()
			f2.Value.Set(value)
		}
	})

	if !strings.EqualFold(s.mode, "node") {
		// Controller service is needed
		if s.cs == nil {
			return fmt.Errorf("Invalid API: %s", api)
		}

		cfgPath = csictx.Getenv(ctx, vTypes.EnvCloudConfig)
		if cfgPath == "" {
			cfgPath = vTypes.DefaultCloudConfigPath
		}

		var cfg *vcfg.Config

		//Read in the vsphere.conf if it exists
		if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
			// config from Env var only
			cfg = &vcfg.Config{}
			if err := vcfg.FromEnv(cfg); err != nil {
				return err
			}
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
