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

	"k8s.io/klog"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/vanilla"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/wcp"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/wcpguest"

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
	Name = "csi.vsphere.vmware.com"

	// VanillaK8SControllerType indicated Vanilla K8S CSI Controller
	VanillaK8SControllerType = "VANILLA"

	// WcpControllerType indicated WCP CSI Controller
	WcpControllerType = "WCP"

	//WcpGuestControllerType indicated WCPGC CSI Controller
	WcpGuestControllerType = "WCPGC"

	defaultController = VanillaK8SControllerType

	// UnixSocketPrefix is the prefix before the path on disk
	UnixSocketPrefix = "unix://"
)

var (
	controllerType = defaultController
	cfgPath        = cnsconfig.DefaultCloudConfigPath
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
	controllerType = os.Getenv(vTypes.EnvControllerType)
	switch controllerType {
	case WcpControllerType:
		s.cnscs = wcp.New()
	case WcpGuestControllerType:
		s.cnscs = wcpguest.New()
	default:
		s.cnscs = vanilla.New()
	}

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

	if !strings.EqualFold(s.mode, "node") {
		// Controller service is needed
		var cfg *cnsconfig.Config
		cfgPath = csictx.Getenv(ctx, cnsconfig.EnvCloudConfig)
		if cfgPath == "" {
			cfgPath = cnsconfig.DefaultCloudConfigPath
		}
		cfg, err := cnsconfig.GetCnsconfig(cfgPath)
		if err != nil {
			klog.Errorf("Failed to read cnsconfig. Error: %v", err)
			return err
		}
		if err := s.cnscs.Init(cfg); err != nil {
			log.WithError(err).Error("Failed to init controller")
			return err
		}
	}
	return nil
}
