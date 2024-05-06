/*
Copyright 2024 The Kubernetes Authors.

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

package vsphere

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"testing"

	cnssim "github.com/vmware/govmomi/cns/simulator"
	pbmsim "github.com/vmware/govmomi/pbm/simulator"
	"github.com/vmware/govmomi/simulator"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

// We should get expected failure when username is not a fully qualified domain name
func TestConnectCnsWithInvalidUser(t *testing.T) {
	// Create context.
	ctx := context.Background()

	cfg := &config.Config{}
	model := simulator.VPX()
	defer model.Remove()

	err := model.Create()
	if err != nil {
		log.Fatal(err)
	}

	model.Service.TLS = new(tls.Config)
	s := model.Service.NewServer()
	defer s.Close()

	// CNS Service simulator.
	model.Service.RegisterSDK(cnssim.New())

	// PBM Service simulator.
	model.Service.RegisterSDK(pbmsim.New())
	cfg.Global.InsecureFlag = true

	cfg.Global.VCenterIP = s.URL.Hostname()
	cfg.Global.VCenterPort = s.URL.Port()
	// Username doesn't have fully qualified domain name
	cfg.Global.User = "username-without-fqdn"
	cfg.Global.Password, _ = s.URL.User.Password()
	cfg.Global.Datacenters = "DC0"

	// Write values to test_vsphere.conf.
	os.Setenv("VSPHERE_CSI_CONFIG", "test_vsphere.conf")
	conf := []byte(fmt.Sprintf("[Global]\ninsecure-flag = \"%t\"\n"+
		"[VirtualCenter \"%s\"]\nuser = \"%s\"\npassword = \"%s\"\ndatacenters = \"%s\"\nport = \"%s\"",
		cfg.Global.InsecureFlag, cfg.Global.VCenterIP, cfg.Global.User, cfg.Global.Password,
		cfg.Global.Datacenters, cfg.Global.VCenterPort))
	err = os.WriteFile("test_vsphere.conf", conf, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		os.Unsetenv("VSPHERE_CSI_CONFIG")
		os.Remove("test_vsphere.conf")
	}()

	cfg.VirtualCenter = make(map[string]*config.VirtualCenterConfig)
	cfg.VirtualCenter[s.URL.Hostname()] = &config.VirtualCenterConfig{
		User:         cfg.Global.User,
		Password:     cfg.Global.Password,
		VCenterPort:  cfg.Global.VCenterPort,
		InsecureFlag: cfg.Global.InsecureFlag,
		Datacenters:  cfg.Global.Datacenters,
	}

	// CNS based CSI requires a valid cluster name.
	cfg.Global.ClusterID = "test-cluster"

	vcenterconfig, err := GetVirtualCenterConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	vcManager := GetVirtualCenterManager(ctx)
	vcenter, err := vcManager.RegisterVirtualCenter(ctx, vcenterconfig)
	if err != nil {
		t.Fatal(err)
	}

	err = vcenter.ConnectCns(ctx)
	if err != nil {
		t.Logf("Expected error received, as username is not a fully qualified domain name.")
	} else {
		t.Fatal("CNS client creation should fail as username is not a fully qualified domain name.")
	}
}
