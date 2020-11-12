/*
Copyright 2020 The Kubernetes Authors.

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

package config

import (
	"context"
	"os"
	"reflect"
	"testing"
)

var (
	ctx           context.Context
	cancel        context.CancelFunc
	idealVCConfig map[string]*VirtualCenterConfig
)

func init() {
	// Create context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	idealVCConfig = map[string]*VirtualCenterConfig{
		"1.1.1.1": {
			User:         "Admin",
			Password:     "Password",
			VCenterPort:  "443",
			Datacenters:  "dc1",
			InsecureFlag: true,
		},
	}
}

func TestValidateConfigWithNoNetPermissionParams(t *testing.T) {
	cfg := &Config{VirtualCenter: idealVCConfig}
	os.Setenv("CLUSTER_FLAVOR", "VANILLA")
	expectedNetPermissions := map[string]*NetPermissionConfig{"#": GetDefaultNetPermission()}
	expectedConfig := &Config{
		VirtualCenter:  idealVCConfig,
		NetPermissions: expectedNetPermissions,
	}

	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("failed to validate config %+v. Received error: %v", *cfg, err)
	}

	if !isConfigEqual(cfg, expectedConfig) {
		t.Errorf("Expected: %+v\n Actual: %+v", cfg, expectedConfig)
	}
}

func TestValidateConfigWithMultipleNetPermissionParams(t *testing.T) {
	netPerms := map[string]*NetPermissionConfig{
		"A": {
			Ips:         "",
			Permissions: "READ_WRITE",
			RootSquash:  false,
		},
		"B": {
			Ips:         "10.20.20.0/24",
			Permissions: "READ_ONLY",
			RootSquash:  true,
		},
		"C": {
			Ips:         "10.20.30.0/24",
			Permissions: "",
			RootSquash:  true,
		},
		"D": {
			Ips:         "10.20.40.0/24",
			Permissions: "NO_ACCESS",
			RootSquash:  false,
		},
	}
	cfg := &Config{
		VirtualCenter:  idealVCConfig,
		NetPermissions: netPerms,
	}

	expectedNetPermissions := map[string]*NetPermissionConfig{
		"A": {
			Ips:         "*",
			Permissions: "READ_WRITE",
			RootSquash:  false,
		},
		"B": {
			Ips:         "10.20.20.0/24",
			Permissions: "READ_ONLY",
			RootSquash:  true,
		},
		"C": {
			Ips:         "10.20.30.0/24",
			Permissions: "READ_WRITE",
			RootSquash:  true,
		},
		"D": {
			Ips:         "10.20.40.0/24",
			Permissions: "NO_ACCESS",
			RootSquash:  false,
		},
	}
	expectedConfig := &Config{
		VirtualCenter:  idealVCConfig,
		NetPermissions: expectedNetPermissions,
	}

	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("failed to validate config %+v. Received error: %v", *cfg, err)
	}

	if !isConfigEqual(cfg, expectedConfig) {
		t.Errorf("Expected: %+v\n Actual: %+v", cfg, expectedConfig)
	}
}

func TestValidateConfigWithInvalidPermissions(t *testing.T) {
	netPerms := map[string]*NetPermissionConfig{
		"A": {
			Ips:         "",
			Permissions: "WRITE_ONLY",
			RootSquash:  false,
		},
	}
	cfg := &Config{
		VirtualCenter:  idealVCConfig,
		NetPermissions: netPerms,
	}

	err := validateConfig(ctx, cfg)
	if err == nil {
		t.Errorf("Expected error due to wrong Permissions value in NetPermissions. Config given - %+v", *cfg)
	}
}

func TestValidateConfigWithInvalidClusterId(t *testing.T) {
	cfg := &Config{
		VirtualCenter: idealVCConfig,
	}
	cfg.Global.ClusterID = "test-cluster-with-a-long-name-with-more-than-sixty-four-characters"

	err := validateConfig(ctx, cfg)
	if err == nil {
		t.Errorf("Expected error due to invalid cluster id. Config given - %+v", *cfg)
	}
}

func isConfigEqual(actual *Config, expected *Config) bool {
	// TODO: Compare Global struct
	// Compare VC Config
	if !reflect.DeepEqual(actual.VirtualCenter, expected.VirtualCenter) {
		return false
	}
	// Compare net permissions
	if !reflect.DeepEqual(actual.NetPermissions, expected.NetPermissions) {
		return false
	}
	return true
}
