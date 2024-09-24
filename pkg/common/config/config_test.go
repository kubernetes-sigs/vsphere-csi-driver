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
	"fmt"
	"os"
	"reflect"
	"strings"
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
			User:         "Administrator@vsphere.local",
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

func TestValidateConfigWithInvalidUsername(t *testing.T) {
	vcConfigInvalidUsername := map[string]*VirtualCenterConfig{
		"1.1.1.1": {
			User:         "Administrator",
			Password:     "Password",
			VCenterPort:  "443",
			Datacenters:  "dc1",
			InsecureFlag: true,
		},
	}
	cfg := &Config{
		VirtualCenter: vcConfigInvalidUsername,
	}

	err := validateConfig(ctx, cfg)
	if err == nil {
		t.Errorf("Expected error due to invalid username. Config given - %+v", *cfg)
	}
}

func TestValidateConfigWithValidUsername1(t *testing.T) {
	vcConfigValidUsername := map[string]*VirtualCenterConfig{
		"1.1.1.1": {
			User:         "Administrator@vsphere.local",
			Password:     "Password",
			VCenterPort:  "443",
			Datacenters:  "dc1",
			InsecureFlag: true,
		},
	}
	cfg := &Config{
		VirtualCenter: vcConfigValidUsername,
	}

	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error, as valid username is specified. Config given - %+v", *cfg)
	}
}

func TestValidateConfigWithValidUsername2(t *testing.T) {
	vcConfigValidUsername := map[string]*VirtualCenterConfig{
		"1.1.1.1": {
			User:         "vsphere.local\\Administrator",
			Password:     "Password",
			VCenterPort:  "443",
			Datacenters:  "dc1",
			InsecureFlag: true,
		},
	}
	cfg := &Config{
		VirtualCenter: vcConfigValidUsername,
	}

	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error, as valid username is specified. Config given - %+v", *cfg)
	}
}

func TestSensitiveConfigFieldsRedacted(t *testing.T) {
	vc := VirtualCenterConfig{
		User:         "Administrator@vsphere.local",
		Password:     "sensitivepassword",
		VCenterPort:  "443",
		Datacenters:  "dc1",
		InsecureFlag: true,
	}

	s := fmt.Sprintf("%+v", vc)
	if strings.Contains(s, "sensitivepassword") {
		t.Errorf("Sensitive information leaked in VirtualCenterConfig struct:\n%s", s)
	}
}

func TestSnapshotConfigWhenMaxUnspecified(t *testing.T) {
	cfg := &Config{
		VirtualCenter: idealVCConfig,
	}
	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error during confid validation - %+v", *cfg)
	}
	if cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume != DefaultGlobalMaxSnapshotsPerBlockVolume {
		t.Errorf("Default max number of snaps incorrect")
	}
}

func TestSnapshotConfigWhenMaxSpecifiedAsEnv(t *testing.T) {
	cfg := &Config{
		VirtualCenter: idealVCConfig,
	}
	// Temporarily set env variable.
	os.Setenv("GLOBAL_MAX_SNAPSHOTS_PER_BLOCK_VOLUME", "5")
	err := FromEnv(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error during confid validation - %+v", *cfg)
	}
	// Unset after reading to prevent effects on future tests.
	os.Unsetenv("GLOBAL_MAX_SNAPSHOTS_PER_BLOCK_VOLUME")
	if cfg.Snapshot.GlobalMaxSnapshotsPerBlockVolume != 5 {
		t.Errorf("Max number of snapshots from env variable ignored")
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
