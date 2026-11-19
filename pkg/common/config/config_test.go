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

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

var (
	ctx           context.Context
	cancel        context.CancelFunc
	idealVCConfig map[types.FQDN]*VirtualCenterConfig
)

func init() {
	// Create context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	idealVCConfig = map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
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
	vcConfigInvalidUsername := map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
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
	vcConfigValidUsername := map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
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

func TestValidateConfigWithSessionManager(t *testing.T) {
	vcConfigValidUsername := map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
			VCenterPort:         "443",
			Datacenters:         "dc1",
			InsecureFlag:        true,
			VCSessionManagerURL: "http://xxx.yyy.com/tld",
		},
	}
	cfg := &Config{
		VirtualCenter: vcConfigValidUsername,
	}

	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error, as valid session manager was used. Config given - %+v", *cfg)
	}
}

func TestValidateConfigWithSessionManagerAndToken(t *testing.T) {
	vcConfigValidUsername := map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
			VCenterPort:           "443",
			Datacenters:           "dc1",
			InsecureFlag:          true,
			VCSessionManagerURL:   "http://xxx.yyy.com/tld",
			VCSessionManagerToken: "a-secret-token",
		},
	}
	cfg := &Config{
		VirtualCenter: vcConfigValidUsername,
	}

	err := validateConfig(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error, as a valid session manager with a token was used. Config given - %+v", *cfg)
	}
}

// TestValidateConfigWithSessionManagerTokenButNoURL confirms that
// VCSessionManagerToken alone does not bypass the username/password
// requirement -- only VCSessionManagerURL does. Per the docs, when the URL is
// unset CSI falls back to the Pod service account token rather than using
// VCSessionManagerToken at all, so a token with no URL configured has nowhere
// to be sent and must not silently pass validation.
func TestValidateConfigWithSessionManagerTokenButNoURL(t *testing.T) {
	vcConfigTokenOnly := map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
			VCenterPort:           "443",
			Datacenters:           "dc1",
			InsecureFlag:          true,
			VCSessionManagerToken: "a-secret-token",
		},
	}
	cfg := &Config{
		VirtualCenter: vcConfigTokenOnly,
	}

	err := validateConfig(ctx, cfg)
	if err == nil {
		t.Errorf("Expected error: a session manager token with no URL and no username/password "+
			"should not validate. Config given - %+v", *cfg)
	}
}

func TestValidateConfigWithValidUsername2(t *testing.T) {
	vcConfigValidUsername := map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("1.1.1.1"): {
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

func newServiceAccountConfig(vc *VirtualCenterConfig) *Config {
	cfg := &Config{VirtualCenter: map[types.FQDN]*VirtualCenterConfig{types.NewFQDN("vc.example.com"): vc}}
	cfg.Global.VSphereServiceAccount = "csi"
	return cfg
}

func TestValidateConfigWithServiceAccount(t *testing.T) {
	os.Setenv("CLUSTER_FLAVOR", "WORKLOAD")
	defer os.Unsetenv("CLUSTER_FLAVOR")

	// Neither user nor password is needed: they come from the service account's Secret.
	cfg := newServiceAccountConfig(&VirtualCenterConfig{SDDCName: "sddc-1"})
	if err := validateConfig(ctx, cfg); err != nil {
		t.Errorf("unexpected error with a service account and no credentials: %v", err)
	}

	cfg = newServiceAccountConfig(&VirtualCenterConfig{})
	if err := validateConfig(ctx, cfg); err != ErrSDDCNameMissing {
		t.Errorf("expected ErrSDDCNameMissing, got %v", err)
	}

	for name, vc := range map[string]*VirtualCenterConfig{
		"user":     {SDDCName: "sddc-1", User: "a@vsphere.local"},
		"password": {SDDCName: "sddc-1", Password: "p"},
	} {
		if err := validateConfig(ctx, newServiceAccountConfig(vc)); err != ErrCredentialsWithServiceAccount {
			t.Errorf("literal %s: expected ErrCredentialsWithServiceAccount, got %v", name, err)
		}
	}
	cfg = newServiceAccountConfig(&VirtualCenterConfig{SDDCName: "sddc-1"})
	cfg.Global.Password = "p"
	if err := validateConfig(ctx, cfg); err != ErrCredentialsWithServiceAccount {
		t.Errorf("global password: expected ErrCredentialsWithServiceAccount, got %v", err)
	}
}

func TestValidateConfigWithoutServiceAccountStillNeedsCredentials(t *testing.T) {
	os.Setenv("CLUSTER_FLAVOR", "WORKLOAD")
	defer os.Unsetenv("CLUSTER_FLAVOR")
	cfg := &Config{VirtualCenter: map[types.FQDN]*VirtualCenterConfig{
		types.NewFQDN("vc.example.com"): {SDDCName: "sddc-1"},
	}}
	if err := validateConfig(ctx, cfg); err != ErrUsernameMissing {
		t.Errorf("expected ErrUsernameMissing, got %v", err)
	}
}

func TestPostProcessor(t *testing.T) {
	defer SetPostProcessor(nil)

	// With nothing registered, a service account cannot be resolved; without one nothing changes.
	SetPostProcessor(nil)
	if err := runPostProcessor(ctx, &Config{}); err != nil {
		t.Errorf("unexpected error without a service account: %v", err)
	}
	withSA := &Config{}
	withSA.Global.VSphereServiceAccount = "csi"
	if err := runPostProcessor(ctx, withSA); err == nil {
		t.Errorf("expected an error for a service account without a registered source")
	}

	called := 0
	SetPostProcessor(func(_ context.Context, cfg *Config) error {
		called++
		cfg.Global.User = "set-by-processor"
		return nil
	})
	cfg := &Config{}
	if err := runPostProcessor(ctx, cfg); err != nil || called != 1 || cfg.Global.User != "set-by-processor" {
		t.Errorf("post processor not applied: err=%v called=%d user=%q", err, called, cfg.Global.User)
	}
}

func TestReadConfigServiceAccountKeys(t *testing.T) {
	os.Setenv("CLUSTER_FLAVOR", "WORKLOAD")
	defer os.Unsetenv("CLUSTER_FLAVOR")
	ini := `[Global]
vsphere-service-account = "csi"
supervisor-id = "sv-1"
[VirtualCenter "vc.example.com"]
sddc-name = "sddc-1"
port = "443"
datacenters = "dc-1"
`
	cfg, err := ReadConfig(ctx, strings.NewReader(ini))
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if cfg.Global.VSphereServiceAccount != "csi" {
		t.Errorf("service account = %q", cfg.Global.VSphereServiceAccount)
	}
	vc := cfg.VirtualCenter[types.NewFQDN("vc.example.com")]
	if vc == nil || vc.SDDCName != "sddc-1" {
		t.Fatalf("sddc-name not read: %+v", vc)
	}
	if err := validateConfig(ctx, cfg); err != nil {
		t.Errorf("validateConfig: %v", err)
	}
}
