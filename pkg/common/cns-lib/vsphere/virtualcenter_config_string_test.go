/*
Copyright 2026 The Kubernetes Authors.

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
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

func TestVirtualCenterConfigStringRedactsSensitiveFields(t *testing.T) {
	cfg := &VirtualCenterConfig{
		Host:                  types.NewFQDN("vc.example.com"),
		Username:              "administrator@vsphere.local",
		Password:              "S3cretPassw0rd!",
		VCSessionManagerToken: "top-secret-token",
	}

	for _, out := range []string{cfg.String(), fmt.Sprintf("%v", cfg), fmt.Sprintf("%+v", cfg)} {
		if strings.Contains(out, cfg.Password) {
			t.Errorf("expected Password to be redacted, got: %s", out)
		}
		if strings.Contains(out, cfg.Username) {
			t.Errorf("expected Username to be redacted, got: %s", out)
		}
		if strings.Contains(out, cfg.VCSessionManagerToken) {
			t.Errorf("expected VCSessionManagerToken to be redacted, got: %s", out)
		}
		if !strings.Contains(out, cfg.Host.String()) {
			t.Errorf("expected non-sensitive Host field to be preserved, got: %s", out)
		}
	}
}

func TestVirtualCenterStringRedactsSensitiveFields(t *testing.T) {
	vc := &VirtualCenter{
		Config: &VirtualCenterConfig{
			Host:     types.NewFQDN("vc.example.com"),
			Username: "administrator@vsphere.local",
			Password: "S3cretPassw0rd!",
		},
	}

	out := fmt.Sprintf("%+v", vc)
	if strings.Contains(out, vc.Config.Password) {
		t.Errorf("expected Password to be redacted in VirtualCenter.String(), got: %s", out)
	}
}
