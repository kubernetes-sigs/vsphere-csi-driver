package config

import (
	"fmt"
	"strings"
	"testing"
)

func TestVirtualCenterConfigStringRedactsSecrets(t *testing.T) {
	vc := VirtualCenterConfig{
		User:                  "administrator@vsphere.local",
		Password:              "s3cr3t-Passw0rd!",
		VCSessionManagerToken: "tok-abc123",
	}
	out := vc.String()
	for _, secret := range []string{vc.User, vc.Password, vc.VCSessionManagerToken} {
		if strings.Contains(out, secret) {
			t.Errorf("String() leaked %q: %s", secret, out)
		}
	}
	if !strings.Contains(out, "Password:****") {
		t.Errorf("expected fixed-length mask for Password, got: %s", out)
	}
}

func TestConfigStringRedactsGlobalCredentials(t *testing.T) {
	var cfg Config
	cfg.Global.User = "administrator@vsphere.local"
	cfg.Global.Password = "s3cr3t-Passw0rd!"
	cfg.Global.VCenterPort = "443"
	for _, out := range []string{cfg.String(), fmt.Sprintf("%v", cfg), fmt.Sprintf("%+v", &cfg)} {
		if strings.Contains(out, cfg.Global.Password) || strings.Contains(out, cfg.Global.User) {
			t.Errorf("Config formatting leaked credentials: %s", out)
		}
		if !strings.Contains(out, "443") {
			t.Errorf("expected non-sensitive fields to remain visible: %s", out)
		}
	}
	// The caller's Config must not be modified by formatting it.
	if cfg.Global.Password != "s3cr3t-Passw0rd!" {
		t.Errorf("String() mutated the receiver's password")
	}
}
