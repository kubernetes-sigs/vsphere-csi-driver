package config

import (
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
