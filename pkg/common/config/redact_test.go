package config

import (
	"context"
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

func TestReadConfigDoesNotEchoRejectedValues(t *testing.T) {
	const secret = "Sup3rSecretValue"
	cases := map[string]string{
		"bool":    "[Global]\ninsecure-flag = " + secret + "\n",
		"int":     "[Global]\nquery-limit = " + secret + "\n",
		"gc-bool": "[Global]\ninsecure-flag = " + secret + "\n",
	}
	for name, content := range cases {
		var err error
		if name == "gc-bool" {
			_, err = ReadGCConfig(context.Background(), strings.NewReader(content))
		} else {
			_, err = ReadConfig(context.Background(), strings.NewReader(content))
		}
		if err == nil {
			t.Fatalf("%s: expected a parse error", name)
		}
		if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(secret)) {
			t.Errorf("%s: error echoed the rejected value: %v", name, err)
		}
		if !strings.Contains(err.Error(), "insecure-flag") && !strings.Contains(err.Error(), "query-limit") {
			t.Errorf("%s: error should still name the offending variable: %v", name, err)
		}
	}
}

func TestRedactConfigParseError(t *testing.T) {
	if redactConfigParseError(nil) != nil {
		t.Error("nil must stay nil")
	}
	syntax := fmt.Errorf("3:19: unknown escape sequence")
	if got := redactConfigParseError(syntax); got != syntax {
		t.Errorf("positioned syntax errors should pass through, got %v", got)
	}
	tricky := fmt.Errorf("failed to parse \"x at section y\" as bool: boom at section \"Global\", variable \"insecure-flag\"")
	got := redactConfigParseError(tricky).Error()
	if strings.Contains(got, "boom") || strings.Contains(got, "x at section y") {
		t.Errorf("value text survived redaction: %s", got)
	}
	if got := redactConfigParseError(fmt.Errorf("something unexpected")).Error(); strings.Contains(got, "unexpected") {
		t.Errorf("unknown error shapes must be replaced by a generic message, got %s", got)
	}
}
