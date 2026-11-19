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

package tlsprofile

import (
	"crypto/fips140"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Production images are built with GOFIPS140 set to Go's native FIPS 140-3 module, which makes
// crypto/tls FIPS-only. This replaces the old crypto/tls/fipsonly import, which only exists under
// the boringcrypto build tag and does not allow the post-quantum groups. If GOFIPS140 is set for
// this test run (as the Makefile does), the binary must actually be running in FIPS mode, so a
// build that silently drops the setting is caught. Skipped for ad hoc runs without it.
func TestFIPSModeEnabledWhenBuiltWithGOFIPS140(t *testing.T) {
	v := os.Getenv("GOFIPS140")
	if v == "" || v == "off" {
		t.Skip("GOFIPS140 not set; not running in FIPS 140-3 mode")
	}
	assert.True(t, fips140.Enabled(), "GOFIPS140=%s but the FIPS 140-3 module is not enabled", v)
}
