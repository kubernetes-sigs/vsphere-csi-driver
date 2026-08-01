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

package admissionhandler

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fipsonly_on.go's import of crypto/tls/fipsonly restricts the CNS-CSI admission webhook
// (port 9883) to FIPS-approved TLS settings, in particular excluding TLS_CHACHA20_POLY1305_SHA256
// from TLS 1.3, which is not FIPS-approved despite the AES-only CipherSuites list configured in
// cnscsi_admissionhandler.go (that field only governs TLS 1.0-1.2, not 1.3).
//
// crypto/tls/fipsonly itself only exists under Go's "boringcrypto" build tag, and every
// production build in this repo already compiles with GOEXPERIMENT=boringcrypto (see
// images/driver/Dockerfile, images/syncer/Dockerfile, hack/cover-images.sh, and the top-level
// Makefile). This file must therefore be gated on "boringcrypto" and nothing else: gating it on
// a different tag that no build passes silently drops FIPS-only TLS enforcement in every real
// build, without failing go build, go vet, or any unit or e2e test - only an external TLS scan
// (e.g. nmap --script ssl-enum-ciphers) against a live webhook would ever notice. This test
// guards against that regression without requiring a live network probe.
func TestFipsonlyOnGatedOnBoringcryptoBuildTag(t *testing.T) {
	data, err := os.ReadFile("fipsonly_on.go")
	require.NoError(t, err, "reading fipsonly_on.go")

	assert.Regexp(t, `(?m)^//go:build boringcrypto\s*$`, string(data),
		"fipsonly_on.go must be gated on the \"boringcrypto\" build tag (set automatically by "+
			"GOEXPERIMENT=boringcrypto) so its crypto/tls/fipsonly import is actually compiled "+
			"into every production binary; gating it on any other tag means nothing in this "+
			"repo's build scripts will ever activate it")

	assert.Contains(t, string(data), `import _ "crypto/tls/fipsonly"`,
		"fipsonly_on.go must import crypto/tls/fipsonly to enforce FIPS-only TLS settings")
}
