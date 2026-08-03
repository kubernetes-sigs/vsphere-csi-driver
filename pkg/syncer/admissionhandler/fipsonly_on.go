//go:build boringcrypto

package admissionhandler

// This file restricts TLS to FIPS-approved settings whenever the binary is compiled with
// GOEXPERIMENT=boringcrypto, which is how every production build (images/driver/Dockerfile,
// images/syncer/Dockerfile, hack/cover-images.sh, Makefile) already compiles this package.
// Do not gate this on any other build tag (e.g. a custom "fips" tag): crypto/tls/fipsonly
// itself only exists under the "boringcrypto" tag, and no build in this repo passes any tag
// other than what GOEXPERIMENT=boringcrypto implies. Gating on a tag that nothing sets
// silently drops FIPS-only TLS enforcement without failing any build or test - see
// fipsonly_on_test.go.
import _ "crypto/tls/fipsonly"
