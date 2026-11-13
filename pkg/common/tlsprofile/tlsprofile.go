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

// Package tlsprofile maps VCF TLS profiles to the TLS 1.3 key exchange groups
// used by the CSI driver and syncer, so that servers and clients offer the
// post-quantum (ML-KEM hybrid) groups required by VCF 9.2.
//
// Only key exchange groups are handled here. Cipher suites, signature
// algorithms and protocol versions are owned by the TLS Profile Management
// initiative and are intentionally left untouched.
//
// Go's crypto/tls ignores the order of Config.CurvePreferences and applies its
// own internal preference (post-quantum hybrids first), and on the server side
// always honours that preference over the client's. Setting CurvePreferences
// therefore acts as an allow-list that restricts the negotiable groups to the
// ones the selected profile permits.
package tlsprofile

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"
)

// Profile is the name of a VCF TLS profile.
type Profile string

const (
	// ProfileCompatible is the default profile.
	ProfileCompatible Profile = "COMPATIBLE"
	// ProfileNIST2024 follows the NIST 2024 guidance.
	ProfileNIST2024 Profile = "NIST_2024"
	// ProfileNIST2024TLS13Only is NIST_2024 restricted to TLS 1.3.
	ProfileNIST2024TLS13Only Profile = "NIST_2024_TLS_13_ONLY"
	// ProfileCNSA10 is the strictest profile, following CNSA 1.0.
	ProfileCNSA10 Profile = "CNSA_10"

	// ProfileEnvVar names the environment variable that selects the server TLS
	// profile until profile selection is provided by TLS Profile Management.
	ProfileEnvVar = "TLS_PROFILE"

	// DefaultProfile is used when no profile is configured.
	DefaultProfile = ProfileCompatible
)

// serverGroups lists, per profile, the groups a server accepts, as defined by the
// VCF 9.2 Post Quantum Cryptography developer guide.
var serverGroups = map[Profile][]tls.CurveID{
	ProfileCompatible: {
		tls.X25519MLKEM768,
		tls.SecP256r1MLKEM768,
		tls.SecP384r1MLKEM1024,
		tls.MLKEM1024,
		tls.CurveP256,
		tls.CurveP384,
		tls.CurveP521,
	},
	ProfileNIST2024: {
		tls.SecP256r1MLKEM768,
		tls.SecP384r1MLKEM1024,
		tls.CurveP256,
		tls.CurveP384,
		tls.CurveP521,
	},
	ProfileNIST2024TLS13Only: {
		tls.SecP256r1MLKEM768,
		tls.SecP384r1MLKEM1024,
		tls.CurveP256,
		tls.CurveP384,
		tls.CurveP521,
	},
	ProfileCNSA10: {
		tls.SecP384r1MLKEM1024,
		tls.MLKEM1024,
		tls.CurveP384,
	},
}

// clientGroups is the full list every TLS client must advertise, independent of
// profile, so that the server can choose any group its profile allows.
var clientGroups = []tls.CurveID{
	tls.X25519MLKEM768,
	tls.SecP256r1MLKEM768,
	tls.SecP384r1MLKEM1024,
	tls.MLKEM1024,
	tls.CurveP256,
	tls.CurveP384,
	tls.CurveP521,
}

// ParseProfile converts a string to a Profile. An empty string selects
// DefaultProfile. Matching is case-insensitive.
func ParseProfile(s string) (Profile, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return DefaultProfile, nil
	}
	p := Profile(strings.ToUpper(s))
	if _, ok := serverGroups[p]; !ok {
		return DefaultProfile, fmt.Errorf("unknown TLS profile %q", s)
	}
	return p, nil
}

// ProfileFromEnv returns the profile named by ProfileEnvVar. On an unknown
// value it returns DefaultProfile together with an error so that the caller can
// log it and continue with a safe default.
func ProfileFromEnv() (Profile, error) {
	return ParseProfile(os.Getenv(ProfileEnvVar))
}

// ServerCurvePreferences returns the groups a server using the given profile
// should accept. An unknown profile yields the DefaultProfile groups.
func ServerCurvePreferences(p Profile) []tls.CurveID {
	groups, ok := serverGroups[p]
	if !ok {
		groups = serverGroups[DefaultProfile]
	}
	return append([]tls.CurveID(nil), groups...)
}

// ClientCurvePreferences returns the full group list a TLS client must
// advertise.
func ClientCurvePreferences() []tls.CurveID {
	return append([]tls.CurveID(nil), clientGroups...)
}

// ApplyServer sets the key exchange groups of the given profile on cfg.
func ApplyServer(cfg *tls.Config, p Profile) {
	cfg.CurvePreferences = ServerCurvePreferences(p)
}

// ApplyServerFromEnv sets the key exchange groups of the profile selected by
// ProfileEnvVar on cfg. If the variable holds an unknown value, DefaultProfile
// is applied and the error is returned for the caller to log.
func ApplyServerFromEnv(cfg *tls.Config) (Profile, error) {
	p, err := ProfileFromEnv()
	ApplyServer(cfg, p)
	return p, err
}

// ApplyClient sets the full client key exchange group list on cfg.
func ApplyClient(cfg *tls.Config) {
	cfg.CurvePreferences = ClientCurvePreferences()
}
