/*
Copyright 2019 The Kubernetes Authors.

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

package types

import "strings"

// FQDN is a normalized, case-insensitive DNS name (RFC 1034/1035), used for
// vCenter and ESXi host identifiers. The unexported field means a value can
// only be constructed via NewFQDN, so it can never carry unnormalized casing.
type FQDN struct {
	host string
}

// NewFQDN normalizes s (lowercase, no trailing dot) and returns it as an FQDN.
func NewFQDN(s string) FQDN {
	return FQDN{host: strings.TrimSuffix(strings.ToLower(s), ".")}
}

// String returns the normalized host value.
func (f FQDN) String() string {
	return f.host
}

// Equal reports whether f refers to the same host as other, a raw
// (possibly differently-cased) host string.
func (f FQDN) Equal(other string) bool {
	return strings.EqualFold(f.host, other)
}

// IsEmpty reports whether f is the zero value.
func (f FQDN) IsEmpty() bool {
	return f.host == ""
}

// UnmarshalText allows FQDN to be populated directly from config (gcfg) and
// JSON/CRD fields, normalizing on the way in.
func (f *FQDN) UnmarshalText(text []byte) error {
	*f = NewFQDN(string(text))
	return nil
}

// MarshalText allows FQDN to be serialized back to its normalized string form.
func (f FQDN) MarshalText() ([]byte, error) {
	return []byte(f.host), nil
}
