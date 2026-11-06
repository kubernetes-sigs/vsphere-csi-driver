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

package runtimeconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	// LabelServiceAccount is the label vsphere-auth puts on a credentials Secret with the
	// (bound) name of the VSphereServiceAccount it belongs to.
	LabelServiceAccount = "vsphere-auth.vmware.com/vsphereserviceaccount"
	// LabelSDDC is the label vsphere-auth puts on a credentials Secret with the (bound) name
	// of the SDDC the credentials are for.
	LabelSDDC = "vsphere-auth.vmware.com/sddc"

	// maxLabelValueLength is the longest value Kubernetes accepts for a label.
	maxLabelValueLength = 63
)

// Bound turns a resource name into a label value. It must stay byte-identical to vsphere-auth's
// label helper, which produced the labels on the Secret: the name lower-cased when that fits in
// a label value, otherwise the first 8 bytes of its SHA-256 as hex.
func Bound(s string) string {
	s = strings.ToLower(s)
	if len(s) <= maxLabelValueLength {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}
