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

package common

import (
	"testing"
)

// TestVMOwnedVolumesFSSInWCPFeatureStates verifies VMOwnedVolumes is registered
// as a WCP capability so IsFSSEnabled reads it from the Capabilities CR.
func TestVMOwnedVolumesFSSInWCPFeatureStates(t *testing.T) {
	if _, ok := WCPFeatureStates[VMOwnedVolumes]; !ok {
		t.Errorf("VMOwnedVolumes (%q) is missing from WCPFeatureStates map", VMOwnedVolumes)
	}
}

// TestVMOwnedVolumesFSSInLateEnablement verifies VMOwnedVolumes is registered
// for late enablement so the driver can detect it being turned on post-startup.
func TestVMOwnedVolumesFSSInLateEnablement(t *testing.T) {
	if _, ok := WCPFeatureStatesSupportsLateEnablement[VMOwnedVolumes]; !ok {
		t.Errorf("VMOwnedVolumes (%q) is missing from WCPFeatureStatesSupportsLateEnablement map", VMOwnedVolumes)
	}
}

// TestVMOwnedVolumesFSSNotInPVCSIAssociation verifies VMOwnedVolumes is NOT in
// the pvCSI association map since this is a supervisor-only feature.
func TestVMOwnedVolumesFSSNotInPVCSIAssociation(t *testing.T) {
	for pvcsiFSS, wcpCap := range WCPFeatureStateAssociatedWithPVCSI {
		if wcpCap == VMOwnedVolumes {
			t.Errorf("VMOwnedVolumes should not appear in WCPFeatureStateAssociatedWithPVCSI "+
				"(found as value for pvCSI FSS %q)", pvcsiFSS)
		}
	}
}
