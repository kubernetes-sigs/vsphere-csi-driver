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

package cnsregistervolume

import (
	"testing"

	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
)

// The CnsRegisterVolume CRD accepts upper-case hex in spec.volumeID, but CNS
// returns lowercase IDs and the derived PV name must be a valid lowercase
// Kubernetes name, so the typed ID must be normalized before use.
func TestNormalizedVolumeID(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"already lowercase", "a1b2c3d4-0000-4000-8000-0123456789ab", "a1b2c3d4-0000-4000-8000-0123456789ab"},
		{"upper case", "A1B2C3D4-0000-4000-8000-0123456789AB", "a1b2c3d4-0000-4000-8000-0123456789ab"},
		{"mixed case with spaces", "  A1b2C3d4-0000-4000-8000-0123456789aB \n", "a1b2c3d4-0000-4000-8000-0123456789ab"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
			instance.Spec.VolumeID = tt.in
			if got := normalizedVolumeID(instance); got != tt.want {
				t.Errorf("normalizedVolumeID(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if instance.Spec.VolumeID != tt.in {
				t.Errorf("normalizedVolumeID must not modify the CR spec")
			}
		})
	}
}
