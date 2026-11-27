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

import "testing"

// vSphere CSI treats StorageClass parameter keys as case insensitive, so the
// storage policy ID must be found whatever spelling the key uses.
func TestGetStoragePolicyIDFromParams(t *testing.T) {
	const id = "aa6d5a82-1c88-47e6-a6a1-1c3e2d1f0a11"
	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"camel case key", map[string]string{"storagePolicyID": id}, id},
		{"lower case key", map[string]string{"storagepolicyid": id}, id},
		{"upper case key", map[string]string{"STORAGEPOLICYID": id}, id},
		{"mixed case key among others", map[string]string{"fsType": "ext4", "StoragePolicyId": id}, id},
		{"exact key wins over other spellings", map[string]string{"storagepolicyid": id, "STORAGEPOLICYID": "other"}, id},
		{"missing key", map[string]string{"fsType": "ext4"}, ""},
		{"nil map", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStoragePolicyIDFromParams(tt.params); got != tt.want {
				t.Errorf("GetStoragePolicyIDFromParams(%v) = %q, want %q", tt.params, got, tt.want)
			}
		})
	}
}
