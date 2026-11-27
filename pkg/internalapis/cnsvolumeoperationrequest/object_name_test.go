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

package cnsvolumeoperationrequest

import "testing"

// TestObjectNameForVolumeID pins the object names derived from volume IDs. The CnsVolumeOperationRequest
// names ("delete-<id>", "expand-<id>") and the CNSVolumeInfo name ("<id>") are persisted, so a change in
// this rule would orphan the existing objects of in-flight operations.
func TestObjectNameForVolumeID(t *testing.T) {
	const uuid = "e6a32a53-2783-42cd-a854-4df28582f04c"
	tests := []struct {
		name     string
		volumeID string
		want     string
	}{
		{"legacy file volume", "file:" + uuid, "file-" + uuid},
		{"block volume", uuid, uuid},
		{"empty id", "", ""},
		// Only the colon after the "file:" prefix is replaced.
		{"legacy file volume with extra colon", "file:" + uuid + ":x", "file-" + uuid + ":x"},
		// FVS ids never reach the CNS delete and expand paths, so they are left as they are.
		{"FVS volume", "fv:ns:file-volume", "fv:ns:file-volume"},
		{"id merely containing file", "my-file-" + uuid, "my-file-" + uuid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ObjectNameForVolumeID(tc.volumeID); got != tc.want {
				t.Errorf("ObjectNameForVolumeID(%q) = %q, want %q", tc.volumeID, got, tc.want)
			}
		})
	}
}

// TestObjectNameForVolumeID_OperationRequestNames pins the full CnsVolumeOperationRequest names built
// for delete and expand of a legacy file volume and of a block volume.
func TestObjectNameForVolumeID_OperationRequestNames(t *testing.T) {
	const uuid = "e6a32a53-2783-42cd-a854-4df28582f04c"
	if got, want := "delete-"+ObjectNameForVolumeID("file:"+uuid), "delete-file-"+uuid; got != want {
		t.Errorf("delete name = %q, want %q", got, want)
	}
	if got, want := "expand-"+ObjectNameForVolumeID("file:"+uuid), "expand-file-"+uuid; got != want {
		t.Errorf("expand name = %q, want %q", got, want)
	}
	if got, want := "delete-"+ObjectNameForVolumeID(uuid), "delete-"+uuid; got != want {
		t.Errorf("block delete name = %q, want %q", got, want)
	}
	if got, want := "expand-"+ObjectNameForVolumeID(uuid), "expand-"+uuid; got != want {
		t.Errorf("block expand name = %q, want %q", got, want)
	}
}
