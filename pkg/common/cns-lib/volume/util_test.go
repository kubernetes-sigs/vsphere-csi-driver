/*
Copyright 2025 The Kubernetes Authors.

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

package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertDiskTypeToBackingType(t *testing.T) {
	tests := []struct {
		name     string
		diskType string
		want     string
	}{
		{
			name:     "thin disk type",
			diskType: "thin",
			want:     "FlatVer2BackingInfo",
		},
		{
			name:     "preallocated disk type",
			diskType: "preallocated",
			want:     "FlatVer2BackingInfo",
		},
		{
			name:     "thick disk type",
			diskType: "thick",
			want:     "FlatVer2BackingInfo",
		},
		{
			name:     "eagerZeroedThick disk type",
			diskType: "eagerZeroedThick",
			want:     "FlatVer2BackingInfo",
		},
		{
			name:     "sparse2Gb disk type",
			diskType: "sparse2Gb",
			want:     "SparseVer2BackingInfo",
		},
		{
			name:     "sparseMonolithic disk type",
			diskType: "sparseMonolithic",
			want:     "SparseVer2BackingInfo",
		},
		{
			name:     "delta disk type",
			diskType: "delta",
			want:     "SparseVer2BackingInfo",
		},
		{
			name:     "vmfsSparse disk type",
			diskType: "vmfsSparse",
			want:     "SparseVer2BackingInfo",
		},
		{
			name:     "thick2Gb disk type",
			diskType: "thick2Gb",
			want:     "FlatVer2BackingInfo",
		},
		{
			name:     "flatMonolithic disk type",
			diskType: "flatMonolithic",
			want:     "FlatVer2BackingInfo",
		},
		{
			name:     "seSparse disk type",
			diskType: "seSparse",
			want:     "SeSparseBackingInfo",
		},
		{
			name:     "rdm disk type",
			diskType: "rdm",
			want:     "RawDiskMappingVer1BackingInfo",
		},
		{
			name:     "rdmp disk type",
			diskType: "rdmp",
			want:     "RawDiskMappingVer1BackingInfo",
		},
		{
			name:     "unknown disk type",
			diskType: "unknown",
			want:     "",
		},
		{
			name:     "empty disk type",
			diskType: "",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConvertDiskTypeToBackingType(tt.diskType)
			assert.Equal(t, tt.want, got)
		})
	}
}
