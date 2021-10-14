//go:build darwin || linux
// +build darwin linux

/*
Copyright 2021 The Kubernetes Authors.

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

package osutils

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGetDiskPath(t *testing.T) {
	osUtils, _ := NewOsUtils(context.TODO())
	tests := []struct {
		devs  []os.FileInfo
		volID string
		match bool
	}{
		{
			devs: []os.FileInfo{
				&FakeFileInfo{name: "wwn-0x702438570234875"},
				&FakeFileInfo{name: "wwn-0x702345804753484"},
			},
			volID: "702438570234875",
			match: true,
		},
		{
			devs: []os.FileInfo{
				&FakeFileInfo{name: "wwn-0x702438570234435"},
				&FakeFileInfo{name: "wwn-0x702345804753484"},
			},
			volID: "702438570234875",
			match: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("", func(st *testing.T) {
			st.Parallel()
			d, e := osUtils.GetDiskPath(tt.volID, tt.devs)
			if e != nil {
				t.Errorf("%v", e)
			}

			disk := filepath.Join(devDiskID, blockPrefix+tt.volID)
			if tt.match {
				if d != disk {
					t.Errorf("Expected disk: %s got: %s", disk, d)
				}
			} else {
				if d != "" {
					t.Errorf("Expected disk: got: %s", d)
				}
			}
		})
	}
}

type FakeFileInfo struct {
	name string
}

func (fi *FakeFileInfo) Name() string {
	return fi.name
}

func (fi *FakeFileInfo) Size() int64 {
	return 0
}

func (fi *FakeFileInfo) Mode() os.FileMode {
	return 0
}

func (fi *FakeFileInfo) ModTime() time.Time {
	return time.Now()
}

func (fi *FakeFileInfo) IsDir() bool {
	return false
}

func (fi *FakeFileInfo) Sys() interface{} {
	return nil
}
