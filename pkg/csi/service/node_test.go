/*
Copyright 2018 The Kubernetes Authors.

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

package service

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestGetDisk(t *testing.T) {
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
			d, e := getDiskPath(tt.volID, tt.devs)
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

func TestUnescape(t *testing.T) {
	tests := []struct {
		in, out string
	}{
		{
			// Space is unescaped. This is basically the only test that can happen in reality
			// and only when in-tree in-line volume in a Pod is used with CSI migration enabled.
			in:  `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-[WorkloadDatastore]\0405137595f-7ce3-e95a-5c03-06d835dea807/e2e-vmdk-1641374604660540311.vmdk/globalmount`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-[WorkloadDatastore] 5137595f-7ce3-e95a-5c03-06d835dea807/e2e-vmdk-1641374604660540311.vmdk/globalmount`,
		},
		{
			// Multiple spaces are unescaped.
			in:  `/var/lib/kube\040let/plug\040ins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-foo\040bar\040baz`,
			out: `/var/lib/kube let/plug ins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-foo bar baz`,
		},
		{
			// Too short escape sequence. Expect the same string on output.
			in:  `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\04`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\04`,
		},
		{
			// Wrong characters in the escape sequence. Expect the same string on output.
			in:  `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\0bc`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\0bc`,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			out := unescape(ctx, test.in)
			if out != test.out {
				t.Errorf("Expected %q to be unescaped as %q, got %q", test.in, test.out, out)
			}
		})
	}
}
