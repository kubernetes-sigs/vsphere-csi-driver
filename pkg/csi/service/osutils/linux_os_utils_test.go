//go:build darwin || linux
// +build darwin linux

package osutils

import (
	"context"
	"strconv"
	"testing"
)

func TestUnescape(t *testing.T) {
	tests := []struct {
		in, out string
	}{
		{
			// Space is unescaped. This is basically the only test that can happen in reality
			// and only when in-tree in-line volume in a Pod is used with CSI migration enabled.
			in: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/csi.vsphere.vmware.com` +
				`-[WorkloadDatastore]\0405137595f-7ce3-e95a-5c03-06d835dea807/` +
				`e2e-vmdk-1641374604660540311.vmdk/globalmount`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/csi.vsphere.vmware.com` +
				`-[WorkloadDatastore] 5137595f-7ce3-e95a-5c03-06d835dea807/` +
				`e2e-vmdk-1641374604660540311.vmdk/globalmount`,
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
