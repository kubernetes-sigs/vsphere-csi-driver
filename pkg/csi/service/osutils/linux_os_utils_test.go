//go:build darwin || linux
// +build darwin linux

package osutils

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"

	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
	testingexec "k8s.io/utils/exec/testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// erroringMounter wraps a *mount.FakeMounter but fails Mount() with mountErr, when set.
type erroringMounter struct {
	*mount.FakeMounter
	mountErr error
}

func (m *erroringMounter) Mount(source, target, fstype string, options []string) error {
	if m.mountErr != nil {
		return m.mountErr
	}
	return m.FakeMounter.Mount(source, target, fstype, options)
}

// fakeMkfsFailure returns a mkfs.<fstype> invocation that fails.
func fakeMkfsFailure(err error) testingexec.FakeCommandAction {
	return func(cmd string, args ...string) utilexec.Cmd {
		return &testingexec.FakeCmd{
			CombinedOutputScript: []testingexec.FakeAction{
				func() ([]byte, []byte, error) { return []byte("mkfs failed"), nil, err },
			},
		}
	}
}

// newFakeOsUtils builds an OsUtils backed by a fake mount.Interface and a fake
// exec.Interface. blkidOutput/blkidErr control what getDiskFormat observes for the
// "is the disk already formatted" check; mkfsAction (if non-nil) is invoked for the
// subsequent mkfs.<fstype> call and can be used to capture/assert on its argv.
func newFakeOsUtils(blkidOutput string, blkidErr error, mkfsAction testingexec.FakeCommandAction) *OsUtils {
	commandScript := []testingexec.FakeCommandAction{
		func(cmd string, args ...string) utilexec.Cmd {
			return &testingexec.FakeCmd{
				CombinedOutputScript: []testingexec.FakeAction{
					func() ([]byte, []byte, error) { return []byte(blkidOutput), nil, blkidErr },
				},
			}
		},
	}
	if mkfsAction != nil {
		commandScript = append(commandScript, mkfsAction)
	}
	return &OsUtils{
		Mounter: &mount.SafeFormatAndMount{
			Interface: mount.NewFakeMounter(nil),
			Exec:      &testingexec.FakeExec{CommandScript: commandScript},
		},
	}
}

// fakeMkfsSuccess records the argv it was invoked with into *argsOut and returns success.
func fakeMkfsSuccess(argsOut *[]string) testingexec.FakeCommandAction {
	return func(cmd string, args ...string) utilexec.Cmd {
		*argsOut = args
		return &testingexec.FakeCmd{
			CombinedOutputScript: []testingexec.FakeAction{
				func() ([]byte, []byte, error) { return []byte(""), nil, nil },
			},
		}
	}
}

func TestXfsFormatAndMount(t *testing.T) {
	var mkfsArgs []string
	osUtils := newFakeOsUtils("", testingexec.FakeExitError{Status: 2}, fakeMkfsSuccess(&mkfsArgs))

	err := osUtils.xfsFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", "xfs")
	if err != nil {
		t.Fatalf("xfsFormatAndMount returned unexpected error: %v", err)
	}

	if !slices.Contains(mkfsArgs, "-K") {
		t.Errorf("expected -K in mkfs.xfs args, got %v", mkfsArgs)
	}
}

func TestXfsFormatAndMount_SkipsMkfsWhenAlreadyFormatted(t *testing.T) {
	mkfsCalled := false
	osUtils := newFakeOsUtils("TYPE=xfs\n", nil, func(cmd string, args ...string) utilexec.Cmd {
		mkfsCalled = true
		return &testingexec.FakeCmd{
			CombinedOutputScript: []testingexec.FakeAction{
				func() ([]byte, []byte, error) { return []byte(""), nil, nil },
			},
		}
	})

	err := osUtils.xfsFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", "xfs")
	if err != nil {
		t.Fatalf("xfsFormatAndMount returned unexpected error: %v", err)
	}
	if mkfsCalled {
		t.Error("expected mkfs.xfs not to be invoked for an already-formatted disk")
	}
}

func TestExtFormatAndMount(t *testing.T) {
	for _, fstype := range []string{common.Ext4FsType, common.Ext3FsType} {
		t.Run(fstype, func(t *testing.T) {
			var mkfsArgs []string
			osUtils := newFakeOsUtils("", testingexec.FakeExitError{Status: 2}, fakeMkfsSuccess(&mkfsArgs))

			err := osUtils.extFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", fstype)
			if err != nil {
				t.Fatalf("extFormatAndMount returned unexpected error: %v", err)
			}

			want := []string{"-F", "-E", "nodiscard", "/dev/fake"}
			if !slices.Equal(mkfsArgs, want) {
				t.Errorf("expected mkfs.%s args %v, got %v", fstype, want, mkfsArgs)
			}
		})
	}
}

func TestExtFormatAndMount_SkipsMkfsWhenAlreadyFormatted(t *testing.T) {
	mkfsCalled := false
	osUtils := newFakeOsUtils("TYPE=ext4\n", nil, func(cmd string, args ...string) utilexec.Cmd {
		mkfsCalled = true
		return &testingexec.FakeCmd{
			CombinedOutputScript: []testingexec.FakeAction{
				func() ([]byte, []byte, error) { return []byte(""), nil, nil },
			},
		}
	})

	err := osUtils.extFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", common.Ext4FsType)
	if err != nil {
		t.Fatalf("extFormatAndMount returned unexpected error: %v", err)
	}
	if mkfsCalled {
		t.Error("expected mkfs.ext4 not to be invoked for an already-formatted disk")
	}
}

func TestGetDiskFormat(t *testing.T) {
	tests := []struct {
		name        string
		blkidOutput string
		blkidErr    error
		want        string
		wantErr     bool
	}{
		{
			name:     "unformatted disk (blkid exit code 2)",
			blkidErr: testingexec.FakeExitError{Status: 2},
			want:     "",
		},
		{
			name:        "already formatted disk",
			blkidOutput: "TYPE=ext4\n",
			want:        "ext4",
		},
		{
			name:        "disk with a partition table",
			blkidOutput: "PTTYPE=gpt\n",
			want:        "unknown data, probably partitions",
		},
		{
			name:        "TYPE and PTTYPE both present: partition table wins",
			blkidOutput: "TYPE=ext4\nPTTYPE=gpt\n",
			want:        "unknown data, probably partitions",
		},
		{
			name:     "blkid fails for a reason other than exit code 2",
			blkidErr: errors.New("blkid: command not found"),
			wantErr:  true,
		},
		{
			name:        "blkid returns malformed output",
			blkidOutput: "not-a-key-value-line\n",
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			osUtils := newFakeOsUtils(test.blkidOutput, test.blkidErr, nil)

			got, err := osUtils.getDiskFormat(context.Background(), "/dev/fake")
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("getDiskFormat returned unexpected error: %v", err)
			}
			if got != test.want {
				t.Errorf("expected fstype %q, got %q", test.want, got)
			}
		})
	}
}

func TestXfsFormatAndMount_Errors(t *testing.T) {
	t.Run("getDiskFormat failure is propagated", func(t *testing.T) {
		osUtils := newFakeOsUtils("", errors.New("blkid: command not found"), nil)

		err := osUtils.xfsFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", "xfs")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("mkfs failure is propagated", func(t *testing.T) {
		osUtils := newFakeOsUtils("", testingexec.FakeExitError{Status: 2},
			fakeMkfsFailure(errors.New("mkfs.xfs: exit status 1")))

		err := osUtils.xfsFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", "xfs")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("mount failure is propagated", func(t *testing.T) {
		var mkfsArgs []string
		commandScript := []testingexec.FakeCommandAction{
			func(cmd string, args ...string) utilexec.Cmd {
				return &testingexec.FakeCmd{
					CombinedOutputScript: []testingexec.FakeAction{
						func() ([]byte, []byte, error) { return []byte(""), nil, testingexec.FakeExitError{Status: 2} },
					},
				}
			},
			fakeMkfsSuccess(&mkfsArgs),
		}
		osUtils := &OsUtils{
			Mounter: &mount.SafeFormatAndMount{
				Interface: &erroringMounter{FakeMounter: mount.NewFakeMounter(nil), mountErr: errors.New("mount failed")},
				Exec:      &testingexec.FakeExec{CommandScript: commandScript},
			},
		}

		err := osUtils.xfsFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", "xfs")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

func TestExtFormatAndMount_Errors(t *testing.T) {
	t.Run("getDiskFormat failure is propagated", func(t *testing.T) {
		osUtils := newFakeOsUtils("", errors.New("blkid: command not found"), nil)

		err := osUtils.extFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", common.Ext4FsType)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("mkfs failure is propagated", func(t *testing.T) {
		osUtils := newFakeOsUtils("", testingexec.FakeExitError{Status: 2},
			fakeMkfsFailure(errors.New("mkfs.ext4: exit status 1")))

		err := osUtils.extFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", common.Ext4FsType)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("mount failure is propagated", func(t *testing.T) {
		var mkfsArgs []string
		commandScript := []testingexec.FakeCommandAction{
			func(cmd string, args ...string) utilexec.Cmd {
				return &testingexec.FakeCmd{
					CombinedOutputScript: []testingexec.FakeAction{
						func() ([]byte, []byte, error) { return []byte(""), nil, testingexec.FakeExitError{Status: 2} },
					},
				}
			},
			fakeMkfsSuccess(&mkfsArgs),
		}
		osUtils := &OsUtils{
			Mounter: &mount.SafeFormatAndMount{
				Interface: &erroringMounter{FakeMounter: mount.NewFakeMounter(nil), mountErr: errors.New("mount failed")},
				Exec:      &testingexec.FakeExec{CommandScript: commandScript},
			},
		}

		err := osUtils.extFormatAndMount(context.Background(), "/dev/fake", "/mnt/fake", common.Ext4FsType)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

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
