// +build linux darwin

package gofsutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
)

// mount mounts source to target as fsType with given options.
//
// The parameters 'source' and 'fsType' must be empty strings in case they
// are not required, e.g. for remount, or for an auto filesystem type where
// the kernel handles fsType automatically.
//
// The 'options' parameter is a list of options. Please see mount(8) for
// more information. If no options are required then please invoke Mount
// with an empty or nil argument.
func (fs *FS) mount(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) error {

	// All Linux distributes should support bind mounts.
	if opts, ok := fs.isBind(ctx, opts...); ok {
		return fs.bindMount(ctx, source, target, opts...)
	}
	return fs.doMount(ctx, "mount", source, target, fsType, opts...)
}

// doMount runs the mount command.
func (fs *FS) doMount(
	ctx context.Context,
	mntCmd, source, target, fsType string,
	opts ...string) error {

	mountArgs := MakeMountArgs(ctx, source, target, fsType, opts...)
	args := strings.Join(mountArgs, " ")

	f := log.Fields{
		"cmd":  mntCmd,
		"args": args,
	}
	log.WithFields(f).Info("mount command")

	buf, err := exec.Command(mntCmd, mountArgs...).CombinedOutput()
	if err != nil {
		out := string(buf)
		log.WithFields(f).WithField("output", out).WithError(
			err).Error("mount Failed")
		return fmt.Errorf(
			"mount failed: %v\nmounting arguments: %s\noutput: %s",
			err, args, out)
	}
	return nil
}

// unmount unmounts the target.
func (fs *FS) unmount(ctx context.Context, target string) error {
	f := log.Fields{
		"path": target,
		"cmd":  "umount",
	}
	log.WithFields(f).Info("unmount command")
	buf, err := exec.Command("umount", target).CombinedOutput()
	if err != nil {
		out := string(buf)
		f["output"] = out
		log.WithFields(f).WithError(err).Error("unmount failed")
		return fmt.Errorf(
			"unmount failed: %v\nunmounting arguments: %s\nOutput: %s",
			err, target, out)
	}
	return nil
}

// isBind detects whether a bind mount is being requested and determines
// which remount options are needed. A secondary mount operation is
// required for bind mounts as the initial operation does not apply the
// request mount options.
//
// The returned options will be "bind", "remount", and the provided
// list of options.
func (fs *FS) isBind(ctx context.Context, opts ...string) ([]string, bool) {
	bind := false
	remountOpts := append([]string(nil), bindRemountOpts...)

	for _, o := range opts {
		switch o {
		case "bind":
			bind = true
			break
		case "remount":
			break
		default:
			remountOpts = append(remountOpts, o)
		}
	}

	return remountOpts, bind
}

// getDevMounts returns a slice of all mounts for dev
func (fs *FS) getDevMounts(ctx context.Context, dev string) ([]Info, error) {

	allMnts, err := fs.getMounts(ctx)
	if err != nil {
		return nil, err
	}

	var mountInfos []Info
	for _, m := range allMnts {
		if m.Device == dev {
			mountInfos = append(mountInfos, m)
		}
	}

	return mountInfos, nil
}

func (fs *FS) validateDevice(
	ctx context.Context, source string) (string, error) {

	if _, err := os.Lstat(source); err != nil {
		return "", err
	}

	// Eval symlinks to ensure the specified path points to a real device.
	if err := EvalSymlinks(ctx, &source); err != nil {
		return "", err
	}

	st, err := os.Stat(source)
	if err != nil {
		return "", err
	}

	if st.Mode()&os.ModeDevice == 0 {
		return "", fmt.Errorf("invalid device: %s", source)
	}

	return source, nil
}
