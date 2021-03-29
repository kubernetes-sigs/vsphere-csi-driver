package gofsutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	procMountsPath = "/proc/self/mountinfo"
	// procMountsRetries is number of times to retry for a consistent
	// read of procMountsPath.
	procMountsRetries = 3
)

var (
	bindRemountOpts = []string{"remount"}
)

// getDiskFormat uses 'lsblk' to see if the given disk is unformatted
func (fs *FS) getDiskFormat(ctx context.Context, disk string) (string, error) {

	args := []string{"-n", "-o", "FSTYPE", disk}

	f := log.Fields{
		"disk": disk,
	}
	log.WithFields(f).WithField("args", args).Info(
		"checking if disk is formatted using lsblk")
	buf, err := exec.Command("lsblk", args...).CombinedOutput()
	out := string(buf)
	log.WithField("output", out).Debug("lsblk output")

	if err != nil {
		log.WithFields(f).WithError(err).Error(
			"failed to determine if disk is formatted")
		return "", err
	}

	// Split lsblk output into lines. Unformatted devices should contain only
	// "\n". Beware of "\n\n", that's a device with one empty partition.
	out = strings.TrimSuffix(out, "\n") // Avoid last empty line
	lines := strings.Split(out, "\n")
	if lines[0] != "" {
		// The device is formatted
		return lines[0], nil
	}

	if len(lines) == 1 {
		// The device is unformatted and has no dependent devices
		return "", nil
	}

	// The device has dependent devices, most probably partitions (LVM, LUKS
	// and MD RAID are reported as FSTYPE and caught above).
	return "unknown data, probably partitions", nil
}

// formatAndMount uses unix utils to format and mount the given disk
func (fs *FS) formatAndMount(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) error {

	opts = append(opts, "defaults")
	f := log.Fields{
		"source":  source,
		"target":  target,
		"fsType":  fsType,
		"options": opts,
	}

	// Try to mount the disk
	log.WithFields(f).Info("attempting to mount disk")
	mountErr := fs.mount(ctx, source, target, fsType, opts...)
	if mountErr == nil {
		return nil
	}

	// Mount failed. This indicates either that the disk is unformatted or
	// it contains an unexpected filesystem.
	existingFormat, err := fs.getDiskFormat(ctx, source)
	if err != nil {
		return err
	}
	if existingFormat == "" {
		// Disk is unformatted so format it.
		args := []string{source}
		// Use 'ext4' as the default
		if len(fsType) == 0 {
			fsType = "ext4"
		}

		if fsType == "ext4" || fsType == "ext3" {
			args = []string{"-F", source}
		}
		f["fsType"] = fsType
		log.WithFields(f).Info(
			"disk appears unformatted, attempting format")

		mkfsCmd := fmt.Sprintf("mkfs.%s", fsType)
		if err := exec.Command(mkfsCmd, args...).Run(); err != nil {
			log.WithFields(f).WithError(err).Error(
				"format of disk failed")
		}

		// the disk has been formatted successfully try to mount it again.
		log.WithFields(f).Info(
			"disk successfully formatted")
		return fs.mount(ctx, source, target, fsType, opts...)
	}

	// Disk is already formatted and failed to mount
	if len(fsType) == 0 || fsType == existingFormat {
		// This is mount error
		return mountErr
	}

	// Block device is formatted with unexpected filesystem
	return fmt.Errorf(
		"failed to mount volume as %q; already contains %s: error: %v",
		fsType, existingFormat, mountErr)
}

// bindMount performs a bind mount
func (fs *FS) bindMount(
	ctx context.Context,
	source, target string,
	opts ...string) error {

	err := fs.doMount(ctx, "mount", source, target, "", "bind")
	if err != nil {
		return err
	}
	return fs.doMount(ctx, "mount", source, target, "", opts...)
}

// getMounts returns a slice of all the mounted filesystems
func (fs *FS) getMounts(ctx context.Context) ([]Info, error) {

	_, hash1, err := fs.readProcMounts(ctx, procMountsPath, false)
	if err != nil {
		return nil, err
	}

	for i := 0; i < procMountsRetries; i++ {
		mps, hash2, err := fs.readProcMounts(ctx, procMountsPath, true)
		if err != nil {
			return nil, err
		}
		if hash1 == hash2 {
			// Success
			return mps, nil
		}
		hash1 = hash2
	}
	return nil, fmt.Errorf(
		"failed to get a consistent snapshot of %v after %d tries",
		procMountsPath, procMountsRetries)
}

// readProcMounts reads procMountsInfo and produce a hash
// of the contents and a list of the mounts as Info objects.
func (fs *FS) readProcMounts(
	ctx context.Context,
	path string,
	info bool) ([]Info, uint32, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	return ReadProcMountsFrom(ctx, file, !info, ProcMountsFields, fs.ScanEntry)
}
