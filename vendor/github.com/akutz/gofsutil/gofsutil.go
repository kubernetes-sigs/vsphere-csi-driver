package gofsutil

import (
	"context"
	"errors"
	"path/filepath"
)

var (
	// ErrNotImplemented is returned when a platform does not implement
	// the contextual function.
	ErrNotImplemented = errors.New("not implemented")

	// fs is the default FS instance.
	fs = &FS{ScanEntry: defaultEntryScanFunc}
)

// GetDiskFormat uses 'lsblk' to see if the given disk is unformatted.
func GetDiskFormat(ctx context.Context, disk string) (string, error) {
	return fs.GetDiskFormat(ctx, disk)
}

// FormatAndMount uses unix utils to format and mount the given disk.
func FormatAndMount(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) error {

	return fs.FormatAndMount(ctx, source, target, fsType, opts...)
}

// Mount mounts source to target as fstype with given options.
//
// The parameters 'source' and 'fstype' must be empty strings in case they
// are not required, e.g. for remount, or for an auto filesystem type where
// the kernel handles fstype automatically.
//
// The 'options' parameter is a list of options. Please see mount(8) for
// more information. If no options are required then please invoke Mount
// with an empty or nil argument.
func Mount(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) error {

	return fs.Mount(ctx, source, target, fsType, opts...)
}

// BindMount behaves like Mount was called with a "bind" flag set
// in the options list.
func BindMount(
	ctx context.Context,
	source, target string,
	opts ...string) error {

	return fs.BindMount(ctx, source, target, opts...)
}

// Unmount unmounts the target.
func Unmount(ctx context.Context, target string) error {
	return fs.Unmount(ctx, target)
}

// GetMounts returns a slice of all the mounted filesystems.
//
// * Linux hosts use mount_namespaces to obtain mount information.
//
//   Support for mount_namespaces was introduced to the Linux kernel
//   in 2.2.26 (http://man7.org/linux/man-pages/man5/proc.5.html) on
//   2004/02/04.
//
//   The kernel documents the contents of "/proc/<pid>/mountinfo" at
//   https://www.kernel.org/doc/Documentation/filesystems/proc.txt.
//
// * Darwin hosts parse the output of the "mount" command to obtain
//   mount information.
func GetMounts(ctx context.Context) ([]Info, error) {
	return fs.GetMounts(ctx)
}

// GetDevMounts returns a slice of all mounts for the provided device.
func GetDevMounts(ctx context.Context, dev string) ([]Info, error) {
	return fs.GetDevMounts(ctx, dev)
}

// EvalSymlinks evaluates the provided path and updates it to remove
// any symlinks in its structure, replacing them with the actual path
// components.
func EvalSymlinks(ctx context.Context, symPath *string) error {
	realPath, err := filepath.EvalSymlinks(*symPath)
	if err != nil {
		return err
	}
	*symPath = realPath
	return nil
}

// ValidateDevice evalutes the specified path and determines whether
// or not it is a valid device. If true then the provided path is
// evaluated and returned as an absolute path without any symlinks.
// Otherwise an empty string is returned.
func ValidateDevice(ctx context.Context, source string) (string, error) {
	return fs.ValidateDevice(ctx, source)
}
