package gofsutil

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

var (
	bindRemountOpts = []string{}
	mountRX         = regexp.MustCompile(`^(.+) on (.+) \((.+)\)$`)
)

// getDiskFormat uses 'lsblk' to see if the given disk is unformated
func (fs *FS) getDiskFormat(ctx context.Context, disk string) (string, error) {

	mps, err := fs.getMounts(ctx)
	if err != nil {
		return "", err
	}
	for _, i := range mps {
		if i.Device == disk {
			return i.Type, nil
		}
	}
	return "", fmt.Errorf("getDiskFormat: failed: %s", disk)
}

// formatAndMount uses unix utils to format and mount the given disk
func (fs *FS) formatAndMount(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) error {

	return ErrNotImplemented
}

// getMounts returns a slice of all the mounted filesystems
func (fs *FS) getMounts(ctx context.Context) ([]Info, error) {

	out, err := exec.Command("mount").CombinedOutput()
	if err != nil {
		return nil, err
	}

	var mountInfos []Info
	scan := bufio.NewScanner(bytes.NewReader(out))

	for scan.Scan() {
		m := mountRX.FindStringSubmatch(scan.Text())
		if len(m) != 4 {
			continue
		}
		device := m[1]
		if !strings.HasPrefix(device, "/") {
			continue
		}
		var (
			path    = m[2]
			source  = device
			options = strings.Split(m[3], ",")
		)
		if len(options) == 0 {
			return nil, fmt.Errorf(
				"getMounts: invalid mount options: %s", device)
		}
		for i, v := range options {
			options[i] = strings.TrimSpace(v)
		}
		fsType := options[0]
		if len(options) > 1 {
			options = options[1:]
		} else {
			options = nil
		}
		mountInfos = append(mountInfos, Info{
			Device: device,
			Path:   path,
			Source: source,
			Type:   fsType,
			Opts:   options,
		})
	}
	return mountInfos, nil
}

// bindMount performs a bind mount
func (fs *FS) bindMount(
	ctx context.Context,
	source, target string, opts ...string) error {

	return fs.doMount(ctx, "bindfs", source, target, "", opts...)
}
