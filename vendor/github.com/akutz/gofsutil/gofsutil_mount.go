package gofsutil

import (
	"bufio"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"path"
	"regexp"
	"strings"
)

// ProcMountsFields is fields per line in procMountsPath as per
// https://www.kernel.org/doc/Documentation/filesystems/proc.txt
const ProcMountsFields = 9

// Info describes a mounted filesystem.
//
// Please note that all fields that represent filesystem paths must
// be absolute and not contain any symlinks.
type Info struct {
	// Device is the filesystem path of the device to which the filesystem is
	// mounted.
	Device string

	// Path is the filesystem path to which Device is mounted.
	Path string

	// Source may be set to one of two values:
	//
	//   1. If this is a bind mount created with "bindfs" then Source
	//      is set to the filesystem path (absolute, no symlinks)
	//      bind mounted to Path.
	//
	//   2. If this is any other type of mount then Source is set to
	//      a concatenation of the mount source and the root of
	//      the mount within the file system (fields 10 & 4 from
	//      the section on /proc/<pid>/mountinfo at
	//      https://www.kernel.org/doc/Documentation/filesystems/proc.txt).
	//
	// It is not possible to diffentiate a native bind mount from a
	// non-bind mount after the native bind mount has been created.
	// Therefore, while the Source field will be set to the filesystem
	// path bind mounted to Path for native bind mounts, the value of
	// the Source field can in no way be used to determine *if* a mount
	// is a bind mount.
	Source string

	// Type is the filesystem type.
	Type string

	// Opts are the mount options (https://linux.die.net/man/8/mount)
	// used to mount the filesystem.
	Opts []string
}

// Entry is a superset of Info and maps to the fields of a mount table
// entry:
//
//   (1) mount ID:  unique identifier of the mount (may be reused after umount)
//   (2) parent ID:  ID of parent (or of self for the top of the mount tree)
//   (3) major:minor:  value of st_dev for files on filesystem
//   (4) root:  root of the mount within the filesystem
//   (5) mount point:  mount point relative to the process's root
//   (6) mount options:  per mount options
//   (7) optional fields:  zero or more fields of the form "tag[:value]"
//   (8) separator:  marks the end of the optional fields
//   (9) filesystem type:  name of filesystem of the form "type[.subtype]"
//   (10) mount source:  filesystem specific information or "none"
//   (11) super options:  per super block options
type Entry struct {
	// Root of the mount within the filesystem.
	Root string

	// MountPoint relative to the process's root
	MountPoint string

	// MountOpts are per-mount options.
	MountOpts []string

	// FSType is the name of filesystem of the form "type[.subtype]".
	FSType string

	// MountSource is filesystem specific information or "none"
	MountSource string
}

// EntryScanFunc defines the signature of the function that is optionally
// provided to the functions in this package that scan the mount table.
// The mount entry table is ignored when this function returns a false
// value or error.
type EntryScanFunc func(
	ctx context.Context,
	entry Entry,
	cache map[string]Entry) (Info, bool, error)

// DefaultEntryScanFunc returns the default entry scan function.
func DefaultEntryScanFunc() EntryScanFunc {
	return defaultEntryScanFunc
}

func defaultEntryScanFunc(
	ctx context.Context,
	entry Entry,
	cache map[string]Entry) (info Info, valid bool, failed error) {

	// Validate the mount table entry.
	validFSType, _ := regexp.MatchString(
		`(?i)^devtmpfs|(?:fuse\..*)|(?:nfs\d?)$`, entry.FSType)
	sourceHasSlashPrefix := strings.HasPrefix(entry.MountSource, "/")
	if valid = validFSType || sourceHasSlashPrefix; !valid {
		return
	}

	// Copy the Entry object's fields to the Info object.
	info.Device = entry.MountSource
	info.Opts = make([]string, len(entry.MountOpts))
	copy(info.Opts, entry.MountOpts)
	info.Path = entry.MountPoint
	info.Type = entry.FSType
	info.Source = entry.MountSource

	// If this is the first time a source is encountered in the
	// output then cache its mountPoint field as the filesystem path
	// to which the source is mounted as a non-bind mount.
	//
	// Subsequent encounters with the source will resolve it
	// to the cached root value in order to set the mount info's
	// Source field to the the cached mountPont field value + the
	// value of the current line's root field.
	if cachedEntry, ok := cache[entry.MountSource]; ok {
		info.Source = path.Join(cachedEntry.MountPoint, entry.Root)
	} else {
		cache[entry.MountSource] = entry
	}

	return
}

/*
ReadProcMountsFrom parses the contents of a mount table file, typically
"/proc/self/mountinfo".

From https://www.kernel.org/doc/Documentation/filesystems/proc.txt:

3.5	/proc/<pid>/mountinfo - Information about mounts
--------------------------------------------------------

This file contains lines of the form:

36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
(1)(2)(3)   (4)   (5)      (6)      (7)   (8) (9)   (10)         (11)

(1) mount ID:  unique identifier of the mount (may be reused after umount)
(2) parent ID:  ID of parent (or of self for the top of the mount tree)
(3) major:minor:  value of st_dev for files on filesystem
(4) root:  root of the mount within the filesystem
(5) mount point:  mount point relative to the process's root
(6) mount options:  per mount options
(7) optional fields:  zero or more fields of the form "tag[:value]"
(8) separator:  marks the end of the optional fields
(9) filesystem type:  name of filesystem of the form "type[.subtype]"
(10) mount source:  filesystem specific information or "none"
(11) super options:  per super block options

Parsers should ignore all unrecognised optional fields.  Currently the
possible optional fields are:

shared:X  mount is shared in peer group X
master:X  mount is slave to peer group X
propagate_from:X  mount is slave and receives propagation from peer group X (*)
unbindable  mount is unbindable
*/
func ReadProcMountsFrom(
	ctx context.Context,
	file io.Reader,
	quick bool,
	expectedFields int,
	scanEntry EntryScanFunc) ([]Info, uint32, error) {

	if scanEntry == nil {
		scanEntry = defaultEntryScanFunc
	}

	var (
		infos []Info
		hash  = fnv.New32a()
		fscan = bufio.NewScanner(file)
		cache = map[string]Entry{}
	)

	for fscan.Scan() {

		// Read the next line of text and attempt to parse it into
		// distinct, space-separated fields.
		line := fscan.Text()
		fields := strings.Fields(line)

		// Remove the optional fields that should be ignored.
		for {
			val := fields[6]
			fields = append(fields[:6], fields[7:]...)
			if val == "-" {
				break
			}
		}

		if len(fields) != expectedFields {
			return nil, 0, fmt.Errorf(
				"readProcMountsFrom: invalid field count: exp=%d, act=%d: %s",
				expectedFields, len(fields), line)
		}

		// Create a new Entry object from the mount table entry.
		e := Entry{
			Root:        fields[3],
			MountPoint:  fields[4],
			MountOpts:   strings.Split(fields[5], ","),
			FSType:      fields[6],
			MountSource: fields[7],
		}

		// If the ScanFunc indicates the mount table entry is invalid
		// then do not consider it for the checksum and continue to the
		// next mount table entry.
		i, valid, err := scanEntry(ctx, e, cache)
		if err != nil {
			return nil, 0, err
		}
		if !valid {
			continue
		}

		fmt.Fprint(hash, line)
		infos = append(infos, i)
	}

	return infos, hash.Sum32(), nil
}

// MakeMountArgs makes the arguments to the mount(8) command.
//
// The argument list returned is built as follows:
//
//         mount [-t $fsType] [-o $options] [$source] $target
func MakeMountArgs(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) []string {

	args := []string{}
	if len(fsType) > 0 {
		args = append(args, "-t", fsType)
	}
	if len(opts) > 0 {
		// Remove any duplicates or empty options from the provided list and
		// check the length of the list once more in case the list is now empty
		// once empty options were removed.
		if opts = RemoveDuplicates(opts); len(opts) > 0 {
			args = append(args, "-o", strings.Join(opts, ","))
		}
	}
	if len(source) > 0 {
		args = append(args, source)
	}
	args = append(args, target)

	return args
}
