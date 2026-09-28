/*
Copyright 2026 The Kubernetes Authors.

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

package nfsmount

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

const (
	// portmapper (RFC 1057 Appendix A / RFC 1833) program/version/procedure numbers,
	// and its well-known port.
	portmapperProg        = 100000
	portmapperVers        = 2
	portmapperProcGetPort = 3
	portmapperPort        = "111"

	// MOUNT protocol (RFC 1813 Appendix I) program/version/procedure numbers. Only
	// version 3 is checked here, matching the nfsvers the driver actually mounts with
	// on this testbed (see pkg/csi/service/wcpguest/nfsdriver) - v3 has no dependency
	// on portmapper's own version, it's the MOUNT program version being requested.
	mountProg    = 100005
	mountVers    = 3
	mountProcMnt = 1

	// IPPROTO_TCP, passed to portmapper's GETPORT to ask specifically for the TCP
	// port mountd is listening on (the driver always mounts over TCP).
	protoTCP = 6
)

// mnt3Status maps NFSv3 MOUNT protocol status codes (RFC 1813 Appendix I) to
// human-readable descriptions, so CheckMountable's errors are actionable. Notably
// includes MNT3ErrAcces (13) - exactly the export-ACL-rejection failure mode this
// package exists to detect ahead of an actual pod mount attempt.
var mnt3Status = map[uint32]string{
	0:     "MNT3_OK",
	1:     "MNT3ERR_PERM: not owner",
	2:     "MNT3ERR_NOENT: no such file or directory",
	5:     "MNT3ERR_IO: I/O error",
	13:    "MNT3ERR_ACCES: permission denied (export access control rejected this client)",
	20:    "MNT3ERR_NOTDIR: not a directory",
	22:    "MNT3ERR_INVAL: invalid argument",
	63:    "MNT3ERR_NAMETOOLONG: filename too long",
	10004: "MNT3ERR_NOTSUPP: operation not supported",
	10006: "MNT3ERR_SERVERFAULT: server fault",
}

func mnt3StatusString(code uint32) string {
	if s, ok := mnt3Status[code]; ok {
		return s
	}
	return fmt.Sprintf("unknown MNT3 status %d", code)
}

// getMountdPort asks server's portmapper (port 111) which TCP port its mountd (MOUNT
// program 100005, version 3) is currently listening on - mountd's port is not fixed,
// so this lookup is required before the actual MNT call.
func getMountdPort(server string, timeout time.Duration) (int, error) {
	return getMountdPortAt(server, portmapperPort, timeout)
}

// getMountdPortAt is getMountdPort with the portmapper's own port overridable, so
// tests can point it at a local fake portmapper without binding the real (privileged)
// port 111.
func getMountdPortAt(server, portmapperAddr string, timeout time.Duration) (int, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(server, portmapperAddr), timeout)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to portmapper on %s:%s: %w", server, portmapperAddr, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return 0, fmt.Errorf("failed to set deadline on portmapper connection: %w", err)
	}

	w := &xdrWriter{}
	w.uint32(mountProg)
	w.uint32(mountVers)
	w.uint32(protoTCP)
	w.uint32(0) // port; ignored by the server in a GETPORT request

	result, err := rpcCall(conn, 1, portmapperProg, portmapperVers, portmapperProcGetPort, w.buf)
	if err != nil {
		return 0, fmt.Errorf("portmapper GETPORT call to %s failed: %w", server, err)
	}
	r := &xdrReader{buf: result}
	port, err := r.uint32()
	if err != nil {
		return 0, fmt.Errorf("failed to read portmapper GETPORT result from %s: %w", server, err)
	}
	if port == 0 {
		return 0, fmt.Errorf("server %s does not have the NFSv3 MOUNT service (program %d, version %d) "+
			"registered with portmapper", server, mountProg, mountVers)
	}
	return int(port), nil
}

// CheckMountable reports whether mounting server:dirpath over NFSv3 would currently
// succeed, by performing the real MOUNT protocol MNT RPC call (RFC 1813) against the
// server's mountd - the same permission/export-ACL check the server itself performs
// for a real mount - without an actual mount(2) syscall (which needs CAP_SYS_ADMIN,
// unavailable to the unprivileged syncer container this runs in) or a client-side NFS
// library dependency (only this one procedure is needed, not file I/O).
//
// dirpath is the server-local export path exactly as configured on the NFS server
// (e.g. "/store/pvc-1234"), not including the server hostname.
//
// Returns nil if the server accepts the mount (MNT3_OK). Any other outcome - the
// server or portmapper unreachable, or an MNT3 error status such as MNT3ERR_ACCES for
// an export-ACL rejection - is returned as a descriptive error.
func CheckMountable(server, dirpath string, timeout time.Duration) error {
	return checkMountableAt(server, portmapperPort, dirpath, timeout)
}

// checkMountableAt is CheckMountable with the portmapper's own port overridable, so
// tests can point it at a local fake portmapper without binding the real (privileged)
// port 111.
func checkMountableAt(server, portmapperAddr, dirpath string, timeout time.Duration) error {
	mountdPort, err := getMountdPortAt(server, portmapperAddr, timeout)
	if err != nil {
		return err
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(server, strconv.Itoa(mountdPort)), timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to mountd on %s:%d: %w", server, mountdPort, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("failed to set deadline on mountd connection: %w", err)
	}

	w := &xdrWriter{}
	w.xdrString(dirpath)

	result, err := rpcCall(conn, 2, mountProg, mountVers, mountProcMnt, w.buf)
	if err != nil {
		return fmt.Errorf("MOUNT MNT call to %s:%s failed: %w", server, dirpath, err)
	}
	r := &xdrReader{buf: result}
	status, err := r.uint32()
	if err != nil {
		return fmt.Errorf("failed to read MNT result status from %s:%s: %w", server, dirpath, err)
	}
	if status != 0 {
		return fmt.Errorf("mount of %s:%s rejected by server: %s", server, dirpath, mnt3StatusString(status))
	}
	return nil
}
