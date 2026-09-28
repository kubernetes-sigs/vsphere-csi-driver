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
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMnt3StatusString(t *testing.T) {
	tests := []struct {
		code uint32
		want string
	}{
		{0, "MNT3_OK"},
		{13, "MNT3ERR_ACCES: permission denied (export access control rejected this client)"},
		{999999, "unknown MNT3 status 999999"},
	}
	for _, tc := range tests {
		if got := mnt3StatusString(tc.code); got != tc.want {
			t.Errorf("mnt3StatusString(%d) = %q, want %q", tc.code, got, tc.want)
		}
	}
}

// fakePortmapper starts a fake portmapper server that responds to any GETPORT call by
// reporting mountdAddr's port, and returns the portmapper's own address.
func fakePortmapper(t *testing.T, mountdAddr string) string {
	t.Helper()
	_, mountdPortStr, err := net.SplitHostPort(mountdAddr)
	if err != nil {
		t.Fatalf("failed to split mountd address %q: %v", mountdAddr, err)
	}
	mountdPort, err := strconv.Atoi(mountdPortStr)
	if err != nil {
		t.Fatalf("failed to parse mountd port %q: %v", mountdPortStr, err)
	}

	result := &xdrWriter{}
	result.uint32(uint32(mountdPort))
	return fakeRPCServer(t, acceptSuccess, result.buf)
}

// fakeMountd starts a fake mountd server that responds to any MNT call with the given
// MNT3 status code, and returns its address.
func fakeMountd(t *testing.T, status uint32) string {
	t.Helper()
	result := &xdrWriter{}
	result.uint32(status)
	// On success a real server would also send a file handle and auth flavors list,
	// but CheckMountable only reads the status field before returning, so a bare
	// status is enough to exercise both the success and error paths.
	return fakeRPCServer(t, acceptSuccess, result.buf)
}

func hostOf(t *testing.T, addr string) string {
	t.Helper()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("failed to split address %q: %v", addr, err)
	}
	return host
}

func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("failed to split address %q: %v", addr, err)
	}
	return port
}

func TestCheckMountableSuccess(t *testing.T) {
	mountdAddr := fakeMountd(t, 0 /* MNT3_OK */)
	portmapperAddr := fakePortmapper(t, mountdAddr)

	err := checkMountableAt(hostOf(t, portmapperAddr), portOf(t, portmapperAddr), "/store", time.Second)
	if err != nil {
		t.Errorf("expected CheckMountable to succeed, got: %v", err)
	}
}

func TestCheckMountableAccessDenied(t *testing.T) {
	mountdAddr := fakeMountd(t, 13 /* MNT3ERR_ACCES */)
	portmapperAddr := fakePortmapper(t, mountdAddr)

	err := checkMountableAt(hostOf(t, portmapperAddr), portOf(t, portmapperAddr), "/store", time.Second)
	if err == nil {
		t.Fatal("expected CheckMountable to fail for MNT3ERR_ACCES, got nil")
	}
	if !strings.Contains(err.Error(), "MNT3ERR_ACCES") {
		t.Errorf("expected error to mention MNT3ERR_ACCES, got: %v", err)
	}
}

func TestCheckMountableNoSuchExport(t *testing.T) {
	mountdAddr := fakeMountd(t, 2 /* MNT3ERR_NOENT */)
	portmapperAddr := fakePortmapper(t, mountdAddr)

	err := checkMountableAt(hostOf(t, portmapperAddr), portOf(t, portmapperAddr), "/does-not-exist", time.Second)
	if err == nil {
		t.Fatal("expected CheckMountable to fail for MNT3ERR_NOENT, got nil")
	}
	if !strings.Contains(err.Error(), "MNT3ERR_NOENT") {
		t.Errorf("expected error to mention MNT3ERR_NOENT, got: %v", err)
	}
}

func TestGetMountdPortNotRegistered(t *testing.T) {
	// Portmapper replies successfully but with port 0, meaning "not registered".
	result := &xdrWriter{}
	result.uint32(0)
	portmapperAddr := fakeRPCServer(t, acceptSuccess, result.buf)

	_, err := getMountdPortAt(hostOf(t, portmapperAddr), portOf(t, portmapperAddr), time.Second)
	if err == nil {
		t.Fatal("expected error for unregistered MOUNT service, got nil")
	}
	if !strings.Contains(err.Error(), "does not have the NFSv3 MOUNT service") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestCheckMountablePortmapperUnreachable(t *testing.T) {
	// Nothing listens on this port.
	err := checkMountableAt("127.0.0.1", "1", "/store", 200*time.Millisecond)
	if err == nil {
		t.Fatal("expected error when portmapper is unreachable, got nil")
	}
	if !strings.Contains(err.Error(), "portmapper") {
		t.Errorf("expected error to mention portmapper, got: %v", err)
	}
}
