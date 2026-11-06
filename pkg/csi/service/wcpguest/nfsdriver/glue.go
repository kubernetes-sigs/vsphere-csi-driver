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

package nfsdriver

import (
	"strings"
	"sync"

	mount "k8s.io/mount-utils"
)

// This file is NOT part of the vendored upstream csi-driver-nfs source. It replaces the
// wiring that upstream's own Driver.Run (dropped, see doc.go) would otherwise have done, so
// that pkg/csi/service/wcpguest and pkg/csi/service/node.go can call this package's
// ControllerServer/NodeServer methods directly, in-process.

// workingMountDir is the scratch directory under which the root NFS export is temporarily
// mounted to create/remove a per-volume subdirectory. It never holds pod data.
const workingMountDir = "/tmp/nfsdriver"

// defaultMountPermissions is applied to every per-PVC subdirectory unless a StorageClass
// overrides it via the "mountPermissions" parameter (case-insensitive, upstream's own
// parameter — see controllerserver.go/nodeserver.go). Upstream defaults this to 0 (no chmod
// at all) because its own deployments commonly run as root with permissive/matching-UID
// exports. Our controller and node containers do not run as root (see
// manifests/guestcluster/*/pvcsi.yaml), and pods mounting these RWX volumes are frequently
// non-root too — fsGroup is not applied by kubelet to RWX/multi-writer CSI volumes — so
// without an explicit chmod here, a freshly created subdirectory ends up writable only by
// whichever UID the controller happened to create it as. 0777 matches what CreateVolume
// already requests from os.MkdirAll (before the process umask trims it), just applied via an
// explicit chmod that isn't subject to umask.
const defaultMountPermissions = 0777

// VolumeIDPrefix marks a CSI VolumeId as backed by this package rather than a Supervisor PVC
// or CNS, so callers can detect it and route accordingly before doing anything else.
const VolumeIDPrefix = "guestnfs:"

// IsGuestVolumeID reports whether id was minted by this package's CreateVolume (via the
// VolumeIDPrefix wrapping its caller applies — see pkg/csi/service/wcpguest.createGuestNFSVolume).
func IsGuestVolumeID(id string) bool {
	return strings.HasPrefix(id, VolumeIDPrefix)
}

var (
	globalDriver *Driver
	initOnce     sync.Once
)

// GetOrCreateDriver returns a process-wide Driver instance, wiring up its NodeServer the way
// upstream's Run() would (minus the gRPC bootstrap). Safe for concurrent use; the instance is
// created once per process, so the controller pod and each node pod each get their own.
func GetOrCreateDriver() *Driver {
	initOnce.Do(func() {
		d := NewDriver(&DriverOptions{
			DriverName:       DefaultDriverName,
			WorkingMountDir:  workingMountDir,
			MountPermissions: defaultMountPermissions,
		})
		// mount.New("") returns a plain mount.Interface; NodeUnpublishVolume already falls
		// back safely (via a comma-ok type assertion) when it isn't also a
		// mount.MounterForceUnmounter, so no unconditional cast is needed here.
		d.ns = NewNodeServer(d, mount.New(""))
		globalDriver = d
	})
	return globalDriver
}

// ControllerServer returns a ControllerServer bound to d, for direct in-process RPC calls.
func (n *Driver) ControllerServer() *ControllerServer {
	return NewControllerServer(n)
}

// NodeServer returns d's NodeServer, for direct in-process RPC calls.
func (n *Driver) NodeServer() *NodeServer {
	return n.ns
}
