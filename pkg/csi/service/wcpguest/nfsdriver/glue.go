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
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/validation"
	mount "k8s.io/mount-utils"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
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
// parameter — see controllerserver.go/nodeserver.go). It matches upstream's default of 0, which
// means no chmod at all: a new subdirectory keeps whatever mode and owner the controller created
// it with, so one workload cannot read or write another PVC's data just by being able to mount
// the export. Workloads that run as a different UID than the controller, and need the volume
// to be writable, opt in per StorageClass, e.g. mountPermissions: "0777" (see example/).
const defaultMountPermissions = 0

// VolumeIDPrefix marks a CSI volume or snapshot ID as backed by this package rather than a
// Supervisor PVC or CNS, so callers can detect it and route accordingly before doing anything
// else. It is added when an ID is minted (getVolumeIDFromNfsVol, getSnapshotIDFromNfsSnapshot)
// and removed when one is decoded (getNfsVolFromID, getNfsSnapFromID); nothing else adds or
// strips it, so callers pass IDs through unchanged in both directions.
const VolumeIDPrefix = common.GuestNFSVolumeIDPrefix

// IsGuestVolumeID reports whether id (a volume or snapshot ID) was minted by this package.
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

// createVolumeParameters are the StorageClass parameters CreateVolume understands, lower-cased.
// The csi.storage.k8s.io/* keys are the ones external-provisioner adds with
// --extra-create-metadata.
var createVolumeParameters = map[string]struct{}{
	paramServer:           {},
	paramShare:            {},
	paramSubDir:           {},
	paramOnDelete:         {},
	mountPermissionsField: {},
	pvcNamespaceKey:       {},
	pvcNameKey:            {},
	pvNameKey:             {},
}

// ValidateCreateVolumeParameters checks the StorageClass parameters of a request that selected
// this backend by setting "server", before anything is created. Because a stray "server" key on
// an otherwise ordinary StorageClass is enough to route here, it fails loudly instead of
// provisioning anything: every key must be one this backend understands, and the server must be
// a well-formed IP address or DNS name.
func ValidateCreateVolumeParameters(params map[string]string) error {
	var server string
	for k, v := range params {
		key := strings.ToLower(k)
		if _, ok := createVolumeParameters[key]; !ok {
			return fmt.Errorf("parameter %q is not supported for guest NFS volumes, which a %q parameter selects; "+
				"supported parameters are: %s", k, paramServer, strings.Join(sortedKeys(createVolumeParameters), ", "))
		}
		if key == paramServer {
			server = v
		}
	}
	return validateServer(server)
}

// validateServer accepts an IPv4/IPv6 address or a DNS name, and nothing that could be read
// as anything else (a path, a port, whitespace, an ID separator).
func validateServer(server string) error {
	if server == "" {
		return fmt.Errorf("%s is a required parameter", paramServer)
	}
	if net.ParseIP(server) != nil {
		return nil
	}
	if errs := validation.IsDNS1123Subdomain(strings.ToLower(server)); len(errs) > 0 {
		return fmt.Errorf("invalid %s %q: must be an IP address or a DNS name: %s", paramServer, server,
			strings.Join(errs, "; "))
	}
	return nil
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
