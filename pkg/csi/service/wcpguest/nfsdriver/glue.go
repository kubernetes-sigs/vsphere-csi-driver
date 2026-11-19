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
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"
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

// GetSnapshot returns the snapshot with the given ID, or nil if its archive does not exist or the
// ID is not one this package mints. Upstream has no ListSnapshots; pvCSI advertises it, so the
// external-snapshotter decides whether a pre-provisioned snapshot is ready by listing it by ID.
// The source volume is not recorded with the archive, so SourceVolumeId is left empty.
func (cs *ControllerServer) GetSnapshot(ctx context.Context, snapshotID string, secrets map[string]string) (
	*csi.Snapshot, error) {
	snap, err := getNfsSnapFromID(snapshotID)
	if err != nil {
		klog.V(4).Infof("GetSnapshot: %q is not a snapshot ID: %v", snapshotID, err)
		return nil, nil
	}
	vol := volumeFromSnapshot(snap)
	if err := cs.internalMount(ctx, vol, nil, getVolumeCapabilityFromSecret(snapshotID, secrets)); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount nfs server to look up snapshot: %v", err)
	}
	defer func() {
		if err := cs.internalUnmount(ctx, vol); err != nil {
			klog.Warningf("failed to unmount nfs server after looking up snapshot: %v", err)
		}
	}()
	snapDir := getInternalVolumePath(cs.Driver.workingMountDir, vol)
	for _, compressed := range []bool{true, false} {
		fi, err := os.Stat(filepath.Join(snapDir, snap.archiveName(compressed)))
		if err == nil {
			return &csi.Snapshot{
				SnapshotId:   snapshotID,
				SizeBytes:    fi.Size(),
				CreationTime: timestamppb.New(fi.ModTime()),
				ReadyToUse:   true,
			}, nil
		}
		if !os.IsNotExist(err) {
			return nil, status.Errorf(codes.Internal, "failed to look up snapshot archive: %v", err)
		}
	}
	return nil, nil
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

// NodePublishGuestVolume is NodePublishVolume as kubelet calls it for a guest NFS volume. The NFS
// location is checked here, before anything is mounted, because a statically provisioned
// PersistentVolume carries whatever volumeAttributes an administrator wrote, which no StorageClass
// validation has seen. A PersistentVolume that sets no location at all is mounted from the
// location in its volume handle; see publishVolumeContext.
func (ns *NodeServer) NodePublishGuestVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (
	*csi.NodePublishVolumeResponse, error) {
	volumeContext, err := publishVolumeContext(req.GetVolumeId(), req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot mount guest NFS volume %q: %v",
			req.GetVolumeId(), err)
	}
	req = proto.Clone(req).(*csi.NodePublishVolumeRequest)
	req.VolumeContext = volumeContext
	return ns.NodePublishVolume(ctx, req)
}

// publishVolumeContext returns the volume context to mount volumeID with, after checking the NFS
// location in it the same way a StorageClass's is checked (ValidateCreateVolumeParameters,
// newNFSVolume).
//
// A dynamically provisioned volume's context always names its server, because CreateVolume
// returns the StorageClass parameters as the context. One that names no location at all (no
// server, share or subdir) is a statically provisioned volume without volumeAttributes, and is
// mounted from the location in its volume handle, which must then be in the form CreateVolume
// mints.
func publishVolumeContext(volumeID string, volumeContext map[string]string) (map[string]string, error) {
	var server, share, subDir string
	var ignored []string
	for k, v := range volumeContext {
		switch key := strings.ToLower(k); key {
		case paramServer:
			server = v
		case paramShare:
			share = v
		case paramSubDir:
			subDir = v
		default:
			if !isKnownVolumeContextKey(key) {
				ignored = append(ignored, k)
			}
		}
	}
	if len(ignored) > 0 {
		sort.Strings(ignored)
		klog.Warningf("guest NFS volume %q: ignoring unknown volume attributes %v", volumeID, ignored)
	}

	if server == "" && share == "" && subDir == "" {
		vol, err := getNfsVolFromID(volumeID)
		if err == nil && !IsGuestVolumeID(volumeID) {
			err = fmt.Errorf("volume ID %q does not start with %q", volumeID, VolumeIDPrefix)
		}
		if err != nil {
			return nil, fmt.Errorf("no NFS location: the PersistentVolume sets none of the volumeAttributes "+
				"%q, %q and %q, and its volume handle does not name one either (%v)",
				paramServer, paramShare, paramSubDir, err)
		}
		server, share, subDir = vol.server, "/"+vol.baseDir, vol.subDir
		derived := make(map[string]string, len(volumeContext)+3)
		for k, v := range volumeContext {
			// An empty "Server" left next to the derived "server" would be read in map order.
			switch strings.ToLower(k) {
			case paramServer, paramShare, paramSubDir:
				continue
			}
			derived[k] = v
		}
		derived[paramServer], derived[paramShare], derived[paramSubDir] = server, share, subDir
		volumeContext = derived
	}

	if err := validateServer(server); err != nil {
		return nil, err
	}
	if share == "" {
		return nil, fmt.Errorf("%s is a required parameter", paramShare)
	}
	for _, p := range []struct{ name, value string }{{paramShare, share}, {paramSubDir, subDir}} {
		if err := validatePath(p.value); err != nil {
			return nil, fmt.Errorf("invalid %s %q: %v", p.name, p.value, err)
		}
		if err := validateIDComponent(p.name, p.value); err != nil {
			return nil, err
		}
	}
	return volumeContext, nil
}

// isKnownVolumeContextKey reports whether a lower-cased volume context key is one NodePublishVolume
// reads, one CreateVolume passes through from the StorageClass, or one Kubernetes adds itself.
func isKnownVolumeContextKey(key string) bool {
	switch key {
	case paramOnDelete, mountOptionsField, mountPermissionsField:
		return true
	}
	return strings.HasPrefix(key, "csi.storage.k8s.io/") || strings.HasPrefix(key, "storage.kubernetes.io/")
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
