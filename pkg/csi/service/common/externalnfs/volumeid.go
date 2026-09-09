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

package externalnfs

import (
	"fmt"
	"strings"
)

const (
	// volumeIDPrefix marks a CSI VolumeId as belonging to this backend, so controller RPCs can
	// detect it and skip CNS/vCenter lookups entirely, before doing anything else.
	volumeIDPrefix    = "nfsext"
	volumeIDSeparator = "#"
	volumeIDFields    = 5 // prefix, server, share, subDir, uuid
)

// VolumeID identifies a subdirectory-backed volume on an external NFS export.
type VolumeID struct {
	Server string
	Share  string
	SubDir string
	UUID   string
}

// IsExternalNFSVolumeID reports whether id was minted by this backend's CreateVolume.
func IsExternalNFSVolumeID(id string) bool {
	return strings.HasPrefix(id, volumeIDPrefix+volumeIDSeparator)
}

// Encode returns the CSI VolumeId string for v: "nfsext#server#share#subDir#uuid".
func (v VolumeID) Encode() string {
	return strings.Join([]string{volumeIDPrefix, v.Server, v.Share, v.SubDir, v.UUID}, volumeIDSeparator)
}

// DecodeVolumeID parses a VolumeId string produced by VolumeID.Encode.
func DecodeVolumeID(id string) (VolumeID, error) {
	parts := strings.Split(id, volumeIDSeparator)
	if len(parts) != volumeIDFields || parts[0] != volumeIDPrefix {
		return VolumeID{}, fmt.Errorf("invalid external nfs volume id: %q", id)
	}
	return VolumeID{Server: parts[1], Share: parts[2], SubDir: parts[3], UUID: parts[4]}, nil
}

// MountSource returns the "server:/share/subDir" string used both as the node-side mount
// source and as the value stashed in PublishContext[common.Nfsv4AccessPoint].
func (v VolumeID) MountSource() string {
	return v.Server + ":" + JoinExportPath(v.Share, v.SubDir)
}

// RootMountSource returns the "server:/share" string for the root export, used to mount the
// export temporarily in order to create/remove the per-volume subdirectory.
func (v VolumeID) RootMountSource() string {
	return v.Server + ":" + v.Share
}

// JoinExportPath joins an NFS export path and a subdirectory name, normalizing slashes.
func JoinExportPath(share, subDir string) string {
	share = strings.TrimSuffix(share, "/")
	if subDir == "" {
		return share
	}
	return share + "/" + strings.TrimPrefix(subDir, "/")
}
