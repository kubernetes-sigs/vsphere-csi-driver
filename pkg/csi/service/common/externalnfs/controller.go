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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/akutz/gofsutil"
	"github.com/google/uuid"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// workingMountDir is the scratch directory under which the root NFS export is temporarily
// mounted to create/remove a per-volume subdirectory. It never holds pod data.
const workingMountDir = "/tmp/nfsext-scratch"

// CreateVolume creates a subdirectory named subDir (params[ParamSubDir], defaulting to
// volumeName) under the root export params[ParamServer]:params[ParamShare], by mounting the
// root export, creating the directory, and unmounting. It returns the CSI VolumeId and the
// VolumeContext to hand back to Kubernetes.
func CreateVolume(ctx context.Context, params map[string]string, volumeName string) (
	string, map[string]string, error) {
	log := logger.GetLogger(ctx)

	server := params[ParamServer]
	share := params[ParamShare]
	subDir := params[ParamSubDir]
	if subDir == "" {
		subDir = volumeName
	}

	id := VolumeID{
		Server: server,
		Share:  share,
		SubDir: subDir,
		UUID:   uuid.NewString(),
	}

	log.Infof("externalnfs.CreateVolume: creating subdirectory %q on export %q",
		subDir, id.RootMountSource())

	if err := withRootExportMounted(ctx, id, func(rootMountPath string) error {
		subDirPath := filepath.Join(rootMountPath, subDir)
		if err := os.MkdirAll(subDirPath, 0777); err != nil {
			return fmt.Errorf("failed to create subdirectory %q: %w", subDirPath, err)
		}
		return nil
	}); err != nil {
		return "", nil, err
	}

	volumeContext := map[string]string{
		ParamServer: server,
		ParamShare:  share,
		ParamSubDir: subDir,
	}
	if mountOptions := params[ParamMountOptions]; mountOptions != "" {
		volumeContext[ParamMountOptions] = mountOptions
	}
	log.Infof("externalnfs.CreateVolume: volume %q ready at %q", id.Encode(), id.MountSource())
	return id.Encode(), volumeContext, nil
}

// DeleteVolume removes the subdirectory backing the given external-NFS VolumeId by mounting
// the root export, removing the directory, and unmounting.
func DeleteVolume(ctx context.Context, volumeID string) error {
	log := logger.GetLogger(ctx)

	id, err := DecodeVolumeID(volumeID)
	if err != nil {
		return err
	}

	log.Infof("externalnfs.DeleteVolume: removing subdirectory %q from export %q",
		id.SubDir, id.RootMountSource())

	return withRootExportMounted(ctx, id, func(rootMountPath string) error {
		subDirPath := filepath.Join(rootMountPath, id.SubDir)
		if err := os.RemoveAll(subDirPath); err != nil {
			return fmt.Errorf("failed to remove subdirectory %q: %w", subDirPath, err)
		}
		return nil
	})
}

// withRootExportMounted mounts id's root export at a scratch path for the duration of fn, and
// always unmounts it afterwards, even if fn fails.
func withRootExportMounted(ctx context.Context, id VolumeID, fn func(rootMountPath string) error) error {
	log := logger.GetLogger(ctx)

	rootMountPath := filepath.Join(workingMountDir, sanitizeForPath(id.Server), sanitizeForPath(id.Share))
	if err := os.MkdirAll(rootMountPath, 0750); err != nil {
		return fmt.Errorf("failed to create scratch mount dir %q: %w", rootMountPath, err)
	}

	if err := gofsutil.Mount(ctx, id.RootMountSource(), rootMountPath, "nfs"); err != nil {
		return fmt.Errorf("failed to mount export %q at %q: %w", id.RootMountSource(), rootMountPath, err)
	}
	defer func() {
		if err := gofsutil.Unmount(ctx, rootMountPath); err != nil {
			log.Errorf("externalnfs: failed to unmount scratch path %q: %v", rootMountPath, err)
		}
	}()

	return fn(rootMountPath)
}

// sanitizeForPath makes s safe to use as a single path component.
func sanitizeForPath(s string) string {
	return strings.NewReplacer("/", "_", ":", "_").Replace(s)
}
