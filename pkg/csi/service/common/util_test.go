/*
Copyright 2019 The Kubernetes Authors.

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

package common

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"gitlab.eng.vmware.com/hatchway/govmomi/vsan/vsanfs/types"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
)

func init() {
	// Create context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
}

func TestIsFileVolumeRequestForBlock(t *testing.T) {
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
		},
	}
	if IsFileVolumeRequest(ctx, volCap) {
		t.Errorf("VolCap = %+v reported as a FILE volume!", volCap)
	}
}

func TestIsFileVolumeRequestForBlockWithUnsetFsType(t *testing.T) {
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
		},
	}
	if IsFileVolumeRequest(ctx, volCap) {
		t.Errorf("VolCap = %+v reported as a FILE volume!", volCap)
	}
}

func TestIsFileVolumeRequestForFile(t *testing.T) {
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs4",
				},
			},
		},
	}
	if !IsFileVolumeRequest(ctx, volCap) {
		t.Errorf("VolCap = %+v reported as a BLOCK volume!", volCap)
	}

	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs",
				},
			},
		},
	}
	if !IsFileVolumeRequest(ctx, volCap) {
		t.Errorf("VolCap = %+v reported as a BLOCK volume!", volCap)
	}
}

func TestValidVolumeCapabilitiesForBlock(t *testing.T) {
	// fstype=ext4 and mode=SINGLE_NODE_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Block VolCap = %+v failed validation!", volCap)
	}
	// fstype=empty and mode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Block VolCap = %+v failed validation!", volCap)
	}
}

func TestInvalidVolumeCapabilitiesForBlock(t *testing.T) {
	// Invalid case: fstype=ext4 and mode=MULTI_NODE_MULTI_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=ext4 and mode=MULTI_NODE_READER_ONLY
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		},
	}
	if IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=ext4 and mode=MULTI_NODE_SINGLE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			},
		},
	}
	if IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}
}

func TestValidVolumeCapabilitiesForFile(t *testing.T) {
	// fstype=nfsv4 and mode=MULTI_NODE_MULTI_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}

	// fstype=nfsv4 and mode=MULTI_NODE_READER_ONLY
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		},
	}
	if !IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}

	// fstype=nfsv4 and mode=MULTI_NODE_SINGLE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}
}

func TestInvalidVolumeCapabilitiesForFile(t *testing.T) {
	// Invalid case: fstype=ext4 and mode=MULTI_NODE_MULTI_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Invalid file VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=ext4 and mode=MULTI_NODE_MULTI_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "nfs",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if IsValidVolumeCapabilities(ctx, volCap) {
		t.Errorf("Invalid file VolCap = %+v passed validation!", volCap)
	}
}

func isStorageClassParamsEqual(expected *StorageClassParams, actual *StorageClassParams) bool {
	if expected.DatastoreURL != actual.DatastoreURL {
		return false
	}
	if expected.StoragePolicyName != actual.StoragePolicyName {
		return false
	}
	if len(expected.NetPermissions) != len(actual.NetPermissions) {
		return false
	}
	for _, netPermissionExp := range expected.NetPermissions {
		found := false
		for _, netPermissionAct := range actual.NetPermissions {
			if netPermissionExp.AllowRoot == netPermissionAct.AllowRoot &&
				netPermissionExp.Permissions == netPermissionAct.Permissions &&
				netPermissionExp.Ips == netPermissionAct.Ips {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestParseStorageClassParamsWithDeprecatedFSType(t *testing.T) {
	params := map[string]string{
		"fstype": "ext4",
	}
	expectedScParams := &StorageClassParams{}
	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithNoNetPermissionParams(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
	}
	expectedNetPermissions := []types.VsanFileShareNetPermission{}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
		NetPermissions:    expectedNetPermissions,
	}

	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidParams(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "true",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		IPs:                        "10.20.30.10/8",
	}
	expectedNetPermissions := []types.VsanFileShareNetPermission{
		{
			AllowRoot:   true,
			Permissions: types.VsanFileShareAccessTypeREAD_ONLY,
			Ips:         "10.20.30.10/8",
		},
	}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
		NetPermissions:    expectedNetPermissions,
	}

	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidRamdomOrderParams(t *testing.T) {
	// Random order of params with different values
	params := map[string]string{
		IPs:                        "10.20.30.0/8",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "false",
		AttributeDatastoreURL:      "ds1",
		Permission:                 string(types.VsanFileShareAccessTypeNO_ACCESS),
	}
	expectedNetPermissions := []types.VsanFileShareNetPermission{
		{
			AllowRoot:   false,
			Permissions: types.VsanFileShareAccessTypeNO_ACCESS,
			Ips:         "10.20.30.0/8",
		},
	}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
		NetPermissions:    expectedNetPermissions,
	}

	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidParamsMultipleNetPermissions(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "false",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		IPs:                        "10.20.30.0/8",
		"allowroot.1":              "true",
		"permission.1":             string(types.VsanFileShareAccessTypeREAD_WRITE),
		"ips.1":                    "10.20.20.0/8",
	}
	expectedNetPermissions := []types.VsanFileShareNetPermission{
		{
			AllowRoot:   false,
			Permissions: types.VsanFileShareAccessTypeREAD_ONLY,
			Ips:         "10.20.30.0/8",
		},
		{
			AllowRoot:   true,
			Permissions: types.VsanFileShareAccessTypeREAD_WRITE,
			Ips:         "10.20.20.0/8",
		},
	}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
		NetPermissions:    expectedNetPermissions,
	}

	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidParamsMultipleNetPermissionsAndDefaults(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		IPs:                        "10.20.30.0/8",
		"allowroot.1":              "true",
		"ips.1":                    "10.20.20.0/8",
		"permission.2":             string(types.VsanFileShareAccessTypeREAD_ONLY),
	}
	expectedNetPermissions := []types.VsanFileShareNetPermission{
		{
			AllowRoot:   true, // Default settings
			Permissions: types.VsanFileShareAccessTypeREAD_ONLY,
			Ips:         "10.20.30.0/8",
		},
		{
			AllowRoot:   true,
			Permissions: types.VsanFileShareAccessTypeREAD_WRITE, // Default settings
			Ips:         "10.20.20.0/8",
		},
		{
			AllowRoot:   true, // Default settings
			Permissions: types.VsanFileShareAccessTypeREAD_ONLY,
			Ips:         "*", // Default settings
		},
	}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
		NetPermissions:    expectedNetPermissions,
	}

	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidRamdomOrderParamsMultipleNetPermissions(t *testing.T) {
	params := map[string]string{
		"ips.1":                    "10.20.20.0/8",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "false",
		"permission.1":             string(types.VsanFileShareAccessTypeREAD_WRITE),
		IPs:                        "10.20.30.0/8",
		"allowroot.1":              "true",
		AttributeDatastoreURL:      "ds1",
	}
	expectedNetPermissions := []types.VsanFileShareNetPermission{
		{
			AllowRoot:   false,
			Permissions: types.VsanFileShareAccessTypeREAD_ONLY,
			Ips:         "10.20.30.0/8",
		},
		{
			AllowRoot:   true,
			Permissions: types.VsanFileShareAccessTypeREAD_WRITE,
			Ips:         "10.20.20.0/8",
		},
	}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
		NetPermissions:    expectedNetPermissions,
	}

	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("Failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidAndUnsupportedParamsMultipleNetPermissions(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "false",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		IPs:                        "10.20.30.0/8",
		"allowroot.1":              "true",
		"permission.1":             string(types.VsanFileShareAccessTypeREAD_WRITE),
		"ips.1":                    "10.20.20.0/8",
		"allowroot.2.2":            "true",                                          // unsupported param
		"xyz.2":                    string(types.VsanFileShareAccessTypeREAD_WRITE), // unsupported param
		"ips.1.1":                  "10.20.10.0/8",                                  // unsupported param
	}
	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("Expected failure due to invalid param. actualScParams: %+v", actualScParams)
	}
}

func TestParseStorageClassParamsWithInvalidParam(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "invalid",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		IPs:                        "10.20.30.10/8",
		"InvalidParam":             "InvalidValue",
	}
	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("Expected failure due to invalid param. actualScParams: %+v", actualScParams)
	}
}

func TestParseStorageClassParamsWithInvalidAllowRootValue(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "invalid",
		Permission:                 string(types.VsanFileShareAccessTypeREAD_ONLY),
		IPs:                        "10.20.30.10/8",
	}
	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("Expected failure due to invalid allow root value. actualScParams: %+v", actualScParams)
	}
}

func TestParseStorageClassParamsWithInvalidPermissionValue(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
		AllowRoot:                  "true",
		Permission:                 "Invalid_Permission",
		IPs:                        "10.20.30.10/8",
	}
	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("Expected failure due to invalid permission value. actualScParams: %+v", actualScParams)
	}
}
