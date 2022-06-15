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
	"strconv"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
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
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
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
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
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
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
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
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
		t.Errorf("Block VolCap = %+v failed validation!", volCap)
	}
}

func TestInvalidVolumeCapabilitiesForBlock(t *testing.T) {
	// Invalid case: fstype=nfs and mode=SINGLE_NODE_WRITER
	volCap := []*csi.VolumeCapability{
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err == nil {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=nfs4 and mode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err == nil {
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}
}

func TestInvalidVolumeCapabilitiesForFile(t *testing.T) {
	// Invalid case: fstype=nfs4 and mode=SINGLE_NODE_WRITER
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err == nil {
		t.Errorf("Invalid file VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=nfs and mode=SINGLE_NODE_WRITER
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
	if err := IsValidVolumeCapabilities(ctx, volCap); err == nil {
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
	return true
}

func TestParseStorageClassParamsWithDeprecatedFSType(t *testing.T) {
	params := map[string]string{
		"fstype": "ext4",
	}
	expectedScParams := &StorageClassParams{}
	csiMigrationFeatureState := false
	actualScParams, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err != nil {
		t.Errorf("failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithValidParams(t *testing.T) {
	params := map[string]string{
		AttributeDatastoreURL:      "ds1",
		AttributeStoragePolicyName: "policy1",
	}
	expectedScParams := &StorageClassParams{
		DatastoreURL:      "ds1",
		StoragePolicyName: "policy1",
	}
	csiMigrationFeatureState := false
	actualScParams, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err != nil {
		t.Errorf("failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithMigrationEnabledNagative(t *testing.T) {
	csiMigrationFeatureState := true
	params := map[string]string{
		CSIMigrationParams:                   "true",
		DatastoreMigrationParam:              "vSANDatastore",
		AttributeStoragePolicyName:           "policy1",
		HostFailuresToTolerateMigrationParam: "1",
		ForceProvisioningMigrationParam:      "true",
		CacheReservationMigrationParam:       " 25",
		DiskstripesMigrationParam:            "2",
		ObjectspacereservationMigrationParam: "50",
		IopslimitMigrationParam:              "16",
	}
	scParam, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err == nil {
		t.Errorf("error expected but not received. scParam received from ParseStorageClassParams: %v", scParam)
	}
	t.Logf("expected err received. err: %v", err)
}

func TestParseStorageClassParamsWithDiskFormatMigrationEnableNegative(t *testing.T) {
	csiMigrationFeatureState := true
	params := map[string]string{
		CSIMigrationParams:       "true",
		DiskFormatMigrationParam: "thick",
	}
	scParam, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err == nil {
		t.Errorf("error expected but not received. scParam received from ParseStorageClassParams: %v", scParam)
	}
	t.Logf("expected err received. err: %v", err)
}

func TestParseStorageClassParamsWithDiskFormatMigrationEnablePositive(t *testing.T) {
	csiMigrationFeatureState := true
	params := map[string]string{
		CSIMigrationParams:       "true",
		DiskFormatMigrationParam: "thin",
	}
	expectedScParams := &StorageClassParams{
		CSIMigration: "true",
	}
	scParam, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err != nil {
		t.Errorf("failed to parse params: %+v, err: %+v", params, err)
	}
	if !isStorageClassParamsEqual(expectedScParams, scParam) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, scParam)
	}
}

func TestParseStorageClassParamsWithMigrationEnabledPositive(t *testing.T) {
	csiMigrationFeatureState := true
	params := map[string]string{
		CSIMigrationParams:         "true",
		DatastoreMigrationParam:    "vSANDatastore",
		AttributeStoragePolicyName: "policy1",
	}
	expectedScParams := &StorageClassParams{
		Datastore:         "vSANDatastore",
		StoragePolicyName: "policy1",
		CSIMigration:      "true",
	}
	scParam, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err != nil {
		t.Errorf("failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, scParam) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, scParam)
	}
}

func TestParseStorageClassParamsWithMigrationDisabled(t *testing.T) {
	csiMigrationFeatureState := false
	params := map[string]string{
		CSIMigrationParams:                   "true",
		DatastoreMigrationParam:              "vSANDatastore",
		AttributeStoragePolicyName:           "policy1",
		HostFailuresToTolerateMigrationParam: "1",
	}
	scParam, err := ParseStorageClassParams(ctx, params, csiMigrationFeatureState)
	if err == nil {
		t.Errorf("error expected but not received. scParam received from ParseStorageClassParams: %v", scParam)
	}
	t.Logf("expected err received. err: %v", err)
}

func TestUnescape(t *testing.T) {
	tests := []struct {
		in, out string
	}{
		{
			// Space is unescaped. This is basically the only test that can happen in reality
			// and only when in-tree in-line volume in a Pod is used with CSI migration enabled.
			in:  `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-[WorkloadDatastore]\0405137595f-7ce3-e95a-5c03-06d835dea807/e2e-vmdk-1641374604660540311.vmdk/globalmount`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-[WorkloadDatastore] 5137595f-7ce3-e95a-5c03-06d835dea807/e2e-vmdk-1641374604660540311.vmdk/globalmount`,
		},
		{
			// Multiple spaces are unescaped.
			in:  `/var/lib/kube\040let/plug\040ins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-foo\040bar\040baz`,
			out: `/var/lib/kube let/plug ins/kubernetes.io/csi/pv/csi.vsphere.vmware.com-foo bar baz`,
		},
		{
			// Too short escape sequence. Expect the same string on output.
			in:  `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\04`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\04`,
		},
		{
			// Wrong characters in the escape sequence. Expect the same string on output.
			in:  `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\0bc`,
			out: `/var/lib/kubelet/plugins/kubernetes.io/csi/pv/foo\0bc`,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			out := Unescape(ctx, test.in)
			if out != test.out {
				t.Errorf("Expected %q to be unescaped as %q, got %q", test.in, test.out, out)
			}
		})
	}
}
