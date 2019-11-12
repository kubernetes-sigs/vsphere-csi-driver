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
	"github.com/container-storage-interface/spec/lib/go/csi"
	"testing"
)

func TestIsFileVolumeRequestForBlock(t *testing.T) {
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"ext4",
				},
			},
		},
	}
	if IsFileVolumeRequest(volCap) {
		t.Errorf("VolCap = %+v reported as a FILE volume!", volCap)
	}
}

func TestIsFileVolumeRequestForBlockWithUnsetFsType(t *testing.T) {
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
				},
			},
		},
	}
	if IsFileVolumeRequest(volCap) {
		t.Errorf("VolCap = %+v reported as a FILE volume!", volCap)
	}
}


func TestIsFileVolumeRequestForFile(t *testing.T) {
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"nfs4",
				},
			},
		},
	}
	if !IsFileVolumeRequest(volCap) {
		t.Errorf("VolCap = %+v reported as a BLOCK volume!", volCap)
	}

	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"nfs",
				},
			},
		},
	}
	if !IsFileVolumeRequest(volCap) {
		t.Errorf("VolCap = %+v reported as a BLOCK volume!", volCap)
	}
}

func TestValidVolumeCapabilitiesForBlock(t *testing.T) {
	// fstype=ext4 and mode=SINGLE_NODE_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(volCap) {
		t.Errorf("Block VolCap = %+v failed validation!", volCap)
	}
	// fstype=empty and mode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(volCap) {
		t.Errorf("Block VolCap = %+v failed validation!", volCap)
	}
}

func TestInvalidVolumeCapabilitiesForBlock(t *testing.T) {
	// Invalid case: fstype=ext4 and mode=MULTI_NODE_MULTI_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if IsValidVolumeCapabilities(volCap) {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=ext4 and mode=MULTI_NODE_READER_ONLY
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		},
	}
	if IsValidVolumeCapabilities(volCap) {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: fstype=ext4 and mode=MULTI_NODE_SINGLE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"ext4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			},
		},
	}
	if IsValidVolumeCapabilities(volCap) {
		t.Errorf("Invalid Block VolCap = %+v passed validation!", volCap)
	}
}

func TestValidVolumeCapabilitiesForFile(t *testing.T) {
	// fstype=nfsv4 and mode=MULTI_NODE_MULTI_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"nfs4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(volCap) {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}

	// fstype=nfsv4 and mode=MULTI_NODE_READER_ONLY
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"nfs",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		},
	}
	if !IsValidVolumeCapabilities(volCap) {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}

	// fstype=nfsv4 and mode=MULTI_NODE_SINGLE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType:"nfs4",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode:csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			},
		},
	}
	if !IsValidVolumeCapabilities(volCap) {
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
	if IsValidVolumeCapabilities(volCap) {
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
	if IsValidVolumeCapabilities(volCap) {
		t.Errorf("Invalid file VolCap = %+v passed validation!", volCap)
	}
}

