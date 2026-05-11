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
	"errors"
	"fmt"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/dynamic"

	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"

	"github.com/container-storage-interface/spec/lib/go/csi"
	cbtconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cbtconfig/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsvolumeoperationrequest "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
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
	// fstype=xfs and mode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "xfs",
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
	// volumeMode=block and accessMode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
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

	// fstype=empty and mode=MULTI_NODE_MULTI_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}

	// fstype=nfs and mode=MULTI_NODE_READER_ONLY
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

	// fstype=ntfs and mode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "ntfs",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}

	// fstype=NTFS and mode=SINGLE_NODE_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "NTFS",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	if err := IsValidVolumeCapabilities(ctx, volCap); err != nil {
		t.Errorf("File VolCap = %+v failed validation!", volCap)
	}
}

func TestInvalidVolumeCapabilitiesForFile(t *testing.T) {
	// Invalid case: fstype=xfs and mode=MULTI_NODE_MULTI_WRITER
	volCap := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{
					FsType: "xfs",
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if err := IsValidVolumeCapabilities(ctx, volCap); err == nil {
		t.Errorf("Invalid file VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: volumeMode=block and accessMode=MULTI_NODE_MULTI_WRITER
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if err := IsValidVolumeCapabilities(ctx, volCap); err == nil {
		t.Errorf("Invalid file VolCap = %+v passed validation!", volCap)
	}

	// Invalid case: volumeMode=block and accessMode=MULTI_NODE_READER_ONLY
	volCap = []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				Block: &csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
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
	actualScParams, err := ParseStorageClassParams(ctx, params)
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
	actualScParams, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, actualScParams) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, actualScParams)
	}
}

func TestParseStorageClassParamsWithMigrationEnabledNagative(t *testing.T) {
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
	scParam, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("error expected but not received. scParam received from ParseStorageClassParams: %v", scParam)
	}
	t.Logf("expected err received. err: %v", err)
}

func TestParseStorageClassParamsWithDiskFormatMigrationEnableNegative(t *testing.T) {
	params := map[string]string{
		CSIMigrationParams:       "true",
		DiskFormatMigrationParam: "thick",
	}
	scParam, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("error expected but not received. scParam received from ParseStorageClassParams: %v", scParam)
	}
	t.Logf("expected err received. err: %v", err)
}

func TestParseStorageClassParamsWithDiskFormatMigrationEnablePositive(t *testing.T) {
	params := map[string]string{
		CSIMigrationParams:       "true",
		DiskFormatMigrationParam: "thin",
	}
	expectedScParams := &StorageClassParams{
		CSIMigration: "true",
	}
	scParam, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("failed to parse params: %+v, err: %+v", params, err)
	}
	if !isStorageClassParamsEqual(expectedScParams, scParam) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, scParam)
	}
}

func TestParseStorageClassParamsWithMigrationEnabledPositive(t *testing.T) {
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
	scParam, err := ParseStorageClassParams(ctx, params)
	if err != nil {
		t.Errorf("failed to parse params: %+v", params)
	}
	if !isStorageClassParamsEqual(expectedScParams, scParam) {
		t.Errorf("Expected: %+v\n Actual: %+v", expectedScParams, scParam)
	}
}

func TestParseStorageClassParamsWithMigrationDisabled(t *testing.T) {
	params := map[string]string{
		CSIMigrationParams:                   "true",
		DatastoreMigrationParam:              "vSANDatastore",
		AttributeStoragePolicyName:           "policy1",
		HostFailuresToTolerateMigrationParam: "1",
	}
	scParam, err := ParseStorageClassParams(ctx, params)
	if err == nil {
		t.Errorf("error expected but not received. scParam received from ParseStorageClassParams: %v", scParam)
	}
	t.Logf("expected err received. err: %v", err)
}

func TestParseCSISnapshotID(t *testing.T) {
	type args struct {
		ctx           context.Context
		csiSnapshotID string
	}
	sampleCnsVolumeID := uuid.New().String()
	sampleCnsSnapshotID := uuid.New().String()
	tests := []struct {
		name                  string
		args                  args
		expectedCnsVolumeID   string
		expectedCnsSnapshotID string
		expectedErr           error
	}{
		{
			name:                  "ExpectedCSISnapshotID",
			args:                  args{ctx: context.TODO(), csiSnapshotID: sampleCnsVolumeID + "+" + sampleCnsSnapshotID},
			expectedCnsVolumeID:   sampleCnsVolumeID,
			expectedCnsSnapshotID: sampleCnsSnapshotID,
			expectedErr:           nil,
		},
		{
			name:                  "EmptyCSISnapshotID",
			args:                  args{ctx: context.TODO(), csiSnapshotID: ""},
			expectedCnsVolumeID:   "",
			expectedCnsSnapshotID: "",
			expectedErr:           errors.New("csiSnapshotID from the input is empty"),
		},
		{
			name:                  "UnexpectedFormattedCSISnapshotID",
			args:                  args{ctx: context.TODO(), csiSnapshotID: sampleCnsVolumeID},
			expectedCnsVolumeID:   "",
			expectedCnsSnapshotID: "",
			expectedErr:           fmt.Errorf("unexpected format in csiSnapshotID: %v", sampleCnsVolumeID),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualCnsVolumeID, actualCnsSnapshotID, actualErr := ParseCSISnapshotID(tt.args.csiSnapshotID)
			assert.Equal(t, tt.expectedErr == nil, actualErr == nil)
			if tt.expectedErr != nil && actualErr != nil {
				assert.Equal(t, tt.expectedErr.Error(), actualErr.Error())
			}
			assert.Equal(t, tt.expectedCnsVolumeID, actualCnsVolumeID)
			assert.Equal(t, tt.expectedCnsSnapshotID, actualCnsSnapshotID)
		})
	}
}

func TestGetClusterComputeResourceMoIds_MultipleClustersPerAZ(t *testing.T) {
	gomega.RegisterTestingT(t)

	patches := gomonkey.NewPatches()
	defer patches.Reset()

	az1 := &unstructured.Unstructured{}
	az1.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "topology.tanzu.vmware.com",
		Version: "v1alpha1",
		Kind:    "AvailabilityZone",
	})
	az1.SetName("zone-a")
	_ = unstructured.SetNestedStringSlice(az1.Object, []string{"domain-c1", "domain-c2"},
		"spec", "clusterComputeResourceMoIDs")

	az2 := &unstructured.Unstructured{}
	az2.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "topology.tanzu.vmware.com",
		Version: "v1alpha1",
		Kind:    "AvailabilityZone",
	})
	az2.SetName("zone-b")
	_ = unstructured.SetNestedStringSlice(az2.Object, []string{"domain-c3"}, "spec", "clusterComputeResourceMoIDs")

	fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), az1, az2)
	patches.ApplyFuncVar(&getAvailabilityZoneClient, func() (dynamic.Interface, error) {
		return fakeClient, nil
	})

	// Patch newDynamicClientForConfig alias to return the fake client
	patches.ApplyFunc(dynamic.NewForConfig, func(cfg *rest.Config) (dynamic.Interface, error) {
		return fakeClient, nil
	})

	moIDs, multiple, err := GetClusterComputeResourceMoIds(context.Background())

	gomega.Expect(err).To(gomega.BeNil())
	gomega.Expect(multiple).To(gomega.BeTrue())
	gomega.Expect(moIDs).To(gomega.ContainElements("domain-c1", "domain-c2", "domain-c3"))
}

func TestGetClusterComputeResourceMoIds_SingleClusterPerAZ(t *testing.T) {
	gomega.RegisterTestingT(t)
	patches := gomonkey.NewPatches()
	defer patches.Reset()

	az1 := &unstructured.Unstructured{}
	az1.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "topology.tanzu.vmware.com",
		Version: "v1alpha1",
		Kind:    "AvailabilityZone",
	})
	az1.SetName("zone-a")
	_ = unstructured.SetNestedStringSlice(az1.Object, []string{"domain-c1"}, "spec", "clusterComputeResourceMoIDs")

	az2 := &unstructured.Unstructured{}
	az2.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "topology.tanzu.vmware.com",
		Version: "v1alpha1",
		Kind:    "AvailabilityZone",
	})
	az2.SetName("zone-b")
	_ = unstructured.SetNestedStringSlice(az2.Object, []string{"domain-c2"}, "spec", "clusterComputeResourceMoIDs")

	fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), az1, az2)
	patches.ApplyFuncVar(&getAvailabilityZoneClient, func() (dynamic.Interface, error) {
		return fakeClient, nil
	})

	moIDs, multiple, err := GetClusterComputeResourceMoIds(context.Background())
	gomega.Expect(err).To(gomega.BeNil())
	gomega.Expect(multiple).To(gomega.BeFalse())
	gomega.Expect(moIDs).To(gomega.ContainElements("domain-c1", "domain-c2"))
}

func cbtConfigUnstructured(name, namespace string, statusEnabled *bool) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   cbtconfigv1alpha1.GroupName,
		Version: cbtconfigv1alpha1.Version,
		Kind:    "CBTConfig",
	})
	u.SetName(name)
	u.SetNamespace(namespace)
	_ = unstructured.SetNestedField(u.Object, true, "spec", "enabled")
	if statusEnabled != nil {
		_ = unstructured.SetNestedField(u.Object, *statusEnabled, "status", "enabled")
	}
	return u
}

func cbtTestDynamicClient(t *testing.T, objs ...runtime.Object) dynamic.Interface {
	t.Helper()
	cbtGVR := cbtconfigv1alpha1.GroupVersion.WithResource(cbtconfigv1alpha1.CBTConfigResource)
	gvrToListKind := map[schema.GroupVersionResource]string{
		cbtGVR: "CBTConfigList",
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

// errDynamicClient is a minimal dynamic.Interface stub that always returns a
// fixed error from List. Only the Resource chain is implemented; all other
// methods panic to catch unexpected calls.
type errDynamicClient struct{ err error }

func (e *errDynamicClient) Resource(schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &errNamespaceableResource{err: e.err}
}
func (e *errDynamicClient) Tracker() k8stesting.ObjectTracker { panic("not implemented") }

type errNamespaceableResource struct{ err error }

func (r *errNamespaceableResource) Namespace(string) dynamic.ResourceInterface { return r }
func (r *errNamespaceableResource) List(
	_ context.Context, _ metav1.ListOptions,
) (*unstructured.UnstructuredList, error) {
	return nil, r.err
}
func (r *errNamespaceableResource) Create(
	_ context.Context, _ *unstructured.Unstructured, _ metav1.CreateOptions, _ ...string,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) Update(
	_ context.Context, _ *unstructured.Unstructured, _ metav1.UpdateOptions, _ ...string,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) UpdateStatus(
	_ context.Context, _ *unstructured.Unstructured, _ metav1.UpdateOptions,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) Delete(
	_ context.Context, _ string, _ metav1.DeleteOptions, _ ...string,
) error {
	panic("not implemented")
}
func (r *errNamespaceableResource) DeleteCollection(
	_ context.Context, _ metav1.DeleteOptions, _ metav1.ListOptions,
) error {
	panic("not implemented")
}
func (r *errNamespaceableResource) Get(
	_ context.Context, _ string, _ metav1.GetOptions, _ ...string,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) Watch(
	_ context.Context, _ metav1.ListOptions,
) (watch.Interface, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) Patch(
	_ context.Context, _ string, _ types.PatchType, _ []byte, _ metav1.PatchOptions, _ ...string,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) Apply(
	_ context.Context, _ string, _ *unstructured.Unstructured, _ metav1.ApplyOptions, _ ...string,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}
func (r *errNamespaceableResource) ApplyStatus(
	_ context.Context, _ string, _ *unstructured.Unstructured, _ metav1.ApplyOptions,
) (*unstructured.Unstructured, error) {
	panic("not implemented")
}

func TestCBTStateForNamespace(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	enabled := true
	disabled := false

	t.Run("API unavailable (NoKindMatchError)", func(t *testing.T) {
		noMatch := &apiMeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "cbt.vsphere.vmware.com"}}
		dyn := &errDynamicClient{err: noMatch}
		en, configured, err := CBTStateForNamespace(ctx, dyn, ns)
		assert.NoError(t, err)
		assert.False(t, configured)
		assert.False(t, en)
	})

	t.Run("no CBTConfig objects", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t)
		en, configured, err := CBTStateForNamespace(ctx, dyn, ns)
		assert.NoError(t, err)
		assert.False(t, configured)
		assert.False(t, en)
	})

	t.Run("status.enabled true", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, &enabled))
		en, configured, err := CBTStateForNamespace(ctx, dyn, ns)
		assert.NoError(t, err)
		assert.True(t, configured)
		assert.True(t, en)
	})

	t.Run("status.enabled false", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, &disabled))
		en, configured, err := CBTStateForNamespace(ctx, dyn, ns)
		assert.NoError(t, err)
		assert.True(t, configured)
		assert.False(t, en)
	})

	t.Run("status.enabled omitted", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, nil))
		en, configured, err := CBTStateForNamespace(ctx, dyn, ns)
		assert.NoError(t, err)
		assert.False(t, configured)
		assert.False(t, en)
	})
}

func TestIsFVSVolumeHandle(t *testing.T) {
	tests := []struct {
		name         string
		volumeHandle string
		want         bool
	}{
		{"empty handle", "", false},
		{"legacy supervisor pvc name", "test-tkc-pvc-12345", false},
		{"FCD volume id", "12345678-1234-1234-1234-123456789012", false},
		{"legacy file: prefix", "file:abc-def", false},
		{"FVS volume id", "fv:my-instance-ns:my-fv-name", true},
		{"FVS prefix only", "fv:", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsFVSVolumeHandle(tt.volumeHandle); got != tt.want {
				t.Fatalf("IsFVSVolumeHandle(%q) = %v, want %v", tt.volumeHandle, got, tt.want)
			}
		})
	}
}

func TestParseFVSVolumeHandle(t *testing.T) {
	tests := []struct {
		name         string
		volumeHandle string
		wantNS       string
		wantName     string
		wantErr      bool
	}{
		{"empty handle", "", "", "", true},
		{"missing prefix", "tenant-ns:fv-foo", "", "", true},
		{"prefix only", "fv:", "", "", true},
		{"missing name", "fv:tenant-ns:", "", "", true},
		{"missing namespace", "fv::fv-foo", "", "", true},
		{"missing separator", "fv:tenant-ns", "", "", true},
		{"valid handle", "fv:tenant-ns:fv-foo", "tenant-ns", "fv-foo", false},
		{"valid handle with colon in name preserves remaining colon",
			"fv:tenant-ns:fv-foo:bar", "tenant-ns", "fv-foo:bar", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNS, gotName, err := ParseFVSVolumeHandle(tt.volumeHandle)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseFVSVolumeHandle(%q) err = %v, wantErr = %v", tt.volumeHandle, err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if gotNS != tt.wantNS || gotName != tt.wantName {
				t.Fatalf("ParseFVSVolumeHandle(%q) = (%q, %q), want (%q, %q)",
					tt.volumeHandle, gotNS, gotName, tt.wantNS, tt.wantName)
			}
		})
	}
}

func TestIsFVSStorageClassName(t *testing.T) {
	tests := []struct {
		name             string
		storageClassName string
		want             bool
	}{
		{"empty", "", false},
		{"unrelated sc", "wcpglobal-storage-profile", false},
		{"legacy file sc", "file-storage-class", false},
		{"FVS immediate", StorageClassVsanFileServicePolicy, true},
		{"FVS late binding", StorageClassVsanFileServicePolicyLateBinding, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsFVSStorageClassName(tt.storageClassName); got != tt.want {
				t.Fatalf("IsFVSStorageClassName(%q) = %v, want %v", tt.storageClassName, got, tt.want)
			}
		})
	}
}

func TestIsFVSPersistentVolumeClaim(t *testing.T) {
	scFVS := StorageClassVsanFileServicePolicy
	scLegacy := "legacy-file-sc"
	tests := []struct {
		name string
		pvc  *corev1.PersistentVolumeClaim
		want bool
	}{
		{"nil pvc", nil, false},
		{"nil storage class", &corev1.PersistentVolumeClaim{}, false},
		{"legacy sc", &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "p"},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scLegacy},
		}, false},
		{"FVS immediate", &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "p"},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &scFVS},
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsFVSPersistentVolumeClaim(tt.pvc); got != tt.want {
				t.Fatalf("IsFVSPersistentVolumeClaim() = %v, want %v", got, tt.want)
			}
		})
	}
}

// cbtFlagsMockVolumeManager is a minimal volume.Manager stub that satisfies only
// the two methods exercised by SyncVolumeCBTState, letting tests inject errors
// and observe which operation was invoked.
type cbtFlagsMockVolumeManager struct {
	setCalled   bool
	clearCalled bool
	setErr      error
	clearErr    error
}

func (m *cbtFlagsMockVolumeManager) SetVolumeControlFlags(
	_ context.Context, _ string, _ []string,
) error {
	m.setCalled = true
	return m.setErr
}

func (m *cbtFlagsMockVolumeManager) ClearVolumeControlFlags(
	_ context.Context, _ string, _ []string,
) error {
	m.clearCalled = true
	return m.clearErr
}

// Remaining volume.Manager methods — not exercised by SyncVolumeCBTState.
func (m *cbtFlagsMockVolumeManager) CreateVolume(context.Context,
	*cnstypes.CnsVolumeCreateSpec, interface{}) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "", nil
}
func (m *cbtFlagsMockVolumeManager) AttachVolume(context.Context,
	*cnsvsphere.VirtualMachine, string, bool) (string, string, error) {
	return "", "", nil
}
func (m *cbtFlagsMockVolumeManager) DetachVolume(context.Context,
	*cnsvsphere.VirtualMachine, string) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) DeleteVolume(context.Context, string, bool) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) UpdateVolumeMetadata(context.Context,
	*cnstypes.CnsVolumeMetadataUpdateSpec) error {
	return nil
}
func (m *cbtFlagsMockVolumeManager) UpdateVolumeCrypto(context.Context,
	*cnstypes.CnsVolumeCryptoUpdateSpec) error {
	return nil
}
func (m *cbtFlagsMockVolumeManager) QueryVolumeInfo(context.Context,
	[]cnstypes.CnsVolumeId) (*cnstypes.CnsQueryVolumeInfoResult, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) QueryAllVolume(context.Context, cnstypes.CnsQueryFilter,
	cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) QueryVolumeAsync(context.Context, cnstypes.CnsQueryFilter,
	*cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) QueryVolume(context.Context,
	cnstypes.CnsQueryFilter) (*cnstypes.CnsQueryResult, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) RelocateVolume(context.Context,
	...cnstypes.BaseCnsVolumeRelocateSpec) (*object.Task, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) ExpandVolume(context.Context, string, int64,
	interface{}) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) ResetManager(context.Context,
	*cnsvsphere.VirtualCenter) error {
	return nil
}
func (m *cbtFlagsMockVolumeManager) ConfigureVolumeACLs(context.Context,
	cnstypes.CnsVolumeACLConfigureSpec) error {
	return nil
}
func (m *cbtFlagsMockVolumeManager) RegisterDisk(context.Context, string, string) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) RetrieveVStorageObject(context.Context,
	string) (*vim25types.VStorageObject, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) ProtectVolumeFromVMDeletion(context.Context, string) error {
	return nil
}
func (m *cbtFlagsMockVolumeManager) CreateSnapshot(context.Context, string, string,
	interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) DeleteSnapshot(context.Context, string, string,
	interface{}) (*cnsvolume.CnsSnapshotInfo, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) QuerySnapshots(context.Context,
	cnstypes.CnsSnapshotQueryFilter) (*cnstypes.CnsSnapshotQueryResult, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) MonitorCreateVolumeTask(context.Context,
	**cnsvolumeoperationrequest.VolumeOperationRequestDetails, *object.Task,
	string, string) (*cnsvolume.CnsVolumeInfo, string, error) {
	return nil, "", nil
}
func (m *cbtFlagsMockVolumeManager) GetOperationStore() cnsvolumeoperationrequest.VolumeOperationRequest {
	return nil
}
func (m *cbtFlagsMockVolumeManager) IsListViewReady() bool               { return true }
func (m *cbtFlagsMockVolumeManager) SetListViewNotReady(context.Context) {}
func (m *cbtFlagsMockVolumeManager) UnregisterVolume(context.Context,
	string, bool) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) BatchAttachVolumes(context.Context,
	*cnsvsphere.VirtualMachine, []cnsvolume.BatchAttachRequest) ([]cnsvolume.BatchAttachResult, string, error) {
	return nil, "", nil
}
func (m *cbtFlagsMockVolumeManager) SyncVolume(context.Context,
	[]cnstypes.CnsSyncVolumeSpec) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) ReRegisterVolume(context.Context, string) error {
	return nil
}
func (m *cbtFlagsMockVolumeManager) QueryFCDAllocatedBlocks(context.Context,
	string, string, uint64, uint32) ([]cnsvolume.AllocatedArea, uint64, error) {
	return nil, 0, nil
}
func (m *cbtFlagsMockVolumeManager) QueryFCDChangedBlocks(context.Context,
	string, string, string, uint64, uint32) ([]cnsvolume.ChangedArea, uint64, error) {
	return nil, 0, nil
}

func TestSyncVolumeCBTState(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	enabled := true
	disabled := false

	t.Run("CBT enabled calls Set for each volume", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, &enabled))
		vm := &cbtFlagsMockVolumeManager{}
		SyncVolumeCBTState(ctx, dyn, ns, vm, "vol-1", "vol-2")
		assert.True(t, vm.setCalled)
		assert.False(t, vm.clearCalled)
	})

	t.Run("CBT disabled calls Clear for each volume", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, &disabled))
		vm := &cbtFlagsMockVolumeManager{}
		SyncVolumeCBTState(ctx, dyn, ns, vm, "vol-1", "vol-2")
		assert.False(t, vm.setCalled)
		assert.True(t, vm.clearCalled)
	})

	t.Run("not configured skips flag change", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t) // no CBTConfig CR
		vm := &cbtFlagsMockVolumeManager{}
		SyncVolumeCBTState(ctx, dyn, ns, vm, "vol-1")
		assert.False(t, vm.setCalled)
		assert.False(t, vm.clearCalled)
	})

	t.Run("Set error is swallowed after logging", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, &enabled))
		vm := &cbtFlagsMockVolumeManager{setErr: errors.New("set failed")}
		SyncVolumeCBTState(ctx, dyn, ns, vm, "vol-1")
		assert.True(t, vm.setCalled)
		assert.False(t, vm.clearCalled)
	})

	t.Run("Clear error is swallowed after logging", func(t *testing.T) {
		dyn := cbtTestDynamicClient(t, cbtConfigUnstructured("default", ns, &disabled))
		vm := &cbtFlagsMockVolumeManager{clearErr: errors.New("clear failed")}
		SyncVolumeCBTState(ctx, dyn, ns, vm, "vol-1")
		assert.False(t, vm.setCalled)
		assert.True(t, vm.clearCalled)
	})
}
