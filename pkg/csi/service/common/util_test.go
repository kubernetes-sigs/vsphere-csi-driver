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
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	vim25types "github.com/vmware/govmomi/vim25/types"

	"github.com/container-storage-interface/spec/lib/go/csi"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

	fakeClient := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), az1, az2)
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

	fakeClient := dynfake.NewSimpleDynamicClient(runtime.NewScheme(), az1, az2)
	patches.ApplyFuncVar(&getAvailabilityZoneClient, func() (dynamic.Interface, error) {
		return fakeClient, nil
	})

	moIDs, multiple, err := GetClusterComputeResourceMoIds(context.Background())
	gomega.Expect(err).To(gomega.BeNil())
	gomega.Expect(multiple).To(gomega.BeFalse())
	gomega.Expect(moIDs).To(gomega.ContainElements("domain-c1", "domain-c2"))
}

// cbtConfigObject builds a typed CBTConfig CR for use in ctrlclient/fake tests.
func cbtConfigObject(name, namespace string, statusState *cbtconfigv1alpha1.CBTState) *cbtconfigv1alpha1.CBTConfig {
	specState := cbtconfigv1alpha1.CBTStateActive
	obj := &cbtconfigv1alpha1.CBTConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       cbtconfigv1alpha1.CBTConfigSpec{State: specState},
	}
	if statusState != nil {
		obj.Status = &cbtconfigv1alpha1.CBTConfigStatus{State: statusState}
	}
	return obj
}

// cbtTestScheme returns a runtime.Scheme with CBTConfig types registered.
func cbtTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := cbtconfigv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add CBTConfig scheme: %v", err)
	}
	return s
}

// cbtTestClient returns a ctrlclient.Client backed by the fake builder,
// pre-populated with the given CBTConfig objects.
func cbtTestClient(t *testing.T, objs ...ctrlclient.Object) ctrlclient.Client {
	t.Helper()
	return ctrlfake.NewClientBuilder().WithScheme(cbtTestScheme(t)).WithObjects(objs...).Build()
}

// cbtErrClient returns a ctrlclient.Client whose List always returns the given error.
func cbtErrClient(t *testing.T, listErr error) ctrlclient.Client {
	t.Helper()
	base := ctrlfake.NewClientBuilder().WithScheme(cbtTestScheme(t)).Build()
	return interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c ctrlclient.WithWatch, list ctrlclient.ObjectList,
			opts ...ctrlclient.ListOption) error {
			return listErr
		},
	})
}

func TestCBTStateForNamespace(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	enabled := cbtconfigv1alpha1.CBTStateActive
	disabled := cbtconfigv1alpha1.CBTStateInactive

	t.Run("API unavailable (NoKindMatchError)", func(t *testing.T) {
		noMatch := &apiMeta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "cbt.vsphere.vmware.com"}}
		c := cbtErrClient(t, noMatch)
		en, configured, err := CBTStateForNamespace(ctx, c, ns)
		assert.NoError(t, err)
		assert.False(t, configured)
		assert.False(t, en)
	})

	t.Run("no CBTConfig objects", func(t *testing.T) {
		c := cbtTestClient(t)
		en, configured, err := CBTStateForNamespace(ctx, c, ns)
		assert.NoError(t, err)
		assert.False(t, configured)
		assert.False(t, en)
	})

	t.Run("status.state Active", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, &enabled))
		en, configured, err := CBTStateForNamespace(ctx, c, ns)
		assert.NoError(t, err)
		assert.True(t, configured)
		assert.True(t, en)
	})

	t.Run("status.state Inactive", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, &disabled))
		en, configured, err := CBTStateForNamespace(ctx, c, ns)
		assert.NoError(t, err)
		assert.True(t, configured)
		assert.False(t, en)
	})

	t.Run("status.state omitted", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, nil))
		en, configured, err := CBTStateForNamespace(ctx, c, ns)
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

func (m *cbtFlagsMockVolumeManager) UnprotectVolumeFromVMDeletion(context.Context, string) error {
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
func (m *cbtFlagsMockVolumeManager) UnregisterVolumeEx(context.Context, string) (string, string, error) {
	return "", "", nil
}
func (m *cbtFlagsMockVolumeManager) QueryPendingUnregisters(
	context.Context) ([]cnsvolume.PendingUnregisterRecord, error) {
	return nil, nil
}
func (m *cbtFlagsMockVolumeManager) AckUnregister(context.Context, string) error {
	return nil
}

func (m *cbtFlagsMockVolumeManager) GetDiskFolderURL(context.Context, string) (string, error) {
	return "", nil
}
func (m *cbtFlagsMockVolumeManager) QueryFCDAllocatedBlocks(context.Context,
	string, string, uint64) ([]cnsvolume.DiskArea, uint64, error) {
	return nil, 0, nil
}
func (m *cbtFlagsMockVolumeManager) QueryFCDChangedBlocks(context.Context,
	string, string, string, uint64) ([]cnsvolume.DiskArea, uint64, error) {
	return nil, 0, nil
}

func TestSyncVolumeCBTState(t *testing.T) {
	ctx := context.Background()
	ns := "test-ns"
	enabled := cbtconfigv1alpha1.CBTStateActive
	disabled := cbtconfigv1alpha1.CBTStateInactive

	t.Run("CBT active calls Set for each volume", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, &enabled))
		vm := &cbtFlagsMockVolumeManager{}
		SyncVolumeCBTState(ctx, c, ns, vm, "vol-1", "vol-2")
		assert.True(t, vm.setCalled)
		assert.False(t, vm.clearCalled)
	})

	t.Run("CBT inactive calls Clear for each volume", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, &disabled))
		vm := &cbtFlagsMockVolumeManager{}
		SyncVolumeCBTState(ctx, c, ns, vm, "vol-1", "vol-2")
		assert.False(t, vm.setCalled)
		assert.True(t, vm.clearCalled)
	})

	t.Run("not configured skips flag change", func(t *testing.T) {
		c := cbtTestClient(t) // no CBTConfig CR
		vm := &cbtFlagsMockVolumeManager{}
		SyncVolumeCBTState(ctx, c, ns, vm, "vol-1")
		assert.False(t, vm.setCalled)
		assert.False(t, vm.clearCalled)
	})

	t.Run("Set error is swallowed after logging", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, &enabled))
		vm := &cbtFlagsMockVolumeManager{setErr: errors.New("set failed")}
		SyncVolumeCBTState(ctx, c, ns, vm, "vol-1")
		assert.True(t, vm.setCalled)
		assert.False(t, vm.clearCalled)
	})

	t.Run("Clear error is swallowed after logging", func(t *testing.T) {
		c := cbtTestClient(t, cbtConfigObject("default", ns, &disabled))
		vm := &cbtFlagsMockVolumeManager{clearErr: errors.New("clear failed")}
		SyncVolumeCBTState(ctx, c, ns, vm, "vol-1")
		assert.False(t, vm.setCalled)
		assert.True(t, vm.clearCalled)
	})
}

func TestBuildGuestPvcAnnotation(t *testing.T) {
	tests := []struct {
		name         string
		clusterID    string
		clusterName  string
		pvcName      string
		pvcNamespace string
		volumeName   string
		expected     map[string]string
	}{
		{
			name:         "all fields populated",
			clusterID:    "tkc-uid",
			clusterName:  "my-tkc",
			pvcName:      "guest-pvc",
			pvcNamespace: "guest-ns",
			volumeName:   "sv-pv-1",
			expected: map[string]string{
				GuestClusterAnnotKeyClusterID:   "tkc-uid",
				GuestClusterAnnotKeyClusterName: "my-tkc",
				GuestClusterAnnotKeyName:        "guest-pvc",
				GuestClusterAnnotKeyNamespace:   "guest-ns",
				GuestClusterAnnotKeyVolumeName:  "sv-pv-1",
			},
		},
		{
			name:        "only cluster identity (provision time, before binding)",
			clusterID:   "tkc-uid",
			clusterName: "my-tkc",
			expected: map[string]string{
				GuestClusterAnnotKeyClusterID:   "tkc-uid",
				GuestClusterAnnotKeyClusterName: "my-tkc",
			},
		},
		{
			name:         "empty optional fields are omitted, not blank",
			clusterID:    "tkc-uid",
			clusterName:  "my-tkc",
			pvcName:      "",
			pvcNamespace: "guest-ns",
			volumeName:   "",
			expected: map[string]string{
				GuestClusterAnnotKeyClusterID:   "tkc-uid",
				GuestClusterAnnotKeyClusterName: "my-tkc",
				GuestClusterAnnotKeyNamespace:   "guest-ns",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildGuestPvcAnnotation(tt.clusterID, tt.clusterName,
				tt.pvcName, tt.pvcNamespace, tt.volumeName)
			assert.Equal(t, tt.expected, got)
			// No optional key should ever carry a blank value.
			for k, v := range got {
				assert.NotEmpty(t, v, "key %q must not have a blank value", k)
			}
		})
	}
}

func TestBuildGuestSnapshotAnnotation(t *testing.T) {
	tests := []struct {
		name              string
		clusterName       string
		snapshotName      string
		snapshotNamespace string
		vscName           string
		expected          map[string]string
	}{
		{
			name:              "all fields populated",
			clusterName:       "my-tkc",
			snapshotName:      "guest-snap",
			snapshotNamespace: "guest-ns",
			vscName:           "snapcontent-abc",
			expected: map[string]string{
				GuestClusterAnnotKeyClusterName: "my-tkc",
				GuestClusterAnnotKeyName:        "guest-snap",
				GuestClusterAnnotKeyNamespace:   "guest-ns",
				GuestClusterAnnotKeyVSCName:     "snapcontent-abc",
			},
		},
		{
			name:        "only cluster name",
			clusterName: "my-tkc",
			expected: map[string]string{
				GuestClusterAnnotKeyClusterName: "my-tkc",
			},
		},
		{
			name:              "empty optional fields are omitted, not blank",
			clusterName:       "my-tkc",
			snapshotName:      "guest-snap",
			snapshotNamespace: "",
			vscName:           "",
			expected: map[string]string{
				GuestClusterAnnotKeyClusterName: "my-tkc",
				GuestClusterAnnotKeyName:        "guest-snap",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildGuestSnapshotAnnotation(tt.clusterName, tt.snapshotName,
				tt.snapshotNamespace, tt.vscName)
			assert.Equal(t, tt.expected, got)
			for k, v := range got {
				assert.NotEmpty(t, v, "key %q must not have a blank value", k)
			}
		})
	}
}
