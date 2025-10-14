/*
Copyright 2025 The Kubernetes Authors.

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

package syncer

import (
	"context"
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

func TestSetFileShareAnnotationsOnPVC_Success(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewSimpleClientset(pv, pvc)

	// Create expected volume response with file share backing details
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv3AccessPointKey,
					Value: "192.168.1.100:/nfs/v3/path",
				},
				{
					Key:   common.Nfsv4AccessPointKey,
					Value: "192.168.1.100:/nfs/v4/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			assert.Equal(t, "test-volume-handle", queryFilter.VolumeIds[0].Id)
			assert.NotNil(t, querySelection)
			assert.Contains(t, querySelection.Names, string(cnstypes.QuerySelectionNameTypeBackingObjectDetails))
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_PVNotFound(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "non-existent-pv",
		},
	}

	// Create fake k8s client without the PV
	k8sClient := k8sfake.NewSimpleClientset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify no annotations were added
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_QueryVolumeError(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewSimpleClientset(pv, pvc)

	// Mock the QueryVolumeUtil function to return error using gomonkey
	expectedError := errors.New("query volume failed")
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return nil, expectedError
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions - The function should return the QueryVolume error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query volume failed")
}

func TestSetFileShareAnnotationsOnPVC_PVCUpdateError(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and configure it to fail on PVC update
	k8sClient := k8sfake.NewSimpleClientset(pv)
	k8sClient.PrependReactor("update", "persistentvolumeclaims",
		func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			return true, nil, errors.New("update failed")
		})

	// Create expected volume response with file share backing details
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv3AccessPointKey,
					Value: "192.168.1.100:/nfs/v3/path",
				},
				{
					Key:   common.Nfsv4AccessPointKey,
					Value: "192.168.1.100:/nfs/v4/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")

	// Verify annotations were set on the PVC object (even though update failed)
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_OnlyNFSv4AccessPoint(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewSimpleClientset(pv, pvc)

	// Create expected volume response with only NFSv4 access point
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv4AccessPointKey,
					Value: "192.168.1.100:/nfs/v4/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Equal(t, "192.168.1.100:/nfs/v4/path", pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_EmptyAccessPoints(t *testing.T) {
	ctx := context.Background()

	// Create test PVC
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pvc",
			Namespace:   "test-namespace",
			Annotations: make(map[string]string),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewSimpleClientset(pv, pvc)

	// Create expected volume response with empty access points
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Empty(t, pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}

func TestSetFileShareAnnotationsOnPVC_ExistingAnnotations(t *testing.T) {
	ctx := context.Background()

	// Create test PVC with existing annotations
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pvc",
			Namespace: "test-namespace",
			Annotations: map[string]string{
				"existing-annotation": "existing-value",
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "test-pv",
		},
	}

	// Create test PV
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pv",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					VolumeHandle: "test-volume-handle",
				},
			},
		},
	}

	// Create fake k8s client with PV and PVC
	k8sClient := k8sfake.NewSimpleClientset(pv, pvc)

	// Create expected volume response with file share backing details
	expectedVolume := &cnstypes.CnsVolume{
		VolumeId: cnstypes.CnsVolumeId{
			Id: "test-volume-handle",
		},
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			AccessPoints: []types.KeyValue{
				{
					Key:   common.Nfsv3AccessPointKey,
					Value: "192.168.1.100:/nfs/v3/path",
				},
			},
		},
	}

	// Mock the QueryVolumeUtil function using gomonkey
	patches := gomonkey.ApplyFunc(utils.QueryVolumeUtil,
		func(ctx context.Context, volManager volumes.Manager, queryFilter cnstypes.CnsQueryFilter,
			querySelection *cnstypes.CnsQuerySelection) (*cnstypes.CnsQueryResult, error) {
			return &cnstypes.CnsQueryResult{
				Volumes: []cnstypes.CnsVolume{*expectedVolume},
			}, nil
		})
	defer patches.Reset()

	// Call the function under test
	err := setFileShareAnnotationsOnPVC(ctx, k8sClient, nil, pvc)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, "existing-value", pvc.Annotations["existing-annotation"])
	assert.Equal(t, "192.168.1.100:/nfs/v3/path", pvc.Annotations[common.Nfsv3ExportPathAnnotationKey])
	assert.Empty(t, pvc.Annotations[common.Nfsv4ExportPathAnnotationKey])
}
