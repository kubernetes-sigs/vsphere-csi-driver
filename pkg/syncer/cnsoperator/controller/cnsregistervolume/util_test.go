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

package cnsregistervolume

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

// TestIsDatastoreAccessibleToAZClusters tests the isDatastoreAccessibleToAZClusters function
// using standard Go testing framework to avoid conflicts with existing Ginkgo suites
func TestIsDatastoreAccessibleToAZClusters(t *testing.T) {
	// Initialize backOffDuration map to prevent nil map assignment panic
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)

	ctx := context.Background()
	mockVC := &cnsvsphere.VirtualCenter{}
	datastoreURL := "ds:///vmfs/volumes/test-datastore"

	t.Run("1 cluster per zone - datastore accessible to 1 cluster", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1"},
			"zone-b": {"cluster-b1"},
		}

		patches := gomonkey.ApplyFunc(cnsvsphere.GetCandidateDatastoresInCluster,
			func(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
				includevSANDirectDatastores bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
				if clusterID == "cluster-a1" {
					return []*cnsvsphere.DatastoreInfo{
						{
							Info: &types.DatastoreInfo{
								Url: datastoreURL,
							},
						},
					}, []*cnsvsphere.DatastoreInfo{}, nil
				}
				return []*cnsvsphere.DatastoreInfo{}, []*cnsvsphere.DatastoreInfo{}, nil
			})
		defer patches.Reset()

		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.True(t, result)
	})

	t.Run("2 clusters per zone - datastore accessible to all clusters", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1", "cluster-a2"},
			"zone-b": {"cluster-b1", "cluster-b2"},
		}

		patches := gomonkey.ApplyFunc(cnsvsphere.GetCandidateDatastoresInCluster,
			func(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
				includevSANDirectDatastores bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
				return []*cnsvsphere.DatastoreInfo{
					{
						Info: &types.DatastoreInfo{
							Url: datastoreURL,
						},
					},
				}, []*cnsvsphere.DatastoreInfo{}, nil
			})
		defer patches.Reset()

		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.True(t, result)
	})

	t.Run("2 clusters per zone - datastore accessible to only 1 cluster", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1", "cluster-a2"},
			"zone-b": {"cluster-b1", "cluster-b2"},
		}

		patches := gomonkey.ApplyFunc(cnsvsphere.GetCandidateDatastoresInCluster,
			func(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
				includevSANDirectDatastores bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
				if clusterID == "cluster-a1" {
					return []*cnsvsphere.DatastoreInfo{
						{
							Info: &types.DatastoreInfo{
								Url: datastoreURL,
							},
						},
					}, []*cnsvsphere.DatastoreInfo{}, nil
				}
				return []*cnsvsphere.DatastoreInfo{}, []*cnsvsphere.DatastoreInfo{}, nil
			})
		defer patches.Reset()

		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.True(t, result)
	})

	t.Run("2 clusters per zone - datastore accessible to only 1 cluster in different zone",
		func(t *testing.T) {
			azClustersMap := map[string][]string{
				"zone-a": {"cluster-a1", "cluster-a2"},
				"zone-b": {"cluster-b1", "cluster-b2"},
			}

			patches := gomonkey.ApplyFunc(cnsvsphere.GetCandidateDatastoresInCluster,
				func(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
					includevSANDirectDatastores bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
					if clusterID == "cluster-b2" {
						return []*cnsvsphere.DatastoreInfo{
							{
								Info: &types.DatastoreInfo{
									Url: datastoreURL,
								},
							},
						}, []*cnsvsphere.DatastoreInfo{}, nil
					}
					return []*cnsvsphere.DatastoreInfo{}, []*cnsvsphere.DatastoreInfo{}, nil
				})
			defer patches.Reset()

			result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
			assert.True(t, result)
		})

	t.Run("datastore not accessible to any cluster", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1"},
			"zone-b": {"cluster-b1"},
		}

		patches := gomonkey.ApplyFunc(cnsvsphere.GetCandidateDatastoresInCluster,
			func(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
				includevSANDirectDatastores bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
				return []*cnsvsphere.DatastoreInfo{}, []*cnsvsphere.DatastoreInfo{}, nil
			})
		defer patches.Reset()

		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.False(t, result)
	})

	t.Run("error handling - should continue processing other clusters", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1"},
			"zone-b": {"cluster-b1"},
		}

		patches := gomonkey.ApplyFunc(cnsvsphere.GetCandidateDatastoresInCluster,
			func(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string,
				includevSANDirectDatastores bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
				if clusterID == "cluster-a1" {
					return nil, nil, fmt.Errorf("failed to get datastores for cluster-a1")
				}
				if clusterID == "cluster-b1" {
					return []*cnsvsphere.DatastoreInfo{
						{
							Info: &types.DatastoreInfo{
								Url: datastoreURL,
							},
						},
					}, []*cnsvsphere.DatastoreInfo{}, nil
				}
				return []*cnsvsphere.DatastoreInfo{}, []*cnsvsphere.DatastoreInfo{}, nil
			})
		defer patches.Reset()

		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.True(t, result)
	})

	t.Run("empty azClustersMap", func(t *testing.T) {
		azClustersMap := map[string][]string{}
		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.False(t, result)
	})

	t.Run("empty cluster lists in zones", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {},
			"zone-b": {},
		}
		result := isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL)
		assert.False(t, result)
	})
}

func TestGetPersistentVolumeSpec_VolumeModeEmpty(t *testing.T) {
	volumeName := "test-pv"
	volumeID := "volume-123"
	capacity := int64(1024)
	accessMode := v1.ReadWriteOnce
	var volumeMode v1.PersistentVolumeMode // empty value
	scName := "test-sc"

	claimRef := &v1.ObjectReference{
		Kind:      "PersistentVolumeClaim",
		Namespace: "default",
		Name:      "test-pvc",
	}

	pv := getPersistentVolumeSpec(
		volumeName,
		volumeID,
		capacity,
		accessMode,
		volumeMode,
		scName,
		claimRef,
	)

	if pv == nil {
		t.Fatalf("expected PersistentVolume, got nil")
	}

	if pv.Spec.VolumeMode == nil {
		t.Fatalf("expected VolumeMode to be set, got nil")
	}

	if *pv.Spec.VolumeMode != v1.PersistentVolumeFilesystem {
		t.Errorf(
			"expected VolumeMode to default to %q, got %q",
			v1.PersistentVolumeFilesystem,
			*pv.Spec.VolumeMode,
		)
	}

	if pv.Spec.PersistentVolumeSource.CSI == nil {
		t.Fatalf("expected CSI source to be set")
	}

	if pv.Spec.PersistentVolumeSource.CSI.FSType != "ext4" {
		t.Errorf(
			"expected FSType to be 'ext4' when VolumeMode is Filesystem, got %q",
			pv.Spec.PersistentVolumeSource.CSI.FSType,
		)
	}

}

func TestGetPersistentVolumeSpec_VolumeModeBlock(t *testing.T) {
	volumeName := "test-pv"
	volumeID := "volume-123"
	capacity := int64(1024)
	accessMode := v1.ReadWriteOnce
	volumeMode := v1.PersistentVolumeBlock
	scName := "test-sc"

	claimRef := &v1.ObjectReference{
		Kind:      "PersistentVolumeClaim",
		Namespace: "default",
		Name:      "test-pvc",
	}

	pv := getPersistentVolumeSpec(
		volumeName,
		volumeID,
		capacity,
		accessMode,
		volumeMode,
		scName,
		claimRef,
	)

	if pv == nil {
		t.Fatalf("expected PersistentVolume, got nil")
	}

	if pv.Spec.VolumeMode == nil {
		t.Fatalf("expected VolumeMode to be set, got nil")
	}

	if pv.Spec.PersistentVolumeSource.CSI == nil {
		t.Fatalf("expected CSI source to be set")
	}

	if pv.Spec.PersistentVolumeSource.CSI.FSType != "" {
		t.Errorf(
			"expected FSType to be empty when VolumeMode is Filesystem, got %q",
			pv.Spec.PersistentVolumeSource.CSI.FSType,
		)
	}

}

func TestGetPersistentVolumeSpec_VolumeModeFilesystem(t *testing.T) {
	volumeName := "test-pv"
	volumeID := "volume-123"
	capacity := int64(1024)
	accessMode := v1.ReadWriteOnce
	volumeMode := v1.PersistentVolumeFilesystem // empty value
	scName := "test-sc"

	claimRef := &v1.ObjectReference{
		Kind:      "PersistentVolumeClaim",
		Namespace: "default",
		Name:      "test-pvc",
	}

	pv := getPersistentVolumeSpec(
		volumeName,
		volumeID,
		capacity,
		accessMode,
		volumeMode,
		scName,
		claimRef,
	)

	if pv == nil {
		t.Fatalf("expected PersistentVolume, got nil")
	}

	if pv.Spec.VolumeMode == nil {
		t.Fatalf("expected VolumeMode to be set, got nil")
	}

	if *pv.Spec.VolumeMode != v1.PersistentVolumeFilesystem {
		t.Errorf(
			"expected VolumeMode to default to %q, got %q",
			v1.PersistentVolumeFilesystem,
			*pv.Spec.VolumeMode,
		)
	}

	if pv.Spec.PersistentVolumeSource.CSI == nil {
		t.Fatalf("expected CSI source to be set")
	}

	if pv.Spec.PersistentVolumeSource.CSI.FSType != "ext4" {
		t.Errorf(
			"expected FSType to be 'ext4' when VolumeMode is Filesystem, got %q",
			pv.Spec.PersistentVolumeSource.CSI.FSType,
		)
	}

}
