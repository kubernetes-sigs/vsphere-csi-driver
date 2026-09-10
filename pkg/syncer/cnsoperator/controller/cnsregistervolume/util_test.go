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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/govmomi/vim25/types"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsstoragepolicyquotasv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
)

// TestIsDatastoreAccessibleToAZClusters tests the isDatastoreAccessibleToAZClusters function
// using standard Go testing framework to avoid conflicts with existing Ginkgo suites.
//
// The test swaps getCandidateDatastoresInClusterFn (a package-level variable) instead of
// using gomonkey, which is unreliable on arm64 when multiple tests share the same binary.
func TestIsDatastoreAccessibleToAZClusters(t *testing.T) {
	// Initialize backOffDuration map to prevent nil map assignment panic.
	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)

	ctx := context.Background()
	mockVC := &cnsvsphere.VirtualCenter{}
	datastoreURL := "ds:///vmfs/volumes/test-datastore"

	// restore restores getCandidateDatastoresInClusterFn to the original after each subtest.
	original := getCandidateDatastoresInClusterFn
	restore := func() { getCandidateDatastoresInClusterFn = original }

	t.Run("1 cluster per zone - datastore accessible to 1 cluster", func(t *testing.T) {
		t.Cleanup(restore)
		getCandidateDatastoresInClusterFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
			clusterID string, _ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			if clusterID == "cluster-a1" {
				return []*cnsvsphere.DatastoreInfo{
					{Info: &types.DatastoreInfo{Url: datastoreURL}},
				}, nil, nil
			}
			return nil, nil, nil
		}
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1"},
			"zone-b": {"cluster-b1"},
		}
		assert.True(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
	})

	t.Run("2 clusters per zone - datastore accessible to all clusters", func(t *testing.T) {
		t.Cleanup(restore)
		getCandidateDatastoresInClusterFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
			_ string, _ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			return []*cnsvsphere.DatastoreInfo{
				{Info: &types.DatastoreInfo{Url: datastoreURL}},
			}, nil, nil
		}
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1", "cluster-a2"},
			"zone-b": {"cluster-b1", "cluster-b2"},
		}
		assert.True(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
	})

	t.Run("2 clusters per zone - datastore accessible to only 1 cluster", func(t *testing.T) {
		t.Cleanup(restore)
		getCandidateDatastoresInClusterFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
			clusterID string, _ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			if clusterID == "cluster-a1" {
				return []*cnsvsphere.DatastoreInfo{
					{Info: &types.DatastoreInfo{Url: datastoreURL}},
				}, nil, nil
			}
			return nil, nil, nil
		}
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1", "cluster-a2"},
			"zone-b": {"cluster-b1", "cluster-b2"},
		}
		assert.True(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
	})

	t.Run("2 clusters per zone - datastore accessible to only 1 cluster in different zone",
		func(t *testing.T) {
			t.Cleanup(restore)
			getCandidateDatastoresInClusterFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
				clusterID string, _ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
				if clusterID == "cluster-b2" {
					return []*cnsvsphere.DatastoreInfo{
						{Info: &types.DatastoreInfo{Url: datastoreURL}},
					}, nil, nil
				}
				return nil, nil, nil
			}
			azClustersMap := map[string][]string{
				"zone-a": {"cluster-a1", "cluster-a2"},
				"zone-b": {"cluster-b1", "cluster-b2"},
			}
			assert.True(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
		})

	t.Run("datastore not accessible to any cluster", func(t *testing.T) {
		t.Cleanup(restore)
		getCandidateDatastoresInClusterFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
			_ string, _ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			return nil, nil, nil
		}
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1"},
			"zone-b": {"cluster-b1"},
		}
		assert.False(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
	})

	t.Run("error handling - should continue processing other clusters", func(t *testing.T) {
		t.Cleanup(restore)
		getCandidateDatastoresInClusterFn = func(_ context.Context, _ *cnsvsphere.VirtualCenter,
			clusterID string, _ bool) ([]*cnsvsphere.DatastoreInfo, []*cnsvsphere.DatastoreInfo, error) {
			if clusterID == "cluster-a1" {
				return nil, nil, fmt.Errorf("failed to get datastores for cluster-a1")
			}
			if clusterID == "cluster-b1" {
				return []*cnsvsphere.DatastoreInfo{
					{Info: &types.DatastoreInfo{Url: datastoreURL}},
				}, nil, nil
			}
			return nil, nil, nil
		}
		azClustersMap := map[string][]string{
			"zone-a": {"cluster-a1"},
			"zone-b": {"cluster-b1"},
		}
		assert.True(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
	})

	t.Run("empty azClustersMap", func(t *testing.T) {
		azClustersMap := map[string][]string{}
		assert.False(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
	})

	t.Run("empty cluster lists in zones", func(t *testing.T) {
		azClustersMap := map[string][]string{
			"zone-a": {},
			"zone-b": {},
		}
		assert.False(t, isDatastoreAccessibleToAZClusters(ctx, mockVC, azClustersMap, datastoreURL))
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
		resource.MustParse(strconv.FormatInt(capacity, 10)+"Mi"),
		accessMode,
		volumeMode,
		scName,
		claimRef,
		"default",
		"test-cr",
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
		resource.MustParse(strconv.FormatInt(capacity, 10)+"Mi"),
		accessMode,
		volumeMode,
		scName,
		claimRef,
		"default",
		"test-cr",
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
		resource.MustParse(strconv.FormatInt(capacity, 10)+"Mi"),
		accessMode,
		volumeMode,
		scName,
		claimRef,
		"default",
		"test-cr",
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

// newImmediateSC and newWFFCSC build StorageClass fixtures for a given storagePolicyID, with
// Immediate and WaitForFirstConsumer binding modes respectively.
func newImmediateSC(name, storagePolicyID string) *storagev1.StorageClass {
	mode := storagev1.VolumeBindingImmediate
	return &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: name},
		Parameters:        map[string]string{scParamStoragePolicyID: storagePolicyID},
		VolumeBindingMode: &mode,
	}
}

func newWFFCSC(name, storagePolicyID string) *storagev1.StorageClass {
	mode := storagev1.VolumeBindingWaitForFirstConsumer
	return &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: name},
		Parameters:        map[string]string{scParamStoragePolicyID: storagePolicyID},
		VolumeBindingMode: &mode,
	}
}

// storagePolicyQuotaFor builds the StoragePolicyQuota CR that isStoragePolicyAssignedToNamespace's
// stretched-supervisor path reads to determine whether a StorageClass is assigned to a namespace.
// ResourceQuota-based tracking was removed in 8.0.3 -- Supervisor now always uses StoragePolicyQuota
// CRs, so isPodVMOnStretchedSupervisorEnabled is effectively always true in practice, and these
// tests model that rather than the legacy ResourceQuota path.
func storagePolicyQuotaFor(namespace, policyID string,
	scNames ...string) *cnsstoragepolicyquotasv1alpha3.StoragePolicyQuota {
	statuses := make([]cnsstoragepolicyquotasv1alpha3.SCLevelQuotaStatus, 0, len(scNames))
	for _, scName := range scNames {
		statuses = append(statuses, cnsstoragepolicyquotasv1alpha3.SCLevelQuotaStatus{StorageClassName: scName})
	}
	return &cnsstoragepolicyquotasv1alpha3.StoragePolicyQuota{
		ObjectMeta: metav1.ObjectMeta{Name: policyID + "-storagepolicyquota", Namespace: namespace},
		Spec:       cnsstoragepolicyquotasv1alpha3.StoragePolicyQuotaSpec{StoragePolicyId: policyID},
		Status:     cnsstoragepolicyquotasv1alpha3.StoragePolicyQuotaStatus{SCLevelQuotaStatuses: statuses},
	}
}

// newFakeCtrlClient builds a controller-runtime fake client with the StoragePolicyQuota CRD
// registered and the given objects preloaded.
func newFakeCtrlClient(objs ...ctrlruntimeclient.Object) ctrlruntimeclient.Client {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = cnsstoragepolicyquotasv1alpha3.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_SingleMatch verifies the common,
// unambiguous case still resolves correctly: exactly one Immediate-mode StorageClass for the
// policy, assigned to the namespace.
func TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_SingleMatch(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	sc := newImmediateSC("sc-a", policyID)
	k8sclient := k8sfake.NewClientset(sc)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sc-a"))

	scName, err := getK8sStorageClassNameWithImmediateBindingModeForPolicy(context.TODO(), k8sclient, ctrlClient,
		policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "sc-a", scName)
}

// TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_IgnoresWFFCCompanion verifies that a
// WaitForFirstConsumer StorageClass sharing the same policy ID (e.g. the "-latebinding" companion
// wcpsvc creates alongside every Immediate StorageClass) does not count as an extra candidate.
func TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_IgnoresWFFCCompanion(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	sc := newImmediateSC("sc-a", policyID)
	wffc := newWFFCSC("sc-a-latebinding", policyID)
	k8sclient := k8sfake.NewClientset(sc, wffc)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sc-a", "sc-a-latebinding"))

	scName, err := getK8sStorageClassNameWithImmediateBindingModeForPolicy(context.TODO(), k8sclient, ctrlClient,
		policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "sc-a", scName)
}

// TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_Ambiguous is the regression test for
// the sgc1-raid-1 / sgc1-raid-1-mirroring-0 incident: two Immediate-mode StorageClasses share the
// same storagePolicyID (e.g. because the underlying storage policy was renamed and a new
// StorageClass was created under the new name without cleaning up the old one), and both are
// assigned to the namespace. This function only reaches this fallback when the caller had no
// stated preference (see resolveStorageClassNameForRegistration), so it must not fail the
// registration outright -- it picks deterministically (lexicographically smallest) rather than
// depending on StorageClass list iteration order.
func TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_Ambiguous(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	scA := newImmediateSC("sgc1-raid-1", policyID)
	scB := newImmediateSC("sgc1-raid-1-mirroring-0", policyID)
	k8sclient := k8sfake.NewClientset(scA, scB)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sgc1-raid-1", "sgc1-raid-1-mirroring-0"))

	scName, err := getK8sStorageClassNameWithImmediateBindingModeForPolicy(context.TODO(), k8sclient, ctrlClient,
		policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "sgc1-raid-1", scName)
}

// TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_NotAssigned verifies that a matching
// StorageClass which is not assigned to the namespace is treated as not found, not as a match.
func TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_NotAssigned(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	sc := newImmediateSC("sc-a", policyID)
	k8sclient := k8sfake.NewClientset(sc)
	// No StoragePolicyQuota CR naming "sc-a" in the namespace, i.e. not assigned.
	ctrlClient := newFakeCtrlClient()

	scName, err := getK8sStorageClassNameWithImmediateBindingModeForPolicy(context.TODO(), k8sclient, ctrlClient,
		policyID, namespace, true)
	assert.Error(t, err)
	assert.Empty(t, scName)
}

// TestResolveStorageClassNameForRegistration_ExplicitInstanceSpec verifies that
// instance.Spec.StorageClassName, when set, is always trusted directly without touching the PVC or
// re-deriving anything from the volume's storage policy.
func TestResolveStorageClassNameForRegistration_ExplicitInstanceSpec(t *testing.T) {
	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{StorageClassName: "explicit-sc"},
	}
	k8sclient := k8sfake.NewClientset()

	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, nil, instance,
		nil /* pvc */, "policy-1", "test-ns", true)
	assert.NoError(t, err)
	assert.Equal(t, "explicit-sc", scName)
}

// TestResolveStorageClassNameForRegistration_TrustsPVCOverAmbiguousDerivation is the core
// regression test for the CnsRegisterVolume registration failure: with two Immediate-mode
// StorageClasses sharing the same storagePolicyID (sgc1-raid-1 and sgc1-raid-1-mirroring-0), the
// PVC's own declared StorageClass must be used, instead of falling into the ambiguous
// policy-ID-based derivation that previously picked the wrong one.
func TestResolveStorageClassNameForRegistration_TrustsPVCOverAmbiguousDerivation(t *testing.T) {
	const policyID = "b306fd38-6c56-489a-bd65-83e8abd1022a"
	const namespace = "ns-virtualisering-m3ctn"

	scA := newImmediateSC("sgc1-raid-1", policyID)
	scB := newImmediateSC("sgc1-raid-1-mirroring-0", policyID)
	k8sclient := k8sfake.NewClientset(scA, scB)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sgc1-raid-1", "sgc1-raid-1-mirroring-0"))

	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "esx1-2e3b3b47"},
	}
	scNameOnPVC := "sgc1-raid-1"
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "esx1-2e3b3b47", Namespace: namespace},
		Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &scNameOnPVC},
	}

	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, ctrlClient, instance, pvc,
		policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "sgc1-raid-1", scName)
}

// TestResolveStorageClassNameForRegistration_PVCStorageClassPolicyMismatch verifies that if the
// PVC's declared StorageClass maps to a different storage policy than the volume actually has,
// resolution fails with a clear error rather than silently using either name.
func TestResolveStorageClassNameForRegistration_PVCStorageClassPolicyMismatch(t *testing.T) {
	const namespace = "test-ns"

	sc := newImmediateSC("sc-a", "policy-1")
	k8sclient := k8sfake.NewClientset(sc)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, "policy-1", "sc-a"))

	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "pvc-a"},
	}
	scNameOnPVC := "sc-a"
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a", Namespace: namespace},
		Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &scNameOnPVC},
	}

	// Volume's actual policy ("policy-2") differs from what "sc-a" maps to ("policy-1").
	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, ctrlClient, instance, pvc,
		"policy-2", namespace, true)
	assert.Error(t, err)
	assert.Empty(t, scName)
	assert.Contains(t, err.Error(), "but volume maps to")
}

// TestResolveStorageClassNameForRegistration_PVCStorageClassNotAssigned verifies that a PVC
// referencing a StorageClass whose policy is not assigned to the namespace is rejected, rather
// than trusted blindly.
func TestResolveStorageClassNameForRegistration_PVCStorageClassNotAssigned(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	sc := newImmediateSC("sc-a", policyID)
	k8sclient := k8sfake.NewClientset(sc)
	// No StoragePolicyQuota CR naming "sc-a" in the namespace, i.e. not assigned.
	ctrlClient := newFakeCtrlClient()

	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "pvc-a"},
	}
	scNameOnPVC := "sc-a"
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a", Namespace: namespace},
		Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &scNameOnPVC},
	}

	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, ctrlClient, instance, pvc,
		policyID, namespace, true)
	assert.Error(t, err)
	assert.Empty(t, scName)
	assert.Contains(t, err.Error(), "not assigned to namespace")
}

// TestResolveStorageClassNameForRegistration_FallsBackWhenNoPVC verifies that when there's no
// existing PVC yet (fresh registration) and no explicit instance.Spec.StorageClassName, resolution
// falls back to deriving the name from the volume's storage policy ID, as before.
func TestResolveStorageClassNameForRegistration_FallsBackWhenNoPVC(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	sc := newImmediateSC("sc-a", policyID)
	k8sclient := k8sfake.NewClientset(sc)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sc-a"))

	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "pvc-a"},
	}

	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, ctrlClient, instance,
		nil /* pvc */, policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "sc-a", scName)
}

// TestResolveStorageClassNameForRegistration_FallsBackWhenNoPVC_Ambiguous verifies that, with no
// PVC and no explicit instance.Spec.StorageClassName to disambiguate with, the fallback path
// resolves deterministically to one of the StorageClasses sharing the policy ID rather than
// failing the registration.
func TestResolveStorageClassNameForRegistration_FallsBackWhenNoPVC_Ambiguous(t *testing.T) {
	const policyID = "b306fd38-6c56-489a-bd65-83e8abd1022a"
	const namespace = "test-ns"

	scA := newImmediateSC("sgc1-raid-1", policyID)
	scB := newImmediateSC("sgc1-raid-1-mirroring-0", policyID)
	k8sclient := k8sfake.NewClientset(scA, scB)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sgc1-raid-1", "sgc1-raid-1-mirroring-0"))

	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "pvc-a"},
	}

	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, ctrlClient, instance,
		nil /* pvc */, policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "sgc1-raid-1", scName)
}

// TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_AmbiguousIsOrderIndependent verifies
// that the deterministic pick doesn't depend on the order StorageClasses happen to be created in
// or returned by the API server -- it must resolve to the same name regardless of which duplicate
// was created first.
func TestGetK8sStorageClassNameWithImmediateBindingModeForPolicy_AmbiguousIsOrderIndependent(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	// "zzz-later" was created after "aaa-first" but sorts after it lexicographically either way;
	// what matters is the object creation/list order below doesn't influence the outcome.
	scLater := newImmediateSC("zzz-later", policyID)
	scFirst := newImmediateSC("aaa-first", policyID)
	k8sclient := k8sfake.NewClientset(scLater, scFirst)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "zzz-later", "aaa-first"))

	scName, err := getK8sStorageClassNameWithImmediateBindingModeForPolicy(context.TODO(), k8sclient, ctrlClient,
		policyID, namespace, true)
	assert.NoError(t, err)
	assert.Equal(t, "aaa-first", scName)
}

// TestGetStoragePolicyIDForStorageClass_RejectsWaitForFirstConsumer verifies that a
// WaitForFirstConsumer StorageClass (e.g. a "-latebinding" companion) is rejected: that binding
// mode exists to delay dynamic provisioning until a consuming pod is scheduled, which has no
// meaning for CnsRegisterVolume's static registration of an already-provisioned volume.
func TestGetStoragePolicyIDForStorageClass_RejectsWaitForFirstConsumer(t *testing.T) {
	wffc := newWFFCSC("sc-a-latebinding", "policy-1")
	k8sclient := k8sfake.NewClientset(wffc)

	policyID, err := getStoragePolicyIDForStorageClass(context.TODO(), k8sclient, "sc-a-latebinding")
	assert.Error(t, err)
	assert.Empty(t, policyID)
	assert.Contains(t, err.Error(), "WaitForFirstConsumer")
}

// TestGetStoragePolicyIDForStorageClass_AcceptsImmediate verifies the common, unaffected case.
func TestGetStoragePolicyIDForStorageClass_AcceptsImmediate(t *testing.T) {
	sc := newImmediateSC("sc-a", "policy-1")
	k8sclient := k8sfake.NewClientset(sc)

	policyID, err := getStoragePolicyIDForStorageClass(context.TODO(), k8sclient, "sc-a")
	assert.NoError(t, err)
	assert.Equal(t, "policy-1", policyID)
}

// TestResolveStorageClassNameForRegistration_PVCStorageClassIsWaitForFirstConsumer verifies that a
// PVC declaring a WaitForFirstConsumer StorageClass is rejected by the PVC-trust tier, rather than
// being trusted blindly the way a plain policy-ID/namespace-assignment mismatch would be.
func TestResolveStorageClassNameForRegistration_PVCStorageClassIsWaitForFirstConsumer(t *testing.T) {
	const policyID = "policy-1"
	const namespace = "test-ns"

	wffc := newWFFCSC("sc-a-latebinding", policyID)
	k8sclient := k8sfake.NewClientset(wffc)
	ctrlClient := newFakeCtrlClient(storagePolicyQuotaFor(namespace, policyID, "sc-a-latebinding"))

	instance := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{PvcName: "pvc-a"},
	}
	scNameOnPVC := "sc-a-latebinding"
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a", Namespace: namespace},
		Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &scNameOnPVC},
	}

	scName, err := resolveStorageClassNameForRegistration(context.TODO(), k8sclient, ctrlClient, instance, pvc,
		policyID, namespace, true)
	assert.Error(t, err)
	assert.Empty(t, scName)
	assert.Contains(t, err.Error(), "WaitForFirstConsumer")
}
