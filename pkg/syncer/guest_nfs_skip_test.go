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

package syncer

import (
	"context"
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const guestNFSHandle = common.GuestNFSVolumeIDPrefix + "10.0.0.1/export/pvc-1234"

func makeGuestNFSPV(name string) *v1.PersistentVolume {
	pv := makeFVSPV(name, guestNFSHandle)
	pv.Spec.CSI.Driver = common.VSphereCSIDriverName
	pv.Spec.ClaimRef = &v1.ObjectReference{Name: "nfs-pvc", Namespace: "ns"}
	pv.Status.Phase = v1.VolumeBound
	return pv
}

func TestIsGuestNFSVolumeHandle(t *testing.T) {
	assert.True(t, common.IsGuestNFSVolumeHandle(guestNFSHandle))
	assert.False(t, common.IsGuestNFSVolumeHandle("2f1c9a1e-5d6b-4b0e-9f0f-0e0e0e0e0e0e"))
	assert.False(t, common.IsGuestNFSVolumeHandle("fv:ns:fv-1"))
	assert.False(t, common.IsGuestNFSVolumeHandle(""))
	assert.False(t, common.IsGuestNFSPersistentVolume(nil))
	assert.False(t, common.IsGuestNFSPersistentVolume(&v1.PersistentVolume{}), "PV without a CSI source")
	assert.True(t, common.IsGuestNFSPersistentVolume(makeGuestNFSPV("nfs")))
	otherDriver := makeGuestNFSPV("other")
	otherDriver.Spec.CSI.Driver = "nfs.csi.k8s.io"
	assert.False(t, common.IsGuestNFSPersistentVolume(otherDriver), "PV of another CSI driver")
}

// The informer handlers must skip a guest NFS volume whether or not the FVS gate is on.
func TestShouldSkipGuestMetadataPush(t *testing.T) {
	nfsPV := makeGuestNFSPV("pv-nfs")
	blockPV := makePVWithSC("pv-block", "vsan-default", "block-handle")
	blockPVC := makePVCWithSC("pvc-block", "ns", "vsan-default")
	fvsPVC := makePVCWithSC("pvc-fvs", "ns", common.StorageClassVsanFileServicePolicy)

	for _, fvsEnabled := range []bool{false, true} {
		withFVSEnabled(t, fvsEnabled)
		assert.True(t, shouldSkipGuestMetadataPush(nil, nfsPV), "PV only, fvs=%v", fvsEnabled)
		assert.True(t, shouldSkipGuestMetadataPush(blockPVC, nfsPV), "PVC and PV, fvs=%v", fvsEnabled)
		assert.False(t, shouldSkipGuestMetadataPush(blockPVC, blockPV), "block volume, fvs=%v", fvsEnabled)
	}
	withFVSEnabled(t, true)
	assert.True(t, shouldSkipGuestMetadataPush(fvsPVC, nil), "FVS skipping is preserved")
}

func TestExcludeGuestNFSPVs(t *testing.T) {
	block := makePVWithSC("pv-block", "vsan-default", "block-handle")
	got := excludeGuestNFSPVs([]*v1.PersistentVolume{makeGuestNFSPV("pv-nfs"), block})
	require.Len(t, got, 1)
	assert.Equal(t, "pv-block", got[0].Name)
	assert.Empty(t, excludeGuestNFSPVs(nil))
}

func newGuestNFSTestSyncer(t *testing.T, pvs []*v1.PersistentVolume, pvcs []*v1.PersistentVolumeClaim,
	pods []*v1.Pod) *metadataSyncInformer {
	t.Helper()
	pvIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	pvcIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, pv := range pvs {
		require.NoError(t, pvIdx.Add(pv))
	}
	for _, pvc := range pvcs {
		require.NoError(t, pvcIdx.Add(pvc))
	}
	for _, pod := range pods {
		require.NoError(t, podIdx.Add(pod))
	}
	configInfo := &cnsconfig.ConfigurationInfo{}
	configInfo.SetCfg(&cnsconfig.Config{GC: cnsconfig.GCConfig{TanzuKubernetesClusterUID: "cluster-uid"}})
	return &metadataSyncInformer{
		configInfo: configInfo,
		pvLister:   corelisters.NewPersistentVolumeLister(pvIdx),
		pvcLister:  corelisters.NewPersistentVolumeClaimLister(pvcIdx),
		podLister:  corelisters.NewPodLister(podIdx),
	}
}

// Full sync must create no CnsVolumeMetadata for a guest NFS PV, its PVC, or a pod that mounts it,
// while a block volume next to it is still synced.
func TestCreateCnsVolumeMetadataListSkipsGuestNFSVolumes(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	nfsPV := makeGuestNFSPV("pv-nfs")
	blockPV := makeBlockPV("pv-block", "sv-pvc", "block-pvc", "ns")
	pvcs := []*v1.PersistentVolumeClaim{
		{ObjectMeta: metav1.ObjectMeta{Name: "nfs-pvc", Namespace: "ns", UID: "nfs-pvc-uid"},
			Spec: v1.PersistentVolumeClaimSpec{VolumeName: "pv-nfs"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "block-pvc", Namespace: "ns", UID: "block-pvc-uid"},
			Spec: v1.PersistentVolumeClaimSpec{VolumeName: "pv-block"}},
	}
	podOn := func(name, claim string) *v1.Pod {
		return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID("uid-" + name)},
			Spec: v1.PodSpec{Volumes: []v1.Volume{{Name: "data", VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}}}}}
	}
	ms := newGuestNFSTestSyncer(t, []*v1.PersistentVolume{nfsPV, blockPV}, pvcs,
		[]*v1.Pod{podOn("nfs-pod", "nfs-pvc"), podOn("block-pod", "block-pvc")})

	orig := getPVsInBoundAvailableOrReleased
	defer func() { getPVsInBoundAvailableOrReleased = orig }()
	getPVsInBoundAvailableOrReleased = func(context.Context, *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
		return []*v1.PersistentVolume{nfsPV, blockPV}, nil
	}

	list := &cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{}
	require.NoError(t, createCnsVolumeMetadataList(ctx, ms, "sv-ns", list))

	names := map[string]bool{}
	for _, item := range list.Items {
		names[item.Spec.EntityName] = true
		assert.NotContains(t, item.Spec.VolumeNames, guestNFSHandle)
	}
	assert.True(t, names["pv-block"] && names["block-pvc"] && names["block-pod"],
		"block volume must still sync: %v", names)
	assert.False(t, names["pv-nfs"] || names["nfs-pvc"] || names["nfs-pod"], "no CR for guest NFS: %v", names)
}

// AddNodeAffinityRulesOnPV resolves each PV's volume handle to a Supervisor PVC; a guest NFS PV has
// none, so it must not trigger any Supervisor API call.
// (setGuestClusterDetailsOnSupervisorPVC applies the same excludeGuestNFSPVs filter, but builds a
// real REST client before its loop, so it is covered by TestExcludeGuestNFSPVs only.)
func TestAddNodeAffinityRulesOnPVSkipsGuestNFS(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	nfsPV := makeGuestNFSPV("pv-nfs")
	svClient := k8sfake.NewClientset()
	guestClient := k8sfake.NewClientset(nfsPV)
	ms := &metadataSyncInformer{supervisorClient: svClient}

	origK8s := k8sNewClient
	defer func() { k8sNewClient = origK8s }()
	k8sNewClient = func(context.Context) (clientset.Interface, error) { return guestClient, nil }
	origNS := cnsconfigGetSupervisorNamespace
	defer func() { cnsconfigGetSupervisorNamespace = origNS }()
	cnsconfigGetSupervisorNamespace = func(context.Context) (string, error) { return "sv-ns", nil }
	orig := getPVsInBoundAvailableOrReleased
	defer func() { getPVsInBoundAvailableOrReleased = orig }()
	getPVsInBoundAvailableOrReleased = func(context.Context, *metadataSyncInformer) ([]*v1.PersistentVolume, error) {
		return []*v1.PersistentVolume{nfsPV}, nil
	}

	AddNodeAffinityRulesOnPV(ctx, ms)
	assert.Empty(t, svClient.Actions(), "no Supervisor API call may be made for a guest NFS PV")
	got, err := guestClient.CoreV1().PersistentVolumes().Get(ctx, "pv-nfs", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Nil(t, got.Spec.NodeAffinity)
}

// A guest NFS VolumeSnapshotContent has no Supervisor VolumeSnapshot: snapshot full sync must not
// look one up or process it.
func TestProcessVolumeSnapshotContentsSkipsGuestNFSSnapshots(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	nfsVSC := makeGuestVSC("snapcontent-nfs", common.GuestNFSVolumeIDPrefix+"10.0.0.1/export/snap-1")
	// Even a Supervisor snapshot that carries the NFS handle as its name must not be processed.
	svClient := newFakeSnapshotClientset(makeSVVSWithAnnotation(*nfsVSC.Status.SnapshotHandle, nil))
	called := 0
	err := processVolumeSnapshotContents(ctx, []snapv1.VolumeSnapshotContent{*nfsVSC}, svClient, sdraNamespace,
		func(context.Context, *snapv1.VolumeSnapshotContent, *snapv1.VolumeSnapshot) error {
			called++
			return nil
		})
	require.NoError(t, err)
	assert.Zero(t, called)
}

// Deleting a guest NFS VolumeSnapshot must leave Supervisor snapshots alone.
func TestSnapshotDeletedRemoveAnnotationSkipsGuestNFSSnapshot(t *testing.T) {
	handle := common.GuestNFSVolumeIDPrefix + "10.0.0.1/export/snap-1"
	annot := map[string]string{common.AnnKeyGuestClusterSnapshot: `{"clusterName":"c","name":"n","namespace":"ns"}`}
	// A Supervisor snapshot that (artificially) carries the NFS handle as its name must stay untouched.
	svVS := makeSVVSWithAnnotation(handle, annot)
	rc := runSnapshotDeletedRemoveAnnotation(t, makeGuestVS(sdraGuestVSCN),
		[]*snapv1.VolumeSnapshotContent{makeGuestVSC(sdraGuestVSCN, handle)}, []*snapv1.VolumeSnapshot{svVS})
	assert.Equal(t, annot, getSVVS(t, rc, handle).Annotations)
}
