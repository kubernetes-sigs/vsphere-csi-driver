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
	"encoding/json"
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	unittestcommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

const (
	sdraNamespace = "vmware-system-csi"
	sdraGuestVSNs = "guest-ns"
	sdraGuestVSN  = "my-guest-snap"
	sdraGuestVSCN = "snapcontent-abc123"
	sdraSvVSName  = "sv-snap-abc123"
)

// buildSDAScheme returns a runtime.Scheme with snapv1 registered.
func buildSDAScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, snapv1.AddToScheme(s))
	return s
}

// runSnapshotDeletedRemoveAnnotation is the test harness for snapshotDeletedRemoveAnnotation.
// It pre-populates fake snapshot clients with the provided objects, invokes the function, and
// returns the controller-runtime fake client so callers can assert the resulting state.
func runSnapshotDeletedRemoveAnnotation(
	t *testing.T,
	guestVS *snapv1.VolumeSnapshot,
	guestVSCs []*snapv1.VolumeSnapshotContent,
	supervisorVSs []*snapv1.VolumeSnapshot,
) client.Client {
	t.Helper()

	scheme := buildSDAScheme(t)

	var guestObjs []runtime.Object
	for _, vsc := range guestVSCs {
		guestObjs = append(guestObjs, vsc.DeepCopy())
	}

	var supervisorSnapshotObjs []runtime.Object
	var runtimeObjs []client.Object
	for _, vs := range supervisorVSs {
		supervisorSnapshotObjs = append(supervisorSnapshotObjs, vs.DeepCopy())
		runtimeObjs = append(runtimeObjs, vs.DeepCopy())
	}

	guestClient := newFakeSnapshotClientset(guestObjs...)
	supervisorClient := newFakeSnapshotClientset(supervisorSnapshotObjs...)
	runtimeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObjs...).Build()

	snapshotDeletedRemoveAnnotation(context.Background(), guestVS, guestClient,
		supervisorClient, runtimeClient, sdraNamespace)

	return runtimeClient
}

// getSVVS fetches the supervisor VolumeSnapshot by name from the fake runtime client.
func getSVVS(t *testing.T, c client.Client, name string) *snapv1.VolumeSnapshot {
	t.Helper()
	vs := &snapv1.VolumeSnapshot{}
	require.NoError(t, c.Get(context.Background(),
		client.ObjectKey{Name: name, Namespace: sdraNamespace}, vs))
	return vs
}

// makeGuestVS builds a deleted guest VolumeSnapshot with the given bound VSC name.
func makeGuestVS(boundVSCName string) *snapv1.VolumeSnapshot {
	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: sdraGuestVSN, Namespace: sdraGuestVSNs},
	}
	if boundVSCName != "" {
		vs.Status = &snapv1.VolumeSnapshotStatus{
			BoundVolumeSnapshotContentName: &boundVSCName,
		}
	}
	return vs
}

// makeGuestVSC builds a guest VolumeSnapshotContent with the given SnapshotHandle.
func makeGuestVSC(name, snapshotHandle string) *snapv1.VolumeSnapshotContent {
	vsc := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: snapv1.VolumeSnapshotContentSpec{
			Driver:            common.VSphereCSIDriverName,
			VolumeSnapshotRef: v1.ObjectReference{Name: sdraGuestVSN, Namespace: sdraGuestVSNs},
		},
	}
	if snapshotHandle != "" {
		vsc.Status = &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: ptrTo(snapshotHandle),
			ReadyToUse:     ptrTo(true),
		}
	}
	return vsc
}

// makeSVVSWithAnnotation builds a supervisor VolumeSnapshot with an optional annotation.
func makeSVVSWithAnnotation(name string, annotations map[string]string) *snapv1.VolumeSnapshot {
	return &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   sdraNamespace,
			Annotations: annotations,
		},
	}
}

// TestSnapshotDeletedRemoveAnnotation tests snapshotDeletedRemoveAnnotation, the testable
// core of pvcsiSnapshotDeleted.
func TestSnapshotDeletedRemoveAnnotation(t *testing.T) {
	annotPresent := map[string]string{
		common.AnnKeyGuestClusterSnapshot: `{"clusterName":"my-tkc",
		"name":"my-guest-snap",
		"namespace":"guest-ns","volumeSnapshotContentName":"snapcontent-abc"}`,
		"other-annot": "keepme",
	}

	t.Run("identity fields cleared when guest VSC exists and has SnapshotHandle", func(t *testing.T) {
		guestVS := makeGuestVS(sdraGuestVSCN)
		guestVSC := makeGuestVSC(sdraGuestVSCN, sdraSvVSName)
		svVS := makeSVVSWithAnnotation(sdraSvVSName, annotPresent)

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, []*snapv1.VolumeSnapshotContent{guestVSC},
			[]*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		require.Contains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot)
		var annMap map[string]string
		require.NoError(t, json.Unmarshal([]byte(got.Annotations[common.AnnKeyGuestClusterSnapshot]), &annMap))
		assert.Equal(t, map[string]string{
			common.GuestClusterAnnotKeyClusterName: "my-tkc",
			common.GuestClusterAnnotKeyVSCName:     "snapcontent-abc",
		}, annMap, "only name/namespace cleared; clusterName and volumeSnapshotContentName preserved for audit")
		assert.Equal(t, "keepme", got.Annotations["other-annot"],
			"unrelated annotations must be preserved")
	})

	t.Run("no-op when annotation already absent on supervisor VS", func(t *testing.T) {
		guestVS := makeGuestVS(sdraGuestVSCN)
		guestVSC := makeGuestVSC(sdraGuestVSCN, sdraSvVSName)
		// Supervisor VS has no annotation to remove.
		svVS := makeSVVSWithAnnotation(sdraSvVSName, map[string]string{"other-annot": "keepme"})

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, []*snapv1.VolumeSnapshotContent{guestVSC},
			[]*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		assert.NotContains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot)
		assert.Equal(t, "keepme", got.Annotations["other-annot"])
	})

	t.Run("skips when guest VS has no Status", func(t *testing.T) {
		guestVS := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: sdraGuestVSN, Namespace: sdraGuestVSNs},
			// Status is nil
		}
		svVS := makeSVVSWithAnnotation(sdraSvVSName, annotPresent)

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, nil, []*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		assert.Contains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot,
			"annotation must not be touched when guest VS has no Status")
	})

	t.Run("skips when BoundVolumeSnapshotContentName is empty string", func(t *testing.T) {
		guestVS := makeGuestVS("") // empty bound VSC name
		svVS := makeSVVSWithAnnotation(sdraSvVSName, annotPresent)

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, nil, []*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		assert.Contains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot,
			"annotation must not be touched when BoundVolumeSnapshotContentName is empty")
	})

	t.Run("skips gracefully when guest VSC not found", func(t *testing.T) {
		guestVS := makeGuestVS(sdraGuestVSCN)
		svVS := makeSVVSWithAnnotation(sdraSvVSName, annotPresent)
		// No guest VSC seeded -> NotFound.

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, nil, []*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		assert.Contains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot,
			"annotation must not be removed when guest VSC is gone (supervisor likely already cleaned)")
	})

	t.Run("skips when guest VSC has no SnapshotHandle", func(t *testing.T) {
		guestVS := makeGuestVS(sdraGuestVSCN)
		// VSC with nil Status -> no SnapshotHandle.
		guestVSC := makeGuestVSC(sdraGuestVSCN, "")
		svVS := makeSVVSWithAnnotation(sdraSvVSName, annotPresent)

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, []*snapv1.VolumeSnapshotContent{guestVSC},
			[]*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		assert.Contains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot,
			"annotation must not be removed when guest VSC has no SnapshotHandle")
	})

	t.Run("skips gracefully when supervisor VS not found", func(t *testing.T) {
		guestVS := makeGuestVS(sdraGuestVSCN)
		guestVSC := makeGuestVSC(sdraGuestVSCN, sdraSvVSName)
		// No supervisor VS seeded -> Get returns NotFound.

		scheme := buildSDAScheme(t)
		guestClient := newFakeSnapshotClientset(guestVSC.DeepCopy())
		supervisorClient := newFakeSnapshotClientset()
		runtimeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		// Should not panic and should return without error.
		snapshotDeletedRemoveAnnotation(context.Background(), guestVS, guestClient,
			supervisorClient, runtimeClient, sdraNamespace)
		// No supervisor VS to assert on; the absence of a panic is the assertion.
	})

	t.Run("ownership label preserved when identity fields cleared", func(t *testing.T) {
		guestVS := makeGuestVS(sdraGuestVSCN)
		guestVSC := makeGuestVSC(sdraGuestVSCN, sdraSvVSName)
		svVS := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sdraSvVSName,
				Namespace: sdraNamespace,
				Annotations: map[string]string{
					common.AnnKeyGuestClusterSnapshot: `{"clusterName":"my-tkc",
					"name":"my-guest-snap",
					"namespace":"guest-ns","volumeSnapshotContentName":"snapcontent-abc"}`,
				},
				Labels: map[string]string{
					"my-tkc/TKGService": "tkc-uid",
				},
			},
		}

		rc := runSnapshotDeletedRemoveAnnotation(t, guestVS, []*snapv1.VolumeSnapshotContent{guestVSC},
			[]*snapv1.VolumeSnapshot{svVS})

		got := getSVVS(t, rc, sdraSvVSName)
		require.Contains(t, got.Annotations, common.AnnKeyGuestClusterSnapshot)
		var annMap map[string]string
		require.NoError(t, json.Unmarshal([]byte(got.Annotations[common.AnnKeyGuestClusterSnapshot]), &annMap))
		assert.NotContains(t, annMap, common.GuestClusterAnnotKeyName, "name must be cleared")
		assert.NotContains(t, annMap, common.GuestClusterAnnotKeyNamespace, "namespace must be cleared")
		assert.Equal(t, "my-tkc", annMap[common.GuestClusterAnnotKeyClusterName], "clusterName preserved")
		assert.Equal(t, "tkc-uid", got.Labels["my-tkc/TKGService"],
			"ownership label must be preserved for CNSSnapshotFinalizer cleanup path")
	})
}

// TestPvcsiSnapshotDeleted tests the FSS gate and namespace-resolution path in the outer
// pvcsiSnapshotDeleted function. The client-interaction paths are covered by
// TestSnapshotDeletedRemoveAnnotation above.
func TestPvcsiSnapshotDeleted(t *testing.T) {
	ctx := context.Background()

	makeSyncer := func(t *testing.T, fssEnabled bool) *metadataSyncInformer {
		t.Helper()
		co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
		require.NoError(t, err)

		if fssEnabled {
			require.NoError(t, co.(interface {
				EnableFSS(context.Context, string) error
			}).EnableFSS(ctx, common.ImprovedVolumeVisibility))
		}

		return &metadataSyncInformer{
			coCommonInterface: co,
			configInfo: &cnsconfig.ConfigurationInfo{
				Cfg: &cnsconfig.Config{
					GC: cnsconfig.GCConfig{
						Endpoint: "sv-endpoint",
						Port:     "443",
					},
				},
			},
		}
	}

	t.Run("returns early when FSS disabled", func(t *testing.T) {
		syncer := makeSyncer(t, false)
		guestVS := makeGuestVS(sdraGuestVSCN)
		// If the function does not return early it will try to create real k8s clients
		// and fail. A clean return here confirms the FSS gate works.
		pvcsiSnapshotDeleted(ctx, guestVS, syncer)
	})
}
