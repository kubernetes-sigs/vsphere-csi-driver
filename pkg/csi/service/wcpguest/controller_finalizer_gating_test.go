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

package wcpguest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	snap "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	testclient "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	fakesnapshotclient "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

// useFakeCO installs a fresh fake orchestrator for the duration of the test and returns it.
func useFakeCO(t *testing.T) *unittestcommon.FakeK8SOrchestrator {
	t.Helper()
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	require.NoError(t, err)
	oldCO := commonco.ContainerOrchestratorUtility
	commonco.ContainerOrchestratorUtility = co
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = oldCO })
	return co.(*unittestcommon.FakeK8SOrchestrator)
}

// TestCreateSnapshotSVFinalizerGating verifies that the CNS snapshot finalizer is added to the
// supervisor VolumeSnapshot only when the supervisor removes it on guest cluster deletion,
// i.e. a new pvCSI on an old supervisor must not add a finalizer nothing will ever remove.
// The guest-cluster ownership label is still added in both cases.
func TestCreateSnapshotSVFinalizerGating(t *testing.T) {
	tests := []struct {
		name               string
		svCleanupSupported bool
		wantFinalizer      bool
	}{
		{name: "old supervisor without cluster deletion cleanup", svCleanupSupported: false, wantFinalizer: false},
		{name: "new supervisor with cluster deletion cleanup", svCleanupSupported: true, wantFinalizer: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			// Cancelling also stops CreateVolume's PVC watch if the test fails early.
			defer cancel()
			co := useFakeCO(t)
			require.NoError(t, co.EnableFSS(ctx, common.SVPVCSnapshotProtectionFinalizer))
			if tt.svCleanupSupported {
				require.NoError(t, co.EnableFSS(ctx, common.SVPVCSnapshotFinalizerClusterDeletionCleanup))
			}

			const (
				clusterUID         = "tkc-uid"
				clusterName        = "my-tkc"
				guestClusterDist   = "TKGService"
				sourcePVCName      = "source-pvc"
				supervisorSnapName = "tkc-uid-mysnap" // clusterUID + "-" + req.Name[9:]
			)
			supervisorClient := testclient.NewClientset()
			snapshotClient := fakesnapshotclient.NewClientset()
			c := &controller{
				supervisorClient:            supervisorClient,
				supervisorSnapshotterClient: snapshotClient,
				supervisorNamespace:         testNamespace,
				tanzukubernetesClusterUID:   clusterUID,
				tanzukubernetesClusterName:  clusterName,
				guestClusterDist:            guestClusterDist,
			}
			_, err := supervisorClient.CoreV1().PersistentVolumeClaims(testNamespace).Create(ctx,
				&v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: sourcePVCName, Namespace: testNamespace}},
				metav1.CreateOptions{})
			require.NoError(t, err)

			// Make the created snapshot ready right away so CreateSnapshot does not poll.
			readyToUse := true
			creationTime := metav1.Now()
			restoreSize := resource.MustParse("1Gi")
			snapshotClient.PrependReactor("create", "volumesnapshots",
				func(action ktesting.Action) (bool, runtime.Object, error) {
					vs := action.(ktesting.CreateAction).GetObject().(*snap.VolumeSnapshot)
					vs.Status = &snap.VolumeSnapshotStatus{
						ReadyToUse: &readyToUse, CreationTime: &creationTime, RestoreSize: &restoreSize,
					}
					return false, nil, nil
				})

			_, err = c.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
				Name:           "snapshot-mysnap",
				SourceVolumeId: sourcePVCName,
				Parameters: map[string]string{
					common.AttributeSupervisorVolumeSnapshotClass: "test-snapshot-class",
				},
			})
			require.NoError(t, err)

			svVS, err := snapshotClient.SnapshotV1().VolumeSnapshots(testNamespace).
				Get(ctx, supervisorSnapName, metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, clusterUID, svVS.Labels[fmt.Sprintf("%s/%s", clusterName, guestClusterDist)],
				"guest-cluster ownership label must be added regardless of supervisor support")
			if tt.wantFinalizer {
				assert.Contains(t, svVS.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
			} else {
				assert.NotContains(t, svVS.Finalizers, cnsoperatortypes.CNSSnapshotFinalizer)
			}
		})
	}
}

// TestDeleteSnapshotRemovesSVFinalizerWhenFSSDisabled verifies that DeleteSnapshot removes the
// CNS snapshot finalizer from the supervisor VolumeSnapshot even when the protection FSS is
// disabled, so a finalizer added earlier never blocks the delete.
func TestDeleteSnapshotRemovesSVFinalizerWhenFSSDisabled(t *testing.T) {
	ctx := context.Background()
	co := useFakeCO(t)
	require.NoError(t, co.DisableFSS(ctx, common.SVPVCSnapshotProtectionFinalizer))
	require.NoError(t, co.DisableFSS(ctx, common.SVPVCSnapshotFinalizerClusterDeletionCleanup))

	const supervisorSnapName = "tkc-uid-mysnap"
	snapshotClient := fakesnapshotclient.NewClientset(&snap.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       supervisorSnapName,
			Namespace:  testNamespace,
			Finalizers: []string{cnsoperatortypes.CNSSnapshotFinalizer, "other-finalizer"},
		},
	})
	// The fake client deletes objects regardless of finalizers, so record what the
	// controller writes back before deleting.
	var updatedFinalizers []string
	updated := false
	snapshotClient.PrependReactor("update", "volumesnapshots",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			vs := action.(ktesting.UpdateAction).GetObject().(*snap.VolumeSnapshot)
			updatedFinalizers = append([]string(nil), vs.Finalizers...)
			updated = true
			return false, nil, nil
		})
	c := &controller{
		supervisorClient:            testclient.NewClientset(),
		supervisorSnapshotterClient: snapshotClient,
		supervisorNamespace:         testNamespace,
	}

	_, err := c.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: supervisorSnapName})
	require.NoError(t, err)

	require.True(t, updated, "supervisor VolumeSnapshot must be updated to remove the CNS finalizer")
	assert.NotContains(t, updatedFinalizers, cnsoperatortypes.CNSSnapshotFinalizer)
	assert.Contains(t, updatedFinalizers, "other-finalizer")
}

// TestCreateVolumeSVFinalizerGating verifies that the CNS PVC finalizer is added to the supervisor
// PVC only when the supervisor removes it on guest cluster deletion, so a new pvCSI on an old
// supervisor does not add a finalizer that nothing will ever remove.
func TestCreateVolumeSVFinalizerGating(t *testing.T) {
	tests := []struct {
		name               string
		svCleanupSupported bool
		wantFinalizer      bool
	}{
		{name: "old supervisor without cluster deletion cleanup", svCleanupSupported: false, wantFinalizer: false},
		{name: "new supervisor with cluster deletion cleanup", svCleanupSupported: true, wantFinalizer: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			co := useFakeCO(t)
			require.NoError(t, co.EnableFSS(ctx, common.SVPVCSnapshotProtectionFinalizer))
			if tt.svCleanupSupported {
				require.NoError(t, co.EnableFSS(ctx, common.SVPVCSnapshotFinalizerClusterDeletionCleanup))
			}

			supervisorClient := testclient.NewClientset()
			// The fake client does not replay events to a watch opened after the change, so the
			// PVC must only be bound once CreateVolume's watch is registered with the tracker.
			// Register the watch ourselves and signal only after the tracker has it.
			watchRegistered := make(chan struct{})
			var registerOnce sync.Once
			supervisorClient.PrependWatchReactor("persistentvolumeclaims",
				func(action ktesting.Action) (bool, watch.Interface, error) {
					w, err := supervisorClient.Tracker().Watch(action.GetResource(), action.GetNamespace())
					if err == nil {
						registerOnce.Do(func() { close(watchRegistered) })
					}
					return true, w, err
				})
			c := &controller{
				supervisorClient:    supervisorClient,
				supervisorNamespace: testNamespace,
			}
			req := &csi.CreateVolumeRequest{
				Name:          testVolumeName,
				CapacityRange: &csi.CapacityRange{RequiredBytes: 1 * common.GbInBytes},
				Parameters:    map[string]string{common.AttributeSupervisorStorageClass: testStorageClass},
				VolumeCapabilities: []*csi.VolumeCapability{{
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
					},
				}},
			}
			errCh := make(chan error, 1)
			go func() {
				_, err := c.CreateVolume(ctx, req)
				errCh <- err
			}()

			// CreateVolume creates the supervisor PVC and then waits for it to be bound.
			var pvc *v1.PersistentVolumeClaim
			require.Eventually(t, func() bool {
				p, err := supervisorClient.CoreV1().PersistentVolumeClaims(testNamespace).
					Get(ctx, testSupervisorPVCName, metav1.GetOptions{})
				if err != nil {
					return false
				}
				pvc = p
				return true
			}, 10*time.Second, 50*time.Millisecond, "supervisor PVC should be created")

			if tt.wantFinalizer {
				assert.Contains(t, pvc.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
			} else {
				assert.NotContains(t, pvc.Finalizers, cnsoperatortypes.CNSVolumeFinalizer)
			}

			// Wait for CreateVolume to start watching the PVC before binding it, otherwise the
			// Bound event is lost and CreateVolume blocks until its provision timeout.
			select {
			case <-watchRegistered:
			case <-time.After(10 * time.Second):
				t.Fatal("CreateVolume did not start watching the supervisor PVC")
			}

			// Bind the PVC so CreateVolume returns.
			pvc.Status.Phase = v1.ClaimBound
			_, err := supervisorClient.CoreV1().PersistentVolumeClaims(testNamespace).
				Update(ctx, pvc, metav1.UpdateOptions{})
			require.NoError(t, err)
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("CreateVolume did not return after the supervisor PVC was bound")
			}
		})
	}
}
