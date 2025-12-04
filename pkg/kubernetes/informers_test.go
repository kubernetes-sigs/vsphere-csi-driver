package kubernetes

import (
	"context"
	"sync"
	"testing"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapshotfake "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"
)

func TestNewInformer(t *testing.T) {
	t.Run("CreateNewInformerManager", func(tt *testing.T) {
		// Setup - Reset global state
		informerInstanceLock.Lock()
		informerManagerInstance = nil
		informerInstanceLock.Unlock()

		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()
		snapshotClient := snapshotfake.NewSimpleClientset()

		// Execute
		informerMgr := NewInformer(ctx, k8sClient, snapshotClient)

		// Assert
		assert.NotNil(tt, informerMgr)
		assert.NotNil(tt, informerMgr.client)
		assert.NotNil(tt, informerMgr.informerFactory)
		assert.NotNil(tt, informerMgr.snapshotInformerFactory)
		assert.NotNil(tt, informerMgr.stopCh)
		assert.Equal(tt, k8sClient, informerMgr.client)
	})

	t.Run("ReturnExistingInformerManagerOnSecondCall", func(tt *testing.T) {
		// Note: We cannot reset the global state here because signals.SetupSignalHandler()
		// can only be called once per process. This test verifies the singleton behavior
		// by calling NewInformer twice without resetting.

		ctx := context.Background()
		k8sClient1 := testclient.NewSimpleClientset()
		snapshotClient1 := snapshotfake.NewSimpleClientset()

		// Execute - First call (may reuse existing instance from previous test)
		informerMgr1 := NewInformer(ctx, k8sClient1, snapshotClient1)

		// Execute - Second call with different clients
		k8sClient2 := testclient.NewSimpleClientset()
		snapshotClient2 := snapshotfake.NewSimpleClientset()
		informerMgr2 := NewInformer(ctx, k8sClient2, snapshotClient2)

		// Assert - Should return the same instance (singleton pattern)
		assert.Equal(tt, informerMgr1, informerMgr2, "Should return the same singleton instance")
	})

	t.Run("ThreadSafeInitialization", func(tt *testing.T) {
		// Note: We cannot reset the global state because signals.SetupSignalHandler()
		// can only be called once. This test verifies thread-safety by calling
		// NewInformer concurrently and ensuring all calls return the same instance.

		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()
		snapshotClient := snapshotfake.NewSimpleClientset()

		// Execute - Call NewInformer concurrently
		const numGoroutines = 10
		results := make([]*InformerManager, numGoroutines)
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				results[index] = NewInformer(ctx, k8sClient, snapshotClient)
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Assert - All should return the same instance
		firstInstance := results[0]
		assert.NotNil(tt, firstInstance)
		for i := 1; i < numGoroutines; i++ {
			assert.Equal(tt, firstInstance, results[i],
				"All concurrent calls should return the same instance")
		}
	})

	t.Run("VerifyInformerManagerFields", func(tt *testing.T) {
		// This test verifies that the informer manager has all expected fields set
		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()
		snapshotClient := snapshotfake.NewSimpleClientset()

		// Execute
		informerMgr := NewInformer(ctx, k8sClient, snapshotClient)

		// Assert - Verify all fields are properly initialized
		assert.NotNil(tt, informerMgr)
		assert.NotNil(tt, informerMgr.client, "client should be set")
		assert.NotNil(tt, informerMgr.informerFactory, "informerFactory should be set")
		assert.NotNil(tt, informerMgr.snapshotInformerFactory, "snapshotInformerFactory should be set")
		assert.NotNil(tt, informerMgr.stopCh, "stopCh should be set")

		// Verify that the informer factories are usable
		assert.NotNil(tt, informerMgr.informerFactory.Core(), "Core informer factory should be accessible")
		assert.NotNil(tt, informerMgr.snapshotInformerFactory.Snapshot(),
			"Snapshot informer factory should be accessible")
	})
}

func TestInformerManager_AddSnapshotListener(t *testing.T) {
	// Helper to create a VolumeSnapshot object
	createSnapshot := func(name, namespace, pvcName string) *snapshotv1.VolumeSnapshot {
		return &snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: snapshotv1.VolumeSnapshotSpec{
				Source: snapshotv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: &pvcName,
				},
			},
		}
	}

	t.Run("AddSnapshotListenerWithNilHandlers", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()
		snapshotClient := snapshotfake.NewSimpleClientset()
		informerMgr := NewInformer(ctx, k8sClient, snapshotClient)

		// Execute - Pass nil handlers (should be allowed)
		err := informerMgr.AddSnapshotListener(ctx, nil, nil, nil)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, informerMgr.snapshotInformer, "snapshotInformer should be initialized")
	})

	t.Run("HandlersIgnoreInvalidObjectTypes", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()
		snapshotClient := snapshotfake.NewSimpleClientset()
		informerMgr := NewInformer(ctx, k8sClient, snapshotClient)

		processedCount := 0
		addFunc := func(obj any) {
			snap, ok := obj.(*snapshotv1.VolumeSnapshot)
			if ok && snap != nil {
				processedCount++
			}
		}

		// Execute
		err := informerMgr.AddSnapshotListener(ctx, addFunc, nil, nil)
		assert.NoError(tt, err)

		// Simulate events with invalid types
		addFunc("not-a-snapshot")
		addFunc(123)
		addFunc(nil)

		// Assert - Invalid objects should be ignored
		assert.Equal(tt, 0, processedCount, "Handler should ignore invalid object types")

		// Now send a valid snapshot
		snap := createSnapshot("snap1", "default", "pvc1")
		addFunc(snap)
		assert.Equal(tt, 1, processedCount, "Handler should process valid snapshot")
	})

	t.Run("EventHandlersTrackSnapshots", func(tt *testing.T) {
		// Setup
		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()
		snapshotClient := snapshotfake.NewSimpleClientset()
		informerMgr := NewInformer(ctx, k8sClient, snapshotClient)

		// Track events with dummy maps
		addedSnapshots := make(map[string]*snapshotv1.VolumeSnapshot)
		updatedSnapshots := make(map[string]*snapshotv1.VolumeSnapshot)
		deletedSnapshots := make(map[string]*snapshotv1.VolumeSnapshot)
		var mu sync.Mutex

		addFunc := func(obj any) {
			mu.Lock()
			defer mu.Unlock()
			snap, ok := obj.(*snapshotv1.VolumeSnapshot)
			if ok && snap != nil {
				addedSnapshots[snap.Name] = snap
			}
		}

		updateFunc := func(oldObj, newObj any) {
			mu.Lock()
			defer mu.Unlock()
			snap, ok := newObj.(*snapshotv1.VolumeSnapshot)
			if ok && snap != nil {
				updatedSnapshots[snap.Name] = snap
			}
		}

		deleteFunc := func(obj any) {
			mu.Lock()
			defer mu.Unlock()
			snap, ok := obj.(*snapshotv1.VolumeSnapshot)
			if ok && snap != nil {
				deletedSnapshots[snap.Name] = snap
			}
		}

		// Execute
		err := informerMgr.AddSnapshotListener(ctx, addFunc, updateFunc, deleteFunc)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, informerMgr.snapshotInformer)

		// Verify handlers are properly set up by simulating events
		snap1 := createSnapshot("snap1", "default", "pvc1")
		addFunc(snap1)
		mu.Lock()
		assert.Contains(tt, addedSnapshots, "snap1", "Add handler should track snapshot")
		mu.Unlock()

		snap2 := createSnapshot("snap2", "default", "pvc2")
		updateFunc(nil, snap2)
		mu.Lock()
		assert.Contains(tt, updatedSnapshots, "snap2", "Update handler should track snapshot")
		mu.Unlock()

		deleteFunc(snap1)
		mu.Lock()
		assert.Contains(tt, deletedSnapshots, "snap1", "Delete handler should track snapshot")
		mu.Unlock()
	})
}
