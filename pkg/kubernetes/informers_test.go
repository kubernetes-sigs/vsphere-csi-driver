package kubernetes

import (
	"context"
	"sync"
	"testing"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sinformers "k8s.io/client-go/informers"
	testclient "k8s.io/client-go/kubernetes/fake"
	snapshotfake "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fakesnapshot"
)

func TestNewInformer(t *testing.T) {
	t.Run("CreateNewInformerManager", func(tt *testing.T) {
		// Reset global state.
		informerInstanceLock.Lock()
		informerManagerInstance = nil
		informerInstanceLock.Unlock()

		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()

		informerMgr := NewInformer(ctx, k8sClient)

		assert.NotNil(tt, informerMgr)
		assert.NotNil(tt, informerMgr.client)
		assert.NotNil(tt, informerMgr.informerFactory)
		assert.NotNil(tt, informerMgr.stopCh)
		assert.Equal(tt, k8sClient, informerMgr.client)
		assert.Nil(tt, informerMgr.snapshotInformerFactory,
			"snapshotInformerFactory should be nil until SetSnapshotInformerFactory is called")
	})

	t.Run("SnapshotFactorySetViaSetSnapshotInformerFactory", func(tt *testing.T) {
		im := &InformerManager{
			client:          testclient.NewSimpleClientset(),
			informerFactory: k8sinformers.NewSharedInformerFactory(testclient.NewSimpleClientset(), 0),
		}
		assert.Nil(tt, im.snapshotInformerFactory)

		im.SetSnapshotInformerFactory(snapshotfake.NewClientset())
		assert.NotNil(tt, im.snapshotInformerFactory,
			"snapshotInformerFactory should be set after SetSnapshotInformerFactory")
	})

	t.Run("SetSnapshotInformerFactoryNoOpIfAlreadySet", func(tt *testing.T) {
		im := &InformerManager{
			client:          testclient.NewSimpleClientset(),
			informerFactory: k8sinformers.NewSharedInformerFactory(testclient.NewSimpleClientset(), 0),
		}
		im.SetSnapshotInformerFactory(snapshotfake.NewClientset())
		first := im.snapshotInformerFactory

		// Second call should be a no-op.
		im.SetSnapshotInformerFactory(snapshotfake.NewClientset())
		assert.Equal(tt, first, im.snapshotInformerFactory, "factory should not be replaced on second call")
	})

	t.Run("ReturnExistingInformerManagerOnSecondCall", func(tt *testing.T) {
		ctx := context.Background()
		k8sClient1 := testclient.NewSimpleClientset()

		informerMgr1 := NewInformer(ctx, k8sClient1)

		k8sClient2 := testclient.NewSimpleClientset()
		informerMgr2 := NewInformer(ctx, k8sClient2)

		assert.Equal(tt, informerMgr1, informerMgr2, "Should return the same singleton instance")
	})

	t.Run("ThreadSafeInitialization", func(tt *testing.T) {
		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()

		const numGoroutines = 10
		results := make([]*InformerManager, numGoroutines)
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				results[index] = NewInformer(ctx, k8sClient)
				done <- true
			}(i)
		}

		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		firstInstance := results[0]
		assert.NotNil(tt, firstInstance)
		for i := 1; i < numGoroutines; i++ {
			assert.Equal(tt, firstInstance, results[i],
				"All concurrent calls should return the same instance")
		}
	})

	t.Run("VerifyInformerManagerFields", func(tt *testing.T) {
		ctx := context.Background()
		k8sClient := testclient.NewSimpleClientset()

		informerMgr := NewInformer(ctx, k8sClient)

		assert.NotNil(tt, informerMgr)
		assert.NotNil(tt, informerMgr.client, "client should be set")
		assert.NotNil(tt, informerMgr.informerFactory, "informerFactory should be set")
		assert.NotNil(tt, informerMgr.stopCh, "stopCh should be set")
		assert.NotNil(tt, informerMgr.informerFactory.Core(), "Core informer factory should be accessible")

		// Attach snapshot factory and verify it is usable.
		informerMgr.SetSnapshotInformerFactory(snapshotfake.NewClientset())
		assert.NotNil(tt, informerMgr.snapshotInformerFactory, "snapshotInformerFactory should be set")
		assert.NotNil(tt, informerMgr.snapshotInformerFactory.Snapshot(),
			"Snapshot informer factory should be accessible")
	})
}

func TestInformerManager_AddSnapshotListener_NilFactory(t *testing.T) {
	im := &InformerManager{
		client:          testclient.NewSimpleClientset(),
		informerFactory: k8sinformers.NewSharedInformerFactory(testclient.NewSimpleClientset(), 0),
	}

	err := im.AddSnapshotListener(context.Background(), nil, nil, nil)

	assert.Error(t, err, "AddSnapshotListener should fail when snapshot factory is nil")
}

func TestInformerManager_AddSnapshotListener(t *testing.T) {
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

	newInformerWithSnapshot := func(tt *testing.T) *InformerManager {
		tt.Helper()
		im := &InformerManager{
			client:          testclient.NewSimpleClientset(),
			informerFactory: k8sinformers.NewSharedInformerFactory(testclient.NewSimpleClientset(), 0),
		}
		im.SetSnapshotInformerFactory(snapshotfake.NewClientset())
		return im
	}

	t.Run("AddSnapshotListenerWithNilHandlers", func(tt *testing.T) {
		informerMgr := newInformerWithSnapshot(tt)

		err := informerMgr.AddSnapshotListener(context.Background(), nil, nil, nil)

		assert.NoError(tt, err)
		assert.NotNil(tt, informerMgr.snapshotInformer, "snapshotInformer should be initialized")
	})

	t.Run("HandlersIgnoreInvalidObjectTypes", func(tt *testing.T) {
		informerMgr := newInformerWithSnapshot(tt)

		processedCount := 0
		addFunc := func(obj any) {
			snap, ok := obj.(*snapshotv1.VolumeSnapshot)
			if ok && snap != nil {
				processedCount++
			}
		}

		err := informerMgr.AddSnapshotListener(context.Background(), addFunc, nil, nil)
		assert.NoError(tt, err)

		addFunc("not-a-snapshot")
		addFunc(123)
		addFunc(nil)
		assert.Equal(tt, 0, processedCount, "Handler should ignore invalid object types")

		snap := createSnapshot("snap1", "default", "pvc1")
		addFunc(snap)
		assert.Equal(tt, 1, processedCount, "Handler should process valid snapshot")
	})

	t.Run("EventHandlersTrackSnapshots", func(tt *testing.T) {
		informerMgr := newInformerWithSnapshot(tt)

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

		err := informerMgr.AddSnapshotListener(context.Background(), addFunc, updateFunc, deleteFunc)

		assert.NoError(tt, err)
		assert.NotNil(tt, informerMgr.snapshotInformer)

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
