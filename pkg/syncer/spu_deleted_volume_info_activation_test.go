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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// countingInstall returns a CRD install function that fails the first failures calls.
func countingInstall(failures int32) (func(context.Context) error, *atomic.Int32) {
	var calls atomic.Int32
	return func(context.Context) error {
		if calls.Add(1) <= failures {
			return errors.New("CRD not established")
		}
		return nil
	}, &calls
}

func TestDeletedVolumeInfoInitializationDisabled(t *testing.T) {
	install, calls := countingInstall(0)
	informer, err := initializeDeletedVolumeInfo(context.Background(), nil, time.Millisecond, install)
	require.NoError(t, err)
	assert.Nil(t, informer, "capability-off must not start DVI observation")
	assert.Zero(t, calls.Load(), "capability-off must not install the CRD")
}

func TestDeletedVolumeInfoInitializationInstallsCRD(t *testing.T) {
	install, calls := countingInstall(0)
	informer, err := initializeDeletedVolumeInfo(context.Background(), testDVIDynamicClient(), time.Millisecond,
		install)
	require.NoError(t, err)
	require.NotNil(t, informer)
	assert.Equal(t, int32(1), calls.Load())
}

func TestDeletedVolumeInfoInitializationRetriesInstall(t *testing.T) {
	install, calls := countingInstall(3)
	informer, err := initializeDeletedVolumeInfo(context.Background(), testDVIDynamicClient(), time.Millisecond,
		install)
	require.NoError(t, err)
	require.NotNil(t, informer, "the reconciler must not start until the CRD is installed")
	assert.Equal(t, int32(4), calls.Load())
}

func TestDeletedVolumeInfoInitializationStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	install, calls := countingInstall(1 << 30)
	done := make(chan struct{})
	go func() {
		defer close(done)
		informer, err := initializeDeletedVolumeInfo(ctx, testDVIDynamicClient(), time.Millisecond, install)
		assert.Error(t, err)
		assert.Nil(t, informer, "an uninstalled CRD must never yield CVI-only accounting")
	}()
	require.Eventually(t, func() bool { return calls.Load() > 1 }, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("initialization ignored cancellation")
	}
}

func TestDeletedVolumeInfoInformerWatchesOnlyCSINamespace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testDVIDynamicClient()
	install, _ := countingInstall(0)
	informer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, install)
	require.NoError(t, err)
	go informer.Run(ctx.Done())
	require.Eventually(t, informer.HasSynced, 5*time.Second, time.Millisecond)
	var lists int
	for _, action := range c.Actions() {
		if action.GetVerb() != "list" {
			continue
		}
		lists++
		assert.Equal(t, common.GetCSINamespace(), action.GetNamespace())
		assert.Equal(t, deletedVolumeInfoGVR, action.(clienttesting.ListAction).GetResource())
	}
	assert.NotZero(t, lists)
}

func TestDeletedVolumeInfoStartupRepairsDVIOnlyAndEmptySPUs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testDVIDynamicClient(testDVI("retained", "gold", "", testPolicyID, 25))
	install, _ := countingInstall(0)
	dviInformer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, install)
	require.NoError(t, err)
	require.NotNil(t, dviInformer)
	empty := testSPU("1Gi")
	empty.Name = "silver-snapshot-usage"
	empty.Spec.StorageClassName = "silver"
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("100Mi"), empty)
	e.informer = testEmptyCVIInformer()
	e.r, err = newSPUSnapshotReconciler(e.client, e.informer, e.queue, dviInformer)
	require.NoError(t, err)
	go e.informer.Run(ctx.Done())
	done := make(chan struct{})
	go func() { e.r.run(ctx, 2); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("snapshot workers did not stop")
		}
	})
	require.Eventually(t, func() bool {
		got := &storagepolicyv1alpha3.StoragePolicyUsage{}
		if err := e.client.Get(ctx, client.ObjectKeyFromObject(empty), got); err != nil {
			return false
		}
		return got.Status.ResourceTypeLevelQuotaUsage.Used.IsZero() &&
			e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value() == 25
	}, 5*time.Second, 10*time.Millisecond,
		"startup must repair both retained-DVI usage and SPUs with no remaining contributors")
	assert.True(t, dviInformer.HasSynced())
}
