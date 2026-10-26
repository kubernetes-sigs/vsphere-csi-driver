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
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	storagepolicyv1alpha3 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha3"
)

func TestDeletedVolumeInfoAPIAvailability(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		available bool
		wantErr   bool
	}{
		{name: "served empty resource", available: true},
		{name: "resource not installed", err: apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), "")},
		{name: "forbidden", err: apierrors.NewForbidden(deletedVolumeInfoGVR.GroupResource(), "",
			errors.New("permission denied")), wantErr: true},
		{name: "unauthorized", err: apierrors.NewUnauthorized("authentication failed"), wantErr: true},
		{name: "timeout", err: apierrors.NewTimeoutError("timed out", 1), wantErr: true},
		{name: "unavailable", err: apierrors.NewServiceUnavailable("API down"), wantErr: true},
		{name: "transport failure", err: errors.New("connection reset"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testDVIDynamicClient()
			if tc.err != nil {
				c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.err
				})
			}
			available, err := deletedVolumeInfoAPIAvailable(context.Background(), c)
			assert.Equal(t, tc.available, available)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, c.Actions(), 1)
			list := c.Actions()[0].(clienttesting.ListActionImpl)
			assert.Empty(t, list.GetNamespace(), "probe all tenant system namespaces")
		})
	}
}

func TestDeletedVolumeInfoProbeUsesLimitedClusterList(t *testing.T) {
	httpClient := restfake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodGet, req.Method)
		assert.Equal(t, "/apis/protectionandrecovery.vmware.com/v1alpha1/deletedvolumeinfos", req.URL.Path)
		assert.Equal(t, "1", req.URL.Query().Get("limit"))
		deadline, ok := req.Context().Deadline()
		assert.True(t, ok, "probe requests must have a timeout")
		assert.LessOrEqual(t, time.Until(deadline), 30*time.Second)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"apiVersion":"protectionandrecovery.vmware.com/v1alpha1",` +
				`"kind":"DeletedVolumeInfoList","items":[]}`)),
		}, nil
	})
	c, err := dynamic.NewForConfigAndClient(&rest.Config{Host: "https://kubernetes.invalid"}, httpClient)
	require.NoError(t, err)
	available, err := deletedVolumeInfoAPIAvailable(context.Background(), c)
	require.NoError(t, err)
	assert.True(t, available, "the API is available even when its first page contains no records")
}

func TestDeletedVolumeInfoInitializationDisabled(t *testing.T) {
	var restarts atomic.Int32
	informer, err := initializeDeletedVolumeInfo(context.Background(), nil, time.Millisecond,
		time.Millisecond, func() { restarts.Add(1) })
	require.NoError(t, err)
	assert.Nil(t, informer)
	assert.Never(t, func() bool { return restarts.Load() != 0 }, 20*time.Millisecond, time.Millisecond,
		"capability-off must not activate or restart the syncer")
}

func TestDeletedVolumeInfoStartupServedDoesNotPoll(t *testing.T) {
	for _, withDVI := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty resource", true: "existing DVI"}[withDVI], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var objects []runtime.Object
			if withDVI {
				objects = append(objects, testDVI("retained", "gold", "", testPolicyID, 25, true))
			}
			c := testDVIDynamicClient(objects...)
			var restarts atomic.Int32
			informer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Millisecond,
				func() { restarts.Add(1) })
			require.NoError(t, err)
			require.NotNil(t, informer)
			assert.False(t, informer.HasSynced(), "the reconciler starts and synchronizes observation")
			assert.Never(t, func() bool { return len(c.Actions()) != 1 || restarts.Load() != 0 },
				20*time.Millisecond, time.Millisecond, "served API needs neither availability polling nor restart")
		})
	}
}

func TestDeletedVolumeInfoLateInstallationRestartsWithoutCVIDeletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var installed atomic.Bool
	var probes, restarts atomic.Int32
	c := testDVIDynamicClient()
	c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
		probes.Add(1)
		if !installed.Load() {
			return true, nil, apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), "")
		}
		return false, nil, nil
	})
	informer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, 5*time.Millisecond,
		func() { restarts.Add(1) })
	require.NoError(t, err)
	require.Nil(t, informer, "confirmed absence selects CVI-only accounting for this process")
	require.Eventually(t, func() bool { return probes.Load() >= 2 }, 5*time.Second, time.Millisecond)
	assert.Zero(t, restarts.Load())

	// No CVI, SPU, or DVI records are needed to notice that DPO established the API.
	installed.Store(true)
	require.Eventually(t, func() bool { return restarts.Load() == 1 }, 5*time.Second, time.Millisecond)
	lastProbe := probes.Load()
	assert.Never(t, func() bool { return probes.Load() != lastProbe || restarts.Load() != 1 },
		30*time.Millisecond, time.Millisecond, "a returning restart callback must not cause repeated restarts")

	// Model the next process startup: a served API selects normal DVI observation.
	restartedInformer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Millisecond,
		func() { restarts.Add(1) })
	require.NoError(t, err)
	require.NotNil(t, restartedInformer)
	assert.Equal(t, int32(1), restarts.Load())
}

func TestDeletedVolumeInfoAbsenceDoesNotImmediatelyPoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testDVIDynamicClient()
	c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), "")
	})
	var restarts atomic.Int32
	informer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Hour,
		func() { restarts.Add(1) })
	require.NoError(t, err)
	assert.Nil(t, informer)
	assert.Never(t, func() bool { return len(c.Actions()) != 1 || restarts.Load() != 0 },
		20*time.Millisecond, time.Millisecond, "the first background probe waits for its interval")
}

func TestDeletedVolumeInfoAbsentModeRemainsCVIOnlyUntilRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var installed atomic.Bool
	var probes, restarts atomic.Int32
	c := testDVIDynamicClient()
	c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
		probes.Add(1)
		if !installed.Load() {
			return true, nil, apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), "")
		}
		return false, nil, nil
	})
	dviInformer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Hour,
		func() { restarts.Add(1) })
	require.NoError(t, err)
	require.Nil(t, dviInformer)
	e := newSPUTestEnv(t, interceptor.Funcs{}, testSPU("30Mi"))
	e.r, err = newSPUSnapshotReconciler(e.client, e.informer, e.queue, dviInformer)
	require.NoError(t, err)
	cvi := testCVI("protected", "gold", "", testPolicyID, "30Mi", true, "")
	e.put(t, cvi)
	require.NoError(t, e.r.reconcile(ctx, testSPUKey))
	assert.Equal(t, int64(30*1024*1024), e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.Value())

	// DP establishes the API and persists the retained charge before releasing CVI.
	installed.Store(true)
	_, err = c.Resource(deletedVolumeInfoGVR).Namespace(testDVISystemNamespace).
		Create(ctx, testDVI("protected", "gold", "", testPolicyID, 30*1024*1024, true), metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, e.informer.GetIndexer().Delete(cvi))
	e.r.enqueue(cache.DeletedFinalStateUnknown{Key: cvi.GetNamespace() + "/" + cvi.GetName(), Obj: cvi})
	require.NoError(t, e.r.enqueueAllSnapshotSPUs(ctx))
	for e.queue.Len() > 0 {
		require.True(t, e.r.processNext(ctx))
	}
	assert.True(t, e.spu(t).Status.ResourceTypeLevelQuotaUsage.Used.IsZero(),
		"CVI-only mode accepts temporary undercount until discovery-triggered restart")
	assert.Equal(t, int32(1), probes.Load(), "CVI deletion and full sync must not probe or enable DVI")
	assert.Zero(t, restarts.Load(), "only the periodic availability check triggers restart")
	assert.Nil(t, e.r.dviInformer)
}

func TestDeletedVolumeInfoStartupErrorsRetryBeforeChoosingMode(t *testing.T) {
	for _, startupErr := range []error{
		apierrors.NewForbidden(deletedVolumeInfoGVR.GroupResource(), "", errors.New("denied")),
		apierrors.NewTimeoutError("timed out", 1),
		apierrors.NewServiceUnavailable("API down"),
		errors.New("connection reset"),
	} {
		t.Run(startupErr.Error(), func(t *testing.T) {
			for _, becomesAvailable := range []bool{false, true} {
				t.Run(map[bool]string{false: "then absent", true: "then served"}[becomesAvailable], func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var probes, restarts atomic.Int32
					c := testDVIDynamicClient()
					c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
						if probes.Add(1) == 1 {
							return true, nil, startupErr
						}
						if !becomesAvailable {
							return true, nil, apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), "")
						}
						return false, nil, nil
					})
					informer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Hour,
						func() { restarts.Add(1) })
					require.NoError(t, err)
					assert.Equal(t, becomesAvailable, informer != nil)
					assert.Equal(t, int32(2), probes.Load(), "errors must not select CVI-only mode")
					assert.Zero(t, restarts.Load(), "resolving startup discovery needs no restart")
				})
			}
		})
	}
}

func TestDeletedVolumeInfoUnresolvedStartupStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probed := make(chan struct{})
	c := testDVIDynamicClient()
	c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
		close(probed)
		return true, nil, apierrors.NewServiceUnavailable("API down")
	})
	var restarts atomic.Int32
	type result struct {
		informer cache.SharedIndexInformer
		err      error
	}
	completed := make(chan result, 1)
	go func() {
		informer, err := initializeDeletedVolumeInfo(ctx, c, time.Hour, time.Millisecond,
			func() { restarts.Add(1) })
		completed <- result{informer: informer, err: err}
	}()
	select {
	case <-probed:
	case <-time.After(5 * time.Second):
		t.Fatal("startup probe did not run")
	}
	select {
	case <-completed:
		t.Fatal("unresolved startup discovery must not select an accounting mode")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case got := <-completed:
		assert.Nil(t, got.informer)
		require.ErrorIs(t, got.err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("startup retry ignored cancellation")
	}
	assert.Zero(t, restarts.Load())
	require.Len(t, c.Actions(), 1)
}

func TestDeletedVolumeInfoStartupAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := testDVIDynamicClient()
	var restarts atomic.Int32
	informer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Millisecond,
		func() { restarts.Add(1) })
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, informer)
	assert.Empty(t, c.Actions())
	assert.Zero(t, restarts.Load())
}

func TestDeletedVolumeInfoPollingErrorsDoNotRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	responses := []error{
		apierrors.NewNotFound(deletedVolumeInfoGVR.GroupResource(), ""),
		apierrors.NewForbidden(deletedVolumeInfoGVR.GroupResource(), "", errors.New("denied")),
		apierrors.NewUnauthorized("authentication failed"),
		apierrors.NewTimeoutError("timed out", 1),
		apierrors.NewServiceUnavailable("API down"),
		errors.New("connection reset"),
	}
	var probes, restarts atomic.Int32
	var restartBeforeSuccess atomic.Bool
	c := testDVIDynamicClient()
	c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
		attempt := int(probes.Add(1))
		if restarts.Load() != 0 {
			restartBeforeSuccess.Store(true)
		}
		if attempt <= len(responses) {
			return true, nil, responses[attempt-1]
		}
		return false, nil, nil
	})
	done := make(chan struct{})
	go func() {
		pollDeletedVolumeInfoAPI(ctx, c, time.Millisecond, func() { restarts.Add(1) })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poller did not finish after API became available")
	}
	assert.Equal(t, int32(len(responses)+1), probes.Load())
	assert.Equal(t, int32(1), restarts.Load())
	assert.False(t, restartBeforeSuccess.Load())
}

func TestDeletedVolumeInfoPollingCancellation(t *testing.T) {
	for _, duringProbe := range []bool{false, true} {
		t.Run(map[bool]string{false: "before first interval", true: "during successful probe"}[duringProbe],
			func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				c := testDVIDynamicClient()
				interval := time.Hour
				if duringProbe {
					interval = time.Millisecond
					c.PrependReactor("list", "deletedvolumeinfos", func(clienttesting.Action) (bool, runtime.Object, error) {
						cancel()
						return false, nil, nil
					})
				} else {
					cancel()
				}
				var restarts atomic.Int32
				done := make(chan struct{})
				go func() {
					pollDeletedVolumeInfoAPI(ctx, c, interval, func() { restarts.Add(1) })
					close(done)
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("poller ignored cancellation")
				}
				assert.Zero(t, restarts.Load(), "shutdown must not request a discovery-triggered restart")
				if !duringProbe {
					assert.Empty(t, c.Actions())
				}
			})
	}
}

func TestDeletedVolumeInfoStartupRepairsDVIOnlyAndEmptySPUs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := testDVIDynamicClient(testDVI("retained", "gold", "", testPolicyID, 25, true))
	var restarts atomic.Int32
	dviInformer, err := initializeDeletedVolumeInfo(ctx, c, time.Millisecond, time.Millisecond,
		func() { restarts.Add(1) })
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
	assert.Zero(t, restarts.Load())
}
