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
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const healthTestNS = "ns"

// guestNFSHealthPV returns a bound guest NFS PV on server, claimed by the PVC returned by boundPVC(claimName).
func guestNFSHealthPV(name, claimName, server string, mountOptions ...string) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{
				Driver:           common.VSphereCSIDriverName,
				VolumeHandle:     common.GuestNFSVolumeIDPrefix + server + "#export#" + name + "#" + name + "#",
				VolumeAttributes: map[string]string{"server": server, "share": "/export", "subdir": name},
			}},
			MountOptions: mountOptions,
			ClaimRef: &v1.ObjectReference{Namespace: healthTestNS, Name: claimName,
				UID: types.UID(claimName + "-uid")},
		},
		Status: v1.PersistentVolumeStatus{Phase: v1.VolumeBound},
	}
}

func boundPVC(name, pvName string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: healthTestNS, Name: name, UID: types.UID(name + "-uid")},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: pvName},
		Status:     v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound},
	}
}

func withHealth(pvc *v1.PersistentVolumeClaim, status string) *v1.PersistentVolumeClaim {
	pvc.Annotations = map[string]string{annVolumeHealth: status, annVolumeHealthTS: "Mon Jan  2 15:04:05 UTC 2006"}
	return pvc
}

// guestNFSHealthEnv runs syncGuestNFSVolumeHealth against a fake guest API server, with listers that are rebuilt
// from it after every pass, as the informers would be.
type guestNFSHealthEnv struct {
	t              *testing.T
	ctx            context.Context
	client         *k8sfake.Clientset
	pvs            []*v1.PersistentVolume
	syncer         *metadataSyncInformer
	clientRequests int
}

func newGuestNFSHealthEnv(t *testing.T, pvs []*v1.PersistentVolume,
	pvcs ...*v1.PersistentVolumeClaim) *guestNFSHealthEnv {
	t.Helper()
	ctx, _ := logger.GetNewContextWithLogger()
	objects := make([]runtime.Object, 0, len(pvcs))
	for _, pvc := range pvcs {
		objects = append(objects, pvc)
	}
	env := &guestNFSHealthEnv{t: t, ctx: ctx, client: k8sfake.NewClientset(objects...), pvs: pvs}
	env.refresh()
	orig := k8sNewClient
	k8sNewClient = func(context.Context) (clientset.Interface, error) {
		env.clientRequests++
		return env.client, nil
	}
	t.Cleanup(func() { k8sNewClient = orig })
	return env
}

func (e *guestNFSHealthEnv) refresh() {
	e.t.Helper()
	list, err := e.client.CoreV1().PersistentVolumeClaims("").List(e.ctx, metav1.ListOptions{})
	require.NoError(e.t, err)
	objects := make([]client.Object, 0, len(e.pvs)+len(list.Items))
	for _, pv := range e.pvs {
		objects = append(objects, pv)
	}
	for i := range list.Items {
		objects = append(objects, &list.Items[i])
	}
	pvLister, pvcLister, _ := newTestListers(e.t, objects...)
	e.syncer = &metadataSyncInformer{pvLister: pvLister, pvcLister: pvcLister,
		coCommonInterface: &mockCOCommonForFullSync{}}
}

func (e *guestNFSHealthEnv) run() {
	e.client.ClearActions()
	syncGuestNFSVolumeHealth(e.ctx, e.syncer)
	e.refresh()
}

func (e *guestNFSHealthEnv) annotations(pvcName string) map[string]string {
	e.t.Helper()
	pvc, err := e.client.CoreV1().PersistentVolumeClaims(healthTestNS).Get(e.ctx, pvcName, metav1.GetOptions{})
	require.NoError(e.t, err)
	return pvc.Annotations
}

func (e *guestNFSHealthEnv) health(pvcName string) string {
	return e.annotations(pvcName)[annVolumeHealth]
}

// updates returns the number of PVC updates since the start of the last run.
func (e *guestNFSHealthEnv) updates() int {
	n := 0
	for _, action := range e.client.Actions() {
		if action.Matches("update", "persistentvolumeclaims") {
			n++
		}
	}
	return n
}

// The PVC of a guest NFS volume follows its NFS server, probed for real against an in-process fake server,
// while other PVCs are left alone.
func TestGuestNFSVolumeHealthFollowsNFSService(t *testing.T) {
	server := nfsServerReplying(t, successReply)
	port := strconv.Itoa(server.port())
	nfsPV := guestNFSHealthPV("pv-nfs", "nfs-pvc", "127.0.0.1", "nfsvers=4.1", "port="+port)
	releasedPV := guestNFSHealthPV("pv-released", "released-pvc", "127.0.0.1", "port="+port)
	releasedPV.Status.Phase = v1.VolumeReleased
	blockPV := makeBlockPV("pv-block", "sv-pvc", "block-pvc", healthTestNS)
	env := newGuestNFSHealthEnv(t, []*v1.PersistentVolume{nfsPV, releasedPV, blockPV},
		boundPVC("nfs-pvc", "pv-nfs"), boundPVC("released-pvc", "pv-released"), boundPVC("block-pvc", "pv-block"))
	assertOthersUntouched := func() {
		t.Helper()
		assert.Empty(t, env.annotations("block-pvc"), "not a guest NFS volume")
		assert.Empty(t, env.annotations("released-pvc"), "PV is not bound")
	}

	env.run()
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("nfs-pvc"))
	timestamp := env.annotations("nfs-pvc")[annVolumeHealthTS]
	_, err := time.Parse(time.UnixDate, timestamp)
	require.NoError(t, err, "timestamp %q", timestamp)
	assert.EqualValues(t, 1, server.calls.Load())
	assert.Equal(t, 1, env.updates())
	assertOthersUntouched()

	env.run()
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("nfs-pvc"))
	assert.Zero(t, env.updates(), "an unchanged health is not written again")
	assert.Equal(t, timestamp, env.annotations("nfs-pvc")[annVolumeHealthTS])
	assert.EqualValues(t, 2, server.calls.Load(), "the server is probed on every pass")

	server.stop()
	env.run()
	assert.Equal(t, common.VolHealthStatusInaccessible, env.health("nfs-pvc"))
	assert.Equal(t, 1, env.updates())
	assertOthersUntouched()

	startFakeNFSServer(t, net.JoinHostPort("127.0.0.1", port), answerNullCalls(t, successReply))
	env.run()
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("nfs-pvc"))
	assert.Equal(t, 1, env.updates())
	assertOthersUntouched()
}

// Each distinct server and port is probed once per pass, and its result applies to all of its volumes.
func TestGuestNFSVolumeHealthProbesEachServiceOnce(t *testing.T) {
	var mu sync.Mutex
	probed := map[string]int{}
	withGuestNFSProbe(t, func(_ context.Context, address string) error {
		mu.Lock()
		defer mu.Unlock()
		probed[address]++
		if address == "nfs-down.example.com:2049" {
			return errors.New("connection refused")
		}
		return nil
	})
	env := newGuestNFSHealthEnv(t, []*v1.PersistentVolume{
		guestNFSHealthPV("pv-a1", "pvc-a1", "10.0.0.1"),
		guestNFSHealthPV("pv-a2", "pvc-a2", "10.0.0.1", "nfsvers=4.1"),
		guestNFSHealthPV("pv-b", "pvc-b", "10.0.0.1", "port=2050"),
		guestNFSHealthPV("pv-c", "pvc-c", "nfs-down.example.com"),
	}, boundPVC("pvc-a1", "pv-a1"), boundPVC("pvc-a2", "pv-a2"), boundPVC("pvc-b", "pv-b"),
		withHealth(boundPVC("pvc-c", "pv-c"), common.VolHealthStatusAccessible))

	env.run()
	assert.Equal(t, map[string]int{"10.0.0.1:2049": 1, "10.0.0.1:2050": 1, "nfs-down.example.com:2049": 1}, probed)
	for _, pvc := range []string{"pvc-a1", "pvc-a2", "pvc-b"} {
		assert.Equal(t, common.VolHealthStatusAccessible, env.health(pvc), pvc)
	}
	assert.Equal(t, common.VolHealthStatusInaccessible, env.health("pvc-c"))
	assert.Equal(t, 1, env.clientRequests)
}

// Volumes whose PVC or NFS service cannot be determined are left alone, and without any volume to check
// nothing is probed and no client is created.
func TestGuestNFSVolumeHealthSkipsVolumesItCannotCheck(t *testing.T) {
	probes := 0
	withGuestNFSProbe(t, func(context.Context, string) error {
		probes++
		return nil
	})
	recreated := guestNFSHealthPV("pv-recreated", "recreated-pvc", "10.0.0.1")
	recreated.Spec.ClaimRef.UID = "uid-of-the-deleted-pvc"
	noServer := guestNFSHealthPV("pv-no-server", "no-server-pvc", "10.0.0.1")
	delete(noServer.Spec.CSI.VolumeAttributes, "server")
	env := newGuestNFSHealthEnv(t, []*v1.PersistentVolume{
		guestNFSHealthPV("pv-deleted", "deleted-pvc", "10.0.0.1"),
		recreated,
		guestNFSHealthPV("pv-other", "other-pvc", "10.0.0.1"),
		guestNFSHealthPV("pv-bad-port", "bad-port-pvc", "10.0.0.1", "port=nfs"),
		noServer,
		makeBlockPV("pv-block", "sv-pvc", "block-pvc", healthTestNS),
	},
		boundPVC("recreated-pvc", "pv-recreated"),
		boundPVC("other-pvc", "pv-something-else"),
		withHealth(boundPVC("bad-port-pvc", "pv-bad-port"), common.VolHealthStatusAccessible),
		boundPVC("no-server-pvc", "pv-no-server"),
		boundPVC("block-pvc", "pv-block"))

	env.run()
	assert.Zero(t, probes)
	assert.Zero(t, env.clientRequests)
	assert.Empty(t, env.annotations("recreated-pvc"))
	assert.Empty(t, env.annotations("other-pvc"))
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("bad-port-pvc"))
	assert.Empty(t, env.annotations("no-server-pvc"))
	assert.Empty(t, env.annotations("block-pvc"))
}

// A server that does not answer within the pass's time bound leaves the health of its volumes unchanged, and
// does not hold the pass up.
func TestGuestNFSVolumeHealthBoundsPassTime(t *testing.T) {
	origTimeout := guestNFSHealthTimeout
	guestNFSHealthTimeout = 100 * time.Millisecond
	t.Cleanup(func() { guestNFSHealthTimeout = origTimeout })
	withGuestNFSProbe(t, func(ctx context.Context, address string) error {
		if address == "10.0.0.2:2049" {
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	})
	env := newGuestNFSHealthEnv(t, []*v1.PersistentVolume{
		guestNFSHealthPV("pv-hung", "hung-pvc", "10.0.0.1"),
		guestNFSHealthPV("pv-ok", "ok-pvc", "10.0.0.2"),
	}, withHealth(boundPVC("hung-pvc", "pv-hung"), common.VolHealthStatusAccessible), boundPVC("ok-pvc", "pv-ok"))

	start := time.Now()
	env.run()
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("hung-pvc"), "not probed, so not changed")
	assert.Equal(t, "Mon Jan  2 15:04:05 UTC 2006", env.annotations("hung-pvc")[annVolumeHealthTS])
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("ok-pvc"))
}

func TestGuestNFSVolumeHealthSkippedWhenContextDone(t *testing.T) {
	probes := 0
	withGuestNFSProbe(t, func(context.Context, string) error {
		probes++
		return nil
	})
	env := newGuestNFSHealthEnv(t, []*v1.PersistentVolume{guestNFSHealthPV("pv-nfs", "nfs-pvc", "10.0.0.1")},
		boundPVC("nfs-pvc", "pv-nfs"))
	ctx, cancel := context.WithCancel(env.ctx)
	cancel()
	syncGuestNFSVolumeHealth(ctx, env.syncer)
	assert.Zero(t, probes)
	assert.Zero(t, env.clientRequests)
	assert.Empty(t, env.annotations("nfs-pvc"))
}

// Full sync reports guest NFS volume health even when it fails early for want of the Supervisor, which guest
// NFS volumes do not depend on.
func TestPvcsiFullSyncSetsGuestNFSVolumeHealthWithoutSupervisor(t *testing.T) {
	if _, err := os.Stat(cnsconfig.DefaultpvCSIProviderPath + "/namespace"); err == nil {
		t.Skip("this test needs the Supervisor namespace to be unavailable")
	}
	withGuestNFSProbe(t, func(context.Context, string) error { return nil })
	env := newGuestNFSHealthEnv(t, []*v1.PersistentVolume{guestNFSHealthPV("pv-nfs", "nfs-pvc", "10.0.0.1")},
		boundPVC("nfs-pvc", "pv-nfs"))

	require.Error(t, PvcsiFullSync(env.ctx, env.syncer))
	assert.Equal(t, common.VolHealthStatusAccessible, env.health("nfs-pvc"))
}

func conflictError() error {
	return apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, "nfs-pvc",
		errors.New("the object has been modified"))
}

func TestSetGuestNFSVolumeHealth(t *testing.T) {
	ctx, _ := logger.GetNewContextWithLogger()
	get := func(c *k8sfake.Clientset) (*v1.PersistentVolumeClaim, error) {
		return c.CoreV1().PersistentVolumeClaims(healthTestNS).Get(ctx, "nfs-pvc", metav1.GetOptions{})
	}

	t.Run("a conflict is retried with the latest PVC", func(t *testing.T) {
		stale := boundPVC("nfs-pvc", "pv-nfs")
		latest := stale.DeepCopy()
		latest.Annotations = map[string]string{"written-by": "someone else"}
		c := k8sfake.NewClientset(latest)
		conflicts := 0
		c.PrependReactor("update", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
			if conflicts > 0 {
				return false, nil, nil
			}
			conflicts++
			return true, nil, conflictError()
		})
		require.NoError(t, setGuestNFSVolumeHealth(ctx, c, stale, common.VolHealthStatusInaccessible))
		got, err := get(c)
		require.NoError(t, err)
		assert.Equal(t, common.VolHealthStatusInaccessible, got.Annotations[annVolumeHealth])
		assert.NotEmpty(t, got.Annotations[annVolumeHealthTS])
		assert.Equal(t, "someone else", got.Annotations["written-by"], "the retry starts from the latest PVC")
		assert.Empty(t, stale.Annotations, "the PVC from the informer cache is not modified")
	})

	t.Run("a conflict that persists is returned", func(t *testing.T) {
		c := k8sfake.NewClientset(boundPVC("nfs-pvc", "pv-nfs"))
		c.PrependReactor("update", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, conflictError()
		})
		err := setGuestNFSVolumeHealth(ctx, c, boundPVC("nfs-pvc", "pv-nfs"), common.VolHealthStatusAccessible)
		assert.True(t, apierrors.IsConflict(err), "got %v", err)
	})

	t.Run("a deleted PVC is left alone", func(t *testing.T) {
		c := k8sfake.NewClientset()
		require.NoError(t, setGuestNFSVolumeHealth(ctx, c, boundPVC("nfs-pvc", "pv-nfs"),
			common.VolHealthStatusAccessible))
		_, err := get(c)
		assert.True(t, apierrors.IsNotFound(err), "the PVC must not be recreated")
	})

	t.Run("a PVC deleted after a conflict is left alone", func(t *testing.T) {
		c := k8sfake.NewClientset(boundPVC("nfs-pvc", "pv-nfs"))
		c.PrependReactor("update", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
			_ = c.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"},
				healthTestNS, "nfs-pvc")
			return true, nil, conflictError()
		})
		require.NoError(t, setGuestNFSVolumeHealth(ctx, c, boundPVC("nfs-pvc", "pv-nfs"),
			common.VolHealthStatusAccessible))
	})

	t.Run("a PVC replaced after a conflict is left alone", func(t *testing.T) {
		replacement := boundPVC("nfs-pvc", "")
		replacement.UID = "uid-of-the-new-pvc"
		c := k8sfake.NewClientset(replacement)
		c.PrependReactor("update", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, conflictError()
		})
		require.NoError(t, setGuestNFSVolumeHealth(ctx, c, boundPVC("nfs-pvc", "pv-nfs"),
			common.VolHealthStatusAccessible))
		got, err := get(c)
		require.NoError(t, err)
		assert.Empty(t, got.Annotations)
	})

	t.Run("a missing timestamp is added", func(t *testing.T) {
		pvc := boundPVC("nfs-pvc", "pv-nfs")
		pvc.Annotations = map[string]string{annVolumeHealth: common.VolHealthStatusAccessible}
		c := k8sfake.NewClientset(pvc)
		require.NoError(t, setGuestNFSVolumeHealth(ctx, c, pvc, common.VolHealthStatusAccessible))
		got, err := get(c)
		require.NoError(t, err)
		assert.NotEmpty(t, got.Annotations[annVolumeHealthTS])
	})
}

// The volume health reconciler copies the health of Supervisor PVCs; a guest NFS volume has none, so the
// reconciler must neither track it nor queue it when it is bound (which would mark its PVC inaccessible).
func TestVolumeHealthReconcilerIgnoresGuestNFSVolumes(t *testing.T) {
	nfsPV := guestNFSHealthPV("pv-nfs", "nfs-pvc", "10.0.0.1")
	nfsHandle := nfsPV.Spec.CSI.VolumeHandle
	blockPV := makeBlockPV("pv-block", "sv-pvc", "block-pvc", healthTestNS)
	tkgClient := k8sfake.NewClientset(nfsPV, blockPV)
	svcClient := k8sfake.NewClientset()
	stopCh := make(chan struct{})
	defer close(stopCh)
	reconciler, err := NewVolumeHealthReconciler(tkgClient, svcClient, 0,
		informers.NewSharedInformerFactory(tkgClient, 0),
		informers.NewSharedInformerFactoryWithOptions(svcClient, 0, informers.WithNamespace("sv-ns")),
		workqueue.DefaultTypedControllerRateLimiter[any](), "sv-ns", stopCh)
	require.NoError(t, err)
	rc := reconciler.(*volumeHealthReconciler)
	defer rc.svcClaimQueue.ShutDown()
	assert.Empty(t, rc.volumeHandleToPVs.get(nfsHandle), "a bound guest NFS PV is not tracked at start")
	assert.Equal(t, []string{"pv-block"}, rc.volumeHandleToPVs.get("sv-pvc"))

	pending := func(pv *v1.PersistentVolume) *v1.PersistentVolume {
		pending := pv.DeepCopy()
		pending.Status.Phase = v1.VolumePending
		return pending
	}
	queued := rc.svcClaimQueue.Len()
	rc.tkgUpdatePV(pending(nfsPV), nfsPV)
	assert.Equal(t, queued, rc.svcClaimQueue.Len(), "binding a guest NFS PV queues nothing")
	assert.Empty(t, rc.volumeHandleToPVs.get(nfsHandle))
	rc.tkgUpdatePV(pending(blockPV), blockPV)
	assert.Equal(t, queued+1, rc.svcClaimQueue.Len(), "binding a block PV still queues its Supervisor PVC")
}
