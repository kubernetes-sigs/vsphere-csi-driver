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

package namespace

import (
	"context"
	"errors"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metadatafake "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/tools/record"
	ccV1beta2 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		v1.AddToScheme, snapv1.AddToScheme, ccV1beta2.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("failed to build scheme: %v", err)
		}
	}
	return s
}

func newTestReconciler(c client.Client) (*ReconcileNamespace, *record.FakeRecorder) {
	recorder := record.NewFakeRecorder(10)
	backOffDurationMapMutex.Lock()
	backOffDuration = map[string]time.Duration{}
	backOffDurationMapMutex.Unlock()
	return &ReconcileNamespace{client: c, recorder: recorder}, recorder
}

func TestIsClusterDeleting(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name string
		cc   ccV1beta2.Cluster
		want bool
	}{
		{"provisioned cluster", ccV1beta2.Cluster{
			Status: ccV1beta2.ClusterStatus{Phase: string(ccV1beta2.ClusterPhaseProvisioned)}}, false},
		{"deletion timestamp set", ccV1beta2.Cluster{
			ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}}, true},
		{"phase deleting", ccV1beta2.Cluster{
			Status: ccV1beta2.ClusterStatus{Phase: string(ccV1beta2.ClusterPhaseDeleting)}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isClusterDeleting(&tt.cc); got != tt.want {
				t.Errorf("isClusterDeleting() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOwnedByGoneCluster(t *testing.T) {
	clusters := map[string]bool{"running": false, "deleting": true}
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"no labels", nil, false},
		{"unrelated label", map[string]string{"app": "x"}, false},
		{"running cluster", map[string]string{"running/TKGService": "uid"}, false},
		{"deleting cluster", map[string]string{"deleting/TKGService": "uid"}, true},
		{"cluster no longer exists", map[string]string{"gone/TKGService": "uid"}, true},
		{"label must end with TKGService", map[string]string{"gone/TKGServiceX": "uid"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ownedByGoneCluster(tt.labels, clusters); got != tt.want {
				t.Errorf("ownedByGoneCluster() = %v, want %v", got, tt.want)
			}
		})
	}
}

func pvcMeta(name string, deleting bool, finalizers []string, labels map[string]string) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns1",
			Finalizers: finalizers, Labels: labels},
	}
	if deleting {
		now := metav1.Now()
		m.DeletionTimestamp = &now
	}
	return m
}

func TestTerminatingObjects(t *testing.T) {
	fin := []string{cnsoperatortypes.CNSVolumeFinalizer}
	gone := map[string]string{"gone/TKGService": "uid"}
	live := map[string]string{"live/TKGService": "uid"}
	clusters := map[string]bool{"live": false}

	s := runtime.NewScheme()
	if err := metav1.AddMetaToScheme(s); err != nil {
		t.Fatal(err)
	}
	mdClient := metadatafake.NewSimpleMetadataClient(s,
		pvcMeta("match", true, fin, gone),
		pvcMeta("not-terminating", false, fin, gone),
		pvcMeta("no-finalizer", true, nil, gone),
		pvcMeta("live-cluster", true, fin, live),
		pvcMeta("no-label", true, fin, nil),
		pvcMeta("deleted-meanwhile", true, fin, gone),
	)

	var fetched []string
	get := func(_ context.Context, name string) (*v1.PersistentVolumeClaim, error) {
		fetched = append(fetched, name)
		if name == "deleted-meanwhile" {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "persistentvolumeclaims"}, name)
		}
		return &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}, nil
	}

	got, err := terminatingObjects(context.TODO(), mdClient, pvcResource, "ns1",
		cnsoperatortypes.CNSVolumeFinalizer, clusters, get)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "match" {
		t.Errorf("got %v, want only the \"match\" PVC", got)
	}
	// Only candidates that pass the metadata filter are fetched in full.
	if len(fetched) != 2 {
		t.Errorf("fetched %v, want exactly the two metadata matches", fetched)
	}

	t.Run("get error is returned", func(t *testing.T) {
		boom := errors.New("boom")
		_, err := terminatingObjects(context.TODO(), mdClient, pvcResource, "ns1",
			cnsoperatortypes.CNSVolumeFinalizer, clusters,
			func(context.Context, string) (*v1.PersistentVolumeClaim, error) { return nil, boom })
		if !errors.Is(err, boom) {
			t.Errorf("err = %v, want %v", err, boom)
		}
	})

	t.Run("nothing terminating", func(t *testing.T) {
		got, err := terminatingObjects(context.TODO(), mdClient, pvcResource, "other-ns",
			cnsoperatortypes.CNSVolumeFinalizer, clusters, get)
		if err != nil || len(got) != 0 {
			t.Errorf("got %v, err %v, want empty", got, err)
		}
	})
}

func TestRecordEvent(t *testing.T) {
	r, recorder := newTestReconciler(fake.NewClientBuilder().Build())
	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}}
	backOffDuration["ns1"] = time.Second

	recordEvent(context.TODO(), r, ns, v1.EventTypeWarning, "failed")
	recordEvent(context.TODO(), r, ns, v1.EventTypeWarning, "failed")
	if got := backOffDuration["ns1"]; got != 4*time.Second {
		t.Errorf("backoff after two warnings = %v, want 4s", got)
	}
	if len(recorder.Events) != 2 {
		t.Errorf("recorded %d events, want 2", len(recorder.Events))
	}

	recordEvent(context.TODO(), r, ns, v1.EventTypeNormal, "ok")
	if got := backOffDuration["ns1"]; got != time.Second {
		t.Errorf("backoff after normal event = %v, want reset to 1s", got)
	}
}

func TestReconcile(t *testing.T) {
	s := newTestScheme(t)
	now := metav1.Now()
	listErr := errors.New("list failed")
	failList := func(kind string) interceptor.Funcs {
		return interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList,
			opts ...client.ListOption) error {
			switch list.(type) {
			case *v1.PersistentVolumeClaimList:
				if kind == "pvc" {
					return listErr
				}
			case *snapv1.VolumeSnapshotList:
				if kind == "vs" {
					return listErr
				}
			case *ccV1beta2.ClusterList:
				if kind == "cluster" {
					return listErr
				}
			}
			return c.List(ctx, list, opts...)
		}}
	}
	terminatingNS := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1", DeletionTimestamp: &now,
		Finalizers: []string{"kubernetes"}}}
	activeNS := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "ns1"}}

	tests := []struct {
		name       string
		objs       []client.Object
		funcs      interceptor.Funcs
		wantRetry  bool
		wantErr    bool
		wantEvents int
	}{
		{name: "namespace not found"},
		{name: "namespace get error", objs: []client.Object{activeNS}, wantErr: true,
			funcs: interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object,
				...client.GetOption) error {
				return errors.New("get failed")
			}}},
		{name: "active namespace, cluster list fails: no retry, no event", objs: []client.Object{activeNS},
			funcs: failList("cluster")},
		{name: "deleting namespace, pvc list fails: requeue with event", objs: []client.Object{terminatingNS},
			funcs: failList("pvc"), wantRetry: true, wantEvents: 1},
		{name: "deleting namespace, snapshot list fails: requeue with event", objs: []client.Object{terminatingNS},
			funcs: failList("vs"), wantRetry: true, wantEvents: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tt.objs...).WithInterceptorFuncs(tt.funcs).Build()
			r, recorder := newTestReconciler(c)
			res, err := r.Reconcile(context.TODO(), req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if (res.RequeueAfter > 0) != tt.wantRetry {
				t.Errorf("RequeueAfter = %v, wantRetry %v", res.RequeueAfter, tt.wantRetry)
			}
			if len(recorder.Events) != tt.wantEvents {
				t.Errorf("recorded %d events, want %d", len(recorder.Events), tt.wantEvents)
			}
		})
	}
}
