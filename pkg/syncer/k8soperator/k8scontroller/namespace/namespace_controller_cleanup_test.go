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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ccV1beta2 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
)

const testNamespace = "ns1"

// fakeAPIServer serves just enough of the supervisor API for the namespace controller: metadata lists, gets
// and updates of PVCs and VolumeSnapshots, and gets of guest Clusters. Updates are recorded in updated.
type fakeAPIServer struct {
	t        *testing.T
	mu       sync.Mutex
	pvcs     map[string]*v1.PersistentVolumeClaim
	vss      map[string]*snapv1.VolumeSnapshot
	clusters map[string]*ccV1beta2.Cluster
	updated  []string // "pvc/<name>" or "vs/<name>" for each update
}

func (f *fakeAPIServer) writeJSON(w http.ResponseWriter, code int, obj any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(obj)
}

func (f *fakeAPIServer) notFound(w http.ResponseWriter, what string) {
	f.writeJSON(w, http.StatusNotFound, metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound,
		Message: what + " not found"})
}

// metadataList builds the PartialObjectMetadataList the metadata client expects.
func metadataList(apiVersion, kind string, objs ...metav1.ObjectMeta) map[string]any {
	items := []map[string]any{}
	for _, o := range objs {
		items = append(items, map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": o})
	}
	return map[string]any{"apiVersion": "meta.k8s.io/v1", "kind": "PartialObjectMetadataList",
		"metadata": map[string]any{}, "items": items}
}

func (f *fakeAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	pvcBase := "/api/v1/namespaces/" + testNamespace + "/persistentvolumeclaims"
	vsBase := "/apis/snapshot.storage.k8s.io/v1/namespaces/" + testNamespace + "/volumesnapshots"
	clBase := "/apis/cluster.x-k8s.io/v1beta2/namespaces/" + testNamespace + "/clusters"
	switch {
	// Discovery, needed by the controller-runtime client used for Cluster objects.
	case path == "/api":
		f.writeJSON(w, 200, metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
	case path == "/apis":
		f.writeJSON(w, 200, metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
			Groups: []metav1.APIGroup{{Name: "cluster.x-k8s.io",
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: "cluster.x-k8s.io/v1beta2", Version: "v1beta2"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "cluster.x-k8s.io/v1beta2", Version: "v1beta2"}}}})
	case path == "/apis/cluster.x-k8s.io/v1beta2":
		f.writeJSON(w, 200, metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: "cluster.x-k8s.io/v1beta2",
			APIResources: []metav1.APIResource{{Name: "clusters", Kind: "Cluster", Namespaced: true,
				Verbs: metav1.Verbs{"get", "list"}}}})
	case path == pvcBase && r.Method == http.MethodGet:
		var metas []metav1.ObjectMeta
		for _, p := range f.pvcs {
			metas = append(metas, p.ObjectMeta)
		}
		f.writeJSON(w, 200, metadataList("v1", "PersistentVolumeClaim", metas...))
	case path == vsBase && r.Method == http.MethodGet:
		var metas []metav1.ObjectMeta
		for _, v := range f.vss {
			metas = append(metas, v.ObjectMeta)
		}
		f.writeJSON(w, 200, metadataList("snapshot.storage.k8s.io/v1", "VolumeSnapshot", metas...))
	case strings.HasPrefix(path, pvcBase+"/"):
		name := strings.TrimPrefix(path, pvcBase+"/")
		p, ok := f.pvcs[name]
		if !ok {
			f.notFound(w, "pvc "+name)
			return
		}
		if r.Method == http.MethodPut {
			updated := &v1.PersistentVolumeClaim{}
			// client-go sends core types as protobuf, so use its universal deserializer.
			body, _ := io.ReadAll(r.Body)
			if _, _, err := clientgoscheme.Codecs.UniversalDeserializer().Decode(body, nil, updated); err != nil {
				f.t.Errorf("failed to decode PVC update (content type %q): %v", r.Header.Get("Content-Type"), err)
			}
			f.pvcs[name] = updated
			f.updated = append(f.updated, "pvc/"+name)
			p = updated
		}
		p.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"}
		f.writeJSON(w, 200, p)
	case strings.HasPrefix(path, vsBase+"/"):
		name := strings.TrimPrefix(path, vsBase+"/")
		v, ok := f.vss[name]
		if !ok {
			f.notFound(w, "vs "+name)
			return
		}
		if r.Method == http.MethodPut {
			updated := &snapv1.VolumeSnapshot{}
			if err := json.NewDecoder(r.Body).Decode(updated); err != nil {
				f.t.Errorf("failed to decode VolumeSnapshot update (content type %q): %v", r.Header.Get("Content-Type"), err)
			}
			f.vss[name] = updated
			f.updated = append(f.updated, "vs/"+name)
			v = updated
		}
		v.TypeMeta = metav1.TypeMeta{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"}
		f.writeJSON(w, 200, v)
	case strings.HasPrefix(path, clBase+"/"):
		name := strings.TrimPrefix(path, clBase+"/")
		c, ok := f.clusters[name]
		if !ok {
			f.notFound(w, "cluster "+name)
			return
		}
		c.TypeMeta = metav1.TypeMeta{APIVersion: "cluster.x-k8s.io/v1beta2", Kind: "Cluster"}
		f.writeJSON(w, 200, c)
	default:
		f.notFound(w, r.Method+" "+path)
	}
}

func (f *fakeAPIServer) wasUpdated(obj string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.updated, obj)
}

func (f *fakeAPIServer) hasFinalizer(finalizer string, obj any) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch o := obj.(type) {
	case string: // "pvc/<name>" or "vs/<name>"
		kind, name, _ := strings.Cut(o, "/")
		if kind == "pvc" {
			return slices.Contains(f.pvcs[name].Finalizers, finalizer)
		}
		return slices.Contains(f.vss[name].Finalizers, finalizer)
	}
	return false
}

// startFakeAPIServer starts the server and points KUBECONFIG at it.
func startFakeAPIServer(t *testing.T, f *fakeAPIServer) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: %q}}]
contexts: [{name: c, context: {cluster: c, user: u}}]
current-context: c
users: [{name: u, user: {}}]
`, srv.URL)
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", path)
}

func testPVC(name, cluster string, terminating bool) *v1.PersistentVolumeClaim {
	p := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace,
		Finalizers: []string{cnsoperatortypes.CNSVolumeFinalizer, "kubernetes.io/pvc-protection"}}}
	if cluster != "" {
		p.Labels = map[string]string{cluster + "/TKGService": "uid"}
	}
	if terminating {
		now := metav1.Now()
		p.DeletionTimestamp = &now
	}
	return p
}

func testVS(name, cluster string, terminating bool) *snapv1.VolumeSnapshot {
	v := &snapv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace,
		Finalizers: []string{cnsoperatortypes.CNSSnapshotFinalizer}}}
	if cluster != "" {
		v.Labels = map[string]string{cluster + "/TKGService": "uid"}
	}
	if terminating {
		now := metav1.Now()
		v.DeletionTimestamp = &now
	}
	return v
}

func testCluster(name string, phase ccV1beta2.ClusterPhase) *ccV1beta2.Cluster {
	return &ccV1beta2.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Status: ccV1beta2.ClusterStatus{Phase: string(phase)}}
}

// TestReconcileGuestClusterDeletion drives Reconcile of an active namespace, as triggered by a Cluster event,
// against a fake API server and checks which PVCs and VolumeSnapshots get their CNS finalizer removed.
func TestReconcileGuestClusterDeletion(t *testing.T) {
	deleting := ccV1beta2.ClusterPhaseDeleting
	running := ccV1beta2.ClusterPhaseProvisioned
	tests := []struct {
		name     string
		clusters []*ccV1beta2.Cluster
		pvcs     []*v1.PersistentVolumeClaim
		vss      []*snapv1.VolumeSnapshot
		// objects ("pvc/<name>", "vs/<name>") that must lose their CNS finalizer, all others must keep it
		wantRemoved []string
	}{
		{
			name:        "cluster already deleted: terminating PVC and snapshot are cleaned",
			pvcs:        []*v1.PersistentVolumeClaim{testPVC("p1", "gone", true)},
			vss:         []*snapv1.VolumeSnapshot{testVS("s1", "gone", true)},
			wantRemoved: []string{"pvc/p1", "vs/s1"},
		},
		{
			name:        "cluster being deleted: terminating PVC and snapshot are cleaned",
			clusters:    []*ccV1beta2.Cluster{testCluster("c1", deleting)},
			pvcs:        []*v1.PersistentVolumeClaim{testPVC("p1", "c1", true)},
			vss:         []*snapv1.VolumeSnapshot{testVS("s1", "c1", true)},
			wantRemoved: []string{"pvc/p1", "vs/s1"},
		},
		{
			name:     "cluster still running: terminating objects are left alone",
			clusters: []*ccV1beta2.Cluster{testCluster("c1", running)},
			pvcs:     []*v1.PersistentVolumeClaim{testPVC("p1", "c1", true)},
			vss:      []*snapv1.VolumeSnapshot{testVS("s1", "c1", true)},
		},
		{
			name: "retained volumes of a deleted cluster, not marked for deletion, keep their finalizers",
			pvcs: []*v1.PersistentVolumeClaim{testPVC("p1", "gone", false)},
			vss:  []*snapv1.VolumeSnapshot{testVS("s1", "gone", false)},
		},
		{
			name: "only objects of the deleted cluster are cleaned",
			clusters: []*ccV1beta2.Cluster{testCluster("live", running),
				testCluster("dying", deleting)},
			pvcs: []*v1.PersistentVolumeClaim{testPVC("of-live", "live", true),
				testPVC("of-dying", "dying", true), testPVC("of-gone", "gone", true),
				testPVC("of-gone-keep", "gone", false), testPVC("no-label", "", true)},
			wantRemoved: []string{"pvc/of-dying", "pvc/of-gone"},
		},
		{
			name: "no PVCs or snapshots: nothing to do",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &fakeAPIServer{t: t, pvcs: map[string]*v1.PersistentVolumeClaim{},
				vss: map[string]*snapv1.VolumeSnapshot{}, clusters: map[string]*ccV1beta2.Cluster{}}
			var crObjs []client.Object
			for _, c := range tt.clusters {
				api.clusters[c.Name] = c.DeepCopy()
				crObjs = append(crObjs, c.DeepCopy())
			}
			for _, p := range tt.pvcs {
				api.pvcs[p.Name] = p.DeepCopy()
			}
			for _, v := range tt.vss {
				api.vss[v.Name] = v.DeepCopy()
			}
			startFakeAPIServer(t, api)

			crObjs = append(crObjs, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}})
			r, _ := newTestReconciler(fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(crObjs...).Build())
			res, err := r.Reconcile(context.TODO(), reconcile.Request{NamespacedName: types.NamespacedName{Name: testNamespace}})
			if err != nil || res.RequeueAfter != 0 {
				t.Fatalf("Reconcile() = %v, %v, want no error and no requeue", res, err)
			}

			var all []string
			for n := range api.pvcs {
				all = append(all, "pvc/"+n)
			}
			for n := range api.vss {
				all = append(all, "vs/"+n)
			}
			for _, obj := range all {
				finalizer := cnsoperatortypes.CNSVolumeFinalizer
				if strings.HasPrefix(obj, "vs/") {
					finalizer = cnsoperatortypes.CNSSnapshotFinalizer
				}
				removed := !api.hasFinalizer(finalizer, obj)
				if want := slices.Contains(tt.wantRemoved, obj); removed != want {
					t.Errorf("%s: CNS finalizer removed = %v, want %v", obj, removed, want)
				}
				if removed && !api.wasUpdated(obj) {
					t.Errorf("%s: finalizer missing without an update call", obj)
				}
			}
			// The other finalizers must be preserved on cleaned objects.
			for _, obj := range tt.wantRemoved {
				if kind, name, _ := strings.Cut(obj, "/"); kind == "pvc" &&
					!api.hasFinalizer("kubernetes.io/pvc-protection", obj) {
					t.Errorf("pvc %s lost kubernetes.io/pvc-protection, has %v", name, api.pvcs[name].Finalizers)
				}
			}
		})
	}
}

// TestReconcileNamespaceDeletion checks that deleting a namespace still removes the CNS finalizers
// unconditionally, even for objects of a running guest cluster or objects not marked for deletion.
func TestReconcileNamespaceDeletion(t *testing.T) {
	api := &fakeAPIServer{
		t:    t,
		pvcs: map[string]*v1.PersistentVolumeClaim{"p1": testPVC("p1", "live", false)},
		vss:  map[string]*snapv1.VolumeSnapshot{"s1": testVS("s1", "live", false)},
		clusters: map[string]*ccV1beta2.Cluster{
			"live": testCluster("live", ccV1beta2.ClusterPhaseProvisioned)},
	}
	startFakeAPIServer(t, api)

	now := metav1.Now()
	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace, DeletionTimestamp: &now,
		Finalizers: []string{"kubernetes"}}}
	r, _ := newTestReconciler(fake.NewClientBuilder().WithScheme(newTestScheme(t)).
		WithObjects(ns, testPVC("p1", "live", false), testVS("s1", "live", false)).Build())

	res, err := r.Reconcile(context.TODO(), reconcile.Request{NamespacedName: types.NamespacedName{Name: testNamespace}})
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("Reconcile() = %v, %v, want no error and no requeue", res, err)
	}
	if api.hasFinalizer(cnsoperatortypes.CNSVolumeFinalizer, "pvc/p1") {
		t.Error("PVC kept its CNS finalizer on namespace deletion")
	}
	if api.hasFinalizer(cnsoperatortypes.CNSSnapshotFinalizer, "vs/s1") {
		t.Error("VolumeSnapshot kept its CNS finalizer on namespace deletion")
	}
	backOffDurationMapMutex.Lock()
	defer backOffDurationMapMutex.Unlock()
	if _, ok := backOffDuration[testNamespace]; ok {
		t.Error("backoff entry was not cleaned up after a successful namespace cleanup")
	}
}
