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

package wcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

const testPlacementVCUUID = "42305b1e-8f1a-4a3b-9c2d-1234567890ab"

// placementFixture is one host-local WFFC claim on the Supervisor of upgradeInventory.
type placementFixture struct {
	sc             *storagev1.StorageClass
	pvc            *v1.PersistentVolumeClaim
	pod            *v1.Pod
	node           *v1.Node
	placement      *podvmv2.PodVMPlacement
	otherPlacement *podvmv2.PodVMPlacement
	readErr        error
	extra          []runtime.Object
	candidates     []vimtypes.ManagedObjectReference
	inventory      map[string]map[string]string
	vcUUID         string
	// missingPVC leaves the claim out of the API server.
	missingPVC bool
}

func newPlacementFixture(selectedNode string, virtual bool, host string) *placementFixture {
	binding := storagev1.VolumeBindingWaitForFirstConsumer
	f := &placementFixture{
		sc: &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "local"}, VolumeBindingMode: &binding},
		pvc: &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "disk", Namespace: "tenant-a",
				Annotations: map[string]string{annSelectedNode: selectedNode}},
		},
		// Not bound and no VM UUID: volume creation precedes PodVM creation.
		pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "tenant-a", UID: "pod-uid"},
			Spec: v1.PodSpec{Volumes: []v1.Volume{{Name: "data", VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "disk"}}}}},
			Status: v1.PodStatus{Phase: v1.PodPending},
		},
		node:       &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: selectedNode, UID: types.UID(selectedNode + "-uid")}},
		candidates: hostRefs("host-11", "host-12", "host-13", "host-14"),
		inventory:  upgradeInventory(),
		vcUUID:     testPlacementVCUUID,
	}
	f.pvc.Spec.StorageClassName = &f.sc.Name
	if virtual {
		f.node.Labels = map[string]string{podvmv2.VirtualNodeProviderLabel: podvmv2.VirtualNodeProviderValue}
	}
	controller := true
	f.placement = &podvmv2.PodVMPlacement{
		ObjectMeta: metav1.ObjectMeta{Name: f.pod.Name, Namespace: f.pod.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: f.pod.Name,
				UID: f.pod.UID, Controller: &controller}}},
		Spec: podvmv2.PodVMPlacementSpec{
			NodeName: selectedNode,
			Target: podvmv2.PodVMPlacementTarget{VCenterInstanceUUID: testPlacementVCUUID,
				ClusterMoID: "domain-c8", HostMoID: host},
		},
	}
	return f
}

// run evaluates the fixture and reports the result, the number of placement reads, the client and
// the error.
func (f *placementFixture) run(t *testing.T) ([]vimtypes.ManagedObjectReference, int, *fake.Clientset, error) {
	t.Helper()
	objects := append([]runtime.Object{}, f.extra...)
	for _, obj := range []runtime.Object{f.sc, f.pvc, f.pod, f.node} {
		if obj == f.pvc && f.missingPVC {
			continue
		}
		if obj != nil && !isNilObject(obj) {
			objects = append(objects, obj)
		}
	}
	client := fake.NewClientset(objects...)
	reads := 0
	read := func(_ context.Context, namespace, name string) (*podvmv2.PodVMPlacement, error) {
		reads++
		require.NotNil(t, f.pod, "the placement is read only for a consumer Pod")
		require.Equal(t, f.pod.Namespace, namespace, "the placement is in the Pod's namespace")
		if name != f.pod.Name {
			if p := f.otherPlacement; p != nil && p.Name == name {
				return p, nil
			}
			return nil, apierrors.NewNotFound(podvmv2.PodVMPlacementGVR.GroupResource(), name)
		}
		if f.readErr != nil {
			return nil, f.readErr
		}
		return f.placement, nil
	}
	result, err := constrainToPodPlacement(context.Background(), client, read, f.vcUUID, f.pvc.Namespace,
		f.pvc.Name, f.candidates, f.inventory)
	return result, reads, client, err
}

func isNilObject(obj runtime.Object) bool {
	switch o := obj.(type) {
	case *storagev1.StorageClass:
		return o == nil
	case *v1.PersistentVolumeClaim:
		return o == nil
	case *v1.Pod:
		return o == nil
	case *v1.Node:
		return o == nil
	}
	return false
}

// readsResource reports whether the client issued the verb on the resource.
func readsResource(client *fake.Clientset, verb, resource string) bool {
	for _, action := range client.Actions() {
		if action.Matches(verb, resource) {
			return true
		}
	}
	return false
}

func otherConsumer(name string, uid types.UID) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenant-a", UID: uid},
		Spec: v1.PodSpec{Volumes: []v1.Volume{{Name: "data", VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "disk"}}}}},
	}
}

func ephemeralVolume(name string) []v1.Volume {
	return []v1.Volume{{Name: name, VolumeSource: v1.VolumeSource{Ephemeral: &v1.EphemeralVolumeSource{}}}}
}

// useEphemeralClaim turns the fixture's claim into the generic ephemeral volume "scratch" of the
// consumer Pod, controlled by that Pod as the ephemeral volume controller creates it.
func useEphemeralClaim(f *placementFixture) {
	controller := true
	f.pvc.Name = f.pod.Name + "-scratch"
	f.pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: f.pod.Name,
		UID: f.pod.UID, Controller: &controller}}
	f.pod.Spec.Volumes = ephemeralVolume("scratch")
}

func TestPodPlacementOnVirtualNode(t *testing.T) {
	notFound := apierrors.NewNotFound(podvmv2.PodVMPlacementGVR.GroupResource(), "app-0")
	addConsumerWithPlacement := func(f *placementFixture, node, host string) *v1.Pod {
		other := otherConsumer("app-1", "app-1-uid")
		f.extra = append(f.extra, other)
		f.otherPlacement = newPlacementFixture(node, true, host).placement
		f.otherPlacement.Name = other.Name
		f.otherPlacement.OwnerReferences[0].Name = other.Name
		f.otherPlacement.OwnerReferences[0].UID = other.UID
		return other
	}
	for _, tc := range []struct {
		name   string
		mutate func(*placementFixture)
		// want is the committed host; empty means the attempt must fail.
		want string
		// noRead means the attempt fails before the placement is read.
		noRead bool
		code   codes.Code
		reads  int
	}{
		{name: "committed host", want: "host-14"},
		{name: "vCenter UUID compared case-insensitively", want: "host-14", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.VCenterInstanceUUID = "42305B1E-8F1A-4A3B-9C2D-1234567890AB"
		}},
		{name: "clusterMoID is not compared", want: "host-14", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.ClusterMoID = "domain-c99"
		}},
		{name: "feature for another consumer does not block CSI", want: "host-14", mutate: func(f *placementFixture) {
			f.placement.Spec.RequiredFeatures = []podvmv2.PodVMPlacementFeature{
				{Name: "MultipleNetworkInterfaces", Consumers: []string{"VirtualNode"}}}
		}},
		{name: "consumer already bound to the selected Node", want: "host-14", mutate: func(f *placementFixture) {
			f.pod.Spec.NodeName = "vn-c8"
		}},
		{name: "deleting and terminal Pods are not consumers", want: "host-14", mutate: func(f *placementFixture) {
			now := metav1.Now()
			deleting := otherConsumer("old-0", "old-uid")
			deleting.DeletionTimestamp = &now
			deleting.Finalizers = []string{"test"}
			done := otherConsumer("job-0", "job-uid")
			done.Status.Phase = v1.PodSucceeded
			failed := otherConsumer("job-1", "job-1-uid")
			failed.Status.Phase = v1.PodFailed
			f.extra = append(f.extra, deleting, done, failed)
		}},
		{name: "generic ephemeral volume consumer", want: "host-14", mutate: useEphemeralClaim},
		// Pod "app" with ephemeral volume "0-scratch" also derives the claim name "app-0-scratch".
		{name: "an ephemeral name collision with its own placement is not a consumer", want: "host-14",
			mutate: func(f *placementFixture) {
				useEphemeralClaim(f)
				other := addConsumerWithPlacement(f, "vn-c8", "host-13")
				other.Name = "app"
				other.Spec.Volumes = ephemeralVolume("0-scratch")
				f.otherPlacement.Name = other.Name
				f.otherPlacement.OwnerReferences[0].Name = other.Name
			}},
		{name: "only an ephemeral name collision is live", noRead: true, code: codes.Unavailable,
			mutate: func(f *placementFixture) {
				useEphemeralClaim(f)
				f.pod.Name, f.pod.UID = "app", "app-uid"
				f.pod.Spec.Volumes = ephemeralVolume("0-scratch")
			}},
		{name: "ephemeral claim not controlled by any Pod", noRead: true, code: codes.Unavailable,
			mutate: func(f *placementFixture) {
				useEphemeralClaim(f)
				f.pvc.OwnerReferences = nil
			}},
		{name: "ephemeral claim owned but not controlled by the Pod", noRead: true, code: codes.Unavailable,
			mutate: func(f *placementFixture) {
				useEphemeralClaim(f)
				f.pvc.OwnerReferences[0].Controller = nil
			}},
		{name: "a Pod using another claim is not a consumer", want: "host-14", mutate: func(f *placementFixture) {
			other := otherConsumer("app-1", "app-1-uid")
			other.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other-disk"
			f.extra = append(f.extra, other)
		}},
		{name: "placement not written yet", code: codes.Unavailable,
			mutate: func(f *placementFixture) { f.readErr = notFound }},
		{name: "placement reader returns no object", code: codes.Unavailable,
			mutate: func(f *placementFixture) { f.placement = nil }},
		{name: "placement read fails", mutate: func(f *placementFixture) { f.readErr = errors.New("unavailable") }},
		{name: "placement being deleted", code: codes.Unavailable, mutate: func(f *placementFixture) {
			now := metav1.Now()
			f.placement.DeletionTimestamp = &now
		}},
		{name: "placement left by an earlier Pod of the same name", code: codes.Unavailable,
			mutate: func(f *placementFixture) { f.placement.OwnerReferences[0].UID = "old-pod-uid" }},
		{name: "owner reference is not the controller", code: codes.Unavailable, mutate: func(f *placementFixture) {
			f.placement.OwnerReferences[0].Controller = nil
		}},
		{name: "no owner reference", code: codes.Unavailable,
			mutate: func(f *placementFixture) { f.placement.OwnerReferences = nil }},
		{name: "feature for CSI", mutate: func(f *placementFixture) {
			f.placement.Spec.RequiredFeatures = []podvmv2.PodVMPlacementFeature{
				{Name: "PerVolumeStorage", Consumers: []string{podvmv2.PodVMPlacementConsumerCSI}}}
		}},
		{name: "placement for another Node", mutate: func(f *placementFixture) {
			f.placement.Spec.NodeName = "vn-c9"
		}},
		{name: "placement in another vCenter", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.VCenterInstanceUUID = "11111111-2222-3333-4444-555555555555"
		}},
		{name: "placement without a vCenter", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.VCenterInstanceUUID = ""
		}},
		{name: "configured vCenter unknown", mutate: func(f *placementFixture) { f.vcUUID = "" }},
		{name: "placement without a host", mutate: func(f *placementFixture) { f.placement.Spec.Target.HostMoID = "" }},
		{name: "host still held by a dedicated Node", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.HostMoID = "host-11"
		}},
		{name: "host with no owner", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.HostMoID = "host-15"
		}},
		{name: "host owned by another VirtualNode", mutate: func(f *placementFixture) {
			f.placement.Spec.Target.HostMoID = "host-21"
		}},
		{name: "host removed from the NodeMapping", mutate: func(f *placementFixture) {
			delete(f.inventory, "host-14")
		}},
		{name: "host excluded by a source constraint", mutate: func(f *placementFixture) {
			f.candidates = hostRefs("host-13")
		}},
		{name: "two pending consumers with only one placement", want: "host-14", reads: 2, mutate: func(f *placementFixture) {
			f.extra = append(f.extra, otherConsumer("app-1", "app-1-uid"))
		}},
		{name: "two pending consumers with placements on different Nodes", want: "host-13", reads: 2,
			mutate: func(f *placementFixture) {
				f.placement.Spec.NodeName = "vn-c9"
				addConsumerWithPlacement(f, "vn-c8", "host-13")
			}},
		{name: "a consumer bound to the selected Node takes precedence", want: "host-14", mutate: func(f *placementFixture) {
			f.pod.Spec.NodeName = "vn-c8"
			addConsumerWithPlacement(f, "vn-c8", "host-13")
		}},
		{name: "a consumer bound elsewhere is ignored", want: "host-14", mutate: func(f *placementFixture) {
			addConsumerWithPlacement(f, "vn-c8", "host-13").Spec.NodeName = "vn-c9"
		}},
		{name: "a stale placement does not cause ambiguity", want: "host-14", reads: 2, mutate: func(f *placementFixture) {
			addConsumerWithPlacement(f, "vn-c8", "host-13")
			f.otherPlacement.OwnerReferences[0].UID = "old-uid"
		}},
		{name: "multiple pending consumers without a matching placement", code: codes.Unavailable,
			mutate: func(f *placementFixture) {
				f.placement.Spec.NodeName = "vn-c9"
				f.extra = append(f.extra, otherConsumer("app-1", "app-1-uid"))
			}},
		{name: "multiple pending consumers targeting the same Node are ambiguous", code: codes.FailedPrecondition,
			mutate: func(f *placementFixture) { addConsumerWithPlacement(f, "vn-c8", "host-13") }},
		{name: "multiple consumers bound to the same Node are ambiguous", noRead: true, code: codes.FailedPrecondition,
			mutate: func(f *placementFixture) {
				f.pod.Spec.NodeName = "vn-c8"
				addConsumerWithPlacement(f, "vn-c8", "host-13").Spec.NodeName = "vn-c8"
			}},
		{name: "no consumer yet", noRead: true, code: codes.Unavailable, mutate: func(f *placementFixture) { f.pod = nil }},
		{name: "consumer bound to another Node", noRead: true, mutate: func(f *placementFixture) {
			f.pod.Spec.NodeName = "vn-c9"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPlacementFixture("vn-c8", true, "host-14")
			if tc.mutate != nil {
				tc.mutate(f)
			}
			result, reads, _, err := f.run(t)
			if tc.want == "" {
				require.Error(t, err)
				if tc.code != codes.OK {
					require.Equal(t, tc.code, status.Code(err))
				}
				require.Nil(t, result, "a failed placement must not return candidates")
				if tc.noRead {
					require.Zero(t, reads)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{tc.want}, refValues(result))
			require.Equal(t, max(1, tc.reads), reads)
		})
	}
}

func TestPodPlacementOnDedicatedNode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*placementFixture)
		want   string
		code   codes.Code
	}{
		{name: "the Node's single host", want: "host-12"},
		{name: "mixed Supervisor", want: "host-12", mutate: func(f *placementFixture) {
			f.inventory["host-11"][v1.LabelHostname] = "vn-c8"
		}},
		{name: "host excluded by a source constraint", mutate: func(f *placementFixture) {
			f.candidates = hostRefs("host-11")
		}},
		{name: "Node owns no host in the requested topology", mutate: func(f *placementFixture) {
			delete(f.inventory, "host-12")
		}},
		{name: "host joined a VirtualNode", mutate: func(f *placementFixture) {
			f.inventory["host-12"][v1.LabelHostname] = "vn-c8"
		}},
		{name: "Node owning two hosts is ambiguous", mutate: func(f *placementFixture) {
			f.inventory["host-11"][v1.LabelHostname] = "esx-12"
		}},
		{name: "Node being deleted", code: codes.ResourceExhausted, mutate: func(f *placementFixture) {
			now := metav1.Now()
			f.node.DeletionTimestamp = &now
			f.node.Finalizers = []string{"test"}
		}},
		{name: "Node missing", code: codes.ResourceExhausted, mutate: func(f *placementFixture) { f.node = nil }},
		{name: "unsupported virtual-kubelet provider", mutate: func(f *placementFixture) {
			f.node.Labels = map[string]string{podvmv2.VirtualNodeProviderLabel: "other-provider"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPlacementFixture("esx-12", false, "host-12")
			if tc.mutate != nil {
				tc.mutate(f)
			}
			result, reads, client, err := f.run(t)
			require.Zero(t, reads, "a dedicated Node never reads a PodVMPlacement")
			require.False(t, readsResource(client, "list", "pods"), "a dedicated Node never looks up the Pod")
			if tc.want == "" {
				require.Error(t, err)
				if tc.code != codes.OK {
					require.Equal(t, tc.code, status.Code(err))
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{tc.want}, refValues(result))
		})
	}
}

func TestPodPlacementBindingContext(t *testing.T) {
	immediate := storagev1.VolumeBindingImmediate
	for _, tc := range []struct {
		name   string
		mutate func(*placementFixture)
		// want is the expected candidate set; nil means the attempt must fail.
		want []string
		// noNodeRead asserts the selected Node and consumer Pod are not read.
		noNodeRead bool
	}{
		{name: "Immediate keeps every candidate", noNodeRead: true,
			want: []string{"host-11", "host-12", "host-13", "host-14"},
			mutate: func(f *placementFixture) {
				f.sc.VolumeBindingMode = &immediate
				delete(f.pvc.Annotations, annSelectedNode)
			}},
		{name: "unset binding mode is Immediate", noNodeRead: true,
			want:   []string{"host-11", "host-12", "host-13", "host-14"},
			mutate: func(f *placementFixture) { f.sc.VolumeBindingMode = nil }},
		{name: "bound claim keeps its PV's host pin", noNodeRead: true, want: []string{"host-13"},
			mutate: func(f *placementFixture) {
				f.pvc.Spec.VolumeName = "pv-disk"
				f.sc = nil
				f.extra = append(f.extra, &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-disk",
					Annotations: map[string]string{common.AnnPodVMHostPinned: "host-13"}}})
			}},
		{name: "bound claim without a host pin", noNodeRead: true, mutate: func(f *placementFixture) {
			f.pvc.Spec.VolumeName = "pv-disk"
			f.extra = append(f.extra, &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-disk"}})
		}},
		{name: "bound claim pinned outside the candidates", noNodeRead: true, mutate: func(f *placementFixture) {
			f.pvc.Spec.VolumeName = "pv-disk"
			f.extra = append(f.extra, &v1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-disk",
				Annotations: map[string]string{common.AnnPodVMHostPinned: "host-21"}}})
		}},
		{name: "bound PV missing", noNodeRead: true, mutate: func(f *placementFixture) {
			f.pvc.Spec.VolumeName = "pv-disk"
		}},
		{name: "PVC missing", noNodeRead: true, mutate: func(f *placementFixture) { f.missingPVC = true }},
		{name: "PVC without a StorageClass", noNodeRead: true, mutate: func(f *placementFixture) {
			f.pvc.Spec.StorageClassName = nil
		}},
		{name: "StorageClass missing", noNodeRead: true, mutate: func(f *placementFixture) { f.sc = nil }},
		{name: "unsupported binding mode", noNodeRead: true, mutate: func(f *placementFixture) {
			mode := storagev1.VolumeBindingMode("Eventually")
			f.sc.VolumeBindingMode = &mode
		}},
		{name: "WaitForFirstConsumer without a selected Node", noNodeRead: true, mutate: func(f *placementFixture) {
			delete(f.pvc.Annotations, annSelectedNode)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPlacementFixture("vn-c8", true, "host-14")
			if tc.mutate != nil {
				tc.mutate(f)
			}
			result, reads, client, err := f.run(t)
			require.Zero(t, reads, "only WaitForFirstConsumer on a VirtualNode reads a PodVMPlacement")
			if tc.noNodeRead {
				require.False(t, readsResource(client, "get", "nodes"), "the selected Node must not be read")
				require.False(t, readsResource(client, "list", "pods"), "the consumer Pod must not be looked up")
			}
			if tc.want == nil {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, refValues(result))
		})
	}

	t.Run("PVC identity is required", func(t *testing.T) {
		_, err := constrainToPodPlacement(context.Background(), fake.NewClientset(), nil, testPlacementVCUUID,
			"", "disk", hostRefs("host-11"), upgradeInventory())
		require.Error(t, err)
		_, err = constrainToPodPlacement(context.Background(), fake.NewClientset(), nil, testPlacementVCUUID,
			"tenant-a", "", hostRefs("host-11"), upgradeInventory())
		require.Error(t, err)
	})
}
