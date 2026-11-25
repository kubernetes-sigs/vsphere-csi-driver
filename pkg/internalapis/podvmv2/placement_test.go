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

package podvmv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const testVCenterUUID = "42305b1e-8f1a-4a3b-9c2d-1234567890ab"

// reviewedPlacement is shaped like the platform's private.iaas.vmware.com/v1alpha1 example,
// including spec.storage, spec.configSpec and status, which CSI does not model.
func reviewedPlacement() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "private.iaas.vmware.com/v1alpha1",
		"kind":       "PodVMPlacement",
		"metadata": map[string]interface{}{
			"name": "app-0", "namespace": "tenant-a",
			"ownerReferences": []interface{}{map[string]interface{}{
				"apiVersion": "v1", "kind": "Pod", "name": "app-0", "uid": "pod-uid", "controller": true,
			}},
		},
		"spec": map[string]interface{}{
			"nodeName": "virtual-node-domain-c52",
			"target": map[string]interface{}{
				"vCenterInstanceUUID": testVCenterUUID, "clusterMoID": "domain-c52", "hostMoID": "host-21",
			},
			"storage":    map[string]interface{}{"storagePolicyID": "aa6d5a82-1c88-45da-85d3-3d74b91a5bad"},
			"configSpec": map[string]interface{}{"_typeName": "VirtualMachineConfigSpec", "version": "vmx-23"},
			"requiredFeatures": []interface{}{map[string]interface{}{
				"name": "MultipleNetworkInterfaces", "consumers": []interface{}{"VirtualNode"},
			}},
		},
		"status": map[string]interface{}{
			"vm": map[string]interface{}{"instanceUUID": "5012abcd-0000-0000-0000-000000000000", "moID": "vm-1042"},
		},
	}}
}

func TestGetPodVMPlacementDecodesReviewedSchema(t *testing.T) {
	ctx := context.Background()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{PodVMPlacementGVR: "PodVMPlacementList"}, reviewedPlacement())

	placement, err := GetPodVMPlacement(ctx, client, "tenant-a", "app-0")
	require.NoError(t, err)
	require.Equal(t, "virtual-node-domain-c52", placement.Spec.NodeName)
	require.Equal(t, PodVMPlacementTarget{
		VCenterInstanceUUID: testVCenterUUID, ClusterMoID: "domain-c52", HostMoID: "host-21",
	}, placement.Spec.Target)
	require.Empty(t, placement.UnsupportedCSIFeatures(), "a feature for another consumer does not block CSI")

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "tenant-a", UID: "pod-uid"}}
	require.True(t, placement.AppliesTo(pod))
	pod.UID = "recreated-pod-uid"
	require.False(t, placement.AppliesTo(pod), "a recreated Pod with the same name must not use the old placement")

	_, err = GetPodVMPlacement(ctx, client, "tenant-a", "app-1")
	require.True(t, apierrors.IsNotFound(err), "NotFound must be returned unchanged, got %v", err)
	_, err = GetPodVMPlacement(ctx, client, "tenant-b", "app-0")
	require.True(t, apierrors.IsNotFound(err), "the GET is namespaced, got %v", err)
}

func TestPodVMPlacementAppliesTo(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "tenant", UID: "pod-uid"}}
	controller, notController := true, false
	placementWith := func(mutate func(*metav1.OwnerReference)) *PodVMPlacement {
		ref := metav1.OwnerReference{APIVersion: "v1", Kind: "Pod", Name: "app", UID: "pod-uid", Controller: &controller}
		if mutate != nil {
			mutate(&ref)
		}
		return &PodVMPlacement{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "tenant",
			OwnerReferences: []metav1.OwnerReference{ref}}}
	}
	for _, tc := range []struct {
		name      string
		placement *PodVMPlacement
		pod       *corev1.Pod
		want      bool
	}{
		{name: "controller owner matches", placement: placementWith(nil), pod: pod, want: true},
		{name: "stale UID", placement: placementWith(func(r *metav1.OwnerReference) { r.UID = "old" }), pod: pod},
		{name: "other name", placement: placementWith(func(r *metav1.OwnerReference) { r.Name = "other" }), pod: pod},
		{name: "not a Pod", placement: placementWith(func(r *metav1.OwnerReference) { r.Kind = "ReplicaSet" }),
			pod: pod},
		{name: "wrong API version", placement: placementWith(func(r *metav1.OwnerReference) { r.APIVersion = "v2" }),
			pod: pod},
		{name: "not controller", placement: placementWith(func(r *metav1.OwnerReference) { r.Controller = &notController }),
			pod: pod},
		{name: "controller unset", placement: placementWith(func(r *metav1.OwnerReference) { r.Controller = nil }),
			pod: pod},
		{name: "no owner", placement: &PodVMPlacement{ObjectMeta: metav1.ObjectMeta{Name: "app"}}, pod: pod},
		{name: "Pod without UID", placement: placementWith(nil),
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "tenant"}}},
		{name: "nil placement", pod: pod},
		{name: "nil Pod", placement: placementWith(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.placement.AppliesTo(tc.pod))
		})
	}
}

func TestPodVMPlacementUnsupportedCSIFeatures(t *testing.T) {
	placement := &PodVMPlacement{Spec: PodVMPlacementSpec{RequiredFeatures: []PodVMPlacementFeature{
		{Name: "MultipleNetworkInterfaces", Consumers: []string{"VirtualNode"}},
		{Name: "PerVolumeStorage", Consumers: []string{"VirtualNode", PodVMPlacementConsumerCSI}},
		{Name: "NoConsumers"},
	}}}
	require.Equal(t, []string{"PerVolumeStorage"}, placement.UnsupportedCSIFeatures())
	require.Empty(t, (&PodVMPlacement{}).UnsupportedCSIFeatures())
	var missing *PodVMPlacement
	require.Empty(t, missing.UnsupportedCSIFeatures())
}
