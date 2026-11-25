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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeinfo"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

type hostLocalTestVolumeInfoService struct {
	cnsvolumeinfo.VolumeInfoService
}

func (hostLocalTestVolumeInfoService) CreateVolumeInfoWithPolicyInfo(context.Context,
	string, string, string, string, string, string, *resource.Quantity, bool) error {
	return nil
}

// TestCreateBlockVolumeHostLocalFeatureMatrix drives createBlockVolume through each host-local
// feature combination and asserts the path taken. This fixture cannot place a volume from a host
// list, so a request whose candidate resolution and placement succeed fails in the CNS create step
// with "failed to create volume"; any earlier failure is a rejection by the code under test.
func TestCreateBlockVolumeHostLocalFeatureMatrix(t *testing.T) {
	ct := getControllerTest(t)
	originalCO := commonco.ContainerOrchestratorUtility
	originalHostLocal := isHostLocalStorageSupportEnabled
	originalMultiCluster := IsMultipleClustersPerVsphereZoneFSSEnabled
	originalClusterHosts := getClusterHostMoIDs
	originalK8sClient := ct.controller.k8sClient
	originalDynamicClient := ct.controller.dynamicClient
	t.Cleanup(func() {
		commonco.ContainerOrchestratorUtility = originalCO
		isHostLocalStorageSupportEnabled = originalHostLocal
		IsMultipleClustersPerVsphereZoneFSSEnabled = originalMultiCluster
		getClusterHostMoIDs = originalClusterHosts
		ct.controller.k8sClient = originalK8sClient
		ct.controller.dynamicClient = originalDynamicClient
	})
	vc, err := common.GetVCenter(ctx, ct.controller.manager)
	require.NoError(t, err)
	vcUUID := vc.Client.ServiceContent.About.InstanceUuid
	require.NotEmpty(t, vcUUID)

	const reachedCNS = "failed to create volume"
	capabilities := []*csi.VolumeCapability{{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
	request := func(params map[string]string, segments ...map[string]string) *csi.CreateVolumeRequest {
		parameters := map[string]string{
			common.AttributePvcNamespace:    "tenant-a",
			common.AttributePvcName:         "disk",
			common.AttributeHostLocalPolicy: "true",
		}
		for key, value := range params {
			parameters[key] = value
		}
		return &csi.CreateVolumeRequest{
			Name:               testVolumeName + "-" + uuid.New().String(),
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 * common.GbInBytes},
			Parameters:         parameters,
			VolumeCapabilities: capabilities,
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: topologies(segments...),
				Requisite: topologies(segments...),
			},
		}
	}
	physicalHosts := map[string]string{common.AttributeHostLocalTopology: `[` +
		`{"topology.kubernetes.io/zone":"zone-1","topology.csi.vmware.com/host":"host-12"},` +
		`{"topology.kubernetes.io/zone":"zone-1","topology.csi.vmware.com/host":"host-13"}]`}

	type env struct {
		co        *hostLocalTestCO
		hostReads int
		k8s       *fake.Clientset
	}
	setup := func(hostLocal, podVMV2 bool, binding storagev1.VolumeBindingMode, selectedNode string,
		k8sObjects ...runtime.Object) *env {
		e := &env{co: &hostLocalTestCO{
			COCommonInterface: originalCO, podVMV2: podVMV2, owners: upgradeOwners(),
			zoneClusters:       map[string][]string{"zone-1": {"domain-c8"}, "zone-2": {"domain-c9"}},
			nodeNameToHostMoID: map[string]string{"esx-11": "host-11", "esx-12": "host-12"},
		}}
		commonco.ContainerOrchestratorUtility = e.co
		isHostLocalStorageSupportEnabled = hostLocal
		IsMultipleClustersPerVsphereZoneFSSEnabled = true
		getClusterHostMoIDs = func(_ context.Context, _ *cnsvsphere.VirtualCenter, cluster string) ([]string, error) {
			e.hostReads++
			return upgradeClusterHosts()[cluster], nil
		}
		scName := "local"
		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "disk", Namespace: "tenant-a", Annotations: map[string]string{}},
			Spec:       v1.PersistentVolumeClaimSpec{StorageClassName: &scName},
		}
		if selectedNode != "" {
			pvc.Annotations[annSelectedNode] = selectedNode
		}
		objects := append([]runtime.Object{pvc,
			&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: scName}, VolumeBindingMode: &binding}},
			k8sObjects...)
		e.k8s = fake.NewClientset(objects...)
		ct.controller.k8sClient = e.k8s
		ct.controller.dynamicClient = nil
		return e
	}
	// assertPodVMV2PathOnly checks that the HostLocalStorage + PodVM v2 resolution was used.
	assertPodVMV2PathOnly := func(t *testing.T, e *env) {
		require.Equal(t, 1, e.co.ownerReads, "ownership is resolved once per request")
		require.NotZero(t, e.hostReads, "candidates come from the vCenter cluster inventory")
		require.Zero(t, e.co.legacyMapReads, "the dedicated-only hostname map must not be used")
	}
	// assertLegacyPathOnly checks that the HostLocalStorage-only resolution was used.
	assertLegacyPathOnly := func(t *testing.T, e *env) {
		require.Zero(t, e.co.ownerReads, "HostLocalStorage alone must not resolve VirtualNode ownership")
		require.Zero(t, e.hostReads, "HostLocalStorage alone must not read the cluster inventory")
	}

	t.Run("HostLocalStorage only", func(t *testing.T) {
		t.Run("rejects the host-local topology parameter", func(t *testing.T) {
			e := setup(true, false, storagev1.VolumeBindingImmediate, "")
			_, faultType, err := ct.controller.createBlockVolume(ctx, request(physicalHosts, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, faultType)
			require.Contains(t, err.Error(), common.AttributeHostLocalTopology)
			assertLegacyPathOnly(t, e)
		})
		t.Run("zone + hostname resolves through the dedicated-Node map", func(t *testing.T) {
			e := setup(true, false, storagev1.VolumeBindingImmediate, "")
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, nodeSegment("zone-1", "esx-12")),
				true, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, reachedCNS)
			require.Equal(t, 1, e.co.legacyMapReads)
			assertLegacyPathOnly(t, e)
		})
		t.Run("zone-only request keeps the existing hostname requirement", func(t *testing.T) {
			e := setup(true, false, storagev1.VolumeBindingImmediate, "")
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, "requires at least one hostname")
			assertLegacyPathOnly(t, e)
		})
	})

	t.Run("cached host-local placement is recovered", func(t *testing.T) {
		originalHostMounts := datastoreHostMounts
		originalOperationStore := operationStore
		originalVolumeInfoService := volumeInfoService
		operationStore = ct.controller.manager.VolumeManager.GetOperationStore()
		volumeInfoService = hostLocalTestVolumeInfoService{}
		t.Cleanup(func() {
			datastoreHostMounts = originalHostMounts
			operationStore = originalOperationStore
			volumeInfoService = originalVolumeInfoService
		})
		for _, tc := range []struct {
			name    string
			podVMV2 bool
			segment map[string]string
		}{
			{name: "legacy", segment: nodeSegment("zone-1", "esx-12")},
			{name: "PodVM v2", podVMV2: true, segment: zoneSegment("zone-1")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				setup(true, tc.podVMV2, storagev1.VolumeBindingImmediate, "")
				originalStretched := isPodVMOnStretchSupervisorFSSEnabled
				t.Cleanup(func() { isPodVMOnStretchSupervisorFSSEnabled = originalStretched })
				// The simulator can create from datastores but lacks host-local placement. Seed CNS
				// and the real manager's operation store with that create, then model the local mount.
				initial := request(nil)
				delete(initial.Parameters, common.AttributeHostLocalPolicy)
				created, _, err := ct.controller.createBlockVolume(ctx, initial, false, clusterComputeResourceMoIds)
				require.NoError(t, err)
				volumeID := created.Volume.VolumeId
				t.Cleanup(func() {
					_, err := common.DeleteVolumeUtil(ctx, ct.controller.manager.VolumeManager, volumeID, true)
					require.NoError(t, err)
					require.NoError(t, operationStore.DeleteRequestDetails(ctx, initial.Name))
				})
				require.True(t, operationStore.HasPriorSuccessfulCreate(ctx, initial.Name))

				retry := request(map[string]string{common.AttributeStorageClassName: "local"}, tc.segment)
				retry.Name = initial.Name
				isPodVMOnStretchSupervisorFSSEnabled = true
				hosts := hostRefs("host-12", "host-13")
				mounts := []vimtypes.DatastoreHostMount{{Key: hosts[0]}, {Key: hosts[1]}}
				mountErr := context.DeadlineExceeded
				datastoreHostMounts = func(context.Context, *cnsvsphere.DatastoreInfo) ([]vimtypes.DatastoreHostMount, error) {
					return mounts, mountErr
				}

				_, _, err = ct.controller.createBlockVolume(ctx, retry, true, clusterComputeResourceMoIds)
				require.ErrorContains(t, err, "context deadline exceeded")
				require.True(t, operationStore.HasPriorSuccessfulCreate(ctx, retry.Name))

				mountErr = nil
				_, _, err = ct.controller.createBlockVolume(ctx, retry, true, clusterComputeResourceMoIds)
				require.ErrorContains(t, err, "does not identify a single backing host")
				require.True(t, operationStore.HasPriorSuccessfulCreate(ctx, retry.Name))

				mounts = mounts[:1]
				response, _, err := ct.controller.createBlockVolume(ctx, retry, true, clusterComputeResourceMoIds)
				require.NoError(t, err)
				require.Equal(t, volumeID, response.Volume.VolumeId)
				require.Equal(t, topologies(tc.segment), response.Volume.AccessibleTopology)
				if tc.podVMV2 {
					require.Equal(t, "host-12", response.Volume.VolumeContext[common.AnnESXiHostMoID])
				}
			})
		}
	})

	t.Run("HostLocalStorage and PodVM v2", func(t *testing.T) {
		t.Run("rejects the parameter on a shared request", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			req := request(physicalHosts, zoneSegment("zone-1"))
			delete(req.Parameters, common.AttributeHostLocalPolicy)
			_, faultType, err := ct.controller.createBlockVolume(ctx, req, true, clusterComputeResourceMoIds)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, faultType)
			require.Zero(t, e.co.ownerReads)
		})
		t.Run("rejects a malformed parameter", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			_, faultType, err := ct.controller.createBlockVolume(ctx, request(map[string]string{
				common.AttributeHostLocalTopology: `[{"topology.csi.vmware.com/host":"host-12"}]`,
			}, zoneSegment("zone-1")), true, clusterComputeResourceMoIds)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, faultType)
			require.Zero(t, e.co.ownerReads)
		})
		t.Run("fails when ownership cannot be resolved", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			e.co.ownersErr = errors.New("cannot list NodeMapping resources")
			_, _, err := ct.controller.createBlockVolume(ctx, request(physicalHosts, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			require.Zero(t, e.hostReads)
			require.Zero(t, e.co.legacyMapReads)
		})
		t.Run("guest physical hosts on an Immediate class reach CNS", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			_, _, err := ct.controller.createBlockVolume(ctx, request(physicalHosts, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, reachedCNS)
			assertPodVMV2PathOnly(t, e)
		})
		t.Run("Immediate provisioning uses the zones assigned to the namespace", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			delete(e.co.zoneClusters, "zone-2")
			_, _, err := ct.controller.createBlockVolume(ctx,
				request(nil, zoneSegment("zone-2"), zoneSegment("zone-1"), zoneSegment("zone-3")),
				true, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, reachedCNS)
			assertPodVMV2PathOnly(t, e)
			require.Equal(t, 1, e.hostReads)
		})
		t.Run("no active clusters in any requested zone fails placement", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-3")),
				true, clusterComputeResourceMoIds)
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			require.Zero(t, e.hostReads)
		})
		t.Run("host-local branch does not require a storage topology type", func(t *testing.T) {
			// Without Workload Domain Isolation the zonal branch rejects a request with no
			// storageTopologyType; a zone-only host-local request must not be routed there.
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-1")),
				false, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, reachedCNS)
			assertPodVMV2PathOnly(t, e)
		})
		t.Run("a physical host without an owner is never a candidate", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingImmediate, "")
			_, _, err := ct.controller.createBlockVolume(ctx, request(map[string]string{
				common.AttributeHostLocalTopology: `[{"topology.kubernetes.io/zone":"zone-1",` +
					`"topology.csi.vmware.com/host":"host-15"}]`,
			}, zoneSegment("zone-1")), true, clusterComputeResourceMoIds)
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			assertPodVMV2PathOnly(t, e)
		})
		t.Run("WFFC on a dedicated Node reaches CNS without a placement", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingWaitForFirstConsumer, "esx-12",
				&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "esx-12"}})
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, nodeSegment("zone-1", "esx-12")),
				true, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, reachedCNS)
			assertPodVMV2PathOnly(t, e)
		})

		virtualNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "vn-c8",
			Labels: map[string]string{podvmv2.VirtualNodeProviderLabel: podvmv2.VirtualNodeProviderValue}}}
		consumer := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "tenant-a", UID: "pod-uid"},
			Spec: v1.PodSpec{Volumes: []v1.Volume{{Name: "data", VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: "disk"}}}}},
		}
		placement := func(host string) *unstructured.Unstructured {
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
					"nodeName": "vn-c8",
					"target": map[string]interface{}{
						"vCenterInstanceUUID": vcUUID, "clusterMoID": "domain-c8", "hostMoID": host,
					},
				},
			}}
		}
		placementClient := func(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
			return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
				map[schema.GroupVersionResource]string{podvmv2.PodVMPlacementGVR: "PodVMPlacementList"}, objects...)
		}

		t.Run("WFFC on a VirtualNode waits for the placement", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingWaitForFirstConsumer, "vn-c8", virtualNode, consumer)
			ct.controller.dynamicClient = placementClient()
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
			require.ErrorContains(t, err, "not written yet")
			assertPodVMV2PathOnly(t, e)
		})
		t.Run("WFFC can reschedule when the selected VirtualNode is being deleted", func(t *testing.T) {
			deletingNode := virtualNode.DeepCopy()
			now := metav1.Now()
			deletingNode.DeletionTimestamp = &now
			deletingNode.Finalizers = []string{"test"}
			setup(true, true, storagev1.VolumeBindingWaitForFirstConsumer, deletingNode.Name, deletingNode, consumer)
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
			require.ErrorContains(t, err, "is being deleted")
		})
		t.Run("WFFC with two pending consumers reaches CNS using the matching placement", func(t *testing.T) {
			other := otherConsumer("app-1", "app-1-uid")
			e := setup(true, true, storagev1.VolumeBindingWaitForFirstConsumer, "vn-c8", virtualNode, consumer, other)
			ct.controller.dynamicClient = placementClient(placement("host-14"))
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.ErrorContains(t, err, reachedCNS)
			assertPodVMV2PathOnly(t, e)
		})
		t.Run("WFFC on a VirtualNode rejects a host it does not own", func(t *testing.T) {
			e := setup(true, true, storagev1.VolumeBindingWaitForFirstConsumer, "vn-c8", virtualNode, consumer)
			ct.controller.dynamicClient = placementClient(placement("host-11"))
			_, _, err := ct.controller.createBlockVolume(ctx, request(nil, zoneSegment("zone-1")),
				true, clusterComputeResourceMoIds)
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
			require.ErrorContains(t, err, `owned by Node "esx-11"`)
			assertPodVMV2PathOnly(t, e)
		})
	})
}
