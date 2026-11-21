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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	fvv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	testDVSA = "50 24 81 df 8a 5a 0a 1d-54 0b 76 18 90 6f 16 e4"
	testDVSB = "50 24 aa bb cc dd ee ff-00 11 22 33 44 55 66 77"
)

// testVDN builds a cluster-scoped VSphereDistributedNetwork CR shaped like the ones net-operator
// creates on a VDS supervisor.
func testVDN(name, portGroupID, dvsUUID string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "netoperator.vmware.com/v1alpha1",
		"kind":       "VSphereDistributedNetwork",
		"metadata": map[string]interface{}{
			"name":        name,
			"annotations": map[string]interface{}{cnsoperatorutil.AnnotationDVSUUID: dvsUUID},
		},
		"spec": map[string]interface{}{
			"portGroupID": portGroupID,
			"gateway":     "166.168.31.253",
			"subnetMask":  "255.255.0.0",
		},
	}}
}

// testVDSNetwork builds a namespaced net-operator Network of type vsphere-distributed that
// references the named VSphereDistributedNetwork.
func testVDSNetwork(namespace, vdnName string, isDefault bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "netoperator.vmware.com/v1alpha1",
		"kind":       "Network",
		"metadata": map[string]interface{}{
			"name":      vdnName,
			"namespace": namespace,
		},
		"spec": map[string]interface{}{
			"type": cnsoperatorutil.NetworkTypeVSphereDistributed,
			"providerRef": map[string]interface{}{
				"apiGroup":   "netoperator.vmware.com",
				"apiVersion": "v1alpha1",
				"kind":       "VSphereDistributedNetwork",
				"name":       vdnName,
			},
		},
	}}
	if isDefault {
		obj.SetLabels(map[string]string{cnsoperatorutil.LabelDefaultNetwork: "true"})
	}
	return obj
}

func fvsInstanceNamespace(name string) *v1.Namespace {
	return &v1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{NamespaceLabelFVSInstance: "true"},
	}}
}

// vdsTopology is the VDS fixture shared by the tests below:
//
//	pvc-ns   → pg-1 on VDS-A  (consumer, default) and pg-9 on VDS-B (secondary)
//	inst-pg  → pg-1 on VDS-A  → Tier 1 (same portgroup)
//	inst-vds → pg-2 on VDS-A  → Tier 2 (same VDS)
//	inst-far → pg-3 on VDS-C  → no match
func vdsTopologyObjects() []runtime.Object {
	return []runtime.Object{
		testVDN("vdn-pg1", "dvportgroup-1", testDVSA),
		testVDN("vdn-pg2", "dvportgroup-2", "  "+testDVSA+"  "),
		testVDN("vdn-pg3", "dvportgroup-3", "50 24 00 00 00 00 00 00-00 00 00 00 00 00 00 00"),
		testVDN("vdn-pg9", "dvportgroup-9", testDVSB),
		testVDSNetwork("pvc-ns", "vdn-pg1", true),
		testVDSNetwork("pvc-ns", "vdn-pg9", false),
		testVDSNetwork("inst-pg", "vdn-pg1", true),
		testVDSNetwork("inst-vds", "vdn-pg2", true),
		testVDSNetwork("inst-far", "vdn-pg3", true),
	}
}

func withZones(t *testing.T, byNS map[string][]string) {
	t.Helper()
	orig := fvsZonesForNamespace
	t.Cleanup(func() { fvsZonesForNamespace = orig })
	fvsZonesForNamespace = func(ns string) map[string]struct{} {
		out := map[string]struct{}{}
		for _, z := range byNS[ns] {
			out[z] = struct{}{}
		}
		return out
	}
}

func newVDSTestController(t *testing.T, namespaces []*v1.Namespace, dynObjs []runtime.Object,
	fvClient ctrlclient.Client) *controller {
	t.Helper()
	k8sObjs := make([]runtime.Object, 0, len(namespaces))
	for _, ns := range namespaces {
		k8sObjs = append(k8sObjs, ns)
	}
	k8s := k8sfake.NewClientset(k8sObjs...)
	return &controller{
		k8sClient:        k8s,
		dynamicClient:    newTestDynamicClient(t, dynObjs...),
		namespaceLister:  testNamespaceLister(t, k8s),
		fileVolumeClient: fvClient,
	}
}

func TestListFVSCandidateInstanceNamespacesVDS(t *testing.T) {
	ctx := context.Background()
	allZoneA := map[string][]string{
		"pvc-ns": {"zone-a"}, "inst-pg": {"zone-a"}, "inst-vds": {"zone-a"}, "inst-far": {"zone-a"},
	}

	tests := []struct {
		name        string
		namespaces  []*v1.Namespace
		dynObjs     []runtime.Object
		zones       map[string][]string
		want        [][]string
		errContains string
	}{
		{
			name: "Tier 1 group precedes Tier 2 group; different VDS is excluded",
			namespaces: []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
				fvsInstanceNamespace("inst-pg"), fvsInstanceNamespace("inst-vds"), fvsInstanceNamespace("inst-far")},
			dynObjs: vdsTopologyObjects(),
			zones:   allZoneA,
			want:    [][]string{{"inst-pg"}, {"inst-vds"}},
		},
		{
			name: "only Tier 2 available",
			namespaces: []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
				fvsInstanceNamespace("inst-vds"), fvsInstanceNamespace("inst-far")},
			dynObjs: vdsTopologyObjects(),
			zones:   allZoneA,
			want:    [][]string{{"inst-vds"}},
		},
		{
			name: "consumer secondary network on VDS-B matches an instance on VDS-B (Tier 2)",
			namespaces: []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
				fvsInstanceNamespace("inst-b")},
			dynObjs: append(vdsTopologyObjects(),
				testVDN("vdn-pg10", "dvportgroup-10", testDVSB),
				testVDSNetwork("inst-b", "vdn-pg10", true)),
			zones: map[string][]string{"pvc-ns": {"zone-a"}, "inst-b": {"zone-a"}},
			want:  [][]string{{"inst-b"}},
		},
		{
			name: "no portgroup or VDS match is an error (no fallback)",
			namespaces: []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
				fvsInstanceNamespace("inst-far")},
			dynObjs:     vdsTopologyObjects(),
			zones:       allZoneA,
			errContains: "no FVS candidate namespace on the same portgroup or VDS",
		},
		{
			name: "zone filter runs before network matching",
			namespaces: []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
				fvsInstanceNamespace("inst-pg"), fvsInstanceNamespace("inst-vds")},
			dynObjs: vdsTopologyObjects(),
			zones:   map[string][]string{"pvc-ns": {"zone-a"}, "inst-pg": {"zone-b"}, "inst-vds": {"zone-a"}},
			want:    [][]string{{"inst-vds"}},
		},
		{
			name:        "consumer without a VDS Network is an error",
			namespaces:  []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}}, fvsInstanceNamespace("inst-pg")},
			dynObjs:     []runtime.Object{testVDN("vdn-pg1", "dvportgroup-1", testDVSA)},
			zones:       allZoneA,
			errContains: "has no vsphere-distributed Network",
		},
		{
			name: "PVC namespace that is itself an FVS instance namespace is excluded",
			namespaces: []*v1.Namespace{fvsInstanceNamespace("pvc-ns"),
				fvsInstanceNamespace("inst-vds")},
			dynObjs: vdsTopologyObjects(),
			zones:   allZoneA,
			want:    [][]string{{"inst-vds"}},
		},
		{
			name: "instance namespace with a missing VSphereDistributedNetwork is skipped",
			namespaces: []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
				fvsInstanceNamespace("inst-dangling"), fvsInstanceNamespace("inst-vds")},
			dynObjs: append(vdsTopologyObjects(), testVDSNetwork("inst-dangling", "vdn-gone", true)),
			zones: map[string][]string{
				"pvc-ns": {"zone-a"}, "inst-dangling": {"zone-a"}, "inst-vds": {"zone-a"},
			},
			want: [][]string{{"inst-vds"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withZones(t, tt.zones)
			c := newVDSTestController(t, tt.namespaces, tt.dynObjs, nil)
			got, err := c.listFVSCandidateInstanceNamespaces(ctx, "pvc-ns", []string{"zone-a"},
				cnsoperatorutil.VDSNetworkProvider)
			if tt.errContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// newReadyOnCreateFileVolumeClient returns a fake FileVolume client that simulates the FVS Volume
// Manager: a FileVolume is Ready with an endpoint and export path as soon as it is created.
func newReadyOnCreateFileVolumeClient(t *testing.T, endpoint string) ctrlclient.Client {
	t.Helper()
	return ctrlfake.NewClientBuilder().
		WithScheme(newFileVolumeSchemeForTest(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
				opts ...ctrlclient.CreateOption) error {
				if fv, ok := obj.(*fvv1alpha1.FileVolume); ok {
					fv.Status = fvv1alpha1.FileVolumeStatus{
						Phase:      fvv1alpha1.FileVolumePhaseReady,
						Endpoint:   endpoint,
						ExportPath: "/export/" + fv.Name,
					}
				}
				return w.Create(ctx, obj, opts...)
			},
		}).
		Build()
}

func vdsCreateVolumeRequest(pvcUID string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name: "pvc-" + pvcUID,
		Parameters: map[string]string{
			common.AttributePvcNamespace:     "pvc-ns",
			common.AttributePvcName:          "my-pvc",
			common.AttributeStorageClassName: common.StorageClassVsanFileServicePolicy,
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{Segments: map[string]string{v1.LabelTopologyZone: "zone-a"}}},
		},
	}
}

func TestCreateFileVolumeViaFVS_VDS(t *testing.T) {
	ctx := context.Background()
	zones := map[string][]string{"pvc-ns": {"zone-a"}, "inst-pg": {"zone-a"}, "inst-vds": {"zone-a"}}
	namespaces := []*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}},
		fvsInstanceNamespace("inst-pg"), fvsInstanceNamespace("inst-vds")}

	t.Run("creates FileVolume in the Tier 1 namespace when it is healthy", func(t *testing.T) {
		withFastFVSWait(t)
		withZones(t, zones)
		pvcUID := "11111111-aaaa-bbbb-cccc-000000000001"
		dyn := append(vdsTopologyObjects(),
			fvsObj("fvs", "inst-pg", "Ready", "True"),
			fvsObj("fvs", "inst-vds", "Ready", "True"))
		fvClient := newReadyOnCreateFileVolumeClient(t, "166.168.20.5")
		c := newVDSTestController(t, namespaces, dyn, fvClient)

		resp, fault, err := c.createFileVolumeViaFVS(ctx, vdsCreateVolumeRequest(pvcUID),
			cnsoperatorutil.VDSNetworkProvider)
		require.NoError(t, err)
		require.Empty(t, fault)
		require.Equal(t, common.FVSVolumeIDPrefix+"inst-pg:"+pvcUID, resp.Volume.VolumeId)
		require.Equal(t, "166.168.20.5:/export/"+pvcUID,
			resp.Volume.VolumeContext[common.Nfsv4ExportPathAnnotationKey])
		require.Equal(t, common.DiskTypeFileVolume, resp.Volume.VolumeContext[common.AttributeDiskType])
		require.Len(t, resp.Volume.AccessibleTopology, 1)
		require.Equal(t, "zone-a", resp.Volume.AccessibleTopology[0].Segments[v1.LabelTopologyZone])

		fv := &fvv1alpha1.FileVolume{}
		require.NoError(t, fvClient.Get(ctx, ctrlclient.ObjectKey{Namespace: "inst-pg", Name: pvcUID}, fv))
		require.Equal(t, pvcUID, fv.Spec.PvcUID)
		require.Equal(t, []fvv1alpha1.FileVolumeProtocol{fvv1alpha1.FileVolumeProtocolNFSv41}, fv.Spec.Protocols)
	})

	t.Run("falls back to Tier 2 when no Tier 1 namespace is healthy", func(t *testing.T) {
		withFastFVSWait(t)
		withZones(t, zones)
		pvcUID := "11111111-aaaa-bbbb-cccc-000000000002"
		dyn := append(vdsTopologyObjects(),
			fvsObj("fvs", "inst-pg", "Degraded", "False"),
			fvsObj("fvs", "inst-vds", "Ready", "True"))
		c := newVDSTestController(t, namespaces, dyn, newReadyOnCreateFileVolumeClient(t, "166.168.20.6"))

		resp, _, err := c.createFileVolumeViaFVS(ctx, vdsCreateVolumeRequest(pvcUID),
			cnsoperatorutil.VDSNetworkProvider)
		require.NoError(t, err)
		require.Equal(t, common.FVSVolumeIDPrefix+"inst-vds:"+pvcUID, resp.Volume.VolumeId)
	})

	t.Run("reuses an existing FileVolume in a Tier 2 namespace (idempotent retry)", func(t *testing.T) {
		withFastFVSWait(t)
		withZones(t, zones)
		pvcUID := "11111111-aaaa-bbbb-cccc-000000000003"
		existing := &fvv1alpha1.FileVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvcUID, Namespace: "inst-vds"},
			Status: fvv1alpha1.FileVolumeStatus{
				Phase: fvv1alpha1.FileVolumePhaseReady, Endpoint: "166.168.20.7", ExportPath: "/export/x",
			},
		}
		fvClient := ctrlfake.NewClientBuilder().WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).Build()
		dyn := append(vdsTopologyObjects(), fvsObj("fvs", "inst-pg", "Ready", "True"))
		c := newVDSTestController(t, namespaces, dyn, fvClient)

		resp, _, err := c.createFileVolumeViaFVS(ctx, vdsCreateVolumeRequest(pvcUID),
			cnsoperatorutil.VDSNetworkProvider)
		require.NoError(t, err)
		require.Equal(t, common.FVSVolumeIDPrefix+"inst-vds:"+pvcUID, resp.Volume.VolumeId)
		require.Equal(t, "166.168.20.7:/export/x", resp.Volume.VolumeContext[common.Nfsv4ExportPathAnnotationKey])
	})

	t.Run("no reachable instance namespace -> FailedPrecondition", func(t *testing.T) {
		withFastFVSWait(t)
		withZones(t, map[string][]string{"pvc-ns": {"zone-a"}, "inst-far": {"zone-a"}})
		c := newVDSTestController(t,
			[]*v1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "pvc-ns"}}, fvsInstanceNamespace("inst-far")},
			append(vdsTopologyObjects(), fvsObj("fvs", "inst-far", "Ready", "True")),
			newReadyOnCreateFileVolumeClient(t, "166.168.20.8"))

		_, _, err := c.createFileVolumeViaFVS(ctx, vdsCreateVolumeRequest("11111111-aaaa-bbbb-cccc-000000000004"),
			cnsoperatorutil.VDSNetworkProvider)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "same portgroup or VDS")
	})

	t.Run("no healthy FileVolumeService in any tier -> FailedPrecondition", func(t *testing.T) {
		withFastFVSWait(t)
		withZones(t, zones)
		dyn := append(vdsTopologyObjects(),
			fvsObj("fvs", "inst-pg", "Degraded", "False"),
			fvsObj("fvs", "inst-vds", "Degraded", "False"))
		c := newVDSTestController(t, namespaces, dyn, newReadyOnCreateFileVolumeClient(t, "166.168.20.9"))

		_, _, err := c.createFileVolumeViaFVS(ctx, vdsCreateVolumeRequest("11111111-aaaa-bbbb-cccc-000000000005"),
			cnsoperatorutil.VDSNetworkProvider)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "no instance namespace with a healthy FileVolumeService")
	})
}
