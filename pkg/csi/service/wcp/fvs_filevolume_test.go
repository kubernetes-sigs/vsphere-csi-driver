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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	fvsapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume"
	fvv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/filevolume/v1alpha1"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	cnsoperatorutil "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

func TestMain(m *testing.M) {
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		panic(err)
	}
	commonco.ContainerOrchestratorUtility = co
	// Stub out ConnectVslmHook so wcp unit tests don't try to dial the
	// govmomi simulator's /vslm/sdk endpoint (the simulator returns 404).
	cnsvolume.ConnectVslmHook = func(ctx context.Context, vc *cnsvsphere.VirtualCenter) error { return nil }
	m.Run()
}

func TestIsVsanFileServicePolicyStorageClass(t *testing.T) {
	require.True(t, isVsanFileServicePolicyStorageClass(common.StorageClassVsanFileServicePolicy))
	require.True(t, isVsanFileServicePolicyStorageClass(common.StorageClassVsanFileServicePolicyLateBinding))
	require.False(t, isVsanFileServicePolicyStorageClass("some-other-sc"))
	require.False(t, isVsanFileServicePolicyStorageClass(""))
}

func TestVPCPathFromVPCNetworkConfiguration(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"vpcs": []interface{}{
				map[string]interface{}{
					"name":    "fvs-vpc-1",
					"vpcPath": "/orgs/default/projects/default/vpcs/fvs-vpc",
				},
			},
		},
	}}
	require.Equal(t, "/orgs/default/projects/default/vpcs/fvs-vpc", vpcPathFromVPCNetworkConfiguration(obj))

	empty := &unstructured.Unstructured{Object: map[string]interface{}{}}
	require.Equal(t, "", vpcPathFromVPCNetworkConfiguration(empty))

	malformed := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"vpcs": []interface{}{"not-a-map"},
		},
	}}
	require.Equal(t, "", vpcPathFromVPCNetworkConfiguration(malformed))

	nameOnly := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"vpcs": []interface{}{
				map[string]interface{}{"name": "no-path"},
			},
		},
	}}
	require.Equal(t, "", vpcPathFromVPCNetworkConfiguration(nameOnly))
}

func TestTopologyListFromZoneMap(t *testing.T) {
	zones := map[string]struct{}{
		"zone-a": {},
		"zone-b": {},
	}
	topo := topologyListFromZoneMap(zones)
	require.Len(t, topo, 2)
	seen := make(map[string]bool)
	for _, tinfo := range topo {
		z := tinfo.Segments[v1.LabelTopologyZone]
		require.NotEmpty(t, z)
		seen[z] = true
	}
	require.True(t, seen["zone-a"])
	require.True(t, seen["zone-b"])
}

func TestNamespaceHasAnyRequestedZone_EmptyRequested(t *testing.T) {
	require.True(t, namespaceHasAnyRequestedZone("any-ns", nil))
	require.True(t, namespaceHasAnyRequestedZone("any-ns", []string{}))
}

func TestNamespaceHasAnyRequestedZone_Intersection(t *testing.T) {
	orig := fvsZonesForNamespace
	defer func() { fvsZonesForNamespace = orig }()
	fvsZonesForNamespace = func(ns string) map[string]struct{} {
		if ns == "has-zone" {
			return map[string]struct{}{"z1": {}, "z2": {}}
		}
		return nil
	}

	require.True(t, namespaceHasAnyRequestedZone("has-zone", []string{"z1"}))
	require.False(t, namespaceHasAnyRequestedZone("has-zone", []string{"z99"}))
	require.False(t, namespaceHasAnyRequestedZone("other-ns", []string{"z1"}))
}

func TestFvsAccessibleTopology(t *testing.T) {
	orig := fvsZonesForNamespace
	defer func() { fvsZonesForNamespace = orig }()
	fvsZonesForNamespace = func(ns string) map[string]struct{} {
		switch ns {
		case "pvc-ns":
			return map[string]struct{}{"z1": {}, "z2": {}}
		case "inst-ns":
			return map[string]struct{}{"z2": {}, "z3": {}}
		case "inst-no-overlap":
			// shares no zones with pvc-ns → publish all PVC namespace zones
			return map[string]struct{}{"z9": {}}
		case "no-zones":
			return nil
		default:
			return nil
		}
	}

	topo, err := fvsAccessibleTopology("pvc-ns", "inst-ns")
	require.NoError(t, err)
	require.Len(t, topo, 1)
	require.Equal(t, "z2", topo[0].Segments[v1.LabelTopologyZone])

	topo2, err := fvsAccessibleTopology("pvc-ns", "inst-no-overlap")
	require.NoError(t, err)
	require.Len(t, topo2, 2)

	_, err = fvsAccessibleTopology("no-zones", "inst-ns")
	require.Error(t, err)
}

// testNamespaceLister builds a synced Namespace lister for unit tests (production uses controller init).
func testNamespaceLister(t *testing.T, k8s kubernetes.Interface) corelisters.NamespaceLister {
	t.Helper()
	factory := informers.NewSharedInformerFactory(k8s, 0)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	nsInformer := factory.Core().V1().Namespaces().Informer()
	factory.Start(stopCh)
	require.True(t, cache.WaitForCacheSync(stopCh, nsInformer.HasSynced))
	return factory.Core().V1().Namespaces().Lister()
}

func newTestDynamicClient(t *testing.T, objs ...runtime.Object) *fake.FakeDynamicClient {
	t.Helper()
	gvrToListKind := map[schema.GroupVersionResource]string{
		vpcNetworkConfigurationGVR: "VPCNetworkConfigurationList",
		fvsFileVolumeGVR:           "FileVolumeList",
		fvsFileVolumeServiceGVR:    "FileVolumeServiceList",
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

func testVPCNetworkConfigurationCR(name, vpcPath string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "crd.nsx.vmware.com/v1alpha1",
			"kind":       "VPCNetworkConfiguration",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"status": map[string]interface{}{
				"vpcs": []interface{}{
					map[string]interface{}{
						"name":    "display-name-ignored-for-matching",
						"vpcPath": vpcPath,
					},
				},
			},
		},
	}
}

func TestFindVPCNetworkConfigurationForNamespace(t *testing.T) {
	ctx := context.Background()
	vpcCR := "consumer-ns-11111111-1111-1111-1111-111111111111"
	k8s := k8sfake.NewClientset(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "consumer-ns",
			Annotations: map[string]string{
				AnnotationVPCNetworkConfig: vpcCR,
			},
		},
	})
	dyn := newTestDynamicClient(t, testVPCNetworkConfigurationCR(vpcCR, "/orgs/default/projects/default/vpcs/vpc-a"))

	c := &controller{k8sClient: k8s, dynamicClient: dyn, namespaceLister: testNamespaceLister(t, k8s)}
	obj, err := c.findVPCNetworkConfigurationForNamespace(ctx, "consumer-ns")
	require.NoError(t, err)
	require.Equal(t, vpcCR, obj.GetName())
	require.Equal(t, "/orgs/default/projects/default/vpcs/vpc-a", vpcPathFromVPCNetworkConfiguration(obj))

	k8sNoAnn := k8sfake.NewClientset(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "n2"}})
	c2 := &controller{k8sClient: k8sNoAnn, dynamicClient: dyn, namespaceLister: testNamespaceLister(t, k8sNoAnn)}
	_, err = c2.findVPCNetworkConfigurationForNamespace(ctx, "n2")
	require.Error(t, err)
}

func TestGetVPCPathForNamespace(t *testing.T) {
	ctx := context.Background()
	vpcCR := "ns1-22222222-2222-2222-2222-222222222222"
	sharedPath := "/orgs/default/projects/default/vpcs/shared-vpc"
	k8s := k8sfake.NewClientset(&v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ns1",
			Annotations: map[string]string{
				AnnotationVPCNetworkConfig: vpcCR,
			},
		},
	})
	dyn := newTestDynamicClient(t, testVPCNetworkConfigurationCR(vpcCR, sharedPath))

	c := &controller{k8sClient: k8s, dynamicClient: dyn, namespaceLister: testNamespaceLister(t, k8s)}
	path, err := c.getVPCPathForNamespace(ctx, "ns1")
	require.NoError(t, err)
	require.Equal(t, sharedPath, path)

	dynNoStatus := newTestDynamicClient(t, &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "crd.nsx.vmware.com/v1alpha1",
			"kind":       "VPCNetworkConfiguration",
			"metadata":   map[string]interface{}{"name": vpcCR},
			"status":     map[string]interface{}{},
		},
	})
	c2 := &controller{k8sClient: k8s, dynamicClient: dynNoStatus, namespaceLister: testNamespaceLister(t, k8s)}
	_, err = c2.getVPCPathForNamespace(ctx, "ns1")
	require.Error(t, err)
}

func TestListFVSCandidateInstanceNamespaces(t *testing.T) {
	ctx := context.Background()
	consumerAnn := "pvc-ns-33333333-3333-3333-3333-333333333333"
	instAnn := "inst-ns-44444444-4444-4444-4444-444444444444"

	k8s := k8sfake.NewClientset(
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pvc-ns",
				Annotations: map[string]string{
					AnnotationVPCNetworkConfig: consumerAnn,
				},
			},
		},
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "inst-ns",
				Labels: map[string]string{
					NamespaceLabelFVSInstance: "true",
				},
				Annotations: map[string]string{
					AnnotationVPCNetworkConfig: instAnn,
				},
			},
		},
		&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system"}},
	)
	sameVPCPath := "/orgs/default/projects/default/vpcs/same-vpc"
	dyn := newTestDynamicClient(t,
		testVPCNetworkConfigurationCR(consumerAnn, sameVPCPath),
		testVPCNetworkConfigurationCR(instAnn, sameVPCPath),
	)

	origZones := fvsZonesForNamespace
	defer func() { fvsZonesForNamespace = origZones }()
	fvsZonesForNamespace = func(ns string) map[string]struct{} {
		if ns == "pvc-ns" || ns == "inst-ns" {
			return map[string]struct{}{"zone-a": {}}
		}
		return nil
	}

	c := &controller{k8sClient: k8s, dynamicClient: dyn, namespaceLister: testNamespaceLister(t, k8s)}
	out, err := c.listFVSCandidateInstanceNamespaces(ctx, "pvc-ns", []string{"zone-a"})
	require.NoError(t, err)
	require.Equal(t, []string{"inst-ns"}, out)

	_, err = c.listFVSCandidateInstanceNamespaces(ctx, "pvc-ns", nil)
	require.Error(t, err)
}

// fvsObj builds an unstructured FileVolumeService CR for tests with the given healthState and
// optional Service-Ready condition status (pass "" to omit the condition entirely).
func fvsObj(name, namespace, healthState, serviceReadyStatus string) *unstructured.Unstructured {
	status := map[string]interface{}{
		"healthState": healthState,
	}
	if serviceReadyStatus != "" {
		status["conditions"] = []interface{}{
			map[string]interface{}{
				"type":   fvsServiceReadyConditionType,
				"status": serviceReadyStatus,
			},
		}
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "fvs.vcf.broadcom.com/v1alpha1",
			"kind":       "FileVolumeService",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"status": status,
		},
	}
}

func TestInstanceNamespaceHasReadyFileVolumeService(t *testing.T) {
	ctx := context.Background()

	t.Run("healthState Ready and Service-Ready True", func(t *testing.T) {
		dyn := newTestDynamicClient(t, fvsObj("fvs1", "ns1", "Ready", "True"))
		c := &controller{dynamicClient: dyn}
		require.NoError(t, c.instanceNamespaceHasReadyFileVolumeService(ctx, "ns1"))
	})

	t.Run("healthState NotReady is rejected", func(t *testing.T) {
		dyn := newTestDynamicClient(t, fvsObj("fvs2", "ns2", "NotReady", "True"))
		c := &controller{dynamicClient: dyn}
		require.Error(t, c.instanceNamespaceHasReadyFileVolumeService(ctx, "ns2"))
	})

	t.Run("healthState Ready but no Service-Ready condition is rejected", func(t *testing.T) {
		dyn := newTestDynamicClient(t, fvsObj("fvs3", "ns3", "Ready", ""))
		c := &controller{dynamicClient: dyn}
		err := c.instanceNamespaceHasReadyFileVolumeService(ctx, "ns3")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Service-Ready")
	})

	t.Run("healthState Ready but Service-Ready=False is rejected", func(t *testing.T) {
		dyn := newTestDynamicClient(t, fvsObj("fvs4", "ns4", "Ready", "False"))
		c := &controller{dynamicClient: dyn}
		require.Error(t, c.instanceNamespaceHasReadyFileVolumeService(ctx, "ns4"))
	})

	t.Run("skips unhealthy item and finds qualifying one", func(t *testing.T) {
		dyn := newTestDynamicClient(t,
			fvsObj("bad", "ns5", "NotReady", "False"),
			fvsObj("good", "ns5", "Ready", "True"),
		)
		c := &controller{dynamicClient: dyn}
		require.NoError(t, c.instanceNamespaceHasReadyFileVolumeService(ctx, "ns5"))
	})

	t.Run("empty namespace returns error", func(t *testing.T) {
		dyn := newTestDynamicClient(t)
		c := &controller{dynamicClient: dyn}
		require.Error(t, c.instanceNamespaceHasReadyFileVolumeService(ctx, "empty"))
	})
}

func TestCreateFileVolumeViaFVS_ValidationAndPVC(t *testing.T) {
	ctx := context.Background()
	c := &controller{k8sClient: k8sfake.NewClientset()}

	t.Run("missing PVC parameters", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{Parameters: map[string]string{}}
		_, fault, err := c.createFileVolumeViaFVS(ctx, req)
		require.Error(t, err)
		require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
	})

	t.Run("missing accessibility requirements", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{
			Parameters: map[string]string{
				common.AttributePvcNamespace: "ns",
				common.AttributePvcName:      "pvc",
			},
		}
		_, fault, err := c.createFileVolumeViaFVS(ctx, req)
		require.Error(t, err)
		require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
	})

	t.Run("volume name without extractable PVC UID", func(t *testing.T) {
		req := &csi.CreateVolumeRequest{
			Name: "invalid-volume-name",
			Parameters: map[string]string{
				common.AttributePvcNamespace: "ns",
				common.AttributePvcName:      "pvc",
			},
			AccessibilityRequirements: &csi.TopologyRequirement{
				Preferred: []*csi.Topology{{Segments: map[string]string{v1.LabelTopologyZone: "z1"}}},
			},
		}
		_, fault, err := c.createFileVolumeViaFVS(ctx, req)
		require.Error(t, err)
		require.Equal(t, csifault.CSIInternalFault, fault)
		require.Contains(t, err.Error(), "failed to extract PVC UID")
	})
}

// withFVSPackageState saves and restores the package-level FSS booleans + cached global provider
// so subtests can exercise shouldProvisionVsanFileVolumeViaFVS / resolveNetworkProviderForFVS in
// isolation without leaking state across tests.
func withFVSPackageState(t *testing.T) {
	t.Helper()
	oldFVS := isVsanFileVolumeServiceFSSEnabled
	oldPerNS := isPerNamespaceNetworkProvidersFSSEnabled
	oldCache := cachedGlobalNetworkProvider
	t.Cleanup(func() {
		isVsanFileVolumeServiceFSSEnabled = oldFVS
		isPerNamespaceNetworkProvidersFSSEnabled = oldPerNS
		cachedGlobalNetworkProvider = oldCache
	})
}

func TestShouldProvisionVsanFileVolumeViaFVS(t *testing.T) {
	ctx := context.Background()

	t.Run("non-reserved SC returns useFVS=false without provider lookup", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = false
		// Cache is intentionally left empty: helper must not consult the cache when SC is non-reserved.
		cachedGlobalNetworkProvider = ""
		ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns", "other-sc")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("FVS FSS disabled returns useFVS=false for reserved SC (controller guard takes over)",
		func(t *testing.T) {
			withFVSPackageState(t)
			isVsanFileVolumeServiceFSSEnabled = false
			ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns",
				common.StorageClassVsanFileServicePolicy)
			require.NoError(t, err)
			require.False(t, ok)
		})

	t.Run("per-namespace OFF + cache=NSX_VPC + reserved SC routes to FVS", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = false
		cachedGlobalNetworkProvider = cnsoperatorutil.VPCNetworkProvider
		ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns",
			common.StorageClassVsanFileServicePolicy)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("per-namespace OFF + cache=NSXT_CONTAINER_PLUGIN + reserved SC -> FailedPrecondition",
		func(t *testing.T) {
			withFVSPackageState(t)
			isVsanFileVolumeServiceFSSEnabled = true
			isPerNamespaceNetworkProvidersFSSEnabled = false
			cachedGlobalNetworkProvider = cnsoperatorutil.NSXTNetworkProvider
			_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns",
				common.StorageClassVsanFileServicePolicy)
			require.Error(t, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Contains(t, err.Error(), "requires NSX_VPC")
		})

	t.Run("per-namespace OFF + cache=VSPHERE_NETWORK + reserved SC -> FailedPrecondition",
		func(t *testing.T) {
			withFVSPackageState(t)
			isVsanFileVolumeServiceFSSEnabled = true
			isPerNamespaceNetworkProvidersFSSEnabled = false
			cachedGlobalNetworkProvider = cnsoperatorutil.VDSNetworkProvider
			_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns",
				common.StorageClassVsanFileServicePolicyLateBinding)
			require.Error(t, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Contains(t, err.Error(), "requires NSX_VPC")
		})

	t.Run("per-namespace OFF + cache empty + reserved SC -> FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = false
		cachedGlobalNetworkProvider = ""
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns",
			common.StorageClassVsanFileServicePolicy)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "global network provider was not resolved")
	})
}

// networkSettingsGVRForTest mirrors the GVR exported privately in cnsoperatorutil; we redeclare
// it locally so the test doesn't introduce a public API just for tests.
var networkSettingsGVRForTest = schema.GroupVersionResource{
	Group:    "netoperator.vmware.com",
	Version:  "v1alpha1",
	Resource: "networksettings",
}

// newFakeDynamicClientForNetworkSettings builds a fake dynamic client that knows how to list
// NetworkSettings CRs (the fake tracker requires the list-kind to be registered explicitly).
func newFakeDynamicClientForNetworkSettings(t *testing.T) *fake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(networkSettingsGVRForTest.GroupVersion())
	gvrToListKind := map[schema.GroupVersionResource]string{
		networkSettingsGVRForTest: "NetworkSettingsList",
	}
	return fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)
}

// seedNetworkSettings creates a NetworkSettings CR via the dynamic client (matches the
// production code path used by getNetworkProviderFromNetworkSettings). Pass provider="" to omit
// the provider field, mirroring a misconfigured CR.
func seedNetworkSettings(t *testing.T, dc *fake.FakeDynamicClient, name, namespace, provider string) {
	t.Helper()
	obj := map[string]interface{}{
		"apiVersion": "netoperator.vmware.com/v1alpha1",
		"kind":       "NetworkSettings",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": namespace,
		},
	}
	if provider != "" {
		obj["provider"] = provider
	}
	cr := &unstructured.Unstructured{Object: obj}
	_, err := dc.Resource(networkSettingsGVRForTest).Namespace(namespace).Create(
		context.Background(), cr, metav1.CreateOptions{})
	require.NoError(t, err)
}

func TestShouldProvisionVsanFileVolumeViaFVS_PerNamespace(t *testing.T) {
	ctx := context.Background()
	const ns = "tenant-ns"

	t.Run("per-namespace ON + provider=vpc routes to FVS", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = true
		dc := newFakeDynamicClientForNetworkSettings(t)
		seedNetworkSettings(t, dc, "ns-1", ns, "vpc")
		ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, dc, ns,
			common.StorageClassVsanFileServicePolicy)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("per-namespace ON + provider=nsx-tier1 -> FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = true
		dc := newFakeDynamicClientForNetworkSettings(t)
		seedNetworkSettings(t, dc, "ns-1", ns, "nsx-tier1")
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, dc, ns,
			common.StorageClassVsanFileServicePolicy)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "requires NSX_VPC")
	})

	t.Run("per-namespace ON + provider=vsphere-distributed -> FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = true
		dc := newFakeDynamicClientForNetworkSettings(t)
		seedNetworkSettings(t, dc, "ns-1", ns, "vsphere-distributed")
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, dc, ns,
			common.StorageClassVsanFileServicePolicyLateBinding)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "requires NSX_VPC")
	})

	t.Run("per-namespace ON + NetworkSettings absent -> FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = true
		dc := newFakeDynamicClientForNetworkSettings(t)
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, dc, ns,
			common.StorageClassVsanFileServicePolicy)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err),
			"ErrNetworkSettingsUnavailable must be re-wrapped as FailedPrecondition, got code: %v", status.Code(err))
		require.Contains(t, err.Error(), "failed to resolve network provider from NetworkSettings")
	})

	t.Run("per-namespace ON + provider field empty -> FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = true
		dc := newFakeDynamicClientForNetworkSettings(t)
		seedNetworkSettings(t, dc, "ns-1", ns, "")
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, dc, ns,
			common.StorageClassVsanFileServicePolicy)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err),
			"empty-provider NetworkSettings must be re-wrapped as FailedPrecondition, got code: %v", status.Code(err))
		require.Contains(t, err.Error(), "failed to resolve network provider from NetworkSettings")
	})

	t.Run("per-namespace ON + dynamic client nil -> FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isVsanFileVolumeServiceFSSEnabled = true
		isPerNamespaceNetworkProvidersFSSEnabled = true
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, ns,
			common.StorageClassVsanFileServicePolicy)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "dynamic client unavailable")
	})
}

func TestResolveNetworkProviderForFVS(t *testing.T) {
	ctx := context.Background()

	t.Run("per-namespace OFF returns cached global provider without dc lookup", func(t *testing.T) {
		withFVSPackageState(t)
		isPerNamespaceNetworkProvidersFSSEnabled = false
		cachedGlobalNetworkProvider = cnsoperatorutil.VPCNetworkProvider
		got, err := resolveNetworkProviderForFVS(ctx, nil, "ignored")
		require.NoError(t, err)
		require.Equal(t, cnsoperatorutil.VPCNetworkProvider, got)
	})

	t.Run("per-namespace OFF + empty cache returns FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isPerNamespaceNetworkProvidersFSSEnabled = false
		cachedGlobalNetworkProvider = ""
		_, err := resolveNetworkProviderForFVS(ctx, nil, "ignored")
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("per-namespace ON reads from NetworkSettings CR", func(t *testing.T) {
		withFVSPackageState(t)
		isPerNamespaceNetworkProvidersFSSEnabled = true
		cachedGlobalNetworkProvider = "should-not-be-used"
		dc := newFakeDynamicClientForNetworkSettings(t)
		seedNetworkSettings(t, dc, "ns-1", "tenant", "vpc")
		got, err := resolveNetworkProviderForFVS(ctx, dc, "tenant")
		require.NoError(t, err)
		require.Equal(t, cnsoperatorutil.VPCNetworkProvider, got)
	})

	t.Run("per-namespace ON + nil dc returns FailedPrecondition", func(t *testing.T) {
		withFVSPackageState(t)
		isPerNamespaceNetworkProvidersFSSEnabled = true
		_, err := resolveNetworkProviderForFVS(ctx, nil, "tenant")
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("per-namespace ON + NetworkSettings lookup error is wrapped as FailedPrecondition", func(t *testing.T) {
		// Validates the review comment: plain errors from getNetworkProviderFromNetworkSettings
		// (e.g. ErrNetworkSettingsUnavailable) must be re-wrapped so callers see
		// codes.FailedPrecondition rather than codes.Unknown.
		withFVSPackageState(t)
		isPerNamespaceNetworkProvidersFSSEnabled = true
		dc := newFakeDynamicClientForNetworkSettings(t) // no CR seeded -> ErrNetworkSettingsUnavailable
		_, err := resolveNetworkProviderForFVS(ctx, dc, "tenant")
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err),
			"plain error from getNetworkProviderFromNetworkSettings must be wrapped as FailedPrecondition")
	})
}

func TestShouldDeleteFileVolumeViaFVS(t *testing.T) {
	tests := []struct {
		name       string
		fssEnabled bool
		volumeID   string
		want       bool
	}{
		{"FSS off, non-FVS handle", false, "abc-block-vol", false},
		{"FSS off, FVS handle", false, common.FVSVolumeIDPrefix + "tenant-ns:fv-foo", false},
		{"FSS off, empty handle", false, "", false},
		{"FSS on, non-FVS handle", true, "abc-block-vol", false},
		{"FSS on, legacy file handle", true, "file:abcd-1234", false},
		{"FSS on, empty handle", true, "", false},
		{"FSS on, FVS prefix only", true, common.FVSVolumeIDPrefix, true},
		{"FSS on, FVS handle", true, common.FVSVolumeIDPrefix + "tenant-ns:fv-foo", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldFSS := isVsanFileVolumeServiceFSSEnabled
			defer func() { isVsanFileVolumeServiceFSSEnabled = oldFSS }()
			isVsanFileVolumeServiceFSSEnabled = tt.fssEnabled

			got := shouldDeleteFileVolumeViaFVS(tt.volumeID)
			require.Equalf(t, tt.want, got,
				"shouldDeleteFileVolumeViaFVS(%q) with FSS=%v: want %v, got %v",
				tt.volumeID, tt.fssEnabled, tt.want, got)
		})
	}
}

// newFileVolumeSchemeForTest returns a runtime.Scheme with the FVS FileVolume types registered so
// the controller-runtime fake client can serialize FileVolume objects.
func newFileVolumeSchemeForTest(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, fvsapis.AddToScheme(s))
	return s
}

// withFastFVSWait dials fvsWaitStep / fvsWaitMax down for tests that exercise the delete-poll loop
// so a missed-NotFound iteration costs milliseconds, not seconds.
func withFastFVSWait(t *testing.T) {
	t.Helper()
	prevStep, prevMax := fvsWaitStep, fvsWaitMax
	fvsWaitStep = 5 * time.Millisecond
	fvsWaitMax = 200 * time.Millisecond
	t.Cleanup(func() { fvsWaitStep, fvsWaitMax = prevStep, prevMax })
}

func TestDeleteFileVolumeViaFVS_Success(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-success"
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			Build(),
	}

	resp, fault, err := c.deleteFileVolumeViaFVS(ctx, common.FVSVolumeIDPrefix+instanceNS+":"+fvName)
	require.NoError(t, err)
	require.Empty(t, fault)
	require.NotNil(t, resp)

	cur := &fvv1alpha1.FileVolume{}
	getErr := c.fileVolumeClient.Get(ctx,
		ctrlclient.ObjectKey{Namespace: instanceNS, Name: fvName}, cur)
	require.True(t, apierrors.IsNotFound(getErr),
		"FileVolume %s/%s should be gone after deleteFileVolumeViaFVS, got err %v",
		instanceNS, fvName, getErr)
}

func TestDeleteFileVolumeViaFVS_NotFoundIsIdempotent(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			Build(),
	}

	resp, fault, err := c.deleteFileVolumeViaFVS(ctx,
		common.FVSVolumeIDPrefix+"missing-ns:missing-fv")
	require.NoError(t, err)
	require.Empty(t, fault)
	require.NotNil(t, resp)
}

func TestDeleteFileVolumeViaFVS_InvalidVolumeID(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			Build(),
	}

	tests := []struct {
		name     string
		volumeID string
	}{
		{"missing FVS prefix", "tenant-ns:fv-foo"},
		{"prefix only", common.FVSVolumeIDPrefix},
		{"missing namespace", common.FVSVolumeIDPrefix + ":fv-foo"},
		{"missing name", common.FVSVolumeIDPrefix + "tenant-ns:"},
		{"missing separator after namespace", common.FVSVolumeIDPrefix + "tenant-ns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, fault, err := c.deleteFileVolumeViaFVS(ctx, tt.volumeID)
			require.Error(t, err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
		})
	}
}

func TestDeleteFileVolumeViaFVS_NilClient(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	c := &controller{}
	_, fault, err := c.deleteFileVolumeViaFVS(ctx,
		common.FVSVolumeIDPrefix+"any-ns:any-fv")
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "FileVolume client is not initialized")
}

func TestDeleteFileVolumeViaFVS_DeleteError(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-error"
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
	}
	injectedErr := errors.New("api server temporarily unavailable")
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
					opts ...ctrlclient.DeleteOption) error {
					if _, ok := obj.(*fvv1alpha1.FileVolume); ok {
						return injectedErr
					}
					return w.Delete(ctx, obj, opts...)
				},
			}).
			Build(),
	}

	_, fault, err := c.deleteFileVolumeViaFVS(ctx,
		common.FVSVolumeIDPrefix+instanceNS+":"+fvName)
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "failed to delete FileVolume")
}

func TestShouldExpandFileVolumeViaFVS(t *testing.T) {
	tests := []struct {
		name       string
		fssEnabled bool
		volumeID   string
		want       bool
	}{
		{"FSS off, non-FVS handle", false, "abc-block-vol", false},
		{"FSS off, FVS handle", false, common.FVSVolumeIDPrefix + "tenant-ns:fv-foo", false},
		{"FSS off, empty handle", false, "", false},
		{"FSS on, non-FVS handle", true, "abc-block-vol", false},
		{"FSS on, legacy file handle", true, "file:abcd-1234", false},
		{"FSS on, empty handle", true, "", false},
		{"FSS on, FVS prefix only", true, common.FVSVolumeIDPrefix, true},
		{"FSS on, FVS handle", true, common.FVSVolumeIDPrefix + "tenant-ns:fv-foo", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldFSS := isVsanFileVolumeServiceFSSEnabled
			defer func() { isVsanFileVolumeServiceFSSEnabled = oldFSS }()
			isVsanFileVolumeServiceFSSEnabled = tt.fssEnabled

			got := shouldExpandFileVolumeViaFVS(tt.volumeID)
			require.Equalf(t, tt.want, got,
				"shouldExpandFileVolumeViaFVS(%q) with FSS=%v: want %v, got %v",
				tt.volumeID, tt.fssEnabled, tt.want, got)
		})
	}
}

// expandRequest builds a minimal ControllerExpandVolumeRequest for the supplied FVS volume id and
// requested size in bytes.
func expandRequest(volumeID string, sizeBytes int64) *csi.ControllerExpandVolumeRequest {
	return &csi.ControllerExpandVolumeRequest{
		VolumeId: volumeID,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: sizeBytes,
		},
	}
}

// TestExpandFileVolumeViaFVS_Success: FVS controller reflects the new spec.size into
// status.lastAppliedSize on Update; CSI sees lastAppliedSize == desired and returns success with
// NodeExpansionRequired=false.
func TestExpandFileVolumeViaFVS_Success(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-success"
		oldGiB     = int64(1)
		newGiB     = int64(5)
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:           fvv1alpha1.FileVolumePhaseReady,
			LastAppliedSize: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
					opts ...ctrlclient.UpdateOption) error {
					if fv, ok := obj.(*fvv1alpha1.FileVolume); ok {
						// Simulate FVS controller reconciling spec->status in lockstep so the poll
						// completes on the next iteration without flake.
						fv.Status.LastAppliedSize = fv.Spec.Size
						fv.Status.Phase = fvv1alpha1.FileVolumePhaseReady
					}
					return w.Update(ctx, obj, opts...)
				},
			}).
			Build(),
	}

	desiredBytes := newGiB << 30
	resp, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, desiredBytes))
	require.NoError(t, err)
	require.Empty(t, fault)
	require.NotNil(t, resp)
	require.Equal(t, desiredBytes, resp.CapacityBytes)
	require.False(t, resp.NodeExpansionRequired,
		"FVS file volumes should never require node-side expansion (NFS server handles growth)")

	cur := &fvv1alpha1.FileVolume{}
	require.NoError(t, c.fileVolumeClient.Get(ctx,
		ctrlclient.ObjectKey{Namespace: instanceNS, Name: fvName}, cur))
	require.Equal(t, desiredBytes, cur.Spec.Size.Value(),
		"FileVolume spec.size should have been bumped to the requested capacity")
}

// TestExpandFileVolumeViaFVS_Idempotent: spec.size is already at or above the requested size and
// status has converged. CSI should not write to spec and should return success immediately.
func TestExpandFileVolumeViaFVS_Idempotent(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-idem"
		sizeGiB    = int64(3)
	)
	current := *resource.NewQuantity(sizeGiB<<30, resource.BinarySI)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec:       fvv1alpha1.FileVolumeSpec{Size: current},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:           fvv1alpha1.FileVolumePhaseReady,
			LastAppliedSize: current,
		},
	}
	updateCalls := 0
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
					opts ...ctrlclient.UpdateOption) error {
					if _, ok := obj.(*fvv1alpha1.FileVolume); ok {
						updateCalls++
					}
					return w.Update(ctx, obj, opts...)
				},
			}).
			Build(),
	}

	resp, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, sizeGiB<<30))
	require.NoError(t, err)
	require.Empty(t, fault)
	require.Equal(t, sizeGiB<<30, resp.CapacityBytes)
	require.False(t, resp.NodeExpansionRequired)
	require.Zero(t, updateCalls,
		"idempotent expand on already-applied size must not write to FileVolume spec")
}

func TestExpandFileVolumeViaFVS_Shrink(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-shrink"
		currentGiB = int64(5)
		smallerGiB = int64(2)
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size: *resource.NewQuantity(currentGiB<<30, resource.BinarySI),
		},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:           fvv1alpha1.FileVolumePhaseReady,
			LastAppliedSize: *resource.NewQuantity(currentGiB<<30, resource.BinarySI),
		},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			Build(),
	}

	_, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, smallerGiB<<30))
	require.Error(t, err)
	require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
	require.Contains(t, err.Error(), "shrinking FVS file volume is not supported")
}

func TestExpandFileVolumeViaFVS_InvalidVolumeID(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			Build(),
	}

	tests := []struct {
		name     string
		volumeID string
	}{
		{"missing FVS prefix", "tenant-ns:fv-foo"},
		{"prefix only", common.FVSVolumeIDPrefix},
		{"missing namespace", common.FVSVolumeIDPrefix + ":fv-foo"},
		{"missing name", common.FVSVolumeIDPrefix + "tenant-ns:"},
		{"missing separator after namespace", common.FVSVolumeIDPrefix + "tenant-ns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, fault, err := c.expandFileVolumeViaFVS(ctx, expandRequest(tt.volumeID, 1<<30))
			require.Error(t, err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
		})
	}
}

func TestExpandFileVolumeViaFVS_NilClient(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	c := &controller{}
	_, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+"any-ns:any-fv", 1<<30))
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "FileVolume client is not initialized")
}

func TestExpandFileVolumeViaFVS_MissingCapacityRange(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-no-cap"
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size: *resource.NewQuantity(1<<30, resource.BinarySI),
		},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			Build(),
	}

	tests := []struct {
		name string
		req  *csi.ControllerExpandVolumeRequest
	}{
		{"nil capacity range", &csi.ControllerExpandVolumeRequest{
			VolumeId: common.FVSVolumeIDPrefix + instanceNS + ":" + fvName,
		}},
		{"zero required bytes", expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, 0)},
		{"negative required bytes", expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, -1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, fault, err := c.expandFileVolumeViaFVS(ctx, tt.req)
			require.Error(t, err)
			require.Equal(t, csifault.CSIInvalidArgumentFault, fault)
			require.Contains(t, err.Error(), "capacity range with positive required_bytes is required")
		})
	}
}

func TestExpandFileVolumeViaFVS_NotFound(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			Build(),
	}

	_, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+"missing-ns:missing-fv", 2<<30))
	require.Error(t, err)
	require.Equal(t, csifault.CSINotFoundFault, fault)
}

func TestExpandFileVolumeViaFVS_UpdateError(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-update-error"
		oldGiB     = int64(1)
		newGiB     = int64(5)
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:           fvv1alpha1.FileVolumePhaseReady,
			LastAppliedSize: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
	}
	injectedErr := errors.New("api server temporarily unavailable")
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
					opts ...ctrlclient.UpdateOption) error {
					if _, ok := obj.(*fvv1alpha1.FileVolume); ok {
						return injectedErr
					}
					return w.Update(ctx, obj, opts...)
				},
			}).
			Build(),
	}

	_, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, newGiB<<30))
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "failed to update spec.size on FileVolume")
}

// TestExpandFileVolumeViaFVS_StatusNeverApplies simulates an FVS controller that accepts the spec
// update but never bumps status.lastAppliedSize: the CSI expand should fail with DeadlineExceeded
// after fvsWaitMax expires (dialed down via withFastFVSWait).
func TestExpandFileVolumeViaFVS_StatusNeverApplies(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-stuck-status"
		oldGiB     = int64(1)
		newGiB     = int64(5)
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:           fvv1alpha1.FileVolumePhaseReady,
			LastAppliedSize: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
					opts ...ctrlclient.UpdateOption) error {
					// Persist spec bump but explicitly do NOT bump status.lastAppliedSize, simulating
					// a stuck FVS controller.
					if fv, ok := obj.(*fvv1alpha1.FileVolume); ok {
						fv.Status.LastAppliedSize = *resource.NewQuantity(oldGiB<<30, resource.BinarySI)
					}
					return w.Update(ctx, obj, opts...)
				},
			}).
			Build(),
	}

	_, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, newGiB<<30))
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "timeout or error waiting for FileVolume")
}

// TestExpandFileVolumeViaFVS_ErrorPhase: the FVS controller reports phase=Error mid-expansion;
// CSI must surface a clear DeadlineExceeded with an error message naming the Error phase rather
// than spinning until fvsWaitMax.
func TestExpandFileVolumeViaFVS_ErrorPhase(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-error-phase"
		oldGiB     = int64(1)
		newGiB     = int64(5)
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: fvName, Namespace: instanceNS},
		Spec: fvv1alpha1.FileVolumeSpec{
			Size: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase:           fvv1alpha1.FileVolumePhaseReady,
			LastAppliedSize: *resource.NewQuantity(oldGiB<<30, resource.BinarySI),
		},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			WithInterceptorFuncs(interceptor.Funcs{
				Update: func(ctx context.Context, w ctrlclient.WithWatch, obj ctrlclient.Object,
					opts ...ctrlclient.UpdateOption) error {
					if fv, ok := obj.(*fvv1alpha1.FileVolume); ok {
						fv.Status.Phase = fvv1alpha1.FileVolumePhaseError
						fv.Status.Conditions = []metav1.Condition{
							{
								Type:    "BackendReady",
								Status:  metav1.ConditionFalse,
								Reason:  "VdfsConnectionFailed",
								Message: "resize: vdfs grpc dial vsock:2:1570: context deadline exceeded",
							},
						}
					}
					return w.Update(ctx, obj, opts...)
				},
			}).
			Build(),
	}

	_, fault, err := c.expandFileVolumeViaFVS(ctx,
		expandRequest(common.FVSVolumeIDPrefix+instanceNS+":"+fvName, newGiB<<30))
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "Error phase")
	// The FileVolume's failing condition must surface so the resize event names the backend cause.
	require.Contains(t, err.Error(), "VdfsConnectionFailed")
	require.Contains(t, err.Error(), "vdfs grpc dial")
}

// TestDeleteFileVolumeViaFVS_StuckFinalizer simulates an FVS controller that has not yet drained
// its finalizer: Delete returns success but Get keeps returning the CR. The CSI delete should fail
// with DeadlineExceeded after fvsWaitMax expires (dialed down via withFastFVSWait).
func TestDeleteFileVolumeViaFVS_StuckFinalizer(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)

	const (
		instanceNS = "fvs-instance-ns"
		fvName     = "fv-stuck"
	)
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:       fvName,
			Namespace:  instanceNS,
			Finalizers: []string{"fvs.vcf.broadcom.com/finalizer"},
		},
	}
	c := &controller{
		fileVolumeClient: ctrlfake.NewClientBuilder().
			WithScheme(newFileVolumeSchemeForTest(t)).
			WithObjects(existing).
			Build(),
	}

	_, fault, err := c.deleteFileVolumeViaFVS(ctx,
		common.FVSVolumeIDPrefix+instanceNS+":"+fvName)
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "timeout or error waiting for FileVolume")
}

// TestCreateFileVolumeViaFVS_ExistingFVGetError verifies that when the candidate FileVolume Get
// returns a non-NotFound error, createFileVolumeViaFVS returns CSIInternalFault rather than
// silently moving on, which would risk creating a duplicate FileVolume CR (orphan risk).
func TestCreateFileVolumeViaFVS_ExistingFVGetError(t *testing.T) {
	ctx := context.Background()
	consumerAnn := "pvc-ns-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	instAnn := "inst-ns-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	sameVPCPath := "/orgs/default/projects/default/vpcs/same-vpc"

	k8s := k8sfake.NewClientset(
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pvc-ns",
				Annotations: map[string]string{
					AnnotationVPCNetworkConfig: consumerAnn,
				},
			},
		},
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "inst-ns",
				Labels: map[string]string{
					NamespaceLabelFVSInstance: "true",
				},
				Annotations: map[string]string{
					AnnotationVPCNetworkConfig: instAnn,
				},
			},
		},
	)
	dyn := newTestDynamicClient(t,
		testVPCNetworkConfigurationCR(consumerAnn, sameVPCPath),
		testVPCNetworkConfigurationCR(instAnn, sameVPCPath),
	)

	origZones := fvsZonesForNamespace
	defer func() { fvsZonesForNamespace = origZones }()
	fvsZonesForNamespace = func(ns string) map[string]struct{} {
		if ns == "pvc-ns" || ns == "inst-ns" {
			return map[string]struct{}{"zone-a": {}}
		}
		return nil
	}

	scheme := newFileVolumeSchemeForTest(t)
	base := ctrlfake.NewClientBuilder().WithScheme(scheme).Build()
	injectedErr := errors.New("simulated transient API error")
	fvClient := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, _ ctrlclient.WithWatch, _ ctrlclient.ObjectKey,
			_ ctrlclient.Object, _ ...ctrlclient.GetOption) error {
			return injectedErr
		},
	})

	c := &controller{
		k8sClient:        k8s,
		dynamicClient:    dyn,
		namespaceLister:  testNamespaceLister(t, k8s),
		fileVolumeClient: fvClient,
	}

	req := &csi.CreateVolumeRequest{
		Name: "pvc-cccccccc-cccc-cccc-cccc-cccccccccccc",
		Parameters: map[string]string{
			common.AttributePvcNamespace: "pvc-ns",
			common.AttributePvcName:      "my-pvc",
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{Segments: map[string]string{v1.LabelTopologyZone: "zone-a"}}},
		},
	}

	resp, fault, err := c.createFileVolumeViaFVS(ctx, req)
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "failed to check for existing FileVolume")
}

// TestSummarizeFileVolumeConditions verifies that only not-True conditions are rendered, with their
// reason and message, so the underlying backend failure surfaces in the CSI error.
func TestSummarizeFileVolumeConditions(t *testing.T) {
	cases := []struct {
		name        string
		conditions  []metav1.Condition
		wantExact   string
		wantContain []string
	}{
		{
			name:      "no conditions",
			wantExact: "no conditions reported",
		},
		{
			name: "only true conditions are excluded",
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Provisioned", Message: "all good"},
			},
			wantExact: "no failing conditions reported",
		},
		{
			name: "single failing backend condition",
			conditions: []metav1.Condition{
				{
					Type:    "BackendReady",
					Status:  metav1.ConditionFalse,
					Reason:  "VdfsConnectionFailed",
					Message: "create: vdfs grpc dial vsock:2:1570: context deadline exceeded",
				},
			},
			wantContain: []string{"BackendReady=VdfsConnectionFailed", "vdfs grpc dial vsock:2:1570"},
		},
		{
			name: "multiple failing conditions joined",
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionFalse, Reason: "BackendVolumeAssignmentNotReady",
					Message: "One or more required conditions are not ready"},
				{Type: "BackendReady", Status: metav1.ConditionFalse, Reason: "VdfsConnectionFailed",
					Message: "context deadline exceeded"},
			},
			wantContain: []string{
				"Ready=BackendVolumeAssignmentNotReady",
				"BackendReady=VdfsConnectionFailed",
				"; ",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fv := &fvv1alpha1.FileVolume{Status: fvv1alpha1.FileVolumeStatus{Conditions: tc.conditions}}
			got := summarizeFileVolumeConditions(fv)
			if tc.wantExact != "" {
				require.Equal(t, tc.wantExact, got)
			}
			for _, sub := range tc.wantContain {
				require.Contains(t, got, sub)
			}
		})
	}
}

// TestCreateFileVolumeViaFVS_ErrorPhasePropagatesCondition verifies that when an existing FileVolume
// is in Error phase, createFileVolumeViaFVS surfaces the failing condition (reason + message) in the
// returned error so the consuming PVC event names the backend cause rather than only "Error phase".
func TestCreateFileVolumeViaFVS_ErrorPhasePropagatesCondition(t *testing.T) {
	ctx := context.Background()
	withFastFVSWait(t)
	consumerAnn := "pvc-ns-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	instAnn := "inst-ns-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	sameVPCPath := "/orgs/default/projects/default/vpcs/same-vpc"
	pvcUID := "cccccccc-cccc-cccc-cccc-cccccccccccc"

	k8s := k8sfake.NewClientset(
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pvc-ns",
				Annotations: map[string]string{AnnotationVPCNetworkConfig: consumerAnn},
			},
		},
		&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "inst-ns",
				Labels:      map[string]string{NamespaceLabelFVSInstance: "true"},
				Annotations: map[string]string{AnnotationVPCNetworkConfig: instAnn},
			},
		},
	)
	dyn := newTestDynamicClient(t,
		testVPCNetworkConfigurationCR(consumerAnn, sameVPCPath),
		testVPCNetworkConfigurationCR(instAnn, sameVPCPath),
	)

	origZones := fvsZonesForNamespace
	defer func() { fvsZonesForNamespace = origZones }()
	fvsZonesForNamespace = func(ns string) map[string]struct{} {
		if ns == "pvc-ns" || ns == "inst-ns" {
			return map[string]struct{}{"zone-a": {}}
		}
		return nil
	}

	// Existing FileVolume already in Error phase carrying the backend failure condition. This exercises
	// the Phase 1 reuse path, so no healthy FileVolumeService is required.
	existing := &fvv1alpha1.FileVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvcUID, Namespace: "inst-ns"},
		Status: fvv1alpha1.FileVolumeStatus{
			Phase: fvv1alpha1.FileVolumePhaseError,
			Conditions: []metav1.Condition{
				{
					Type:    "BackendReady",
					Status:  metav1.ConditionFalse,
					Reason:  "VdfsConnectionFailed",
					Message: "create: vdfs grpc dial vsock:2:1570: context deadline exceeded",
				},
			},
		},
	}
	fvClient := ctrlfake.NewClientBuilder().
		WithScheme(newFileVolumeSchemeForTest(t)).
		WithObjects(existing).
		Build()

	c := &controller{
		k8sClient:        k8s,
		dynamicClient:    dyn,
		namespaceLister:  testNamespaceLister(t, k8s),
		fileVolumeClient: fvClient,
	}

	req := &csi.CreateVolumeRequest{
		Name: "pvc-" + pvcUID,
		Parameters: map[string]string{
			common.AttributePvcNamespace: "pvc-ns",
			common.AttributePvcName:      "my-pvc",
		},
		AccessibilityRequirements: &csi.TopologyRequirement{
			Preferred: []*csi.Topology{{Segments: map[string]string{v1.LabelTopologyZone: "zone-a"}}},
		},
	}

	resp, fault, err := c.createFileVolumeViaFVS(ctx, req)
	require.Nil(t, resp)
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "Error phase")
	require.Contains(t, err.Error(), "VdfsConnectionFailed")
	require.Contains(t, err.Error(), "vdfs grpc dial")
}

// TestReservedFVSStorageClassGuard documents the invariant the new CreateVolume guard relies on:
// the reserved vSAN file-service storage classes are recognised by isVsanFileServicePolicyStorageClass,
// and when the FVS FSS is disabled, shouldProvisionVsanFileVolumeViaFVS returns useFVS=false. The
// conjunction of these two booleans is what the guard at controller.go uses to reject requests for
// the reserved StorageClass when FVS routing is unavailable.
func TestReservedFVSStorageClassGuard(t *testing.T) {
	ctx := context.Background()

	require.True(t, isVsanFileServicePolicyStorageClass(common.StorageClassVsanFileServicePolicy))
	require.True(t, isVsanFileServicePolicyStorageClass(common.StorageClassVsanFileServicePolicyLateBinding))
	require.False(t, isVsanFileServicePolicyStorageClass("ordinary-sc"))

	withFVSPackageState(t)
	isVsanFileVolumeServiceFSSEnabled = false

	for _, sc := range []string{
		common.StorageClassVsanFileServicePolicy,
		common.StorageClassVsanFileServicePolicyLateBinding,
	} {
		useFVS, err := shouldProvisionVsanFileVolumeViaFVS(ctx, nil, "ns", sc)
		require.NoError(t, err, "sc=%s", sc)
		require.False(t, useFVS, "sc=%s: with FSS disabled the guard must reject by hitting the reserved-SC branch", sc)
		require.True(t, isVsanFileServicePolicyStorageClass(sc),
			"sc=%s: reserved-SC predicate must remain true so the guard rejects rather than falling through", sc)
	}
}
