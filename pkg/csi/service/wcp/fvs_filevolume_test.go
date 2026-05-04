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
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
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

func TestInstanceNamespaceHasReadyFileVolumeService(t *testing.T) {
	ctx := context.Background()
	fvs := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "fvs.vcf.broadcom.com/v1alpha1",
			"kind":       "FileVolumeService",
			"metadata": map[string]interface{}{
				"name":      "fvs1",
				"namespace": "ns1",
			},
			"status": map[string]interface{}{
				"healthState": "Ready",
			},
		},
	}
	dyn := newTestDynamicClient(t, fvs)
	c := &controller{dynamicClient: dyn}
	require.NoError(t, c.instanceNamespaceHasReadyFileVolumeService(ctx, "ns1"))

	notReady := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "fvs.vcf.broadcom.com/v1alpha1",
			"kind":       "FileVolumeService",
			"metadata": map[string]interface{}{
				"name":      "fvs2",
				"namespace": "ns2",
			},
			"status": map[string]interface{}{
				"healthState": "NotReady",
			},
		},
	}
	dyn2 := newTestDynamicClient(t, notReady)
	c2 := &controller{dynamicClient: dyn2}
	require.Error(t, c2.instanceNamespaceHasReadyFileVolumeService(ctx, "ns2"))

	dynEmpty := newTestDynamicClient(t)
	c3 := &controller{dynamicClient: dynEmpty}
	require.Error(t, c3.instanceNamespaceHasReadyFileVolumeService(ctx, "empty"))
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

func TestShouldProvisionVsanFileVolumeViaFVS(t *testing.T) {
	ctx := context.Background()

	ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, "other-sc")
	require.NoError(t, err)
	require.False(t, ok)

	t.Run("non-VPC network provider returns FailedPrecondition", func(t *testing.T) {
		orig := cnsoperatorutil.GetNetworkProviderFunc
		defer func() { cnsoperatorutil.GetNetworkProviderFunc = orig }()
		cnsoperatorutil.GetNetworkProviderFunc = func(ctx context.Context) (string, error) {
			return "NSX_T", nil
		}
		_, err := shouldProvisionVsanFileVolumeViaFVS(ctx, common.StorageClassVsanFileServicePolicy)
		require.Error(t, err)
	})

	t.Run("VPC provider FSS disabled", func(t *testing.T) {
		orig := cnsoperatorutil.GetNetworkProviderFunc
		defer func() { cnsoperatorutil.GetNetworkProviderFunc = orig }()
		cnsoperatorutil.GetNetworkProviderFunc = func(ctx context.Context) (string, error) {
			return cnsoperatorutil.VPCNetworkProvider, nil
		}
		oldFSS := isVsanFileVolumeServiceFSSEnabled
		defer func() { isVsanFileVolumeServiceFSSEnabled = oldFSS }()
		isVsanFileVolumeServiceFSSEnabled = false
		ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, common.StorageClassVsanFileServicePolicy)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("VPC provider FSS enabled", func(t *testing.T) {
		orig := cnsoperatorutil.GetNetworkProviderFunc
		defer func() { cnsoperatorutil.GetNetworkProviderFunc = orig }()
		cnsoperatorutil.GetNetworkProviderFunc = func(ctx context.Context) (string, error) {
			return cnsoperatorutil.VPCNetworkProvider, nil
		}
		oldFSS := isVsanFileVolumeServiceFSSEnabled
		defer func() { isVsanFileVolumeServiceFSSEnabled = oldFSS }()
		isVsanFileVolumeServiceFSSEnabled = true
		ok, err := shouldProvisionVsanFileVolumeViaFVS(ctx, common.StorageClassVsanFileServicePolicy)
		require.NoError(t, err)
		require.True(t, ok)
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

// fvsTestScheme returns a runtime.Scheme with the FVS FileVolume types registered for use with
// the controller-runtime fake client.
func fvsTestScheme(t *testing.T) *runtime.Scheme {
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
		fileVolumeClient: ctrlclientfake.NewClientBuilder().
			WithScheme(fvsTestScheme(t)).
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
		fileVolumeClient: ctrlclientfake.NewClientBuilder().
			WithScheme(fvsTestScheme(t)).
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
		fileVolumeClient: ctrlclientfake.NewClientBuilder().
			WithScheme(fvsTestScheme(t)).
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
		fileVolumeClient: ctrlclientfake.NewClientBuilder().
			WithScheme(fvsTestScheme(t)).
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
		fileVolumeClient: ctrlclientfake.NewClientBuilder().
			WithScheme(fvsTestScheme(t)).
			WithObjects(existing).
			Build(),
	}

	_, fault, err := c.deleteFileVolumeViaFVS(ctx,
		common.FVSVolumeIDPrefix+instanceNS+":"+fvName)
	require.Error(t, err)
	require.Equal(t, csifault.CSIInternalFault, fault)
	require.Contains(t, err.Error(), "timeout or error waiting for FileVolume")
}
