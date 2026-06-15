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

package wcpguest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

// fvsCreateGuestPVCName/Namespace are the metadata the external-provisioner
// sidecar would inject via "--extra-create-metadata" parameters
// (csi.storage.k8s.io/pvc/name and csi.storage.k8s.io/pvc/namespace).
const (
	fvsCreateGuestPVCName      = "pvc-12345"
	fvsCreateGuestPVCNamespace = "guest-app-ns"
	fvsCreateSupervisorPVCName = "-12345"
	fvsCreateSupervisorNS      = "test-namespace-fvs-create"
	fvsCreateNonFVSStorageSC   = "non-fvs-sc"
)

// newFVSCreateController builds a controller for the FVS create-volume tests
// with isolated supervisor and guest fake clients and the package CO interface
// initialized with a known FSS map.
//
// The default in unittestcommon.GetFakeContainerOrchestratorInterface enables
// "tkgs-ha" and "workload-domain-isolation"; we leave those alone so the
// existing TKGsHA topology block runs in the no-override fall-through tests.
func newFVSCreateController(t *testing.T, guestObjs ...*v1.PersistentVolumeClaim) *controller {
	t.Helper()
	supervisorClient := testclient.NewClientset()
	guestClient := testclient.NewClientset()
	for _, p := range guestObjs {
		_, err := guestClient.CoreV1().PersistentVolumeClaims(p.Namespace).Create(
			context.Background(), p, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to seed guest PVC %s/%s: %v", p.Namespace, p.Name, err)
		}
	}
	c := &controller{
		supervisorClient:    supervisorClient,
		guestClient:         guestClient,
		supervisorNamespace: fvsCreateSupervisorNS,
		topologyEnabled:     true,
	}
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	commonco.ContainerOrchestratorUtility = co
	return c
}

// makeGuestPVC builds a guest cluster PVC with the supplied storage class and
// optional csi.vsphere.volume-requested-topology annotation. An empty SC name
// leaves Spec.StorageClassName nil.
func makeGuestPVC(name, namespace, sc, requestedTopologyAnnotation string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
		},
	}
	if sc != "" {
		scCopy := sc
		pvc.Spec.StorageClassName = &scCopy
	}
	if requestedTopologyAnnotation != "" {
		pvc.Annotations = map[string]string{
			common.AnnGuestClusterRequestedTopology: requestedTopologyAnnotation,
		}
	}
	return pvc
}

// fvsCreateRequest constructs a CreateVolumeRequest with PVC metadata params
// (supplied by external-provisioner --extra-create-metadata) and an optional
// AccessibilityRequirements topology.
func fvsCreateRequest(accessMode csi.VolumeCapability_AccessMode_Mode,
	preferredZone string) *csi.CreateVolumeRequest {
	params := map[string]string{
		common.AttributeSupervisorStorageClass: testStorageClass,
		common.AttributePvcName:                fvsCreateGuestPVCName,
		common.AttributePvcNamespace:           fvsCreateGuestPVCNamespace,
	}
	req := &csi.CreateVolumeRequest{
		Name: testVolumeName,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1 * common.GbInBytes,
		},
		Parameters: params,
		VolumeCapabilities: []*csi.VolumeCapability{
			{AccessMode: &csi.VolumeCapability_AccessMode{Mode: accessMode}},
		},
	}
	if preferredZone != "" {
		segment := map[string]string{"topology.kubernetes.io/zone": preferredZone}
		req.AccessibilityRequirements = &csi.TopologyRequirement{
			Requisite: []*csi.Topology{{Segments: segment}},
			Preferred: []*csi.Topology{{Segments: segment}},
		}
	}
	return req
}

// runCreateVolumeAndBindSupervisorPVC invokes CreateVolume in a goroutine and
// flips the supervisor PVC to Bound so the call returns. It also sets the
// csi.vsphere.volume-accessible-topology annotation on the supervisor PVC so
// the response-side topology generation in CreateVolume can succeed (the
// supervisor reconciler would set this in production). Returns the bound
// supervisor PVC (with annotations as set by CreateVolume) and the response.
func runCreateVolumeAndBindSupervisorPVC(t *testing.T, c *controller,
	req *csi.CreateVolumeRequest) (*v1.PersistentVolumeClaim, *csi.CreateVolumeResponse, error) {
	t.Helper()
	respCh := make(chan *csi.CreateVolumeResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := c.CreateVolume(context.Background(), req)
		respCh <- resp
		errCh <- err
	}()

	var pvc *v1.PersistentVolumeClaim
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("supervisor PVC %s was not created within timeout", fvsCreateSupervisorPVCName)
		default:
		}
		got, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
			context.Background(), fvsCreateSupervisorPVCName, metav1.GetOptions{})
		if err == nil {
			pvc = got
			break
		}
		// CreateVolume may have failed before creating the PVC; fall through.
		select {
		case respErr := <-errCh:
			resp := <-respCh
			return nil, resp, respErr
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[common.AnnVolumeAccessibleTopology] =
		`[{"topology.kubernetes.io/zone":"zone-1"}]`
	pvc.Status.Phase = v1.ClaimBound
	if _, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(
		context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to mark supervisor PVC bound: %v", err)
	}

	resp := <-respCh
	err := <-errCh
	if err != nil {
		return nil, resp, err
	}
	pvc, getErr := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
		context.Background(), fvsCreateSupervisorPVCName, metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("failed to read back supervisor PVC: %v", getErr)
	}
	return pvc, resp, nil
}

// expectSupervisorAnnotation asserts the supervisor PVC's
// csi.vsphere.volume-requested-topology annotation matches `want` exactly.
// If `want` is empty, asserts the annotation is unset.
func expectSupervisorAnnotation(t *testing.T, pvc *v1.PersistentVolumeClaim, want string) {
	t.Helper()
	got, ok := pvc.Annotations[common.AnnGuestClusterRequestedTopology]
	if want == "" {
		if ok {
			t.Fatalf("expected supervisor PVC annotation %s to be unset, got %q",
				common.AnnGuestClusterRequestedTopology, got)
		}
		return
	}
	if !ok {
		t.Fatalf("expected supervisor PVC annotation %s = %q, but it is unset",
			common.AnnGuestClusterRequestedTopology, want)
	}
	if got != want {
		t.Fatalf("supervisor PVC annotation %s mismatch:\n  got:  %s\n  want: %s",
			common.AnnGuestClusterRequestedTopology, got, want)
	}
}

// --- helper validation tests ---------------------------------------------

func TestValidateGuestClusterRequestedTopologyAnnotation(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantOut   string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "single zone canonical",
			input:   `[{"topology.kubernetes.io/zone":"zone-1"}]`,
			wantOut: `[{"topology.kubernetes.io/zone":"zone-1"}]`,
		},
		{
			name: "multi zone with whitespace",
			input: `  [ {"topology.kubernetes.io/zone":"zone-1"} , ` +
				`{"topology.kubernetes.io/zone":"zone-2"}, ` +
				`{"topology.kubernetes.io/zone":"zone-3"} ]  `,
			wantOut: `[{"topology.kubernetes.io/zone":"zone-1"},` +
				`{"topology.kubernetes.io/zone":"zone-2"},` +
				`{"topology.kubernetes.io/zone":"zone-3"}]`,
		},
		{
			name:      "empty string",
			input:     "",
			wantErr:   true,
			errSubstr: "empty",
		},
		{
			name:      "whitespace only",
			input:     "   \t\n",
			wantErr:   true,
			errSubstr: "empty",
		},
		{
			name:      "malformed json",
			input:     `[{"topology.kubernetes.io/zone":"zone-1"`,
			wantErr:   true,
			errSubstr: "failed to parse",
		},
		{
			name:      "empty array",
			input:     `[]`,
			wantErr:   true,
			errSubstr: "at least one",
		},
		{
			name:      "empty inner map",
			input:     `[{}]`,
			wantErr:   true,
			errSubstr: "empty topology segment",
		},
		{
			name:      "empty value",
			input:     `[{"topology.kubernetes.io/zone":""}]`,
			wantErr:   true,
			errSubstr: "empty topology value",
		},
		{
			name:      "empty key",
			input:     `[{"":"zone-1"}]`,
			wantErr:   true,
			errSubstr: "empty topology key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateGuestClusterRequestedTopologyAnnotation(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got nil; out=%q", tt.input, got)
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Fatalf("error %q does not contain expected substring %q",
						err.Error(), tt.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tt.input, err)
			}
			if got != tt.wantOut {
				t.Fatalf("normalized output mismatch:\n  got:  %s\n  want: %s",
					got, tt.wantOut)
			}
		})
	}
}

// --- CreateVolume FVS override tests --------------------------------------

// TestCreateVolume_FVS_RequestedTopologyAnnotation_SingleZone: guest PVC has
// canonical single-zone JSON annotation; supervisor PVC must carry the same
// canonical JSON.
func TestCreateVolume_FVS_RequestedTopologyAnnotation_SingleZone(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	annotationValue := `[{"topology.kubernetes.io/zone":"zone-1"}]`
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, annotationValue)
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, ""))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc, annotationValue)
}

// TestCreateVolume_FVS_RequestedTopologyAnnotation_MultiZone: multi-zone
// annotation with extra whitespace; supervisor PVC must carry whitespace-
// normalized canonical JSON.
func TestCreateVolume_FVS_RequestedTopologyAnnotation_MultiZone(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	rawAnnotation := `[{"topology.kubernetes.io/zone":"zone-1"}, ` +
		`{"topology.kubernetes.io/zone":"zone-2"}, ` +
		`{"topology.kubernetes.io/zone":"zone-3"}, ` +
		`{"topology.kubernetes.io/zone":"zone-4"}, ` +
		`{"topology.kubernetes.io/zone":"zone-5"}]`
	canonical := `[{"topology.kubernetes.io/zone":"zone-1"},` +
		`{"topology.kubernetes.io/zone":"zone-2"},` +
		`{"topology.kubernetes.io/zone":"zone-3"},` +
		`{"topology.kubernetes.io/zone":"zone-4"},` +
		`{"topology.kubernetes.io/zone":"zone-5"}]`
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicyLateBinding, rawAnnotation)
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, ""))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc, canonical)
}

// TestCreateVolume_FVS_RequestedTopologyAnnotation_OverridesRequest: when both
// the request topology and the guest PVC annotation are set, the annotation
// wins.
func TestCreateVolume_FVS_RequestedTopologyAnnotation_OverridesRequest(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	annotationValue := `[{"topology.kubernetes.io/zone":"zone-1"}]`
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, annotationValue)
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "zone-3"))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc, annotationValue)
}

// TestCreateVolume_FVS_NoAnnotation_FallsBackToRequest: FVS guest PVC without
// the override annotation falls back to the existing TKGsHA path which uses
// req.AccessibilityRequirements.Preferred.
func TestCreateVolume_FVS_NoAnnotation_FallsBackToRequest(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, "")
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "zone-3"))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc,
		`[{"topology.kubernetes.io/zone":"zone-3"}]`)
}

// TestCreateVolume_FVS_EmptyAnnotation_FallsBackToRequest: empty-string
// annotation behaves the same as missing annotation.
func TestCreateVolume_FVS_EmptyAnnotation_FallsBackToRequest(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, "")
	guestPVC.Annotations = map[string]string{
		common.AnnGuestClusterRequestedTopology: "",
	}
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "zone-3"))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc,
		`[{"topology.kubernetes.io/zone":"zone-3"}]`)
}

// TestCreateVolume_FVS_FSSDisabled_NoOverride: when IsVsanFileVolumeServiceEnabled
// is false, the override is skipped even if the annotation is set on the FVS
// guest PVC. The supervisor PVC reflects the request-topology fallback (via the
// existing TKGsHA + WorkloadDomainIsolation path for file volumes).
func TestCreateVolume_FVS_FSSDisabled_NoOverride(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, false)
	annotationValue := `[{"topology.kubernetes.io/zone":"zone-1"}]`
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, annotationValue)
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "zone-3"))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc,
		`[{"topology.kubernetes.io/zone":"zone-3"}]`)
}

// TestCreateVolume_NonFVSStorageClass_NoOverride: file volume but guest PVC
// has a non-FVS storage class. Override is skipped via IsFVSPersistentVolumeClaim;
// existing path runs.
func TestCreateVolume_NonFVSStorageClass_NoOverride(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	annotationValue := `[{"topology.kubernetes.io/zone":"zone-1"}]`
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		fvsCreateNonFVSStorageSC, annotationValue)
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "zone-3"))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc,
		`[{"topology.kubernetes.io/zone":"zone-3"}]`)
}

// TestCreateVolume_BlockVolume_NoOverride: block volume request with the
// annotation set on the guest PVC; the override gate requires
// isFileVolumeRequest, so the annotation is NOT propagated. Supervisor PVC
// gets the request-topology via the existing TKGsHA path.
func TestCreateVolume_BlockVolume_NoOverride(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	annotationValue := `[{"topology.kubernetes.io/zone":"zone-1"}]`
	guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
		common.StorageClassVsanFileServicePolicy, annotationValue)
	guestPVC.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	c := newFVSCreateController(t, guestPVC)

	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c,
		fvsCreateRequest(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "zone-3"))
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc,
		`[{"topology.kubernetes.io/zone":"zone-3"}]`)
}

// TestCreateVolume_FVS_InvalidAnnotation_Errors: malformed JSON / empty array
// / empty key or value in the annotation must surface as InvalidArgument
// without creating the supervisor PVC.
func TestCreateVolume_FVS_InvalidAnnotation_Errors(t *testing.T) {
	cases := []struct {
		name       string
		annotation string
	}{
		{"malformed json", `[{"topology.kubernetes.io/zone":"zone-1"`},
		{"empty array", `[]`},
		{"empty inner map", `[{}]`},
		{"empty value", `[{"topology.kubernetes.io/zone":""}]`},
		{"empty key", `[{"":"zone-1"}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setVsanFileVolumeServiceEnabledForTest(t, true)
			guestPVC := makeGuestPVC(fvsCreateGuestPVCName, fvsCreateGuestPVCNamespace,
				common.StorageClassVsanFileServicePolicy, tc.annotation)
			c := newFVSCreateController(t, guestPVC)

			req := fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "")
			_, err := c.CreateVolume(context.Background(), req)
			if err == nil {
				t.Fatalf("expected InvalidArgument error for annotation %q, got nil", tc.annotation)
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected gRPC status error, got %T: %v", err, err)
			}
			if st.Code() != codes.InvalidArgument {
				t.Fatalf("expected codes.InvalidArgument, got %s: %v", st.Code(), err)
			}
			// Supervisor PVC must not have been created.
			_, getErr := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
				context.Background(), fvsCreateSupervisorPVCName, metav1.GetOptions{})
			if getErr == nil {
				t.Fatalf("supervisor PVC %s should not exist after invalid override",
					fvsCreateSupervisorPVCName)
			}
		})
	}
}

// TestCreateVolume_FVS_MissingPVCMetadata_Errors: when the
// csi.storage.k8s.io/pvc/name parameter is missing (e.g. external-provisioner
// not deployed with --extra-create-metadata), the FVS override path returns
// Internal error (strict — design choice).
func TestCreateVolume_FVS_MissingPVCMetadata_Errors(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newFVSCreateController(t)

	req := fvsCreateRequest(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "")
	delete(req.Parameters, common.AttributePvcName)

	_, err := c.CreateVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected Internal error when guest PVC name parameter is missing, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("expected codes.Internal, got %s: %v", st.Code(), err)
	}
}
