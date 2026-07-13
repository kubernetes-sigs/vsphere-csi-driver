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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

const (
	hostLocalZoneValue  = "az1"
	hostLocalHostValue  = "lvn-dvm-10-161-28-26.dvm.lvn.broadcom.net"
	hostLocalAccessMode = csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER
)

// hostLocalWantAnnotation builds the expected JSON value for a single-segment topology
// annotation (used for both the requested- and accessible-topology annotations, which share
// the same array-of-segment-maps shape) carrying hostKey and the standard zone key.
func hostLocalWantAnnotation(hostKey string) string {
	return `[{"` + hostKey + `":"` + hostLocalHostValue + `","topology.kubernetes.io/zone":"` + hostLocalZoneValue + `"}]`
}

// hostLocalCreateRequest builds a CreateVolumeRequest carrying the VKS host
// topology key (common.GuestClusterTopologyLabelHost) alongside the standard
// zone key, mirroring what a WaitForFirstConsumer PVC scheduled to a VKS node
// with a host-local storage class would produce. Either zone or host may be
// left empty to omit that segment.
func hostLocalCreateRequest(accessMode csi.VolumeCapability_AccessMode_Mode,
	zone, host string) *csi.CreateVolumeRequest {
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
	segment := map[string]string{}
	if zone != "" {
		segment["topology.kubernetes.io/zone"] = zone
	}
	if host != "" {
		segment[common.GuestClusterTopologyLabelHost] = host
	}
	if len(segment) > 0 {
		req.AccessibilityRequirements = &csi.TopologyRequirement{
			Requisite: []*csi.Topology{{Segments: segment}},
			Preferred: []*csi.Topology{{Segments: segment}},
		}
	}
	return req
}

// waitForSupervisorPVCCreated polls until the supervisor PVC referenced by the CreateVolume call
// under way in respCh/errCh appears, or that call has already returned (e.g. an early rejection
// before the PVC would have been created). Returns the created PVC, or nil plus the CreateVolume
// result if the call already completed without creating one.
func waitForSupervisorPVCCreated(t *testing.T, c *controller, respCh chan *csi.CreateVolumeResponse,
	errCh chan error) (*v1.PersistentVolumeClaim, *csi.CreateVolumeResponse, error) {
	t.Helper()
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
			return got, nil, nil
		}
		select {
		case respErr := <-errCh:
			resp := <-respCh
			return nil, resp, respErr
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// runCreateVolumeAndBindSupervisorPVCWithAccessibleTopology mirrors
// runCreateVolumeAndBindSupervisorPVC (controller_fvs_create_test.go) but lets
// the caller control the csi.vsphere.volume-accessible-topology annotation
// set on the supervisor PVC before it is marked Bound, so tests can exercise
// the response-side host-key translation with an arbitrary annotation value.
// The optional beforeBind hook runs after the supervisor PVC is observed but
// before it is patched to Bound, so a test can mutate shared state (e.g.
// toggle an FSS) in that window.
func runCreateVolumeAndBindSupervisorPVCWithAccessibleTopology(t *testing.T, c *controller,
	req *csi.CreateVolumeRequest, accessibleTopologyAnnotation string,
	beforeBind func()) (*csi.CreateVolumeResponse, error) {
	t.Helper()
	respCh := make(chan *csi.CreateVolumeResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := c.CreateVolume(context.Background(), req)
		respCh <- resp
		errCh <- err
	}()

	pvc, earlyResp, earlyErr := waitForSupervisorPVCCreated(t, c, respCh, errCh)
	if pvc == nil {
		return earlyResp, earlyErr
	}

	if beforeBind != nil {
		beforeBind()
	}

	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[common.AnnVolumeAccessibleTopology] = accessibleTopologyAnnotation
	pvc.Status.Phase = v1.ClaimBound
	if _, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Update(
		context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to mark supervisor PVC bound: %v", err)
	}

	resp := <-respCh
	err := <-errCh
	return resp, err
}

// --- translateTopologyHostKey helper tests --------------------------------

func TestTranslateTopologyHostKey(t *testing.T) {
	tests := []struct {
		name        string
		segments    map[string]string
		fromKey     string
		toKey       string
		keepHostKey bool
		want        map[string]string
	}{
		{
			name: "key present, keepHostKey true renames the key",
			segments: map[string]string{
				common.GuestClusterTopologyLabelHost: hostLocalHostValue,
				"topology.kubernetes.io/zone":        hostLocalZoneValue,
			},
			fromKey:     common.GuestClusterTopologyLabelHost,
			toKey:       v1.LabelHostname,
			keepHostKey: true,
			want: map[string]string{
				v1.LabelHostname:              hostLocalHostValue,
				"topology.kubernetes.io/zone": hostLocalZoneValue,
			},
		},
		{
			name: "key present, keepHostKey false drops the key",
			segments: map[string]string{
				common.GuestClusterTopologyLabelHost: hostLocalHostValue,
				"topology.kubernetes.io/zone":        hostLocalZoneValue,
			},
			fromKey:     common.GuestClusterTopologyLabelHost,
			toKey:       v1.LabelHostname,
			keepHostKey: false,
			want: map[string]string{
				"topology.kubernetes.io/zone": hostLocalZoneValue,
			},
		},
		{
			name: "key absent is a no-op regardless of keepHostKey",
			segments: map[string]string{
				"topology.kubernetes.io/zone": hostLocalZoneValue,
			},
			fromKey:     common.GuestClusterTopologyLabelHost,
			toKey:       v1.LabelHostname,
			keepHostKey: true,
			want: map[string]string{
				"topology.kubernetes.io/zone": hostLocalZoneValue,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translateTopologyHostKey(tc.segments, tc.fromKey, tc.toKey, tc.keepHostKey)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("translateTopologyHostKey() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// --- CreateVolume request-side translation tests --------------------------

func TestCreateVolume_HostLocal_RequestedTopology_TranslatesHostKey(t *testing.T) {
	c := newFVSCreateController(t)
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	commonco.ContainerOrchestratorUtility = co
	if err := co.EnableFSS(context.Background(), common.HostLocalStorageSupportFSS); err != nil {
		t.Fatalf("failed to enable FSS: %v", err)
	}

	req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, hostLocalHostValue)
	pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c, req)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	expectSupervisorAnnotation(t, pvc, hostLocalWantAnnotation(v1.LabelHostname))
}

func TestCreateVolume_HostLocal_FSSDisabled_RejectsHostKey(t *testing.T) {
	c := newFVSCreateController(t)
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	commonco.ContainerOrchestratorUtility = co
	if err := co.DisableFSS(context.Background(), common.HostLocalStorageSupportFSS); err != nil {
		t.Fatalf("failed to disable FSS: %v", err)
	}

	req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, hostLocalHostValue)
	_, err = c.CreateVolume(context.Background(), req)
	if err == nil {
		t.Fatalf("expected CreateVolume to fail when host key is present and FSS is disabled")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("expected codes.Unimplemented, got: %v", err)
	}

	_, getErr := c.supervisorClient.CoreV1().PersistentVolumeClaims(c.supervisorNamespace).Get(
		context.Background(), fvsCreateSupervisorPVCName, metav1.GetOptions{})
	if !apierrors.IsNotFound(getErr) {
		t.Fatalf("expected no supervisor PVC to be created, got err: %v", getErr)
	}
}

func TestCreateVolume_HostLocal_NoHostKey_Unaffected(t *testing.T) {
	for _, fssEnabled := range []bool{true, false} {
		fssEnabled := fssEnabled
		t.Run(fmt.Sprintf("fssEnabled=%v", fssEnabled), func(t *testing.T) {
			c := newFVSCreateController(t)
			co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
			if err != nil {
				t.Fatalf("failed to build fake CO: %v", err)
			}
			commonco.ContainerOrchestratorUtility = co
			if fssEnabled {
				err = co.EnableFSS(context.Background(), common.HostLocalStorageSupportFSS)
			} else {
				err = co.DisableFSS(context.Background(), common.HostLocalStorageSupportFSS)
			}
			if err != nil {
				t.Fatalf("failed to set FSS: %v", err)
			}

			req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, "")
			pvc, _, err := runCreateVolumeAndBindSupervisorPVC(t, c, req)
			if err != nil {
				t.Fatalf("CreateVolume failed: %v", err)
			}
			want := `[{"topology.kubernetes.io/zone":"` + hostLocalZoneValue + `"}]`
			expectSupervisorAnnotation(t, pvc, want)
		})
	}
}

// --- CreateVolume response-side translation tests --------------------------

func TestCreateVolume_HostLocal_AccessibleTopology_TranslatesBack(t *testing.T) {
	c := newFVSCreateController(t)
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	commonco.ContainerOrchestratorUtility = co
	if err := co.EnableFSS(context.Background(), common.HostLocalStorageSupportFSS); err != nil {
		t.Fatalf("failed to enable FSS: %v", err)
	}

	req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, hostLocalHostValue)
	accessibleTopologyAnnotation := hostLocalWantAnnotation(v1.LabelHostname)
	resp, err := runCreateVolumeAndBindSupervisorPVCWithAccessibleTopology(t, c, req, accessibleTopologyAnnotation, nil)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	assertAccessibleTopologyHasHostKey(t, resp)
}

// TestCreateVolume_HostLocal_AccessibleTopology_TranslatesBack_FSSDisabledAtReadTime proves the
// response-side translation is unconditional: even if the FSS becomes disabled after the request
// was accepted (but before the response-side block re-reads the bound supervisor PVC), a hostname
// key already present in the accessible-topology annotation is still translated back to the VKS
// host key, since the volume's host pinning in CNS is already a committed fact by that point
// (idempotent-retry / FSS-toggled-off-after-creation case).
func TestCreateVolume_HostLocal_AccessibleTopology_TranslatesBack_FSSDisabledAtReadTime(t *testing.T) {
	c := newFVSCreateController(t)
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to build fake CO: %v", err)
	}
	commonco.ContainerOrchestratorUtility = co
	// Enabled so the request-side check (which runs before the PVC is created) does not reject
	// the request; it is disabled below, after the PVC is observed but before it is marked Bound,
	// so only the response-side read-path behavior is under test.
	if err := co.EnableFSS(context.Background(), common.HostLocalStorageSupportFSS); err != nil {
		t.Fatalf("failed to enable FSS: %v", err)
	}

	req := hostLocalCreateRequest(hostLocalAccessMode, hostLocalZoneValue, hostLocalHostValue)
	accessibleTopologyAnnotation := hostLocalWantAnnotation(v1.LabelHostname)

	disableFSS := func() {
		if err := co.DisableFSS(context.Background(), common.HostLocalStorageSupportFSS); err != nil {
			t.Fatalf("failed to disable FSS: %v", err)
		}
	}
	resp, err := runCreateVolumeAndBindSupervisorPVCWithAccessibleTopology(t, c, req,
		accessibleTopologyAnnotation, disableFSS)
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	assertAccessibleTopologyHasHostKey(t, resp)
}

func assertAccessibleTopologyHasHostKey(t *testing.T, resp *csi.CreateVolumeResponse) {
	t.Helper()
	if len(resp.Volume.AccessibleTopology) != 1 {
		t.Fatalf("expected exactly one accessible topology segment, got: %+v", resp.Volume.AccessibleTopology)
	}
	segments := resp.Volume.AccessibleTopology[0].Segments
	want := map[string]string{
		common.GuestClusterTopologyLabelHost: hostLocalHostValue,
		"topology.kubernetes.io/zone":        hostLocalZoneValue,
	}
	if !reflect.DeepEqual(segments, want) {
		t.Fatalf("AccessibleTopology segments = %+v, want %+v", segments, want)
	}
}
