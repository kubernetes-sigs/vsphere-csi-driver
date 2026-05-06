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
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	testclient "k8s.io/client-go/kubernetes/fake"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// ---- helpers ---------------------------------------------------------------

// fvsExpandScheme returns a runtime.Scheme with corev1 registered.
func fvsExpandScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

// newPVCForExpand builds a supervisor PVC with the given spec-request and
// status-capacity sizes (empty string → no status capacity set).
func newPVCForExpand(name, sc, specSize, statusCap string) *corev1.PersistentVolumeClaim {
	scCopy := sc
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fvsTestNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scCopy,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(specSize),
				},
			},
		},
	}
	if statusCap != "" {
		pvc.Status = corev1.PersistentVolumeClaimStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(statusCap),
			},
		}
	}
	return pvc
}

// fvsExpandController builds a *controller wired with fake clients for expand tests.
// runtimePVC is the PVC registered in the fake controller-runtime client (for patching);
// watchPVC is registered in the fake k8s clientset (for watching status.capacity changes).
func fvsExpandController(t *testing.T, runtimePVC, watchPVC *corev1.PersistentVolumeClaim) *controller {
	t.Helper()
	rtClient := ctrlclientfake.NewClientBuilder().
		WithScheme(fvsExpandScheme(t)).
		WithObjects(runtimePVC).
		Build()
	svClient := testclient.NewClientset(watchPVC)
	return &controller{
		supervisorClient:        svClient,
		supervisorRuntimeClient: rtClient,
		supervisorNamespace:     fvsTestNamespace,
	}
}

// expandReqRWX returns a ControllerExpandVolumeRequest with MULTI_NODE_MULTI_WRITER
// access mode and the given required size in bytes.
func expandReqRWX(volumeID string, sizeBytes int64) *csi.ControllerExpandVolumeRequest {
	return &csi.ControllerExpandVolumeRequest{
		VolumeId: volumeID,
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: sizeBytes,
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
		},
	}
}

const (
	fvsExpandPVC      = "sv-pvc-expand-1"
	gi5               = 5 * common.GbInBytes
	gi10              = 10 * common.GbInBytes
)

// ---- checkForSupervisorPVCCapacityReached -----------------------------------

// TestCheckForSupervisorPVCCapacityReached_ContextCancelled verifies that the
// function returns promptly (error) when the context is already cancelled.
func TestCheckForSupervisorPVCCapacityReached_ContextCancelled(t *testing.T) {
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "5Gi", "5Gi")
	svClient := testclient.NewClientset(pvc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	err := checkForSupervisorPVCCapacityReached(ctx, svClient, pvc,
		mustParseQuantity("10Gi"), 30*time.Second)
	if err == nil {
		t.Fatal("expected error when context is cancelled, got nil")
	}
}

// TestCheckForSupervisorPVCCapacityReached_CapacityReachedViaUpdate verifies
// that the function returns nil when the PVC's status.capacity is updated to
// the requested size from a concurrent goroutine.
func TestCheckForSupervisorPVCCapacityReached_CapacityReachedViaUpdate(t *testing.T) {
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "5Gi")
	svClient := testclient.NewClientset(pvc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- checkForSupervisorPVCCapacityReached(ctx, svClient, pvc,
			mustParseQuantity("10Gi"), 30*time.Second)
	}()

	// Give the goroutine time to start the watch, then update capacity.
	time.Sleep(50 * time.Millisecond)
	updated := pvc.DeepCopy()
	updated.Status.Capacity = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("10Gi"),
	}
	if _, err := svClient.CoreV1().PersistentVolumeClaims(fvsTestNamespace).UpdateStatus(
		ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for checkForSupervisorPVCCapacityReached to return")
	}
}

// TestCheckForSupervisorPVCCapacityReached_WatchChannelClosed verifies that
// the function returns an error when the watch channel is closed (simulated by
// cancelling the context after the watch has started).
func TestCheckForSupervisorPVCCapacityReached_WatchChannelClosed(t *testing.T) {
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "5Gi")
	svClient := testclient.NewClientset(pvc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- checkForSupervisorPVCCapacityReached(ctx, svClient, pvc,
			mustParseQuantity("10Gi"), 30*time.Second)
	}()

	// Cancel context after a short delay to simulate watch timeout / closure.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error when context is cancelled, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for checkForSupervisorPVCCapacityReached to return after context cancel")
	}
}

// ---- controllerExpandForFVSFileVolume ---------------------------------------

// TestControllerExpandForFVSFileVolume_Shrink_Rejected verifies that requesting a
// size smaller than the current supervisor PVC spec is rejected with InvalidArgument.
func TestControllerExpandForFVSFileVolume_Shrink_Rejected(t *testing.T) {
	// PVC spec already at 10Gi; request only 5Gi → shrink
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "10Gi")
	c := fvsExpandController(t, pvc, pvc)

	req := expandReqRWX(fvsExpandPVC, gi5)
	_, faultType, err := controllerExpandForFVSFileVolume(context.Background(), req, pvc.DeepCopy(), c)
	if err == nil {
		t.Fatal("expected error for shrink request, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
	if faultType != csifault.CSIInternalFault {
		t.Fatalf("faultType=%q want %q", faultType, csifault.CSIInternalFault)
	}
}

// TestControllerExpandForFVSFileVolume_Equal_CapacityAlreadyMet verifies that
// when the spec and status.capacity are both already at the requested size, the
// function returns success immediately without patching or waiting.
func TestControllerExpandForFVSFileVolume_Equal_CapacityAlreadyMet(t *testing.T) {
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "10Gi")
	c := fvsExpandController(t, pvc, pvc)

	req := expandReqRWX(fvsExpandPVC, gi10)
	resp, faultType, err := controllerExpandForFVSFileVolume(context.Background(), req, pvc.DeepCopy(), c)
	if err != nil {
		t.Fatalf("unexpected error: %v (fault=%q)", err, faultType)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if resp.NodeExpansionRequired {
		t.Error("NodeExpansionRequired must be false for FVS file volumes")
	}
	if resp.CapacityBytes != gi10 {
		t.Errorf("CapacityBytes=%d want %d", resp.CapacityBytes, gi10)
	}
}

// TestControllerExpandForFVSFileVolume_Equal_CapacityNotYet_Success verifies
// that when spec is already at the requested size but status.capacity has not
// caught up, the function waits and returns success once capacity is updated.
func TestControllerExpandForFVSFileVolume_Equal_CapacityNotYet_Success(t *testing.T) {
	// spec=10Gi (already patched), status.capacity=5Gi (FVS not yet converged)
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "5Gi")
	c := fvsExpandController(t, pvc, pvc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	respCh := make(chan *csi.ControllerExpandVolumeResponse, 1)
	go func() {
		req := expandReqRWX(fvsExpandPVC, gi10)
		resp, _, err := controllerExpandForFVSFileVolume(ctx, req, pvc.DeepCopy(), c)
		respCh <- resp
		errCh <- err
	}()

	// Simulate the FVS controller updating status.capacity on the supervisor PVC.
	time.Sleep(50 * time.Millisecond)
	updated := pvc.DeepCopy()
	updated.Status.Capacity = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("10Gi"),
	}
	if _, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(fvsTestNamespace).UpdateStatus(
		ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp := <-respCh
		if resp == nil || resp.NodeExpansionRequired {
			t.Errorf("unexpected response: %+v", resp)
		}
	case <-ctx.Done():
		t.Fatal("timed out")
	}
}

// TestControllerExpandForFVSFileVolume_Equal_CapacityNotYet_ContextCancelled verifies
// that when the context is cancelled while waiting for capacity, an error is returned.
func TestControllerExpandForFVSFileVolume_Equal_CapacityNotYet_ContextCancelled(t *testing.T) {
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "5Gi")
	c := fvsExpandController(t, pvc, pvc)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		req := expandReqRWX(fvsExpandPVC, gi10)
		_, _, err := controllerExpandForFVSFileVolume(ctx, req, pvc.DeepCopy(), c)
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error when context cancelled, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for function to return after context cancel")
	}
}

// TestControllerExpandForFVSFileVolume_Grow_Success verifies the full grow path:
// spec < requested → patch spec via runtime client, wait for capacity → success.
func TestControllerExpandForFVSFileVolume_Grow_Success(t *testing.T) {
	// Supervisor PVC starts at 5Gi spec and 5Gi capacity.
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "5Gi", "5Gi")
	c := fvsExpandController(t, pvc, pvc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	respCh := make(chan *csi.ControllerExpandVolumeResponse, 1)
	go func() {
		req := expandReqRWX(fvsExpandPVC, gi10)
		resp, _, err := controllerExpandForFVSFileVolume(ctx, req, pvc.DeepCopy(), c)
		respCh <- resp
		errCh <- err
	}()

	// After the function patches spec, it watches status.capacity. Simulate the
	// supervisor updating status.capacity once it has grown the underlying volume.
	time.Sleep(80 * time.Millisecond)
	updated := pvc.DeepCopy()
	updated.Status.Capacity = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse("10Gi"),
	}
	if _, err := c.supervisorClient.CoreV1().PersistentVolumeClaims(fvsTestNamespace).UpdateStatus(
		ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		resp := <-respCh
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if resp.NodeExpansionRequired {
			t.Error("NodeExpansionRequired must be false for FVS file volumes")
		}
		if resp.CapacityBytes != gi10 {
			t.Errorf("CapacityBytes=%d want %d", resp.CapacityBytes, gi10)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for grow to complete")
	}
}

// TestControllerExpandForFVSFileVolume_Grow_NilRuntimeClientConfig verifies that
// a nil restClientConfig (and no cached supervisorRuntimeClient) surfaces a clear
// Internal error rather than a nil-pointer panic.
func TestControllerExpandForFVSFileVolume_Grow_NilRuntimeClientConfig(t *testing.T) {
	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "5Gi", "5Gi")
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
		// supervisorRuntimeClient is nil AND restClientConfig is nil → ctrlclient.New fails
	}

	req := expandReqRWX(fvsExpandPVC, gi10)
	_, faultType, err := controllerExpandForFVSFileVolume(context.Background(), req, pvc.DeepCopy(), c)
	if err == nil {
		t.Fatal("expected error when runtime client cannot be created, got nil")
	}
	if faultType != csifault.CSIInternalFault {
		t.Fatalf("faultType=%q want %q", faultType, csifault.CSIInternalFault)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

// ---- ControllerExpandVolume routing (FVS gate) ------------------------------

// TestControllerExpandVolume_FVS_RoutedToFVSExpand verifies that with the FVS
// gate on and a FVS supervisor PVC (spec=capacity=requested), ControllerExpandVolume
// succeeds and sets NodeExpansionRequired=false.
func TestControllerExpandVolume_FVS_RoutedToFVSExpand(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)

	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicy, "10Gi", "10Gi")
	c := fvsExpandController(t, pvc, pvc)

	req := expandReqRWX(fvsExpandPVC, gi10)
	resp, err := c.ControllerExpandVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if resp.NodeExpansionRequired {
		t.Error("NodeExpansionRequired must be false for FVS file volumes")
	}
}

// TestControllerExpandVolume_FVS_GateDisabled_LegacyFileRejected verifies that
// when the FVS gate is off and the request is for a file (RWX) volume, the
// legacy guard returns Unimplemented.
func TestControllerExpandVolume_FVS_GateDisabled_LegacyFileRejected(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, false)

	// non-FVS storage class so IsFVSPersistentVolumeClaim is false
	pvc := newPVCForExpand(fvsExpandPVC, "standard-sc", "10Gi", "10Gi")
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}

	req := expandReqRWX(fvsExpandPVC, gi10)
	_, err := c.ControllerExpandVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for legacy file volume expansion, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

// TestControllerExpandVolume_FVS_Enabled_NonFVSPVC_LegacyFileRejected verifies that
// even with the FVS gate on, a non-FVS PVC with file capability goes to the legacy
// guard and returns Unimplemented.
func TestControllerExpandVolume_FVS_Enabled_NonFVSPVC_LegacyFileRejected(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)

	pvc := newPVCForExpand(fvsExpandPVC, "standard-sc", "10Gi", "10Gi")
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}

	req := expandReqRWX(fvsExpandPVC, gi10)
	_, err := c.ControllerExpandVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for legacy file volume, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("expected Unimplemented, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

// TestControllerExpandVolume_FVS_SupervisorPVCNotFound verifies that a missing
// supervisor PVC surfaces a retriable Internal error.
func TestControllerExpandVolume_FVS_SupervisorPVCNotFound(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)

	c := &controller{
		supervisorClient:    testclient.NewClientset(), // empty — no PVCs
		supervisorNamespace: fvsTestNamespace,
	}

	req := expandReqRWX("missing-pvc", gi10)
	_, err := c.ControllerExpandVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when supervisor PVC not found, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

// TestControllerExpandVolume_FVS_InvalidRequest_NoVolumeID verifies that a
// request with no volume ID is rejected with InvalidArgument.
func TestControllerExpandVolume_FVS_InvalidRequest_NoVolumeID(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)

	c := &controller{
		supervisorClient:    testclient.NewClientset(),
		supervisorNamespace: fvsTestNamespace,
	}

	req := &csi.ControllerExpandVolumeRequest{
		VolumeId: "",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: gi10,
		},
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	_, err := c.ControllerExpandVolume(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for missing volume ID, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

// TestControllerExpandVolume_FVS_LateBinding_RoutedToFVSExpand verifies that the
// late-binding FVS storage class is also routed to the FVS expand path.
func TestControllerExpandVolume_FVS_LateBinding_RoutedToFVSExpand(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)

	pvc := newPVCForExpand(fvsExpandPVC, common.StorageClassVsanFileServicePolicyLateBinding, "10Gi", "10Gi")
	c := fvsExpandController(t, pvc, pvc)

	req := expandReqRWX(fvsExpandPVC, gi10)
	resp, err := c.ControllerExpandVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.NodeExpansionRequired {
		t.Errorf("unexpected response: %+v", resp)
	}
}

// ---- utilities -------------------------------------------------------------

func mustParseQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}
