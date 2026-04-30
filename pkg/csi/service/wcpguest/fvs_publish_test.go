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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

const (
	fvsTestNamespace      = "test-namespace-fvs"
	fvsTestSupervisorPVC  = "tkc-uid-fvs-pvc-12345"
	fvsTestNodeID         = "guest-vm-node-1"
	fvsTestNfsv41Endpoint = "10.20.30.40:/nfs/v4/share-1"
)

func newPVC(name, sc, exportPath string) *v1.PersistentVolumeClaim {
	scCopy := sc
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: fvsTestNamespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			StorageClassName: &scCopy,
		},
	}
	if exportPath != "" {
		pvc.Annotations = map[string]string{
			common.Nfsv4ExportPathAnnotationKey: exportPath,
		}
	}
	return pvc
}

func setVsanFileVolumeServiceEnabledForTest(t *testing.T, enabled bool) {
	t.Helper()
	prev := IsVsanFileVolumeServiceEnabled
	IsVsanFileVolumeServiceEnabled = enabled
	t.Cleanup(func() { IsVsanFileVolumeServiceEnabled = prev })
}

// --- controllerPublishForFileVolume / controllerUnpublishForFileVolume -----

func TestControllerPublishForFileVolume_GateOn_FVSSupervisorPVC(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicy, fvsTestNfsv41Endpoint)
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerPublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, faultType, err := controllerPublishForFileVolume(context.Background(), req, c)
	if err != nil {
		t.Fatalf("unexpected error: %v fault=%q", err, faultType)
	}
	if resp == nil || resp.PublishContext[common.Nfsv4AccessPoint] != fvsTestNfsv41Endpoint {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp.PublishContext[common.AttributeDiskType] != common.DiskTypeFileVolume {
		t.Fatalf("unexpected disk type: %q", resp.PublishContext[common.AttributeDiskType])
	}
}

func TestControllerPublishForFileVolume_GateOn_SupervisorPVCNotFound(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := &controller{
		supervisorClient:    testclient.NewClientset(),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerPublishVolumeRequest{VolumeId: "missing-supervisor-pvc", NodeId: fvsTestNodeID}

	_, faultType, err := controllerPublishForFileVolume(context.Background(), req, c)
	if err == nil {
		t.Fatal("expected error when supervisor PVC is missing")
	}
	if faultType != csifault.CSINotFoundFault {
		t.Fatalf("faultType=%q want %q", faultType, csifault.CSINotFoundFault)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected gRPC NotFound, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

func TestControllerPublishForFileVolume_GateOn_FVSSC_MissingExportAnnotation(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicy, "")
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerPublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	_, faultType, err := controllerPublishForFileVolume(context.Background(), req, c)
	if err == nil {
		t.Fatal("expected error when export-path annotation is missing")
	}
	if faultType != csifault.CSIInternalFault {
		t.Fatalf("faultType=%q want %q", faultType, csifault.CSIInternalFault)
	}
}

func TestControllerUnpublishForFileVolume_GateOn_FVSSupervisorPVC(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicy, fvsTestNfsv41Endpoint)
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerUnpublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, faultType, err := controllerUnpublishForFileVolume(context.Background(), req, c)
	if err != nil {
		t.Fatalf("unexpected error: %v fault=%q", err, faultType)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestControllerUnpublishForFileVolume_GateOn_LateBindingFVS(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicyLateBinding, fvsTestNfsv41Endpoint)
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerUnpublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, _, err := controllerUnpublishForFileVolume(context.Background(), req, c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestControllerUnpublishForFileVolume_GateOn_SupervisorPVCNotFound(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := &controller{
		supervisorClient:    testclient.NewClientset(),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerUnpublishVolumeRequest{VolumeId: "missing-supervisor-pvc", NodeId: fvsTestNodeID}

	_, faultType, err := controllerUnpublishForFileVolume(context.Background(), req, c)
	if err == nil {
		t.Fatal("expected error when supervisor PVC is missing")
	}
	if faultType != csifault.CSINotFoundFault {
		t.Fatalf("faultType=%q want %q", faultType, csifault.CSINotFoundFault)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected gRPC NotFound, got ok=%v code=%v err=%v", ok, st.Code(), err)
	}
}

func TestControllerPublishForFileVolume_GateOn_LateBindingFVS(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicyLateBinding, fvsTestNfsv41Endpoint)
	c := &controller{
		supervisorClient:    testclient.NewClientset(pvc),
		supervisorNamespace: fvsTestNamespace,
	}
	req := &csi.ControllerPublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, _, err := controllerPublishForFileVolume(context.Background(), req, c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.PublishContext[common.Nfsv4AccessPoint] != fvsTestNfsv41Endpoint {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

// --- ControllerPublishForFileVolumeServiceVolume --------------------------------------

func TestControllerPublishForFileVolumeServiceVolume_Success(t *testing.T) {
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicy, fvsTestNfsv41Endpoint)
	req := &csi.ControllerPublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, faultType, err := ControllerPublishForFileVolumeServiceVolume(context.Background(), req, pvc)
	if err != nil {
		t.Fatalf("unexpected error: %v (fault=%q)", err, faultType)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
	if got := resp.PublishContext[common.AttributeDiskType]; got != common.DiskTypeFileVolume {
		t.Fatalf("unexpected DiskType: %q", got)
	}
	if got := resp.PublishContext[common.Nfsv4AccessPoint]; got != fvsTestNfsv41Endpoint {
		t.Fatalf("unexpected Nfsv4AccessPoint: %q (want %q)", got, fvsTestNfsv41Endpoint)
	}
}

func TestControllerPublishForFileVolumeServiceVolume_LateBindingSC(t *testing.T) {
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicyLateBinding, fvsTestNfsv41Endpoint)
	req := &csi.ControllerPublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, _, err := ControllerPublishForFileVolumeServiceVolume(context.Background(), req, pvc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil || resp.PublishContext[common.Nfsv4AccessPoint] != fvsTestNfsv41Endpoint {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestControllerPublishForFileVolumeServiceVolume_MissingExportAnnotation(t *testing.T) {
	pvc := newPVC(fvsTestSupervisorPVC, common.StorageClassVsanFileServicePolicy, "")
	req := &csi.ControllerPublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, _, err := ControllerPublishForFileVolumeServiceVolume(context.Background(), req, pvc)
	if err == nil {
		t.Fatalf("expected error when FVS PVC is missing export-path annotation")
	}
	if resp != nil {
		t.Fatalf("expected nil response on error")
	}
}

// --- ControllerUnpublishForFileVolumeServiceVolume ------------------------------------

func TestControllerUnpublishForFileVolumeServiceVolume_Success(t *testing.T) {
	req := &csi.ControllerUnpublishVolumeRequest{VolumeId: fvsTestSupervisorPVC, NodeId: fvsTestNodeID}

	resp, _, err := ControllerUnpublishForFileVolumeServiceVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
}
