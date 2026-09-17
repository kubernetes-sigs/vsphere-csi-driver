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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	"google.golang.org/grpc/codes"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	testclient "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const listVolumesTestNamespace = "test-namespace"

// newListVolumesController builds a controller with a fresh fake CO interface, restored via
// t.Cleanup, and fresh fake clients for the guest clientset, the supervisor clientset, and the
// vmOperator/cnsOperator controller-runtime clients. commonco.ContainerOrchestratorUtility is a
// package global whose fake FSS map is otherwise shared across tests, so each test needs its
// own to avoid an FSS toggled in one test leaking into another.
func newListVolumesController(t *testing.T, vmObjs []ctrlclient.Object, guestObjs []runtime.Object) *controller {
	t.Helper()
	return newListVolumesControllerWithSupervisorObjs(t, vmObjs, guestObjs, nil, nil)
}

// newListVolumesControllerWithSupervisorObjs is newListVolumesController plus the ability to
// seed supervisor PersistentVolumeClaims (the classification pass) and CnsFileAccessConfig
// objects (the legacy file pass).
func newListVolumesControllerWithSupervisorObjs(t *testing.T, vmObjs []ctrlclient.Object, guestObjs []runtime.Object,
	supervisorPVCs []runtime.Object, cnsFileAccessConfigs []ctrlclient.Object) *controller {
	t.Helper()

	prevCO := commonco.ContainerOrchestratorUtility
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to create fake container orchestrator: %v", err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = prevCO })

	vmScheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(vmScheme); err != nil {
		t.Fatalf("failed to add vmoperator types to scheme: %v", err)
	}
	vmOperatorClient := ctrlclientfake.NewClientBuilder().WithScheme(vmScheme).WithObjects(vmObjs...).Build()

	cnsScheme := runtime.NewScheme()
	if err := cnsoperatorapis.AddToScheme(cnsScheme); err != nil {
		t.Fatalf("failed to add cnsoperator types to scheme: %v", err)
	}
	cnsOperatorClient := ctrlclientfake.NewClientBuilder().WithScheme(cnsScheme).WithObjects(cnsFileAccessConfigs...).
		Build()

	return &controller{
		guestClient:         testclient.NewClientset(guestObjs...),
		supervisorClient:    testclient.NewClientset(supervisorPVCs...),
		vmOperatorClient:    vmOperatorClient,
		cnsOperatorClient:   cnsOperatorClient,
		supervisorNamespace: listVolumesTestNamespace,
	}
}

// newGuestPV builds a guest PersistentVolume whose Name equals its volume handle. Real guest
// PVs never satisfy that equality (the PV name follows the "pvc-<uid>" convention, unrelated to
// the CNS volume handle) - use newGuestPVWithName when a test needs to exercise that mismatch.
func newGuestPV(handle string, accessModes ...v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	return newGuestPVWithName(handle, handle, accessModes...)
}

// newGuestPVWithName is newGuestPV with an explicit, independent PersistentVolume Name.
func newGuestPVWithName(pvName, handle string, accessModes ...v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	if len(accessModes) == 0 {
		accessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	}
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: v1.PersistentVolumeSpec{
			AccessModes: accessModes,
			Capacity: v1.ResourceList{
				v1.ResourceStorage: *resource.NewQuantity(1024*1024*1024, resource.BinarySI),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       csitypes.Name,
					VolumeHandle: handle,
				},
			},
		},
	}
}

func newVM(name string, volumes ...vmoperatortypes.VirtualMachineVolumeStatus) *vmoperatortypes.VirtualMachine {
	return &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: listVolumesTestNamespace},
		Status: vmoperatortypes.VirtualMachineStatus{
			Volumes: volumes,
		},
	}
}

// newSupervisorPVC builds a supervisor PersistentVolumeClaim with the given storage class name,
// used by the classification pass to tell legacy and FVS file volumes apart. An empty
// storageClassName produces a PVC with no storage class set at all (legacy).
func newSupervisorPVC(handle, storageClassName string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: handle, Namespace: listVolumesTestNamespace},
	}
	if storageClassName != "" {
		pvc.Spec.StorageClassName = &storageClassName
	}
	return pvc
}

// newCnsFileAccessConfig builds a CnsFileAccessConfig for the legacy file pass.
func newCnsFileAccessConfig(pvcName, vmName string, done bool,
	statusError string) *cnsfileaccessconfigv1alpha1.CnsFileAccessConfig {
	return &cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{
		ObjectMeta: metav1.ObjectMeta{Name: vmName + "-" + pvcName, Namespace: listVolumesTestNamespace},
		Spec: cnsfileaccessconfigv1alpha1.CnsFileAccessConfigSpec{
			PvcName: pvcName,
			VMName:  vmName,
		},
		Status: cnsfileaccessconfigv1alpha1.CnsFileAccessConfigStatus{
			Done:  done,
			Error: statusError,
		},
	}
}

// newGuestVolumeAttachment builds a guest VolumeAttachment as external-attacher would create
// for a CSI attach, with Status.Attached set as it would be once the attach has actually
// completed. Used by the FVS file volume echo path. Its Source.PersistentVolumeName equals
// handle; use newGuestVolumeAttachmentForPV when a test needs the PV name to differ from the
// handle, as it always does for a real guest PV.
func newGuestVolumeAttachment(handle, nodeName string) *storagev1.VolumeAttachment {
	return newGuestVolumeAttachmentForPV(handle, handle, nodeName)
}

// newGuestVolumeAttachmentForPV is newGuestVolumeAttachment with an explicit, independent PV
// name in Source.PersistentVolumeName.
func newGuestVolumeAttachmentForPV(pvName, handle, nodeName string) *storagev1.VolumeAttachment {
	name := pvName
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: volumeAttachmentName(handle, csitypes.Name, nodeName)},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: csitypes.Name,
			NodeName: nodeName,
			Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &name},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}
}

func entryForHandle(resp *csi.ListVolumesResponse, handle string) *csi.ListVolumesResponse_Entry {
	for _, e := range resp.Entries {
		if e.GetVolume().GetVolumeId() == handle {
			return e
		}
	}
	return nil
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func disableListVolumesFSS(t *testing.T) {
	t.Helper()
	fakeCO, ok := commonco.ContainerOrchestratorUtility.(interface {
		DisableFSS(ctx context.Context, featureName string) error
	})
	if !ok {
		t.Fatalf("fake container orchestrator does not implement DisableFSS")
	}
	if err := fakeCO.DisableFSS(context.Background(), common.ListVolumes); err != nil {
		t.Fatalf("failed to disable %s FSS: %v", common.ListVolumes, err)
	}
}

// TestListVolumesFSSDisabled verifies that ListVolumes returns Unimplemented when the
// list-volumes FSS is disabled.
func TestListVolumesFSSDisabled(t *testing.T) {
	c := newListVolumesController(t, nil, nil)
	disableListVolumesFSS(t)

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Unimplemented)
}

// TestListVolumesNegativeMaxEntries verifies that a negative MaxEntries is rejected with
// InvalidArgument.
func TestListVolumesNegativeMaxEntries(t *testing.T) {
	c := newListVolumesController(t, nil, nil)
	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{MaxEntries: -1})
	assertGRPCCode(t, err, codes.InvalidArgument)
}

// TestListVolumesBlockAttachPredicate verifies that a volume is reported published only when
// its VirtualMachine status entry has both Attached == true and a non-empty DiskUUID, and
// that an owned volume with neither is still reported with an empty, non-nil Status.
func TestListVolumesBlockAttachPredicate(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-attached", Attached: true, DiskUUID: "uuid-1"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-attached-no-uuid", Attached: true, DiskUUID: ""},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-not-attached", Attached: false, DiskUUID: "uuid-2"},
			),
		},
		[]runtime.Object{
			newGuestPV("vol-attached"),
			newGuestPV("vol-attached-no-uuid"),
			newGuestPV("vol-not-attached"),
			newGuestPV("vol-owned-nothing-attached"),
		})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	attached := entryForHandle(resp, "vol-attached")
	if attached == nil {
		t.Fatalf("expected an entry for %q", "vol-attached")
	}
	if got, want := attached.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}

	for _, handle := range []string{"vol-attached-no-uuid", "vol-not-attached", "vol-owned-nothing-attached"} {
		e := entryForHandle(resp, handle)
		if e == nil {
			t.Fatalf("handle %q must still appear in the response", handle)
		}
		if e.GetStatus() == nil {
			t.Fatalf("Status must never be nil for handle %q, lister silently drops nil-Status entries", handle)
		}
		if len(e.GetStatus().GetPublishedNodeIds()) != 0 {
			t.Errorf("handle %q: PublishedNodeIds = %v, want empty", handle, e.GetStatus().GetPublishedNodeIds())
		}
	}
}

// TestListVolumesDetachingSuffix verifies that a Status.Volumes entry suffixed with
// ":detaching" is reported published under its plain name, not the suffixed one.
func TestListVolumesDetachingSuffix(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1:detaching", Attached: true, DiskUUID: "uuid-1"},
			),
		},
		[]runtime.Object{newGuestPV("vol-1")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	e := entryForHandle(resp, "vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
	if entryForHandle(resp, "vol-1:detaching") != nil {
		t.Errorf("entry must not appear under the :detaching name")
	}
}

// TestListVolumesDetachingSuffixDedupesWithPlainName verifies that a VM reporting both "foo"
// and "foo:detaching" for the same volume contributes that node only once.
func TestListVolumesDetachingSuffixDedupesWithPlainName(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u1"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1:detaching", Attached: true, DiskUUID: "u1"},
			),
		},
		[]runtime.Object{newGuestPV("vol-1")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v (node must not be counted twice)", got, want)
	}
}

// TestListVolumesMultiAttach verifies that a volume attached on two VirtualMachines is
// reported published on both nodes, sorted.
func TestListVolumesMultiAttach(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a", vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u1"}),
			newVM("node-b", vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u2"}),
		},
		[]runtime.Object{newGuestPV("vol-1", v1.ReadWriteOnce)})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "vol-1")
	}
	got := sortedStrings(e.GetStatus().GetPublishedNodeIds())
	want := []string{"node-a", "node-b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesOwnershipScoping verifies that a volume attached on a VirtualMachine with
// no matching guest PersistentVolume never appears in the response, regardless of how the
// Supervisor namespace's VirtualMachines look.
func TestListVolumesOwnershipScoping(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "foreign-vol", Attached: true, DiskUUID: "u1"},
			),
		},
		nil) // No guest PVs, so nothing is owned.

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected no entries, got %+v", resp.Entries)
	}
}

// TestListVolumesOwnedFileVolumeIncludedButUnpublishedWithoutConfig verifies that an owned
// file volume with no matching CnsFileAccessConfig appears in the response (not omitted -
// omission would read to external-attacher as "detached" just the same) but with no published
// nodes, alongside a block volume reported via the ordinary VirtualMachine.Status.Volumes check.
func TestListVolumesOwnedFileVolumeIncludedButUnpublishedWithoutConfig(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a", vmoperatortypes.VirtualMachineVolumeStatus{
				Name: "block-vol-1", Attached: true, DiskUUID: "u1",
			}),
		},
		[]runtime.Object{
			newGuestPV("file-vol-1", v1.ReadWriteMany),
			newGuestPV("block-vol-1"),
		})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fileEntry := entryForHandle(resp, "file-vol-1")
	if fileEntry == nil {
		t.Fatalf("file volume must still appear in the response, not be omitted")
	}
	if got := fileEntry.GetStatus().GetPublishedNodeIds(); len(got) != 0 {
		t.Errorf("file-vol-1: PublishedNodeIds = %v, want empty (no CnsFileAccessConfig exists)", got)
	}
	e := entryForHandle(resp, "block-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "block-vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesEmptyVMListWithOwnedVolumesFails verifies that an empty VirtualMachine list
// fails the RPC when the cluster owns provisioned volumes, rather than returning an empty
// response that would read as everything being detached.
func TestListVolumesEmptyVMListWithOwnedVolumesFails(t *testing.T) {
	c := newListVolumesController(t,
		nil, // No VMs at all.
		[]runtime.Object{newGuestPV("vol-1")})

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.FailedPrecondition)
}

// TestListVolumesEmptyVMListWithNoOwnedVolumesSucceeds verifies that the empty-VM-list guard
// does not fire on a genuinely empty cluster that owns no volumes.
func TestListVolumesEmptyVMListWithNoOwnedVolumesSucceeds(t *testing.T) {
	c := newListVolumesController(t, nil, nil)
	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected no entries, got %+v", resp.Entries)
	}
}

// TestListVolumesTokenStateMachine verifies that a malformed, negative, or non-continuing
// starting_token is rejected with Aborted rather than serviced by rebuilding and paging from
// the supplied index.
func TestListVolumesTokenStateMachine(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{newVM("node-a",
			vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u1"},
			vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-2", Attached: true, DiskUUID: "u2"},
		)},
		[]runtime.Object{newGuestPV("vol-1"), newGuestPV("vol-2")})

	ctx := context.Background()
	first, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first.NextToken == "" {
		t.Fatalf("expected a non-empty NextToken")
	}

	tests := []struct {
		name  string
		token string
	}{
		{name: "not an integer", token: "not-an-integer"},
		{name: "negative generation", token: "-1"},
		{name: "negative index", token: "0:-1"},
		{name: "unknown generation", token: "999:0"},
		{name: "trailing garbage", token: first.NextToken + "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{StartingToken: tt.token})
			assertGRPCCode(t, err, codes.Aborted)
		})
	}
}

// TestListVolumesPaginationCoversAllEntriesOnce verifies that paging through a listing with
// MaxEntries smaller than the result set returns every entry exactly once, with no gaps or
// duplicates.
func TestListVolumesPaginationCoversAllEntriesOnce(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u1"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-2", Attached: true, DiskUUID: "u2"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-3", Attached: true, DiskUUID: "u3"},
			),
		},
		[]runtime.Object{newGuestPV("vol-1"), newGuestPV("vol-2"), newGuestPV("vol-3")})

	ctx := context.Background()
	seen := map[string]bool{}
	token := ""
	for {
		resp, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: token})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for _, e := range resp.Entries {
			handle := e.GetVolume().GetVolumeId()
			if seen[handle] {
				t.Errorf("handle %q returned twice", handle)
			}
			seen[handle] = true
		}
		token = resp.NextToken
		if token == "" {
			break
		}
	}
	if len(seen) != 3 {
		t.Errorf("got %d distinct handles, want 3", len(seen))
	}
}

// TestListVolumesReplayedFinalPageTokenReplays verifies that replaying the token from the
// final page re-serves the same final page instead of aborting. This matters because the
// cursor moves forward (and, on the final page, the sequence is considered complete) before
// the caller has any confirmation the response for that page actually reached the client: if
// it was lost in transit, the client retries the same starting_token, and that retry must not
// be punished with a full rebuild of the whole listing.
func TestListVolumesReplayedFinalPageTokenReplays(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u1"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-2", Attached: true, DiskUUID: "u2"},
			),
		},
		[]runtime.Object{newGuestPV("vol-1"), newGuestPV("vol-2")})

	ctx := context.Background()
	first, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if first.NextToken == "" {
		t.Fatalf("expected a non-empty NextToken")
	}

	second, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: first.NextToken})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if second.NextToken != "" {
		t.Fatalf("expected an empty NextToken on the final page, got %q", second.NextToken)
	}

	replayed, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: first.NextToken})
	if err != nil {
		t.Fatalf("unexpected error replaying the final page's starting_token: %v", err)
	}
	if len(replayed.Entries) != 1 || replayed.Entries[0].Volume.VolumeId != second.Entries[0].Volume.VolumeId {
		t.Fatalf("expected the replay to return the same final page %+v, got %+v",
			second.Entries, replayed.Entries)
	}
	if replayed.NextToken != "" {
		t.Fatalf("expected an empty NextToken on the replayed final page, got %q", replayed.NextToken)
	}
}

// TestListVolumesEntryOrderStable verifies that entries are returned sorted by volume
// handle, not in the randomized order Go map iteration would otherwise produce.
func TestListVolumesEntryOrderStable(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-c", Attached: true, DiskUUID: "u1"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-a", Attached: true, DiskUUID: "u2"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-b", Attached: true, DiskUUID: "u3"},
			),
		},
		[]runtime.Object{newGuestPV("vol-c"), newGuestPV("vol-a"), newGuestPV("vol-b")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var handles []string
	for _, e := range resp.Entries {
		handles = append(handles, e.GetVolume().GetVolumeId())
	}
	want := []string{"vol-a", "vol-b", "vol-c"}
	if !reflect.DeepEqual(handles, want) {
		t.Errorf("entry order = %v, want %v", handles, want)
	}
}

// TestListVolumesVMListErrorPropagates verifies that a failure listing Supervisor
// VirtualMachines fails the whole RPC rather than returning a response assembled from
// partial data.
func TestListVolumesVMListErrorPropagates(t *testing.T) {
	c := newListVolumesController(t, nil, []runtime.Object{newGuestPV("vol-1")})

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add vmoperator types to scheme: %v", err)
	}
	c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(
		interceptor.Funcs{
			List: func(ctx context.Context, cli ctrlclient.WithWatch, list ctrlclient.ObjectList,
				opts ...ctrlclient.ListOption) error {
				if _, ok := list.(*vmoperatortypes.VirtualMachineList); ok {
					return errors.New("injected VirtualMachine list failure")
				}
				return cli.List(ctx, list, opts...)
			},
		},
	).Build()

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Internal)
}

// TestListVolumesGuestPVListErrorPropagates verifies that a failure listing guest
// PersistentVolumes fails the whole RPC rather than returning a response assembled from
// partial data.
func TestListVolumesGuestPVListErrorPropagates(t *testing.T) {
	c := newListVolumesController(t, nil, nil)
	c.guestClient.(*testclient.Clientset).PrependReactor("list", "persistentvolumes",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("injected guest PersistentVolume list failure")
		})

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Internal)
}

// TestListVolumesStatusErrorStillReportedPublished verifies that Attached && DiskUUID != ""
// is treated as an affirmative statement that a disk is on the VM, even when Status.Error is
// also set. A non-empty Error most plausibly reflects a failed detach retry rather than a
// failed attach, so suppressing the entry would report published nowhere on a volume whose
// disk is demonstrably present, which is the exact regression this RPC exists to avoid.
func TestListVolumesStatusErrorStillReportedPublished(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a", vmoperatortypes.VirtualMachineVolumeStatus{
				Name: "vol-1", Attached: true, DiskUUID: "u1", Error: "some transient detach error",
			}),
		},
		[]runtime.Object{newGuestPV("vol-1")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesAlwaysSetsNonNilStatus verifies that every entry has a non-nil Status even
// when PublishedNodeIds is empty, since a nil Status is silently dropped by the attacher's
// lister.
func TestListVolumesAlwaysSetsNonNilStatus(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{newVM("node-a")},
		[]runtime.Object{newGuestPV("vol-1"), newGuestPV("vol-2")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(resp.Entries))
	}
	for _, e := range resp.Entries {
		if e.GetStatus() == nil {
			t.Errorf("handle %q must have a non-nil Status", e.GetVolume().GetVolumeId())
		}
	}
}

// TestListVolumesConcurrentPagination holds a response returned by one full listing, then
// triggers a rebuild via a second full listing, and confirms the first response's
// PublishedNodeIds slice is untouched. A rebuild replaces the cache wholesale rather than
// mutating it in place, so an older response must never observe a write happening after its
// RPC returned.
func TestListVolumesConcurrentPagination(t *testing.T) {
	c := newListVolumesController(t,
		[]ctrlclient.Object{
			newVM("node-a",
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-1", Attached: true, DiskUUID: "u1"},
				vmoperatortypes.VirtualMachineVolumeStatus{Name: "vol-2", Attached: true, DiskUUID: "u2"},
			),
		},
		[]runtime.Object{newGuestPV("vol-1"), newGuestPV("vol-2")})

	held, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	heldNodes := held.Entries[0].GetStatus().GetPublishedNodeIds()
	heldCopy := append([]string(nil), heldNodes...)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{}); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if !reflect.DeepEqual(heldCopy, heldNodes) {
		t.Errorf("an older response's PublishedNodeIds was mutated by a later rebuild: got %v, want %v",
			heldNodes, heldCopy)
	}
}

// TestControllerGetCapabilitiesListVolumesGating also serves as the regression test for the
// wcp bug class where appending to the package-level controllerCaps slice on every call grows
// it unboundedly across repeated invocations.
func TestControllerGetCapabilitiesListVolumesGating(t *testing.T) {
	c := newListVolumesController(t, nil, nil)
	ctx := context.Background()

	before := len(controllerCaps)
	resp, err := c.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasCap(resp, csi.ControllerServiceCapability_RPC_LIST_VOLUMES) {
		t.Errorf("expected LIST_VOLUMES to be advertised when the FSS is enabled")
	}
	if !hasCap(resp, csi.ControllerServiceCapability_RPC_LIST_VOLUMES_PUBLISHED_NODES) {
		t.Errorf("expected LIST_VOLUMES_PUBLISHED_NODES to be advertised when the FSS is enabled")
	}

	disableListVolumesFSS(t)
	resp2, err := c.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasCap(resp2, csi.ControllerServiceCapability_RPC_LIST_VOLUMES) {
		t.Errorf("expected LIST_VOLUMES not to be advertised when the FSS is disabled")
	}

	if got := len(controllerCaps); got != before {
		t.Errorf("len(controllerCaps) = %d, want %d (package-level slice must not grow across calls)", got, before)
	}
}

// TestListVolumesLargePublishedPairDropSucceeds verifies that a rebuild seeing far fewer
// published pairs than an earlier rebuild is not treated as a possibly-partial listing: this
// flavor has no second, independently-measured system to compare against (unlike vanilla/wcp,
// where the check compares two quantities from the same pass and self-heals), so a large drop
// from a real event like a scale-down must not wedge ListVolumes. Three consecutive successful
// rebuilds are asserted, not just one, since a latching guard that updates its remembered value
// only on success would otherwise only be caught by a second and third call after the drop.
func TestListVolumesLargePublishedPairDropSucceeds(t *testing.T) {
	handles := make([]string, 10)
	pvs := make([]runtime.Object, 10)
	allAttached := make([]vmoperatortypes.VirtualMachineVolumeStatus, 10)
	for i := 0; i < 10; i++ {
		handle := fmt.Sprintf("vol-%d", i)
		handles[i] = handle
		pvs[i] = newGuestPV(handle)
		allAttached[i] = vmoperatortypes.VirtualMachineVolumeStatus{
			Name: handle, Attached: true, DiskUUID: fmt.Sprintf("uuid-%d", i),
		}
	}

	vm := newVM("node-a", allAttached...)
	c := newListVolumesController(t, []ctrlclient.Object{vm}, pvs)

	ctx := context.Background()
	first, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error on first rebuild: %v", err)
	}
	if got := countPublishedPairs(first); got != 10 {
		t.Fatalf("first rebuild: got %d published pairs, want 10", got)
	}

	// Detach 9 of the 10 volumes, leaving only 2 statuses (one still attached, one detached)
	// so the VM object need not be empty.
	vm.Status.Volumes = []vmoperatortypes.VirtualMachineVolumeStatus{
		allAttached[0],
		{Name: handles[1], Attached: false, DiskUUID: ""},
	}
	if err := c.vmOperatorClient.Update(ctx, vm); err != nil {
		t.Fatalf("failed to update VM status: %v", err)
	}

	for i := 0; i < 3; i++ {
		resp, err := c.ListVolumes(ctx, &csi.ListVolumesRequest{})
		if err != nil {
			t.Fatalf("rebuild %d: unexpected error after published-pair drop: %v", i+1, err)
		}
		if len(resp.Entries) != 10 {
			t.Errorf("rebuild %d: got %d entries, want all 10 volumes listed", i+1, len(resp.Entries))
		}
		if got := countPublishedPairs(resp); got != 1 {
			t.Errorf("rebuild %d: got %d published pairs, want 1", i+1, got)
		}
	}
}

func countPublishedPairs(resp *csi.ListVolumesResponse) int {
	count := 0
	for _, e := range resp.Entries {
		count += len(e.GetStatus().GetPublishedNodeIds())
	}
	return count
}

func hasCap(resp *csi.ControllerGetCapabilitiesResponse, capType csi.ControllerServiceCapability_RPC_Type) bool {
	for _, c := range resp.Capabilities {
		if c.GetRpc().GetType() == capType {
			return true
		}
	}
	return false
}

// TestListVolumesLegacyFileVolumeDonePublished verifies that a legacy file volume with a
// Status.Done CnsFileAccessConfig is reported published on the config's VMName: a check that
// only reads VirtualMachine.Status would call every file volume unattached forever.
func TestListVolumesLegacyFileVolumeDonePublished(t *testing.T) {
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		[]ctrlclient.Object{newCnsFileAccessConfig("file-vol-1", "node-a", true, "")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "file-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "file-vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesLegacyFileVolumeNotDoneUnpublished verifies that a legacy file volume whose
// CnsFileAccessConfig has not completed (Status.Done false) is reported with no published
// nodes, rather than trusting an in-progress ACL grant.
func TestListVolumesLegacyFileVolumeNotDoneUnpublished(t *testing.T) {
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		[]ctrlclient.Object{newCnsFileAccessConfig("file-vol-1", "node-a", false, "")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "file-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "file-vol-1")
	}
	if got := e.GetStatus().GetPublishedNodeIds(); len(got) != 0 {
		t.Errorf("PublishedNodeIds = %v, want empty", got)
	}
}

// TestListVolumesLegacyFileVolumeErrorUnpublished verifies that a CnsFileAccessConfig with a
// non-empty Status.Error is not treated as published, even if Status.Done is true.
func TestListVolumesLegacyFileVolumeErrorUnpublished(t *testing.T) {
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		[]ctrlclient.Object{newCnsFileAccessConfig("file-vol-1", "node-a", true, "some ACL error")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "file-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "file-vol-1")
	}
	if got := e.GetStatus().GetPublishedNodeIds(); len(got) != 0 {
		t.Errorf("PublishedNodeIds = %v, want empty", got)
	}
}

// TestListVolumesLegacyFileVolumeDeletingUnpublished verifies that a CnsFileAccessConfig with a
// deletion timestamp is not treated as published, since CnsOperator is in the process of
// revoking access for it.
func TestListVolumesLegacyFileVolumeDeletingUnpublished(t *testing.T) {
	cfg := newCnsFileAccessConfig("file-vol-1", "node-a", true, "")
	now := metav1.Now()
	cfg.DeletionTimestamp = &now
	cfg.Finalizers = []string{"test.vmware.com/keep-around-for-fake-client"}

	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		[]ctrlclient.Object{cfg})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "file-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "file-vol-1")
	}
	if got := e.GetStatus().GetPublishedNodeIds(); len(got) != 0 {
		t.Errorf("PublishedNodeIds = %v, want empty", got)
	}
}

// TestListVolumesFVSFileVolumeEchoesVolumeAttachment verifies that an FVS-backed file volume
// (a supervisor PVC on one of the vSAN File Service marker storage classes) is not reported
// unpublished merely for lacking a CnsFileAccessConfig - it reports the node named by its
// guest VolumeAttachment instead. This is the regression test for an attach-storm-on-every-
// FVS-volume bug: skipping file volumes entirely would report every FVS volume unpublished on
// every reconcile cycle.
func TestListVolumesFVSFileVolumeEchoesVolumeAttachment(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{
			newGuestPV("fvs-vol-1", v1.ReadWriteMany),
			newGuestVolumeAttachment("fvs-vol-1", "node-a"),
		},
		[]runtime.Object{newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy)},
		nil) // No CnsFileAccessConfig - FVS volumes never get one.

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "fvs-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "fvs-vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesFVSFileVolumePVNameDiffersFromHandle verifies the FVS echo path matches
// VolumeAttachments by PV name, not by the CNS volume handle - unlike a real guest PV, other
// FVS tests in this file give the PV the same name as the handle, which would mask this bug.
func TestListVolumesFVSFileVolumePVNameDiffersFromHandle(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	handle := "fvs-vol-1"
	pvName := "pv-" + handle
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{
			newGuestPVWithName(pvName, handle, v1.ReadWriteMany),
			newGuestVolumeAttachmentForPV(pvName, handle, "node-a"),
		},
		[]runtime.Object{newSupervisorPVC(handle, common.StorageClassVsanFileServicePolicy)},
		nil) // No CnsFileAccessConfig - FVS volumes never get one.

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, handle)
	if e == nil {
		t.Fatalf("expected an entry for %q", handle)
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesFVSFileVolumeNoVolumeAttachmentUnpublished verifies that an FVS file volume
// with no guest VolumeAttachment at all is reported with no published nodes - there is nothing
// to echo.
func TestListVolumesFVSFileVolumeNoVolumeAttachmentUnpublished(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("fvs-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy)},
		nil)

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "fvs-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "fvs-vol-1")
	}
	if got := e.GetStatus().GetPublishedNodeIds(); len(got) != 0 {
		t.Errorf("PublishedNodeIds = %v, want empty", got)
	}
}

// TestListVolumesFVSFileVolumeNotYetAttachedUnpublished verifies that a VolumeAttachment whose
// Status.Attached is still false is not echoed as published: the object exists once created,
// before the attach completes, so echoing on presence alone would report an in-flight attach as
// already done.
func TestListVolumesFVSFileVolumeNotYetAttachedUnpublished(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	va := newGuestVolumeAttachment("fvs-vol-1", "node-a")
	va.Status.Attached = false

	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{
			newGuestPV("fvs-vol-1", v1.ReadWriteMany),
			va,
		},
		[]runtime.Object{newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy)},
		nil)

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "fvs-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "fvs-vol-1")
	}
	if got := e.GetStatus().GetPublishedNodeIds(); len(got) != 0 {
		t.Errorf("PublishedNodeIds = %v, want empty (attach not yet completed)", got)
	}
}

// TestListVolumesFVSFileVolumeIgnoresCnsFileAccessConfig verifies that even if a
// CnsFileAccessConfig somehow exists for an FVS-classified handle, the legacy file pass does
// not use it - classification, not object presence, decides which check applies.
func TestListVolumesFVSFileVolumeIgnoresCnsFileAccessConfig(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{
			newGuestPV("fvs-vol-1", v1.ReadWriteMany),
			newGuestVolumeAttachment("fvs-vol-1", "node-a"),
		},
		[]runtime.Object{newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy)},
		[]ctrlclient.Object{newCnsFileAccessConfig("fvs-vol-1", "node-b", true, "")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "fvs-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "fvs-vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v (only the VolumeAttachment echo, not the stray CnsFileAccessConfig)",
			got, want)
	}
}

// TestListVolumesFVSClassificationDisabledByFSS verifies that classifyFileVolumes does not
// reclassify a PVC on the vSAN File Service storage class as FVS when
// IsVsanFileVolumeServiceEnabled is false: the storage-class name is not trusted on its own
// once the capability has been disabled (e.g. after a rollback), so the volume stays
// volumeKindLegacyFile and is reported via its CnsFileAccessConfig instead of the
// VolumeAttachment echo.
func TestListVolumesFVSClassificationDisabledByFSS(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, false)
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{
			newGuestPV("fvs-vol-1", v1.ReadWriteMany),
			newGuestVolumeAttachment("fvs-vol-1", "node-b"),
		},
		[]runtime.Object{newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy)},
		[]ctrlclient.Object{newCnsFileAccessConfig("fvs-vol-1", "node-a", true, "")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "fvs-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "fvs-vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v (legacy CnsFileAccessConfig path, FVS echo skipped)", got, want)
	}
}

// TestListVolumesEmptyVMListWithOnlyFileVolumesSucceeds verifies that the empty-VirtualMachine-
// list safety guard does not fire for a cluster that owns only file volumes: file volumes never
// appear in VirtualMachine.Status.Volumes regardless of cluster health, so an empty VM list
// says nothing about them.
func TestListVolumesEmptyVMListWithOnlyFileVolumesSucceeds(t *testing.T) {
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil, // No VMs at all.
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		[]ctrlclient.Object{newCnsFileAccessConfig("file-vol-1", "node-a", true, "")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	e := entryForHandle(resp, "file-vol-1")
	if e == nil {
		t.Fatalf("expected an entry for %q", "file-vol-1")
	}
	if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("PublishedNodeIds = %v, want %v", got, want)
	}
}

// TestListVolumesMixedBlockAndFileVolumes verifies that a single listing correctly reports a
// block volume (via VirtualMachine.Status.Volumes), a legacy file volume (via
// CnsFileAccessConfig) and an FVS file volume (via the VolumeAttachment echo) at once, each
// through its own applicable check.
func TestListVolumesMixedBlockAndFileVolumes(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newListVolumesControllerWithSupervisorObjs(t,
		[]ctrlclient.Object{
			newVM("node-a", vmoperatortypes.VirtualMachineVolumeStatus{
				Name: "block-vol-1", Attached: true, DiskUUID: "u1",
			}),
		},
		[]runtime.Object{
			newGuestPV("block-vol-1"),
			newGuestPV("legacy-file-vol-1", v1.ReadWriteMany),
			newGuestPV("fvs-vol-1", v1.ReadWriteMany),
			newGuestVolumeAttachment("fvs-vol-1", "node-a"),
		},
		[]runtime.Object{
			newSupervisorPVC("legacy-file-vol-1", ""),
			newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy),
		},
		[]ctrlclient.Object{newCnsFileAccessConfig("legacy-file-vol-1", "node-a", true, "")})

	resp, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(resp.Entries))
	}
	for _, handle := range []string{"block-vol-1", "legacy-file-vol-1", "fvs-vol-1"} {
		e := entryForHandle(resp, handle)
		if e == nil {
			t.Fatalf("expected an entry for %q", handle)
		}
		if got, want := e.GetStatus().GetPublishedNodeIds(), []string{"node-a"}; !reflect.DeepEqual(got, want) {
			t.Errorf("handle %q: PublishedNodeIds = %v, want %v", handle, got, want)
		}
	}
}

// TestListVolumesLegacyFileVolumeVMListErrorPropagates verifies that a cluster owning only
// legacy file volumes still propagates a VirtualMachine list failure, since the block pass
// always runs regardless of which volume kinds are owned.
func TestListVolumesLegacyFileVolumeVMListErrorPropagates(t *testing.T) {
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		[]ctrlclient.Object{newCnsFileAccessConfig("file-vol-1", "node-a", true, "")})

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add vmoperator types to scheme: %v", err)
	}
	c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(
		interceptor.Funcs{
			List: func(ctx context.Context, cli ctrlclient.WithWatch, list ctrlclient.ObjectList,
				opts ...ctrlclient.ListOption) error {
				if _, ok := list.(*vmoperatortypes.VirtualMachineList); ok {
					return errors.New("injected VirtualMachine list failure")
				}
				return cli.List(ctx, list, opts...)
			},
		},
	).Build()

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Internal)
}

// TestListVolumesCnsFileAccessConfigListErrorPropagates verifies that a failure listing
// CnsFileAccessConfig objects fails the whole RPC rather than returning a response assembled
// from partial data.
func TestListVolumesCnsFileAccessConfigListErrorPropagates(t *testing.T) {
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("file-vol-1", "")},
		nil)

	scheme := runtime.NewScheme()
	if err := cnsoperatorapis.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add cnsoperator types to scheme: %v", err)
	}
	c.cnsOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(
		interceptor.Funcs{
			List: func(ctx context.Context, cli ctrlclient.WithWatch, list ctrlclient.ObjectList,
				opts ...ctrlclient.ListOption) error {
				if _, ok := list.(*cnsfileaccessconfigv1alpha1.CnsFileAccessConfigList); ok {
					return errors.New("injected CnsFileAccessConfig list failure")
				}
				return cli.List(ctx, list, opts...)
			},
		},
	).Build()

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Internal)
}

// TestListVolumesSupervisorPVCListErrorPropagates verifies that a failure listing supervisor
// PersistentVolumeClaims (the classification pass) fails the whole RPC.
func TestListVolumesSupervisorPVCListErrorPropagates(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("file-vol-1", v1.ReadWriteMany)},
		nil,
		nil)
	c.supervisorClient.(*testclient.Clientset).PrependReactor("list", "persistentvolumeclaims",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("injected supervisor PersistentVolumeClaim list failure")
		})

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Internal)
}

// TestListVolumesFVSVolumeAttachmentListErrorPropagates verifies that a failure listing guest
// VolumeAttachments (the FVS echo path) fails the whole RPC rather than reporting every owned
// FVS volume as unpublished: since the echo path is the only source of published-node truth for
// FVS volumes, silently swallowing this error would make a healthy, attached FVS volume
// indistinguishable from a genuinely detached one, and external-attacher would patch its
// VolumeAttachment to Attached=false on the strength of that wrong answer.
func TestListVolumesFVSVolumeAttachmentListErrorPropagates(t *testing.T) {
	setVsanFileVolumeServiceEnabledForTest(t, true)
	c := newListVolumesControllerWithSupervisorObjs(t,
		nil,
		[]runtime.Object{newGuestPV("fvs-vol-1", v1.ReadWriteMany)},
		[]runtime.Object{newSupervisorPVC("fvs-vol-1", common.StorageClassVsanFileServicePolicy)},
		nil)
	c.guestClient.(*testclient.Clientset).PrependReactor("list", "volumeattachments",
		func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("injected guest VolumeAttachment list failure")
		})

	_, err := c.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
	assertGRPCCode(t, err, codes.Internal)
}
