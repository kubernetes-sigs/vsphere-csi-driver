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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	testclient "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const listVolumesTestNamespace = "test-namespace"

// newListVolumesController builds a controller with a fresh fake CO interface, restored via
// t.Cleanup, and fresh fake clients for the guest clientset and the vmOperator
// controller-runtime client. commonco.ContainerOrchestratorUtility is a package global whose
// fake FSS map is otherwise shared across tests, so each test needs its own to avoid an FSS
// toggled in one test leaking into another.
func newListVolumesController(t *testing.T, vmObjs []ctrlclient.Object, guestObjs []runtime.Object) *controller {
	t.Helper()

	prevCO := commonco.ContainerOrchestratorUtility
	fakeCO, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatalf("failed to create fake container orchestrator: %v", err)
	}
	commonco.ContainerOrchestratorUtility = fakeCO
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = prevCO })

	scheme := runtime.NewScheme()
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add vmoperator types to scheme: %v", err)
	}
	vmOperatorClient := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(vmObjs...).Build()

	return &controller{
		guestClient:         testclient.NewClientset(guestObjs...),
		vmOperatorClient:    vmOperatorClient,
		supervisorNamespace: listVolumesTestNamespace,
	}
}

func newGuestPV(handle string, accessModes ...v1.PersistentVolumeAccessMode) *v1.PersistentVolume {
	if len(accessModes) == 0 {
		accessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	}
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: handle},
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

// TestListVolumesOwnedFileVolumeExcluded verifies that this RPC, which handles block volumes
// only, excludes an owned file volume from the ownership pass rather than admitting it and
// reporting it unpublished (which would assert a detach that never happened) or failing the
// whole rebuild (which would let a single file PV disable ListVolumes for every block volume
// in the cluster for as long as that PV exists).
func TestListVolumesOwnedFileVolumeExcluded(t *testing.T) {
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
	if entryForHandle(resp, "file-vol-1") != nil {
		t.Errorf("file volume must not appear in a block-only listing")
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

// TestListVolumesReplayedFinalPageTokenAborts verifies that replaying the token from the
// final page is rejected with Aborted, since the cache is discarded once the listing
// completes, rather than served a stale page.
func TestListVolumesReplayedFinalPageTokenAborts(t *testing.T) {
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

	_, err = c.ListVolumes(ctx, &csi.ListVolumesRequest{MaxEntries: 1, StartingToken: first.NextToken})
	assertGRPCCode(t, err, codes.Aborted)
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
