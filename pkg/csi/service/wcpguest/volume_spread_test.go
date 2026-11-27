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
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatorv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	testclient "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const (
	spreadTestNamespace   = "test-ns"
	spreadTestNode        = "test-node"
	spreadTestPodNS       = "app"
	spreadTestPod         = "db-0"
	spreadTestSC          = "vsphere-sc"
	spreadTestHandlePrefx = "cluster-uid-"
)

// setPodVolumeSpreadState overrides the two feature checks for one test.
func setPodVolumeSpreadState(t *testing.T, fssEnabled, supported bool) {
	t.Helper()
	origFSS, origSupported := isPodVolumeSpreadFSSEnabled, isPodVolumeSpreadSupported
	isPodVolumeSpreadFSSEnabled = func(context.Context) bool { return fssEnabled }
	isPodVolumeSpreadSupported = func(context.Context) bool { return supported }
	t.Cleanup(func() {
		isPodVolumeSpreadFSSEnabled, isPodVolumeSpreadSupported = origFSS, origSupported
	})
}

// busState builds a spreadVMState from "used/capacity/podVolumes" triples.
func busState(buses ...[3]int) spreadVMState {
	var state spreadVMState
	for i, b := range buses {
		state[i] = spreadBusState{used: b[0], capacity: b[1], podVolumes: b[2]}
	}
	return state
}

func emptyBusState() spreadVMState {
	return busState([3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0})
}

func TestFeatureChecksWithoutOrchestrator(t *testing.T) {
	// The default checks must not panic, and must report "off", when the
	// container orchestrator has not been initialized (as in most unit tests).
	ctx := context.Background()
	if isPodVolumeSpreadFSSEnabled(ctx) && isPodVolumeSpreadSupported(ctx) {
		t.Skip("container orchestrator is initialized with the feature on in this test binary")
	}
	if podVolumeSpreadNeedsLateEnablement(ctx) && !isPodVolumeSpreadFSSEnabled(ctx) {
		t.Error("late enablement must not be requested when the FSS is off")
	}
}

func TestPodVolumeSpreadNeedsLateEnablement(t *testing.T) {
	tests := []struct {
		name           string
		fss, supported bool
		want           bool
	}{
		{"FSS off", false, false, false},
		{"FSS on, capability missing", true, false, true},
		{"FSS on, capability present", true, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setPodVolumeSpreadState(t, tc.fss, tc.supported)
			if got := podVolumeSpreadNeedsLateEnablement(context.Background()); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvenSpreadSharesAndString(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{1, "1/0/0/0"}, {3, "1/1/1/0"}, {4, "1/1/1/1"}, {5, "2/1/1/1"}, {8, "2/2/2/2"}, {10, "3/3/2/2"},
	}
	for _, tc := range tests {
		if got := evenSpreadShares(tc.n).String(); got != tc.want {
			t.Errorf("evenSpreadShares(%d) = %s, want %s", tc.n, got, tc.want)
		}
	}
	// String sorts largest first regardless of which bus holds which share.
	if got := (spreadShares{1, 2, 1, 1}).String(); got != "2/1/1/1" {
		t.Errorf("String() = %s, want 2/1/1/1", got)
	}
}

func TestSpreadBusStateFree(t *testing.T) {
	if got := (spreadBusState{used: 10, capacity: 64}).free(); got != 54 {
		t.Errorf("free() = %d, want 54", got)
	}
	if got := (spreadBusState{used: 70, capacity: 64}).free(); got != 0 {
		t.Errorf("free() of an over-full bus = %d, want 0", got)
	}
	if got := (spreadBusState{used: 3, capacity: 0}).free(); got != 0 {
		t.Errorf("free() of an unusable bus = %d, want 0", got)
	}
}

func TestComputeSpreadShares(t *testing.T) {
	tests := []struct {
		name    string
		n       int
		state   spreadVMState
		want    spreadShares
		wantErr string
	}{
		{name: "8 volumes on an empty VM", n: 8, state: emptyBusState(), want: spreadShares{2, 2, 2, 2}},
		{name: "4 volumes", n: 4, state: emptyBusState(), want: spreadShares{1, 1, 1, 1}},
		{name: "3 volumes, ties go to the lowest buses", n: 3, state: emptyBusState(), want: spreadShares{1, 1, 1, 0}},
		{name: "1 volume", n: 1, state: emptyBusState(), want: spreadShares{1, 0, 0, 0}},
		{
			name: "larger share goes to the bus with the most room",
			n:    5,
			// bus 0 holds the OS disk and other volumes, bus 2 is emptiest.
			state: busState([3]int{20, 64, 0}, [3]int{5, 64, 0}, [3]int{0, 64, 0}, [3]int{5, 64, 0}),
			want:  spreadShares{1, 1, 2, 1},
		},
		{
			name:  "10 volumes, two larger shares to the two emptiest buses",
			n:     10,
			state: busState([3]int{30, 64, 0}, [3]int{1, 64, 0}, [3]int{20, 64, 0}, [3]int{2, 64, 0}),
			want:  spreadShares{2, 3, 2, 3},
		},
		{
			name: "a bus that already holds the larger share keeps it",
			n:    5,
			// bus 0 already has two of the pod's volumes although it is the fullest.
			state: busState([3]int{40, 64, 2}, [3]int{1, 64, 1}, [3]int{0, 64, 0}, [3]int{0, 64, 0}),
			want:  spreadShares{2, 1, 1, 1},
		},
		{
			name:  "siblings already placed count toward the share",
			n:     8,
			state: busState([3]int{3, 64, 2}, [3]int{2, 64, 2}, [3]int{1, 64, 1}, [3]int{0, 64, 0}),
			want:  spreadShares{2, 2, 2, 2},
		},
		{
			name:  "a nearly full bus that still fits its share",
			n:     8,
			state: busState([3]int{62, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}),
			want:  spreadShares{2, 2, 2, 2},
		},
		{
			name:    "a bus without room for its share",
			n:       8,
			state:   busState([3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{64, 64, 0}),
			wantErr: "pod needs 2/2/2/2 across SCSI 0-3; bus 3 has 0 free slots, needs 2",
		},
		{
			name:    "a shared controller has no capacity",
			n:       4,
			state:   busState([3]int{0, 64, 0}, [3]int{0, 0, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}),
			wantErr: "bus 1 has 0 free slots, needs 1",
		},
		{
			name: "3 volumes avoid the one full bus",
			n:    3,
			// With 3 volumes one bus gets nothing; it must be the full one.
			state: busState([3]int{64, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}),
			want:  spreadShares{0, 1, 1, 1},
		},
		{
			name:    "volumes already attached unevenly",
			n:       8,
			state:   busState([3]int{6, 64, 5}, [3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}),
			wantErr: "bus 0 already holds 5 of the pod's volumes",
		},
		{
			name:    "more buses hold the larger share than the layout allows",
			n:       5,
			state:   busState([3]int{2, 64, 2}, [3]int{2, 64, 2}, [3]int{0, 64, 0}, [3]int{0, 64, 0}),
			wantErr: "2 buses already hold 2 of the pod's volumes",
		},
		{
			name:    "every volume is already placed",
			n:       4,
			state:   busState([3]int{1, 64, 1}, [3]int{1, 64, 1}, [3]int{1, 64, 1}, [3]int{1, 64, 1}),
			wantErr: "no share is left for this volume",
		},
		{name: "no volumes", n: 0, state: emptyBusState(), wantErr: "no block volumes to spread"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := computeSpreadShares(tc.n, tc.state)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("shares = %v, want %v", got, tc.want)
			}
			total := 0
			for _, share := range got {
				total += share
			}
			if total != tc.n {
				t.Errorf("shares %v sum to %d, want %d", got, total, tc.n)
			}
		})
	}
}

func TestChooseSpreadBus(t *testing.T) {
	tests := []struct {
		name    string
		shares  spreadShares
		state   spreadVMState
		want    int32
		wantErr bool
	}{
		{
			name:   "first volume, all equal, lowest bus",
			shares: spreadShares{2, 2, 2, 2}, state: emptyBusState(), want: 0,
		},
		{
			name:   "bus furthest from its share wins",
			shares: spreadShares{2, 2, 2, 2},
			state:  busState([3]int{1, 64, 1}, [3]int{1, 64, 1}, [3]int{0, 64, 0}, [3]int{1, 64, 1}),
			want:   2,
		},
		{
			name:   "tie goes to the bus with fewer devices",
			shares: spreadShares{1, 1, 1, 1},
			state:  busState([3]int{20, 64, 0}, [3]int{3, 64, 0}, [3]int{9, 64, 0}, [3]int{3, 64, 0}),
			want:   1,
		},
		{
			name:   "buses with no share left are skipped",
			shares: spreadShares{1, 1, 1, 0},
			state:  busState([3]int{1, 64, 1}, [3]int{1, 64, 1}, [3]int{5, 64, 0}, [3]int{0, 64, 0}),
			want:   2,
		},
		{
			name:    "every share is filled",
			shares:  spreadShares{1, 1, 1, 1},
			state:   busState([3]int{1, 64, 1}, [3]int{1, 64, 1}, [3]int{1, 64, 1}, [3]int{1, 64, 1}),
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := chooseSpreadBus(tc.shares, tc.state)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("bus = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestSpreadSequenceIsEven places a pod's volumes one at a time, the way
// successive ControllerPublishVolume calls do, and checks the final layout.
func TestSpreadSequenceIsEven(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 10, 13} {
		t.Run(fmt.Sprintf("%d volumes", n), func(t *testing.T) {
			// Bus 0 starts with the OS disk and some default-placed volumes.
			state := busState([3]int{6, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0}, [3]int{0, 64, 0})
			for i := 0; i < n; i++ {
				shares, err := computeSpreadShares(n, state)
				if err != nil {
					t.Fatalf("volume %d: %v", i, err)
				}
				bus, err := chooseSpreadBus(shares, state)
				if err != nil {
					t.Fatalf("volume %d: %v", i, err)
				}
				state[bus].used++
				state[bus].podVolumes++
			}
			minShare, maxShare := n, 0
			for _, b := range state {
				minShare, maxShare = min(minShare, b.podVolumes), max(maxShare, b.podVolumes)
			}
			if maxShare-minShare > 1 {
				t.Errorf("layout %+v is not even", state)
			}
			if want := (n + spreadSCSIBusCount - 1) / spreadSCSIBusCount; maxShare != want {
				t.Errorf("largest share = %d, want ceil(%d/4) = %d", maxShare, n, want)
			}
		})
	}
}

func spreadVMVolume(name string, controllerType vmoperatorv1alpha5.VirtualControllerType,
	bus *int32) vmoperatorv1alpha5.VirtualMachineVolume {
	return vmoperatorv1alpha5.VirtualMachineVolume{
		Name: name,
		VirtualMachineVolumeSource: vmoperatorv1alpha5.VirtualMachineVolumeSource{
			PersistentVolumeClaim: &vmoperatorv1alpha5.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{ClaimName: name},
			},
		},
		ControllerType:      controllerType,
		ControllerBusNumber: bus,
		UnitNumber:          ptr.To(int32(0)),
	}
}

func TestSpreadStateFromVM(t *testing.T) {
	scsi := vmoperatorv1alpha5.VirtualControllerTypeSCSI

	t.Run("no controllers and no volumes", func(t *testing.T) {
		state, err := spreadStateFromVM(&vmoperatorv1alpha5.VirtualMachine{}, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for bus, b := range state {
			if b.capacity != 64 || b.used != 0 || b.podVolumes != 0 {
				t.Errorf("bus %d = %+v, want an empty bus with 64 slots", bus, b)
			}
		}
	})

	t.Run("counts devices and siblings per bus", func(t *testing.T) {
		vm := &vmoperatorv1alpha5.VirtualMachine{}
		vm.Spec.Volumes = []vmoperatorv1alpha5.VirtualMachineVolume{
			spreadVMVolume("boot", scsi, ptr.To(int32(0))),
			spreadVMVolume("other", scsi, ptr.To(int32(0))),
			spreadVMVolume("sib-a", scsi, ptr.To(int32(1))),
			spreadVMVolume("sib-b", scsi, ptr.To(int32(3))),
			// Not on a SCSI controller: takes no SCSI slot.
			spreadVMVolume("nvme", vmoperatorv1alpha5.VirtualControllerTypeNVME, ptr.To(int32(1))),
			// Out of range bus numbers are ignored.
			spreadVMVolume("odd", scsi, ptr.To(int32(9))),
		}
		state, err := spreadStateFromVM(vm, map[string]struct{}{"sib-a": {}, "sib-b": {}, "not-attached": {}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := busState([3]int{2, 64, 0}, [3]int{1, 64, 1}, [3]int{0, 64, 0}, [3]int{1, 64, 1})
		if state != want {
			t.Errorf("state = %+v, want %+v", state, want)
		}
	})

	t.Run("controller types and sharing modes set the capacity", func(t *testing.T) {
		vm := &vmoperatorv1alpha5.VirtualMachine{}
		vm.Spec.Hardware = &vmoperatorv1alpha5.VirtualMachineHardwareSpec{
			SCSIControllers: []vmoperatorv1alpha5.SCSIControllerSpec{
				{BusNumber: 0, Type: vmoperatorv1alpha5.SCSIControllerTypeParaVirtualSCSI,
					SharingMode: vmoperatorv1alpha5.VirtualControllerSharingModeNone},
				// Type left empty defaults to ParaVirtual.
				{BusNumber: 1},
				{BusNumber: 2, Type: vmoperatorv1alpha5.SCSIControllerTypeParaVirtualSCSI,
					SharingMode: vmoperatorv1alpha5.VirtualControllerSharingModePhysical},
				{BusNumber: 3, Type: vmoperatorv1alpha5.SCSIControllerTypeLsiLogic},
				{BusNumber: 7, Type: vmoperatorv1alpha5.SCSIControllerTypeParaVirtualSCSI},
			},
		}
		state, err := spreadStateFromVM(vm, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for bus, want := range []int{64, 64, 0, 15} {
			if state[bus].capacity != want {
				t.Errorf("bus %d capacity = %d, want %d", bus, state[bus].capacity, want)
			}
		}
	})

	t.Run("a volume without a bus means the layout is unknown", func(t *testing.T) {
		vm := &vmoperatorv1alpha5.VirtualMachine{}
		vm.Spec.Volumes = []vmoperatorv1alpha5.VirtualMachineVolume{
			spreadVMVolume("placed", scsi, ptr.To(int32(0))),
			spreadVMVolume("unplaced", "", nil),
		}
		_, err := spreadStateFromVM(vm, nil)
		var notPlaced errVMVolumeNotPlaced
		if !errors.As(err, &notPlaced) || notPlaced.volume != "unplaced" {
			t.Fatalf("error = %v, want errVMVolumeNotPlaced for volume \"unplaced\"", err)
		}
		if !strings.Contains(err.Error(), "unplaced") {
			t.Errorf("error %q does not name the volume", err)
		}
	})
}

func TestPodClaimNamesAndOptIn(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "db-0"},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{Name: "data", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-db-0"}}},
			{Name: "data-again", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-db-0"}}},
			{Name: "scratch", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}},
			{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			{Name: "empty-claim", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{}}},
		}},
	}
	got := podClaimNames(pod)
	want := []string{"data-db-0", "db-0-scratch"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("podClaimNames = %v, want %v", got, want)
	}

	if isPodVolumeSpreadRequested(pod) {
		t.Error("a pod without the label must not be opted in")
	}
	pod.Labels = map[string]string{common.PodVolumePlacementLabel: "something-else"}
	if isPodVolumeSpreadRequested(pod) {
		t.Error("a pod with another label value must not be opted in")
	}
	pod.Labels[common.PodVolumePlacementLabel] = common.PodVolumePlacementSpread
	if !isPodVolumeSpreadRequested(pod) {
		t.Error("a pod with the spread label must be opted in")
	}
}

// spreadFixture builds guest cluster objects for a pod and its claims.
type spreadFixture struct {
	objects []runtime.Object
}

func spreadHandle(claim string) string { return spreadTestHandlePrefx + claim }

func (f *spreadFixture) storageClass(name, provisioner string, params map[string]string) {
	f.objects = append(f.objects, &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: name}, Provisioner: provisioner, Parameters: params,
	})
}

// boundClaim adds a claim bound to a PV of the given driver, and the
// VolumeAttachment for it on the test node.
func (f *spreadFixture) boundClaim(namespace, name, driver, handle string, modes ...corev1.PersistentVolumeAccessMode) {
	if len(modes) == 0 {
		modes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	pvName := "pv-" + name
	f.objects = append(f.objects,
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: modes, VolumeName: pvName, StorageClassName: ptr.To(spreadTestSC),
			},
		},
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{Namespace: namespace, Name: name},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: driver, VolumeHandle: handle},
				},
			},
		},
		&storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: volumeAttachmentName(handle, csitypes.Name, spreadTestNode)},
			Spec: storagev1.VolumeAttachmentSpec{
				NodeName: spreadTestNode,
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To(pvName)},
			},
		})
}

func (f *spreadFixture) unboundClaim(namespace, name string, storageClass *string) {
	f.objects = append(f.objects, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, StorageClassName: storageClass,
		},
	})
}

func (f *spreadFixture) pod(name, node string, optIn bool, claims ...string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: spreadTestPodNS, Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
	}
	if optIn {
		pod.Labels = map[string]string{common.PodVolumePlacementLabel: common.PodVolumePlacementSpread}
	}
	for _, claim := range claims {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "vol-" + claim,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
			},
		})
	}
	f.objects = append(f.objects, pod)
	return pod
}

func (f *spreadFixture) controller() *controller {
	return &controller{
		guestClient:         testclient.NewClientset(f.objects...),
		supervisorNamespace: spreadTestNamespace,
	}
}

func spreadPublishRequest(claim string) *csi.ControllerPublishVolumeRequest {
	return &csi.ControllerPublishVolumeRequest{VolumeId: spreadHandle(claim), NodeId: spreadTestNode}
}

// blockPodFixture is an opted-in pod with n bound vSphere block claims.
func blockPodFixture(n int) (*spreadFixture, []string) {
	f := &spreadFixture{}
	f.storageClass(spreadTestSC, csitypes.Name, nil)
	claims := make([]string, 0, n)
	for i := 0; i < n; i++ {
		claim := fmt.Sprintf("data-%d", i)
		claims = append(claims, claim)
		f.boundClaim(spreadTestPodNS, claim, csitypes.Name, spreadHandle(claim))
	}
	f.pod(spreadTestPod, spreadTestNode, true, claims...)
	return f, claims
}

func TestResolvePodVolumeSpread(t *testing.T) {
	ctx := context.Background()

	t.Run("FSS off: default placement without any lookup", func(t *testing.T) {
		setPodVolumeSpreadState(t, false, false)
		f, _ := blockPodFixture(4)
		c := f.controller()
		fake := c.guestClient.(*testclient.Clientset)
		spread, _, err := c.resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", spread, err)
		}
		if len(fake.Actions()) != 0 {
			t.Errorf("expected no API calls when the FSS is off, got %v", fake.Actions())
		}
	})

	t.Run("opted-in pod with block volumes", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f, claims := blockPodFixture(4)
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spread == nil || spread.count != 4 || spread.podKey != spreadTestPodNS+"/"+spreadTestPod {
			t.Fatalf("spread = %+v, want count 4 for the test pod", spread)
		}
		for _, claim := range claims {
			if _, ok := spread.siblings[spreadHandle(claim)]; !ok {
				t.Errorf("sibling %q missing from %v", spreadHandle(claim), spread.siblings)
			}
		}
	})

	t.Run("only block volumes of this driver are counted", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.storageClass(spreadTestSC, csitypes.Name, nil)
		f.storageClass("other-sc", "other.csi.example.com", nil)
		f.storageClass("nfs-sc", csitypes.Name, map[string]string{"server": "10.0.0.1"})
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.boundClaim(spreadTestPodNS, "data-1", csitypes.Name, spreadHandle("data-1"), corev1.ReadWriteOncePod)
		// Skipped: a file volume, a read-only-many volume, another driver's
		// volume and a guest-local NFS volume.
		f.boundClaim(spreadTestPodNS, "shared", csitypes.Name, spreadHandle("shared"), corev1.ReadWriteMany)
		f.boundClaim(spreadTestPodNS, "ro", csitypes.Name, spreadHandle("ro"), corev1.ReadOnlyMany)
		f.boundClaim(spreadTestPodNS, "foreign", "other.csi.example.com", "foreign-handle")
		f.boundClaim(spreadTestPodNS, "guestnfs", csitypes.Name, common.GuestNFSVolumeIDPrefix+"vol")
		// Not bound yet (WaitForFirstConsumer): judged by StorageClass.
		f.unboundClaim(spreadTestPodNS, "pending", ptr.To(spreadTestSC))
		f.unboundClaim(spreadTestPodNS, "pending-other", ptr.To("other-sc"))
		f.unboundClaim(spreadTestPodNS, "pending-nfs", ptr.To("nfs-sc"))
		f.unboundClaim(spreadTestPodNS, "pending-no-sc", nil)
		f.unboundClaim(spreadTestPodNS, "pending-empty-sc", ptr.To(""))
		// The claim of a generic ephemeral volume is named <pod>-<volume>.
		f.boundClaim(spreadTestPodNS, spreadTestPod+"-scratch", csitypes.Name, spreadHandle("scratch"))
		pod := f.pod(spreadTestPod, spreadTestNode, true, "data-0", "data-1", "shared", "ro", "foreign",
			"guestnfs", "pending", "pending-other", "pending-nfs", "pending-no-sc", "pending-empty-sc")
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			corev1.Volume{Name: "scratch", VolumeSource: corev1.VolumeSource{
				Ephemeral: &corev1.EphemeralVolumeSource{}}},
			corev1.Volume{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})

		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// data-0, data-1, pending and the ephemeral claim.
		if spread.count != 4 {
			t.Errorf("count = %d, want 4", spread.count)
		}
		wantSiblings := []string{spreadHandle("data-0"), spreadHandle("data-1"), spreadHandle("scratch")}
		if len(spread.siblings) != len(wantSiblings) {
			t.Errorf("siblings = %v, want %v", spread.siblings, wantSiblings)
		}
		for _, handle := range wantSiblings {
			if _, ok := spread.siblings[handle]; !ok {
				t.Errorf("sibling %q missing from %v", handle, spread.siblings)
			}
		}
	})

	t.Run("pod without the label: default placement", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.pod(spreadTestPod, spreadTestNode, false, "data-0")
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", spread, err)
		}
	})

	t.Run("pods that do not count are ignored", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.boundClaim(spreadTestPodNS, "data-1", csitypes.Name, spreadHandle("data-1"))
		// The pod the attach is for: not opted in.
		f.pod("plain", spreadTestNode, false, "data-0")
		// Opted-in pods that must not be picked: on another node, finished,
		// or using a different claim.
		f.pod("elsewhere", "other-node", true, "data-0")
		f.pod("done", spreadTestNode, true, "data-0").Status.Phase = corev1.PodSucceeded
		f.pod("failed", spreadTestNode, true, "data-0").Status.Phase = corev1.PodFailed
		f.pod("unrelated", spreadTestNode, true, "data-1")
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", spread, err)
		}
	})

	t.Run("opted-in pod wins over a plain pod sharing the claim", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.pod("plain", spreadTestNode, false, "data-0")
		f.pod(spreadTestPod, spreadTestNode, true, "data-0")
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread == nil || spread.count != 1 {
			t.Fatalf("got (%+v, %v), want a spread of 1 volume", spread, err)
		}
	})

	t.Run("claim shared by two opted-in pods fails", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.pod("web-a", spreadTestNode, true, "data-0")
		f.pod("web-b", spreadTestNode, true, "data-0")
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.FailedPrecondition)
		assertSpreadNotSatisfiable(t, err)
	})

	t.Run("opted-in pod without the supervisor capability fails", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, false)
		f, _ := blockPodFixture(2)
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.FailedPrecondition)
		assertSpreadNotSatisfiable(t, err)
		if !strings.Contains(err.Error(), common.SharedDiskFss) {
			t.Errorf("error %q does not name the missing capability", err)
		}
	})

	t.Run("plain pod is unaffected by a missing capability", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, false)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.pod(spreadTestPod, spreadTestNode, false, "data-0")
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", spread, err)
		}
	})

	t.Run("no pod uses the claim yet: retry, never default", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.Unavailable)
	})

	t.Run("VolumeAttachment gone: attach no longer requested", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.FailedPrecondition)
	})

	t.Run("VolumeAttachment without a PersistentVolume: default placement", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.objects = append(f.objects, &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name: volumeAttachmentName(spreadHandle("data-0"), csitypes.Name, spreadTestNode)},
		})
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", spread, err)
		}
	})

	t.Run("PersistentVolume without a claim: default placement", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		for _, obj := range f.objects {
			if pv, ok := obj.(*corev1.PersistentVolume); ok {
				pv.Spec.ClaimRef = nil
			}
		}
		spread, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		if err != nil || spread != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", spread, err)
		}
	})

	t.Run("PersistentVolume missing: retry", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.objects = append(f.objects, &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name: volumeAttachmentName(spreadHandle("data-0"), csitypes.Name, spreadTestNode)},
			Spec: storagev1.VolumeAttachmentSpec{
				Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: ptr.To("pv-missing")}},
		})
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.Unavailable)
	})

	t.Run("a sibling claim that is missing: retry", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.pod(spreadTestPod, spreadTestNode, true, "data-0", "not-created-yet")
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.Unavailable)
	})

	t.Run("a sibling claim whose PersistentVolume is missing: retry", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.objects = append(f.objects, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: spreadTestPodNS, Name: "data-1"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, VolumeName: "pv-gone",
			},
		})
		f.pod(spreadTestPod, spreadTestNode, true, "data-0", "data-1")
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.Unavailable)
	})

	t.Run("published volume is not one of the pod's block volumes: retry", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		// The claim is ReadWriteMany, so it is not counted, yet a block attach
		// was requested for it.
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"), corev1.ReadWriteMany)
		f.pod(spreadTestPod, spreadTestNode, true, "data-0")
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.Unavailable)
	})

	apiFailures := []struct {
		name, verb, resource string
	}{
		{"VolumeAttachment lookup fails", "get", "volumeattachments"},
		{"PersistentVolume lookup fails", "get", "persistentvolumes"},
		{"pod list fails", "list", "pods"},
		{"claim lookup fails", "get", "persistentvolumeclaims"},
	}
	for _, tc := range apiFailures {
		t.Run(tc.name+": retry", func(t *testing.T) {
			setPodVolumeSpreadState(t, true, true)
			f, _ := blockPodFixture(2)
			c := f.controller()
			c.guestClient.(*testclient.Clientset).PrependReactor(tc.verb, tc.resource,
				func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("api server unavailable")
				})
			_, _, err := c.resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
			assertGRPCCode(t, err, codes.Unavailable)
		})
	}

	t.Run("StorageClass lookup fails for an unbound claim: retry", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.unboundClaim(spreadTestPodNS, "pending", ptr.To("missing-sc"))
		f.pod(spreadTestPod, spreadTestNode, true, "data-0", "pending")
		_, _, err := f.controller().resolvePodVolumeSpread(ctx, spreadPublishRequest("data-0"))
		assertGRPCCode(t, err, codes.Unavailable)
	})
}

func assertSpreadNotSatisfiable(t *testing.T, err error) {
	t.Helper()
	st, _ := status.FromError(err)
	if !strings.HasPrefix(st.Message(), volumeSpreadNotSatisfiablePrefix) {
		t.Errorf("message %q does not start with %q", st.Message(), volumeSpreadNotSatisfiablePrefix)
	}
}

func spreadVMScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := vmoperatorv1alpha5.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator v1alpha5 scheme: %v", err)
	}
	if err := vmoperatortypes.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to register vmoperator v1alpha2 scheme: %v", err)
	}
	return scheme
}

// assignUnitsOnPatch stands in for vm-operator's mutating webhook: it gives
// every volume in a patched VirtualMachine a unit number, as an upgraded VM
// gets. patches counts the VirtualMachine patches seen.
func assignUnitsOnPatch(patches *int) interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch,
			opts ...client.PatchOption) error {
			if vm, ok := obj.(*vmoperatorv1alpha5.VirtualMachine); ok {
				if patches != nil {
					*patches++
				}
				for i := range vm.Spec.Volumes {
					if vm.Spec.Volumes[i].UnitNumber == nil {
						vm.Spec.Volumes[i].UnitNumber = ptr.To(int32(i))
					}
				}
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

func newSpreadVM(volumes ...vmoperatorv1alpha5.VirtualMachineVolume) *vmoperatorv1alpha5.VirtualMachine {
	return &vmoperatorv1alpha5.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: spreadTestNode, Namespace: spreadTestNamespace},
		Spec:       vmoperatorv1alpha5.VirtualMachineSpec{Volumes: volumes},
	}
}

func getSpreadVM(t *testing.T, c client.Client) *vmoperatorv1alpha5.VirtualMachine {
	t.Helper()
	vm := &vmoperatorv1alpha5.VirtualMachine{}
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: spreadTestNamespace, Name: spreadTestNode}, vm); err != nil {
		t.Fatalf("failed to read back VirtualMachine: %v", err)
	}
	return vm
}

func spreadVMKey() types.NamespacedName {
	return types.NamespacedName{Namespace: spreadTestNamespace, Name: spreadTestNode}
}

// shortenVMPatchBackoff keeps retry tests fast.
func shortenVMPatchBackoff(t *testing.T) {
	t.Helper()
	orig := vmPatchRetryBackoff
	vmPatchRetryBackoff.Duration = time.Millisecond
	vmPatchRetryBackoff.Cap = 2 * time.Millisecond
	origRemove := removeUnplacedVolumeTimeout
	removeUnplacedVolumeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { vmPatchRetryBackoff, removeUnplacedVolumeTimeout = orig, origRemove })
}

func TestAddSpreadVolumeToVMSpec(t *testing.T) {
	ctx := context.Background()
	scsi := vmoperatorv1alpha5.VirtualControllerTypeSCSI
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }

	// siblingsOf is the sibling set resolvePodVolumeSpread would produce.
	siblingsOf := func(claims []string) map[string]struct{} {
		out := map[string]struct{}{}
		for _, claim := range claims {
			out[spreadHandle(claim)] = struct{}{}
		}
		return out
	}

	t.Run("8 volumes end up 2/2/2/2, each with only type and bus set", func(t *testing.T) {
		f, claims := blockPodFixture(8)
		c := f.controller()
		patches := 0
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM(spreadVMVolume("boot", scsi, ptr.To(int32(0))))).
			WithInterceptorFuncs(assignUnitsOnPatch(&patches)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 8, siblings: siblingsOf(claims)}

		for _, claim := range claims {
			vm, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest(claim), spreadVMKey(), spread, deadline())
			if err != nil {
				t.Fatalf("claim %s: unexpected error: %v", claim, err)
			}
			if vm == nil || vm.Name != spreadTestNode {
				t.Fatalf("claim %s: returned VirtualMachine = %+v", claim, vm)
			}
		}
		if patches != 8 {
			t.Errorf("patches = %d, want 8", patches)
		}
		perBus := map[int32]int{}
		for _, volume := range getSpreadVM(t, c.vmOperatorClient).Spec.Volumes {
			if volume.Name == "boot" {
				continue
			}
			if volume.ControllerType != scsi || volume.ControllerBusNumber == nil {
				t.Fatalf("volume %q has no SCSI bus: %+v", volume.Name, volume)
			}
			if volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != volume.Name {
				t.Errorf("volume %q has the wrong claim source: %+v", volume.Name, volume.PersistentVolumeClaim)
			}
			perBus[*volume.ControllerBusNumber]++
		}
		for bus := int32(0); bus < spreadSCSIBusCount; bus++ {
			if perBus[bus] != 2 {
				t.Errorf("bus %d holds %d of the pod's volumes, want 2 (layout %v)", bus, perBus[bus], perBus)
			}
		}
	})

	t.Run("the patch pvCSI sends carries no unit number", func(t *testing.T) {
		f, claims := blockPodFixture(1)
		c := f.controller()
		var sent *vmoperatorv1alpha5.VirtualMachineVolume
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM()).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch,
					opts ...client.PatchOption) error {
					vm := obj.(*vmoperatorv1alpha5.VirtualMachine)
					sent = vm.Spec.Volumes[len(vm.Spec.Volumes)-1].DeepCopy()
					return assignUnitsOnPatch(nil).Patch(ctx, cl, obj, patch, opts...)
				},
			}).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 1, siblings: siblingsOf(claims)}
		if _, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(),
			spread, deadline()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sent == nil || sent.UnitNumber != nil {
			t.Errorf("pvCSI must leave the unit number to vm-operator, sent %+v", sent)
		}
	})

	t.Run("volume already in the spec is left alone", func(t *testing.T) {
		f, claims := blockPodFixture(2)
		c := f.controller()
		patches := 0
		existing := newSpreadVM(spreadVMVolume(spreadHandle("data-0"), scsi, ptr.To(int32(1))))
		existing.Status.Volumes = []vmoperatorv1alpha5.VirtualMachineVolumeStatus{
			{Name: spreadHandle("data-0"), Attached: true, DiskUUID: "6000c29-disk"},
		}
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(existing).WithStatusSubresource(existing).
			WithInterceptorFuncs(assignUnitsOnPatch(&patches)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 2, siblings: siblingsOf(claims)}
		vm, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if patches != 0 {
			t.Errorf("patches = %d, want 0", patches)
		}
		if len(vm.Status.Volumes) != 1 || vm.Status.Volumes[0].DiskUUID != "6000c29-disk" ||
			!vm.Status.Volumes[0].Attached {
			t.Errorf("attach status was not carried over: %+v", vm.Status.Volumes)
		}
	})

	t.Run("layout that does not fit fails without patching", func(t *testing.T) {
		f, claims := blockPodFixture(8)
		c := f.controller()
		patches := 0
		vm := newSpreadVM()
		// Bus 3 is full.
		for i := 0; i < 64; i++ {
			vm.Spec.Volumes = append(vm.Spec.Volumes, spreadVMVolume(fmt.Sprintf("other-%d", i), scsi, ptr.To(int32(3))))
		}
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(vm).WithInterceptorFuncs(assignUnitsOnPatch(&patches)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 8, siblings: siblingsOf(claims)}

		// Every sibling fails the same way, so nothing attaches on this node.
		for _, claim := range claims {
			_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest(claim), spreadVMKey(), spread, deadline())
			assertGRPCCode(t, err, codes.ResourceExhausted)
			assertSpreadNotSatisfiable(t, err)
			if !strings.Contains(err.Error(), "pod needs 2/2/2/2 across SCSI 0-3; bus 3 has 0 free slots") {
				t.Errorf("error %q does not describe the layout and the short bus", err)
			}
		}
		if patches != 0 {
			t.Errorf("patches = %d, want 0", patches)
		}
		if got := len(getSpreadVM(t, c.vmOperatorClient).Spec.Volumes); got != 64 {
			t.Errorf("VM spec has %d volumes, want it untouched at 64", got)
		}
	})

	t.Run("shared controller is treated as full", func(t *testing.T) {
		f, claims := blockPodFixture(4)
		c := f.controller()
		vm := newSpreadVM()
		vm.Spec.Hardware = &vmoperatorv1alpha5.VirtualMachineHardwareSpec{
			SCSIControllers: []vmoperatorv1alpha5.SCSIControllerSpec{{
				BusNumber: 2, Type: vmoperatorv1alpha5.SCSIControllerTypeParaVirtualSCSI,
				SharingMode: vmoperatorv1alpha5.VirtualControllerSharingModeVirtual,
			}},
		}
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(vm).WithInterceptorFuncs(assignUnitsOnPatch(nil)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 4, siblings: siblingsOf(claims)}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		assertGRPCCode(t, err, codes.ResourceExhausted)
		if !strings.Contains(err.Error(), "bus 2 has 0 free slots") {
			t.Errorf("error %q does not name the shared bus", err)
		}
	})

	t.Run("existing volume without a bus: retry later", func(t *testing.T) {
		f, claims := blockPodFixture(2)
		c := f.controller()
		patches := 0
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM(spreadVMVolume("legacy", "", nil))).
			WithInterceptorFuncs(assignUnitsOnPatch(&patches)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 2, siblings: siblingsOf(claims)}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		assertGRPCCode(t, err, codes.Unavailable)
		if patches != 0 {
			t.Errorf("patches = %d, want 0", patches)
		}
	})

	t.Run("vm-operator assigns no unit: entry is removed and the attach retried", func(t *testing.T) {
		shortenVMPatchBackoff(t)
		f, claims := blockPodFixture(2)
		c := f.controller()
		// No interceptor: the volume is stored exactly as written, which is
		// what vm-operator does for a VM it has not upgraded yet.
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM()).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 2, siblings: siblingsOf(claims)}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		assertGRPCCode(t, err, codes.Unavailable)
		if got := getSpreadVM(t, c.vmOperatorClient).Spec.Volumes; len(got) != 0 {
			t.Errorf("the volume without a unit must be removed again, spec has %+v", got)
		}
	})

	t.Run("no unit and the removal fails: Internal", func(t *testing.T) {
		shortenVMPatchBackoff(t)
		f, claims := blockPodFixture(2)
		c := f.controller()
		patches := 0
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM()).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch,
					opts ...client.PatchOption) error {
					patches++
					if patches == 1 {
						return cl.Patch(ctx, obj, patch, opts...)
					}
					return errors.New("supervisor unreachable")
				},
			}).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 2, siblings: siblingsOf(claims)}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		assertGRPCCode(t, err, codes.Internal)
		if patches < 2 {
			t.Errorf("expected the removal to be attempted, saw %d patch(es)", patches)
		}
	})

	t.Run("conflict is retried against the new state", func(t *testing.T) {
		shortenVMPatchBackoff(t)
		f, claims := blockPodFixture(4)
		c := f.controller()
		patches := 0
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM()).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch,
					opts ...client.PatchOption) error {
					patches++
					if patches == 1 {
						// A sibling's attach lands first, on bus 0, and ours
						// loses the race.
						other := getSpreadVMFrom(ctx, cl)
						other.Spec.Volumes = append(other.Spec.Volumes,
							spreadVMVolume(spreadHandle("data-1"), scsi, ptr.To(int32(0))))
						if err := cl.Update(ctx, other); err != nil {
							return err
						}
						return apierrors.NewConflict(
							schema.GroupResource{Group: "vmoperator.vmware.com", Resource: "virtualmachines"},
							spreadTestNode, errors.New("the object has been modified"))
					}
					return assignUnitsOnPatch(nil).Patch(ctx, cl, obj, patch, opts...)
				},
			}).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 4, siblings: siblingsOf(claims)}
		if _, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(),
			spread, deadline()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if patches != 2 {
			t.Errorf("patches = %d, want 2", patches)
		}
		for _, volume := range getSpreadVM(t, c.vmOperatorClient).Spec.Volumes {
			if volume.Name == spreadHandle("data-0") && *volume.ControllerBusNumber == 0 {
				t.Errorf("after the retry the volume must avoid bus 0, which the sibling took")
			}
		}
	})

	t.Run("patch keeps failing until the deadline", func(t *testing.T) {
		shortenVMPatchBackoff(t)
		f, claims := blockPodFixture(1)
		c := f.controller()
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM()).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch,
					...client.PatchOption) error {
					return errors.New("supervisor unreachable")
				},
			}).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 1, siblings: siblingsOf(claims)}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread,
			time.Now().Add(20*time.Millisecond))
		assertGRPCCode(t, err, codes.DeadlineExceeded)
	})

	rejections := map[string]error{
		"Invalid": apierrors.NewInvalid(schema.GroupKind{Kind: "VirtualMachine"}, spreadTestNode, nil),
		"Forbidden": apierrors.NewForbidden(schema.GroupResource{Resource: "virtualmachines"}, spreadTestNode,
			errors.New("admission webhook denied the request: controller is full")),
		"BadRequest": apierrors.NewBadRequest("admission webhook denied the request"),
	}
	for name, rejection := range rejections {
		t.Run("vm-operator rejects the placement ("+name+"): no other bus is tried", func(t *testing.T) {
			f, claims := blockPodFixture(4)
			c := f.controller()
			patches := 0
			c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
				WithObjects(newSpreadVM()).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(context.Context, client.WithWatch, client.Object, client.Patch,
						...client.PatchOption) error {
						patches++
						return rejection
					},
				}).Build()
			spread := &podVolumeSpread{podKey: "app/db-0", count: 4, siblings: siblingsOf(claims)}
			_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
			assertGRPCCode(t, err, codes.ResourceExhausted)
			assertSpreadNotSatisfiable(t, err)
			if patches != 1 {
				t.Errorf("patches = %d, want exactly 1", patches)
			}
		})
	}

	t.Run("stale attach is not written", func(t *testing.T) {
		f := &spreadFixture{} // no VolumeAttachment
		c := f.controller()
		patches := 0
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM()).WithInterceptorFuncs(assignUnitsOnPatch(&patches)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 1,
			siblings: map[string]struct{}{spreadHandle("data-0"): {}}}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		assertGRPCCode(t, err, codes.FailedPrecondition)
		if patches != 0 {
			t.Errorf("patches = %d, want 0", patches)
		}
	})

	t.Run("VirtualMachine not found", func(t *testing.T) {
		f, claims := blockPodFixture(1)
		c := f.controller()
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).Build()
		spread := &podVolumeSpread{podKey: "app/db-0", count: 1, siblings: siblingsOf(claims)}
		_, _, err := c.addSpreadVolumeToVMSpec(ctx, spreadPublishRequest("data-0"), spreadVMKey(), spread, deadline())
		assertGRPCCode(t, err, codes.Internal)
	})
}

func getSpreadVMFrom(ctx context.Context, c client.Client) *vmoperatorv1alpha5.VirtualMachine {
	vm := &vmoperatorv1alpha5.VirtualMachine{}
	if err := c.Get(ctx, spreadVMKey(), vm); err != nil {
		panic(err)
	}
	return vm
}

func TestRemoveSpreadVolumeFromVMSpec(t *testing.T) {
	ctx := context.Background()
	scsi := vmoperatorv1alpha5.VirtualControllerTypeSCSI
	deadline := func() time.Time { return time.Now().Add(time.Second) }

	t.Run("removes only the named volume", func(t *testing.T) {
		c := &controller{vmOperatorClient: ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM(
				spreadVMVolume("keep-a", scsi, ptr.To(int32(0))),
				spreadVMVolume("drop", scsi, ptr.To(int32(1))),
				spreadVMVolume("keep-b", scsi, ptr.To(int32(2))))).Build()}
		if err := c.removeSpreadVolumeFromVMSpec(ctx, spreadVMKey(), "drop", deadline()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		volumes := getSpreadVM(t, c.vmOperatorClient).Spec.Volumes
		if len(volumes) != 2 || volumes[0].Name != "keep-a" || volumes[1].Name != "keep-b" {
			t.Errorf("volumes = %+v, want keep-a and keep-b", volumes)
		}
	})

	t.Run("volume not in the spec is a no-op", func(t *testing.T) {
		patches := 0
		c := &controller{vmOperatorClient: ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM(spreadVMVolume("keep", scsi, ptr.To(int32(0))))).
			WithInterceptorFuncs(assignUnitsOnPatch(&patches)).Build()}
		if err := c.removeSpreadVolumeFromVMSpec(ctx, spreadVMKey(), "absent", deadline()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if patches != 0 {
			t.Errorf("patches = %d, want 0", patches)
		}
	})

	t.Run("VirtualMachine gone is a no-op", func(t *testing.T) {
		c := &controller{vmOperatorClient: ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).Build()}
		if err := c.removeSpreadVolumeFromVMSpec(ctx, spreadVMKey(), "any", deadline()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("get error is returned", func(t *testing.T) {
		c := &controller{vmOperatorClient: ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object,
					...client.GetOption) error {
					return errors.New("supervisor unreachable")
				},
			}).Build()}
		if err := c.removeSpreadVolumeFromVMSpec(ctx, spreadVMKey(), "any", deadline()); err == nil {
			t.Fatal("expected the get error to be returned")
		}
	})

	t.Run("retries a failed patch", func(t *testing.T) {
		shortenVMPatchBackoff(t)
		patches := 0
		c := &controller{vmOperatorClient: ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(newSpreadVM(spreadVMVolume("drop", scsi, ptr.To(int32(1))))).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch,
					opts ...client.PatchOption) error {
					patches++
					if patches == 1 {
						return errors.New("temporary failure")
					}
					return cl.Patch(ctx, obj, patch, opts...)
				},
			}).Build()}
		if err := c.removeSpreadVolumeFromVMSpec(ctx, spreadVMKey(), "drop", deadline()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if patches != 2 || len(getSpreadVM(t, c.vmOperatorClient).Spec.Volumes) != 0 {
			t.Errorf("patches = %d, want 2 and an empty spec", patches)
		}
	})
}

func TestSpreadVMForAttachWait(t *testing.T) {
	vm := newSpreadVM()
	vm.Status.Volumes = []vmoperatorv1alpha5.VirtualMachineVolumeStatus{
		{Name: "a", Attached: true, DiskUUID: "uuid-a"},
		{Name: "b", Error: "attach failed"},
	}
	out := spreadVMForAttachWait(vm)
	if out.Name != spreadTestNode || out.Namespace != spreadTestNamespace {
		t.Errorf("name/namespace = %s/%s", out.Namespace, out.Name)
	}
	if len(out.Status.Volumes) != 2 {
		t.Fatalf("status volumes = %+v", out.Status.Volumes)
	}
	if a := out.Status.Volumes[0]; a.Name != "a" || !a.Attached || a.DiskUUID != "uuid-a" {
		t.Errorf("volume a = %+v", a)
	}
	if b := out.Status.Volumes[1]; b.Name != "b" || b.Attached || b.Error != "attach failed" {
		t.Errorf("volume b = %+v", b)
	}
}

// TestControllerPublishForBlockVolumeSpread drives the whole block publish path.
func TestControllerPublishForBlockVolumeSpread(t *testing.T) {
	ctx := context.Background()

	// attachedStatus makes the call return without the VirtualMachine watch.
	attachedStatus := func(claims []string) []vmoperatorv1alpha5.VirtualMachineVolumeStatus {
		var out []vmoperatorv1alpha5.VirtualMachineVolumeStatus
		for _, claim := range claims {
			out = append(out, vmoperatorv1alpha5.VirtualMachineVolumeStatus{
				Name: spreadHandle(claim), Attached: true, DiskUUID: "uuid-" + claim,
			})
		}
		return out
	}

	t.Run("FSS on: opted-in pod's volumes are placed on separate buses", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f, claims := blockPodFixture(4)
		c := f.controller()
		vm := newSpreadVM()
		vm.Status.Volumes = attachedStatus(claims)
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(vm).WithStatusSubresource(vm).WithInterceptorFuncs(assignUnitsOnPatch(nil)).Build()

		buses := map[int32]bool{}
		for _, claim := range claims {
			resp, _, err := controllerPublishForBlockVolume(ctx, spreadPublishRequest(claim), c)
			if err != nil {
				t.Fatalf("claim %s: unexpected error: %v", claim, err)
			}
			if resp == nil {
				t.Fatalf("claim %s: expected a ControllerPublishVolumeResponse", claim)
			}
		}
		for _, volume := range getSpreadVM(t, c.vmOperatorClient).Spec.Volumes {
			if volume.ControllerBusNumber == nil {
				t.Fatalf("volume %q was added without a bus", volume.Name)
			}
			buses[*volume.ControllerBusNumber] = true
		}
		if len(buses) != 4 {
			t.Errorf("4 volumes landed on %d bus(es), want 4", len(buses))
		}
	})

	t.Run("FSS on: layout that does not fit fails the publish", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f, _ := blockPodFixture(4)
		c := f.controller()
		vm := newSpreadVM()
		for i := 0; i < 64; i++ {
			vm.Spec.Volumes = append(vm.Spec.Volumes, spreadVMVolume(fmt.Sprintf("other-%d", i),
				vmoperatorv1alpha5.VirtualControllerTypeSCSI, ptr.To(int32(1))))
		}
		c.vmOperatorClient = ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
			WithObjects(vm).WithInterceptorFuncs(assignUnitsOnPatch(nil)).Build()
		_, _, err := controllerPublishForBlockVolume(ctx, spreadPublishRequest("data-0"), c)
		assertGRPCCode(t, err, codes.ResourceExhausted)
		assertSpreadNotSatisfiable(t, err)
	})

	t.Run("FSS on: pod without the label takes the default path", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f := &spreadFixture{}
		f.boundClaim(spreadTestPodNS, "data-0", csitypes.Name, spreadHandle("data-0"))
		f.pod(spreadTestPod, spreadTestNode, false, "data-0")
		c := f.controller()
		c.vmOperatorClient = defaultPathVMClient(t, spreadHandle("data-0"))
		if _, _, err := controllerPublishForBlockVolume(ctx, spreadPublishRequest("data-0"), c); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		assertDefaultPlacement(t, c.vmOperatorClient, spreadHandle("data-0"))
	})

	t.Run("FSS off: the label is ignored and the default path is used", func(t *testing.T) {
		setPodVolumeSpreadState(t, false, false)
		f, _ := blockPodFixture(1)
		c := f.controller()
		c.vmOperatorClient = defaultPathVMClient(t, spreadHandle("data-0"))
		if _, _, err := controllerPublishForBlockVolume(ctx, spreadPublishRequest("data-0"), c); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		assertDefaultPlacement(t, c.vmOperatorClient, spreadHandle("data-0"))
	})

	t.Run("FSS on: pod lookup failure fails the publish instead of defaulting", func(t *testing.T) {
		setPodVolumeSpreadState(t, true, true)
		f, _ := blockPodFixture(1)
		c := f.controller()
		c.guestClient.(*testclient.Clientset).PrependReactor("list", "pods",
			func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("api server unavailable")
			})
		c.vmOperatorClient = defaultPathVMClient(t, spreadHandle("data-0"))
		_, _, err := controllerPublishForBlockVolume(ctx, spreadPublishRequest("data-0"), c)
		assertGRPCCode(t, err, codes.Unavailable)
		vm := &vmoperatortypes.VirtualMachine{}
		if getErr := c.vmOperatorClient.Get(ctx, spreadVMKey(), vm); getErr != nil {
			t.Fatalf("failed to read back VirtualMachine: %v", getErr)
		}
		if len(vm.Spec.Volumes) != 0 {
			t.Errorf("nothing may be written when the pod is unknown, spec has %+v", vm.Spec.Volumes)
		}
	})
}

// defaultPathVMClient returns a client holding the node VM in the API version
// the default publish path uses, with the volume already reported attached.
func defaultPathVMClient(t *testing.T, volumeHandle string) client.Client {
	t.Helper()
	vm := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: spreadTestNode, Namespace: spreadTestNamespace},
	}
	vm.Status.Volumes = []vmoperatortypes.VirtualMachineVolumeStatus{
		{Name: volumeHandle, Attached: true, DiskUUID: "6000c29-fake-disk-uuid"},
	}
	return ctrlclientfake.NewClientBuilder().WithScheme(spreadVMScheme(t)).
		WithObjects(vm).WithStatusSubresource(vm).Build()
}

// assertDefaultPlacement checks the volume was added through the default path:
// to the v1alpha2 VirtualMachine, which has no placement fields at all.
func assertDefaultPlacement(t *testing.T, c client.Client, volumeHandle string) {
	t.Helper()
	vm := &vmoperatortypes.VirtualMachine{}
	if err := c.Get(context.Background(), spreadVMKey(), vm); err != nil {
		t.Fatalf("failed to read back VirtualMachine: %v", err)
	}
	if len(vm.Spec.Volumes) != 1 || vm.Spec.Volumes[0].Name != volumeHandle {
		t.Errorf("expected volume %q to be added through the default path, got %+v", volumeHandle, vm.Spec.Volumes)
	}
}
