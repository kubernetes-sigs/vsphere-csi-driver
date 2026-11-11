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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	vmoperatortypes "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoperatorv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	csitypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/types"
)

const (
	// spreadSCSIBusCount is the number of SCSI controllers a pod's volumes are
	// spread across: buses 0 to 3, the most a VM can have.
	spreadSCSIBusCount = 4

	// volumeSpreadNotSatisfiablePrefix starts every error returned because an
	// opted-in pod's volumes cannot be spread evenly on the node. Kubernetes
	// copies the error into a FailedAttachVolume event on the pod, so the prefix
	// is what DevOps and alerts match on.
	volumeSpreadNotSatisfiablePrefix = "VolumeSpreadNotSatisfiable:"
)

var (
	// isPodVolumeSpreadFSSEnabled reports whether the pod-volume-spread pvCSI
	// FSS is on. When it is off the feature does not exist: the placement label
	// is ignored and every volume takes the default path. Declared as a var so
	// it can be replaced in tests.
	isPodVolumeSpreadFSSEnabled = func(ctx context.Context) bool {
		return commonco.ContainerOrchestratorUtility != nil &&
			commonco.ContainerOrchestratorUtility.IsPVCSIFSSEnabled(ctx, common.PodVolumeSpreadFSS)
	}

	// isPodVolumeSpreadSupported reports whether the pvCSI FSS is on and the
	// supervisor has the capability paired with it, i.e. whether vm-operator
	// will honor a controller bus chosen here. Declared as a var so it can be
	// replaced in tests.
	isPodVolumeSpreadSupported = func(ctx context.Context) bool {
		return commonco.ContainerOrchestratorUtility != nil &&
			commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.PodVolumeSpreadFSS)
	}

	// removeUnplacedVolumeTimeout bounds how long we retry removing a volume
	// that vm-operator stored without assigning a unit number. Declared as a
	// var so it can be shortened in tests.
	removeUnplacedVolumeTimeout = 30 * time.Second
)

// podVolumeSpreadNeedsLateEnablement reports whether the late-enablement
// watcher should be started for the supervisor capability paired with the
// pod-volume-spread FSS: only when the pvCSI FSS is on but the capability is
// not there yet.
func podVolumeSpreadNeedsLateEnablement(ctx context.Context) bool {
	return isPodVolumeSpreadFSSEnabled(ctx) && !isPodVolumeSpreadSupported(ctx)
}

// spreadBusState describes one SCSI bus of the node VM as far as spreading is
// concerned.
type spreadBusState struct {
	// used is the number of devices the VM spec places on the bus.
	used int
	// capacity is the number of devices the bus can hold. It is zero for a
	// bus that must not be used, such as one whose controller is shared.
	capacity int
	// podVolumes is how many of the pod's own volumes are already on the bus.
	podVolumes int
}

// free returns the number of free slots on the bus.
func (b spreadBusState) free() int {
	return max(b.capacity-b.used, 0)
}

// spreadVMState is the state of the node VM's four SCSI buses, indexed by bus
// number.
type spreadVMState [spreadSCSIBusCount]spreadBusState

// spreadShares is how many of the pod's volumes each bus should hold, indexed
// by bus number.
type spreadShares [spreadSCSIBusCount]int

// String renders the shares largest first, e.g. "2/2/2/2" or "2/1/1/1". That is
// the shape of the layout; which bus gets which share depends on free slots.
func (s spreadShares) String() string {
	sorted := s
	sort.Sort(sort.Reverse(sort.IntSlice(sorted[:])))
	parts := make([]string, 0, spreadSCSIBusCount)
	for _, share := range sorted {
		parts = append(parts, strconv.Itoa(share))
	}
	return strings.Join(parts, "/")
}

// evenSpreadShares returns the even layout for n volumes without regard to any
// VM: each bus gets floor(n/4), and the remainder goes to the lowest buses.
func evenSpreadShares(n int) spreadShares {
	var shares spreadShares
	for bus := range shares {
		shares[bus] = n / spreadSCSIBusCount
		if bus < n%spreadSCSIBusCount {
			shares[bus]++
		}
	}
	return shares
}

// computeSpreadShares works out how many of the pod's n spread volumes each bus
// must end up with, and verifies the whole layout fits on the VM.
//
// Every bus gets floor(n/4) or ceil(n/4) volumes. Buses that already hold the
// larger share keep it; the remaining larger shares go to the buses that have
// the most room left after taking the smaller share, ties to the lowest bus.
// An error is returned, with a message suitable for the pod event, when the
// pod's volumes that are already attached are not an even layout, or when some
// bus does not have free slots for the rest of its share.
func computeSpreadShares(n int, state spreadVMState) (spreadShares, error) {
	var shares spreadShares
	if n <= 0 {
		return shares, fmt.Errorf("pod has no block volumes to spread")
	}
	base := n / spreadSCSIBusCount
	extra := n % spreadSCSIBusCount
	want := evenSpreadShares(n)

	placed := 0
	for bus, b := range state {
		placed += b.podVolumes
		limit := base
		if extra > 0 {
			limit = base + 1
		}
		if b.podVolumes > limit {
			return shares, fmt.Errorf("pod needs %s across SCSI 0-3; bus %d already holds %d of the pod's volumes",
				want, bus, b.podVolumes)
		}
	}
	if placed >= n {
		return shares, fmt.Errorf("pod needs %s across SCSI 0-3; %d of the pod's %d volumes are already placed "+
			"and no share is left for this volume", want, placed, n)
	}

	// Buses that already hold the larger share keep it.
	larger := make(map[int]bool, spreadSCSIBusCount)
	for bus, b := range state {
		if extra > 0 && b.podVolumes == base+1 {
			larger[bus] = true
		}
	}
	if len(larger) > extra {
		return shares, fmt.Errorf("pod needs %s across SCSI 0-3; %d buses already hold %d of the pod's volumes",
			want, len(larger), base+1)
	}

	// Hand the remaining larger shares to the buses with the most room left
	// once they have taken the smaller share.
	candidates := make([]int, 0, spreadSCSIBusCount)
	for bus := range state {
		if !larger[bus] {
			candidates = append(candidates, bus)
		}
	}
	slack := func(bus int) int {
		return state[bus].free() - (base - state[bus].podVolumes)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if slack(candidates[i]) != slack(candidates[j]) {
			return slack(candidates[i]) > slack(candidates[j])
		}
		return candidates[i] < candidates[j]
	})
	for _, bus := range candidates[:extra-len(larger)] {
		larger[bus] = true
	}

	for bus := range shares {
		shares[bus] = base
		if larger[bus] {
			shares[bus]++
		}
	}
	for bus, b := range state {
		if need := shares[bus] - b.podVolumes; need > b.free() {
			return shares, fmt.Errorf("pod needs %s across SCSI 0-3; bus %d has %d free slots, needs %d",
				shares, bus, b.free(), need)
		}
	}
	return shares, nil
}

// chooseSpreadBus picks the bus for the next of the pod's volumes: the bus whose
// share is furthest from filled, ties to the bus with the fewest devices, then
// to the lowest bus.
func chooseSpreadBus(shares spreadShares, state spreadVMState) (int32, error) {
	chosen := -1
	for bus, b := range state {
		need := shares[bus] - b.podVolumes
		if need <= 0 {
			continue
		}
		if chosen == -1 {
			chosen = bus
			continue
		}
		chosenNeed := shares[chosen] - state[chosen].podVolumes
		if need > chosenNeed || (need == chosenNeed && b.used < state[chosen].used) {
			chosen = bus
		}
	}
	if chosen == -1 {
		return 0, fmt.Errorf("pod needs %s across SCSI 0-3; every bus already holds its share", shares)
	}
	return int32(chosen), nil
}

// errVMVolumeNotPlaced reports a volume in the VM spec with no controller bus:
// the VM's device layout is not known, so nothing can be counted.
type errVMVolumeNotPlaced struct {
	volume string
}

func (e errVMVolumeNotPlaced) Error() string {
	return fmt.Sprintf("volume %q in the VirtualMachine spec has no controller bus number yet", e.volume)
}

// spreadStateFromVM reads the per-bus state from the VirtualMachine spec.
// siblings holds the VM volume names of the pod's own volumes.
//
// A bus that does not exist yet is counted as an empty ParaVirtual controller,
// because vm-operator creates it on demand. A bus whose controller is shared
// has no capacity: vm-operator does not stop a plain volume from landing on a
// shared controller, so it is excluded here.
func spreadStateFromVM(vm *vmoperatorv1alpha5.VirtualMachine, siblings map[string]struct{}) (spreadVMState, error) {
	var state spreadVMState
	paraVirtual := vmoperatorv1alpha5.SCSIControllerSpec{Type: vmoperatorv1alpha5.SCSIControllerTypeParaVirtualSCSI}
	for bus := range state {
		// MaxSlots counts the slot the controller itself occupies.
		state[bus].capacity = int(paraVirtual.MaxSlots()) - 1
	}
	if vm.Spec.Hardware != nil {
		for _, controller := range vm.Spec.Hardware.SCSIControllers {
			if controller.BusNumber < 0 || controller.BusNumber >= spreadSCSIBusCount {
				continue
			}
			if controller.Type == "" {
				controller.Type = vmoperatorv1alpha5.SCSIControllerTypeParaVirtualSCSI
			}
			capacity := int(controller.MaxSlots()) - 1
			if controller.SharingMode != "" && controller.SharingMode != vmoperatorv1alpha5.VirtualControllerSharingModeNone {
				capacity = 0
			}
			state[controller.BusNumber].capacity = max(capacity, 0)
		}
	}
	for _, volume := range vm.Spec.Volumes {
		if volume.ControllerBusNumber == nil {
			return state, errVMVolumeNotPlaced{volume: volume.Name}
		}
		if volume.ControllerType != vmoperatorv1alpha5.VirtualControllerTypeSCSI {
			continue
		}
		bus := *volume.ControllerBusNumber
		if bus < 0 || bus >= spreadSCSIBusCount {
			continue
		}
		state[bus].used++
		if _, ok := siblings[volume.Name]; ok {
			state[bus].podVolumes++
		}
	}
	return state, nil
}

// podVolumeSpread is what ControllerPublishVolume needs to know to place one
// volume of an opted-in pod.
type podVolumeSpread struct {
	// podKey is the namespace/name of the opted-in pod, for messages.
	podKey string
	// count is the number of block volumes of the pod that are spread (N).
	count int
	// siblings holds the VM volume names (volume handles) of the pod's block
	// volumes that are bound, including the one being published.
	siblings map[string]struct{}
}

// isPodVolumeSpreadRequested reports whether the pod opted in to spreading.
func isPodVolumeSpreadRequested(pod *corev1.Pod) bool {
	return pod.Labels[common.PodVolumePlacementLabel] == common.PodVolumePlacementSpread
}

// podClaimNames returns the names of the PersistentVolumeClaims the pod uses,
// including those created for its generic ephemeral volumes.
func podClaimNames(pod *corev1.Pod) []string {
	names := make([]string, 0, len(pod.Spec.Volumes))
	seen := make(map[string]struct{}, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		name := ""
		switch {
		case volume.PersistentVolumeClaim != nil:
			name = volume.PersistentVolumeClaim.ClaimName
		case volume.Ephemeral != nil:
			name = pod.Name + "-" + volume.Name
		}
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

// resolvePodVolumeSpread finds out whether the volume being published belongs
// to a pod that opted in to spreading, and if so collects what is needed to
// place it. It returns nil, with no error, when the volume takes the default
// placement: the feature is off, or no pod using the volume on the node opted
// in.
//
// ControllerPublishVolume is not told which pod the attach is for, so the pod
// is found from the VolumeAttachment, its PersistentVolume and that volume's
// claim. If the pod cannot be determined the call fails with Unavailable so the
// attach is retried; it never falls back to default placement, because that
// would silently give an opted-in pod the wrong layout.
func (c *controller) resolvePodVolumeSpread(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (
	*podVolumeSpread, string, error) {
	log := logger.GetLogger(ctx)
	if !isPodVolumeSpreadFSSEnabled(ctx) {
		return nil, "", nil
	}

	vaName := volumeAttachmentName(req.VolumeId, csitypes.Name, req.NodeId)
	va, err := c.guestClient.StorageV1().VolumeAttachments().Get(ctx, vaName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			msg := fmt.Sprintf("attach of volume %q to node %q is no longer requested: VolumeAttachment %q not found",
				req.VolumeId, req.NodeId, vaName)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition, msg)
		}
		return nil, csifault.CSIInternalFault, spreadLookupError(ctx,
			"failed to get VolumeAttachment %q for volume %q: %v", vaName, req.VolumeId, err)
	}
	if va.Spec.Source.PersistentVolumeName == nil {
		// Not attached on behalf of a PersistentVolume, so no pod reaches it
		// through a claim and there is nothing to spread.
		return nil, "", nil
	}
	pv, err := c.guestClient.CoreV1().PersistentVolumes().Get(ctx, *va.Spec.Source.PersistentVolumeName,
		metav1.GetOptions{})
	if err != nil {
		return nil, csifault.CSIInternalFault, spreadLookupError(ctx,
			"failed to get PersistentVolume %q for volume %q: %v",
			*va.Spec.Source.PersistentVolumeName, req.VolumeId, err)
	}
	if pv.Spec.ClaimRef == nil {
		return nil, "", nil
	}
	claimNamespace, claimName := pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name

	// The pod is bound to the node before its volumes are attached, so a live
	// list of the node's pods is authoritative.
	pods, err := c.guestClient.CoreV1().Pods(claimNamespace).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", req.NodeId).String(),
	})
	if err != nil {
		return nil, csifault.CSIInternalFault, spreadLookupError(ctx,
			"failed to list pods on node %q in namespace %q: %v", req.NodeId, claimNamespace, err)
	}
	var users, optedIn []*corev1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		// The fake clientset used in tests ignores field selectors.
		if pod.Spec.NodeName != req.NodeId {
			continue
		}
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		usesClaim := false
		for _, name := range podClaimNames(pod) {
			if name == claimName {
				usesClaim = true
				break
			}
		}
		if !usesClaim {
			continue
		}
		users = append(users, pod)
		if isPodVolumeSpreadRequested(pod) {
			optedIn = append(optedIn, pod)
		}
	}
	if len(users) == 0 {
		return nil, csifault.CSIInternalFault, spreadLookupError(ctx,
			"no pod on node %q uses PersistentVolumeClaim %s/%s of volume %q yet",
			req.NodeId, claimNamespace, claimName, req.VolumeId)
	}
	if len(optedIn) == 0 {
		return nil, "", nil
	}
	if len(optedIn) > 1 {
		msg := fmt.Sprintf("%s PersistentVolumeClaim %s/%s is used by %d pods on node %q that ask for spread "+
			"placement (%s/%s and %s/%s); one volume cannot satisfy the layout of more than one pod",
			volumeSpreadNotSatisfiablePrefix, claimNamespace, claimName, len(optedIn), req.NodeId,
			optedIn[0].Namespace, optedIn[0].Name, optedIn[1].Namespace, optedIn[1].Name)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition, msg)
	}
	pod := optedIn[0]
	podKey := pod.Namespace + "/" + pod.Name

	if !isPodVolumeSpreadSupported(ctx) {
		msg := fmt.Sprintf("%s pod %s asks for spread placement, but the supervisor does not have the %q capability",
			volumeSpreadNotSatisfiablePrefix, podKey, common.SharedDiskFss)
		log.Error(msg)
		return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition, msg)
	}

	spread := &podVolumeSpread{podKey: podKey, siblings: map[string]struct{}{}}
	for _, name := range podClaimNames(pod) {
		handle, isBlock, err := c.spreadVolumeOfClaim(ctx, pod.Namespace, name)
		if err != nil {
			return nil, csifault.CSIInternalFault, spreadLookupError(ctx,
				"failed to inspect PersistentVolumeClaim %s/%s of pod %s: %v", pod.Namespace, name, podKey, err)
		}
		if !isBlock {
			continue
		}
		spread.count++
		if handle != "" {
			spread.siblings[handle] = struct{}{}
		}
	}
	if _, ok := spread.siblings[req.VolumeId]; !ok {
		// The volume being published is one of the pod's claims, so it must
		// have been counted. If it was not, the claims changed under us.
		return nil, csifault.CSIInternalFault, spreadLookupError(ctx,
			"volume %q was not found among the block volumes of pod %s", req.VolumeId, podKey)
	}
	log.Infof("volume %q belongs to pod %s which asks for spread placement of its %d block volume(s)",
		req.VolumeId, podKey, spread.count)
	return spread, "", nil
}

// spreadLookupError logs and returns the Unavailable error used whenever the
// pod behind an attach, or its volumes, cannot be determined. Unavailable makes
// the external-attacher retry.
func spreadLookupError(ctx context.Context, format string, args ...interface{}) error {
	msg := "cannot determine volume placement: " + fmt.Sprintf(format, args...)
	logger.GetLogger(ctx).Error(msg)
	return status.Error(codes.Unavailable, msg)
}

// spreadVolumeOfClaim reports whether the claim is a vSphere block volume that
// takes part in spreading, and the volume handle (the VM volume name) when the
// claim is already bound.
//
// File volumes are never attached to the node VM, so claims that request
// ReadWriteMany or ReadOnlyMany are skipped, and so are guest-local NFS
// volumes and volumes of other drivers. A claim that is not bound yet, as
// happens with WaitForFirstConsumer, is judged by its StorageClass.
func (c *controller) spreadVolumeOfClaim(ctx context.Context, namespace, name string) (string, bool, error) {
	pvc, err := c.guestClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", false, err
	}
	if isFileVolumeAccessMode(pvc.Spec.AccessModes) {
		return "", false, nil
	}
	if pvc.Spec.VolumeName != "" {
		pv, err := c.guestClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return "", false, err
		}
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != csitypes.Name || IsGuestNFSVolumeID(pv.Spec.CSI.VolumeHandle) {
			return "", false, nil
		}
		return pv.Spec.CSI.VolumeHandle, true, nil
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return "", false, nil
	}
	sc, err := c.guestClient.StorageV1().StorageClasses().Get(ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return "", false, err
	}
	if sc.Provisioner != csitypes.Name || hasNFSServerParam(sc.Parameters) {
		return "", false, nil
	}
	return "", true, nil
}

// addSpreadVolumeToVMSpec adds the volume of an opted-in pod to the node VM's
// spec on the SCSI bus that keeps the pod's volumes evenly spread, and returns
// the VirtualMachine, in the API version the rest of ControllerPublishVolume
// works with, so the caller can wait for the attach.
//
// The whole layout of the pod is checked before anything is written. If it
// does not fit, the call fails with ResourceExhausted and nothing is patched,
// so every volume of the pod fails the same way and none attaches. Only the
// controller type and bus are set; vm-operator assigns the unit number and
// creates the controller if needed.
func (c *controller) addSpreadVolumeToVMSpec(ctx context.Context, req *csi.ControllerPublishVolumeRequest,
	vmKey types.NamespacedName, spread *podVolumeSpread, deadline time.Time) (
	*vmoperatortypes.VirtualMachine, string, error) {
	log := logger.GetLogger(ctx)
	backoff := vmPatchRetryBackoff
	for {
		vm := &vmoperatorv1alpha5.VirtualMachine{}
		if err := c.vmOperatorClient.Get(ctx, vmKey, vm); err != nil {
			msg := fmt.Sprintf("failed to get VirtualMachines for the node: %q. Error: %+v", req.NodeId, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Internal, msg)
		}
		for _, volume := range vm.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.Name == req.VolumeId {
				log.Infof("Volume %q is already present in the virtualMachine.Spec.Volumes", volume.Name)
				return spreadVMForAttachWait(vm), "", nil
			}
		}

		state, err := spreadStateFromVM(vm, spread.siblings)
		if err != nil {
			// vm-operator has not recorded where the VM's devices are, so the
			// layout cannot be checked yet. Retry once it has.
			msg := fmt.Sprintf("cannot determine volume placement for pod %s on virtualmachine %q: %v",
				spread.podKey, vm.Name, err)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.Unavailable, msg)
		}
		shares, err := computeSpreadShares(spread.count, state)
		if err != nil {
			return nil, csifault.CSIInternalFault, spreadNotSatisfiableError(ctx, spread.podKey, vm.Name, err)
		}
		bus, err := chooseSpreadBus(shares, state)
		if err != nil {
			return nil, csifault.CSIInternalFault, spreadNotSatisfiableError(ctx, spread.podKey, vm.Name, err)
		}

		// Same guard as the default path: do not record an attach that is no
		// longer requested, because vm-operator would keep reconciling it.
		if !c.isAttachStillRequested(ctx, req.VolumeId, req.NodeId) {
			msg := fmt.Sprintf("attach of volume %q to node %q is no longer requested, "+
				"not adding it to virtualmachine %q spec", req.VolumeId, req.NodeId, vm.Name)
			log.Error(msg)
			return nil, csifault.CSIInternalFault, status.Error(codes.FailedPrecondition, msg)
		}

		oldVM := vm.DeepCopy()
		vm.Spec.Volumes = append(vm.Spec.Volumes, vmoperatorv1alpha5.VirtualMachineVolume{
			Name: req.VolumeId,
			VirtualMachineVolumeSource: vmoperatorv1alpha5.VirtualMachineVolumeSource{
				PersistentVolumeClaim: &vmoperatorv1alpha5.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: req.VolumeId,
					},
				},
			},
			ControllerType:      vmoperatorv1alpha5.VirtualControllerTypeSCSI,
			ControllerBusNumber: ptr.To(bus),
		})
		// The optimistic lock makes this read the state vm-operator admits the
		// volume against: a concurrent attach to the same VM causes a conflict
		// and the layout is recomputed from the new state.
		err = c.vmOperatorClient.Patch(ctx, vm,
			client.MergeFromWithOptions(oldVM, client.MergeFromWithOptimisticLock{}))
		if err == nil {
			log.Infof("placed volume %q of pod %s on SCSI bus %d of virtualmachine %q (layout %s)",
				req.VolumeId, spread.podKey, bus, vm.Name, shares)
			if faultType, err := c.ensureSpreadVolumeHasUnit(ctx, req, vmKey, vm); err != nil {
				return nil, faultType, err
			}
			return spreadVMForAttachWait(vm), "", nil
		}
		log.Errorf("failed to update virtualmachine. Err: %v", err)
		if apierrors.IsInvalid(err) || apierrors.IsForbidden(err) || apierrors.IsBadRequest(err) {
			// vm-operator refused the placement, e.g. the bus filled up
			// between our read and its admission. Do not fall back to
			// another bus: that would break the even layout.
			return nil, csifault.CSIInternalFault, spreadNotSatisfiableError(ctx, spread.podKey, vm.Name,
				fmt.Errorf("vm-operator rejected placing volume %q on SCSI bus %d: %v", req.VolumeId, bus, err))
		}
		if !waitBeforeVMPatchRetry(ctx, &backoff, deadline) {
			faultType, err := vmPatchRetryStoppedError(ctx, "add", req.VolumeId, vm.Name)
			return nil, faultType, err
		}
	}
}

// spreadNotSatisfiableError logs and returns the ResourceExhausted error for a
// pod whose volumes cannot be spread evenly on the node VM.
func spreadNotSatisfiableError(ctx context.Context, podKey, vmName string, cause error) error {
	msg := fmt.Sprintf("%s %v (pod %s, virtualmachine %q)", volumeSpreadNotSatisfiablePrefix, cause, podKey, vmName)
	logger.GetLogger(ctx).Error(msg)
	return status.Error(codes.ResourceExhausted, msg)
}

// ensureSpreadVolumeHasUnit checks the VirtualMachine returned by the patch
// that added the volume. vm-operator assigns the unit number in the same
// admission step that stores the volume, except on a VM it has not upgraded
// yet, where it skips placement and stores the volume as written. Such a
// volume would be attached with default placement, so its entry is removed
// again and the call fails with Unavailable to be retried after vm-operator has
// caught up.
func (c *controller) ensureSpreadVolumeHasUnit(ctx context.Context, req *csi.ControllerPublishVolumeRequest,
	vmKey types.NamespacedName, patched *vmoperatorv1alpha5.VirtualMachine) (string, error) {
	log := logger.GetLogger(ctx)
	for _, volume := range patched.Spec.Volumes {
		if volume.Name == req.VolumeId && volume.UnitNumber != nil {
			return "", nil
		}
	}
	msg := fmt.Sprintf("vm-operator did not assign a unit number to volume %q on virtualmachine %q; "+
		"the virtualmachine is not ready for controller placement yet", req.VolumeId, vmKey.Name)
	log.Error(msg)
	// Detached from the RPC's context so a canceled call still cleans up.
	cleanupDeadline := time.Now().Add(removeUnplacedVolumeTimeout)
	cleanupCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx),
		cleanupDeadline.Add(removeStaleVolumeGracePeriod))
	defer cancel()
	if err := c.removeSpreadVolumeFromVMSpec(cleanupCtx, vmKey, req.VolumeId, cleanupDeadline); err != nil {
		return csifault.CSIInternalFault, status.Errorf(codes.Internal,
			"%s. Failed to remove it from the virtualmachine spec, volume may attach with default placement: %v",
			msg, err)
	}
	return csifault.CSIInternalFault, status.Error(codes.Unavailable, msg)
}

// removeSpreadVolumeFromVMSpec removes volumeID from the VirtualMachine's
// Spec.Volumes, retrying on patch errors until deadline. It is a no-op when the
// VirtualMachine is gone or the volume is not in the spec.
func (c *controller) removeSpreadVolumeFromVMSpec(ctx context.Context, vmKey types.NamespacedName,
	volumeID string, deadline time.Time) error {
	log := logger.GetLogger(ctx)
	backoff := vmPatchRetryBackoff
	for {
		vm := &vmoperatorv1alpha5.VirtualMachine{}
		if err := c.vmOperatorClient.Get(ctx, vmKey, vm); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		oldVM := vm.DeepCopy()
		found := false
		for index, volume := range vm.Spec.Volumes {
			if volume.Name == volumeID {
				vm.Spec.Volumes = append(vm.Spec.Volumes[:index], vm.Spec.Volumes[index+1:]...)
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		err := c.vmOperatorClient.Patch(ctx, vm,
			client.MergeFromWithOptions(oldVM, client.MergeFromWithOptimisticLock{}))
		if err == nil {
			log.Infof("Removed volume %q from virtualmachine %q spec", volumeID, vmKey.Name)
			return nil
		}
		log.Errorf("failed to remove volume %q from virtualmachine %q spec. Err: %v", volumeID, vmKey.Name, err)
		if !waitBeforeVMPatchRetry(ctx, &backoff, deadline) {
			return err
		}
	}
}

// spreadVMForAttachWait converts what the rest of ControllerPublishVolume reads
// from the VirtualMachine while it waits for the attach: its name and the
// attach status of its volumes.
func spreadVMForAttachWait(vm *vmoperatorv1alpha5.VirtualMachine) *vmoperatortypes.VirtualMachine {
	out := &vmoperatortypes.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{Name: vm.Name, Namespace: vm.Namespace},
	}
	for _, volume := range vm.Status.Volumes {
		out.Status.Volumes = append(out.Status.Volumes, vmoperatortypes.VirtualMachineVolumeStatus{
			Name:     volume.Name,
			Attached: volume.Attached,
			DiskUUID: volume.DiskUUID,
			Error:    volume.Error,
		})
	}
	return out
}
