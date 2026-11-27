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
	"fmt"
	"strings"

	vimtypes "github.com/vmware/govmomi/vim25/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/podvmv2"
)

// annSelectedNode is the PVC annotation through which the scheduler's VolumeBinding plugin names
// the Node selected for a WaitForFirstConsumer claim.
const annSelectedNode = "volume.kubernetes.io/selected-node"

// placementReader reads the PodVMPlacement that has the given Pod's namespace and name.
type placementReader func(ctx context.Context, namespace, name string) (*podvmv2.PodVMPlacement, error)

// readPodVMPlacement uses a GET by Pod name. CSI never lists, watches or writes the resource.
func (c *controller) readPodVMPlacement(ctx context.Context, namespace, name string) (
	*podvmv2.PodVMPlacement, error) {
	if c.dynamicClient == nil {
		return nil, fmt.Errorf("dynamic client is not initialized")
	}
	return podvmv2.GetPodVMPlacement(ctx, c.dynamicClient, namespace, name)
}

// constrainToPodPlacement narrows the host-local candidates of a HostLocalStorage + PodVM v2
// request by the claim's binding mode:
//
//   - A claim already bound to a PV keeps the host in that PV's cns.vmware.com/podvm-host-pinned
//     annotation.
//   - An Immediate claim, including every guest backing claim, keeps all candidates; CNS selects.
//   - A WaitForFirstConsumer claim on a dedicated ESXi Node is narrowed to the one host that Node
//     owns. No Pod or PodVMPlacement is read.
//   - A WaitForFirstConsumer claim on a VirtualNode is narrowed to spec.target.hostMoID of the
//     consumer Pod's PodVMPlacement, because a VirtualNode identifies a cluster, not a host.
func constrainToPodPlacement(ctx context.Context, client kubernetes.Interface, read placementReader,
	vcUUID, namespace, pvcName string, candidates []vimtypes.ManagedObjectReference,
	inventory map[string]map[string]string) ([]vimtypes.ManagedObjectReference, error) {
	if namespace == "" || pvcName == "" {
		return nil, fmt.Errorf("PVC name and namespace are required for host-local provisioning with PodVM v2")
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC %s/%s: %w", namespace, pvcName, err)
	}
	if pvc.Spec.VolumeName != "" {
		return constrainToBoundPV(ctx, client, pvc, candidates)
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return nil, fmt.Errorf("PVC %s/%s has no StorageClass", namespace, pvcName)
	}
	sc, err := client.StorageV1().StorageClasses().Get(ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get StorageClass %q of PVC %s/%s: %w",
			*pvc.Spec.StorageClassName, namespace, pvcName, err)
	}
	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode == storagev1.VolumeBindingImmediate {
		return candidates, nil
	}
	if *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		return nil, fmt.Errorf("StorageClass %q has unsupported volume binding mode %q",
			sc.Name, *sc.VolumeBindingMode)
	}
	return constrainToSelectedNode(ctx, client, read, vcUUID, pvc, candidates, inventory)
}

func constrainToBoundPV(ctx context.Context, client kubernetes.Interface, pvc *v1.PersistentVolumeClaim,
	candidates []vimtypes.ManagedObjectReference) ([]vimtypes.ManagedObjectReference, error) {
	pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get PV %q bound to PVC %s/%s: %w",
			pvc.Spec.VolumeName, pvc.Namespace, pvc.Name, err)
	}
	host := pv.Annotations[common.AnnPodVMHostPinned]
	if host == "" {
		return nil, fmt.Errorf("PV %q bound to PVC %s/%s has no %s annotation; refusing to guess its host",
			pv.Name, pvc.Namespace, pvc.Name, common.AnnPodVMHostPinned)
	}
	logger.GetLogger(ctx).Infof("PVC %s/%s is bound to PV %q pinned to host %q", pvc.Namespace, pvc.Name, pv.Name, host)
	return narrowToPlacementHost(candidates, host, "bound PV "+pv.Name)
}

func constrainToSelectedNode(ctx context.Context, client kubernetes.Interface, read placementReader,
	vcUUID string, pvc *v1.PersistentVolumeClaim, candidates []vimtypes.ManagedObjectReference,
	inventory map[string]map[string]string) ([]vimtypes.ManagedObjectReference, error) {
	namespace, pvcName := pvc.Namespace, pvc.Name
	selectedNode := pvc.Annotations[annSelectedNode]
	if selectedNode == "" {
		return nil, status.Errorf(codes.Unavailable,
			"WaitForFirstConsumer PVC %s/%s has no selected Node", namespace, pvcName)
	}
	node, err := client.CoreV1().Nodes().Get(ctx, selectedNode, metav1.GetOptions{})
	// ResourceExhausted lets the provisioner request rescheduling from a Node that is going away.
	if apierrors.IsNotFound(err) {
		return nil, status.Errorf(codes.ResourceExhausted,
			"selected Node %q of PVC %s/%s no longer exists", selectedNode, namespace, pvcName)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get Node %q selected for PVC %s/%s: %w", selectedNode, namespace, pvcName, err)
	}
	if node.DeletionTimestamp != nil {
		return nil, status.Errorf(codes.ResourceExhausted,
			"selected Node %q of PVC %s/%s is being deleted", selectedNode, namespace, pvcName)
	}
	switch {
	case podvmv2.IsDedicatedHostNode(node):
		host, err := dedicatedNodeHost(selectedNode, inventory)
		if err != nil {
			return nil, err
		}
		logger.GetLogger(ctx).Infof("PVC %s/%s selected dedicated Node %q, which owns host %q",
			namespace, pvcName, selectedNode, host)
		return narrowToPlacementHost(candidates, host, "dedicated Node "+selectedNode)
	case podvmv2.IsVirtualNode(node):
		return constrainToVirtualNodePlacement(ctx, client, read, vcUUID, selectedNode, pvc, candidates, inventory)
	default:
		return nil, fmt.Errorf("selected Node %q of PVC %s/%s is served by unsupported virtual-kubelet provider %q",
			selectedNode, namespace, pvcName, podvmv2.NodeTypeOf(node))
	}
}

// dedicatedNodeHost resolves ownership from the inventory. Source constraints can already
// have narrowed the candidate list to a different host.
func dedicatedNodeHost(nodeName string, inventory map[string]map[string]string) (string, error) {
	var host string
	for id, topology := range inventory {
		if topology[v1.LabelHostname] != nodeName {
			continue
		}
		if host != "" {
			return "", fmt.Errorf("dedicated Node %q owns more than one host (%q, %q)", nodeName, host, id)
		}
		host = id
	}
	if host == "" {
		return "", fmt.Errorf("selected dedicated Node %q owns no host in the requested topology", nodeName)
	}
	return host, nil
}

func constrainToVirtualNodePlacement(ctx context.Context, client kubernetes.Interface, read placementReader,
	vcUUID, selectedNode string, pvc *v1.PersistentVolumeClaim, candidates []vimtypes.ManagedObjectReference,
	inventory map[string]map[string]string) ([]vimtypes.ManagedObjectReference, error) {
	consumers, err := findClaimConsumers(ctx, client, pvc)
	if err != nil {
		return nil, err
	}
	placement, err := findClaimConsumerPlacement(ctx, read, pvc, selectedNode, consumers)
	if err != nil {
		return nil, err
	}
	host, err := committedPlacementHost(vcUUID, selectedNode, placement, inventory)
	if err != nil {
		return nil, err
	}
	logger.GetLogger(ctx).Infof("PodVMPlacement %s/%s commits PVC %s on VirtualNode %q to host %q",
		placement.Namespace, placement.Name, pvc.Name, selectedNode, host)
	return narrowToPlacementHost(candidates, host, "PodVMPlacement "+placement.Namespace+"/"+placement.Name)
}

// findClaimConsumers returns the live Pods that mount the claim, including Pods still waiting to be bound.
// TODO: Check if the scheduler can record the selected Pod's name and UID on the PVC alongside the selected Node
// and refresh them on scheduling retries. CSI could then use a direct Pod GET, verify the UID and claim reference,
// and read the PodVMPlacement without scanning Pods in the namespace.
func findClaimConsumers(ctx context.Context, client kubernetes.Interface,
	pvc *v1.PersistentVolumeClaim) ([]*v1.Pod, error) {
	namespace, pvcName := pvc.Namespace, pvc.Name
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list Pods in namespace %q: %w", namespace, err)
	}
	var consumers []*v1.Pod
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			continue
		}
		if podUsesClaim(pod, pvc) {
			consumers = append(consumers, pod)
		}
	}
	if len(consumers) == 0 {
		return nil, status.Errorf(codes.Unavailable,
			"no live Pod consumes WaitForFirstConsumer PVC %s/%s yet", namespace, pvcName)
	}
	return consumers, nil
}

// findClaimConsumerPlacement prefers a consumer already bound to the selected Node. Before the
// Pod binds, its current scheduler placement identifies its target Node. The PVC records only the
// Node, so multiple equally matching consumers are ambiguous.
func findClaimConsumerPlacement(ctx context.Context, read placementReader, pvc *v1.PersistentVolumeClaim,
	selectedNode string, consumers []*v1.Pod) (*podvmv2.PodVMPlacement, error) {
	var bound *v1.Pod
	var pending []*v1.Pod
	for _, pod := range consumers {
		switch pod.Spec.NodeName {
		case selectedNode:
			if bound != nil {
				return nil, status.Errorf(codes.FailedPrecondition,
					"PVC %s/%s has multiple consumers bound to selected Node %q (%q, %q); the host is ambiguous",
					pvc.Namespace, pvc.Name, selectedNode, bound.Name, pod.Name)
			}
			bound = pod
		case "":
			pending = append(pending, pod)
		}
	}
	if bound != nil {
		return readCurrentPodPlacement(ctx, read, bound)
	}
	if len(pending) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"all live consumers of PVC %s/%s are bound to Nodes other than selected Node %q",
			pvc.Namespace, pvc.Name, selectedNode)
	}
	if len(pending) == 1 {
		return readCurrentPodPlacement(ctx, read, pending[0])
	}

	var match *podvmv2.PodVMPlacement
	for _, pod := range pending {
		placement, err := readCurrentPodPlacement(ctx, read, pod)
		if status.Code(err) == codes.Unavailable {
			continue
		}
		if err != nil {
			return nil, err
		}
		if placement.Spec.NodeName != selectedNode {
			continue
		}
		if match != nil {
			return nil, status.Errorf(codes.FailedPrecondition,
				"PVC %s/%s has multiple consumer placements targeting selected Node %q (%q, %q); the host is ambiguous",
				pvc.Namespace, pvc.Name, selectedNode, match.Name, placement.Name)
		}
		match = placement
	}
	if match == nil {
		return nil, status.Errorf(codes.Unavailable,
			"no current PodVMPlacement for a consumer of PVC %s/%s targets selected Node %q yet",
			pvc.Namespace, pvc.Name, selectedNode)
	}
	return match, nil
}

// podUsesClaim reports whether the Pod mounts the claim directly or through a generic ephemeral
// volume. An ephemeral claim is named <pod name>-<volume name>, which is not unique: Pod "web" with
// volume "data-x" and Pod "web-data" with volume "x" both map to "web-data-x". As in Kubernetes'
// ephemeral.VolumeIsForPod, the claim must also be controlled by the Pod to count as its consumer.
func podUsesClaim(pod *v1.Pod, pvc *v1.PersistentVolumeClaim) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
			return true
		}
		if volume.Ephemeral != nil && pod.Name+"-"+volume.Name == pvc.Name && metav1.IsControlledBy(pvc, pod) {
			return true
		}
	}
	return false
}

// readCurrentPodPlacement requires the scheduler's placement to be owned by the live consumer Pod.
// The scheduler's PreBind plugin creates the object before VolumeBinding PreBind sets the selected
// Node, so a missing or stale object is retryable.
func readCurrentPodPlacement(ctx context.Context, read placementReader,
	consumer *v1.Pod) (*podvmv2.PodVMPlacement, error) {
	name := consumer.Namespace + "/" + consumer.Name
	placement, err := read(ctx, consumer.Namespace, consumer.Name)
	if apierrors.IsNotFound(err) || (err == nil && placement == nil) {
		return nil, status.Errorf(codes.Unavailable, "PodVMPlacement %s is not written yet", name)
	}
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to get PodVMPlacement %s: %v", name, err)
	}
	if placement.DeletionTimestamp != nil {
		return nil, status.Errorf(codes.Unavailable, "PodVMPlacement %s is being deleted", name)
	}
	// Pod names are reused and garbage collection is asynchronous, so an object left by an earlier
	// Pod of the same name can still exist. Treat it as not written yet for this Pod.
	if !placement.AppliesTo(consumer) {
		return nil, status.Errorf(codes.Unavailable,
			"PodVMPlacement %s is not controlled by Pod UID %q; the placement for this Pod is not written yet",
			name, consumer.UID)
	}
	return placement, nil
}

// committedPlacementHost validates placement against the selected Node and inventory. CSI does not
// require fields such as clusterMoID or status to resolve the host.
func committedPlacementHost(vcUUID, selectedNode string, placement *podvmv2.PodVMPlacement,
	inventory map[string]map[string]string) (string, error) {
	name := placement.Namespace + "/" + placement.Name
	// A feature that names CSI as a consumer carries data CSI cannot see through v1alpha1.
	if unsupported := placement.UnsupportedCSIFeatures(); len(unsupported) > 0 {
		return "", fmt.Errorf("PodVMPlacement %s requires features this CSI driver does not support: %v",
			name, unsupported)
	}
	if placement.Spec.NodeName != selectedNode {
		return "", fmt.Errorf("PodVMPlacement %s targets Node %q, not the selected Node %q",
			name, placement.Spec.NodeName, selectedNode)
	}
	// NodeMapping publishes bare host MoIDs with no vCenter identity, so this is the only
	// per-placement guard that hostMoID belongs to the vCenter CSI resolves hosts against.
	target := placement.Spec.Target
	if vcUUID == "" || !strings.EqualFold(target.VCenterInstanceUUID, vcUUID) {
		return "", fmt.Errorf("PodVMPlacement %s names vCenter %q, not the configured vCenter %q",
			name, target.VCenterInstanceUUID, vcUUID)
	}
	if target.HostMoID == "" {
		return "", fmt.Errorf("PodVMPlacement %s has no spec.target.hostMoID", name)
	}
	// The scheduler and the NodeMapping producer write independently, so they must agree: the
	// host must be owned by the selected VirtualNode in the ownership view. That holds only when
	// the VirtualNode's validated, Ready NodeMapping lists the host and no other Node contests it.
	if owner := inventory[target.HostMoID][v1.LabelHostname]; owner != selectedNode {
		if owner == "" {
			return "", fmt.Errorf("PodVMPlacement %s names host %q, which has no current owner in the "+
				"requested topology", name, target.HostMoID)
		}
		return "", fmt.Errorf("PodVMPlacement %s names host %q, which is owned by Node %q, not the selected "+
			"VirtualNode %q", name, target.HostMoID, owner, selectedNode)
	}
	return target.HostMoID, nil
}

// narrowToPlacementHost reduces the candidates to the committed host, which must already be one
// of them after zone, namespace, ownership and source restrictions.
func narrowToPlacementHost(candidates []vimtypes.ManagedObjectReference, host,
	source string) ([]vimtypes.ManagedObjectReference, error) {
	for _, candidate := range candidates {
		if candidate.Value == host {
			return []vimtypes.ManagedObjectReference{candidate}, nil
		}
	}
	return nil, fmt.Errorf("host %q from %s is not among the candidate hosts %v allowed by the volume "+
		"topology and source constraints", host, source, candidates)
}
