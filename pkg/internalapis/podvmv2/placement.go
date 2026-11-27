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

package podvmv2

import (
	"context"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// PodVMPlacementGVR identifies the scheduler's placement decision for one Pod.
//
// TODO: These types are a local mirror of private.iaas.vmware.com/v1alpha1
// PodVMPlacement, which is still under review in the Supervisor
// controller-managers APIs (apis/private/podvm/v1alpha1), and the API group is
// still Proposed. Replace this mirror with the published types once the CRD
// merges.
var PodVMPlacementGVR = schema.GroupVersionResource{
	Group:    "private.iaas.vmware.com",
	Version:  "v1alpha1",
	Resource: "podvmplacements",
}

// PodVMPlacementConsumerCSI is the spec.requiredFeatures consumer value that
// names CSI.
const PodVMPlacementConsumerCSI = "CSI"

// PodVMPlacement records where the scheduler placed a Pod's PodVM. The
// scheduler's PreBind plugin creates it before VolumeBinding PreBind sets the
// PVC's selected Node, so CSI can read the committed host before the Pod is
// bound or its VM exists.
//
// It has the same name and namespace as its Pod and a controller owner
// reference to that Pod. Pod names are reused and garbage collection is
// asynchronous, so a PodVMPlacement applies to a Pod only when that owner
// reference carries the Pod's UID.
//
// Only the fields CSI reads are modelled. spec.storage, spec.configSpec and
// status are deliberately absent, and the unstructured conversion ignores them.
type PodVMPlacement struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PodVMPlacementSpec `json:"spec"`
}

// PodVMPlacementSpec is the placement decision. The scheduler owns it and
// rewrites it when placement is retried.
type PodVMPlacementSpec struct {
	// NodeName is the Kubernetes Node selected for the Pod, recorded before the
	// Pod is bound to it.
	NodeName string `json:"nodeName,omitempty"`
	// Target is the vCenter, cluster and ESXi host selected for the PodVM.
	Target PodVMPlacementTarget `json:"target"`
	// RequiredFeatures lists features whose data this object carries beyond
	// v1alpha1.
	RequiredFeatures []PodVMPlacementFeature `json:"requiredFeatures,omitempty"`
}

// PodVMPlacementTarget scopes the cluster and host MoIDs to one vCenter.
type PodVMPlacementTarget struct {
	// VCenterInstanceUUID is ServiceContent.about.instanceUuid of the vCenter
	// that manages ClusterMoID and HostMoID.
	VCenterInstanceUUID string `json:"vCenterInstanceUUID,omitempty"`
	// ClusterMoID is not validated by CSI: NodeMapping has no cluster MoID to
	// compare it with, and host ownership already confines HostMoID to the Node.
	ClusterMoID string `json:"clusterMoID,omitempty"`
	// HostMoID is the ESXi host selected for the PodVM.
	HostMoID string `json:"hostMoID,omitempty"`
}

// PodVMPlacementFeature names a feature whose data a PodVMPlacement carries,
// and the components that must understand it.
type PodVMPlacementFeature struct {
	Name      string   `json:"name,omitempty"`
	Consumers []string `json:"consumers,omitempty"`
}

// AppliesTo reports whether this placement was written for this Pod
// incarnation: its controller owner reference must be v1/Pod with the Pod's
// name and UID. A mismatch means the placement for this Pod is not written yet.
func (p *PodVMPlacement) AppliesTo(pod *corev1.Pod) bool {
	if p == nil || pod == nil || pod.UID == "" {
		return false
	}
	owner := metav1.GetControllerOfNoCopy(p)
	return owner != nil && owner.APIVersion == "v1" && owner.Kind == "Pod" &&
		owner.Name == pod.Name && owner.UID == pod.UID
}

// UnsupportedCSIFeatures returns the names of the required features that list
// CSI as a consumer. CSI supports no feature beyond v1alpha1, so any such entry
// means the object carries data CSI cannot see, and CSI must not act on it.
// Features for other consumers do not block CSI.
func (p *PodVMPlacement) UnsupportedCSIFeatures() []string {
	if p == nil {
		return nil
	}
	var unsupported []string
	for _, feature := range p.Spec.RequiredFeatures {
		if slices.Contains(feature.Consumers, PodVMPlacementConsumerCSI) {
			unsupported = append(unsupported, feature.Name)
		}
	}
	return unsupported
}

// GetPodVMPlacement reads the placement named after a Pod with a single GET.
// CSI never lists, watches or writes this resource. A NotFound error is
// returned unchanged so callers can tell "not written yet" from a failed read.
func GetPodVMPlacement(ctx context.Context, client dynamic.Interface,
	namespace, name string) (*PodVMPlacement, error) {
	obj, err := client.Resource(PodVMPlacementGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var placement PodVMPlacement
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &placement); err != nil {
		return nil, err
	}
	return &placement, nil
}
