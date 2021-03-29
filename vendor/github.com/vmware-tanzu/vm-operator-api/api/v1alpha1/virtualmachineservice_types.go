// Copyright (c) 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VirtualMachineService Type string describes ingress methods for a service
type VirtualMachineServiceType string

// These types correspond to a subset of the core Service Types
const (
	// VirtualMachineServiceTypeClusterIP means a service will only be accessible inside the
	// cluster, via the cluster IP.
	VirtualMachineServiceTypeClusterIP VirtualMachineServiceType = "ClusterIP"

	// VirtualMachineServiceTypeLoadBalancer means a service will be exposed via an
	// external load balancer (if the cloud provider supports it), in addition
	// to 'NodePort' type.
	VirtualMachineServiceTypeLoadBalancer VirtualMachineServiceType = "LoadBalancer"

	// VirtualMachineServiceTypeExternalName means a service consists of only a reference to
	// an external name that kubedns or equivalent will return as a CNAME
	// record, with no exposing or proxying of any pods involved.
	VirtualMachineServiceTypeExternalName VirtualMachineServiceType = "ExternalName"
)

// VirtualMachineServicePort describes the specification of a service port to be exposed by a VirtualMachineService.
// This VirtualMachineServicePort specification includes attributes that define the external and internal
// representation of the service port.
type VirtualMachineServicePort struct {
	// Name describes the name to be used to identify this VirtualMachineServicePort
	Name string `json:"name"`

	// Protocol describes the Layer 4 transport protocol for this port.  Supports "TCP", "UDP", and "SCTP".
	Protocol string `json:"protocol"`

	// Port describes the external port that will be exposed by the service.
	Port int32 `json:"port"`

	// TargetPort describes the internal port open on a VirtualMachine that should be mapped to the external Port.
	TargetPort int32 `json:"targetPort"`
}

// LoadBalancerStatus represents the status of a Load Balancer instance.
type LoadBalancerStatus struct {
	// Ingress is a list containing ingress addresses for the Load Balancer.
	// Traffic intended for the service should be sent to any of these ingress points.
	// +optional
	Ingress []LoadBalancerIngress `json:"ingress,omitempty"`
}

// LoadBalancerIngress represents the status of a Load Balancer ingress point. Traffic intended for the service should
// be sent to network endpoints specified by the endpoints in the LoadBalancerStatus.  IP or Hostname may both be set
// in this structure.  It is up to the consumer to determine which field should be used when accessing this
// LoadBalancer.
type LoadBalancerIngress struct {
	// IP is set for Load Balancer ingress points that are specified by an IP address.
	// +optional
	IP string `json:"ip,omitempty"`

	// Hostname is set for Load Balancer ingress points that are specified by a DNS address.
	// +optional
	Hostname string `json:"hostname,omitempty"`
}

// VirtualMachineServiceSpec defines the desired state of VirtualMachineService.  Each VirtualMachineService exposes
// a set of TargetPorts on a set of VirtualMachine instances as a network endpoint within or outside of the
// Kubernetes cluster.  The VirtualMachineService is loosely coupled to the VirtualMachines that are backing it through
// the use of a Label Selector.  In Kubernetes, a Label Selector enables matching of a resource using a set of
// key-value pairs, aka Labels.  By using a Label Selector, the VirtualMachineService can be generically defined to apply
// to any VirtualMachine that has the appropriate set of labels.
type VirtualMachineServiceSpec struct {
	// Type specifies a desired VirtualMachineServiceType for this VirtualMachineService.  The supported types
	// are VirtualMachineServiceTypeClusterIP and VirtualMachineServiceTypeLoadBalancer.
	Type VirtualMachineServiceType `json:"type"`

	// Ports specifies a list of VirtualMachineServicePort to expose with this VirtualMachineService.  Each of these ports
	// will be an accessible network entry point to access this service by.
	Ports []VirtualMachineServicePort `json:"ports"`

	// Selector specifies a map of key-value pairs, also known as a Label Selector, that is used to match this
	// VirtualMachineService with the set of VirtualMachines that should back this VirtualMachineService.
	Selector map[string]string `json:"selector"`

	// Just support cluster IP for now
	ClusterIP    string `json:"clusterIp,omitempty"`
	ExternalName string `json:"externalName,omitempty"`
}

// VirtualMachineServiceStatus defines the observed state of VirtualMachineService
type VirtualMachineServiceStatus struct {
	// LoadBalancer contains the current status of the Load Balancer.  The LoadBalancer status can be used to introspect
	// the state and attributes of any LoadBalancer instances that are fulfilling the VirtualmachineService.
	// +optional
	LoadBalancer LoadBalancerStatus `json:"loadBalancer,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=vmservice
// +kubebuilder:storageversion
// +kubebuilder:subresource:status

// VirtualMachineService is the Schema for the virtualmachineservices API.
// A VirtualMachineService represents the desired specification and the observed status of a VirtualMachineService
// instance.  A VirtualMachineService represents a network service, provided by one or more VirtualMachines, that is
// desired to be exposed to other workloads both internal and external to the cluster.
type VirtualMachineService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineServiceSpec   `json:"spec,omitempty"`
	Status VirtualMachineServiceStatus `json:"status,omitempty"`
}

func (s *VirtualMachineService) NamespacedName() string {
	return s.Namespace + "/" + s.Name
}

// +kubebuilder:object:root=true

// VirtualMachineServiceList contains a list of VirtualMachineService.
type VirtualMachineServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineService `json:"items"`
}

func init() {
	RegisterTypeWithScheme(&VirtualMachineService{}, &VirtualMachineServiceList{})
}
