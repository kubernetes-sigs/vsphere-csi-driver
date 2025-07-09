/*
Copyright (c) 2025 Broadcom. All Rights Reserved.
Broadcom Confidential. The term "Broadcom" refers to Broadcom Inc.
and/or its subsidiaries.

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

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type CapabilityName string
type CapabilityType string
type ServiceID string

const (
	VCenterCapabilityType           CapabilityType = "VCenter"
	SupervisorCapabilityType        CapabilityType = "Supervisor"
	SupervisorServiceCapabilityType CapabilityType = "Service"
)

// Notes on Supervisor Level Capability:
// 1. Supervisor-level capability is associated with functionality that would only make changes to IaaS controllers.
// 2. The capability is associated with functionality that would require changes to infrastructure (VC) as well as to
//	IaaS controllers.
// 3. All functionalities must expose supervisor-level capability if they fall into one of the above cases.
// 4. Supervisor capability can exist independently or depend on Kubernetes versions and/or VC capabilities.
// 5. Capability specification defines supervisor-level capabilities and their dependencies (if any) with Kubernetes
//	version and VC capabilities.
// Each capability will transition through the states enable=false --> enable=true --> (enable=true, activated=false)
//	--> (enable=true, activated=true) --> Deprecate.
// The enable status is statically set in-house at the time of feature completion, and the activated state is evaluated
// at runtime during supervisor install and upgrade.
// Capability enablement rules can be specified though ActivatedWhenRule. The rule is evaluated when specified and the
// effective capability status is set in the status field.

// Capability represents a feature and its specification (enablement status, activation rules, user facing).
type Capability struct {
	// Name of the capability. For e.g. supports_supervisor_async_upgrade
	Name CapabilityName `json:"name"`

	// Type of the capability (optional), it can be VCenter or Supervisor
	//+kubebuilder:validation:Enum=VCenter;Supervisor;Service
	Type CapabilityType `json:"type,omitempty"`

	// Description of the capability
	Description string `json:"description,omitempty"`

	// Enabled represents if the capability is enabled or not. This is a build time status,
	// and it need not necessarily mean its activated.
	// +kubebuilder:default:=false
	Enabled bool `json:"enabled"`

	// Notes on ActivatedWhenRule:
	// Considering there is a single input to CEL that is baked from:
	// CSP -> SupervisorCompatibilityDoc -> kubernetesVersion
	// CSP -> Capabilities CR -> SupervisorCapabilities -> supervisor
	// WCPSVC -> Platform (Extended with infra(VC) capabilities) ->  InfraCapabilities -> infra
	// CEL input format:
	//{
	//  "kubernetesVersion": "1.25",
	//  "self": {
	//		"infra": [
	//			{
	//				"name": "supports_supervisor_upgrade_improvements",
	//				"type": "VCenter",
	//				"enabled": true
	//			}
	//		],
	//  	"supervisor": [
	//			{
	//				"name": "Resume_Failed_Supervisor_Upgrade_Supported",
	//				"type": "Supervisor",
	//				"enabled": true
	//				"activatedWhenRule": `kubernetesVersion >= "1.25" && self.infra.exists(e,
	//					e.name = 'supports_supervisor_upgrade_improvements' && e.enabled)`
	//			},
	//			{
	//				"name": "MultipleCL_For_TKG_Supported",
	//				"enabled": true
	//				"activationRule": `self.services.exists(svc, svc.name == 'tkg.vmware.com' &&
	//					svc.capabilities.exists(e, e.name == 'supports_multiple_cl' && e.enabled))
	//			}
	//  	]
	//  }
	//}
	// then, the CEL expression activatedWhenRule would evaluate to true and set effective CapabilityStatus.Activated
	// for Resume_Failed_Supervisor_Upgrade_Supported to true

	// ActivatedWhenRule is the CEL-Go(https://github.com/google/cel-go?tab=readme-ov-file) rule(s) that helps in
	// computing the effective status of the supervisor capability at runtime.
	// If ActivatedWhenRule is not specified, that means the supervisor capability is not dependent on infra(VC)
	// or Service capabilities.
	// In that case, Supervisor Capability enabled value will be considered as effective activated status value.
	ActivatedWhenRule string `json:"activatedWhenRule,omitempty"`

	// ActivationRule is the CEL-Go(https://github.com/google/cel-go?tab=readme-ov-file) rule(s) that helps in
	// computing the effective status of the supervisor capability at runtime.
	// If ActivationRule is not specified, that means the supervisor capability is not dependent on infra(VC) or
	// Service capabilities.
	// In that case, Supervisor Capability enabled value will be considered as effective activated status value.
	ActivationRule string `json:"activationRule,omitempty"`

	// UserFacing represents if the capability is user facing or not.
	// +kubebuilder:default:=false
	UserFacing bool `json:"userFacing,omitempty"`
}

// CapabilitiesSpec defines the overall infra and supervisor capabilities
type CapabilitiesSpec struct {
	// InfraCapabilities Infrastructure capabilities. This structure include all the VCenter capabilities.
	// +listType=map
	// +listMapKey=name
	InfraCapabilities []Capability `json:"infra,omitempty"`

	// SupervisorCapabilities Supervisor cluster capabilities. This structure will include all the Supervisor
	// cluster level capabilities that
	// exist either independently or depend on the infrastructure or service capabilities.
	// +listType=map
	// +listMapKey=name
	SupervisorCapabilities []Capability `json:"supervisor,omitempty"`

	// ServiceCapabilities contains all the Supervisor Service capabilities.
	// +listType=map
	// +listMapKey=serviceID
	ServiceCapabilities []ServiceCapabilitiesSpec `json:"services,omitempty"`
}

// ServiceCapability defines the overall capabilities presented by a Supervisor Service.
type ServiceCapabilitiesSpec struct {
	// ServiceID is the service's ID.
	ServiceID ServiceID `json:"serviceID"`

	// Capabilities contains a list of the all the Capability specifications provided by the Service.
	// +listType=map
	// +listMapKey=name
	Capabilities []Capability `json:"capabilities,omitempty"`
}

// CapabilityStatus represents capability status for given capability
type CapabilityStatus struct {
	// Activated status represents the effective supervisor capability that decides whether the supervisor supports the
	// capability or not
	Activated bool `json:"activated"`
}

// CapabilitiesStatus represents the computed status of each Supervisor Capability
type CapabilitiesStatus struct {
	// Supervisor map of supervisor capability to its activated status
	Supervisor map[CapabilityName]CapabilityStatus `json:"supervisor,omitempty"`

	// Services map of service to service capability to its activated status.
	Services map[ServiceID]map[CapabilityName]CapabilityStatus `json:"services,omitempty"`
}

/*
Summary of Capabilities Specification:
1. Supervisor capabilities are exposed at the supervisor level to identify the new functionality added to Supervisor.
   Any supervisor service can have its local set of capabilities that may or may not rely on the supervisor
   capabilities. If a supervisor service needs an infrastructure change, then a capability to be defined at the
   supervisor level and then define VC-level capability if VC changes are required.
2. Supervisor capability can exist independently or depend on Kubernetes versions and/or VC capabilities.
3. The supervisor capabilities specification defines supervisor-level capabilities and their dependencies (if any) with
   Kubernetes version and VC capabilities. Each capability will transition through the states enable=false -->
   enable=true --> (enable=true, activated=false) --> (enable=true, activated=true) --> Deprecate.
   The enable status is statically set in-house at the time of feature completion, and the activated state is
   evaluated at runtime during supervisor install and upgrade. To define such a structure, a CRD is helpful and
   will be extensible for future use cases. Evaluation of the activated state is performed in WCP Service as it
   is needed to find feature incompatibilities with infrastructure. IaaS controllers will use the activated state
   to turn on the new functionality.
4. A static supervisor capabilities file will exist in CSP for component owners to start their feature development,
   or component owners can also come up with a capability patch managed in their component SCM repository.
   The CSP build will merge all the documents into a single manifest YAML.
5. The supervisor capabilities manifest YAML is distributed along with an async release for WCP Service to find feature
   incompatibilities and evaluate the activated state and then update the state during supervisor install and upgrade.
   A supervisor service would re-release a new patch if one of its local capability runs into issue.
*/

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +genclient
type Capabilities struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CapabilitiesSpec `json:"spec"`
	// Status represents the final computed supervisor capabilities (based on infra and supervisor)
	Status CapabilitiesStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// CapabilitiesList contains a list of Capabilities
type CapabilitiesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Capabilities `json:"items"`
}
