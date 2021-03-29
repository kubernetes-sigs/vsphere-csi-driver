// Copyright (c) 2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type VirtualMachinePowerState string

// See govmomi.vim25.types.VirtualMachinePowerState
const (
	VirtualMachinePoweredOff VirtualMachinePowerState = "poweredOff"
	VirtualMachinePoweredOn  VirtualMachinePowerState = "poweredOn"
)

// VMStatusPhase is used to indicate the phase of a VirtualMachine's lifecycle.
type VMStatusPhase string

const (
	// The Created phase indicates that the VirtualMachine has been already been created by the backing infrastructure
	// provider.
	Created VMStatusPhase = "Created"

	// The Deleted phase indicates that the VirtualMachine has been deleted by the backing infrastructure provider.
	Deleted VMStatusPhase = "Deleted"
)

// VirtualMachinePort is unused and can be considered deprecated.
type VirtualMachinePort struct {
	Port     int             `json:"port"`
	Ip       string          `json:"ip"`
	Name     string          `json:"name"`
	Protocol corev1.Protocol `json:"protocol"`
}

// VirtualMachineNetworkInterface defines the properties of a network interface to attach to a VirtualMachine
// instance.  A VirtualMachineNetworkInterface describes network integration configuration that is to be
// used by the VirtualMachine controller when integrating the VirtualMachine into a VirtualNetwork.  Currently,
// only integration with NSX-T network integration is supported using this VirtualMachineNetworkInterface structure.
type VirtualMachineNetworkInterface struct {
	// NetworkType describes the type of VIrtualNetwork that is referenced by the NetworkName.  Currently, the only
	// supported NetworkType is "nsx-t".
	NetworkType string `json:"networkType,omitempty"`

	// NetworkName describes the name of an existing virtual network that this interface should be added to.  For
	// NSX-T networks, this is the name of a pre-existing NSX-T VirtualNetwork.
	NetworkName string `json:"networkName"`

	// EthernetCardType describes an optional ethernet card that should be used by the VirtualNetworkInterface (vNIC)
	// associated with this network integration.  The default is "vmxnet3".
	EthernetCardType string `json:"ethernetCardType,omitempty"`
}

// VirtualMachineMetadata defines any metadata that should be passed to the VirtualMachine instance.  A typical use
// case is for this metadata to be used for Guest Customization, however the intended use of the metadata is
// agnostic to the VirtualMachine controller.  VirtualMachineMetadata is read from a configured ConfigMap and then
// propagated to the VirtualMachine instance using a desired "Transport" mechanism.
type VirtualMachineMetadata struct {
	// ConfigMapName describes the name of the ConfigMap, in the same Namespace as the VirtualMachine, that should be
	// used for VirtualMachine metadata.  The contents of the Data field of the ConfigMap is used as the VM Metadata.
	// The format of the contents of the VM Metadata are not parsed or interpreted by the VirtualMachine controller.
	ConfigMapName string `json:"configMapName,omitempty"`

	// Transport describes the name of a supported VirtualMachineMetadata transport "protocol".  Currently, the only supported
	// transport prototocol is "ExtraConfig".  The ExtraConfig protocol places the VM Metadata as "ExtraConfig" on the
	// vSphere VM ConfigSpec when creating the VirtualMachine.
	Transport string `json:"transport,omitempty"`
}

// VirtualMachineVolume describes a Volume that should be attached to a specific VirtualMachine.  Currently,
// only PersistentVolumes are supported.
type VirtualMachineVolume struct {
	// Name specifies the name of the VirtualMachineVolume.  Each volume within the scope of a VirtualMachine must
	// have a unique name.
	Name string `json:"name"`

	// PersistentVolumeClaim represents a reference to a PersistentVolumeClaim in the same namespace. The PersistentVolumeClaim
	// must match a persistent volume provisioned (either statically or dynamically) by the cluster's CSI provider.
	PersistentVolumeClaim *corev1.PersistentVolumeClaimVolumeSource `json:"persistentVolumeClaim,omitempty"`
}

// Probe describes a health check to be performed against a VirtualMachine to determine whether it is
// alive or ready to receive traffic.
type Probe struct {
	// TCPSocket specifies an action involving a TCP port.
	// +optional
	TCPSocket *TCPSocketAction `json:"tcpSocket,omitempty"`

	// TimeoutSeconds specifies a number of seconds after which the probe times out.
	// Defaults to 10 seconds. Minimum value is 1.
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// TCPSocketAction describes an action based on opening a socket.
type TCPSocketAction struct {
	// Port specifies a number or name of the port to access on the VirtualMachine.
	// If the format of port is a number, it must be in the range 1 to 65535.
	// If the format of name is a string, it must be an IANA_SVC_NAME.
	Port intstr.IntOrString `json:"port"`

	// Host is an optional host name to connect to.  Host defaults to the VirtualMachine IP.
	// +optional
	Host string `json:"host,omitempty"`
}

// VirtualMachineSpec defines the desired state of a VirtualMachine
type VirtualMachineSpec struct {
	// ImageName describes the name of a VirtualMachineImage that is to be used as the base Operating System image of
	// the desired VirtualMachine instances.  The VirtualMachineImage resources can be introspected to discover identifying
	// attributes that may help users to identify the desired image to use.
	ImageName string `json:"imageName"`

	// ClassName describes the name of a VirtualMachineClass that is to be used as the overlayed resource configuration
	// of VirtualMachine.  A VirtualMachineClass is used to further customize the attributes of the VirtualMachine
	// instance.  See VirtualMachineClass for more description.
	ClassName string `json:"className"`

	// PowerState describes the desired power state of a VirtualMachine.  Valid power states are "poweredOff" and "poweredOn".
	PowerState VirtualMachinePowerState `json:"powerState"`

	// Ports is currently unused and can be considered deprecated.
	// +optional
	Ports []VirtualMachinePort `json:"ports,omitempty"`

	// VmMetadata describes any optional metadata that should be passed to the Guest OS.
	// +optional
	VmMetadata *VirtualMachineMetadata `json:"vmMetadata,omitempty"`

	// StorageClass describes the name of a StorageClass that should be used to configure storage-related attributes of the VirtualMachine
	// instance.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`

	// NetworkInterfaces describes a list of VirtualMachineNetworkInterfaces to be configured on the VirtualMachine instance.
	// Each of these VirtualMachineNetworkInterfaces describes external network integration configurations that are to be
	// used by the VirtualMachine controller when integrating the VirtualMachine into one or more external networks.
	// +optional
	NetworkInterfaces []VirtualMachineNetworkInterface `json:"networkInterfaces,omitempty"`

	// ResourcePolicyName describes the name of a VirtualMachineSetResourcePolicy to be used when creating the
	// VirtualMachine instance.
	// +optional
	ResourcePolicyName string `json:"resourcePolicyName"`

	// Volumes describes the list of VirtualMachineVolumes that are desired to be attached to the VirtualMachine.  Each of
	// these volumes specifies a volume identity that the VirtualMachine controller will attempt to satisfy, potentially
	// with an external Volume Management service.
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	Volumes []VirtualMachineVolume `json:"volumes,omitempty" patchStrategy:"merge" patchMergeKey:"name"`

	// ReadinessProbe describes a network probe that can be used to determine if the VirtualMachine is available and
	// responding to the probe.
	// +optional
	ReadinessProbe *Probe `json:"readinessProbe,omitempty"`
}

// VirtualMachineVolumeStatus defines the observed state of a VirtualMachineVolume instance.
type VirtualMachineVolumeStatus struct {
	// Name is the the name of the volume in a VirtualMachine.
	Name string `json:"name"`

	// Attached represents whether a volume has been successfully attached to the VirtualMachine or not.
	Attached bool `json:"attached"`

	// DiskUuid represents the underlying virtual disk UUID and is present when attachment succeeds.
	DiskUuid string `json:"diskUUID"`

	// Error represents the last error seen when attaching or detaching a volume.  Error will be empty if attachment succeeds.
	Error string `json:"error"`
}

// VirtualMachineStatus defines the observed state of a VirtualMachine instance.
type VirtualMachineStatus struct {
	// Host describes the hostname or IP address of the infrastructure host that the VirtualMachine is executing on.
	Host string `json:"host"`

	// PowerState describes the current power state of the VirtualMachine.
	PowerState VirtualMachinePowerState `json:"powerState"`

	// Phase describes the current phase information of the VirtualMachine.
	Phase VMStatusPhase `json:"phase"`

	// VmIp describes the IP address of the VirtualMachine.  Currently, a VirtualMachine only supports a single
	// network interface and interface address.
	VmIp string `json:"vmIp"`

	// UniqueID describes a unique identifier that is provided by the underlying infrastructure provider, such as
	// vSphere.
	UniqueID string `json:"uniqueID"`

	// BiosUUID describes a unique identifier provided by the underlying infrastructure provider that is exposed to the
	// Guest OS BIOS as a unique hardware identifier.
	BiosUUID string `json:"biosUUID"`

	// Volumes describes a list of current status information for each Volume that is desired to be attached to the
	// VirtualMachine.
	Volumes []VirtualMachineVolumeStatus `json:"volumes,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=vm
// +kubebuilder:storageversion
// +kubebuilder:subresource:status

// VirtualMachine is the Schema for the virtualmachines API.
// A VirtualMachine represents the desired specification and the observed status of a VirtualMachine instance.  A
// VirtualMachine is realized by the VirtualMachine controller on a backing Virtual Infrastructure provider such as
// vSphere.
type VirtualMachine struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineSpec   `json:"spec,omitempty"`
	Status VirtualMachineStatus `json:"status,omitempty"`
}

func (vm VirtualMachine) NamespacedName() string {
	return vm.Namespace + "/" + vm.Name
}

// VirtualMachineList contains a list of VirtualMachine.
//
// +kubebuilder:object:root=true
type VirtualMachineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachine `json:"items"`
}

func init() {
	RegisterTypeWithScheme(&VirtualMachine{}, &VirtualMachineList{})
}
