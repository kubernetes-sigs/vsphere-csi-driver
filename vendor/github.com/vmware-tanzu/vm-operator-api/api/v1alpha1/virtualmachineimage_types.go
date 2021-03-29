// Copyright (c) 2018 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VirtualMachineImageProductInfo describes optional product-related information that can be added to an image
// template.  This information can be used by the image author to communicate details of the product contained in the
// image.
type VirtualMachineImageProductInfo struct {
	// Product typically describes the type of product contained in the image.
	// +optional
	Product string `json:"product,omitempty"`

	// Vendor typically describes the name of the vendor that is producing the image.
	// +optional
	Vendor string `json:"vendor,omitempty"`

	// Version typically describes a short-form version of the image.
	// +optional
	Version string `json:"version,omitempty"`

	// FullVersion typically describes a long-form version of the image.
	// +optional
	FullVersion string `json:"fullVersion,omitempty"`
}

// VirtualMachineImageOSInfo describes optional information related to the image operating system that can be added
// to an image template. This information can be used by the image author to communicate details of the operating
// system associated with the image.
type VirtualMachineImageOSInfo struct {
	// Version typically describes the version of the guest operating system.
	// +optional
	Version string `json:"version,omitempty"`

	// Type typically describes the type of the guest operating system.
	// +optional
	Type string `json:"type,omitempty"`
}

// VirtualMachineImageSpec defines the desired state of VirtualMachineImage
type VirtualMachineImageSpec struct {
	// Type describes the type of the VirtualMachineImage. Currently, the only supported image is "OVF"
	Type string `json:"type"`

	// ImageSourceType describes the type of content source of the VirtualMachineImage.  The only Content Source
	// supported currently is the vSphere Content Library.
	// +optional
	ImageSourceType string `json:"imageSourceType,omitempty"`

	// ProductInfo describes the attributes of the VirtualMachineImage relating to the product contained in the
	// image.
	// +optional
	ProductInfo VirtualMachineImageProductInfo `json:"productInfo,omitempty"`

	// OSInfo describes the attributes of the VirtualMachineImage relating to the Operating System contained in the
	// image.
	// +optional
	OSInfo VirtualMachineImageOSInfo `json:"osInfo,omitempty"`
}

// VirtualMachineImageStatus defines the observed state of VirtualMachineImage
type VirtualMachineImageStatus struct {
	Uuid       string `json:"uuid,omitempty"`
	InternalId string `json:"internalId"`
	PowerState string `json:"powerState,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=vmimage
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.productInfo.version"
// +kubebuilder:printcolumn:name="OsType",type="string",JSONPath=".spec.osInfo.type"

// VirtualMachineImage is the Schema for the virtualmachineimages API
// A VirtualMachineImage represents a VirtualMachine image (e.g. VM template) that can be used as the base image
// for creating a VirtualMachine instance.  The VirtualMachineImage is a required field of the VirtualMachine
// spec.  Currently, VirtualMachineImages are immutable to end users.  They are created and managed by a
// VirtualMachineImage controller whose role is to discover available images in the backing infrastructure provider
// that should be surfaced as consumable VirtualMachineImage resources.
type VirtualMachineImage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VirtualMachineImageSpec   `json:"spec,omitempty"`
	Status VirtualMachineImageStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VirtualMachineImageList contains a list of VirtualMachineImage
type VirtualMachineImageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VirtualMachineImage `json:"items"`
}

func init() {
	RegisterTypeWithScheme(&VirtualMachineImage{}, &VirtualMachineImageList{})
}
