/*
Copyright 2019 The Kubernetes Authors.

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

// NOTE: Boilerplate only.  Ignore this file.

// Package apis v1alpha1 contains API Schema definitions for the cns v1alpha1 API group
// +k8s:deepcopy-gen=package,register
// +groupName=cns.vmware.com
package apis

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsnodevmattachment/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/apis/cnsvolumemetadata/v1alpha1"
)

var (
	// SchemeGroupVersion is group version used to register these objects
	SchemeGroupVersion = schema.GroupVersion{Group: "cns.vmware.com", Version: "v1alpha1"}
	// CnsNodeVMAttachmentPlural is plural of CnsNodeVMAttachment
	CnsNodeVMAttachmentPlural = "cnsnodevmattachments"
	// CnsVolumeMetadataPlural is plural of CnsVolumeMetadata
	CnsVolumeMetadataPlural = "cnsvolumemetadatas"
)

var (
	// SchemeBuilder is the builder used to register these objects
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	localSchemeBuilder.Register(addKnownTypes)
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// Adds the list of known types to the given scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsvolumemetadatav1alpha1.CnsVolumeMetadata{},
		&cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsnodevmattachmentv1alpha1.CnsNodeVMAttachment{},
		&cnsnodevmattachmentv1alpha1.CnsNodeVMAttachmentList{},
	)
	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&metav1.Status{},
	)

	metav1.AddToGroupVersion(
		scheme,
		SchemeGroupVersion,
	)

	return nil
}
