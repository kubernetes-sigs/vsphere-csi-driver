/*
Copyright 2021 The Kubernetes Authors.

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

// Package internalapis contains API Schema definitions for internal apis
// +k8s:deepcopy-gen=package,register
// +groupName=cns.vmware.com
package internalapis

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	cnsfilevolclientv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/cnsfilevolumeclient/v1alpha1"
	triggercsifullsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsoperator/triggercsifullsync/v1alpha1"
	cnscsisvfeaturestatesv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/featurestates/v1alpha1"
)

// GroupName represents the group for cns operator apis
const GroupName = "cns.vmware.com"

// Version represents the version for cns operator apis
const Version = "v1alpha1"

var (
	// SchemeGroupVersion is group version used to register these objects
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

	// CnsFileVolumeClientPlural is plural of CnsFileVolumeClient
	CnsFileVolumeClientPlural = "cnsfilevolumeclients"

	// TriggerCsiFullSyncPlural is plural of TriggerCsiFullSyncPlural
	TriggerCsiFullSyncPlural = "triggercsifullsyncs"
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
		&cnsfilevolclientv1alpha1.CnsFileVolumeClient{},
		&cnsfilevolclientv1alpha1.CnsFileVolumeClientList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&triggercsifullsyncv1alpha1.TriggerCsiFullSync{},
		&triggercsifullsyncv1alpha1.TriggerCsiFullSyncList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnscsisvfeaturestatesv1alpha1.CnsCsiSvFeatureStates{},
		&cnscsisvfeaturestatesv1alpha1.CnsCsiSvFeatureStatesList{},
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
