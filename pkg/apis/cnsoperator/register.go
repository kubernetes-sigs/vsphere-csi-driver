/*
Copyright 2020 The Kubernetes Authors.

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
	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	cnsnodevmattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmattachment/v1alpha1"
	cnsnodevmbatchattachmentv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsnodevmbatchattachment/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsunregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsunregistervolume/v1alpha1"
	cnsvolumemetadatav1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsvolumemetadata/v1alpha1"
	storagepolicyv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha1"
	storagepolicyv1alpha2 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagepolicy/v1alpha2"
	storagequotaperiodicsyncv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/storagequotaperiodicsync/v1alpha1"
)

// GroupName represents the group for cns operator apis
const GroupName = "cns.vmware.com"

// Version represents the version for cns operator apis
const Version = "v1alpha1"
const VersionV2 = "v1alpha2"

var (
	// SchemeGroupVersion is group version used to register these objects
	SchemeGroupVersion   = schema.GroupVersion{Group: GroupName, Version: Version}
	SchemeGroupVersionV2 = schema.GroupVersion{Group: GroupName, Version: VersionV2}
	// CnsNodeVMAttachmentSingular is Singular of CnsNodeVmAttachment
	CnsNodeVMAttachmentSingular = "cnsnodevmattachment"
	// CnsNodeVMAttachmentPlural is plural of CnsNodeVmAttachment
	CnsNodeVMAttachmentPlural = "cnsnodevmattachments"
	// CnsNodeVmBatchAttachmentPlural is plural of CnsNodeVmBatchAttachment
	CnsNodeVmBatchAttachmentPlural = "cnsnodevmbatchattachments"
	// CnsVolumeMetadataSingular is Singular of CnsVolumeMetadata
	CnsVolumeMetadataSingular = "cnsvolumemetadata"
	// CnsVolumeMetadataPlural is plural of CnsVolumeMetadata
	CnsVolumeMetadataPlural = "cnsvolumemetadatas"
	// CnsRegisterVolumePlural is plural of CnsRegisterVolume
	CnsRegisterVolumePlural = "cnsregistervolumes"
	// CnsUnregisterVolumePlural is plural of CnsUnregisterVolume
	CnsUnregisterVolumePlural = "cnsunregistervolumes"
	// CnsFileAccessConfigPlural is plural of CnsFileAccessConfig
	CnsFileAccessConfigPlural = "cnsfileaccessconfigs"
	// CnsStoragePolicyUsageSingular is singular of StoragePolicyUsage
	CnsStoragePolicyUsageSingular = "storagepolicyusage"
	// CnsStoragePolicyUsagePlural is plural of StoragePolicyUsage
	CnsStoragePolicyUsagePlural = "storagepolicyusages"
	// CnsStoragePolicyQuotaSingular is Singular of StoragePolicyQuota
	CnsStoragePolicyQuotaSingular = "storagepolicyquota"
	// CnsStoragePolicyQuotaPlural is plural of StoragePolicyQuota
	CnsStoragePolicyQuotaPlural = "storagepolicyquotas"
	// CnsStoragePolicyReservationSingular is Singular of StoragePolicyReservation
	CnsStoragePolicyReservationSingular = "storagepolicyreservation"
	// CnsStoragePolicyReservationPlural is plural of StoragePolicyReservation
	CnsStoragePolicyReservationPlural = "storagepolicyreservations"
	// CnsVirtualMachineSnapshotSingular is Singular of VirtualMachineSnapshot
	CnsVirtualMachineSnapshotSingular = "vitualmachinesnapshot"
	// CnsVirtualMachineSnapshotPlural is plural of VirtualMachineSnapshot
	CnsVirtualMachineSnapshotPlural = "vitualmachinesnapshots"
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
		&cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{},
		&cnsfileaccessconfigv1alpha1.CnsFileAccessConfigList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsregistervolumev1alpha1.CnsRegisterVolume{},
		&cnsregistervolumev1alpha1.CnsRegisterVolumeList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsunregistervolumev1alpha1.CnsUnregisterVolume{},
		&cnsunregistervolumev1alpha1.CnsUnregisterVolumeList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsvolumemetadatav1alpha1.CnsVolumeMetadata{},
		&cnsvolumemetadatav1alpha1.CnsVolumeMetadataList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsnodevmattachmentv1alpha1.CnsNodeVmAttachment{},
		&cnsnodevmattachmentv1alpha1.CnsNodeVmAttachmentList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachment{},
		&cnsnodevmbatchattachmentv1alpha1.CnsNodeVmBatchAttachmentList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&storagepolicyv1alpha1.StoragePolicyQuota{},
		&storagepolicyv1alpha1.StoragePolicyQuotaList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&storagequotaperiodicsyncv1alpha1.StorageQuotaPeriodicSync{},
		&storagequotaperiodicsyncv1alpha1.StorageQuotaPeriodicSyncList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&storagepolicyv1alpha1.StoragePolicyUsage{},
		&storagepolicyv1alpha1.StoragePolicyUsageList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersionV2,
		&storagepolicyv1alpha2.StoragePolicyUsage{},
		&storagepolicyv1alpha2.StoragePolicyUsageList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersionV2,
		&storagepolicyv1alpha2.StoragePolicyQuota{},
		&storagepolicyv1alpha2.StoragePolicyQuotaList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersionV2,
		&storagepolicyv1alpha2.StoragePolicyReservation{},
		&storagepolicyv1alpha2.StoragePolicyReservationList{},
	)

	scheme.AddKnownTypes(
		SchemeGroupVersion,
		&metav1.Status{},
	)

	metav1.AddToGroupVersion(
		scheme,
		SchemeGroupVersion,
	)

	metav1.AddToGroupVersion(
		scheme,
		SchemeGroupVersionV2,
	)

	return nil
}
