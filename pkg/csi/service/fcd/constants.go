/*
Copyright 2018 The Kubernetes Authors.

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

package fcd

const (
	// MbInBytes is the number of bytes in one mebibyte.
	// TODO(codenrhoden) Should this be MiBInBytes?
	MbInBytes = int64(1024 * 1024)

	// GbInBytes is the number of bytes in one gibibyte.
	// TODO(codenrhoden) Should this be GiBInBytes?
	GbInBytes = int64(1024 * 1024 * 1024)

	// DefaultGbDiskSize is the default disk size in gibibytes.
	// TODO(codenrhoden) Should this be DefaultGiBDiskSize?
	DefaultGbDiskSize = int64(10)

	// FirstClassDiskTypeString in string form
	FirstClassDiskTypeString = "First Class Disk"

	//
	// Kubernetes volume labels
	//

	// Please see https://github.com/kubernetes/cloud-provider-vsphere/issues/134
	// for an example of the following volume labels.

	// AttributeFirstClassDiskType is a Kubernetes volume label.
	AttributeFirstClassDiskType = "type"
	// AttributeFirstClassDiskName is a Kubernetes volume label.
	AttributeFirstClassDiskName = "name"
	// AttributeFirstClassDiskParentType is a Kubernetes volume label.
	AttributeFirstClassDiskParentType = "parent_type"
	// AttributeFirstClassDiskParentName is a Kubernetes volume label.
	AttributeFirstClassDiskParentName = "parent_name"
	// AttributeFirstClassDiskOwningDatastore is a Kubernetes volume label.
	AttributeFirstClassDiskOwningDatastore = "owning_datastore"
	// AttributeFirstClassDiskVcenter is a Kubernetes volume label.
	AttributeFirstClassDiskVcenter = "vcenter"
	// AttributeFirstClassDiskDatacenter is a Kubernetes volume label.
	AttributeFirstClassDiskDatacenter = "datacenter"
	// AttributeFirstClassDiskPage83Data is a Kubernetes volume label.
	AttributeFirstClassDiskPage83Data = "page83data"
	// AttributeFirstClassDiskZone is a Kubernetes volume label.
	AttributeFirstClassDiskZone = "zone"
	// AttributeFirstClassDiskRegion is a Kubernetes volume label.
	AttributeFirstClassDiskRegion = "region"

	//
	// Kubernetes node/persistent volume labels
	//

	// LabelZoneFailureDomain is a label placed on nodes and persistent
	// volumes by the Kubelet with information obtained from the cloud
	// provider. For more information please see
	// https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints/#failure-domain-beta-kubernetes-io-region.
	LabelZoneFailureDomain = "failure-domain.beta.kubernetes.io/zone"

	// LabelZoneRegion is documented with LabelZoneFailureDomain.
	LabelZoneRegion = "failure-domain.beta.kubernetes.io/region"
)
