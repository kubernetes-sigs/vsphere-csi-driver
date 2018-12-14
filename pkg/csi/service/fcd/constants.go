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
	// MbInBytes... just like it sounds but as int64
	MbInBytes = int64(1024 * 1024)

	// GbInBytes... just like it sounds but as int64
	GbInBytes = int64(1024 * 1024 * 1024)

	// DefaultGbDiskSize... just like it sounds but as int64
	DefaultGbDiskSize = int64(10)

	// FirstClassDiskTypeString in string form
	FirstClassDiskTypeString = "First Class Disk"

	// FCD attribute labels
	AttributeFirstClassDiskType            = "type"
	AttributeFirstClassDiskName            = "name"
	AttributeFirstClassDiskParentType      = "parent_type"
	AttributeFirstClassDiskParentName      = "parent_name"
	AttributeFirstClassDiskOwningDatastore = "owning_datastore"
	AttributeFirstClassDiskVcenter         = "vcenter"
	AttributeFirstClassDiskDatacenter      = "datacenter"
	AttributeFirstClassDiskPage83Data      = "page83data"
)
