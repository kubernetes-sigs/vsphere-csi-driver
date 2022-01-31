/*
Copyright 2021 VMware, Inc.

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
package cnsutils

type VolumeMigrationJobParameters struct {
	// Datacenter on which source and target datastores reside.
	Datacenter string `json:"datacenter,omitempty"`
	// Name of the source datastore for volume migration.
	SourceDatastore string `json:"sourceDatastore,omitempty"`
	// Name of the target datastore for volume migration.
	TargetDatastore string `json:"targetDatastore,omitempty"`
	// Array of volumes provided to be migrated.
	VolumesToMigrate []string `json:"volumesToMigrate,omitempty"`
}
