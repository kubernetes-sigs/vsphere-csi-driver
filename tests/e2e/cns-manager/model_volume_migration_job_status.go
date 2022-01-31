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

import "time"

type VolumeMigrationJobStatus struct {
	// Overall phase of the volume migration job.
	OverallPhase string `json:"overallPhase,omitempty"`
	// Time at which the job started processing.
	StartTime time.Time `json:"startTime,omitempty"`
	// Time at which the job completed processing.
	EndTime time.Time `json:"endTime,omitempty"`
	// Array of status of individual volume migration tasks in the job.
	VolumeMigrationTasks []VolumeMigrationTaskStatus `json:"volumeMigrationTasks,omitempty"`
}
