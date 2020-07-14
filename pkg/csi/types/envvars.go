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

package types

const (
	// EnvClusterFlavor is the k8s cluster type on which CSI Driver is being deployed
	EnvClusterFlavor = "CLUSTER_FLAVOR"

	// EnvSupervisorClientQPS  is the QPS for all clients to the supervisor cluster API server
	EnvSupervisorClientQPS = "SUPERVISOR_CLIENT_QPS"

	// EnvSupervisorClientBurst  is the Burst for all clients to the supervisor cluster API server
	EnvSupervisorClientBurst = "SUPERVISOR_CLIENT_BURST"

	// EnvInClusterClientQPS is the QPS for all clients to the API server
	EnvInClusterClientQPS = "INCLUSTER_CLIENT_QPS"

	// EnvInClusterClientBurst is the Burst for all clients to the API server
	EnvInClusterClientBurst = "INCLUSTER_CLIENT_BURST"
)
