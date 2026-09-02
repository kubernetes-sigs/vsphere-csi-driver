/*
Copyright 2026 The Kubernetes Authors.

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

// Package nfsdriver vendors the ControllerServer/NodeServer implementation from the
// upstream kubernetes-csi/csi-driver-nfs project (https://github.com/kubernetes-csi/csi-driver-nfs,
// package pkg/nfs, Apache License 2.0), largely unmodified, for in-process reuse by the
// guest-cluster (pvCSI) flavor of this driver (pkg/csi/service/wcpguest).
//
// Unlike upstream, this package is never run as its own gRPC server: pkg/csi/service/wcpguest
// and pkg/csi/service/node.go instantiate a Driver directly and call its ControllerServer /
// NodeServer methods as plain Go function calls when a StorageClass's "server" parameter
// selects this backend, entirely bypassing the Supervisor cluster. Accordingly, the upstream
// gRPC bootstrap (server.go), the identity server (identityserver.go — this driver already has
// its own, in pkg/csi/service/identity.go), Windows chmod support, and the fake mounter test
// helper were dropped; everything else (controllerserver.go, nodeserver.go, utils.go, cache.go,
// tar.go, nfs.go, chmod_unix.go, version.go) is copied over intact, aside from renaming the
// package and DefaultDriverName (to "nfs.csi.vsphere.vmware.com").
//
// Deviations from vendored upstream, each marked inline with a "DEVIATION FROM VENDORED
// UPSTREAM" comment:
//   - controllerserver.go internalMount pins mountPermissions to 0 for the controller's
//     internal scratch mount of the root export, so it is never chmod'd. Required because
//     glue.go sets a non-zero driver-level mountPermissions default, which upstream leaves at
//     0; without this, the export root gets chmod'd and the mount fails under root_squash.
package nfsdriver
