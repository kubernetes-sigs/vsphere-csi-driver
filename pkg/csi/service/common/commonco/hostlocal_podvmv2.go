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

package commonco

import (
	"context"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// IsHostLocalStorageAndPodVMV2Enabled is the Supervisor (WCP CSI and syncer) check for
// the HostLocalStorage + PodVM v2 host-local behavior. It returns true only when both
// features are enabled:
//   - HostLocalStorage: the supports_host_local_storage capability;
//   - PodVM v2: the supports_podvm_v2 capability, as latched at startup.
//
// When it returns false and HostLocalStorage is enabled, host-local volumes keep the
// HostLocalStorage behavior (zone + hostname PV node affinity).
func IsHostLocalStorageAndPodVMV2Enabled(ctx context.Context, co COCommonInterface) bool {
	if co == nil {
		return false
	}
	return co.IsFSSEnabled(ctx, common.HostLocalStorageSupport) && co.IsPodVMV2EnabledAtStartup(ctx)
}

// IsHostLocalStorageAndPodVMV2EnabledInGuest is the PVCSI check for the
// HostLocalStorage + PodVM v2 host-local behavior. It returns true only when all four
// switches are enabled:
//   - HostLocalStorage: the host-local-storage-support PVCSI FSS and the
//     supports_host_local_storage Supervisor capability;
//   - PodVM v2: the podvm-v2-support PVCSI FSS and the supports_podvm_v2 Supervisor
//     capability.
//
// IsFSSEnabled ANDs each PVCSI FSS with its Supervisor capability through
// common.WCPFeatureStateAssociatedWithPVCSI. When it returns false, PVCSI keeps the
// HostLocalStorage behavior gated by IsPVCSIFSSEnabled(HostLocalStorageSupportFSS).
func IsHostLocalStorageAndPodVMV2EnabledInGuest(ctx context.Context, co COCommonInterface) bool {
	if co == nil {
		return false
	}
	return co.IsFSSEnabled(ctx, common.HostLocalStorageSupportFSS) &&
		co.IsFSSEnabled(ctx, common.PodVMV2SupportFSS)
}
