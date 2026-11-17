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

package k8sorchestrator

import (
	"context"
	"sync"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

func TestPodVMV2SupervisorCapability(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	for _, enabled := range []bool{false, true} {
		WcpCapabilitiesMap = &sync.Map{}
		WcpCapabilitiesMap.Store(common.HostLocalStorageSupport, true)
		if enabled {
			WcpCapabilitiesMap.Store(common.PodVMV2Support, true)
		}
		c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
		if got := c.IsFSSEnabled(context.Background(), common.PodVMV2Support); got != enabled {
			t.Fatalf("Supervisor capability = %t, want %t", got, enabled)
		}
	}
}

// The latched value is what every per-process PodVM v2 decision reads. It must not follow a
// later capability change: IsFSSEnabled re-reads the capabilities CR for a late-enablement
// capability, and a decision that silently flips mid-process would leave the inventory, CSINode
// registration and the restart watcher disagreeing with each other.
func TestPodVMV2EnabledAtStartupIsLatched(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	for _, startup := range []bool{false, true} {
		WcpCapabilitiesMap = &sync.Map{}
		WcpCapabilitiesMap.Store(common.PodVMV2Support, startup)
		ctx := context.Background()
		c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorWorkload}
		c.podVMV2AtStartup = c.IsFSSEnabled(ctx, common.PodVMV2Support)
		if got := c.IsPodVMV2EnabledAtStartup(ctx); got != startup {
			t.Fatalf("latched value = %t, want %t", got, startup)
		}
		// The capability changes after the orchestrator initialized.
		WcpCapabilitiesMap.Store(common.PodVMV2Support, !startup)
		if got := c.IsPodVMV2EnabledAtStartup(ctx); got != startup {
			t.Fatalf("latched value changed to %t after a capability flip, want %t", got, startup)
		}
	}
}
