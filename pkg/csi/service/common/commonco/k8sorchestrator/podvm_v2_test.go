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

package k8sorchestrator

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

func TestPodVMV2GuestFeatureGating(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	for _, local := range []string{"", "false", "invalid", "true"} {
		for _, capability := range []string{"absent", "disabled", "enabled"} {
			t.Run(local+"/"+capability, func(t *testing.T) {
				WcpCapabilitiesMap = &sync.Map{}
				// Keep the cache populated when the new capability is absent, as on older Supervisors.
				WcpCapabilitiesMap.Store(common.HostLocalStorageSupport, true)
				if capability != "absent" {
					WcpCapabilitiesMap.Store(common.PodVMV2Support, capability == "enabled")
				}
				states := map[string]string{common.HostLocalStorageSupportFSS: "true"}
				if local != "" {
					states[common.PodVMV2SupportFSS] = local
				}
				c := &K8sOrchestrator{
					clusterFlavor: cnstypes.CnsClusterFlavorGuest,
					internalFSS:   FSSConfigMapInfo{featureStates: states, featureStatesLock: &sync.RWMutex{}},
				}
				want := local == "true" && capability == "enabled"
				if got := c.IsFSSEnabled(context.Background(), common.PodVMV2SupportFSS); got != want {
					t.Fatalf("PodVM v2 enabled = %t, want %t", got, want)
				}
				if !c.IsFSSEnabled(context.Background(), common.HostLocalStorageSupportFSS) {
					t.Fatal("HostLocalStorage must remain independent of PodVM v2")
				}
			})
		}
	}
}

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

// Exercise independent local switches and Supervisor capabilities, including
// a newer guest on a Supervisor where either capability is unavailable.
func TestGuestStorageFeatureMatrix(t *testing.T) {
	original := WcpCapabilitiesMap
	t.Cleanup(func() { WcpCapabilitiesMap = original })
	for _, hostLocal := range []bool{false, true} {
		for _, podVMV2 := range []bool{false, true} {
			for _, localHost := range []bool{false, true} {
				for _, localVirtual := range []bool{false, true} {
					name := fmt.Sprintf("hostCapability=%t/virtualCapability=%t/localHost=%t/localVirtual=%t",
						hostLocal, podVMV2, localHost, localVirtual)
					t.Run(name, func(t *testing.T) {
						WcpCapabilitiesMap = &sync.Map{}
						WcpCapabilitiesMap.Store(common.HostLocalStorageSupport, hostLocal)
						WcpCapabilitiesMap.Store(common.PodVMV2Support, podVMV2)
						c := &K8sOrchestrator{clusterFlavor: cnstypes.CnsClusterFlavorGuest,
							internalFSS: FSSConfigMapInfo{featureStatesLock: &sync.RWMutex{}, featureStates: map[string]string{
								common.HostLocalStorageSupportFSS: strconv.FormatBool(localHost),
								common.PodVMV2SupportFSS:          strconv.FormatBool(localVirtual),
							}}}
						for feature, want := range map[string]bool{
							common.HostLocalStorageSupportFSS: hostLocal && localHost,
							common.PodVMV2SupportFSS:          podVMV2 && localVirtual,
						} {
							if got := c.IsFSSEnabled(context.Background(), feature); got != want {
								t.Fatalf("%s = %t, want %t", feature, got, want)
							}
						}
					})
				}
			}
		}
	}
}
