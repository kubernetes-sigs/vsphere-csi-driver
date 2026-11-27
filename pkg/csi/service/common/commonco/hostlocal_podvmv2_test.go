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
	"testing"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// guestFSSCO models the guest view: IsPVCSIFSSEnabled reads only the PVCSI FSS, while
// IsFSSEnabled ANDs the PVCSI FSS with its associated Supervisor capability.
type guestFSSCO struct {
	COCommonInterface
	pvcsiFSS     map[string]bool
	capabilities map[string]bool
}

func (f *guestFSSCO) IsPVCSIFSSEnabled(_ context.Context, fss string) bool { return f.pvcsiFSS[fss] }

func (f *guestFSSCO) IsFSSEnabled(_ context.Context, fss string) bool {
	return f.pvcsiFSS[fss] && f.capabilities[common.WCPFeatureStateAssociatedWithPVCSI[fss]]
}

func TestHostLocalStorageAndPodVMV2CapabilitiesToWatchInGuest(t *testing.T) {
	for _, tc := range []struct {
		name                string
		hostLocalFSS, v2FSS bool
		hostLocalCap, v2Cap bool
		wantWatch           []string
	}{
		{name: "PVCSI FSSs off", hostLocalCap: true, v2Cap: true},
		{name: "only host-local FSS on", hostLocalFSS: true},
		{name: "only podvm-v2 FSS on", v2FSS: true},
		{name: "both FSSs on, Supervisor not upgraded", hostLocalFSS: true, v2FSS: true,
			wantWatch: []string{common.HostLocalStorageSupport, common.PodVMV2Support}},
		{name: "both FSSs on, only HostLocalStorage enabled", hostLocalFSS: true, v2FSS: true, hostLocalCap: true,
			wantWatch: []string{common.PodVMV2Support}},
		{name: "both FSSs on, only PodVM v2 enabled", hostLocalFSS: true, v2FSS: true, v2Cap: true,
			wantWatch: []string{common.HostLocalStorageSupport}},
		{name: "everything enabled", hostLocalFSS: true, v2FSS: true, hostLocalCap: true, v2Cap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			co := &guestFSSCO{
				pvcsiFSS: map[string]bool{
					common.HostLocalStorageSupportFSS: tc.hostLocalFSS,
					common.PodVMV2SupportFSS:          tc.v2FSS,
				},
				capabilities: map[string]bool{
					common.HostLocalStorageSupport: tc.hostLocalCap,
					common.PodVMV2Support:          tc.v2Cap,
				},
			}
			assert.Equal(t, tc.wantWatch, HostLocalStorageAndPodVMV2CapabilitiesToWatchInGuest(context.Background(), co))
			allEnabled := tc.hostLocalFSS && tc.v2FSS && tc.hostLocalCap && tc.v2Cap
			assert.Equal(t, allEnabled, IsHostLocalStorageAndPodVMV2EnabledInGuest(context.Background(), co))
		})
	}
	assert.Nil(t, HostLocalStorageAndPodVMV2CapabilitiesToWatchInGuest(context.Background(), nil))
}
