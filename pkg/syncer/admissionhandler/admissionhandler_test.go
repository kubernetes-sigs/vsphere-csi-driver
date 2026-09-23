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

package admissionhandler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cnstypes "github.com/vmware/govmomi/cns/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// TestInitWorkloadFSSFlag covers initWorkloadFSSFlag, the helper StartWebhookServer uses to
// initialize each Workload-flavor feature flag from the live capability/FSS state. Add a new
// row here (rather than a bespoke test) when wiring up a new Workload capability.
func TestInitWorkloadFSSFlag(t *testing.T) {
	tests := []struct {
		name                 string
		capability           string
		fssEnabled           bool
		expectFlag           bool
		expectLateEnablement bool
	}{
		{
			name:                 "VAC policy mutability enabled",
			capability:           common.VMPVCStoragePolicyMutability,
			fssEnabled:           true,
			expectFlag:           true,
			expectLateEnablement: false,
		},
		{
			name:                 "VAC policy mutability disabled starts late-enablement watcher",
			capability:           common.VMPVCStoragePolicyMutability,
			fssEnabled:           false,
			expectFlag:           false,
			expectLateEnablement: true,
		},
		{
			name:                 "shared disk support enabled",
			capability:           common.SharedDiskFss,
			fssEnabled:           true,
			expectFlag:           true,
			expectLateEnablement: false,
		},
		{
			name:                 "linked clone support disabled starts late-enablement watcher",
			capability:           common.LinkedCloneSupport,
			fssEnabled:           false,
			expectFlag:           false,
			expectLateEnablement: true,
		},
		{
			name:                 "vSAN file volume service enabled",
			capability:           common.VsanFileVolumeService,
			fssEnabled:           true,
			expectFlag:           true,
			expectLateEnablement: false,
		},
		{
			name:                 "vSAN file volume service disabled starts late-enablement watcher",
			capability:           common.VsanFileVolumeService,
			fssEnabled:           false,
			expectFlag:           false,
			expectLateEnablement: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Setup
			ctx := context.Background()
			mockCO := new(MockCOCommonInterface)
			mockCO.On("IsFSSEnabled", ctx, test.capability).Return(test.fssEnabled)

			lateEnablementCalled := make(chan struct{}, 1)
			mockCO.On("HandleLateEnablementOfCapability", ctx, cnstypes.CnsClusterFlavorWorkload,
				test.capability, "", "").Run(func(_ mock.Arguments) {
				lateEnablementCalled <- struct{}{}
			}).Return()

			// Execute
			var flag bool
			initWorkloadFSSFlag(ctx, mockCO, test.capability, &flag)

			// Assert
			assert.Equal(t, test.expectFlag, flag)

			select {
			case <-lateEnablementCalled:
				assert.True(t, test.expectLateEnablement,
					"HandleLateEnablementOfCapability was called but not expected")
			case <-time.After(200 * time.Millisecond):
				assert.False(t, test.expectLateEnablement,
					"HandleLateEnablementOfCapability was expected but not called")
			}
		})
	}
}
