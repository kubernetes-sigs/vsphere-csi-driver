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

package syncer

import (
	"context"
	"fmt"
	"testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
)

// CSINode registration used to depend on PodVMOnStretchedSupervisor alone, so a Supervisor with
// only the PodVM v2 capability created no VirtualNode CSINodes at all.
func TestCSINodeRegistrationRequired(t *testing.T) {
	ctx := context.Background()
	for _, stretched := range []bool{false, true} {
		for _, podVMV2 := range []bool{false, true} {
			t.Run(fmt.Sprintf("stretched=%t/podVMV2=%t", stretched, podVMV2), func(t *testing.T) {
				co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
				if err != nil {
					t.Fatal(err)
				}
				if podVMV2 {
					if err := co.EnableFSS(ctx, common.PodVMV2Support); err != nil {
						t.Fatal(err)
					}
				}
				want := stretched || podVMV2
				if got := csiNodeRegistrationRequired(ctx, stretched, co); got != want {
					t.Fatalf("csiNodeRegistrationRequired = %t, want %t", got, want)
				}
			})
		}
	}
}
