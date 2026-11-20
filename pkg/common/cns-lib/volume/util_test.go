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

package volume

import (
	"errors"
	"fmt"
	"testing"

	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/vim25/types"
)

func TestGetTerminalFaultCause(t *testing.T) {
	newFault := func(keys ...string) *types.LocalizedMethodFault {
		f := &types.InvalidDeviceSpec{}
		for _, k := range keys {
			f.FaultMessage = append(f.FaultMessage, types.LocalizableMessage{Key: k})
		}
		return &types.LocalizedMethodFault{Fault: f}
	}
	tests := []struct {
		name  string
		fault *types.LocalizedMethodFault
		want  bool
	}{
		{"nil fault", nil, false},
		{"nil inner fault", &types.LocalizedMethodFault{}, false},
		{"InvalidDeviceSpec without message", newFault(), false},
		{"InvalidDeviceSpec for another reason", newFault("msg.disk.invalidUnitNumber"), false},
		{"disk part of a VM snapshot", newFault("msg.disk.removeDiskAfterVMSnapshot"), true},
		{"terminal key not first", newFault("msg.other", "removeDiskAfterVMSnapshot"), true},
		// Structure of the fault returned by CNS on a testbed for a detach from a VM with snapshots.
		{"terminal fault nested in CnsFault", &types.LocalizedMethodFault{
			Fault: &cnstypes.CnsFault{
				MethodFault: types.MethodFault{
					FaultCause: newFault("com.vmware.vim.vm.error.removeDiskAfterVMSnapshot"),
				},
				Reason: "Task failed with fault vim.fault.InvalidDeviceSpec",
			},
		}, true},
		{"non-terminal fault nested in CnsFault", &types.LocalizedMethodFault{
			Fault: &cnstypes.CnsFault{
				MethodFault: types.MethodFault{FaultCause: newFault("msg.disk.invalidUnitNumber")},
			},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cause, got := GetTerminalFaultCause(tt.fault)
			if got != tt.want {
				t.Fatalf("GetTerminalFaultCause() = %v, want %v", got, tt.want)
			}
			if got && cause == "" {
				t.Fatal("expected a cause for a terminal fault")
			}
		})
	}
}

func TestIsTerminalError(t *testing.T) {
	terminal := &TerminalError{Cause: "cause", Err: errors.New("detach failed")}
	transient := errors.New("transient")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"transient", transient, false},
		{"terminal", terminal, true},
		{"wrapped terminal", fmt.Errorf("reconcile failed: %w", terminal), true},
		{"joined terminal and nil", errors.Join(nil, terminal), true},
		{"joined terminal errors", errors.Join(terminal, terminal), true},
		{"joined terminal and transient", errors.Join(terminal, transient), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTerminalError(tt.err); got != tt.want {
				t.Fatalf("IsTerminalError() = %v, want %v", got, tt.want)
			}
		})
	}
}
