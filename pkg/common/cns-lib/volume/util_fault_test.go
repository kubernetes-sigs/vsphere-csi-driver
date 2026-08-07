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
	"context"
	"testing"

	"github.com/vmware/govmomi/vim25/types"
)

// TestExtractDetailedFaultMessageNilFault verifies that a nil LocalizedMethodFault
// returns an empty string without panicking.
func TestExtractDetailedFaultMessageNilFault(t *testing.T) {
	got := ExtractDetailedFaultMessage(context.Background(), nil)
	if got != "" {
		t.Errorf("expected empty string for nil fault, got %q", got)
	}
}

// TestExtractDetailedFaultMessageNoNestedFault verifies that when Fault.Fault is nil,
// only the top-level LocalizedMessage is returned.
func TestExtractDetailedFaultMessageNoNestedFault(t *testing.T) {
	fault := &types.LocalizedMethodFault{
		LocalizedMessage: "generic failure",
	}
	got := ExtractDetailedFaultMessage(context.Background(), fault)
	want := "generic failure"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// TestExtractDetailedFaultMessageWithFaultMessages verifies that FaultMessage entries on
// the nested MethodFault are appended to the generic LocalizedMessage. This mirrors the
// real vim.fault.InvalidDeviceSpec case where LocalizedMessage is a generic templated
// string ("Invalid configuration for device '2'.") and the actual root cause is only
// present in FaultMessage.
func TestExtractDetailedFaultMessageWithFaultMessages(t *testing.T) {
	fault := &types.LocalizedMethodFault{
		LocalizedMessage: "Invalid configuration for device '2'.",
		Fault: &types.InvalidDeviceSpec{
			InvalidVmConfig: types.InvalidVmConfig{
				VmConfigFault: types.VmConfigFault{
					VimFault: types.VimFault{
						MethodFault: types.MethodFault{
							FaultMessage: []types.LocalizableMessage{
								{Message: "Provided backing file is not accessible from host."},
								{Message: "Device: VirtualDisk."},
							},
						},
					},
				},
				Property: "deviceChange[2].device.backing.fileName",
			},
			DeviceIndex: 2,
		},
	}

	got := ExtractDetailedFaultMessage(context.Background(), fault)
	want := "Invalid configuration for device '2'. Details: " +
		"Provided backing file is not accessible from host.; Device: VirtualDisk."
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// TestExtractDetailedFaultMessageWithFaultCause verifies that a non-empty FaultCause
// LocalizedMessage is appended when the nested MethodFault has no FaultMessage entries.
func TestExtractDetailedFaultMessageWithFaultCause(t *testing.T) {
	fault := &types.LocalizedMethodFault{
		LocalizedMessage: "resource is in use",
		Fault: &types.ResourceInUse{
			VimFault: types.VimFault{
				MethodFault: types.MethodFault{
					FaultCause: &types.LocalizedMethodFault{
						LocalizedMessage: "disk is attached to another VM",
					},
				},
			},
			Type: "VirtualDisk",
			Name: "disk-1",
		},
	}

	got := ExtractDetailedFaultMessage(context.Background(), fault)
	want := "resource is in use Details: disk is attached to another VM"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// TestExtractDetailedFaultMessageWithFaultMessagesAndFaultCause verifies that both
// FaultMessage entries and a FaultCause LocalizedMessage are appended together, in that
// order, when both are present on the nested MethodFault.
func TestExtractDetailedFaultMessageWithFaultMessagesAndFaultCause(t *testing.T) {
	fault := &types.LocalizedMethodFault{
		LocalizedMessage: "generic failure",
		Fault: &types.ResourceInUse{
			VimFault: types.VimFault{
				MethodFault: types.MethodFault{
					FaultMessage: []types.LocalizableMessage{
						{Message: "detail one"},
					},
					FaultCause: &types.LocalizedMethodFault{
						LocalizedMessage: "underlying cause",
					},
				},
			},
		},
	}

	got := ExtractDetailedFaultMessage(context.Background(), fault)
	want := "generic failure Details: detail one; underlying cause"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}

// TestExtractDetailedFaultMessageIgnoresEmptyFaultMessageAndCause verifies that empty
// FaultMessage entries and an empty FaultCause LocalizedMessage do not produce a
// "Details:" suffix, and a FaultCause with an empty LocalizedMessage is skipped.
func TestExtractDetailedFaultMessageIgnoresEmptyFaultMessageAndCause(t *testing.T) {
	fault := &types.LocalizedMethodFault{
		LocalizedMessage: "generic failure",
		Fault: &types.ResourceInUse{
			VimFault: types.VimFault{
				MethodFault: types.MethodFault{
					FaultMessage: []types.LocalizableMessage{
						{Message: ""},
					},
					FaultCause: &types.LocalizedMethodFault{
						LocalizedMessage: "",
					},
				},
			},
		},
	}

	got := ExtractDetailedFaultMessage(context.Background(), fault)
	want := "generic failure"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
}
