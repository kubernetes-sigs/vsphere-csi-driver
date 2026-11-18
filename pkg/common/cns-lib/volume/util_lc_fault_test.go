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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vim25/xml"

	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	lcCryptoMessage = "Crypto ops are not supported when creating Linked Clone FCD."
	lcErrorPrefix   = "linked clone is not supported (vim.fault.CnsLinkedCloneNotSupportedFault): "
)

// notSupportedCause returns a NotSupported fault, as CNS carries it in the fault cause, with the given messages.
func notSupportedCause(localizedMessage string, messages ...string) *vim25types.LocalizedMethodFault {
	notSupported := &vim25types.NotSupported{}
	for _, m := range messages {
		notSupported.FaultMessage = append(notSupported.FaultMessage, vim25types.LocalizableMessage{Message: m})
	}
	return &vim25types.LocalizedMethodFault{Fault: notSupported, LocalizedMessage: localizedMessage}
}

// linkedCloneNotSupportedResult returns a CreateVolume result carrying a CnsLinkedCloneNotSupportedFault.
func linkedCloneNotSupportedResult(cause *vim25types.LocalizedMethodFault, reason,
	outerMessage string) *cnstypes.CnsVolumeOperationResult {
	return &cnstypes.CnsVolumeOperationResult{
		VolumeId: cnstypes.CnsVolumeId{Id: "test-volume"},
		Fault: &vim25types.LocalizedMethodFault{
			LocalizedMessage: outerMessage,
			Fault: &cnstypes.CnsLinkedCloneNotSupportedFault{
				CnsFault: cnstypes.CnsFault{
					MethodFault: vim25types.MethodFault{FaultCause: cause},
					Reason:      reason,
				},
			},
		},
	}
}

func TestIsCnsLinkedCloneNotSupportedFault(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	tests := []struct {
		name      string
		faultType string
		want      bool
	}{
		{name: "linked clone not supported", faultType: "vim.fault.CnsLinkedCloneNotSupportedFault", want: true},
		{name: "constant", faultType: csifault.VimFaultCnsLinkedCloneNotSupported, want: true},
		{name: "generic CnsFault", faultType: "vim.fault.CnsFault"},
		{name: "generic NotSupported", faultType: "vim25:NotSupported"},
		{name: "already exists", faultType: "vim.fault.CnsVolumeAlreadyExistsFault"},
		{name: "empty", faultType: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsCnsLinkedCloneNotSupportedFault(ctx, tt.faultType))
		})
	}
}

// TestLinkedCloneNotSupportedFaultClassification verifies that the typed fault gets its own fault type, and that it
// is not mistaken for the generic NotSupported fault, which triggers a retry of CreateVolume without a VolumeID.
func TestLinkedCloneNotSupportedFaultClassification(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	typed := linkedCloneNotSupportedResult(notSupportedCause("not supported", lcCryptoMessage),
		"Task failed with fault NotSupported", "Task failed with fault NotSupported")
	assert.Equal(t, csifault.VimFaultCnsLinkedCloneNotSupported, ExtractFaultTypeFromVolumeResponseResult(ctx, typed))
	assert.False(t, IsNotSupportedFault(ctx, typed.Fault),
		"the typed fault must not be classified as vim25:NotSupported")

	// A vCenter that does not return the typed fault returns a generic CnsFault with a NotSupported cause. That must
	// keep being classified as vim25:NotSupported.
	generic := &cnstypes.CnsVolumeOperationResult{
		VolumeId: cnstypes.CnsVolumeId{Id: "test-volume"},
		Fault: &vim25types.LocalizedMethodFault{Fault: &cnstypes.CnsFault{
			MethodFault: vim25types.MethodFault{FaultCause: notSupportedCause("not supported", lcCryptoMessage)},
			Reason:      "Task failed with fault NotSupported",
		}},
	}
	assert.True(t, IsNotSupportedFault(ctx, generic.Fault))
	assert.Equal(t, "vim.fault.CnsFault", ExtractFaultTypeFromVolumeResponseResult(ctx, generic))
	assert.False(t, IsCnsLinkedCloneNotSupportedFault(ctx,
		ExtractFaultTypeFromVolumeResponseResult(ctx, generic)))
}

func TestValidateCreateVolumeResponseFault_LinkedCloneNotSupported(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	tests := []struct {
		name       string
		resp       *cnstypes.CnsVolumeOperationResult
		wantReason string
	}{
		{
			name: "FCD message from the NotSupported cause",
			resp: linkedCloneNotSupportedResult(notSupportedCause("The operation is not supported.", lcCryptoMessage),
				"Task failed with fault NotSupported", "Task failed with fault NotSupported"),
			wantReason: lcCryptoMessage,
		},
		{
			name: "several FCD messages are joined",
			resp: linkedCloneNotSupportedResult(notSupportedCause("", "first message.", "", "second message."),
				"reason", "outer"),
			wantReason: "first message. - second message.",
		},
		{
			name: "cause without messages falls back to its localized message",
			resp: linkedCloneNotSupportedResult(notSupportedCause("The operation is not supported."),
				"reason", "outer"),
			wantReason: "The operation is not supported.",
		},
		{
			name:       "no cause falls back to the CNS reason",
			resp:       linkedCloneNotSupportedResult(nil, "Task failed with fault NotSupported", "outer"),
			wantReason: "Task failed with fault NotSupported",
		},
		{
			name:       "no cause and no reason falls back to the outer message",
			resp:       linkedCloneNotSupportedResult(nil, "", "outer message"),
			wantReason: "outer message",
		},
		{
			name:       "nothing at all",
			resp:       linkedCloneNotSupportedResult(nil, "", ""),
			wantReason: "CNS did not provide a reason",
		},
		{
			name: "whitespace and newlines are collapsed into a single line",
			resp: linkedCloneNotSupportedResult(notSupportedCause("", "line one\n\tline   two\r\nline three"),
				"reason", "outer"),
			wantReason: "line one line two line three",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := validateCreateVolumeResponseFault(ctx, "pvc-1", tt.resp)
			assert.Nil(t, info)
			assert.EqualError(t, err, lcErrorPrefix+tt.wantReason)
			assert.NotContains(t, err.Error(), "\n")
		})
	}
}

func TestValidateCreateVolumeResponseFault_LinkedCloneNotSupportedReasonIsCapped(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())
	// Multi-byte runes: the cap must not cut one in half.
	long := strings.Repeat("é", maxLinkedCloneNotSupportedReasonLen+100)
	resp := linkedCloneNotSupportedResult(notSupportedCause("", long), "reason", "outer")

	_, err := validateCreateVolumeResponseFault(ctx, "pvc-1", resp)

	want := lcErrorPrefix + strings.Repeat("é", maxLinkedCloneNotSupportedReasonLen) + "..."
	assert.EqualError(t, err, want)
}

// TestValidateCreateVolumeResponseFault_OtherFaultsUnchanged verifies that the other faults are handled as before.
func TestValidateCreateVolumeResponseFault_OtherFaultsUnchanged(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	registered := &cnstypes.CnsVolumeOperationResult{Fault: &vim25types.LocalizedMethodFault{
		Fault: &cnstypes.CnsAlreadyRegisteredFault{VolumeId: cnstypes.CnsVolumeId{Id: "vol-registered"}},
	}}
	info, err := validateCreateVolumeResponseFault(ctx, "pvc-1", registered)
	assert.NoError(t, err)
	assert.Equal(t, "vol-registered", info.VolumeID.Id)

	generic := &cnstypes.CnsVolumeOperationResult{Fault: &vim25types.LocalizedMethodFault{
		Fault: &cnstypes.CnsFault{Reason: "something else failed"},
	}}
	info, err = validateCreateVolumeResponseFault(ctx, "pvc-1", generic)
	assert.Nil(t, info)
	assert.ErrorContains(t, err, "failed to create volume with fault")
	assert.NotContains(t, err.Error(), "linked clone is not supported")
}

// linkedCloneNotSupportedFaultXML is a CnsVolumeOperationResult fault as returned by CNS for a linked clone that FCD
// rejects as unsupported. Replace it with a capture from a real vCenter.
const linkedCloneNotSupportedFaultXML = `
<fault xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <fault xsi:type="CnsLinkedCloneNotSupportedFault">
    <faultCause>
      <fault xsi:type="NotSupported">
        <faultMessage>
          <key>com.vmware.vim.vslm.vStorageObjectManager.EncryptionOpOnLCNotSupported</key>
          <message>Crypto ops are not supported when creating Linked Clone FCD.</message>
        </faultMessage>
      </fault>
      <localizedMessage>The operation is not supported on the object.</localizedMessage>
    </faultCause>
    <reason>Task failed with fault NotSupported</reason>
  </fault>
  <localizedMessage>Task failed with fault NotSupported</localizedMessage>
</fault>`

// TestLinkedCloneNotSupportedFaultDecode guards the govmomi dependency: without CnsLinkedCloneNotSupportedFault
// registered, govmomi decodes the fault to a nil Fault, CSI can no longer tell it apart from any other failure, and
// it does not create a full clone when a linked clone is not supported.
func TestLinkedCloneNotSupportedFaultDecode(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	dec := xml.NewDecoder(strings.NewReader(linkedCloneNotSupportedFaultXML))
	dec.TypeFunc = vim25types.TypeFunc()
	var fault vim25types.LocalizedMethodFault
	assert.NoError(t, dec.Decode(&fault))
	resp := &cnstypes.CnsVolumeOperationResult{Fault: &fault}

	assert.Equal(t, csifault.VimFaultCnsLinkedCloneNotSupported, ExtractFaultTypeFromVolumeResponseResult(ctx, resp))
	_, err := validateCreateVolumeResponseFault(ctx, "pvc-1", resp)
	assert.EqualError(t, err, lcErrorPrefix+lcCryptoMessage)
	assert.False(t, IsNotSupportedFault(ctx, resp.Fault))
}
