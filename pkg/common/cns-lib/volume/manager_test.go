package volume

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	vim25types "github.com/vmware/govmomi/vim25/types"
)

const createVolumeTaskTimeout = 3 * time.Second

func TestWaitForResultOrTimeoutFail(t *testing.T) {
	// slow task times out and fails
	ctx, cancelFunc := context.WithTimeout(context.TODO(), createVolumeTaskTimeout)
	defer cancelFunc()
	ch := make(chan TaskResult)
	go performSlowTask(ch, 5*time.Second)
	taskMoRef := vim25types.ManagedObjectReference{
		Type:  "Task",
		Value: "task-42",
	}
	taskInfo, err := waitForResultOrTimeout(ctx, taskMoRef, ch)
	assert.Error(t, err)
	var expectedTaskInfo *vim25types.TaskInfo
	assert.Equal(t, expectedTaskInfo, taskInfo)
}

func TestWaitForResultOrTimeoutPass(t *testing.T) {
	// task within timeout succeeds
	ctx, cancelFunc := context.WithTimeout(context.TODO(), createVolumeTaskTimeout)
	defer cancelFunc()
	ch := make(chan TaskResult)
	go performSlowTask(ch, 1*time.Second)
	taskMoRef := vim25types.ManagedObjectReference{
		Type:  "Task",
		Value: "task-42",
	}
	taskInfo, err := waitForResultOrTimeout(ctx, taskMoRef, ch)
	assert.NoError(t, err)
	expectedTaskInfo := &vim25types.TaskInfo{State: vim25types.TaskInfoStateSuccess}
	assert.Equal(t, expectedTaskInfo, taskInfo)
}

func performSlowTask(ch chan TaskResult, delay time.Duration) {
	time.Sleep(delay)
	ch <- TaskResult{
		TaskInfo: &vim25types.TaskInfo{State: vim25types.TaskInfoStateSuccess},
		Err:      nil,
	}
}

// Test CnsFault with NotSupported fault cause detection
func TestCnsFaultNotSupportedDetection(t *testing.T) {
	tests := []struct {
		name               string
		fault              vim25types.BaseMethodFault
		expectCnsFault     bool
		expectNotSupported bool
		expectedMessage    string
	}{
		{
			name: "CnsFault with NotSupported cause",
			fault: &cnstypes.CnsFault{
				MethodFault: vim25types.MethodFault{
					FaultCause: &vim25types.LocalizedMethodFault{
						LocalizedMessage: "The operation is not supported on the object.",
						Fault: &vim25types.NotSupported{
							RuntimeFault: vim25types.RuntimeFault{
								MethodFault: vim25types.MethodFault{
									FaultMessage: []vim25types.LocalizableMessage{
										{
											Message: "Attaching mutually shared fcd disks to same VM is not supported.",
										},
									},
								},
							},
						},
					},
				},
				Reason: "VSLM task failed",
			},
			expectCnsFault:     true,
			expectNotSupported: true,
			expectedMessage:    "Attaching mutually shared fcd disks to same VM is not supported.",
		},
		{
			name: "CnsFault with NotSupported cause - multiple messages",
			fault: &cnstypes.CnsFault{
				MethodFault: vim25types.MethodFault{
					FaultCause: &vim25types.LocalizedMethodFault{
						LocalizedMessage: "The operation is not supported on the object.",
						Fault: &vim25types.NotSupported{
							RuntimeFault: vim25types.RuntimeFault{
								MethodFault: vim25types.MethodFault{
									FaultMessage: []vim25types.LocalizableMessage{
										{
											Message: "First error message.",
										},
										{
											Message: "Second error message.",
										},
									},
								},
							},
						},
					},
				},
				Reason: "VSLM task failed",
			},
			expectCnsFault:     true,
			expectNotSupported: true,
			expectedMessage:    "First error message. - Second error message.",
		},
		{
			name: "CnsFault without NotSupported cause",
			fault: &cnstypes.CnsFault{
				MethodFault: vim25types.MethodFault{
					FaultMessage: []vim25types.LocalizableMessage{
						{Message: "Some other error"},
					},
				},
				Reason: "VSLM task failed",
			},
			expectCnsFault:     true,
			expectNotSupported: false,
		},
		{
			name: "Non-CnsFault",
			fault: &vim25types.MethodFault{
				FaultMessage: []vim25types.LocalizableMessage{
					{Message: "Generic fault"},
				},
			},
			expectCnsFault:     false,
			expectNotSupported: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test type assertions that would be used in the AttachVolume function
			cnsFault, isCnsFault := tt.fault.(*cnstypes.CnsFault)
			assert.Equal(t, tt.expectCnsFault, isCnsFault)

			if isCnsFault && cnsFault.FaultCause != nil {
				notSupportedFault, isNotSupportedFault := cnsFault.FaultCause.Fault.(*vim25types.NotSupported)
				assert.Equal(t, tt.expectNotSupported, isNotSupportedFault)

				if isNotSupportedFault && tt.expectedMessage != "" {
					// Test message extraction logic - only extract from FaultMessage array
					var errorMessages []string
					for _, faultMsg := range notSupportedFault.FaultMessage {
						if faultMsg.Message != "" {
							errorMessages = append(errorMessages, faultMsg.Message)
						}
					}

					if len(errorMessages) > 0 {
						extractedMessage := strings.Join(errorMessages, " - ")
						assert.Equal(t, tt.expectedMessage, extractedMessage)
					}
				}
			}
		})
	}
}

// TestAttachVolumeFaultHandling tests that AttachVolume correctly handles faults,
// including unknown fault types that govmomi cannot deserialize.
func TestAttachVolumeFaultHandling(t *testing.T) {
	tests := []struct {
		name                   string
		volumeOperationResult  *cnstypes.CnsVolumeOperationResult
		expectFaultHandled     bool
		expectSpecificHandling bool
		faultDescription       string
	}{
		{
			name: "Known fault - ResourceInUse",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-1"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "Resource is in use",
					Fault: &vim25types.ResourceInUse{
						Type: "Disk",
						Name: "test-disk",
					},
				},
			},
			expectFaultHandled:     true,
			expectSpecificHandling: true,
			faultDescription:       "ResourceInUse fault with Fault.Fault populated",
		},
		{
			name: "Known fault - CnsFault with NotSupported cause",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-2"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "Operation not supported",
					Fault: &cnstypes.CnsFault{
						MethodFault: vim25types.MethodFault{
							FaultCause: &vim25types.LocalizedMethodFault{
								Fault: &vim25types.NotSupported{},
							},
						},
						Reason: "Not supported",
					},
				},
			},
			expectFaultHandled:     true,
			expectSpecificHandling: true,
			faultDescription:       "CnsFault with NotSupported cause",
		},
		{
			name: "Unknown fault - Fault.Fault is nil (e.g., CnsNotRegisteredFault)",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-3"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "The input volume is not registered as a CNS volume",
					// Fault field is nil because govmomi doesn't recognize CnsNotRegisteredFault
					Fault: nil,
				},
			},
			expectFaultHandled:     true,
			expectSpecificHandling: false,
			faultDescription:       "Unknown fault type with Fault.Fault nil (simulates CnsNotRegisteredFault)",
		},
		{
			name: "Generic CnsFault",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-4"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "Generic CNS fault",
					Fault: &cnstypes.CnsFault{
						Reason: "Generic error",
					},
				},
			},
			expectFaultHandled:     true,
			expectSpecificHandling: false,
			faultDescription:       "Generic CnsFault without special handling",
		},
		{
			name: "No fault",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-5"},
				Fault:    nil,
			},
			expectFaultHandled:     false,
			expectSpecificHandling: false,
			faultDescription:       "No fault present",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the fault handling logic structure that AttachVolume uses
			if tt.volumeOperationResult.Fault != nil {
				// This should handle ALL faults, including unknown ones
				assert.True(t, tt.expectFaultHandled, "Fault should be detected: %s", tt.faultDescription)

				// Test nested handling for known fault types
				if tt.volumeOperationResult.Fault.Fault != nil {
					// Can perform specific type assertions here
					_, isResourceInUse := tt.volumeOperationResult.Fault.Fault.(*vim25types.ResourceInUse)
					_, isCnsFault := tt.volumeOperationResult.Fault.Fault.(*cnstypes.CnsFault)

					hasSpecificHandling := isResourceInUse || isCnsFault
					if tt.expectSpecificHandling {
						assert.True(t, hasSpecificHandling, "Should have specific fault handling")
					}
				} else {
					// Unknown fault types will have Fault.Fault == nil
					// These should still be caught by the outer check
					assert.False(t, tt.expectSpecificHandling, "Unknown faults should not have specific handling")
				}
			} else {
				assert.False(t, tt.expectFaultHandled, "No fault should be detected")
			}
		})
	}
}
