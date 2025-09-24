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
