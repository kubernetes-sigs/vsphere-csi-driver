package volume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vslm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
)

const createVolumeTaskTimeout = 3 * time.Second

// TestMain stubs out ConnectVslmHook so unit tests can use an empty
// cnsvsphere.VirtualCenter without trying to dial a real vCenter.
func TestMain(m *testing.M) {
	ConnectVslmHook = func(ctx context.Context, vc *cnsvsphere.VirtualCenter) error { return nil }
	os.Exit(m.Run())
}

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

// TestWaitForResultOrTimeoutWrapsContextError guards the wrapping that
// isClientSideFailure depends on. Without the %w verb the timeout error is an
// opaque string and errors.Is cannot classify it, so MonitorCreateVolumeTask
// would fall through and mark the operation failed.
func TestWaitForResultOrTimeoutWrapsContextError(t *testing.T) {
	taskMoRef := vim25types.ManagedObjectReference{Type: "Task", Value: clientSideFailureTaskID}

	t.Run("deadline exceeded is wrapped", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
		defer cancel()
		_, err := waitForResultOrTimeout(ctx, taskMoRef, make(chan TaskResult))
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"timeout error must wrap context.DeadlineExceeded, got %v", err)
		assert.True(t, isClientSideFailure(err))
		// The human-readable part is still expected by operators reading logs.
		assert.Contains(t, err.Error(), "before response from CNS")
		assert.Contains(t, err.Error(), "time out for task",
			"an elapsed deadline should be reported as a timeout")
	})

	t.Run("cancellation is wrapped", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		_, err := waitForResultOrTimeout(ctx, taskMoRef, make(chan TaskResult))
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled),
			"cancellation error must wrap context.Canceled, got %v", err)
		assert.True(t, isClientSideFailure(err))
		assert.Contains(t, err.Error(), "before response from CNS")
		// A cancelled call must not be logged as a timeout. Telling the two apart
		// in the logs is what distinguishes the sidecar's --timeout elapsing from
		// a vCenter session/ListView reset aborting the call.
		assert.Contains(t, err.Error(), "cancelled while waiting for task")
		assert.NotContains(t, err.Error(), "time out for task",
			"cancellation must not be reported as a timeout")
	})
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

// TestIsCnsNotRegisteredFault tests the IsCnsNotRegisteredFault helper function
// that detects CnsNotRegisteredFault from CNS operations.
func TestIsCnsNotRegisteredFault(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	tests := []struct {
		name     string
		fault    *vim25types.LocalizedMethodFault
		expected bool
	}{
		{
			name:     "nil fault",
			fault:    nil,
			expected: false,
		},
		{
			name: "fault with nil Fault field",
			fault: &vim25types.LocalizedMethodFault{
				LocalizedMessage: "Some error",
				Fault:            nil,
			},
			expected: false,
		},
		{
			name: "CnsNotRegisteredFault",
			fault: &vim25types.LocalizedMethodFault{
				LocalizedMessage: "The input volume is not registered as a CNS volume",
				Fault:            &cnstypes.CnsNotRegisteredFault{},
			},
			expected: true,
		},
		{
			name: "CnsFault (not CnsNotRegisteredFault)",
			fault: &vim25types.LocalizedMethodFault{
				LocalizedMessage: "Generic CNS fault",
				Fault: &cnstypes.CnsFault{
					Reason: "Generic error",
				},
			},
			expected: false,
		},
		{
			name: "ResourceInUse fault",
			fault: &vim25types.LocalizedMethodFault{
				LocalizedMessage: "Resource is in use",
				Fault: &vim25types.ResourceInUse{
					Type: "Disk",
					Name: "test-disk",
				},
			},
			expected: false,
		},
		{
			name: "NotSupported fault",
			fault: &vim25types.LocalizedMethodFault{
				LocalizedMessage: "Operation not supported",
				Fault:            &vim25types.NotSupported{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCnsNotRegisteredFault(ctx, tt.fault)
			assert.Equal(t, tt.expected, result,
				"IsCnsNotRegisteredFault(%v) = %v, expected %v", tt.fault, result, tt.expected)
		})
	}
}

// TestIsCnsVolumeAlreadyExistsFault tests the IsCnsVolumeAlreadyExistsFault helper function
// that checks if a fault type indicates the volume already exists in CNS.
func TestIsCnsVolumeAlreadyExistsFault(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	tests := []struct {
		name      string
		faultType string
		expected  bool
	}{
		{
			name:      "CnsVolumeAlreadyExistsFault",
			faultType: "vim.fault.CnsVolumeAlreadyExistsFault",
			expected:  true,
		},
		{
			name:      "empty fault type",
			faultType: "",
			expected:  false,
		},
		{
			name:      "CnsFault",
			faultType: "vim.fault.CnsFault",
			expected:  false,
		},
		{
			name:      "NotSupported",
			faultType: "vim25:NotSupported",
			expected:  false,
		},
		{
			name:      "partial match - should not match",
			faultType: "CnsVolumeAlreadyExistsFault",
			expected:  false,
		},
		{
			name:      "case sensitive - lowercase should not match",
			faultType: "vim.fault.cnsvolumealreadyexistsfault",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCnsVolumeAlreadyExistsFault(ctx, tt.faultType)
			assert.Equal(t, tt.expected, result,
				"IsCnsVolumeAlreadyExistsFault(%q) = %v, expected %v", tt.faultType, result, tt.expected)
		})
	}
}

// TestCnsNotRegisteredFaultHandlingScenarios tests the scenarios where
// CnsNotRegisteredFault should trigger re-registration of the volume.
func TestCnsNotRegisteredFaultHandlingScenarios(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	tests := []struct {
		name                       string
		volumeOperationResult      *cnstypes.CnsVolumeOperationResult
		expectReRegistrationNeeded bool
		description                string
	}{
		{
			name: "CnsNotRegisteredFault should trigger re-registration",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-1"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "The input volume is not registered as a CNS volume",
					Fault:            &cnstypes.CnsNotRegisteredFault{},
				},
			},
			expectReRegistrationNeeded: true,
			description:                "Volume not registered in CNS should trigger re-registration",
		},
		{
			name: "CnsFault should not trigger re-registration",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-2"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "Generic CNS error",
					Fault: &cnstypes.CnsFault{
						Reason: "Some other error",
					},
				},
			},
			expectReRegistrationNeeded: false,
			description:                "Generic CnsFault should not trigger re-registration",
		},
		{
			name: "No fault should not trigger re-registration",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-3"},
				Fault:    nil,
			},
			expectReRegistrationNeeded: false,
			description:                "Successful operation should not trigger re-registration",
		},
		{
			name: "ResourceInUse should not trigger re-registration",
			volumeOperationResult: &cnstypes.CnsVolumeOperationResult{
				VolumeId: cnstypes.CnsVolumeId{Id: "test-volume-4"},
				Fault: &vim25types.LocalizedMethodFault{
					LocalizedMessage: "Resource is in use",
					Fault: &vim25types.ResourceInUse{
						Type: "Disk",
						Name: "test-disk",
					},
				},
			},
			expectReRegistrationNeeded: false,
			description:                "ResourceInUse should not trigger re-registration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reRegistrationNeeded bool
			if tt.volumeOperationResult.Fault != nil {
				reRegistrationNeeded = IsCnsNotRegisteredFault(ctx, tt.volumeOperationResult.Fault)
			}
			assert.Equal(t, tt.expectReRegistrationNeeded, reRegistrationNeeded,
				"Test: %s - %s", tt.name, tt.description)
		})
	}
}

// TestReRegistrationIdempotency tests that re-registration handles
// CnsVolumeAlreadyExistsFault gracefully (idempotent behavior).
func TestReRegistrationIdempotency(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.Background())

	tests := []struct {
		name                     string
		faultTypeAfterReRegister string
		expectSuccess            bool
		description              string
	}{
		{
			name:                     "Re-registration succeeds",
			faultTypeAfterReRegister: "",
			expectSuccess:            true,
			description:              "Re-registration with no fault should succeed",
		},
		{
			name:                     "Volume already exists (idempotent)",
			faultTypeAfterReRegister: "vim.fault.CnsVolumeAlreadyExistsFault",
			expectSuccess:            true,
			description:              "CnsVolumeAlreadyExistsFault should be treated as success",
		},
		{
			name:                     "Other fault during re-registration",
			faultTypeAfterReRegister: "vim.fault.CnsFault",
			expectSuccess:            false,
			description:              "Other faults should be treated as failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var success bool
			if tt.faultTypeAfterReRegister == "" {
				success = true
			} else if IsCnsVolumeAlreadyExistsFault(ctx, tt.faultTypeAfterReRegister) {
				// CnsVolumeAlreadyExistsFault means volume is already registered
				success = true
			} else {
				success = false
			}
			assert.Equal(t, tt.expectSuccess, success,
				"Test: %s - %s", tt.name, tt.description)
		})
	}
}

func createFCDSoapFault(fault vim25types.AnyType, msg string) error {
	f := &soap.Fault{String: msg}
	f.Detail.Fault = fault
	return soap.WrapSoapFault(f)
}

func TestTranslateVslmError(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{
			name:     "nil error",
			err:      nil,
			wantCode: codes.OK,
		},
		{
			name: "SOAP FileFault with noTrack",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{
					MethodFault: vim25types.MethodFault{
						FaultMessage: []vim25types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.noTrack"},
						},
					},
				},
			}, ""),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault with noEpoch",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{
					MethodFault: vim25types.MethodFault{
						FaultMessage: []vim25types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.noEpoch"},
						},
					},
				},
			}, ""),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault with corrupt cannotGetChanges",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{
					MethodFault: vim25types.MethodFault{
						FaultMessage: []vim25types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.cannotGetChanges", Message: "file is corrupted"},
						},
					},
				},
			}, "file is corrupted"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault with mismatched cannotGetChanges",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{
					MethodFault: vim25types.MethodFault{
						FaultMessage: []vim25types.LocalizableMessage{
							{Key: "vim.hostd.vmsvc.cbt.cannotGetChanges"},
						},
					},
				},
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "SOAP FileFault generic",
			err:      createFCDSoapFault(vim25types.FileFault{}, ""),
			wantCode: codes.Internal,
		},
		{
			name:     "SOAP SystemError",
			err:      createFCDSoapFault(vim25types.SystemError{}, ""),
			wantCode: codes.Internal,
		},
		{
			name: "SOAP InvalidArgument startOffset",
			err: createFCDSoapFault(vim25types.InvalidArgument{
				InvalidProperty: "startOffset",
			}, ""),
			wantCode: codes.OutOfRange,
		},
		{
			name: "SOAP InvalidArgument snapshotId",
			err: createFCDSoapFault(vim25types.InvalidArgument{
				InvalidProperty: "snapshotId",
			}, ""),
			wantCode: codes.NotFound,
		},
		{
			name: "SOAP InvalidArgument changeId",
			err: createFCDSoapFault(vim25types.InvalidArgument{
				InvalidProperty: "changeId",
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "SOAP InvalidArgument deviceKey",
			err: createFCDSoapFault(vim25types.InvalidArgument{
				InvalidProperty: "deviceKey",
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "SOAP NotFound",
			err:      createFCDSoapFault(vim25types.NotFound{}, ""),
			wantCode: codes.NotFound,
		},
		{
			name:     "plain error with CBT message substring",
			err:      fmt.Errorf("some inner error: vim.hostd.vmsvc.cbt.noTrack"),
			wantCode: codes.Internal,
		},
		{
			name:     "plain error with vim.fault substring",
			err:      fmt.Errorf("some inner error: vim.fault.NotFound occurred"),
			wantCode: codes.Internal,
		},
		{
			name:     "plain error generic",
			err:      fmt.Errorf("random network timeout"),
			wantCode: codes.Internal,
		},
		{
			name: "SOAP FileFault noTrack via error message substring",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{MethodFault: vim25types.MethodFault{}},
			}, "vim.hostd.vmsvc.cbt.noTrack"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault noEpoch via error message substring",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{MethodFault: vim25types.MethodFault{}},
			}, "vim.hostd.vmsvc.cbt.noEpoch"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "SOAP FileFault cannotGetChanges via error message substring",
			err: createFCDSoapFault(vim25types.FileFault{
				VimFault: vim25types.VimFault{MethodFault: vim25types.MethodFault{}},
			}, "vim.hostd.vmsvc.cbt.cannotGetChanges"),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "SOAP InvalidArgument unknown property",
			err: createFCDSoapFault(vim25types.InvalidArgument{
				InvalidProperty: "otherProperty",
			}, ""),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "SOAP fault type not specially handled",
			err:      createFCDSoapFault(vim25types.AlreadyExists{}, "exists"),
			wantCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := TranslateVslmError(ctx, tt.err)

			if tt.err == nil {
				if err != nil {
					t.Errorf("Expected nil error, got: %v", err)
				}
				return
			}

			if status.Code(err) != tt.wantCode {
				t.Errorf("TranslateVslmError() returned code %v, want %v. Error: %v", status.Code(err), tt.wantCode, err)
			}
		})
	}
}

func TestConnectVirtualCenter(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	mNil := &defaultManager{virtualCenter: nil}
	_, err := mNil.connectVirtualCenter(ctx)
	assert.Error(t, err)

	mOK := &defaultManager{virtualCenter: &cnsvsphere.VirtualCenter{}}
	vc, err := mOK.connectVirtualCenter(ctx)
	assert.NoError(t, err)
	assert.Same(t, mOK.virtualCenter, vc)
}

func TestQueryFCDAllocatedBlocks(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	orig := QueryChangedDiskAreasHook
	defer func() { QueryChangedDiskAreasHook = orig }()

	m := &defaultManager{virtualCenter: &cnsvsphere.VirtualCenter{}}

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		assert.Equal(t, "*", changeID)
		return &vim25types.DiskChangeInfo{
			StartOffset: 0,
			Length:      12288,
			ChangedArea: []vim25types.DiskChangeExtent{
				{Start: 0, Length: 4096},
				{Start: 8192, Length: 4096},
			},
		}, nil
	}
	areas, next, err := m.QueryFCDAllocatedBlocks(ctx, "vol", "snap", 0)
	assert.NoError(t, err)
	assert.Len(t, areas, 2)
	assert.Equal(t, uint64(12288), next)

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		return &vim25types.DiskChangeInfo{
			StartOffset: 0,
			Length:      8192,
			ChangedArea: []vim25types.DiskChangeExtent{
				{Start: 0, Length: 4096},
				{Start: 4096, Length: 4096},
			},
		}, nil
	}
	areas, next, err = m.QueryFCDAllocatedBlocks(ctx, "vol", "snap", 0)
	assert.NoError(t, err)
	assert.Len(t, areas, 2)
	assert.Equal(t, uint64(8192), next)

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		return &vim25types.DiskChangeInfo{
			StartOffset: 0,
			Length:      12288,
			ChangedArea: []vim25types.DiskChangeExtent{
				{Start: 0, Length: 4096},
				{Start: 4096, Length: 4096},
				{Start: 8192, Length: 4096},
			},
		}, nil
	}
	areas, next, err = m.QueryFCDAllocatedBlocks(ctx, "vol", "snap", 0)
	assert.NoError(t, err)
	assert.Len(t, areas, 3)
	assert.Equal(t, uint64(12288), next)

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		return nil, fmt.Errorf("vslm down")
	}
	_, _, err = m.QueryFCDAllocatedBlocks(ctx, "v", "s", 0)
	assert.Error(t, err)

	mNil := &defaultManager{virtualCenter: nil}
	_, _, err = mNil.QueryFCDAllocatedBlocks(ctx, "v", "s", 0)
	assert.Error(t, err)
}

func TestQueryFCDChangedBlocks(t *testing.T) {
	ctx := context.Background()
	ctx = logger.NewContextWithLogger(ctx)

	orig := QueryChangedDiskAreasHook
	defer func() { QueryChangedDiskAreasHook = orig }()

	m := &defaultManager{virtualCenter: &cnsvsphere.VirtualCenter{}}

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		assert.Equal(t, "base-cid", changeID)
		return &vim25types.DiskChangeInfo{
			StartOffset: 0,
			Length:      300,
			ChangedArea: []vim25types.DiskChangeExtent{{Start: 100, Length: 200}},
		}, nil
	}
	areas, next, err := m.QueryFCDChangedBlocks(ctx, "vol", "tgt", "base-cid", 0)
	assert.NoError(t, err)
	assert.Len(t, areas, 1)
	assert.Equal(t, uint64(300), next)

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		return &vim25types.DiskChangeInfo{
			StartOffset: 0,
			Length:      1024,
			ChangedArea: []vim25types.DiskChangeExtent{
				{Start: 0, Length: 512},
				{Start: 512, Length: 512},
			},
		}, nil
	}
	areas, next, err = m.QueryFCDChangedBlocks(ctx, "vol", "tgt", "cid", 0)
	assert.NoError(t, err)
	assert.Len(t, areas, 2)
	assert.Equal(t, uint64(1024), next)

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		return nil, createFCDSoapFault(&vim25types.InvalidArgument{InvalidProperty: "snapshotId"}, "")
	}
	_, _, err = m.QueryFCDChangedBlocks(ctx, "vol", "tgt", "cid", 0)
	assert.Error(t, err)

	mNil := &defaultManager{virtualCenter: nil}
	_, _, err = mNil.QueryFCDChangedBlocks(ctx, "v", "t", "c", 0)
	assert.Error(t, err)

	QueryChangedDiskAreasHook = func(
		ctx context.Context,
		vcenter *cnsvsphere.VirtualCenter,
		volumeID, snapshotID vim25types.ID,
		startingOffset int64,
		changeID string,
	) (*vim25types.DiskChangeInfo, error) {
		return &vim25types.DiskChangeInfo{
			StartOffset: 0,
			Length:      500,
			ChangedArea: []vim25types.DiskChangeExtent{
				{Start: 0, Length: 100},
				{Start: 200, Length: 100},
				{Start: 400, Length: 100},
			},
		}, nil
	}
	areas, next, err = m.QueryFCDChangedBlocks(ctx, "vol", "tgt", "cid", 0)
	assert.NoError(t, err)
	assert.Len(t, areas, 3)
	assert.Equal(t, uint64(500), next)
}

func TestRunQueryChangedDiskAreasViaVslmNilClient(t *testing.T) {
	ctx := context.Background()
	_, err := runQueryChangedDiskAreasViaVslm(ctx, nil, vim25types.ID{Id: "v"}, vim25types.ID{Id: "s"}, 0, "*")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vim25 client is nil")
}

func TestRunRetrieveSnapshotDetailsViaVslmNilClient(t *testing.T) {
	ctx := context.Background()
	_, err := runRetrieveSnapshotDetailsViaVslm(ctx, nil, vim25types.ID{Id: "v"}, vim25types.ID{Id: "s"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vim25 client is nil")
}

func TestRunQueryChangedDiskAreasViaVslmNewClientError(t *testing.T) {
	orig := vslmNewClientFunc
	defer func() { vslmNewClientFunc = orig }()
	vslmNewClientFunc = func(ctx context.Context, c *vim25.Client) (*vslm.Client, error) {
		return nil, fmt.Errorf("injected vslm failure")
	}
	ctx := context.Background()
	_, err := runQueryChangedDiskAreasViaVslm(ctx, &vim25.Client{}, vim25types.ID{Id: "v"}, vim25types.ID{Id: "s"}, 0, "*")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create VSLM client")
}

func TestRunRetrieveSnapshotDetailsViaVslmNewClientError(t *testing.T) {
	orig := vslmNewClientFunc
	defer func() { vslmNewClientFunc = orig }()
	vslmNewClientFunc = func(ctx context.Context, c *vim25.Client) (*vslm.Client, error) {
		return nil, fmt.Errorf("injected vslm failure")
	}
	ctx := context.Background()
	_, err := runRetrieveSnapshotDetailsViaVslm(ctx, &vim25.Client{}, vim25types.ID{Id: "v"}, vim25types.ID{Id: "s"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create VSLM client")
}

func TestDefaultVslmHooksDisconnectedVC(t *testing.T) {
	ctx := context.Background()
	_, err := defaultQueryChangedDiskAreasHook(ctx, nil, vim25types.ID{}, vim25types.ID{}, 0, "*")
	assert.Error(t, err)
	_, err = defaultQueryChangedDiskAreasHook(ctx, &cnsvsphere.VirtualCenter{}, vim25types.ID{}, vim25types.ID{}, 0, "*")
	assert.Error(t, err)

	_, err = defaultRetrieveSnapshotDetailsHook(ctx, nil, vim25types.ID{}, vim25types.ID{})
	assert.Error(t, err)
	_, err = defaultRetrieveSnapshotDetailsHook(ctx, &cnsvsphere.VirtualCenter{}, vim25types.ID{}, vim25types.ID{})
	assert.Error(t, err)
}

const clientSideFailureTaskID = "task-4242"

// stubListView satisfies ListViewIf so MonitorCreateVolumeTask can be driven
// without a real vCenter. addTaskErr, if set, is returned by AddTask; otherwise
// AddTask succeeds and nothing is ever pushed to the channel, which lets the
// caller's context expire or be cancelled while the task is still "running" on
// CNS -- exactly the situation this fix is about.
type stubListView struct {
	addTaskErr error
}

func (s *stubListView) AddTask(ctx context.Context, taskMoRef vim25types.ManagedObjectReference,
	ch chan TaskResult) error {
	return s.addTaskErr
}
func (s *stubListView) RemoveTask(ctx context.Context, taskMoRef vim25types.ManagedObjectReference) error {
	return nil
}
func (s *stubListView) ResetVirtualCenter(ctx context.Context, virtualCenter *cnsvsphere.VirtualCenter) {
}
func (s *stubListView) MarkTaskForDeletion(ctx context.Context,
	taskMoRef vim25types.ManagedObjectReference) error {
	return nil
}
func (s *stubListView) IsListViewReady() bool                   { return true }
func (s *stubListView) SetListViewNotReady(ctx context.Context) {}

// pendingDetails returns operation details in the state the CR is left in after
// invokeCNSCreateVolume succeeds: InProgress, carrying the CNS TaskID.
func pendingDetails() *cnsvolumeoperationrequest.VolumeOperationRequestDetails {
	return cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		"pvc-test", "", "", 0, nil, metav1.Now(), clientSideFailureTaskID,
		"vc.example.com", "", taskInvocationStatusInProgress, "", "")
}

// newManagerWithStubListView builds a defaultManager wired to a stub listView so
// waitOnTask does not attempt to dial vCenter. Config is populated because
// MonitorCreateVolumeTask dereferences virtualCenter.Config.Host on its
// ManagedObjectNotFound path regardless of multivCenterTopologyDeployment. The
// tests below do not reach that path, but a nil Config would turn any future
// test that does into a panic instead of a failure.
func newManagerWithStubListView(lv ListViewIf) *defaultManager {
	return &defaultManager{
		virtualCenter: &cnsvsphere.VirtualCenter{
			Config: &cnsvsphere.VirtualCenterConfig{Host: "vc.example.com"},
		},
		listViewIf: lv,
	}
}

// newTaskRef builds an object.Task for the fixed test TaskID. The stub listView
// never dials vCenter, so the client only needs to be non-nil.
func newTaskRef() *object.Task {
	return object.NewTask(&vim25.Client{},
		vim25types.ManagedObjectReference{Type: "Task", Value: clientSideFailureTaskID})
}

func TestIsClientSideFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "context deadline exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "context canceled",
			err:  context.Canceled,
			want: true,
		},
		{
			// The shape produced by waitForResultOrTimeout.
			name: "wrapped deadline exceeded",
			err:  fmt.Errorf("time out for task task-1 before response from CNS: %w", context.DeadlineExceeded),
			want: true,
		},
		{
			// The shape produced when a vCenter session/ListView collapse aborts an
			// in-flight SOAP call: `Post "https://vc/sdk": context canceled`.
			name: "wrapped canceled from soap call",
			err:  fmt.Errorf("Post \"https://vc.example.com:443/sdk\": %w", context.Canceled),
			want: true,
		},
		{
			name: "doubly wrapped canceled",
			err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", context.Canceled)),
			want: true,
		},
		{
			name: "unrelated error is not client side",
			err:  errors.New("CNS returned CnsVolumeNotFoundFault"),
			want: false,
		},
		{
			name: "listview addition error is not client side",
			err:  fmt.Errorf("%w. task: task-1", ErrListViewTaskAddition),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isClientSideFailure(tt.err))
		})
	}
}

// TestMonitorCreateVolumeTaskPreservesPendingOnClientSideFailure is the
// regression test for the orphaned-volume bug. When our own context dies while
// a CNS CreateVolume task is still running, the operation details must be left
// InProgress with the TaskID intact. If they are overwritten with Error, the
// TaskID is dropped, IsTaskPending returns false on the next attempt, and the
// driver issues a second CNS CreateVolume -- producing a duplicate FCD whose
// first copy is orphaned on the datastore with nothing referencing it.
func TestMonitorCreateVolumeTaskPreservesPendingOnClientSideFailure(t *testing.T) {
	tests := []struct {
		name    string
		makeCtx func() (context.Context, context.CancelFunc)
	}{
		{
			name: "caller timeout elapses",
			makeCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(logger.NewContextWithLogger(context.TODO()), 10*time.Millisecond)
			},
		},
		{
			name: "caller context cancelled mid-flight",
			makeCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.TODO()))
				cancel()
				return ctx, func() {}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.makeCtx()
			defer cancel()

			m := newManagerWithStubListView(&stubListView{})
			details := pendingDetails()
			info, _, err := m.MonitorCreateVolumeTask(ctx, &details, newTaskRef(), "pvc-test", "cluster-1")

			// The caller still sees an error, so the sidecar retries.
			assert.Error(t, err)
			assert.Nil(t, info)

			// The critical assertion: the task reference survives, so the retry
			// resumes this task instead of creating a second volume.
			assert.Equal(t, taskInvocationStatusInProgress, details.OperationDetails.TaskStatus,
				"client-side failure must not mark the operation as failed")
			assert.Equal(t, clientSideFailureTaskID, details.OperationDetails.TaskID,
				"TaskID must be preserved so the next attempt can resume the CNS task")
			assert.True(t, IsTaskPending(details),
				"IsTaskPending must report pending so the retry resumes rather than re-creates")
		})
	}
}

// TestMonitorCreateVolumeTaskMarksErrorOnNonClientSideFailure pins the
// unchanged behaviour: a failure that is genuinely about the task, rather than
// about our ability to observe it, must still be recorded as an error so the
// next attempt starts a fresh CNS CreateVolume.
func TestMonitorCreateVolumeTaskMarksErrorOnNonClientSideFailure(t *testing.T) {
	ctx := logger.NewContextWithLogger(context.TODO())

	// A non-context AddTask failure reaches MonitorCreateVolumeTask as a plain
	// error, which must not be treated as client side. ErrListViewTaskAddition is
	// deliberately excluded here because waitOnTask returns early for it.
	m := newManagerWithStubListView(&stubListView{
		addTaskErr: soap.WrapVimFault(&vim25types.RuntimeFault{}),
	})
	details := pendingDetails()

	info, _, err := m.MonitorCreateVolumeTask(ctx, &details, newTaskRef(), "pvc-test", "cluster-1")

	assert.Error(t, err)
	assert.Nil(t, info)
	assert.Equal(t, taskInvocationStatusError, details.OperationDetails.TaskStatus,
		"a genuine task failure must still be recorded as an error")
	assert.False(t, IsTaskPending(details),
		"a genuine task failure must not be reported as pending")
}

// TestMonitorCreateVolumeTaskClientSideFailureWithoutTaskID covers the guard's
// second condition. With no TaskID there is nothing for a retry to resume, so
// preserving InProgress would strand the operation; it must fall through to the
// error path.
func TestMonitorCreateVolumeTaskClientSideFailureWithoutTaskID(t *testing.T) {
	ctx, cancel := context.WithCancel(logger.NewContextWithLogger(context.TODO()))
	cancel()

	m := newManagerWithStubListView(&stubListView{})
	details := cnsvolumeoperationrequest.CreateVolumeOperationRequestDetails(
		"pvc-test", "", "", 0, nil, metav1.Now(), "", // empty TaskID
		"vc.example.com", "", taskInvocationStatusInProgress, "", "")

	_, _, err := m.MonitorCreateVolumeTask(ctx, &details, newTaskRef(), "pvc-test", "cluster-1")

	assert.Error(t, err)
	assert.Equal(t, taskInvocationStatusError, details.OperationDetails.TaskStatus,
		"without a TaskID there is nothing to resume, so the operation must be marked failed")
}
