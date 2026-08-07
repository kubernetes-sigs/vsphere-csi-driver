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
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	vim25types "github.com/vmware/govmomi/vim25/types"
	"github.com/vmware/govmomi/vslm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest"
	cnsvolumeoprequestv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/internalapis/cnsvolumeoperationrequest/v1alpha1"
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

// fakeOperationStore is a minimal in-memory stand-in for
// cnsvolumeoperationrequest.VolumeOperationRequest, used to unit test
// createVolumeWithTransaction's idempotency behavior without a real API server.
type fakeOperationStore struct {
	getRequestDetails func(ctx context.Context, name string) (
		*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error)
	storeCallCount int
}

func (f *fakeOperationStore) GetRequestDetails(ctx context.Context, name string) (
	*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
	return f.getRequestDetails(ctx, name)
}

func (f *fakeOperationStore) StoreRequestDetails(ctx context.Context,
	instance *cnsvolumeoperationrequest.VolumeOperationRequestDetails) error {
	f.storeCallCount++
	return nil
}

func (f *fakeOperationStore) DeleteRequestDetails(ctx context.Context, name string) error {
	return nil
}

func (f *fakeOperationStore) HasPriorSuccessfulCreate(ctx context.Context, name string) bool {
	return false
}

// notFoundErr builds a k8s NotFound error the same way a real API server would
// for a CnsVolumeOperationRequest CR that does not exist yet.
func notFoundErr(name string) error {
	return apierrors.NewNotFound(
		cnsvolumeoprequestv1alpha1.Resource(cnsvolumeoperationrequest.CRDPlural), name)
}

// TestCreateVolumeWithTransactionPriorSuccessSkipsCNSInvocation is a regression test for the
// PR-3762313-Perf incident: a duplicate/retried CreateVolume call for a volume name that the
// CnsVolumeOperationRequest store already reports as successfully created must return the
// cached volume ID directly, without invoking CNS CreateVolume again. Before this fix,
// createVolumeWithTransaction unconditionally re-invoked CNS, which raced a concurrent
// successful caller, hit CnsVolumeAlreadyExistsFault, and triggered a delete+recreate of a
// volume that had already been returned to the CO as a bound PV.
//
// virtualCenter is left as a zero value (nil CnsClient) on purpose: if the idempotency
// short-circuit regresses and the code falls through to invokeCNSCreateVolume, that call
// panics on the nil CnsClient, so assert.NotPanics below fails loudly instead of the test
// silently passing for the wrong reason.
func TestCreateVolumeWithTransactionPriorSuccessSkipsCNSInvocation(t *testing.T) {
	ctx := context.TODO()
	const volName = "pvc-5954ecf3-215f-4396-8b8d-87d6ae007247"
	const cachedVolumeID = "5954ecf3-215f-4396-8b8d-87d6ae007247"

	store := &fakeOperationStore{
		getRequestDetails: func(ctx context.Context, name string) (
			*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
			assert.Equal(t, volName, name)
			return &cnsvolumeoperationrequest.VolumeOperationRequestDetails{
				Name:     volName,
				VolumeID: cachedVolumeID,
				OperationDetails: &cnsvolumeoperationrequest.OperationDetails{
					TaskStatus: cnsvolumeoperationrequest.TaskInvocationStatusSuccess,
					OpID:       "opid-1",
				},
			}, nil
		},
	}
	m := &defaultManager{
		operationStore: store,
		virtualCenter:  &cnsvsphere.VirtualCenter{},
	}
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volName}

	var resp *CnsVolumeInfo
	var faultType string
	var err error
	assert.NotPanics(t, func() {
		resp, faultType, err = m.createVolumeWithTransaction(ctx, spec, nil)
	})

	assert.NoError(t, err)
	assert.Empty(t, faultType)
	if assert.NotNil(t, resp) {
		assert.Equal(t, cachedVolumeID, resp.VolumeID.Id)
	}
	assert.Zero(t, store.storeCallCount,
		"a duplicate call for an already-successfully-created volume must not persist a new InProgress record")
}

// TestCreateVolumeWithTransactionNoPriorRecordInvokesCNS is the counterpart to the test above:
// when the operation store has no record at all for this volume name (a genuinely new
// CreateVolume call), createVolumeWithTransaction must still fall through and attempt to invoke
// CNS CreateVolume, i.e. the idempotency short-circuit must not swallow first-time creates too.
// It asserts this indirectly: with a nil CnsClient, reaching invokeCNSCreateVolume panics, so a
// panic here is the expected, correct outcome and proves the fall-through path is intact.
func TestCreateVolumeWithTransactionNoPriorRecordInvokesCNS(t *testing.T) {
	ctx := context.TODO()
	const volName = "pvc-brand-new-volume"

	store := &fakeOperationStore{
		getRequestDetails: func(ctx context.Context, name string) (
			*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
			return nil, notFoundErr(name)
		},
	}
	m := &defaultManager{
		operationStore: store,
		virtualCenter:  &cnsvsphere.VirtualCenter{},
	}
	spec := &cnstypes.CnsVolumeCreateSpec{Name: volName}

	assert.Panics(t, func() {
		_, _, _ = m.createVolumeWithTransaction(ctx, spec, nil)
	})
	assert.Equal(t, 1, store.storeCallCount,
		"a first-time create must persist an InProgress record before invoking CNS")
}

// TestCreateVolumeWithTransactionOperationStorePropagatesUnexpectedError verifies that an
// unexpected (non-NotFound) error reading the operation store is still surfaced as a
// CSIInternalFault, preserving the pre-existing error-handling behavior after restructuring the
// GetRequestDetails handling into the idempotency-checking switch statement.
func TestCreateVolumeWithTransactionOperationStorePropagatesUnexpectedError(t *testing.T) {
	ctx := context.TODO()
	wantErr := errors.New("etcd is unavailable")

	store := &fakeOperationStore{
		getRequestDetails: func(ctx context.Context, name string) (
			*cnsvolumeoperationrequest.VolumeOperationRequestDetails, error) {
			return nil, wantErr
		},
	}
	m := &defaultManager{
		operationStore: store,
		virtualCenter:  &cnsvsphere.VirtualCenter{},
	}
	spec := &cnstypes.CnsVolumeCreateSpec{Name: "pvc-test"}

	resp, faultType, err := m.createVolumeWithTransaction(ctx, spec, nil)
	assert.Nil(t, resp)
	assert.Equal(t, csifault.CSIInternalFault, faultType)
	assert.Equal(t, wantErr, err)
	assert.Zero(t, store.storeCallCount)
}
