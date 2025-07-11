package cnsvolumeattachment

import "context"

type MockCnsVolumeAttachment struct{}

func (m *MockCnsVolumeAttachment) AddVmToAttachedList(ctx context.Context, volumeName,
	VmInstanceUUID string) error {
	// Mock behavior
	return nil
}

func (m *MockCnsVolumeAttachment) RemoveVmFromAttachedList(ctx context.Context, volumeName,
	VmInstanceUUID string) (error, bool) {
	// Mock behavior
	return nil, true
}
