package vsphere

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi/vapi/tags"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// GetTagManager returns tagManager connected to given VirtualCenter.
func (vc *VirtualCenter) GetTagManager(ctx context.Context) (*tags.Manager, error) {
	log := logger.GetLogger(ctx)
	// Validate input.
	if vc == nil || vc.Client == nil || vc.Client.Client == nil {
		return nil, fmt.Errorf("vCenter not initialized")
	}

	if err := vc.Connect(ctx); err != nil {
		return nil, fmt.Errorf("error connecting to VC: %w", err)
	}

	vc.tagManager = tags.NewManager(vc.RestClient)
	if vc.tagManager == nil {
		return nil, fmt.Errorf("failed to create a tagManager")
	}
	log.Infof("New tag manager with useragent '%s'", vc.tagManager.UserAgent)
	return vc.tagManager, nil
}
