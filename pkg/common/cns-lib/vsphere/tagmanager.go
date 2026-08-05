package vsphere

import (
	"context"
	"fmt"

	"github.com/vmware/govmomi/vapi/tags"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// GetTagManager returns a tagManager connected to the given VirtualCenter.
//
// It is built fresh on every call rather than cached on VirtualCenter: the
// RestClient it wraps is replaced whenever connect() re-authenticates, and
// VirtualCenter is a shared, concurrently accessed singleton, so a cached
// manager would need its own synchronization to avoid handing out one built
// from a RestClient that is mid-replacement.
func (vc *VirtualCenter) GetTagManager(ctx context.Context) (*tags.Manager, error) {
	log := logger.GetLogger(ctx)
	// vc == nil is unsafe to let Connect handle: it dereferences vc immediately
	// (vc.ClientMutex.Lock()) and would panic instead of returning an error.
	//
	// A non-nil vc.Client whose own inner Client is nil is also unsafe to leave
	// to Connect, but for a different reason: connect() only replaces vc.Client
	// when the outer pointer itself is nil, so this state slips past that
	// check, and connect() goes on to call session.NewManager(vc.Client.Client)
	// and use it -- panicking inside govmomi's property collector. Confirmed by
	// reproducing it directly (session.Manager.UserSession -> property.
	// DefaultCollector, both against a nil *vim25.Client).
	//
	// A nil vc.Client alone does not need a check: connect() explicitly creates
	// one in that case, matching GetDatacenters and ListDatacenters, which call
	// Connect unconditionally with no such guard.
	if vc == nil || (vc.Client != nil && vc.Client.Client == nil) {
		return nil, fmt.Errorf("vCenter not initialized")
	}

	if err := vc.Connect(ctx); err != nil {
		return nil, fmt.Errorf("error connecting to VC: %w", err)
	}

	tagManager := tags.NewManager(vc.RestClient)
	if tagManager == nil {
		return nil, fmt.Errorf("failed to create a tagManager")
	}
	log.Infof("New tag manager with useragent '%s'", tagManager.UserAgent)
	return tagManager, nil
}
