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
	//
	// vc.Client is read under ClientMutex here, not bare: connect() rewrites it
	// under that same lock on every reconnect, and reading it unlocked races
	// that write (confirmed with -race, forcing a concurrent reconnect against
	// vcsim -- the existing single-connect concurrency test never exercises
	// connect()'s write path at all, since a still-valid session takes its
	// early-return branch, so it can't catch this).
	if vc == nil {
		return nil, fmt.Errorf("vCenter not initialized")
	}
	vc.ClientMutex.Lock()
	brokenInnerClient := vc.Client != nil && vc.Client.Client == nil
	vc.ClientMutex.Unlock()
	if brokenInnerClient {
		return nil, fmt.Errorf("vCenter not initialized")
	}

	if err := vc.Connect(ctx); err != nil {
		return nil, fmt.Errorf("error connecting to VC: %w", err)
	}

	// Same reasoning as above, for the RestClient this call is actually built
	// from: Connect has released the lock by the time it returns here, and a
	// different concurrent caller's reconnect can still be rewriting
	// vc.RestClient under it at this exact moment.
	vc.ClientMutex.Lock()
	restClient := vc.RestClient
	vc.ClientMutex.Unlock()

	tagManager := tags.NewManager(restClient)
	if tagManager == nil {
		return nil, fmt.Errorf("failed to create a tagManager")
	}
	log.Infof("New tag manager with useragent '%s'", tagManager.UserAgent)
	return tagManager, nil
}
