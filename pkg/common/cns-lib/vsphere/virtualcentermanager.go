/*
Copyright 2019 The Kubernetes Authors.

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

package vsphere

import (
	"context"
	"errors"
	"sync"

	"github.com/vmware/govmomi/cns"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

var (
	// ErrVCAlreadyRegistered is returned when registration for a previously
	// registered virtual center is attempted.
	ErrVCAlreadyRegistered = errors.New("virtual center was already registered")
	// ErrVCNotFound is returned when a virtual center instance isn't found.
	ErrVCNotFound = errors.New("virtual center wasn't found in registry")
)

// VirtualCenterManager provides functionality to manage virtual centers.
type VirtualCenterManager interface {
	// GetVirtualCenter returns the VirtualCenter instance given the host.
	GetVirtualCenter(ctx context.Context, host string) (*VirtualCenter, error)
	// GetAllVirtualCenters returns all VirtualCenter instances. If virtual
	// centers are added or removed concurrently, they may or may not be
	// reflected in the result of a call to this method.
	GetAllVirtualCenters() []*VirtualCenter
	// RegisterVirtualCenter registers a virtual center, but doesn't initiate
	// the connection to the host.
	RegisterVirtualCenter(ctx context.Context, config *VirtualCenterConfig) (*VirtualCenter, error)
	// UnregisterVirtualCenter disconnects and unregisters the virtual center
	// given it's host.
	UnregisterVirtualCenter(ctx context.Context, host string) error
	// UnregisterAllVirtualCenters disconnects and unregisters all virtual centers.
	UnregisterAllVirtualCenters(ctx context.Context) error
	// IsvSANFileServicesSupported checks if vSAN file services is supported or not.
	IsvSANFileServicesSupported(ctx context.Context, host string) (bool, error)
	// IsExtendVolumeSupported checks if extend volume is supported or not.
	IsExtendVolumeSupported(ctx context.Context, host string) (bool, error)
	// IsOnlineExtendVolumeSupported checks if online extend volume is supported
	// or not on the vCenter Host.
	IsOnlineExtendVolumeSupported(ctx context.Context, host string) (bool, error)
}

var (
	// vcManagerInst is a VirtualCenterManager singleton.
	vcManagerInst *defaultVirtualCenterManager
	// onceForVCManager is used for initializing the VirtualCenterManager singleton.
	onceForVCManager sync.Once
)

// GetVirtualCenterManager returns the VirtualCenterManager singleton.
func GetVirtualCenterManager(ctx context.Context) VirtualCenterManager {
	onceForVCManager.Do(func() {
		log := logger.GetLogger(ctx)
		log.Info("Initializing defaultVirtualCenterManager...")
		vcManagerInst = &defaultVirtualCenterManager{virtualCenters: sync.Map{}}
		log.Info("Successfully initialized defaultVirtualCenterManager")
	})
	return vcManagerInst
}

// defaultVirtualCenterManager holds virtual center information and provides
// functionality around it.
type defaultVirtualCenterManager struct {
	// virtualCenters map hosts to *VirtualCenter instances.
	virtualCenters sync.Map
}

func (m *defaultVirtualCenterManager) GetVirtualCenter(ctx context.Context, host string) (*VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	if vc, exists := m.virtualCenters.Load(host); exists {
		return vc.(*VirtualCenter), nil
	}
	log.Errorf("Couldn't find VC %s in registry", host)
	return nil, ErrVCNotFound
}

func (m *defaultVirtualCenterManager) GetAllVirtualCenters() []*VirtualCenter {
	var vcs []*VirtualCenter
	m.virtualCenters.Range(func(_, vcInf interface{}) bool {
		// If an entry was concurrently deleted from virtualCenters, Range could
		// possibly return a nil value for that key.
		// See https://golang.org/pkg/sync/#Map.Range for more info.
		if vcInf != nil {
			vcs = append(vcs, vcInf.(*VirtualCenter))
		}
		return true
	})
	return vcs
}

func (m *defaultVirtualCenterManager) RegisterVirtualCenter(ctx context.Context,
	config *VirtualCenterConfig) (*VirtualCenter, error) {
	log := logger.GetLogger(ctx)
	if _, exists := m.virtualCenters.Load(config.Host); exists {
		log.Errorf("VC was already found in registry, failed to register with config %v", config)
		return nil, ErrVCAlreadyRegistered
	}

	// Note that the Client isn't initialized here.
	vc := &VirtualCenter{Config: config}
	m.virtualCenters.Store(config.Host, vc)
	log.Infof("Successfully registered VC %q", vc.Config.Host)
	return vc, nil
}

func (m *defaultVirtualCenterManager) UnregisterVirtualCenter(ctx context.Context, host string) error {
	log := logger.GetLogger(ctx)
	vc, err := m.GetVirtualCenter(ctx, host)
	if err != nil {
		if err == ErrVCNotFound {
			log.Warnf("failed to find vCenter: %q Assuming vCenter is already unregistered.", host)
			return nil
		}
		return err
	}
	if err = vc.DisconnectPbm(ctx); err != nil {
		log.Warnf("failed to disconnect VC pbm %s, couldn't unregister", host)
	}
	if err = vc.Disconnect(ctx); err != nil {
		log.Warnf("failed to disconnect VC %s, couldn't unregister", host)
	}
	vc.DisconnectCns(ctx)
	m.virtualCenters.Delete(host)
	log.Infof("Successfully unregistered VC %s", host)
	return nil
}

func (m *defaultVirtualCenterManager) UnregisterAllVirtualCenters(ctx context.Context) error {
	var err error
	log := logger.GetLogger(ctx)
	m.virtualCenters.Range(func(hostInf, _ interface{}) bool {
		if err = m.UnregisterVirtualCenter(ctx, hostInf.(string)); err != nil {
			log.Warnf("failed to unregister vCenter: %q, err: %+v", hostInf.(string), err)
		}
		return true
	})
	return err
}

// IsvSANFileServicesSupported checks if vSAN file services is supported or not.
func (m *defaultVirtualCenterManager) IsvSANFileServicesSupported(ctx context.Context, host string) (bool, error) {
	log := logger.GetLogger(ctx)
	is67u3Release, err := isVsan67u3Release(ctx, m, host)
	if err != nil {
		log.Errorf("Failed to identify the vCenter release with error: %+v", err)
		return false, err
	}
	return !is67u3Release, nil
}

// IsExtendVolumeSupported checks if extend volume is supported or not.
func (m *defaultVirtualCenterManager) IsExtendVolumeSupported(ctx context.Context, host string) (bool, error) {
	log := logger.GetLogger(ctx)
	is67u3Release, err := isVsan67u3Release(ctx, m, host)
	if err != nil {
		log.Errorf("Failed to identify the vCenter release with error: %+v", err)
		return false, err
	}
	return !is67u3Release, nil
}

// IsOnlineExtendVolumeSupported checks if online extend volume is supported or not.
func (m *defaultVirtualCenterManager) IsOnlineExtendVolumeSupported(ctx context.Context, host string) (bool, error) {
	log := logger.GetLogger(ctx)

	// Get VC instance.
	vcenter, err := m.GetVirtualCenter(ctx, host)
	if err != nil {
		log.Errorf("Failed to get vCenter. Err: %v", err)
		return false, err
	}
	vCenterVersion := vcenter.Client.Version
	if vCenterVersion != cns.ReleaseVSAN67u3 && vCenterVersion != cns.ReleaseVSAN70 &&
		vCenterVersion != cns.ReleaseVSAN70u1 {
		return true, nil
	}
	log.Infof("Online volume expansion is not supported on vCenter version %q", vCenterVersion)
	return false, nil
}
