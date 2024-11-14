package vcenter

import (
	"context"
	"fmt"
	"sync"

	"github.com/vmware/govmomi/pbm"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
)

type Provider interface {
	DoesProfileSupportEncryption(ctx context.Context, profileID string) (bool, error)
	ResetVC()
}

func NewProvider() Provider {
	return &defaultProvider{}
}

type defaultProvider struct {
	client       *cnsvsphere.VirtualCenter
	reinitialize bool
	lock         sync.RWMutex
}

func (p *defaultProvider) DoesProfileSupportEncryption(ctx context.Context, profileID string) (bool, error) {
	pbmClient, err := p.getPbmClient(ctx)
	if err != nil {
		return false, err
	}
	return pbmClient.SupportsEncryption(ctx, profileID)
}

func (p *defaultProvider) ResetVC() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.client = nil
	p.reinitialize = true
}

func (p *defaultProvider) getVcClient(ctx context.Context) (*cnsvsphere.VirtualCenter, error) {
	if client := func() *cnsvsphere.VirtualCenter {
		p.lock.RLock()
		defer p.lock.RUnlock()
		return p.client
	}(); client != nil {
		return client, nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if p.client != nil {
		return p.client, nil
	}

	config, err := cnsconfig.GetConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get VirtualCenterConfig: %w", err)
	}

	if !config.Global.InsecureFlag && config.Global.CAFile != cnsconfig.SupervisorCAFilePath {
		config.Global.CAFile = cnsconfig.SupervisorCAFilePath
	}

	client, err := cnsvsphere.GetVirtualCenterInstance(ctx, &cnsconfig.ConfigurationInfo{Cfg: config}, p.reinitialize)
	if err != nil {
		return nil, fmt.Errorf("failed to get VirtualCenter: %w", err)
	}

	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect VirtualCenter: %w", err)
	}

	p.client = client

	return client, nil
}

func (p *defaultProvider) getPbmClient(ctx context.Context) (*pbm.Client, error) {
	vcClient, err := p.getVcClient(ctx)
	if err != nil {
		return nil, err
	}

	if pbmClient := vcClient.PbmClient; pbmClient != nil {
		return pbmClient, nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	if vcClient.PbmClient == nil {
		vimClient := vcClient.Client.Client
		if vcClient.PbmClient, err = pbm.NewClient(ctx, vimClient); err != nil {
			return nil, fmt.Errorf("failed to create pbm client with err: %w", err)
		}
	}

	return vcClient.PbmClient, nil
}
