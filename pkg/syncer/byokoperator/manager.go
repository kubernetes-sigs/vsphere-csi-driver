package byokoperator

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/fsnotify/fsnotify"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller"
	ctrlcommon "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/controller/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/byokoperator/vcenter"
)

type Manager struct {
	started    bool
	ctrlMgr    ctrlmgr.Manager
	vcProvider vcenter.Provider
	logger     *zap.SugaredLogger
}

func NewManager(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, configInfo *config.ConfigurationInfo) (*Manager, error) {
	scheme, err := crypto.NewK8sScheme()
	if err != nil {
		return nil, err
	}

	kubeConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}

	ctrlMgr, err := ctrl.NewManager(kubeConfig, ctrl.Options{
		Scheme: scheme,
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{
					&corev1.ConfigMap{},
					&corev1.Secret{},
				},
			},
		},
		MetricsBindAddress: "0",
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create crypto manager: %w", err)
	}

	logger := logger.GetLogger(ctx)
	vcProvider := vcenter.NewProvider()
	cryptoClient := crypto.NewClient(ctx, ctrlMgr.GetClient())

	vc, err := cnsvsphere.GetVirtualCenterInstance(ctx, configInfo, false)
	if err != nil {
		return nil, err
	}

	volumeManager, err := volume.GetManager(ctx, vc, nil, false, false, false, clusterFlavor)
	if err != nil {
		return nil, fmt.Errorf("failed to create an instance of volume manager: %w", err)
	}

	if err := controller.AddToManager(ctx, ctrlMgr, ctrlcommon.Options{
		ClusterFlavor:   clusterFlavor,
		VCenterProvider: vcProvider,
		CryptoClient:    cryptoClient,
		VolumeManager:   volumeManager,
	}); err != nil {
		return nil, err
	}

	mgr := &Manager{
		ctrlMgr:    ctrlMgr,
		vcProvider: vcProvider,
		logger:     logger,
	}

	return mgr, nil
}

func (mgr *Manager) Start(ctx context.Context) error {
	if mgr.started {
		return nil
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, 2)
	defer close(errChan)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mgr.watchConfigChanges(ctx); err != nil {
			errChan <- fmt.Errorf("failed to watch for config changes: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mgr.ctrlMgr.Start(ctx); err != nil {
			errChan <- fmt.Errorf("unable to run crypto controller manager: %w", err)
		}
	}()

	mgr.started = true
	mgr.logger.Info("Started Crypto Operator")

	defer func() {
		mgr.logger.Info("Terminating Crypto Operator")
		cancel()
		wg.Wait()
		mgr.started = false
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

func (mgr *Manager) watchConfigChanges(ctx context.Context) error {
	cfgPath := cnsconfig.GetConfigPath(ctx)
	fswatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	defer func() {
		fswatcher.Close()
	}()

	if err := fswatcher.Add(filepath.Dir(cfgPath)); err != nil {
		return err
	}

	if err := fswatcher.Add(filepath.Dir(cnsconfig.SupervisorCAFilePath)); err != nil {
		return err
	}

	log := logger.GetLogger(ctx)

	for {
		log.Debug("Waiting for event on fsnotify watcher")
		select {
		case event, ok := <-fswatcher.Events:
			if !ok {
				return fmt.Errorf("failed to listen for config changes")
			}
			log.Debugf("fsnotify event: %s\n", event.String())
			if event.Op&fsnotify.Remove == fsnotify.Remove || (event.Op&fsnotify.Create == fsnotify.Create && event.Name == cnsconfig.SupervisorCAFilePath) {
				mgr.vcProvider.ResetVC()
			}
		case err, ok := <-fswatcher.Errors:
			log.Errorf("fsnotify error: %+v", err)
			if !ok {
				return fmt.Errorf("failed to listen for config changes")
			}

		case <-ctx.Done():
			return nil
		}
	}
}
