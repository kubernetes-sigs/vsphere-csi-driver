/*
Copyright 2026 The Kubernetes Authors.

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

package runtimeconfig

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// retryInterval is how long a failed reload waits before it is tried again.
var retryInterval = 60 * time.Second

// Reloader is what the driver's controller and syncer expose to react to configuration changes.
type Reloader interface {
	// ReloadConfiguration reloads the configuration, re-establishing the vCenter connection when
	// reconnectToVC is true.
	ReloadConfiguration(reconnectToVC bool) error
}

// StartReloading is Start with the callbacks the driver wants: a credentials change reconnects to
// vCenter, a file volume change only reloads the configuration.
func StartReloading(ctx context.Context, cfg *config.Config, r Reloader) error {
	return Start(ctx, cfg, reloadFunc(r, true), reloadFunc(r, false))
}

func startReloading(ctx context.Context, client kubernetes.Interface, namespace string, cfg *config.Config,
	r Reloader) error {
	return start(ctx, client, namespace, cfg, reloadFunc(r, true), reloadFunc(r, false))
}

func reloadFunc(r Reloader, reconnectToVC bool) func() error {
	return func() error { return r.ReloadConfiguration(reconnectToVC) }
}

// Start watches the objects the runtime sources come from and calls onCredentialsChange when the
// credentials Secret changes and onFileVolumeChange when the file volume ConfigMap changes. The
// callbacks run one at a time per source, are retried until they succeed, and collapse bursts of
// events into one call. It does nothing when the runtime sources are not in use. The watches run
// until ctx is done.
//
// The driver's shared InformerManager is not used: it keeps a single ConfigMap informer per
// process and cannot take a label or field selector.
func Start(ctx context.Context, cfg *config.Config, onCredentialsChange, onFileVolumeChange func() error) error {
	if !Enabled(ctx) {
		return nil
	}
	// Init runs again when a replica wins a leader election after losing it, and a second set of
	// informers would reload every change twice.
	startedMu.Lock()
	defer startedMu.Unlock()
	if started {
		logger.GetLogger(ctx).Info("The runtime configuration sources are already being watched")
		return nil
	}
	client, err := getClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a Kubernetes client: %w", err)
	}
	if err := start(ctx, client, config.GetCSINamespace(), cfg, onCredentialsChange, onFileVolumeChange); err != nil {
		return err
	}
	started = true
	go func() {
		<-ctx.Done()
		startedMu.Lock()
		started = false
		startedMu.Unlock()
	}()
	return nil
}

var (
	startedMu sync.Mutex
	started   bool
)

func start(ctx context.Context, client kubernetes.Interface, namespace string, cfg *config.Config,
	onCredentialsChange, onFileVolumeChange func() error) error {
	log := logger.GetLogger(ctx)

	if account := cfg.Global.VSphereServiceAccount; account != "" {
		selector := labels.Set{LabelServiceAccount: Bound(account)}.AsSelector().String()
		factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(namespace),
			informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector }))
		informer := factory.Core().V1().Secrets().Informer()
		trigger := make(chan struct{}, 1)
		drifted := func() bool {
			return driftedFrom(cfg, func(c *config.Config) error { return applyCredentials(ctx, client, namespace, c) })
		}
		if err := addTrigger(informer, trigger, drifted); err != nil {
			return err
		}
		factory.Start(ctx.Done())
		go runReloads(ctx, "credentials Secret", trigger, onCredentialsChange)
		log.Infof("Watching the credentials Secret of VSphereServiceAccount %q in namespace %q", account, namespace)
	}

	if hasSDDCName(cfg) {
		selector := fields.OneTermEqualSelector("metadata.name", FileVolumeConfigMapName).String()
		factory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithNamespace(namespace),
			informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.FieldSelector = selector }))
		informer := factory.Core().V1().ConfigMaps().Informer()
		trigger := make(chan struct{}, 1)
		drifted := func() bool {
			return driftedFrom(cfg, func(c *config.Config) error { return applyFileVolumeConfig(ctx, client, namespace, c) })
		}
		if err := addTrigger(informer, trigger, drifted); err != nil {
			return err
		}
		factory.Start(ctx.Done())
		go runReloads(ctx, "file volume ConfigMap", trigger, onFileVolumeChange)
		log.Infof("Watching ConfigMap %s/%s", namespace, FileVolumeConfigMapName)
	}
	return nil
}

func hasSDDCName(cfg *config.Config) bool {
	for _, vc := range cfg.VirtualCenter {
		if vc.SDDCName != "" {
			return true
		}
	}
	return false
}

// driftedFrom reports whether apply, run on a copy of cfg, would change the credentials or the vSAN
// file-share settings that cfg holds.
func driftedFrom(cfg *config.Config, apply func(*config.Config) error) bool {
	probe := &config.Config{Global: cfg.Global, VirtualCenter: map[types.FQDN]*config.VirtualCenterConfig{}}
	for host, vc := range cfg.VirtualCenter {
		c := *vc
		probe.VirtualCenter[host] = &c
	}
	if err := apply(probe); err != nil {
		return false // the reload would fail the same way; the next change retries it
	}
	for host, vc := range cfg.VirtualCenter {
		p := probe.VirtualCenter[host]
		if p.User != vc.User || p.Password != vc.Password || p.FileVolumeActivated != vc.FileVolumeActivated ||
			p.TargetvSANFileShareClusters != vc.TargetvSANFileShareClusters {
			return true
		}
	}
	return false
}

// addTrigger signals trigger for every change to an object seen by informer. Periodic resyncs are
// not changes. Neither are the objects present at start-up, unless drifted says the configuration
// the driver started with no longer matches them: they may have changed between reading the
// configuration and the informer's first list.
func addTrigger(informer cache.SharedIndexInformer, trigger chan<- struct{}, drifted func() bool) error {
	fire := func() {
		select {
		case trigger <- struct{}{}:
		default: // a reload is already pending
		}
	}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(_ interface{}, isInInitialList bool) {
			if !isInInitialList || drifted() {
				fire()
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if resourceVersion(oldObj) != resourceVersion(newObj) {
				fire()
			}
		},
		DeleteFunc: func(interface{}) { fire() },
	})
	if err != nil {
		return fmt.Errorf("failed to add an event handler: %w", err)
	}
	return nil
}

func resourceVersion(obj interface{}) string {
	switch o := obj.(type) {
	case *corev1.Secret:
		return o.ResourceVersion
	case *corev1.ConfigMap:
		return o.ResourceVersion
	}
	return ""
}

// runReloads calls reload for every signal on trigger, retrying failures.
func runReloads(ctx context.Context, what string, trigger <-chan struct{}, reload func() error) {
	log := logger.GetLogger(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-trigger:
		}
		log.Infof("Observed a change of the %s", what)
		for {
			err := reload()
			if err == nil {
				log.Infof("Reloaded the configuration after a change of the %s", what)
				break
			}
			log.Errorf("failed to reload the configuration after a change of the %s, will retry in %v. err: %+v",
				what, retryInterval, err)
			// A new change may fix what failed, so it ends the wait instead of queuing behind it.
			timer := time.NewTimer(retryInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-trigger:
				timer.Stop()
				log.Infof("Observed another change of the %s, retrying now", what)
			case <-timer.C:
			}
		}
	}
}
