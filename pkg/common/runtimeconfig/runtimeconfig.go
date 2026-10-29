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

// Package runtimeconfig completes the Supervisor driver configuration from objects in the
// Supervisor's API server instead of the vSphere config secret:
//   - the vCenter credentials, from the Secret vsphere-auth mints for a VSphereServiceAccount;
//   - the vSAN file-share settings, from the vsphere-csi-file-volume-config ConfigMap;
//   - trust-bundle rotation, which is delivered through a ConfigMap volume.
//
// Everything here is behind the vsphere-csi-runtime-config FSS. With the FSS off the driver
// behaves exactly as it does without this package.
package runtimeconfig

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	cnstypes "github.com/vmware/govmomi/cns/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

const (
	// FileVolumeConfigMapName is the ConfigMap that carries the vSAN file-share settings.
	FileVolumeConfigMapName = "vsphere-csi-file-volume-config"

	secretUsernameKey = "username"
	secretPasswordKey = "password"

	// The ConfigMap keys are "<sddc-name>" followed by one of these suffixes.
	fileVolumeActivatedSuffix       = ".fileVolumeActivated"
	targetvSANFileShareClustersSufx = ".targetvSANFileShareClusters"

	// caFileDataLink is the symlink a ConfigMap volume swaps to publish new content.
	caFileDataLink = "..data"
)

// reloadMu serializes configuration reloads. The config secret, credentials, file volume and CA
// watchers each run in their own goroutine and would otherwise reload, and reconnect to vCenter,
// concurrently.
var reloadMu sync.Mutex

// LockReload takes the lock every ReloadConfiguration implementation holds for its whole run.
// The returned function releases it.
func LockReload() (unlock func()) {
	reloadMu.Lock()
	return reloadMu.Unlock
}

var (
	clientMu     sync.Mutex
	cachedClient kubernetes.Interface

	// newClient is replaced in tests.
	newClient = func(ctx context.Context) (kubernetes.Interface, error) { return k8s.NewClient(ctx) }
)

// Register makes config.GetConfig complete Supervisor configs from the runtime sources. Call it
// once, after the container orchestrator utility is initialized and before the first
// GetConfig whose result is used to connect to vCenter.
func Register() {
	ctx, log := logger.GetNewContextWithLogger()
	latched.Store(boolState(enabled(ctx)))
	log.Infof("Runtime configuration sources enabled: %t", latched.Load() == stateOn)
	if latched.Load() == stateOff && isSupervisor(ctx) && commonco.ContainerOrchestratorUtility != nil &&
		!commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SupportsSelfManagedMode) {
		// The capability may turn on after the driver started. The container restarts when it does,
		// and Register then latches the runtime sources on.
		go commonco.ContainerOrchestratorUtility.HandleLateEnablementOfCapability(ctx,
			cnstypes.CnsClusterFlavorWorkload, common.SupportsSelfManagedMode, "", "")
	}
	config.SetPostProcessor(process)
}

const (
	stateUnset int32 = iota
	stateOff
	stateOn
)

// latched is the decision Register took. A feature state can flip while the driver runs, but the
// credentials, the file volume settings and the CA rotation handling are wired up once, so the
// decision must not change underneath them.
var latched atomic.Int32

func boolState(on bool) int32 {
	if on {
		return stateOn
	}
	return stateOff
}

// Enabled reports whether the runtime sources are in use: a Supervisor with the
// vsphere-csi-runtime-config FSS on and the supports_self_managed_mode capability on. Both are
// required, so a Supervisor without the capability always keeps the legacy configuration path. The answer is fixed by Register; before it is called the
// feature state is read each time.
func Enabled(ctx context.Context) bool {
	if state := latched.Load(); state != stateUnset {
		return state == stateOn
	}
	return enabled(ctx)
}

func enabled(ctx context.Context) bool {
	if commonco.ContainerOrchestratorUtility == nil {
		return false
	}
	if !isSupervisor(ctx) {
		return false
	}
	return commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.SupportsSelfManagedMode) &&
		commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VSphereCSIRuntimeConfig)
}

func isSupervisor(ctx context.Context) bool {
	flavor, err := config.GetClusterFlavor(ctx)
	return err == nil && flavor == cnstypes.CnsClusterFlavorWorkload
}

// IsCAFileRotation reports whether a file system event in the trust bundle directory means the
// vCenter CA bundle changed. WCP rotates the bundle by renaming a temporary file over vmca.pem;
// a ConfigMap volume instead swaps its ..data symlink, which only counts with the FSS on.
func IsCAFileRotation(ctx context.Context, name string, op fsnotify.Op) bool {
	if op&fsnotify.Create != fsnotify.Create {
		return false
	}
	if name == config.SupervisorCAFilePath {
		return true
	}
	return filepath.Dir(name) == filepath.Dir(config.SupervisorCAFilePath) &&
		filepath.Base(name) == caFileDataLink && Enabled(ctx)
}

// caReload coalesces CA rotations: while a reconnect is running or retrying, further rotations only
// mark it to run once more.
var caReload struct {
	sync.Mutex
	running, again bool
}

// HandleCAFileEvent reconnects to vCenter, through reconnect, when event is a rotation of the CA
// bundle (see IsCAFileRotation). The reconnect runs in the background, so the caller's file watcher
// keeps handling events, and a failed reconnect is retried until it succeeds or ctx is done.
func HandleCAFileEvent(ctx context.Context, event fsnotify.Event, reconnect func() error) {
	if !IsCAFileRotation(ctx, event.Name, event.Op) {
		return
	}
	logger.GetLogger(ctx).Infof("Observed ca file rotation at: %q", event.Name)
	caReload.Lock()
	defer caReload.Unlock()
	if caReload.running {
		caReload.again = true
		return
	}
	caReload.running = true
	go runCAReconnects(ctx, reconnect)
}

func runCAReconnects(ctx context.Context, reconnect func() error) {
	log := logger.GetLogger(ctx)
	for {
		for {
			err := reconnect()
			if err == nil {
				log.Infof("Successfully re-established connection with VC from: %q", config.SupervisorCAFilePath)
				break
			}
			log.Errorf("failed to re-establish VC connection. Will retry again in %v. err: %+v", retryInterval, err)
			select {
			case <-ctx.Done():
				caReload.Lock()
				caReload.running, caReload.again = false, false
				caReload.Unlock()
				return
			case <-time.After(retryInterval):
			}
		}
		caReload.Lock()
		if !caReload.again {
			caReload.running = false
			caReload.Unlock()
			return
		}
		caReload.again = false
		caReload.Unlock()
	}
}

func getClient(ctx context.Context) (kubernetes.Interface, error) {
	clientMu.Lock()
	defer clientMu.Unlock()
	if cachedClient != nil {
		return cachedClient, nil
	}
	c, err := newClient(ctx)
	if err != nil {
		return nil, err
	}
	cachedClient = c
	return c, nil
}

// process is the config.PostProcessor.
func process(ctx context.Context, cfg *config.Config) error {
	if !Enabled(ctx) {
		if cfg.Global.VSphereServiceAccount != "" {
			return fmt.Errorf("vsphere-service-account is set but the %s feature state is not enabled",
				common.VSphereCSIRuntimeConfig)
		}
		return nil
	}
	client, err := getClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a Kubernetes client: %w", err)
	}
	return apply(ctx, client, config.GetCSINamespace(), cfg, runtimeFileVolumes)
}

func apply(ctx context.Context, client kubernetes.Interface, namespace string, cfg *config.Config,
	fileVolumes *fileVolumeSource) error {
	if err := applyCredentials(ctx, client, namespace, cfg); err != nil {
		return err
	}
	// The vSAN settings are optional. A ConfigMap that cannot be read or parsed must not stop the
	// credentials from being applied. Keep the last successfully read settings, or the config
	// Secret's values when no matching cache entry exists, and let the file volume worker retry
	// independently. Otherwise, a fresh config Secret load could silently disable file volumes.
	if err := fileVolumes.apply(ctx, client, namespace, cfg); err != nil {
		logger.GetLogger(ctx).Warnf("failed to apply the file volume ConfigMap; using fallback vSAN file-share "+
			"settings and scheduling a retry. err: %+v", err)
	}
	return nil
}

// applyCredentials fills the user and password of every vCenter from the Secret of the
// configured VSphereServiceAccount. Exactly one Secret must match a vCenter: none means
// vsphere-auth has not minted it yet, several mean the labels are ambiguous.
func applyCredentials(ctx context.Context, client kubernetes.Interface, namespace string,
	cfg *config.Config) error {
	log := logger.GetLogger(ctx)
	account := cfg.Global.VSphereServiceAccount
	if account == "" {
		return nil
	}
	for host, vc := range cfg.VirtualCenter {
		selector := labels.Set{
			LabelServiceAccount: Bound(account),
			LabelSDDC:           Bound(vc.SDDCName),
		}.AsSelector().String()
		secrets, err := client.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return fmt.Errorf("failed to list the credentials Secret for vCenter %q in namespace %q: %w",
				host, namespace, err)
		}
		if len(secrets.Items) != 1 {
			return fmt.Errorf("expected exactly one credentials Secret for vCenter %q in namespace %q matching %q, "+
				"found %d", host, namespace, selector, len(secrets.Items))
		}
		secret := secrets.Items[0]
		user := string(secret.Data[secretUsernameKey])
		password := string(secret.Data[secretPasswordKey])
		if user == "" || password == "" {
			return fmt.Errorf("credentials Secret %s/%s for vCenter %q has no %q or %q", namespace, secret.Name,
				host, secretUsernameKey, secretPasswordKey)
		}
		if !config.IsValidvCenterUsernameWithDomain(user) {
			return fmt.Errorf("username in credentials Secret %s/%s for vCenter %q is invalid, "+
				"make sure that username is a fully qualified domain name", namespace, secret.Name, host)
		}
		vc.User = user
		vc.Password = password
		log.Debugf("Using the credentials of Secret %s/%s for vCenter %q", namespace, secret.Name, host)
	}
	return nil
}

// applyFileVolumeConfig sets the vSAN file-share settings of every vCenter that has an
// sddc-name from the file volume ConfigMap. Once the ConfigMap exists it is authoritative for
// those vCenters, so a missing key means "off" and not "whatever the config secret says". Without
// the ConfigMap, or for a vCenter without sddc-name, the values from the config secret stay.
func applyFileVolumeConfig(ctx context.Context, client kubernetes.Interface, namespace string,
	cfg *config.Config) error {
	cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, FileVolumeConfigMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get ConfigMap %s/%s: %w", namespace, FileVolumeConfigMapName, err)
	}
	// Parse everything before assigning anything, so an error leaves cfg as it was.
	type settings struct {
		activated bool
		clusters  string
	}
	parsed := map[types.FQDN]settings{}
	for host, vc := range cfg.VirtualCenter {
		if vc.SDDCName == "" {
			continue
		}
		var st settings
		_, hasActivated := cm.Data[vc.SDDCName+fileVolumeActivatedSuffix]
		_, hasClusters := cm.Data[vc.SDDCName+targetvSANFileShareClustersSufx]
		if !hasActivated && !hasClusters {
			logger.GetLogger(ctx).Warnf("ConfigMap %s/%s has no keys for SDDC %q, so vSAN file volumes are off for "+
				"vCenter %q", namespace, FileVolumeConfigMapName, vc.SDDCName, host)
		}
		if v, ok := cm.Data[vc.SDDCName+fileVolumeActivatedSuffix]; ok {
			activated, err := strconv.ParseBool(strings.TrimSpace(v))
			if err != nil {
				return fmt.Errorf("ConfigMap %s/%s key %q for vCenter %q is not a boolean: %w", namespace,
					FileVolumeConfigMapName, vc.SDDCName+fileVolumeActivatedSuffix, host, err)
			}
			st.activated = activated
		}
		if v, ok := cm.Data[vc.SDDCName+targetvSANFileShareClustersSufx]; ok {
			st.clusters = strings.TrimSpace(v)
		}
		parsed[host] = st
	}
	for host, st := range parsed {
		vc := cfg.VirtualCenter[host]
		vc.FileVolumeActivated = st.activated
		vc.TargetvSANFileShareClusters = st.clusters
	}
	return nil
}
