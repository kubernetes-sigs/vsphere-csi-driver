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
	"time"

	"github.com/fsnotify/fsnotify"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	cnstypes "github.com/vmware/govmomi/cns/types"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
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
	config.SetPostProcessor(process)
}

// Enabled reports whether the runtime sources are in use: a Supervisor with the
// vsphere-csi-runtime-config FSS on.
func Enabled(ctx context.Context) bool {
	if commonco.ContainerOrchestratorUtility == nil {
		return false
	}
	flavor, err := config.GetClusterFlavor(ctx)
	if err != nil || flavor != cnstypes.CnsClusterFlavorWorkload {
		return false
	}
	return commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VSphereCSIRuntimeConfig)
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

// HandleCAFileEvent reconnects to vCenter, through reconnect, when event is a rotation of the CA
// bundle (see IsCAFileRotation). A failed reconnect is retried until it succeeds, as the callers'
// file watchers always did.
func HandleCAFileEvent(ctx context.Context, event fsnotify.Event, reconnect func() error) {
	if !IsCAFileRotation(ctx, event.Name, event.Op) {
		return
	}
	log := logger.GetLogger(ctx)
	log.Infof("Observed ca file rotation at: %q", event.Name)
	for {
		err := reconnect()
		if err == nil {
			log.Infof("Successfully re-established connection with VC from: %q", config.SupervisorCAFilePath)
			return
		}
		log.Errorf("failed to re-establish VC connection. Will retry again in %v. err: %+v", retryInterval, err)
		time.Sleep(retryInterval)
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
	return apply(ctx, client, config.GetCSINamespace(), cfg)
}

func apply(ctx context.Context, client kubernetes.Interface, namespace string, cfg *config.Config) error {
	if err := applyCredentials(ctx, client, namespace, cfg); err != nil {
		return err
	}
	return applyFileVolumeConfig(ctx, client, namespace, cfg)
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
	for host, vc := range cfg.VirtualCenter {
		if vc.SDDCName == "" {
			continue
		}
		vc.FileVolumeActivated = false
		vc.TargetvSANFileShareClusters = ""
		if v, ok := cm.Data[vc.SDDCName+fileVolumeActivatedSuffix]; ok {
			activated, err := strconv.ParseBool(strings.TrimSpace(v))
			if err != nil {
				return fmt.Errorf("ConfigMap %s/%s key %q for vCenter %q is not a boolean: %w", namespace,
					FileVolumeConfigMapName, vc.SDDCName+fileVolumeActivatedSuffix, host, err)
			}
			vc.FileVolumeActivated = activated
		}
		if v, ok := cm.Data[vc.SDDCName+targetvSANFileShareClustersSufx]; ok {
			vc.TargetvSANFileShareClusters = strings.TrimSpace(v)
		}
	}
	return nil
}
