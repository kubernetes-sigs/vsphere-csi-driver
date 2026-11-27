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
	"strconv"
	"strings"

	"gopkg.in/gcfg.v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	// legacyConfigSecretName and legacyConfigSecretKey locate the CSI config Secret that the
	// Supervisor package replaces. Before that happens it still holds the legacy INI.
	legacyConfigSecretName = "vsphere-config-secret"
	legacyConfigSecretKey  = "vsphere-cloud-provider.conf"
)

// MigrateFileVolumeConfig copies the legacy file-volume activation into the runtime ConfigMap before the
// Supervisor package replaces the CSI config Secret. It is meant for a one-shot Job that kapp reruns on
// every package spec change, so it is safe to run repeatedly: it writes "<sddc>.fileVolumeActivated" only
// when the key is absent, and never overwrites a value that an administrator or an earlier run set.
//
// The legacy value is false when the Secret is missing or already holds the package-rendered INI
// (vsphere-service-account set), because neither carries a legacy activation to preserve. The ConfigMap
// must exist: the package creates it before this Job runs.
func MigrateFileVolumeConfig(ctx context.Context, client kubernetes.Interface, namespace, sddcName string) error {
	if len(validation.IsDNS1123Label(namespace)) > 0 {
		return fmt.Errorf("invalid file-volume namespace %q: it must be a valid DNS-1123 label", namespace)
	}
	if len(validation.IsDNS1123Subdomain(sddcName)) > 0 {
		return fmt.Errorf("invalid SDDC name %q: it must be a valid DNS-1123 subdomain", sddcName)
	}
	key := sddcName + fileVolumeActivatedSuffix
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cm, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, FileVolumeConfigMapName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get ConfigMap %s/%s: %w", namespace, FileVolumeConfigMapName, err)
		}
		if _, exists := cm.Data[key]; exists {
			// Once the key exists it belongs to the Supervisor's management service. Return before reading
			// the Secret, so a rerun never depends on it.
			return nil
		}
		activated, err := legacyFileVolumeActivated(ctx, client, namespace)
		if err != nil {
			// Fail the Job: the package then stops before it replaces the Secret.
			return err
		}
		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}
		cm.Data[key] = strconv.FormatBool(activated)
		_, err = client.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

// legacyFileVolumeActivated reads fileVolumeActivated from the legacy CSI config Secret. Errors never
// include the INI content, which holds vCenter credentials.
func legacyFileVolumeActivated(ctx context.Context, client kubernetes.Interface, namespace string) (bool, error) {
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, legacyConfigSecretName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get Secret %s/%s: %w", namespace, legacyConfigSecretName, err)
	}
	raw := secret.Data[legacyConfigSecretKey]
	if len(raw) == 0 {
		return false, fmt.Errorf("secret %s/%s has no %q key", namespace, legacyConfigSecretName,
			legacyConfigSecretKey)
	}
	cfg := &config.Config{}
	if err := gcfg.FatalOnly(gcfg.ReadInto(cfg, strings.NewReader(string(raw)))); err != nil {
		// Parser errors can quote configuration values, so report only where the parse failed.
		return false, fmt.Errorf("the %q key of Secret %s/%s cannot be parsed", legacyConfigSecretKey, namespace,
			legacyConfigSecretName)
	}
	if cfg.Global.VSphereServiceAccount != "" {
		logger.GetLogger(ctx).Warnf("ConfigMap %s/%s has no file-volume activation and Secret %s/%s is already "+
			"package-rendered; defaulting to false", namespace, FileVolumeConfigMapName, namespace,
			legacyConfigSecretName)
		return false, nil
	}
	if len(cfg.VirtualCenterRaw) != 1 {
		return false, fmt.Errorf("the %q key of Secret %s/%s must have exactly one [VirtualCenter] section, found %d",
			legacyConfigSecretKey, namespace, legacyConfigSecretName, len(cfg.VirtualCenterRaw))
	}
	for _, vc := range cfg.VirtualCenterRaw {
		return vc.FileVolumeActivated, nil
	}
	return false, nil
}
