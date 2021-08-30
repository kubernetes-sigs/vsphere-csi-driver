/*
Copyright 2020 The Kubernetes Authors.

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

package admissionhandler

import (
	"context"
	"os"

	"gopkg.in/gcfg.v1"

	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

const (
	envWebHookConfigPath     = "WEBHOOK_CONFIG_PATH"
	defaultWebHookConfigPath = "/etc/webhook/webhook.config"
	defaultWebhookServerPort = "8443"
)

// config holds webhook configuration and FeatureStatesConfig
type config struct {
	// WebHookConfig contains the detail about webhook - certfile, keyfile, port etc.
	WebHookConfig webHookConfig
}

// webHookConfig holds webhook configuration using which webhook http server will be created
type webHookConfig struct {
	// CertFile is the location of the certificate in the container
	CertFile string `gcfg:"cert-file"`
	// KeyFile is the location of the private-key in the container
	KeyFile string `gcfg:"key-file"`
	// Port is the webhook port on which http server should be started
	Port string `gcfg:"port"`
}

// getWebHookConfig returns webhook config
func getWebHookConfig(ctx context.Context) (*config, error) {
	log := logger.GetLogger(ctx)
	webHookConfigPath := os.Getenv(envWebHookConfigPath)
	if webHookConfigPath == "" {
		webHookConfigPath = defaultWebHookConfigPath
	}
	file, err := os.Open(webHookConfigPath)
	if err != nil {
		log.Errorf("failed to open %q. Err: %v", webHookConfigPath, err)
		return nil, err
	}
	cfg := &config{}
	if err := gcfg.FatalOnly(gcfg.ReadInto(cfg, file)); err != nil {
		log.Errorf("error while reading webhook config from file: %q: err: %+v", webHookConfigPath, err)
		return nil, err
	}
	return cfg, nil
}
