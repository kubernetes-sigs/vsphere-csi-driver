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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/fsnotify/fsnotify"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	cnsconfig "sigs.k8s.io/vsphere-csi-driver/v2/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v2/pkg/csi/service/logger"
)

type (
	parameterSet map[string]struct{}
)

// Has checks if specified paramName present in the paramSet.
func (paramSet parameterSet) Has(paramName string) bool {
	_, ok := paramSet[paramName]
	return ok
}

var (
	server *http.Server
	cfg    *config
	// COInitParams stores the input params required for initiating the
	// CO agnostic orchestrator in the admission handler package.
	COInitParams                 *interface{}
	containerOrchestratorUtility commonco.COCommonInterface
)

// watchConfigChange watches on the webhook configuration directory for changes
// like cert, key etc. This is required for certificate rotation.
func watchConfigChange() {
	ctx, log := logger.GetNewContextWithLogger()
	cfg, err := getWebHookConfig(ctx)
	if err != nil {
		log.Fatalf("failed to get webhook config. err: %v", err)
	}
	log.Debugf("Webhook config: %v", cfg)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("failed to create fsnotify watcher. err=%v", err)
	}
	go func() {
		for {
			var restartWebHookError error
			log.Debugf("Waiting for event on fsnotify watcher")
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Debugf("fsnotify event: %q", event.String())
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					log.Infof("Restarting webhook server with new config")
					errChan := make(chan error)
					go func() {
						err := restartWebhookServer(ctx)
						if err != nil {
							errChan <- err
							return
						}
					}()
					restartWebHookError = <-errChan
					if restartWebHookError != nil {
						log.Errorf("failed to restart webhook server. err: %v", err)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Errorf("fsnotify error: %+v", err)
					return
				}
			}
			log.Debugf("fsnotify event processed")
		}
	}()
	webHookConfigPath := os.Getenv(envWebHookConfigPath)
	if webHookConfigPath == "" {
		webHookConfigPath = defaultWebHookConfigPath
	}
	err = watcher.Add(webHookConfigPath)
	if err != nil {
		log.Fatalf("failed to watch on path: %q. err=%v", webHookConfigPath, err)
	}
}

// StartWebhookServer starts the webhook server.
func StartWebhookServer(ctx context.Context) error {
	var stopCh = make(chan bool)
	log := logger.GetLogger(ctx)
	var err error
	if cfg == nil {
		cfg, err = getWebHookConfig(ctx)
		if err != nil {
			log.Errorf("failed to get webhook config. err: %v", err)
			return err
		}
		log.Debugf("webhook config: %v", cfg)
	}
	if containerOrchestratorUtility == nil {
		clusterFlavor, err := cnsconfig.GetClusterFlavor(ctx)
		if err != nil {
			log.Errorf("Failed retrieving cluster flavor. Error: %v", err)
			return err
		}
		containerOrchestratorUtility, err = commonco.GetContainerOrchestratorInterface(ctx,
			common.Kubernetes, clusterFlavor, *COInitParams)
		if err != nil {
			log.Errorf("failed to get k8s interface. err: %v", err)
			return err
		}
	}
	if containerOrchestratorUtility.IsFSSEnabled(ctx, common.CSIMigration) {
		certs, err := tls.LoadX509KeyPair(cfg.WebHookConfig.CertFile, cfg.WebHookConfig.KeyFile)
		if err != nil {
			log.Errorf("failed to load key pair. certFile: %q, keyFile: %q err: %v",
				cfg.WebHookConfig.CertFile, cfg.WebHookConfig.KeyFile, err)
			return err
		}
		if cfg.WebHookConfig.Port == "" {
			cfg.WebHookConfig.Port = defaultWebhookServerPort
		}
		server = &http.Server{
			Addr:      fmt.Sprintf(":%v", cfg.WebHookConfig.Port),
			TLSConfig: &tls.Config{Certificates: []tls.Certificate{certs}},
		}
		// Define http server and server handler.
		mux := http.NewServeMux()
		mux.HandleFunc("/validate", validationHandler)
		server.Handler = mux

		// Start webhook server.
		log.Debugf("Starting webhook server on port: %v", cfg.WebHookConfig.Port)
		go func() {
			if err = server.ListenAndServeTLS(cfg.WebHookConfig.CertFile, cfg.WebHookConfig.KeyFile); err != nil {
				if err == http.ErrServerClosed {
					log.Info("Webhook server stopped")
				} else {
					log.Fatalf("failed to listen and serve webhook server. err: %v", err)
				}
			}
		}()
		log.Info("Webhook server started")
		watchConfigChange()
		<-stopCh
		return nil
	}
	return logger.LogNewError(log, "can't start webhook. no features are enabled which requires webhook")
}

// restartWebhookServer stops the webhook server and start webhook using
// updated config.
func restartWebhookServer(ctx context.Context) error {
	log := logger.GetLogger(ctx)
	cfg, err := getWebHookConfig(ctx)
	if err != nil {
		log.Errorf("failed to get webhook config. err: %v", err)
	}
	log.Debugf("Webhook config: %v", cfg)
	log.Info("Shutting down webhook server gracefully...")
	err = server.Shutdown(context.Background())
	if err != nil {
		log.Errorf("failed to get shutdown webhook server. err: %v", err)
		return err
	}
	log.Info("Restarting webhook server...")
	return StartWebhookServer(ctx)
}

// validationHandler is the handler for webhook http multiplexer to help
// validate resources. Depending on the URL validation of AdmissionReview
// will be redirected to appropriate validation function.
func validationHandler(w http.ResponseWriter, r *http.Request) {
	var body []byte
	ctx, log := logger.GetNewContextWithLogger()
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}
	if len(body) == 0 {
		log.Error("received empty request body")
		http.Error(w, "received empty request body", http.StatusBadRequest)
		return
	}
	log.Debugf("Received request")
	// Verify the content type is accurate.
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		log.Errorf("content-Type=%s, expect application/json", contentType)
		http.Error(w, "invalid Content-Type, expect `application/json`", http.StatusUnsupportedMediaType)
		return
	}

	var admissionResponse *admissionv1.AdmissionResponse
	ar := admissionv1.AdmissionReview{}
	codecs := serializer.NewCodecFactory(runtime.NewScheme())
	deserializer := codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(body, nil, &ar); err != nil {
		log.Errorf("Can't decode body: %v", err)
		admissionResponse = &admissionv1.AdmissionResponse{
			Result: &metav1.Status{
				Message: err.Error(),
			},
		}
	} else {
		if r.URL.Path == "/validate" {
			log.Debugf("request URL path is /validate")
			log.Debugf("admissionReview: %+v", ar)
			switch ar.Request.Kind.Kind {
			case "StorageClass":
				admissionResponse = validateStorageClass(ctx, &ar)
			default:
				log.Infof("Skipping validation for resource type: %q", ar.Request.Kind.Kind)
				admissionResponse = &admissionv1.AdmissionResponse{
					Allowed: true,
				}
			}
			log.Debugf("admissionResponse: %+v", admissionResponse)
		}
	}
	admissionReview := admissionv1.AdmissionReview{}
	admissionReview.APIVersion = "admission.k8s.io/v1"
	admissionReview.Kind = "AdmissionReview"
	if admissionResponse != nil {
		admissionReview.Response = admissionResponse
		if ar.Request != nil {
			admissionReview.Response.UID = ar.Request.UID
		}
	}
	resp, err := json.Marshal(admissionReview)
	if err != nil {
		log.Errorf("Can't encode response: %v", err)
		http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
	}
	log.Debugf("Ready to write response")
	if _, err := w.Write(resp); err != nil {
		log.Errorf("Can't write response: %v", err)
		http.Error(w, fmt.Sprintf("could not write response: %v", err), http.StatusInternalServerError)
	}
}
