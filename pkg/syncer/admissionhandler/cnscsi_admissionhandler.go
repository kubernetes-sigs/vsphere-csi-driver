package admissionhandler

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	ValidationWebhookPath            = "/validate"
	MutationWebhookPath              = "/mutate"
	DefaultWebhookPort               = 9883
	DefaultWebhookMetricsBindAddress = "0"
)

func getWebhookPort() int {
	portStr, ok := os.LookupEnv("CNSCSI_WEBHOOK_SERVICE_CONTAINER_PORT")
	if !ok {
		return DefaultWebhookPort
	}

	result, err := strconv.ParseInt(portStr, 0, 0)
	if err != nil {
		panic(fmt.Sprintf("malformed configuration: CNSCSI_WEBHOOK_SERVICE_CONTAINER_PORT, expected int: %v", err))
	}

	return int(result)
}

func getMetricsBindAddress() string {
	metricsAddr, ok := os.LookupEnv("CNSCSI_WEBHOOK_SERVICE_METRICS_BIND_ADDR")
	if !ok {
		return DefaultWebhookMetricsBindAddress
	}

	return metricsAddr
}

// startCNSCSIWebhookManager starts the webhook server in supervisor cluster
func startCNSCSIWebhookManager(ctx context.Context) error {
	log := logger.GetLogger(ctx)

	webhookPort := getWebhookPort()
	metricsBindAddress := getMetricsBindAddress()
	log.Infof("setting up webhook manager with webhookPort %v and metricsBindAddress %v",
		webhookPort, metricsBindAddress)

	mgrOpts := manager.Options{
		Metrics: metricsserver.Options{
			BindAddress: metricsBindAddress,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port: webhookPort,
			TLSOpts: []func(*tls.Config){
				func(t *tls.Config) {
					// CipherSuites allows us to specify TLS 1.2 cipher suites that have been recommended by the Security team
					t.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
						tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384}
					t.MinVersion = tls.VersionTLS12
				},
			},
		})}

	if featureGateByokEnabled {
		var err error
		if mgrOpts.Scheme, err = crypto.NewK8sScheme(); err != nil {
			return err
		}

		mgrOpts.Client = client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{
					&corev1.ConfigMap{},
					&corev1.Secret{},
				},
			},
		}
	}

	mgr, err := manager.New(crConfig.GetConfigOrDie(), mgrOpts)
	if err != nil {
		log.Fatal(err, "unable to set up overall controller manager")
	}

	k8sClient := mgr.GetClient()
	cryptoClient := crypto.NewClient(ctx, k8sClient)

	log.Infof("registering validating webhook with the endpoint %v", ValidationWebhookPath)

	webhookServer := mgr.GetWebhookServer()

	webhookServer.Register(ValidationWebhookPath, &webhook.Admission{Handler: &CSISupervisorWebhook{
		Client:       k8sClient,
		CryptoClient: cryptoClient,
		clientConfig: mgr.GetConfig(),
	}})

	log.Infof("registering mutation webhook with the endpoint %v", MutationWebhookPath)
	webhookServer.Register(MutationWebhookPath, &webhook.Admission{Handler: &CSISupervisorMutationWebhook{
		Client:       k8sClient,
		CryptoClient: cryptoClient,
	}})

	if err = mgr.Start(signals.SetupSignalHandler()); err != nil {
		return fmt.Errorf("unable to run the webhook manager: %w", err)
	}

	return nil
}

var _ admission.Handler = &CSISupervisorWebhook{}

type CSISupervisorWebhook struct {
	client.Client
	CryptoClient crypto.Client
	clientConfig *rest.Config
}

func (h *CSISupervisorWebhook) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	log := logger.GetLogger(ctx)
	log.Debugf("CNS-CSI validation webhook handler called with request: %+v", req)
	defer log.Debugf("CNS-CSI validation webhook handler completed for the request: %+v", req)

	resp = admission.Allowed("")
	if req.Kind.Kind == "PersistentVolumeClaim" {
		if featureGateByokEnabled {
			resp = validatePVCRequestForCrypto(ctx, h.CryptoClient, req)
			if !resp.Allowed {
				return
			}
		}
		if featureGateTKGSHaEnabled {
			resp = validatePVCAnnotationForTKGSHA(ctx, req)
			if !resp.Allowed {
				return
			}
		}
		if featureGateVolumeHealthEnabled {
			resp = validatePVCAnnotationForVolumeHealth(ctx, req)
			if !resp.Allowed {
				return
			}
		}
		if featureGateBlockVolumeSnapshotEnabled {
			admissionResp := validatePVC(ctx, &req.AdmissionRequest)
			resp.AdmissionResponse = *admissionResp.DeepCopy()
		}
	} else if req.Kind.Kind == "CnsFileAccessConfig" {
		if featureFileVolumesWithVmServiceEnabled {
			admissionResp := validateCnsFileAccessConfig(ctx, h.clientConfig, &req.AdmissionRequest)
			resp.AdmissionResponse = *admissionResp.DeepCopy()

		}
	}
	return
}

var _ admission.Handler = &CSISupervisorMutationWebhook{}

type CSISupervisorMutationWebhook struct {
	client.Client
	CryptoClient crypto.Client
}

func (h *CSISupervisorMutationWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("CNS-CSI mutation webhook handler called with request: %+v", req)
	defer log.Debugf("CNS-CSI mutation webhook handler completed for the request: %+v", req)

	if req.Kind.Kind == "PersistentVolumeClaim" {
		switch req.Operation {
		case admissionv1.Create:
			return h.mutateNewPVC(ctx, req)
		}
	}

	return admission.Allowed("")
}

func (h *CSISupervisorMutationWebhook) mutateNewPVC(ctx context.Context, req admission.Request) admission.Response {
	newPVC := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(req.Object.Raw, newPVC); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	var wasMutated bool

	if featureGateByokEnabled {
		if ok, err := setDefaultEncryptionClass(ctx, h.CryptoClient, newPVC); err != nil {
			return admission.Denied(err.Error())
		} else if ok {
			wasMutated = true
		}
	}

	if !wasMutated {
		return admission.Allowed("")
	}

	newRawPVC, err := json.Marshal(newPVC)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, newRawPVC)
}
