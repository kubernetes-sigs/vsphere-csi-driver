package admissionhandler

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	ValidationWebhookPath            = "/validate"
	DefaultWebhookPort               = 9883
	DefaultWebhookMetricsBindAddress = "0"
	WebhookTlsMinVersion             = "1.2"
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
func startCNSCSIWebhookManager(ctx context.Context) {
	log := logger.GetLogger(ctx)

	webhookPort := getWebhookPort()
	metricsBindAddress := getMetricsBindAddress()
	log.Infof("setting up webhook manager with webhookPort %v and metricsBindAddress %v",
		webhookPort, metricsBindAddress)
	mgr, err := manager.New(crConfig.GetConfigOrDie(), manager.Options{
		MetricsBindAddress: metricsBindAddress,
		Port:               webhookPort})
	if err != nil {
		log.Fatal(err, "unable to set up overall controller manager")
	}

	log.Infof("registering validating webhook with the endpoint %v", ValidationWebhookPath)
	// we should not allow TLS < 1.2
	mgr.GetWebhookServer().TLSMinVersion = WebhookTlsMinVersion
	mgr.GetWebhookServer().Register(ValidationWebhookPath, &webhook.Admission{Handler: &CSISupervisorWebhook{
		Client:       mgr.GetClient(),
		clientConfig: mgr.GetConfig(),
	}})

	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		log.Fatal(err, "unable to run the webhook manager")
	}
}

var _ admission.Handler = &CSISupervisorWebhook{}

type CSISupervisorWebhook struct {
	client.Client
	clientConfig *rest.Config
}

func (h *CSISupervisorWebhook) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	log := logger.GetLogger(ctx)
	log.Debugf("CNS-CSI validation webhook handler called with request: %+v", req)
	defer log.Debugf("CNS-CSI validation webhook handler completed for the request: %+v", req)

	resp = admission.Allowed("")
	if req.Kind.Kind == "PersistentVolumeClaim" {
		if featureGateTKGSHaEnabled {
			resp = validatePVCAnnotationForTKGSHA(ctx, req)
			if !resp.Allowed {
				return
			}
		}
		if featureGateVolumeHealthEnabled {
			resp = validatePVCAnnotationForVolumeHealth(ctx, req)
		}
	}
	return
}
